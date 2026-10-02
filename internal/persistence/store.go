// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package persistence owns every write to data/: atomic JSON files and the
// settings-shaped Store the rest of the program reads through.
package persistence

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// pathLockShards is the number of mutexes guarding atomic replacement, hashed
// by target path. A shard array rather than a map keeps memory bounded no
// matter how many paths a caller invents, at the cost of occasional false
// sharing between two unrelated files — which is harmless here, since every
// write in this program is a handful of kilobytes to a few hundred kilobytes
// and the alternative is unbounded map growth.
const pathLockShards = 64

var pathLocks [pathLockShards]sync.Mutex

// lockPath serializes writers that target the same file and returns the
// unlock function.
//
// Unique temp names are not sufficient on their own (B5). Even with a private
// temp file, two goroutines calling rename onto the same destination race:
// on Windows the second MoveFileEx can fail with "Access is denied" while the
// first is still swapping the destination, and on any platform the loser can
// observe a partially visible replacement. Since the whole point of
// WriteJSONFile is "readers never see a half-written file", the replacement
// itself has to be exclusive per target.
func lockPath(file string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(filepath.Clean(file)))
	m := &pathLocks[h.Sum32()%pathLockShards]
	m.Lock()
	return m.Unlock
}

// WriteJSONFile writes v to file atomically: a temp file in the same directory
// followed by a rename.
//
// The error return is load-bearing. Every caller (settings, node registry,
// health, stats) reads the file back immediately afterwards, and sing-box
// config is validated on disk before anything starts; a silent failure would
// leave the program running on the previous contents and reporting success.
//
// The temp name must be unique per call (B5). The JS version used
// `${file}.${process.pid}.tmp` (src/persistence.js:82-93), which is unique
// across processes but *not* within one — and the Go side has many more
// concurrent writers in a single process than the JS side ever did, because
// stats.Record writes synchronously from every request goroutine. With a fixed
// `file + ".tmp"`, two writers race on the same temp path: one renames it away,
// the other's rename then fails with ENOENT, or on Windows with a sharing
// violation, and the target file is left truncated or empty. os.CreateTemp
// appends a random suffix and opens with O_EXCL, so two calls can never pick
// the same name.
func WriteJSONFile(file string, v any, indent bool) error {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("persistence: 建目录 %s: %w", dir, err)
	}
	var b []byte
	var err error
	if indent {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		return fmt.Errorf("persistence: 序列化 %s: %w", file, err)
	}

	// 同一目标路径的替换必须互斥：临时名唯一只解决了「撞同一个 tmp」，
	// 两个 goroutine 同时 rename 到同一目标在 Windows 上仍会 "Access is
	// denied"（B5 实测）。
	unlock := lockPath(file)
	defer unlock()

	// 临时文件必须与目标同目录：跨卷的 rename 不是原子替换。
	// 模式串里的 `*` 会被 CreateTemp 换成随机串。
	f, err := os.CreateTemp(dir, filepath.Base(file)+".*.tmp")
	if err != nil {
		return fmt.Errorf("persistence: 建临时文件 %s: %w", file, err)
	}
	tmp := f.Name()
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("persistence: 写临时文件 %s: %w", tmp, err)
	}
	// 先 Close 再 Rename：Windows 不允许改名一个仍被打开的句柄。
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("persistence: 关临时文件 %s: %w", tmp, err)
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("persistence: 设权限 %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, file); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("persistence: 替换 %s: %w", file, err)
	}
	return nil
}

// ReadJSONFile decodes file into out. A missing file reports fs.ErrNotExist so
// callers can use errors.Is to tell "first run" from "corrupt file".
func ReadJSONFile(file string, out any) error {
	b, err := os.ReadFile(file)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("persistence: 文件不存在 %s: %w", file, fs.ErrNotExist)
		}
		return fmt.Errorf("persistence: 读 %s: %w", file, err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("persistence: 解析 %s: %w", file, err)
	}
	return nil
}

// PromoteFile 删掉了(审计 O6):它是 WriteJSONFile 内部那一步 rename 的公开外壳,
// 生产从未单独调用 —— 而 B5 之后,提升必须与「同一目标路径的写锁」「唯一临时文件名」
// 两件事一起做,单独暴露一个 rename 等于提供一个能绕过这两条保护的入口。
// 原子替换的唯一路径是 WriteJSONFile。

// Store is the settings-shaped bag of top-level keys the rest of the program
// reads and writes. It is deliberately not generic: every persisted file in
// this project is a flat map with a handful of keys plus one nested object, and
// a typed struct per file would force the panel to re-derive the shape anyway.
//
// The three behaviours below are the migration's deliberate departures from the
// JS version, each pinned by a test:
//
//   - Get returns a deep copy. The JS version returned the live object, so a
//     caller mutating a field silently rewrote the store's state.
//   - Update merges only the top level. The JS version did the same; it is
//     spelled out so a future nested write is a conscious decision.
//   - Load lets disk win per key and keeps defaults for absent keys, so a
//     settings file written by an older build still starts.
type Store struct {
	name    string
	file    string
	initial map[string]any
	mu      sync.RWMutex
	value   map[string]any
}

// NewStore returns a store seeded with initial. Nothing touches disk until
// Load or Flush.
func NewStore(name, file string, initial any) *Store {
	s := &Store{name: name, file: file, initial: map[string]any{}}
	if m, ok := initial.(map[string]any); ok {
		for k, v := range m {
			s.initial[k] = cloneValue(v)
		}
	}
	s.value = cloneMap(s.initial)
	return s
}

// Get returns a deep copy of the current value.
func (s *Store) Get() any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneMap(s.value)
}

// Update merges patch into the value and returns the new deep copy.
func (s *Store) Update(patch map[string]any) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range patch {
		s.value[k] = cloneValue(v)
	}
	return cloneMap(s.value)
}

// Flush writes the value to disk with 2-space indentation.
//
// The lock is exclusive, not shared (B5): Flush writes the value it reads, and
// with only RLock two concurrent Flush calls could interleave with an Update
// and serialize a half-updated map. The panel's PUT /api/settings and the
// shutdown path can both land here, so serializing is the only correct choice.
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := WriteJSONFile(s.file, s.value, true); err != nil {
		return fmt.Errorf("store %s: %w", s.name, err)
	}
	return nil
}

// Load merges the file over the defaults. A missing file is not an error: it is
// the first run. A corrupt file is an error, because silently falling back to
// defaults would rewrite the user's settings on the next Flush.
func (s *Store) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var onDisk map[string]any
	if err := ReadJSONFile(s.file, &onDisk); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			s.value = cloneMap(s.initial)
			return nil
		}
		return fmt.Errorf("store %s: %w", s.name, err)
	}
	merged := cloneMap(s.initial)
	for k, v := range onDisk {
		merged[k] = cloneValue(v)
	}
	s.value = merged
	return nil
}

// File returns the store's on-disk path (settings, log, ...). Callers that
// need to stat or delete the file use this instead of keeping their own copy.
func (s *Store) File() string { return s.file }

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneMap(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	default:
		return v
	}
}
