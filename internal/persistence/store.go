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
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// WriteJSONFile writes v to file atomically: a temp file in the same directory
// followed by a rename.
//
// The error return is load-bearing. Every caller (settings, node registry,
// health, stats) reads the file back immediately afterwards, and sing-box
// config is validated on disk before anything starts; a silent failure would
// leave the program running on the previous contents and reporting success.
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
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("persistence: 写临时文件 %s: %w", tmp, err)
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

// PromoteFile renames a fully written temp file over its target.
func PromoteFile(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("persistence: 提升 %s -> %s: %w", from, to, err)
	}
	return nil
}

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
func (s *Store) Flush() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
