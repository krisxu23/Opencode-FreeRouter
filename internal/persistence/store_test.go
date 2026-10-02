// SPDX-License-Identifier: GPL-3.0-or-later
package persistence

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteJSONFileIsAtomicAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "a.json")
	if err := WriteJSONFile(file, map[string]any{"x": 1}, true); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out map[string]int
	if err := ReadJSONFile(file, &out); err != nil || out["x"] != 1 {
		t.Fatalf("read back = %v, err = %v", out, err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(ents) != 1 {
		t.Fatalf("dir has %d entries, want 1 (temp file leaked): %+v", len(ents), ents)
	}
}

func TestWriteJSONFilePropagatesFailure(t *testing.T) {
	// 写入失败必须报错。调用方紧接着就要读这个文件（sing-box 配置、
	// node-registry 都是这条路径），静默降级会让它拿旧文件继续跑。
	// 缺失目录会被 MkdirAll 建出来（JS 版 writeJsonFile 的 mkdirSync
	// recursive 同样如此，见 src/persistence.js:84），所以制造失败得用
	// 「路径中间是普通文件」这个 MkdirAll 救不了的死局。
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed blocker: %v", err)
	}
	missing := filepath.Join(blocker, "no-such-dir", "a.json")
	if err := WriteJSONFile(missing, map[string]int{}, false); err == nil {
		t.Fatal("write into an unwritable path returned nil, want an error")
	}
}

func TestReadJSONFileMissingIsNotExist(t *testing.T) {
	var out map[string]int
	err := ReadJSONFile(filepath.Join(t.TempDir(), "nope.json"), &out)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestStoreUpdateMergesThenFlush(t *testing.T) {
	file := filepath.Join(t.TempDir(), "settings.json")
	s := NewStore("settings", file, map[string]any{"a": 1, "b": 2})
	s.Update(map[string]any{"b": 9, "c": 3})
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	var out map[string]any
	if err := ReadJSONFile(file, &out); err != nil {
		t.Fatalf("read: %v", err)
	}
	if out["a"].(float64) != 1 || out["b"].(float64) != 9 || out["c"].(float64) != 3 {
		t.Fatalf("merged = %v, want a=1 b=9 c=3", out)
	}
}

func TestStoreGetReturnsACopy(t *testing.T) {
	s := NewStore("s", filepath.Join(t.TempDir(), "s.json"), map[string]any{"a": 1})
	got := s.Get().(map[string]any)
	got["a"] = 999
	if again := s.Get().(map[string]any)["a"]; again != 1 {
		t.Fatalf("mutating Get() result leaked into the store: a = %v", again)
	}
}

func TestStoreLoadLetsDiskWinOverDefaults(t *testing.T) {
	file := filepath.Join(t.TempDir(), "settings.json")
	if err := WriteJSONFile(file, map[string]any{"forwardPort": 9999}, true); err != nil {
		t.Fatalf("seed: %v", err)
	}
	s := NewStore("settings", file, map[string]any{"forwardPort": 3457})
	if err := s.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := s.Get().(map[string]any)["forwardPort"].(float64); got != 9999 {
		t.Fatalf("forwardPort = %v, want 9999 (disk must win)", got)
	}
}

func TestStoreLoadKeepsDefaultsForAbsentKeys(t *testing.T) {
	file := filepath.Join(t.TempDir(), "settings.json")
	if err := WriteJSONFile(file, map[string]any{"forwardPort": 9999}, true); err != nil {
		t.Fatalf("seed: %v", err)
	}
	s := NewStore("settings", file, map[string]any{"forwardPort": 3457, "panelPort": 3458})
	if err := s.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if got := s.Get().(map[string]any)["panelPort"]; got != 3458 {
		t.Fatalf("panelPort = %v, want 3458 (absent key must fall back to the default)", got)
	}
}

func TestStoreLoadWithoutFileKeepsDefaults(t *testing.T) {
	s := NewStore("settings", filepath.Join(t.TempDir(), "absent.json"), map[string]any{"panelPort": 3458})
	if err := s.Load(); err != nil {
		t.Fatalf("load on a fresh install must not be an error: %v", err)
	}
	if got := s.Get().(map[string]any)["panelPort"]; got != 3458 {
		t.Fatalf("panelPort = %v, want 3458", got)
	}
}

// Store 的三项行为各有专门测试：Get 必须深拷贝（TestStoreGetReturnsACopy）、
// Update 必须浅层 merge、Load 必须让磁盘赢过默认值（TestStoreLoadLetsDiskWinOverDefaults
// 与 TestStoreLoadKeepsDefaultsForAbsentKeys）。这三件事在 JS 版里互相纠缠：
// Get 返回引用、Update 浅合并且写回、Load 整表替换。Go 版把它们拆成三个可单测的语义。
func TestPromoteFileReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	from := filepath.Join(dir, "b.json.tmp")
	to := filepath.Join(dir, "a.json")
	if err := os.WriteFile(from, []byte(`{"k":2}`), 0o644); err != nil {
		t.Fatalf("seed from: %v", err)
	}
	if err := os.WriteFile(to, []byte(`{"k":1}`), 0o644); err != nil {
		t.Fatalf("seed to: %v", err)
	}
	if err := PromoteFile(from, to); err != nil {
		t.Fatalf("promote: %v", err)
	}
	b, err := os.ReadFile(to)
	if err != nil {
		t.Fatalf("read to: %v", err)
	}
	if string(b) != `{"k":2}` {
		t.Fatalf("to = %s, want the promoted content", b)
	}
	if _, err := os.Stat(from); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("from still exists, err = %v", err)
	}
}
