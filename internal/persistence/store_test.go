// SPDX-License-Identifier: GPL-3.0-or-later
package persistence

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// TestWriteJSONFileConcurrentWritersAllSucceed 钉住 B5：临时文件名必须每次
// 调用唯一。旧实现用固定的 `file + ".tmp"`，两个并发写者会互相踩：A 先 rename
// 走了 tmp，B 再 rename 就报 ENOENT（Windows 上还可能是共享冲突），于是
// data/stats.json 这类大文件被截断或清空，而调用方只看到一个写错误。
//
// 载荷故意做大（2000 个键），好让两个写者的 marshal+write 真正重叠；否则
// 完全串行的调度也能让旧实现碰巧通过。
func TestWriteJSONFileConcurrentWritersAllSucceed(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "big.json")

	big := make(map[string]string, 2000)
	for i := 0; i < 2000; i++ {
		big[strconv.Itoa(i)] = strings.Repeat("x", 64)
	}

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			errs[w] = WriteJSONFile(file, big, true)
		}(w)
	}
	wg.Wait()

	for w, err := range errs {
		if err != nil {
			t.Fatalf("写者 %d 失败：%v（并发写共用了同一个临时名）", w, err)
		}
	}

	// 目标文件必须是某一次完整写入的内容，不能是被截断的半截 JSON。
	var out map[string]string
	if err := ReadJSONFile(file, &out); err != nil {
		t.Fatalf("读回目标文件失败（说明它被写坏了）：%v", err)
	}
	if len(out) != 2000 {
		t.Fatalf("目标文件有 %d 个键，期望 2000（内容不完整）", len(out))
	}

	// 临时文件不得残留。
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(ents) != 1 {
		t.Fatalf("目录里有 %d 个条目，期望 1（临时文件泄漏）：%+v", len(ents), ents)
	}
}

// TestStoreFlushIsSafeWhenCalledConcurrently 是 B5 的第二面：Flush 过去只拿
// RLock，两个并发 Flush 会同时进入 WriteJSONFile 抢同一个临时名。改成写锁后
// 两次 Flush 串行，都必须成功。
func TestStoreFlushIsSafeWhenCalledConcurrently(t *testing.T) {
	file := filepath.Join(t.TempDir(), "settings.json")
	seed := make(map[string]any, 500)
	for i := 0; i < 500; i++ {
		seed[strconv.Itoa(i)] = strings.Repeat("y", 64)
	}
	s := NewStore("settings", file, seed)

	const flushers = 8
	var wg sync.WaitGroup
	errs := make([]error, flushers)
	for i := 0; i < flushers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Flush()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 个 Flush 失败：%v", i, err)
		}
	}
	var out map[string]any
	if err := ReadJSONFile(file, &out); err != nil {
		t.Fatalf("读回失败：%v", err)
	}
	if len(out) != 500 {
		t.Fatalf("落盘 %d 个键，期望 500", len(out))
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
//
// 原来钉在这里的 TestPromoteFileReplacesAtomically 随 PromoteFile 一起删了(审计
// O6):原子替换的契约由 TestWriteJSONFileConcurrentWritersAllSucceed 那一条在
// **唯一入口**上钉住,单独测一个可绕锁的 rename 反而会把那条路径写成"支持"。
