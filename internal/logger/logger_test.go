// SPDX-License-Identifier: GPL-3.0-or-later
package logger

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestRecentReturnsTheLastNLinesInChronologicalOrder(t *testing.T) {
	// JS 版 recent() 是 ring.slice(-limit)：返回**最近 N 行、时间正序**，
	// 面板按数组顺序渲染、最新在最下。这一顺序是前端契约，不能反。
	Init("")
	for i := 0; i < 5; i++ {
		Info("line", i)
	}
	got := Recent(3)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if !strings.Contains(got[0].Msg, "2") || !strings.Contains(got[2].Msg, "4") {
		t.Fatalf("want chronological [2,3,4], got %+v", got)
	}
}

func TestRecentLimitZeroReturnsNothing(t *testing.T) {
	Init("")
	Info("x")
	if got := Recent(0); len(got) != 0 {
		t.Fatalf("Recent(0) = %+v, want empty", got)
	}
}

func TestLevelsAreTagged(t *testing.T) {
	Init("")
	Info("a")
	Warn("b")
	Error("c")
	got := Recent(3)
	want := []string{"info", "warn", "error"}
	for i, w := range want {
		if got[i].Level != w {
			t.Fatalf("level[%d] = %q, want %q", i, got[i].Level, w)
		}
	}
}

func TestFileIsAppendedAndSurvivesReinit(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "freerouter.log")
	Init(file)
	Info("first")
	Init(file)
	Info("second")
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(b), "first") || !strings.Contains(string(b), "second") {
		t.Fatalf("log file lost a line across reinit:\n%s", b)
	}
}

func TestInitOnUnusablePathStillKeepsTheRing(t *testing.T) {
	// 面板与轮转日志绝不能因为磁盘问题整个失效；ring 是内存里的兜底。
	Init(filepath.Join(t.TempDir(), "missing", "a", "b.log"))
	Info("still works")
	if got := Recent(1); len(got) != 1 {
		t.Fatalf("ring lost the line, got %+v", got)
	}
}

func TestConcurrentWritesDoNotLoseLines(t *testing.T) {
	file := filepath.Join(t.TempDir(), "c.log")
	Init(file)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				Info("w", n, j)
			}
		}(i)
	}
	wg.Wait()
	if got := Recent(1000); len(got) != 200 {
		t.Fatalf("ring has %d lines, want 200", len(got))
	}
}

func TestLongMessageIsTruncatedOnRuneBoundary(t *testing.T) {
	Init("")
	Info(strings.Repeat("中", 5000))
	got := Recent(1)[0]
	// JS 是 .slice(0, 2000)：截到恰好 2000 字符，不追加省略号。
	if n := len([]rune(got.Msg)); n != 2000 {
		t.Fatalf("message is %d runes, want exactly 2000", n)
	}
	if !strings.HasPrefix(got.Msg, "中") {
		t.Fatalf("truncation mangled the head: %q", got.Msg[:20])
	}
}

// R10:轮转失败时不能把 size 归零。从前 `_ = os.Rename(...)` 丢掉错误后无条件
// `size = 0`,于是 Windows 上 .old.log 被占用(MoveFileEx 失败)时,记账归零而文件
// 还在原地 —— 下一次写又从头累加,data/gateway.log 无界增长且永远不会再轮转。
// 这里直接摆一个同名目录占住轮转目标,让 rename 必然失败。
func TestRotationKeepsTheSizeWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "freerouter.log")
	// 轮转目标是 <file 去掉 .log>.old.log;把它做成目录,rename 必失败。
	if err := os.Mkdir(filepath.Join(dir, "freerouter.old.log"), 0o755); err != nil {
		t.Fatalf("mkdir rotation target: %v", err)
	}
	Init(file)
	// 白盒:直接推到轮转阈值,省掉 5MB 的落盘。
	fileMu.Lock()
	size = maxFileBytes
	fileMu.Unlock()
	Info("after a failed rotation")

	fileMu.Lock()
	sz := size
	fileMu.Unlock()
	if sz <= maxFileBytes {
		t.Fatalf("size = %d after a failed rotation, want > %d:轮转失败不能把记账归零,否则文件无界增长", sz, maxFileBytes)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(b), "after a failed rotation") {
		t.Fatalf("轮转失败后这一行仍必须落盘:\n%s", b)
	}
}

// 轮转成功的正常路径:size 归零,旧文件改名成 .old.log。
func TestRotationResetsTheSizeOnSuccess(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "freerouter.log")
	Init(file)
	Info("first generation")
	fileMu.Lock()
	size = maxFileBytes
	fileMu.Unlock()
	Info("second generation")

	fileMu.Lock()
	sz := size
	fileMu.Unlock()
	if sz > maxFileBytes {
		t.Fatalf("size = %d, want <= %d(成功轮转后重新记账)", sz, maxFileBytes)
	}
	b, err := os.ReadFile(filepath.Join(dir, "freerouter.old.log"))
	if err != nil {
		t.Fatalf("read rotated file: %v", err)
	}
	if !strings.Contains(string(b), "first generation") {
		t.Fatalf(".old.log 应当是被轮转出去的那一代:\n%s", b)
	}
}
