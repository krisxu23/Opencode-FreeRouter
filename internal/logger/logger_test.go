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
