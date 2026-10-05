// SPDX-License-Identifier: GPL-3.0-or-later
package logger

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

// TestRecentConcurrentWithWrites 是 fileMu 拆分的收益形状(六审 F8-1):
// 面板每 5s 用 RLock 轮询 Recent,拆分保证它**永不**被 open/write/rename 的
// 磁盘 IO 排队;并发 write + Recent 交错还要求 ring 自身不缺行、不崩。
// 拆回单锁不会让这里的断言变红,但会把它变成「读等磁盘」的串行 —— 与 -race
// 一起构成回归的兜底网。
func TestRecentConcurrentWithWrites(t *testing.T) {
	file := filepath.Join(t.TempDir(), "rw.log")
	Init(file)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(21)
	for i := 0; i < 20; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-stop:
					return
				default:
				}
				Info("w", n, j)
			}
		}(i)
	}
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			Recent(800) // 不空转:读一次,ring 锁与 fileMu 并发验证
		}
	}()
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()
	// 环形缓冲只兜最近 800 行:读到的行数必须在 [1, ringMax]。
	if got := len(Recent(1000)); got < 1 || got > 800 {
		t.Fatalf("ring rows = %d, want 1..800", got)
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

// TestHardCapStopsFileWritesWhenRotationIsBlocked 钉六审 F2 的封顶闸:退避
// 只降 rename 的重试频率、不解封顶 —— 杀软/备份常驻锁住 .old.log 时,无上限
// 的 gateway.log 会把同盘写满(连带 stats 落盘与 route 证据一起死,正是轮转
// 要防的事故)。越过 2×阈值后文件写停、ring 照常;障碍排除 + 探测窗到,下一条
// 写恢复落盘并关闸。
func TestHardCapStopsFileWritesWhenRotationIsBlocked(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "freerouter.log")
	blocker := filepath.Join(dir, "freerouter.old.log")
	if err := os.Mkdir(blocker, 0o755); err != nil {
		t.Fatalf("mkdir rotation target: %v", err)
	}
	Init(file)
	// 白盒把 size 直接推到硬封顶线下方;下一条 write 落盘后越线,rename 失败
	// → capped。
	fileMu.Lock()
	size = int64(hardCapFactor)*maxFileBytes - 10
	fileMu.Unlock()
	Info("cross the hard cap")
	fileMu.Lock()
	on := capped
	fileMu.Unlock()
	if !on {
		t.Fatal("越过 2×阈值且 rename 失败后必须进入 capped")
	}
	before := countFileLines(t, file)
	for i := 0; i < 5; i++ {
		Info("during cap")
	}
	if got := countFileLines(t, file); got != before {
		t.Fatalf("capped 期文件从 %d 行涨到 %d:封顶失效", before, got)
	}
	if n := len(Recent(100)); n < 6 {
		t.Fatalf("capped 期 ring 只收了 %d 行, want ≥6(封顶停文件不停内存)", n)
	}
	// 障碍排除 + 探测窗到(白盒拨前 nextProbe):下一条写恢复落盘,capped 关。
	// 恢复本身就是一次成功轮转:封顶期前唯一的那一行被搬进 .old.log,新文件
	// 从 0 行起,「after recovery」这一行必须在新文件里。
	if err := os.Remove(blocker); err != nil {
		t.Fatalf("unblock rotation target: %v", err)
	}
	fileMu.Lock()
	nextProbe = time.Now().Add(-time.Second)
	fileMu.Unlock()
	Info("after recovery")
	if n := countFileLines(t, file); n != 1 {
		t.Fatalf("恢复后的新文件行数 = %d, want 1(恢复 rename 轮转走旧文件,本行必须落新文件)", n)
	}
	bb, err := os.ReadFile(file)
	if err == nil && !strings.Contains(string(bb), "after recovery") {
		t.Fatalf("新文件里没有恢复那一行:\n%s", bb)
	}
	fileMu.Lock()
	stillCapped := capped
	fileMu.Unlock()
	if stillCapped {
		t.Fatal("rename 成功后 capped 应关闭")
	}
}

// 复审 A(第七轮):封顶期源文件被**外部删除**之后必须仍能重建。
//
// 杀软 quarantine 的动作就是删文件 —— 而这正是硬封顶要对付的场景。旧实现
// 只认「Rename 成功」为恢复判据,源不存在时 Rename 永远 ENOENT ⇒ capped 永为
// true,而能重建文件的 O_CREATE 路径只在 recovered 时到达 ⇒ 文件日志**永久
// 停摆**,只剩每小时一行内存 ring,面板之外的运维完全看不到(size 也停在
// ≥10MB,`size>>20` 的告警数字不再变化)。现在 ENOENT 视为「源已不在」,
// 归零放开,下一条写用 O_CREATE 把文件重建出来。
func TestQuarantinedLogFileIsRebuiltWhileCapped(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "freerouter.log")
	blocker := filepath.Join(dir, "freerouter.old.log")
	if err := os.Mkdir(blocker, 0o755); err != nil {
		t.Fatalf("mkdir rotation target: %v", err)
	}
	Init(file)
	// 白盒:推到硬封顶线下方;下一条 write 落盘后越线,rename 撞目录 → capped。
	fileMu.Lock()
	size = int64(hardCapFactor)*maxFileBytes - 10
	fileMu.Unlock()
	Info("cross the hard cap")
	fileMu.Lock()
	on := capped
	fileMu.Unlock()
	if !on {
		t.Fatal("前置:越过 2×阈值且 rename 失败后必须先进入 capped")
	}
	// 杀软把 gateway.log 隔离掉(文件消失),轮转目标(.old.log 目录)仍在。
	if err := os.Remove(file); err != nil {
		t.Fatalf("remove log file: %v", err)
	}
	fileMu.Lock()
	nextProbe = time.Now().Add(-time.Second)
	fileMu.Unlock()
	Info("after quarantine")

	fileMu.Lock()
	stillCapped := capped
	fileMu.Unlock()
	if stillCapped {
		t.Fatal("源文件已被外部删除:不得继续 capped —— Rename 永远 ENOENT,文件日志会永久停摆")
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("日志文件必须被重建(O_CREATE): %v", err)
	}
	if !strings.Contains(string(b), "after quarantine") {
		t.Fatalf("重建出来的文件里没有恢复那一行:\n%s", b)
	}
}

func countFileLines(t *testing.T, file string) int {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	return strings.Count(string(b), "\n")
}
