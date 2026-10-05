// SPDX-License-Identifier: GPL-3.0-or-later
package tracelog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func todayUTC() string { return time.Now().UTC().Format("2006-01-02") }

func TestRecordWritesJsonlToTodaysFile(t *testing.T) {
	dir := t.TempDir()
	Init(dir)
	// JS 的 dayKey 是 record.at 的 UTC 日期；不传 at 才落到今天。
	// 显式传当前时刻，断言文件按该时刻的 UTC 日归档。
	Record(Route{Kind: "route", Model: "m", Result: "成功", MS: 12, At: time.Now().UnixMilli(),
		Order: []OrderRow{{Tag: "a", Bucket: -1, Cost: 30, Latency: 30}},
		Tries: []TryRow{{Tag: "a", Code: "ok", MS: 12, Served: true}}})
	b, err := os.ReadFile(filepath.Join(dir, todayUTC()+".jsonl"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got Route
	if err := json.Unmarshal(b[:len(b)-1], &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Model != "m" || len(got.Order) != 1 || len(got.Tries) != 1 {
		t.Fatalf("roundtrip lost fields: %+v", got)
	}
	if got.Order[0].Bucket != -1 {
		t.Fatalf("bucket = %d, want -1 (negative buckets are real)", got.Order[0].Bucket)
	}
}

func TestRecentReturnsTheLastNInChronologicalOrder(t *testing.T) {
	// JS routeRecent 是 ring.slice(-limit)：最近 N 条、时间正序。
	Init(t.TempDir())
	Record(Route{Result: "一"})
	Record(Route{Result: "二"})
	got := Recent(5)
	if len(got) != 2 || got[0].Result != "一" || got[1].Result != "二" {
		t.Fatalf("want chronological [一,二], got %+v", got)
	}
}

func TestRingIsCapped(t *testing.T) {
	Init(t.TempDir())
	for i := 0; i < ringMax+20; i++ {
		Record(Route{Result: string(rune('a' + i%26)), MS: int64(i)})
	}
	if got := Recent(1000); len(got) != ringMax {
		t.Fatalf("ring has %d, want %d", len(got), ringMax)
	}
}

func TestOrderIsCappedAtEight(t *testing.T) {
	Init(t.TempDir())
	var order []OrderRow
	for i := 0; i < 30; i++ {
		order = append(order, OrderRow{Tag: string(rune('a' + i%26))})
	}
	Record(Route{Order: order})
	if got := Recent(1)[0].Order; len(got) != 8 {
		t.Fatalf("order kept %d rows, want 8", len(got))
	}
}

func TestPruneKeepsTheReadmeSentinel(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "2000-01-01.jsonl")
	if err := os.WriteFile(old, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("sentinel"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	Init(dir)
	Record(Route{Result: "x"})
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old jsonl survived, err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "README.md")); err != nil {
		t.Fatalf("README sentinel was deleted: %v", err)
	}
}

func TestUnusableDirectoryDegradesToRingOnly(t *testing.T) {
	Init(filepath.Join(t.TempDir(), "no", "such", "dir"))
	Record(Route{Result: "still here"})
	if got := Recent(1); len(got) != 1 {
		t.Fatalf("ring lost the record, got %+v", got)
	}
}

// TestRecordKeepsBackdatedRowsOnTheirOwnDay 钉住「At 早于当前轮转日的行
// 不再被永久丢弃」。旧实现把 `lastPruneDay != day` 当成丢弃条件:时钟回拨、
// 上游回放、调用方显式给历史时刻,任何一条这样的记录都在第一个新一天之后
// 永远进不了盘 —— 旧日文件明明还在,面板上那条轨迹就是空的。
func TestRecordKeepsBackdatedRowsOnTheirOwnDay(t *testing.T) {
	dir := t.TempDir()
	Init(dir)

	today := time.Now().UTC()
	yesterday := today.AddDate(0, 0, -1)
	// 先落一条今天,把 lastPruneDay 推进到今天。
	Record(Route{Result: "今天第一条", At: today.UnixMilli()})
	// 再落一条昨天的:它必须写进昨天那个文件。
	Record(Route{Result: "回拨的一行", At: yesterday.Add(time.Hour).UnixMilli()})

	b, err := os.ReadFile(filepath.Join(dir, yesterday.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("回拨记录必须落到它自己的日文件: %v", err)
	}
	if !strings.Contains(string(b), "回拨的一行") {
		t.Fatalf("昨天的文件里没有那一条: %s", b)
	}
	// 字节账记在**当前轮转日**的额度上(保守裁决,见实现注释):两条都扣。
	mu.RLock()
	q := dayBytes
	mu.RUnlock()
	if q <= 0 {
		t.Fatalf("dayBytes = %d, want >0(两行都真实落盘才扣)", q)
	}
}

// TestFusesResetOnDayRollover 与跨天重置块配套:熔过的日子过去后,新的一天
// 计数从 0 起 —— 单独成例是因为它需要把 writeOff 先熔满(与
// TestWriteOffRevives 的重叠路径容易互相遮蔽)。
func TestFusesResetOnDayRollover(t *testing.T) {
	dir := t.TempDir()
	Init(dir)
	today := time.Now().UTC()
	block := filepath.Join(dir, today.Format("2006-01-02")+".jsonl")
	if err := os.Mkdir(block, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < maxConsecFails; i++ {
		Record(Route{Result: "熔", At: today.UnixMilli()})
	}
	mu.RLock()
	off := writeOff
	mu.RUnlock()
	if !off {
		t.Fatal("熔满后 writeOff 应为真")
	}
	if err := os.Remove(block); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	// 新的一天:reset 块必须同时清 writeOff 与 consecFails。
	tomorrow := today.AddDate(0, 0, 1)
	Record(Route{Result: "明天", At: tomorrow.UnixMilli()})
	mu.RLock()
	fails := consecFails
	off = writeOff
	mu.RUnlock()
	if off {
		t.Fatal("跨天后 writeOff 应复活(旧 B6 语义保持)")
	}
	if fails != 0 {
		t.Fatalf("跨天后 consecFails = %d, want 0(六审 F6)", fails)
	}
}

func TestRecordNeverPanicsOnGarbage(t *testing.T) {
	Init(t.TempDir())
	// Route is a plain struct, so there is nothing unserialisable here; the
	// point of this test is that Record has no error return to forget to check.
	Record(Route{Kind: "route", At: 1})
}

// TestWriteOffRevivesOnANewDay 是 B6 的回归测试。JS 的配额账是
// dayBytes: Map<day, bytes>,所以跨天天然不受影响;Go 用单变量 + 一个
// writeOff 标志,而跨天分支只重置 dayBytes,从不复活 writeOff ⇒
// 某一天触顶(或被杀软占用一次日文件)之后,data/route/<day>.jsonl
// 在整个进程余下生命周期都不再增长,面板正常、历史为空、任何地方都不报错。
//
// 这里把 dayBytes 顶到上限触发 writeOff,再喂一条「明天」的记录:
// 新的一天必须重新开始写盘。
func TestWriteOffRevivesOnANewDay(t *testing.T) {
	dir := t.TempDir()
	Init(dir)

	today := time.Now().UTC()
	Record(Route{Result: "今天第一条", At: today.UnixMilli()})
	if _, err := os.Stat(filepath.Join(dir, today.Format("2006-01-02")+".jsonl")); err != nil {
		t.Fatalf("today's file missing: %v", err)
	}

	// 模拟当天配额耗尽。JS 在 size >= MAX 时就停写,所以直接顶到上限。
	mu.Lock()
	dayBytes = maxBytesPerDay
	mu.Unlock()
	Record(Route{Result: "触顶", At: today.UnixMilli()})
	mu.RLock()
	off := writeOff
	mu.RUnlock()
	if !off {
		t.Fatal("配额耗尽后 writeOff 应为真")
	}

	// 跨天:明天必须重新落盘,而不是沿用昨天的停写状态。
	tomorrow := today.AddDate(0, 0, 1)
	Record(Route{Result: "明天", At: tomorrow.UnixMilli()})
	b, err := os.ReadFile(filepath.Join(dir, tomorrow.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("B6:跨天后没有恢复写盘: %v", err)
	}
	if !strings.Contains(string(b), "明天") {
		t.Fatalf("明天的文件里没有新记录: %s", b)
	}
}

// TestWriteOffRevivesWhenTheFileBecomesWritableAgain 覆盖另一条永久停写的
// 入口:日文件被占用(杀软/编辑器)导致 OpenFile 或 Write 失败。JS 同样置
// writeDisabled,但它的配额按天独立,所以第二天照样能写;Go 的 writeOff
// 是进程级标志,必须跨天复活,否则一次瞬时 I/O 错误就终结整个进程的追踪。
func TestWriteOffRevivesWhenTheFileBecomesWritableAgain(t *testing.T) {
	dir := t.TempDir()
	Init(dir)

	// 用一个目录冒充当天的 jsonl 文件:OpenFile 必然失败。
	today := time.Now().UTC()
	target := filepath.Join(dir, today.Format("2006-01-02")+".jsonl")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	Record(Route{Result: "写不进去", At: today.UnixMilli()})
	mu.RLock()
	off := writeOff
	fails := consecFails
	mu.RUnlock()
	if off {
		t.Fatal("单次 OpenFile 失败不应整日停写:瞬时抖动只丢几行,连续 5 次才停")
	}
	if fails != 1 {
		t.Fatalf("consecFails = %d, want 1", fails)
	}

	// 再来 4 次连续失败才整日停写。
	for i := 0; i < maxConsecFails-1; i++ {
		Record(Route{Result: "还是写不进去", At: today.UnixMilli()})
	}
	mu.RLock()
	off = writeOff
	mu.RUnlock()
	if !off {
		t.Fatalf("连续 %d 次失败后 writeOff 应为真", maxConsecFails)
	}

	// 障碍排除(文件被释放)+ 新的一天:两个条件合起来必须恢复。
	if err := os.Remove(target); err != nil {
		t.Fatalf("unblock: %v", err)
	}
	tomorrow := today.AddDate(0, 0, 1)
	Record(Route{Result: "恢复", At: tomorrow.UnixMilli()})
	b, err := os.ReadFile(filepath.Join(dir, tomorrow.Format("2006-01-02")+".jsonl"))
	if err != nil {
		t.Fatalf("B6:障碍排除 + 跨天后没有恢复写盘: %v", err)
	}
	if !strings.Contains(string(b), "恢复") {
		t.Fatalf("明天的文件里没有新记录: %s", b)
	}

	// 六审 F6 的回归主体:跨天块必须把 consecFails 一起清零。漏清的话,熔过
	// 的坏天把计数留在 ≥maxConsecFails,第二天第 1 次瞬时抖动就重新熔掉整天
	// —— 「一次抖动不丢一天」只对首个坏天成立。在**新的一天**再制造 1 次
	// 失败:writeOff 必须仍是 false、计数只到 1。(明天的文件此刻是刚才
	// 「恢复」那一行建出来的普通文件:删掉换成目录,OpenFile 必失败。)
	tomorrowFile := filepath.Join(dir, tomorrow.Format("2006-01-02")+".jsonl")
	if err := os.Remove(tomorrowFile); err != nil {
		t.Fatalf("unblock tomorrow: %v", err)
	}
	if err := os.Mkdir(tomorrowFile, 0o755); err != nil { // 用目录占住明天的文件
		t.Fatalf("block tomorrow: %v", err)
	}
	Record(Route{Result: "新的一天抖一下", At: tomorrow.Add(time.Hour).UnixMilli()})
	mu.RLock()
	off = writeOff
	fails = consecFails
	mu.RUnlock()
	if off {
		t.Fatal("跨天后第 1 次失败就整日停写:consecFails 没有随日重置(六审 F6)")
	}
	if fails != 1 {
		t.Fatalf("跨天后 consecFails = %d, want 1(新的一天从头数)", fails)
	}
	_ = os.Remove(tomorrowFile)
}

// 复审 B(第七轮):历史日期的记录**不得**清零当前轮转日的账。
//
// 旧判据是 `day != lastPruneDay`:一条 At 早于当前轮转日的记录(时钟回拨、
// 上游回放、调用方显式给历史时刻)会把 lastPruneDay 拉回旧日期,并把
// dayBytes/writeOff/consecFails 一起清零 —— 在两天之间交替的 At 能把 32MB/天
// 的配额账无限重置,而写的是不同日名的文件,连「撞限」都不会发生。生产不可达
// (唯一调用点的 At ≡ nowMS()),但账本不该有这条路:今天已经熔断的日子,不能
// 被一条历史的记录洗白。
func TestBackdatedRowDoesNotResetTheDayLedger(t *testing.T) {
	dir := t.TempDir()
	Init(dir)
	today := time.Now().UTC()
	yesterday := today.AddDate(0, 0, -1)
	// 用同名目录占住今天的日文件,把 writeOff 熔满(maxConsecFails 次)。
	block := filepath.Join(dir, today.Format("2006-01-02")+".jsonl")
	if err := os.Mkdir(block, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < maxConsecFails; i++ {
		Record(Route{Result: "熔", At: today.UnixMilli()})
	}
	mu.RLock()
	off := writeOff
	mu.RUnlock()
	if !off {
		t.Fatal("前置:今天的 writeOff 应先熔上")
	}
	// 一条昨天的记录:它不得把今天的 writeOff(以及 dayBytes/consecFails)
	// 洗掉。注意 writeOff 期间这条本身仍被丢弃(`off` 早退) —— 闸门是「今天
	// 的追踪停写」,不是「只允许写今天」;旧实现在这里会先被跨天块把 writeOff
	// 清成 false,于是不但账被洗白,这条历史行还会顺着昨天的文件写出去。
	Record(Route{Result: "回拨", At: yesterday.UnixMilli()})
	mu.RLock()
	off = writeOff
	mu.RUnlock()
	if !off {
		t.Fatal("一条历史日期的记录把今天的 writeOff 洗掉了:每日配额账被回拨重置")
	}
}
