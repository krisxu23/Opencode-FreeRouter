// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package stats

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// dayKeyOf mirrors the production rule so a test can name the bucket it expects
// without importing the unexported helper.
func dayKeyOf(at int64) string {
	return time.UnixMilli(at).UTC().Format("2006-01-02")
}

func readFile(t *testing.T, file string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("读 %s: %v", file, err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("解析 %s: %v", file, err)
	}
	return out
}

func TestNewWritesInitialShapeOnFirstFlush(t *testing.T) {
	file := filepath.Join(t.TempDir(), "stats.json")
	s := New(file)
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got := readFile(t, file)
	if got["requests"] != float64(0) {
		t.Fatalf("requests = %v, 期望 0", got["requests"])
	}
	// 三个容器必须是 {} / [] 而不是 null：JS 的 openStore 初值就是空对象与空数组，
	// 前端第一帧直接把它当对象用，null 会让整页崩掉。
	days, ok := got["days"].(map[string]any)
	if !ok || len(days) != 0 {
		t.Fatalf("days = %#v, 期望空对象", got["days"])
	}
	models, ok := got["models"].(map[string]any)
	if !ok || len(models) != 0 {
		t.Fatalf("models = %#v, 期望空对象", got["models"])
	}
	samples, ok := got["samples"].([]any)
	if !ok || len(samples) != 0 {
		t.Fatalf("samples = %#v, 期望空数组", got["samples"])
	}
}

func TestLoadOnMissingFileIsNotAnError(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "stats.json"))
	if err := s.Load(); err != nil {
		t.Fatalf("Load 缺文件应返回 nil，得到 %v", err)
	}
	if got := s.Snapshot().Requests; got != 0 {
		t.Fatalf("Requests = %d, 期望 0", got)
	}
}

func TestRecordCountsDayModelAndTotal(t *testing.T) {
	file := filepath.Join(t.TempDir(), "stats.json")
	s := New(file)
	now := time.Now().UnixMilli()
	s.Record(Record{At: now, Model: "space-bunny-free", OK: true, Input: 10, Output: 5})

	snap := s.Snapshot()
	if snap.Requests != 1 {
		t.Fatalf("Requests = %d, 期望 1", snap.Requests)
	}
	day := snap.Days[dayKeyOf(now)]
	if day.Req != 1 || day.In != 10 || day.Out != 5 {
		t.Fatalf("Days[today] = %+v, 期望 {1 10 5}", day)
	}
	m := snap.Models["space-bunny-free"]
	if m.Req != 1 || m.In != 10 || m.Out != 5 {
		t.Fatalf("Models[model] = %+v, 期望 {1 10 5}", m)
	}
}

// TestDayKeyIsUTCNotLocal 是 UTC 语义的回归锁：东八区的 07:30 在 UTC 还是前一天，
// 若有人把 dayKey 改成 time.Local，这条会立刻红。
func TestDayKeyIsUTCNotLocal(t *testing.T) {
	shanghai := time.FixedZone("CST", 8*3600)
	at := time.Date(2026, 10, 1, 7, 30, 0, 0, shanghai).UnixMilli()

	s := New("")
	s.Record(Record{At: at, Model: "m", OK: true, Input: 1, Output: 1})

	snap := s.Snapshot()
	if _, ok := snap.Days["2026-09-30"]; !ok {
		t.Fatalf("Days 键 = %v, 期望 2026-09-30（UTC 日切）", snap.Days)
	}
	if _, ok := snap.Days["2026-10-01"]; ok {
		t.Fatal("2026-10-01 不该出现：本地日期不是统计口径")
	}
	hist := s.History(1, at)
	if len(hist) != 1 || hist[0].Date != "2026-09-30" {
		t.Fatalf("History(1, at) = %+v, 期望一行 2026-09-30", hist)
	}
}

func TestSamplesAreCappedByCount(t *testing.T) {
	s := New("")
	now := time.Now().UnixMilli()
	for i := 0; i < 2500; i++ {
		s.Record(Record{At: now, Model: "m", OK: true, Output: int64(i)})
	}
	samples := s.Snapshot().Samples
	if len(samples) != sampleMax {
		t.Fatalf("len(Samples) = %d, 期望 %d", len(samples), sampleMax)
	}
	if samples[0].Out != 500 || samples[len(samples)-1].Out != 2499 {
		t.Fatalf("保留的不是最后 %d 条：首 %d 末 %d", sampleMax, samples[0].Out, samples[len(samples)-1].Out)
	}
}

// TestSamplesAreCappedByTimeWindow 把条数上限压低，好让时间窗成为唯一的筛选条件。
// JS 的时间窗只在条数超过上限时才生效（src/index.js:196-199），直接记 11 条永远
// 走不到那段代码，所以这里必须走 sampleMax 这个测试缝。
func TestSamplesAreCappedByTimeWindow(t *testing.T) {
	restore := sampleMax
	sampleMax = 10
	defer func() { sampleMax = restore }()

	s := New("")
	stale := time.Now().Add(-25 * time.Hour).UnixMilli()
	for i := 0; i < 10; i++ {
		s.Record(Record{At: stale, Model: "m", OK: true, Output: 1})
	}
	s.Record(Record{At: time.Now().UnixMilli(), Model: "m", OK: true, Output: 2})

	samples := s.Snapshot().Samples
	if len(samples) != 1 {
		t.Fatalf("len(Samples) = %d, 期望 1（25 小时前的样本已被时间窗滤掉）", len(samples))
	}
	if samples[0].Out != 2 {
		t.Fatalf("留下的样本 Out = %d, 期望 2（新鲜那条）", samples[0].Out)
	}
}

func TestSnapshotIsADeepCopy(t *testing.T) {
	s := New("")
	now := time.Now().UnixMilli()
	s.Record(Record{At: now, Model: "m", OK: true, Input: 1, Output: 2})

	first := s.Snapshot()
	second := s.Snapshot()

	first.Days[dayKeyOf(now)] = Bucket{Req: 99, In: 99, Out: 99}
	first.Models["m"] = Bucket{Req: 99}
	first.Samples[0].Out = 99

	if second.Days[dayKeyOf(now)].Req != 1 {
		t.Fatalf("改第一次快照污染了第二次：Days = %+v", second.Days)
	}
	if second.Models["m"].Req != 1 {
		t.Fatalf("改第一次快照污染了第二次：Models = %+v", second.Models)
	}
	if second.Samples[0].Out != 2 {
		t.Fatalf("改第一次快照污染了第二次：Samples = %+v", second.Samples)
	}
}

func TestHistoryPadsMissingDaysAscending(t *testing.T) {
	s := New("")
	now := time.Now().UnixMilli()
	s.Record(Record{At: now, Model: "m", OK: true, Input: 3, Output: 4})

	hist := s.History(7, now)
	if len(hist) != 7 {
		t.Fatalf("len(History) = %d, 期望 7", len(hist))
	}
	wantFirst := dayKeyOf(now - 6*86400000)
	wantLast := dayKeyOf(now)
	if hist[0].Date != wantFirst {
		t.Fatalf("首行 = %s, 期望 %s", hist[0].Date, wantFirst)
	}
	if hist[6].Date != wantLast {
		t.Fatalf("末行 = %s, 期望 %s", hist[6].Date, wantLast)
	}
	if hist[6].Req != 1 || hist[6].In != 3 || hist[6].Out != 4 {
		t.Fatalf("今天那行 = %+v, 期望 {1 3 4}", hist[6])
	}
	for i := 0; i < 6; i++ {
		if hist[i].Req != 0 || hist[i].In != 0 || hist[i].Out != 0 {
			t.Fatalf("第 %d 行没补零：%+v", i, hist[i])
		}
	}
}

// TestRecordNeverPanicsOnAReadOnlyFile 钉住「stats 绝不能影响一次回答」：
// 落盘失败只记进 lastErr，内存计数照常。
func TestRecordNeverPanicsOnAReadOnlyFile(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("准备阻断文件: %v", err)
	}
	// 父路径是一个普通文件 ⇒ MkdirAll/WriteFile 必然失败。
	s := New(filepath.Join(blocker, "stats.json"))

	s.Record(Record{At: time.Now().UnixMilli(), Model: "m", OK: true, Output: 1})

	if got := s.Snapshot().Requests; got != 1 {
		t.Fatalf("Requests = %d, 期望 1（内存里必须记上）", got)
	}
	if s.LastError() == "" {
		t.Fatal("落盘失败应被记进 LastError，供 /api/status 诊断")
	}
}

func TestRecordSurvivesConcurrentCalls(t *testing.T) {
	s := New("")
	now := time.Now().UnixMilli()
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.Record(Record{At: now, Model: "m", OK: true, Output: 1})
			}
		}()
	}
	wg.Wait()

	snap := s.Snapshot()
	if snap.Requests != 5000 {
		t.Fatalf("Requests = %d, 期望 5000", snap.Requests)
	}
	if got := snap.Days[dayKeyOf(now)].Req; got != 5000 {
		t.Fatalf("Days[today].Req = %d, 期望 5000", got)
	}
}

func TestLoadReplacesStateNotMerges(t *testing.T) {
	file := filepath.Join(t.TempDir(), "stats.json")
	if err := os.WriteFile(file, []byte(`{"requests":7}`), 0o644); err != nil {
		t.Fatalf("准备文件: %v", err)
	}

	s := New(file)
	if err := s.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := s.Snapshot().Requests; got != 7 {
		t.Fatalf("Requests = %d, 期望 7", got)
	}
	if got := len(s.Snapshot().Samples); got != 0 {
		t.Fatalf("len(Samples) = %d, 期望 0（Load 是整体替换，不是合并）", got)
	}

	s.Record(Record{At: time.Now().UnixMilli(), Model: "m", OK: true, Output: 1})
	if got := len(s.Snapshot().Samples); got != 1 {
		t.Fatalf("Record 后 len(Samples) = %d, 期望 1", got)
	}
}
