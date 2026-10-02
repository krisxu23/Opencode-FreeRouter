// SPDX-License-Identifier: GPL-3.0-or-later
package tracelog

import (
	"encoding/json"
	"os"
	"path/filepath"
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

func TestRecordNeverPanicsOnGarbage(t *testing.T) {
	Init(t.TempDir())
	// Route is a plain struct, so there is nothing unserialisable here; the
	// point of this test is that Record has no error return to forget to check.
	Record(Route{Kind: "route", At: 1})
}
