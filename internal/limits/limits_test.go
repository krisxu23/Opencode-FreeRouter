// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package limits

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// 对照阶段 3 计划任务 12 步骤 5 的 7 条测试表,另加 2 条 Load 持久化测试。
// 引擎用 fake 注入:limits 是 L1,不能 import L4 的 engine,只认运行期形状
// `interface{ Stats(ctx) (any, error) }`。

// fakeEngine 是 Stats 形状的最小实现,stats 用 any 传 —— 与真实引擎的
// 松耦合方式和 app 组装时一致(序列化往返也是 Refresh 自己的解析路径)。
type fakeEngine struct {
	stats any
	err   error
}

func (f *fakeEngine) Stats(context.Context) (any, error) { return f.stats, f.err }

func statsPayload(rows map[string]map[string]any) map[string]any {
	return map[string]any{"usage": rows}
}

func newLimits(t *testing.T, eng *fakeEngine) *Limits {
	t.Helper()
	return New(filepath.Join(t.TempDir(), "limits.json"), eng)
}

func findRow(rows []Row, ip string) (Row, bool) {
	for _, r := range rows {
		if r.ExitIP == ip {
			return r, true
		}
	}
	return Row{}, false
}

func TestTotalSumsEveryExit(t *testing.T) {
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 3, "cap": 10},
		"2.2.2.2": {"used": 4, "cap": 10},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	used, cap := l.Total()
	if used != 7 {
		t.Fatalf("used = %d, want 7", used)
	}
	if cap != 20 {
		t.Fatalf("cap = %d, want 20", cap)
	}
}

func TestTotalSkipsZeroCapRows(t *testing.T) {
	// cap==0 的行不计入 cap:未知额度不等于无限额度,合计成无穷大会把
	// 面板的「余量」显示错。
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 3, "cap": 10},
		"2.2.2.2": {"used": 4},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	used, cap := l.Total()
	if used != 7 || cap != 10 {
		t.Fatalf("used, cap = %d, %d; want 7, 10", used, cap)
	}
}

func TestInFlightDeltaNeverGoesNegative(t *testing.T) {
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"9.9.9.9": {"used": 1, "cap": 5},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	l.InFlightDelta("9.9.9.9", -1)
	row, ok := findRow(l.Snapshot(), "9.9.9.9")
	if !ok {
		t.Fatal("row vanished after InFlightDelta(-1)")
	}
	if row.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0(钳底,不许为负)", row.InFlight)
	}
}

func TestInFlightZeroEntriesAreKept(t *testing.T) {
	// 与 health.exitBusy 同一教训:删掉再建会丢掉并发 Inc 的那一次,
	// 归零条目必须保留,由下一轮 Refresh 整体替换时自然回收。
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"9.9.9.9": {"used": 1, "cap": 5, "in_flight": 1},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	l.InFlightDelta("9.9.9.9", -1)
	rows := l.Snapshot()
	if len(rows) != 1 {
		t.Fatalf("len(Snapshot) = %d, want 1(归零条目保留)", len(rows))
	}
	if rows[0].InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", rows[0].InFlight)
	}
}

func TestSnapshotIsNewestFirst(t *testing.T) {
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"a": {"used": 1, "at": 100},
		"b": {"used": 1, "at": 300},
		"c": {"used": 1, "at": 200},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := l.Snapshot()
	if len(rows) != 3 {
		t.Fatalf("len(Snapshot) = %d, want 3", len(rows))
	}
	want := []string{"b", "c", "a"}
	for i, w := range want {
		if rows[i].ExitIP != w {
			t.Fatalf("Snapshot[%d] = %q, want %q(新→旧)", i, rows[i].ExitIP, w)
		}
	}
}

func TestRefreshReplacesTheWholeSnapshot(t *testing.T) {
	eng := &fakeEngine{}
	l := newLimits(t, eng)
	eng.stats = statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 1, "cap": 5},
		"2.2.2.2": {"used": 1, "cap": 5},
	})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 下一轮只剩一个 IP:整体替换,不是合并 —— 消失的出口必须消失,
	// 否则面板会一直给已经不存在的出口显示额度。
	eng.stats = statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 2, "cap": 5},
	})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := l.Snapshot()
	if len(rows) != 1 || rows[0].ExitIP != "1.1.1.1" {
		t.Fatalf("rows = %v, want only 1.1.1.1", rows)
	}
}

func TestRefreshKeepsOldDataOnError(t *testing.T) {
	eng := &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 3, "cap": 10},
	})}
	l := newLimits(t, eng)
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	eng.stats, eng.err = nil, errors.New("engine down")
	if err := l.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh must report the engine failure")
	}
	// 清空会让面板把「暂时查不到」显示成「额度全满」,所以必须保留旧数据。
	used, cap := l.Total()
	if used != 3 || cap != 10 {
		t.Fatalf("used, cap = %d, %d; want 3, 10(失败保留旧数据)", used, cap)
	}
}

func TestRowsAreKeyedByRawIP(t *testing.T) {
	// 额度按 IP 计(m09171 拍板:换 IP 额度就是全新的),任何归一化都会把
	// 两个真实不同的出口串成一行 —— "1.2.3.4 " 带空格也是一行。
	l := newLimits(t, &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"1.2.3.4":  {"used": 1},
		"1.2.3.4 ": {"used": 2},
	})})
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	rows := l.Snapshot()
	if len(rows) != 2 {
		t.Fatalf("len(Snapshot) = %d, want 2(键不归一化)", len(rows))
	}
	for _, ip := range []string{"1.2.3.4", "1.2.3.4 "} {
		if _, ok := findRow(rows, ip); !ok {
			t.Fatalf("row %q missing (键必须是原样 IP)", ip)
		}
	}
}

func TestLoadAcceptsMissingFile(t *testing.T) {
	// 文件不存在是首次运行,不是错误。
	l := New(filepath.Join(t.TempDir(), "missing.json"), &fakeEngine{})
	if err := l.Load(); err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if rows := l.Snapshot(); len(rows) != 0 {
		t.Fatalf("rows = %v, want empty", rows)
	}
}

func TestLoadRestoresSnapshotRows(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "limits.json")
	eng := &fakeEngine{stats: statsPayload(map[string]map[string]any{
		"1.1.1.1": {"used": 3, "cap": 10, "model": "big-pickle"},
	})}
	l1 := New(file, eng)
	if err := l1.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 新实例只 Load 不 Refresh:重启后不用等下一轮引擎拉取就有视图。
	l2 := New(file, nil)
	if err := l2.Load(); err != nil {
		t.Fatal(err)
	}
	rows := l2.Snapshot()
	if len(rows) != 1 || rows[0].ExitIP != "1.1.1.1" || rows[0].Used != 3 || rows[0].Cap != 10 || rows[0].Model != "big-pickle" {
		t.Fatalf("rows = %v, want restored snapshot row", rows)
	}
}
