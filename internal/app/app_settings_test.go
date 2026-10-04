// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// B7 的现场:面板 PUT(写 *Parts.Settings)与 5 秒一次的 /api/status 轮询、
// 探测轮(读同一块内存)在同一个 *Settings 上无锁对撞。这些测试用**不启动
// sing-box 的极小 Parts** —— internal/app 里起真 host 会先撞上上游
// sing-box v1.14.0 的既有竞态(route/network.go:574 读 vs :220 写,经
// sing-tun 的 monitor),那个 WARNING 里没有 freerouter 帧,会把本包的
// 竞态淹掉,所以验证 B7 必须绕开 Host。

package app

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/health"
	"freerouter/internal/limits"
	"freerouter/internal/persistence"
	"freerouter/internal/registry"
)

// settingsOnlyParts 组装一个只够跑 Status/SettingsView/ApplySettings 的
// Parts:没有 Host、没有 Forward、没有 Panel、没有 Prober。
func settingsOnlyParts(t *testing.T) *Parts {
	t.Helper()
	dir := t.TempDir()
	reg := registry.NewRegistry(filepath.Join(dir, "node-registry.json"))
	if err := reg.Load(); err != nil {
		t.Fatalf("registry load: %v", err)
	}
	s := defaultSettings()
	store := persistence.NewStore("settings", filepath.Join(dir, "settings.json"), settingsMap(s))
	if err := store.Load(); err != nil {
		t.Fatalf("settings load: %v", err)
	}
	return &Parts{
		Root:          dir,
		Settings:      &s,
		Registry:      reg,
		Health:        health.NewHealth(filepath.Join(dir, "node-health.json")),
		catalog:       &catalogBox{list: catalog.Static()},
		settingsStore: store,
	}
}

// TestSettingsSnapshotIsAnIsolatedCopy 断言快照与运行中的设置脱钩:改快照
// 不回写,改设置不追加快照。同时覆盖 setSettings 在 nil 指针上的分配路径。
func TestSettingsSnapshotIsAnIsolatedCopy(t *testing.T) {
	p := &Parts{}
	p.setSettings(Settings{ProbeWorkers: 7})
	if got := p.settingsSnapshot(); got.ProbeWorkers != 7 {
		t.Fatalf("setSettings 后快照 = %d, want 7", got.ProbeWorkers)
	}
	got := p.settingsSnapshot()
	got.ProbeWorkers = 9
	if again := p.settingsSnapshot(); again.ProbeWorkers != 7 {
		t.Fatalf("快照是内部状态的别名: %d", again.ProbeWorkers)
	}
	p.setSettings(Settings{ProbeWorkers: 11})
	if again := p.settingsSnapshot(); again.ProbeWorkers != 11 {
		t.Fatalf("setSettings 没有覆盖旧值: %d", again.ProbeWorkers)
	}
	// 零值 Parts 也必须安全:Load 失败后的 Parts 会被 Shutdown 碰到。
	var zero Parts
	if got := zero.settingsSnapshot(); got.ProbeWorkers != 0 || got.SubURLs != nil {
		t.Fatalf("零值 Parts 的快照 = %+v, want 零值", got)
	}
	if zero.hasSettings() {
		t.Fatal("零值 Parts 报告已装配设置")
	}
	p2 := &Parts{}
	p2.setSettings(Settings{})
	if !p2.hasSettings() {
		t.Fatal("setSettings 之后应报告已装配设置")
	}
}

// TestSettingsAreRaceFreeUnderConcurrentReadAndWrite 是 B7 的回归测试:
// 一个 goroutine 反复面板 PUT(写),另一个反复读 SettingsView/Status。
// 修复前 -race 会报 status.go 的写与 status.go 的读竞争。
// TestHotColdIntervalsClamp:1.3.0 的三档间隔读取处必须各自有 clamp —— 这条
// 路径不经面板 PUT 校验(手改 settings.json / 旧版本落盘的超大值直接灌进
// Settings),读取处不 clamp,启动参数就是越界的 time.Duration 算术。
func TestHotColdIntervalsClamp(t *testing.T) {
	p := settingsOnlyParts(t)

	p.setSettings(Settings{HotIntervalSec: 0})
	if got := p.hotInterval(); got != 15*time.Second {
		t.Fatalf("hotInterval(0) = %v, want 下限 15s", got)
	}
	p.setSettings(Settings{HotIntervalSec: 30 * 24 * 3600})
	if got := p.hotInterval(); got != 3600*time.Second {
		t.Fatalf("hotInterval(超大) = %v, want 上限 3600s", got)
	}
	p.setSettings(Settings{HotIntervalSec: 60})
	if got := p.hotInterval(); got != 60*time.Second {
		t.Fatalf("hotInterval(60) = %v", got)
	}
	p.setSettings(Settings{ColdIntervalSec: 0})
	if got := p.coldInterval(); got != 60*time.Second {
		t.Fatalf("coldInterval(0) = %v, want 下限 60s", got)
	}
	p.setSettings(Settings{ColdIntervalSec: 1 << 53})
	if got := p.coldInterval(); got != 86400*time.Second {
		t.Fatalf("coldInterval(2^53) = %v, want 上限 86400s", got)
	}
	p.setSettings(Settings{RefreshIntervalMin: 0})
	if got := p.refreshInterval(); got != 5*time.Minute {
		t.Fatalf("refreshInterval(0) = %v, want 下限 5 分钟", got)
	}
	p.setSettings(Settings{RefreshIntervalMin: 1 << 53})
	if got := p.refreshInterval(); got != 10080*time.Minute {
		t.Fatalf("refreshInterval(2^53) = %v, want 上限 10080 分钟", got)
	}
}

// TestApplySettingsRejectsOverCapIntervals 钉住 PUT 那条路的同步上限:超界
// 必须被拒,且**不动磁盘**(与 B10 同一纪律)。
func TestApplySettingsRejectsOverCapIntervals(t *testing.T) {
	p := newProbeParts(t, 1)
	settingsFile := filepath.Join(p.Root, "settings.json")
	p.settingsStore = persistence.NewStore("settings", settingsFile, settingsMap(defaultSettings()))
	if err := p.settingsStore.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := p.settingsStore.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before, _ := os.ReadFile(settingsFile)

	for key, val := range map[string]float64{"refreshIntervalMin": 10081, "hotIntervalSec": 3601, "coldIntervalSec": 86401} {
		if _, err := p.ApplySettings(map[string]any{key: val}); err == nil {
			t.Fatalf("ApplySettings 接受了越界的 %s=%v", key, val)
		}
	}
	after, _ := os.ReadFile(settingsFile)
	if string(after) != string(before) {
		t.Fatalf("被拒的补丁动了磁盘")
	}
	// 合法值必须仍能写入(别把 clamp 写成一律拒绝)。
	if _, err := p.ApplySettings(map[string]any{"refreshIntervalMin": float64(30)}); err != nil {
		t.Fatalf("refreshIntervalMin=30 应当合法: %v", err)
	}
}

func TestSettingsAreRaceFreeUnderConcurrentReadAndWrite(t *testing.T) {
	p := settingsOnlyParts(t)

	const rounds = 200
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = p.SettingsView()
			_ = p.Status()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			patch := map[string]any{
				"hotIntervalSec": float64(15 + i%20),
				"probeWorkers":   float64(1 + i%8),
			}
			if _, err := p.ApplySettings(patch); err != nil {
				t.Errorf("ApplySettings: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}

// TestLimitsFetchedAtIsRaceFreeUnderConcurrentReadAndWrite 是 B8 的回归测试。
// refreshLimitsOverlay 原先在 overlayMu 下读 limitsFetchedAt、在 limitsMu 下写
// 它,两把锁各自「正确」合起来不构成互斥;面板的 /api/status 与 /api/limits
// 同时读这三个字段。这里把 fetchedAt 预置成刚刚,OverlayStale 于是为假 ——
// 一轮刷新不会出网,只留下读-写对撞。
func TestLimitsFetchedAtIsRaceFreeUnderConcurrentReadAndWrite(t *testing.T) {
	p := settingsOnlyParts(t)
	p.overlayByID = map[string]limits.OverlayRow{}
	p.limitsFetchedAt = p.nowMS()

	const rounds = 3000
	var wg sync.WaitGroup

	// 读侧:面板的 /api/status 与 /api/limits 走这两个入口。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = p.LimitsView()
		}
	}()

	// 写侧:另一条路径把 fetchedAt 拨回「刚刚」,免得刷新真的去打 models.dev。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			p.limitsMu.Lock()
			p.limitsFetchedAt = p.nowMS()
			p.limitsMu.Unlock()
		}
	}()

	// 刷新侧:被 refreshLimitsOverlay 读 fetchedAt 的那一处。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if err := p.refreshLimitsOverlay(context.Background()); err != nil {
				t.Errorf("refreshLimitsOverlay: %v", err)
				return
			}
		}
	}()

	wg.Wait()
}
