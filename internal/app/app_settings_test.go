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
// TestProbeIntervalIsClampedAgainstDurationOverflow 是生命周期审计 #2 的钉:
// time.Duration(minutes)*time.Minute 在 minutes 超过约 1.5 亿时**溢出成 ≤0**,
// 而调用方 probeLoop 的 time.After(≤0) 立即就绪 → 整轮 O(pool) 缓存扫描的
// 自旋循环,探测预算全烧在 spin 上。审计员的复现:2e8 → -1790760h,2^53 → 0s。
//
// 这条路径**不经面板 PUT 校验**就能到达:手改 settings.json 或旧版本落盘的
// 超大值,Load/Reload 直接把它灌进 Settings.ProbeIntervalMin。所以修复必须落
// 在**实时读取处**(probeInterval 的 clamp),而不是只在 validateSettingsPatch
// 加个上限 —— 这里同时钉两处。
func TestProbeIntervalIsClampedAgainstDurationOverflow(t *testing.T) {
	p := settingsOnlyParts(t)

	for _, tc := range []struct {
		name    string
		minutes int
		wantMin int // clamp 之后期望的分钟数
	}{
		{"正常值不动", 30, 30},
		{"下界", 5, 5},
		{"低于下界归 5", 0, 5},
		{"审计复现值 2e8 溢出", 200000000, 43200},
		{"2^53 归零", 1 << 53, 43200},
		{"恰好上限", 43200, 43200},
		{"超上限归 43200", 43201, 43200},
	} {
		p.setSettings(Settings{ProbeIntervalMin: tc.minutes})
		got := p.probeInterval()
		if want := time.Duration(tc.wantMin) * time.Minute; got != want {
			t.Errorf("%s: probeInterval() = %v, want %v(clamp 到 %d 分钟;溢出成 ≤0 就是这条 bug)",
				tc.name, got, want, tc.wantMin)
		}
		// 溢出即 ≤0,这条直接判死自旋条件。
		if got <= 0 {
			t.Errorf("%s: probeInterval() = %v,必须恒 >0(≤0 = probeLoop 自旋)", tc.name, got)
		}
	}
}

// TestApplySettingsRejectsProbeIntervalOverTheCap 钉住 PUT 那条路的同步上限:
// 面板字段只有 min=5、没有 max,填 1e9 必须被拒,且**不动磁盘**(与 B10 同一
// 纪律)。与上面的实时 clamp 是两道独立的闸:一个管新写入,一个管已落盘的历史值。
func TestApplySettingsRejectsProbeIntervalOverTheCap(t *testing.T) {
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

	if _, err := p.ApplySettings(map[string]any{"probeIntervalMin": float64(1_000_000_000)}); err == nil {
		t.Fatal("ApplySettings 接受了溢出的 probeIntervalMin(1e9),未设上限")
	}
	after, _ := os.ReadFile(settingsFile)
	if string(after) != string(before) {
		t.Fatalf("被拒的补丁动了磁盘:\n before %s\n after  %s", before, after)
	}
	// 合法的上界值必须仍能写入(别把 clamp 写成一律拒绝)。
	if _, err := p.ApplySettings(map[string]any{"probeIntervalMin": float64(43200)}); err != nil {
		t.Fatalf("probeIntervalMin=43200 应当合法: %v", err)
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
				"probeIntervalMin": float64(5 + i%20),
				"probeWorkers":     float64(1 + i%8),
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
