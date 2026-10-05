// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 八审 M8+L11:开场订阅轮(openingSubscription)与重建入口(Rebuild 排队分支)。
package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/gate"
	"freerouter/internal/health"
	"freerouter/internal/logger"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/parse"
	"freerouter/internal/registry"
	"freerouter/internal/sbx"
)

// newOpeningParts 组装一个只跑 openingSubscription/subExits 的 Parts:真实 sbx
// 宿主装载 2 个 shadowsocks 节点,健康表留空。
func newOpeningParts(t *testing.T) *Parts {
	t.Helper()
	logger.Init("")
	dir := t.TempDir()
	// 节点指向 TEST-NET-3(203.0.113.1):公网字面量(过 IsUnroutableServer 的
	// 入池闸)但保证黑洞 —— 即便被误借为出口,SYN 也只会悬死,不触真实主机。
	// 不能用 127.0.0.1:sbx 的最后一道闸会把 loopback 节点整个拒载(「地址
	// 不可路由」),宿主里一个拨号器都没有,测试就测了个空。
	hangNode := func(tag, pw string) parse.Outbound {
		return parse.Outbound{Tag: tag, Type: "shadowsocks", Server: "203.0.113.1",
			ServerPort: 1, Method: "aes-128-gcm", Password: pw}
	}
	outs := []parse.Outbound{hangNode("n0", "pw-0"), hangNode("n1", "pw-1")}
	h := sbx.NewHost(func(level, msg string) { t.Logf("sbx %s: %s", level, msg) })
	if err := h.Start(context.Background(), outs); err != nil {
		t.Fatalf("host start: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	reg := registry.NewRegistry(filepath.Join(dir, "node-registry.json"))
	if err := reg.Load(); err != nil {
		t.Fatalf("registry load: %v", err)
	}
	reg.Merge(outs)

	s := defaultSettings()
	s.ProbeWorkers = 8
	s.Countries = []string{"OTHER"}
	return &Parts{
		Root:       dir,
		Settings:   &s,
		Host:       h,
		Registry:   reg,
		Health:     health.NewHealth(filepath.Join(dir, "node-health.json")),
		Prober:     &fakeProber{},
		tierGate:   gate.New(tierGapMS),
		catalog:    &catalogBox{list: catalog.Static()},
		hotNudge:   make(chan struct{}, 1),
		coldNudge:  make(chan struct{}, 1),
		firstNudge: make(chan struct{}, 1),
	}
}

// TestSubExitsOnlyBorrowsAliveNodes 钉八审 M8 的「只借活出口」:健康表为空 →
// subExits 必须为空;一活一死 → 只借活的。旧开场形状用注册表全量拨号出口,
// 死出口在订阅复拉里逐一烧满 20s attemptTimeout —— 正是 subExits 注释里写明
// 不做的形状。
func TestSubExitsOnlyBorrowsAliveNodes(t *testing.T) {
	p := newOpeningParts(t)
	if got := p.subExits(); len(got) != 0 {
		t.Fatalf("健康表为空 → subExits 必须为空(死出口不得进订阅复拉), got %d 个", len(got))
	}
	p.Health.MarkProbe("n0", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 10, LatencyMin: 10})
	p.Health.MarkProbe("n1", nodeprobe.ProbeResult{State: nodeprobe.StateDead})
	got := p.subExits()
	if len(got) != 1 || got[0].Name != "n0" || got[0].Dial == nil {
		t.Fatalf("subExits = %+v, want 只含 n0 且带拨号器", got)
	}
}

// TestOpeningSubscriptionUsesFetchBudget 钉八审 M8 的「总闸必须接着」:拉取源
// 是接受连接但永不回应的本地 HTTP 服务器,每个 fetchOne 尝试要烧满 20s
// attemptTimeout。把 subFetchBudget 收缩到 300ms 后,整轮必须在总闸处断——
// 总闸若被旁路(裸 ctx 直传 sub.Fetch),第一轮就要 20s,8s 观察窗必红。
func TestOpeningSubscriptionUsesFetchBudget(t *testing.T) {
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	}))
	t.Cleanup(hang.Close)
	old := subFetchBudget
	subFetchBudget = 300 * time.Millisecond
	t.Cleanup(func() { subFetchBudget = old })

	p := newOpeningParts(t)
	s := *p.Settings
	s.SubURLs = []string{hang.URL}
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.openingSubscription(context.Background(), s)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("开场订阅 8s 未完成:总闸没有接在 sub.Fetch 上(八审 M8)")
	}
	p.rebuildStateMu.Lock()
	defer p.rebuildStateMu.Unlock()
	if p.lastRebuildOK || p.lastRebuildErr == "" {
		t.Fatalf("总闸超时未记为失败: ok %v err %q", p.lastRebuildOK, p.lastRebuildErr)
	}
	if got := p.Registry.Len(); got != 2 {
		t.Fatalf("registry = %d, want 2(失败轮不得合并也不得删节点)", got)
	}
}

// TestOpeningSubscriptionMergesAndPrunes 钉 M8 重构的契约面:开场轮仍按订阅
// 成员资格整理池子 —— 合并源里的新节点、删净不在源里的旧节点、结果记进
// lastCheck/lastRebuild(6.5 步存在的全部理由)。
func TestOpeningSubscriptionMergesAndPrunes(t *testing.T) {
	source := subAndCatalogServer(t, vlink("nn1"), "{}", http.StatusOK)
	p := newOpeningParts(t) // n0/n1 在池,健康表无行
	s := *p.Settings
	s.SubURLs = []string{source}

	p.openingSubscription(context.Background(), s)

	p.rebuildStateMu.Lock()
	ok, added, removed, dropped := p.lastRebuildOK, p.lastAdded, p.lastRemoved, p.lastDropped
	p.rebuildStateMu.Unlock()
	if !ok || added != 1 || removed != 2 || dropped != 0 {
		t.Fatalf("记账 = ok %v added %d removed %d dropped %d, want true/1/2/0", ok, added, removed, dropped)
	}
	if got := p.Registry.Len(); got != 1 {
		t.Fatalf("registry = %d, want 1(不在源里的 n0/n1 删净)", got)
	}
	if !p.Registry.Has("nn1") {
		t.Fatal("源里的 nn1 没有被合并进池子")
	}
}

// TestRebuildQueuedEntryHonestUnderShutdown 钉八审 L11:关停窗口里撞上「已有
// 一轮在跑」的调用方拿到的必须是普通错误 —— 面板/托盘把 errRebuildQueued 哨兵
// 按「已受理,等补跑」处理,而 ctx 已取消意味着排队的一手注定被循环里的关停
// 检查丢弃,哨兵承诺永不兑现。活 ctx 的排队语义不变。
func TestRebuildQueuedEntryHonestUnderShutdown(t *testing.T) {
	p := newOpeningParts(t)
	p.rebuildMu.Lock()
	p.rebuilding = true // 白盒:模拟另一轮重建在跑
	p.rebuildMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.Rebuild(ctx)
	if err == nil || strings.Contains(err.Error(), errRebuildQueued.Error()) {
		t.Fatalf("关停窗口必须回普通错误,拿到: %v", err)
	}
	if !strings.Contains(err.Error(), "关停中") {
		t.Fatalf("文案缺「关停中」: %v", err)
	}

	p2 := newOpeningParts(t)
	p2.rebuildMu.Lock()
	p2.rebuilding = true
	p2.rebuildMu.Unlock()
	if err := p2.Rebuild(context.Background()); err == nil {
		t.Fatal("活 ctx 的排队必须返回哨兵,拿到 nil(谎报成功)")
	}
}
