// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 19 条选路侧测试,逐条对照计划任务 17 的 pick 测试表。
//
// 夹具:8 个节点的池(n1..n8),n1/n2 同出口 IP 1.1.1.1(模拟聚簇),全部
// alive、latencyMin 依次 10..80ms,国家 US。tag 全部小写无国家词素,避免
// parse.CountryOf 意外识别出国家把 geoMismatch 点亮。
package health

import (
	"fmt"
	"testing"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/parse"
)

var pickIPs = map[string]string{
	"n1": "1.1.1.1", "n2": "1.1.1.1", // 聚簇
	"n3": "2.2.2.2", "n4": "3.3.3.3", "n5": "4.4.4.4", "n6": "5.5.5.5", "n7": "6.6.6.6", "n8": "7.7.7.7",
}

var allTags = []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}

func newPickFixture(t *testing.T) *Health {
	t.Helper()
	h := NewHealth("")
	for i, tag := range allTags {
		lat := int64(10 * (i + 1))
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: lat, LatencyMin: lat, ExitIP: pickIPs[tag], ExitCountry: "US"})
	}
	return h
}

func pickPool(tags ...string) []PoolNode {
	out := make([]PoolNode, 0, len(tags))
	for _, tag := range tags {
		out = append(out, PoolNode{Tag: tag, Country: "US"})
	}
	return out
}

func TestPickPrefersTheLowestBucketThenCost(t *testing.T) {
	h := newPickFixture(t)
	p := h.Pick(PickRequest{Pool: pickPool("n1", "n2")})
	if p == nil || p.NodeKey != "n1" {
		t.Fatalf("picked = %v, want n1: 同 bucket 下 latencyMin 小者胜", p)
	}
	if len(p.Order) != 2 || p.Order[0].Tag != "n1" {
		t.Fatalf("order = %+v, want [n1 n2]", p.Order)
	}
	if p.Order[0].Cost != 10 {
		t.Fatalf("cost = %d, want 10 (= latency × (load+1) = 10×1)", p.Order[0].Cost)
	}
}

func TestPickDemotesBannedQuotaButStillReturnsIt(t *testing.T) {
	h := newPickFixture(t)
	for _, tag := range allTags {
		h.NoteQuota(tag)
	}
	p := h.Pick(PickRequest{Pool: pickPool(allTags...)})
	if p == nil {
		t.Fatal("Pick = nil: 全部候选被记配额也必须仍返回一个(降级不排除)")
	}
	if len(p.Order) != 8 {
		t.Fatalf("order rows = %d, want 8", len(p.Order))
	}
	for _, r := range p.Order {
		if !r.Throttled {
			t.Fatalf("%s throttled = false", r.Tag)
		}
		if r.Bucket != 1 {
			t.Fatalf("%s bucket = %d, want 1(alive 0 + throttled 1)", r.Tag, r.Bucket)
		}
	}
	if p.NodeKey != "n1" {
		t.Fatalf("picked = %q, want n1: 同 bucket 下 cost 最小者", p.NodeKey)
	}
}

func TestPickSkipsNodesInCooling(t *testing.T) {
	h := newPickFixture(t)
	h.NoteCooldown("n1", 0)
	if p := h.Pick(PickRequest{Pool: pickPool("n1")}); p != nil {
		t.Fatalf("picked = %v, want nil: 冷却中的节点不可用", p)
	}
	// 冷却过期(NodeUsable 的懒清理)后恢复可选
	h.cool["n1"].Until = time.Now().UnixMilli() - 1
	if p := h.Pick(PickRequest{Pool: pickPool("n1")}); p == nil || p.NodeKey != "n1" {
		t.Fatalf("picked after expiry = %v, want n1", p)
	}
}

func TestPickSpreadsSessionsOffClusteredIPs(t *testing.T) {
	h := newPickFixture(t)
	// 3 个会话钉在 1.1.1.1(s1/s2 都在 n1 上也各算一个会话)→ 该 IP load 3
	h.NoteSticky("s1", "n1", false)
	h.NoteSticky("s2", "n1", false)
	h.NoteSticky("s3", "n2", false)
	p := h.Pick(PickRequest{Pool: pickPool(allTags...)})
	if p == nil || p.NodeKey != "n3" {
		t.Fatalf("picked = %v, want n3: shared +1(还加 saturated +1)把聚簇 IP 压到空闲 IP 之后", p)
	}
	if p.Order[0].Tag != "n3" || p.Order[0].Bucket != 0 {
		t.Fatalf("order[0] = %s bucket %d, want n3 bucket 0", p.Order[0].Tag, p.Order[0].Bucket)
	}
	for _, r := range p.Order {
		if r.Tag == "n1" && r.Bucket != 2 {
			t.Fatalf("n1 bucket = %d, want 2(load 3 > 0 → shared 1, > exitSoftCap 2 → saturated 1)", r.Bucket)
		}
	}
}

func TestPickPutsSaturatedExitsLast(t *testing.T) {
	h := newPickFixture(t)
	for i := 0; i < 3; i++ {
		h.NoteExitBusy("1.1.1.1") // 3 条在途 > exitSoftCap 2
	}
	p := h.Pick(PickRequest{Pool: pickPool(allTags...)})
	if p == nil || p.NodeKey != "n3" {
		t.Fatalf("picked = %v, want n3: 满载出口排最后但绝不排除", p)
	}
	found := false
	for _, r := range p.Order {
		if r.Tag == "n1" {
			found = true
			if r.Bucket != 2 || r.Load != 3 {
				t.Fatalf("n1 bucket=%d load=%d, want bucket 2 load 3", r.Bucket, r.Load)
			}
		}
	}
	if !found {
		t.Fatal("n1 不在候选快照里:满载是排队不是除名")
	}
}

func TestPickCostsLatencyTimesLoad(t *testing.T) {
	h := newPickFixture(t)
	h.NoteSticky("s1", "n2", false) // n2(20ms)所在 IP load 1
	p := h.Pick(PickRequest{Pool: pickPool("n2", "n3")})
	if p == nil || p.NodeKey != "n3" {
		t.Fatalf("picked = %v, want n3: 30×1 = 30 < 20×2 = 40,乘法而非先比负载(src/health.js:995-998)", p)
	}
}

// TestPickNegativeLatencySortsLast 是引擎审计 M2 的钉:latencyMin/latencyMS
// 的 -1 是「没有任何延迟数据」的内部哨兵,不是可以参战的真实数字。过去
// cost=(load+1)×(-1) 是**负数**,于是「一个测量都没做过」的节点在同 bucket
// 里稳赢所有有真实延迟的节点 —— 每回合都被首选。修复把 ≤0 归回哨兵语义
// (排最后)。
//
// 造一个 alive 但延迟字段是 -1 的节点:MarkProbe 对 alive 且 res.LatencyMS=0
// 的行,会按 (src/health.js:284/:290) 的 ?? 回退把 latMS/latMin 都写成 -1
// —— 正是「节点被判活但这一轮没拿到任何延迟样本」的形状(noteQuota 的
// synthetic 行走同一 -1 通道)。
func TestPickNegativeLatencySortsLast(t *testing.T) {
	h := NewHealth("")
	// nBad:alive,延迟字段 -1(两个 IP 分开,load 恒 0,排除 sticky 干扰)。
	h.MarkProbe("nBad", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 0, LatencyMin: 0, ExitIP: "9.9.9.9", ExitCountry: "US"})
	// nGood:alive,真实 10ms。
	h.MarkProbe("nGood", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 10, LatencyMin: 10, ExitIP: "8.8.8.8", ExitCountry: "US"})
	pool := []PoolNode{{Tag: "nBad", Country: "US"}, {Tag: "nGood", Country: "US"}}
	p := h.Pick(PickRequest{Pool: pool})
	if p == nil {
		t.Fatal("Pick = nil")
	}
	if p.NodeKey != "nGood" {
		t.Fatalf("picked = %q, want nGood: 无延迟数据(-1 哨兵)的节点不得赢过 10ms 的真实节点(M2 负 cost 复发)", p.NodeKey)
	}
	// 排序快照里 nBad 必须压底(哨兵 cost),且**仍在候选里**(没有数据 ≠ 除名)。
	if len(p.Order) != 2 || p.Order[len(p.Order)-1].Tag != "nBad" {
		t.Fatalf("order = %+v, want nBad 排最后但保留", p.Order)
	}
	// 哨兵在落盘行里被有意压成 0(见 orderRowOf:MaxInt64 是面板看不懂的魔数),
	// 所以这里判的是**次序**而不是 cost 量级:cost 不得为负(旧病根是
	// (load+1)×(-1) = -1 稳赢全桶),而 nBad 必须排在有真实数据的节点之后。
	for _, r := range p.Order {
		if r.Cost < 0 {
			t.Fatalf("%s cost = %d,不得为负(M2 的病根就是这个负 cost 稳赢全桶)", r.Tag, r.Cost)
		}
	}
}

func TestRestrictedOnlyConsidersBTier(t *testing.T) {
	h := newPickFixture(t) // 全是 A
	if p := h.Pick(PickRequest{Restricted: true, Pool: pickPool(allTags...)}); p != nil {
		t.Fatalf("picked = %v, want nil: 受限模型只认 B 类", p)
	}
	h.MarkTierProbe("n8", "available")
	p := h.Pick(PickRequest{Restricted: true, Pool: pickPool(allTags...)})
	if p == nil || p.NodeKey != "n8" {
		t.Fatalf("picked = %v, want n8(唯一的 B)", p)
	}
	if len(p.Order) != 1 {
		t.Fatalf("order rows = %d, want 1: 非 B 出口被 rank 过滤", len(p.Order))
	}
}

func TestRestrictedIsComputedFromTheModelName(t *testing.T) {
	if !IsRestrictedModel("muse-spark-1.3-contributor-free") {
		t.Fatal("muse-spark 前缀应为受限")
	}
	if !IsRestrictedModel("muse-spark-1.3-pro") {
		t.Fatal("第二个 muse-spark 行同为受限")
	}
	if IsRestrictedModel("big-pickle") {
		t.Fatal("big-pickle 不是受限模型")
	}
	if IsRestrictedModel("") {
		t.Fatal("空串不是受限模型")
	}
	if IsRestrictedModel("xmuse-spark") {
		t.Fatal("前缀匹配,不是子串匹配")
	}
}

func TestUnavailableEverywhereOnlyForGatedModels(t *testing.T) {
	h := newPickFixture(t) // 无 B
	if !h.UnavailableEverywhere("muse-spark-1.3-contributor-free") {
		t.Fatal("gated 模型在无 B 出口时应不可服务")
	}
	// 非受限模型必须恒 false:忘了传 model 会把整个目录藏起来(src/health.js:551-553)
	if h.UnavailableEverywhere("big-pickle") {
		t.Fatal("非受限模型不得被判 UnavailableEverywhere")
	}
	h.MarkTierProbe("n1", "available")
	if h.UnavailableEverywhere("muse-spark-1.3-contributor-free") {
		t.Fatal("有了 alive B 之后 gated 模型应可服务")
	}
}

func TestCountriesAreGroupsNotCodes(t *testing.T) {
	if got := parse.BucketOf("NL"); got != "EU" {
		t.Fatalf("BucketOf(NL) = %q, want EU: countries 是分组不是国家码", got)
	}
	h := newPickFixture(t)
	h.MarkProbe("peer07", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 500, LatencyMin: 500, ExitIP: "8.8.8.8", ExitCountry: "NL"})
	pool := append(pickPool(allTags...), PoolNode{Tag: "peer07", Country: "NL"})
	p := h.Pick(PickRequest{Countries: []string{"EU"}, Pool: pool})
	if p == nil || p.NodeKey != "peer07" {
		t.Fatalf("picked = %v, want peer07: NL 归入 EU 分组后被选中", p)
	}
	if p.Country != "NL" {
		t.Fatalf("country = %q, want NL", p.Country)
	}
}

func TestCountryOrderIsHonoured(t *testing.T) {
	h := newPickFixture(t)
	h.MarkProbe("peer08", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 800, LatencyMin: 800, ExitIP: "9.9.9.9", ExitCountry: "JP"})
	pool := append(pickPool(allTags...), PoolNode{Tag: "peer08", Country: "JP"})
	p := h.Pick(PickRequest{Countries: []string{"JP", "US"}, Pool: pool})
	if p == nil || p.NodeKey != "peer08" {
		t.Fatalf("picked = %v, want peer08: 即便 US(n1,10ms)更快,JP 组在回退序里在前", p)
	}
}

func TestPoolFallbackBeatsFailing(t *testing.T) {
	h := newPickFixture(t)
	h.MarkProbe("peer07", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 500, LatencyMin: 500, ExitIP: "8.8.8.8", ExitCountry: "NL"})
	for _, tag := range allTags {
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateDead, LatencyMS: 400})
	}
	p := h.Pick(PickRequest{Countries: []string{"US"}, Pool: append(pickPool(allTags...), PoolNode{Tag: "peer07", Country: "NL"})})
	if p == nil || p.NodeKey != "peer07" {
		t.Fatalf("picked = %v, want peer07: 指定国家全灭时落到全池兜底 —— 错国家好过没答案", p)
	}
}

func TestStickyHitReturnsASingleRowOrder(t *testing.T) {
	h := newPickFixture(t)
	p := h.Pick(PickRequest{Pool: pickPool(allTags...), StickyNode: "n3"})
	if p == nil || p.NodeKey != "n3" {
		t.Fatalf("picked = %v, want n3", p)
	}
	if len(p.Order) != 1 {
		t.Fatalf("order rows = %d, want 1: sticky 命中时 Order 只有它一行", len(p.Order))
	}
	if !p.Order[0].Sticky || p.Order[0].Tag != "n3" {
		t.Fatalf("order[0] = %+v, want n3 且 sticky=true", p.Order[0])
	}
	if p.ExitIP != pickIPs["n3"] {
		t.Fatalf("exitIp = %q, want %q", p.ExitIP, pickIPs["n3"])
	}
}

func TestStickyHitIsSkippedWhenThePoolLostIt(t *testing.T) {
	h := newPickFixture(t)
	p := h.Pick(PickRequest{Pool: pickPool(allTags...), StickyNode: "nX"})
	if p == nil || p.NodeKey != "n1" {
		t.Fatalf("picked = %v, want n1: StickyNode 不在池里就走正常选路", p)
	}
	if len(p.Order) != 8 {
		t.Fatalf("order rows = %d, want 8", len(p.Order))
	}
	for _, r := range p.Order {
		if r.Sticky {
			t.Fatalf("%s sticky = true: 没有粘性命中不得标记", r.Tag)
		}
	}
}

func TestOrderIsCappedAtEightRows(t *testing.T) {
	h := NewHealth("")
	pool := make([]PoolNode, 0, 50)
	for i := 0; i < 50; i++ {
		tag := fmt.Sprintf("m%02d", i)
		lat := int64(100 + i)
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: lat, LatencyMin: lat, ExitIP: fmt.Sprintf("10.0.0.%d", i+1)})
		pool = append(pool, PoolNode{Tag: tag, Country: "US"})
	}
	p := h.Pick(PickRequest{Pool: pool})
	if p == nil {
		t.Fatal("Pick = nil")
	}
	if len(p.Order) != 8 {
		t.Fatalf("order rows = %d, want 8: 整池候选只留前 8", len(p.Order))
	}
	for i := 1; i < len(p.Order); i++ {
		if p.Order[i-1].Bucket > p.Order[i].Bucket {
			t.Fatalf("order not sorted by bucket at %d", i)
		}
		if p.Order[i-1].Bucket == p.Order[i].Bucket && p.Order[i-1].Cost > p.Order[i].Cost {
			t.Fatalf("order not sorted by cost within bucket at %d", i)
		}
	}
	if p.Order[0].Tag != "m00" || p.NodeKey != "m00" {
		t.Fatalf("picked %q, want m00(真实排序的第一名)", p.NodeKey)
	}
}

func TestOrderSentinelsBecomeZeroNotMegaIntegers(t *testing.T) {
	h := newPickFixture(t)
	pool := append(pickPool("n1"), PoolNode{Tag: "ghost", Country: "US"}) // ghost 从未探测
	p := h.Pick(PickRequest{Pool: pool})
	if p == nil {
		t.Fatal("Pick = nil: unknown 可用,无数据不是不可用")
	}
	found := false
	for i, r := range p.Order {
		if r.Tag == "ghost" {
			found = true
			if i != len(p.Order)-1 {
				t.Fatalf("ghost at %d, want last: 无延迟数据的哨兵排最后", i)
			}
			if r.Latency != 0 {
				t.Fatalf("latency = %d, want 0: JS 的 MAX_SAFE_INTEGER 哨兵落盘写 null,Go 写 0,不得把 1<<53 级别的魔数写进面板", r.Latency)
			}
			if r.Cost != 0 {
				t.Fatalf("cost = %d, want 0(哨兵同样不落盘)", r.Cost)
			}
		}
	}
	if !found {
		t.Fatal("ghost 不在候选快照里")
	}
}

func TestPickNeverReturnsDirect(t *testing.T) {
	h := newPickFixture(t)
	for _, tag := range allTags {
		h.NoteCooldown(tag, 0)
	}
	if p := h.Pick(PickRequest{Pool: pickPool(allTags...)}); p != nil {
		t.Fatalf("picked = %v, want nil: 池全不可用时 Pick 的 nil 就是「没有可用出口」", p)
	}
	// 引擎不得回落到 DirectTag —— 直连证明不了出口可用,本包不提供任何直连路径
}

func TestPickIsDeterministicForEqualCandidates(t *testing.T) {
	newTwo := func() *Health {
		h := NewHealth("")
		h.MarkProbe("pa", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 50, LatencyMin: 50, ExitIP: "1.1.1.1"})
		h.MarkProbe("pb", nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 50, LatencyMin: 50, ExitIP: "2.2.2.2"})
		return h
	}
	h := newTwo()
	pool := pickPool("pa", "pb")
	first := h.Pick(PickRequest{Pool: pool})
	if first == nil {
		t.Fatal("Pick = nil")
	}
	for i := 0; i < 10; i++ {
		p := h.Pick(PickRequest{Pool: pool})
		if p.NodeKey != first.NodeKey {
			t.Fatalf("pick %d = %q, want %q: 相同输入必须同答案(排序必须稳定)", i, p.NodeKey, first.NodeKey)
		}
	}
	// 同 bucket 同 cost 时按 tag 定序(计划新增的第三排序键),跨实例也一致
	if first.NodeKey != "pa" {
		t.Fatalf("picked = %q, want pa(tag 字典序)", first.NodeKey)
	}
	if again := newTwo().Pick(PickRequest{Pool: pool}); again.NodeKey != "pa" {
		t.Fatalf("fresh instance picked = %q, want pa", again.NodeKey)
	}
}

func TestNoGlobalFailureCodeCoolsTheExit(t *testing.T) {
	// src/engine.js:47 的 COOLDOWN_ON 只有 transport/timeout:region/quota/
	// empty/server 是全局性问题(指纹回归的 403、模型侧空响应、上游整体限流),
	// 给它们冷却会把整个池子冻住。这里把那份契约钉成数据;任务 18 的 engine
	// 按 Failure.Retryable 决定冷却时只有 COOLDOWN_ON 里的码走 NoteCooldown,
	// 那边的测试再断言一次调用侧。
	coolingOn := map[string]bool{
		check.CodeTransport: true,
		check.CodeTimeout:   true,
	}
	for _, code := range []string{check.CodeRegion, check.CodeQuota, check.CodeEmpty, check.CodeServer} {
		if coolingOn[code] {
			t.Errorf("%s 不得触发冷却:全局性问题冻结整池比重试一个坏节点严重得多", code)
		}
	}
	if !coolingOn[check.CodeTransport] || !coolingOn[check.CodeTimeout] {
		t.Fatal("transport/timeout 是节点自身可归因的失败,必须冷却")
	}
	// 健康包自己能证明的一半:配额码的入口 NoteQuota 不进 cooling
	h := newPickFixture(t)
	h.NoteQuota("n1")
	if snap := h.CooldownSnapshot(); len(snap) != 0 {
		t.Fatalf("cooldown entries after NoteQuota = %d, want 0", len(snap))
	}
	if !h.NodeUsable("n1") {
		t.Fatal("NoteQuota 不得让节点不可用")
	}
}
