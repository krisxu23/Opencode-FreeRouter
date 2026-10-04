// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 25 条状态侧测试,逐条对照计划任务 17 的 health 测试表。表里列出的
// TestNoteCooldownIsNotTriggeredByGlobalFailures 按计划在 pick_test.go 覆盖
// (那里的 TestNoGlobalFailureCodeCoolsTheExit)。
//
// row/sticky/busy 等类型非导出,测试与实现在同一个包内:字段级断言允许直接
// 摸 h.nodes/h.sticky/h.busy/h.ttft(手工把时间戳拨旧正是几张表要求的做法),
// 行字段则统一走 NodeSnapshot() 拷贝断言。
package health

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"freerouter/internal/nodeprobe"
)

func aliveRes(lat int64, ip, cc string) nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: lat, LatencyMin: lat, ExitIP: ip, ExitCountry: cc}
}

func deadRes() nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateDead, LatencyMS: 400, LatencyMin: 400}
}

func unknownRes() nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateUnknown, LatencyMS: -1, Incomplete: true}
}

// pinExitIpAt / pinStickyCacheAt / pinTtftAt / pinBusyAt:JS 测试靠 Date.now
// 注入,Go 没有时钟注入,同包测试直接把行内时间戳拨旧 —— 与「手工把 exitIpAt
// 改到 5 小时前」的计划原文一致。
func pinExitIpAt(h *Health, nodeKey string, at int64) {
	r := h.nodes[nodeKey]
	r.ExitIPAt = at
	h.nodes[nodeKey] = r
}

func TestMarkProbeKeepsTheLastExitIPWhenEchoFails(t *testing.T) {
	h := NewHealth("")
	// 带空白与小写国家码:量测值按 JS 一样 trim/上截 2 位大写(src/health.js:267-268)
	h.MarkProbe("n1", aliveRes(100, " 1.2.3.4 ", "us"))
	snap := h.NodeSnapshot()
	if snap["n1"].ExitIP != "1.2.3.4" || snap["n1"].ExitCountry != "US" {
		t.Fatalf("first probe: exitIp=%q exitCountry=%q, want 1.2.3.4/US", snap["n1"].ExitIP, snap["n1"].ExitCountry)
	}
	at := snap["n1"].ExitIPAt
	time.Sleep(5 * time.Millisecond) // Windows 时钟粒度粗,拉开毫秒差才有判别力

	// 分支 2:存活但 echo 全败(没探到 IP)→ 保留上一次的 IP 与国家,exitIpAt 原样不动
	h.MarkProbe("n1", aliveRes(110, "", ""))
	snap = h.NodeSnapshot()
	if snap["n1"].ExitIP != "1.2.3.4" {
		t.Fatalf("exitIp = %q, want kept 1.2.3.4", snap["n1"].ExitIP)
	}
	if snap["n1"].ExitCountry != "US" {
		t.Fatalf("exitCountry = %q, want kept US", snap["n1"].ExitCountry)
	}
	if snap["n1"].ExitIPAt != at {
		t.Fatalf("exitIpAt refreshed %d -> %d: 保留分支不得刷新时间戳,否则信任窗口形同虚设", at, snap["n1"].ExitIPAt)
	}
	if got := h.ExitIPOf("n1", time.Now().UnixMilli()); got != "1.2.3.4" {
		t.Fatalf("ExitIPOf = %q, want 1.2.3.4", got)
	}
}

func TestMarkProbeClearsExitIPOnDead(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(100, "1.2.3.4", "US"))
	h.MarkProbe("n1", deadRes())
	if got := h.ExitIPOf("n1", time.Now().UnixMilli()); got != "" {
		t.Fatalf("ExitIPOf = %q, want \"\": 不存在的出口谈不上出口 IP", got)
	}
	snap := h.NodeSnapshot()
	if snap["n1"].State != StateDead {
		t.Fatalf("state = %q, want dead", snap["n1"].State)
	}
	if snap["n1"].Tier != TierNone {
		t.Fatalf("tier = %q, want \"\": dead 无 tier", snap["n1"].Tier)
	}
	if snap["n1"].ExitIP != "" || snap["n1"].ExitIPAt != 0 {
		t.Fatalf("dead 行残留出口: ip=%q at=%d", snap["n1"].ExitIP, snap["n1"].ExitIPAt)
	}
}

func TestMarkProbeIgnoresUnknownVerdicts(t *testing.T) {
	h := NewHealth("")
	// unknown 是本轮不完整,不是判决:不该凭空造行
	h.MarkProbe("n1", unknownRes())
	if snap := h.NodeSnapshot(); len(snap) != 0 {
		t.Fatalf("unknown verdict created %d rows, want 0", len(snap))
	}
	h.MarkProbe("n1", aliveRes(50, "1.1.1.1", "US"))
	at := h.NodeSnapshot()["n1"].LastProbeAt
	time.Sleep(5 * time.Millisecond)
	h.MarkProbe("n1", unknownRes())
	snap := h.NodeSnapshot()
	if snap["n1"].LastProbeAt != at {
		t.Fatalf("lastProbeAt refreshed by unknown verdict: 不刷新 lastProbeAt,否则 unknown 冒充刚测过")
	}
	if snap["n1"].State != StateAlive {
		t.Fatalf("state = %q, want alive: unknown 不得改写 state", snap["n1"].State)
	}
}

func TestExitIPExpiresAfterFourHours(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(100, "1.2.3.4", "US"))
	now := time.Now().UnixMilli()
	if got := h.ExitIPOf("n1", now); got != "1.2.3.4" {
		t.Fatalf("fresh ExitIPOf = %q, want 1.2.3.4", got)
	}
	// 5 小时前量到的 → 信任窗口(4h)外,视为没有
	pinExitIpAt(h, "n1", now-5*60*60*1000)
	if got := h.ExitIPOf("n1", now); got != "" {
		t.Fatalf("stale ExitIPOf = %q, want \"\"", got)
	}
	// 3 小时仍窗口内
	pinExitIpAt(h, "n1", now-3*60*60*1000)
	if got := h.ExitIPOf("n1", now); got != "1.2.3.4" {
		t.Fatalf("in-window ExitIPOf = %q, want 1.2.3.4", got)
	}
	// 旧版落盘数据没有 exitIpAt:退回 lastProbeAt 判定,自然到期而不是永久新鲜
	r := h.nodes["n1"]
	r.ExitIPAt = 0
	r.LastProbeAt = now - 60*1000
	h.nodes["n1"] = r
	if got := h.ExitIPOf("n1", now); got != "1.2.3.4" {
		t.Fatalf("fallback-to-lastProbeAt ExitIPOf = %q, want 1.2.3.4", got)
	}
}

func TestMarkTierProbeLeavesOtherVerdictsAlone(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(100, "1.1.1.1", "US"))
	if got := h.TierOf("n1"); got != TierA {
		t.Fatalf("tier after coarse probe = %q, want A", got)
	}
	// 不确定的判决一律不动:throttled/transport/unknown 都不得降级
	for _, v := range []string{"throttled", "transport", "unknown"} {
		h.MarkTierProbe("n1", v)
		if got := h.TierOf("n1"); got != TierA {
			t.Fatalf("tier after %q = %q, want A (不确定的探针不得降级)", v, got)
		}
	}
	h.MarkTierProbe("n1", "available")
	if got := h.TierOf("n1"); got != TierB {
		t.Fatalf("tier after available = %q, want B", got)
	}
	// B 也不得被不确定判决降掉
	for _, v := range []string{"throttled", "transport", "unknown"} {
		h.MarkTierProbe("n1", v)
		if got := h.TierOf("n1"); got != TierB {
			t.Fatalf("tier after %q on B = %q, want B", v, got)
		}
	}
	h.MarkTierProbe("n1", "region-blocked")
	if got := h.TierOf("n1"); got != TierA {
		t.Fatalf("tier after region-blocked = %q, want A", got)
	}
	// 缺行:不动也不炸
	h.MarkTierProbe("ghost", "available")
	if got := h.TierOf("ghost"); got != TierNone {
		t.Fatalf("tier of missing node = %q, want \"\"", got)
	}
}

// TestPassStateMachineTwoTierThresholds 直接钉 1.3.0 双档状态机的门槛本身
// (app 层的 pass 测试只间接经过它)。规则:热区连续 2 轮失败降冷(streak 归零
// 重数),冷区连续 3 轮失败删除;任何一次通过清零连败;allowDelete=false
// (数据面信号)永不触发删除 —— 删除是冷区 pass 的专属判决。
func TestPassStateMachineTwoTierThresholds(t *testing.T) {
	h := NewHealth("")

	// 行不存在:返回 ""(没有可推进的状态机)。
	if got := h.MarkPassFail("ghost", true); got != "" {
		t.Fatalf("无行节点 MarkPassFail = %q, want \"\"", got)
	}

	h.MarkPassSuccess("n1")
	if h.HealthOf("n1") != StateAlive {
		t.Fatal("MarkPassSuccess 必须建 alive 行(理论不可达路径的保守兜底)")
	}
	// 热区:第 1 轮失败 —— 不降档。
	if got := h.MarkPassFail("n1", true); got != "" {
		t.Fatalf("热区 1 败 = %q, want \"\"(门槛是 2)", got)
	}
	// 热区:第 2 轮失败 —— 降冷区,streak 归零重数。
	if got := h.MarkPassFail("n1", true); got != "demote" {
		t.Fatalf("热区 2 败 = %q, want demote", got)
	}
	if snap := h.NodeSnapshot(); snap["n1"].State != StateDead || snap["n1"].Streak != 0 {
		t.Fatalf("降档后行 = %+v, want dead + streak 0", snap["n1"])
	}
	// 冷区:数据面信号(allowDelete=false)数满也不删。
	h.MarkPassFail("n1", false)
	h.MarkPassFail("n1", false)
	if got := h.MarkPassFail("n1", false); got != "" {
		t.Fatalf("allowDelete=false 数满 3 = %q, want \"\"(删除是冷区 pass 专属)", got)
	}
	// 中途一次通过:连败清零,3 次要从头数。
	h.MarkPassSuccess("n1")
	if snap := h.NodeSnapshot(); snap["n1"].State != StateAlive || snap["n1"].Streak != 0 {
		t.Fatalf("复活后行 = %+v, want alive + streak 0", snap["n1"])
	}
	// 冷区满 3(delete 后行被删,节点回到「无行」)。
	h.MarkProbe("n2", nodeprobe.ProbeResult{State: nodeprobe.StateDead, LatencyMS: 1})
	h.MarkPassFail("n2", true)
	h.MarkPassFail("n2", true)
	if got := h.MarkPassFail("n2", true); got != "delete" {
		t.Fatalf("冷区 3 败 = %q, want delete", got)
	}
	if _, ok := h.NodeSnapshot()["n2"]; ok {
		t.Fatal("delete 之后必须不留健康行(零记录,墓碑已退役)")
	}
}

func TestSurvivingCoarseProbeKeepsBTier(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(100, "1.1.1.1", "US"))
	h.MarkTierProbe("n1", "available")
	time.Sleep(5 * time.Millisecond)
	// liveness 重查不推翻 B:粗探只权威 A-vs-无(src/health.js:302-305)
	h.MarkProbe("n1", aliveRes(120, "1.1.1.1", "US"))
	if got := h.TierOf("n1"); got != TierB {
		t.Fatalf("tier after re-probe = %q, want B (不是退回 A)", got)
	}
}

// TestBTierIsInvalidatedWhenTheExitIPMoved 是上游层审计「过期 B 档钉死」的
// 镜像钉:B 的含义是「从这个出口出去,门控模型可用」。同一个 tag 的节点换了
// 出口 IP(动态 IP / 后端池漂移),这句话就不再成立;而 runTierPipeline 对
// 已证 B 的节点**永不重测**,所以不在这里撤销凭证,一个漂到墙外国家的节点
// 会永远挂着 B,被 gated 流量优选(rankLocked 给 B 桶位 −1),每轮真实对话
// 先付一次 REGION 失败再轮换 —— 坏凭证永不自愈。
//
// 判据只用**本轮量到**的 IP:echo 失败(没量到)不构成移动证据,凭证保留 ——
// 那条路径由 TestMarkProbeKeepsTheLastExitIPWhenEchoFails 钉住。
func TestBTierIsInvalidatedWhenTheExitIPMoved(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(100, "1.1.1.1", "US"))
	h.MarkTierProbe("n1", "available")
	if got := h.TierOf("n1"); got != TierB {
		t.Fatalf("setup: tier = %q, want B", got)
	}
	time.Sleep(5 * time.Millisecond)
	// 同一个 tag,出口 IP 变成 2.2.2.2 → 降回 A(下一轮 runTierPipeline 重验)。
	h.MarkProbe("n1", aliveRes(110, "2.2.2.2", "US"))
	if got := h.TierOf("n1"); got != TierA {
		t.Fatalf("tier after exit-IP move = %q, want A: 换出口即作废 B 凭证", got)
	}
	// 反面对照:本轮没量到 IP(沿用旧值)→ 不动凭证。
	h.MarkProbe("n1", aliveRes(120, "", ""))
	if got := h.TierOf("n1"); got != TierA {
		t.Fatalf("tier = %q, want A(沿用上一步的降档,不因 echo 失败升回 B)", got)
	}
}

func TestNoteQuotaOnAUnknownNodeCreatesARow(t *testing.T) {
	h := NewHealth("")
	h.NoteQuota("n1")
	if !h.NodeUsable("n1") {
		t.Fatal("NodeUsable = false: 不能因记配额把节点变成死")
	}
	if got := h.HealthOf("n1"); got != StateUnknown {
		t.Fatalf("HealthOf = %q, want unknown", got)
	}
	snap := h.NodeSnapshot()
	r, ok := snap["n1"]
	if !ok {
		t.Fatal("配额记号被静默丢掉:无行时必须建 unknown 行(src/health.js:390-397)")
	}
	if r.LastQuotaAt == 0 {
		t.Fatal("lastQuotaAt 未写入")
	}
}

func TestQuotaMarkDemotesButNeverExcludes(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("good", aliveRes(10, "1.1.1.1", "US"))
	pool := []PoolNode{{Tag: "good", Country: "US"}}
	for i := 0; i < 199; i++ {
		tag := fmt.Sprintf("filler-%03d", i)
		h.MarkProbe(tag, deadRes())
		pool = append(pool, PoolNode{Tag: tag, Country: "US"})
	}
	h.NoteQuota("good")
	p := h.Pick(PickRequest{Pool: pool})
	if p == nil {
		t.Fatal("Pick = nil: 记配额的出口被排除了,违反「降级但永不排除」")
	}
	if p.NodeKey != "good" {
		t.Fatalf("picked = %q, want good: 200 个候选里唯一的可用节点必须仍能被选中", p.NodeKey)
	}
	if len(p.Order) != 1 || p.Order[0].Tag != "good" {
		t.Fatalf("order = %+v, want [good]", p.Order)
	}
	if p.Order[0].Bucket != 1 {
		t.Fatalf("bucket = %d, want 1: 被记配额的出口 bucket +1(alive 0 + throttled 1)", p.Order[0].Bucket)
	}
	if !p.Order[0].Throttled {
		t.Fatal("throttled = false, want true")
	}
}

func TestQuotaSpreadsAcrossTheExitIP(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(20, "5.5.5.5", "US"))
	h.MarkProbe("b", aliveRes(10, "5.5.5.5", "US"))
	h.NoteQuota("a")
	p := h.Pick(PickRequest{Pool: []PoolNode{{Tag: "a", Country: "US"}, {Tag: "b", Country: "US"}}})
	if p == nil {
		t.Fatal("Pick = nil")
	}
	if len(p.Order) != 2 {
		t.Fatalf("order rows = %d, want 2", len(p.Order))
	}
	for _, r := range p.Order {
		if !r.Throttled {
			t.Fatalf("%s throttled = false: 同出口 IP 的兄弟节点必须吃到配额扩散", r.Tag)
		}
	}
	if p.NodeKey != "b" {
		t.Fatalf("picked = %q, want b: 同 bucket 下 cost(10) < a 的 20", p.NodeKey)
	}
}

func TestNoteQuotaNeverCoolsDown(t *testing.T) {
	h := NewHealth("")
	h.NoteQuota("n1")
	if !h.NodeUsable("n1") {
		t.Fatal("NodeUsable = false")
	}
	if snap := h.CooldownSnapshot(); len(snap) != 0 {
		t.Fatalf("cooldown snapshot = %d entries: 429 是上游的计费决定,不是节点失败", len(snap))
	}
}

func TestStickyTTLFollowsMeasuredCacheSavings(t *testing.T) {
	h := NewHealth("")
	// 基线:还没有任何 usage,保守 30min
	h.NoteSticky("s1", "n1", false)
	if ttl, ok := h.StickyTTL("s1"); !ok || ttl != stickyTTLBase {
		t.Fatalf("baseline ttl = %d ok=%v, want %d", ttl, ok, stickyTTLBase)
	}
	if _, ok := h.StickyTTL("ghost"); ok {
		t.Fatal("无 sticky 行必须返回不存在(JS 的 null),区别于「有行且等于基线」")
	}
	// 命中 ≥ 1024 → 放大到 2h
	if ttl, ok := h.NoteCacheRead("s1", 5000); !ok || ttl != cacheStickKeep {
		t.Fatalf("cacheRead=5000 ttl = %d, want %d", ttl, cacheStickKeep)
	}
	// 0 命中且 prompt 大到本该命中(≥4096)→ 缩到 5min
	h.NoteSticky("s2", "n1", false)
	h.NoteStickyUsage("s2", StickyUsage{InputTokens: 9000})
	if ttl, ok := h.StickyTTL("s2"); !ok || ttl != cacheCold {
		t.Fatalf("cacheRead=0 prompt=9000 ttl = %d, want %d", ttl, cacheCold)
	}
	// 0 命中但 prompt 很小 → 没有信息,保守基线
	h.NoteSticky("s3", "n1", false)
	h.NoteStickyUsage("s3", StickyUsage{InputTokens: 100})
	if ttl, ok := h.StickyTTL("s3"); !ok || ttl != stickyTTLBase {
		t.Fatalf("cacheRead=0 prompt=100 ttl = %d, want %d", ttl, stickyTTLBase)
	}
	// cacheRead 观测超过 10min(CACHE_FRESH)不再代表现在 → 回基线。
	// 注:公开 API(noteCacheRead)总是先刷新 cacheAt 再定档,该分支只能通过
	// 定档函数本身观察到 —— 与 JS 相同(noteCacheRead 内恒新鲜),这里直接
	// 对 stickyTTLOfLocked 断言。
	h.NoteSticky("s4", "n1", false)
	h.NoteCacheRead("s4", 5000)
	h.sticky["s4"].CacheAt = time.Now().UnixMilli() - 11*60*1000
	if ttl := h.stickyTTLOfLocked(h.sticky["s4"], time.Now().UnixMilli()); ttl != stickyTTLBase {
		t.Fatalf("stale cacheAt ttl = %d, want %d", ttl, stickyTTLBase)
	}
}

func TestStickyRotatesWithinTheSameExitIP(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.MarkProbe("b", aliveRes(20, "1.1.1.1", "US"))
	h.NoteSticky("s", "a", false)
	// 裁决:计划测试表写「删掉 A 的健康行 → 轮换到 B」,但 JS 的
	// exitForSession(src/health.js:763)只在 nodeUsable 为 false 时轮换,而行被
	// 删 = unknown = 可用(「unknown 即可用」是本模块的底线)。按修正案的裁决
	// 原则以 JS 为准:删行不轮换,一并钉住;真正触发同 IP 轮换的是原节点实测
	// 不可用(dead/冷却)。
	h.Forget("a")
	if got := h.ExitForSession("s"); got != "a" {
		t.Fatalf("ExitForSession after row deletion = %q, want a: unknown 可用,不轮换", got)
	}
	h.MarkProbe("a", deadRes()) // 原节点实测死亡 → 同 IP 内轮换
	at := h.sticky["s"].At
	time.Sleep(5 * time.Millisecond)
	alt := h.ExitForSession("s")
	if alt != "b" {
		t.Fatalf("ExitForSession = %q, want b: 同 IP 内必须轮换而不是作废", alt)
	}
	if h.sticky["s"].NodeKey != "b" {
		t.Fatalf("sticky node = %q, want b", h.sticky["s"].NodeKey)
	}
	if h.sticky["s"].At != at {
		t.Fatalf("at refreshed on rotation: 坏 IP 被反复轮换就能把会话无限续命")
	}
}

func TestStickyDropsWhenTheWholeExitDies(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.NoteSticky("s", "a", false)
	h.MarkProbe("a", deadRes()) // 同 IP 无替代
	if got := h.ExitForSession("s"); got != "" {
		t.Fatalf("ExitForSession = %q, want \"\": 整个出口死了必须作废", got)
	}
	if len(h.sticky) != 0 {
		t.Fatalf("sticky rows = %d, want 0: 作废时行要删掉", len(h.sticky))
	}
	if len(h.stickyFail) != 0 {
		t.Fatalf("stickyFail rows = %d, want 0", len(h.stickyFail))
	}
}

func TestStickyRotationPrefersLowerLatency(t *testing.T) {
	// M4a 的钉:同 IP 内有多个存活替补时,轮换必须按 latencyMin 择优 ——
	// 注释一直这么承诺(「排序与 pick 一致」),旧代码只比存活位,并列时取
	// 字典序第一个,于是一个 500ms 的替补会赢过 20ms 的。
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.MarkProbe("b", aliveRes(500, "1.1.1.1", "US"))
	h.MarkProbe("c", aliveRes(20, "1.1.1.1", "US"))
	h.NoteSticky("s", "a", false)
	h.MarkProbe("a", deadRes()) // 原节点死 → 同 IP 轮换
	if got := h.ExitForSession("s"); got != "c" {
		t.Fatalf("ExitForSession = %q, want c(20ms 替补): 同 IP 轮换必须按延迟择优", got)
	}
}

func TestStickyRotationPrefersAliveOverLatency(t *testing.T) {
	// 存活位优先于延迟:20ms 但 dead 的 b 不得赢过 500ms 且 alive 的 c。
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.MarkProbe("b", aliveRes(20, "1.1.1.1", "US"))
	h.MarkProbe("c", aliveRes(500, "1.1.1.1", "US"))
	h.NoteSticky("s", "a", false)
	h.MarkProbe("a", deadRes())
	h.MarkProbe("b", deadRes()) // 最快的替补也死了
	if got := h.ExitForSession("s"); got != "c" {
		t.Fatalf("ExitForSession = %q, want c: 存活优先,延迟只做同档比较", got)
	}
}

func TestStickyBurnedAfterTwoFailures(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.NoteSticky("s", "a", false)
	h.NoteStickyFailure("s")
	if got := h.ExitForSession("s"); got != "a" {
		t.Fatalf("one failure: ExitForSession = %q, want a(一次跪不烧)", got)
	}
	h.NoteStickyFailure("s")
	if got := h.ExitForSession("s"); got != "" {
		t.Fatalf("two failures: ExitForSession = %q, want \"\": 2 连跪烧掉粘性", got)
	}
	if h.StickyBurned("s") {
		t.Fatal("burned 行随作废一起清理,StickyBurned 应为 false")
	}
}

func TestStickyDoesNotInheritCacheSavingsOnIPChange(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.MarkProbe("b", aliveRes(20, "2.2.2.2", "US"))
	h.NoteSticky("s", "a", false)
	if ttl, ok := h.NoteCacheRead("s", 5000); !ok || ttl != cacheStickKeep {
		t.Fatalf("setup ttl = %d, want %d", ttl, cacheStickKeep)
	}
	h.NoteSticky("s", "b", false) // 换 IP:全新的一次决定
	if ttl, ok := h.StickyTTL("s"); !ok || ttl != stickyTTLBase {
		t.Fatalf("ttl after IP change = %d, want %d: 换 IP 不继承旧收益", ttl, stickyTTLBase)
	}
	if h.sticky["s"].CacheRead != 0 {
		t.Fatalf("cacheRead = %v, want 0: 旧出口的 cacheRead 不该继承给新出口", h.sticky["s"].CacheRead)
	}
	if h.sticky["s"].NodeKey != "b" {
		t.Fatalf("node = %q, want b", h.sticky["s"].NodeKey)
	}
}

func TestWithinTurnNeverShrinksSticky(t *testing.T) {
	h := NewHealth("")
	h.NoteSticky("s", "a", false)
	h.NoteStickyUsage("s", StickyUsage{InputTokens: 9000}) // 缩到 5min
	if ttl, _ := h.StickyTTL("s"); ttl != cacheCold {
		t.Fatalf("setup ttl = %d, want %d", ttl, cacheCold)
	}
	h.NoteSticky("s", "a", true) // 轮内工具结果回传
	if ttl, ok := h.StickyTTL("s"); !ok || ttl != stickyTTLBase {
		t.Fatalf("ttl after withinTurn = %d, want %d: 轮内不许低于基线", ttl, stickyTTLBase)
	}
	// 对照:非轮内的同情形不抬
	h.NoteSticky("s2", "a", false)
	h.NoteStickyUsage("s2", StickyUsage{InputTokens: 9000})
	h.NoteSticky("s2", "a", false)
	if ttl, _ := h.StickyTTL("s2"); ttl != cacheCold {
		t.Fatalf("non-withinTurn ttl = %d, want %d(保持 5min)", ttl, cacheCold)
	}
}

func TestCooldownBackoffIsCappedAtEightTimes(t *testing.T) {
	h := NewHealth("")
	for i := 0; i < 5; i++ {
		h.NoteCooldown("n1", 0)
	}
	c, ok := h.CooldownSnapshot()["n1"]
	if !ok {
		t.Fatal("n1 不在冷却表中")
	}
	if c.Failures != 5 {
		t.Fatalf("failures = %d, want 5", c.Failures)
	}
	if c.RemainingMS > 8*60*1000 {
		t.Fatalf("remainingMs = %d, want <= %d: 指数退避最多 8 倍基础时长", c.RemainingMS, 8*60*1000)
	}
	if c.RemainingMS <= 0 {
		t.Fatalf("remainingMs = %d, want > 0", c.RemainingMS)
	}
	// 上游给的 Retry-After 比退避更长时以它为准(那是权威的恢复时间)
	h.NoteCooldown("n2", 10*60*1000)
	c2, ok := h.CooldownSnapshot()["n2"]
	if !ok {
		t.Fatal("n2 不在冷却表中")
	}
	if c2.RemainingMS <= 8*60*1000 {
		t.Fatalf("remainingMs = %d, want > %d: retryAfter 更大时必须赢过退避", c2.RemainingMS, 8*60*1000)
	}
}

func TestExitBusySurvivesAnOverlongRequest(t *testing.T) {
	h := NewHealth("")
	if got := h.NoteExitBusy(""); got != "" {
		t.Fatalf("NoteExitBusy(\"\") = %q, want \"\"", got)
	}
	if got := h.NoteExitBusy("9.9.9.9"); got != "9.9.9.9" {
		t.Fatalf("NoteExitBusy 返回 %q, want 原样 ip 供调用方归账", got)
	}
	now := time.Now().UnixMilli()
	if got := h.exitBusyCountLocked("9.9.9.9", now); got != 1 {
		t.Fatalf("exitBusyCount = %d, want 1", got)
	}
	// 刚归还到 0 的条目必须留下(Resin Dec 注释):删掉再建会把并发 Inc
	// 的那一次丢掉,那个 IP 从此永远少算一条在途请求。
	h.ReleaseExitBusy("9.9.9.9")
	if _, ok := h.busy["9.9.9.9"]; !ok {
		t.Fatal("刚归零的条目被删除:并发 Inc 会丢计数")
	}
	// at 拨到 11 分钟前(> 10min 窗口,而上游单次超时只有 300s):只可能来自
	// 漏调的 Release。R9:过窗条目必须**删掉**,不能只原地归零 —— 这张表按
	// 出口 IP 建键,订阅轮换会让 IP 不断换代,原地归零就是无界增长。
	h.busy["9.9.9.9"].At = now - 11*60*1000
	if got := h.exitBusyCountLocked("9.9.9.9", now); got != 0 {
		t.Fatalf("stale exitBusyCount = %d, want 0", got)
	}
	if _, ok := h.busy["9.9.9.9"]; ok {
		t.Fatal("过窗条目没被回收:busy 表会随订阅轮换无界增长")
	}
}

func TestPruneBusyReclaimsOnlyStaleEntries(t *testing.T) {
	h := NewHealth("")
	now := time.Now().UnixMilli()
	// 三条:新鲜、窗内零值、过窗。
	h.NoteExitBusy("fresh.example")
	h.NoteExitBusy("zero.example")
	h.ReleaseExitBusy("zero.example")
	h.NoteExitBusy("stale.example")
	h.busy["stale.example"].At = now - exitBusyStale - 1000

	h.mu.Lock()
	h.pruneBusyLocked(now)
	h.mu.Unlock()

	if _, ok := h.busy["stale.example"]; ok {
		t.Fatal("过窗条目没被回收")
	}
	if _, ok := h.busy["fresh.example"]; !ok {
		t.Fatal("窗内条目被误删")
	}
	if _, ok := h.busy["zero.example"]; !ok {
		t.Fatal("窗内零值条目被误删:并发 Inc 会丢计数")
	}
}

func TestTtftMedianOverridesProbeLatency(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(50, "1.1.1.1", "US"))
	h.NoteTtft("n1", 300)
	h.NoteTtft("n1", 310)
	h.NoteTtft("n1", 305)
	p := h.Pick(PickRequest{Pool: []PoolNode{{Tag: "n1", Country: "US"}}})
	if p == nil {
		t.Fatal("Pick = nil")
	}
	if p.Order[0].Latency != 305 {
		t.Fatalf("order latency = %d, want 305(中位数), 不是探测的 50", p.Order[0].Latency)
	}
}

func TestTtftIsIgnoredWhenStale(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(50, "1.1.1.1", "US"))
	h.NoteTtft("n1", 300)
	h.NoteTtft("n1", 310)
	h.NoteTtft("n1", 305)
	h.ttft["n1"].At = time.Now().UnixMilli() - 7*60*60*1000 // 超 6h 新鲜窗口
	p := h.Pick(PickRequest{Pool: []PoolNode{{Tag: "n1", Country: "US"}}})
	if p == nil {
		t.Fatal("Pick = nil")
	}
	if p.Order[0].Latency != 50 {
		t.Fatalf("order latency = %d, want 50: 过期 TTFT 退回 latencyMin", p.Order[0].Latency)
	}
}

func TestTtftIsDroppedOnForget(t *testing.T) {
	h := NewHealth("")
	h.NoteTtft("n1", 100)
	h.NoteTtft("n1", 110)
	h.NoteTtft("n1", 120)
	if len(h.TtftSnapshot()) != 1 {
		t.Fatalf("ttft rows = %d, want 1", len(h.TtftSnapshot()))
	}
	// 节点名可能被别的订阅条目复用,旧出口的速度不能记在新出口头上
	h.Forget("n1")
	if len(h.TtftSnapshot()) != 0 {
		t.Fatalf("ttft rows after Forget = %d, want 0", len(h.TtftSnapshot()))
	}
	// 非正数忽略(JS 的 ms <= 0 return)
	h.NoteTtft("n2", 0)
	if len(h.TtftSnapshot()) != 0 {
		t.Fatalf("ms=0 created a row, want none")
	}
}

func TestForgetAndPruneStaleKeepLiveVerdicts(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("a", aliveRes(10, "1.1.1.1", "US"))
	h.MarkTierProbe("a", "available") // B
	h.MarkProbe("b", aliveRes(20, "2.2.2.2", "US"))
	if got := h.TierCounts(); got.B != 1 || got.Alive != 2 {
		t.Fatalf("tier counts before prune = %+v, want b=1 alive=2", got)
	}
	// 池子每轮 rebuild churn 上千 tag:删掉池外行绝不能动活着的判决 ——
	// 这正是当年 model x node 矩阵被清空的回归测试(src/health.js:22-27)
	h.PruneStale([]string{"a"})
	if got := h.TierCounts(); got.B != 1 {
		t.Fatalf("B count after prune = %d, want 1", got.B)
	}
	if got := h.HealthOf("a"); got != StateAlive {
		t.Fatalf("a HealthOf = %q, want alive", got)
	}
	if _, ok := h.NodeSnapshot()["b"]; ok {
		t.Fatal("池外 tag b 的行未被删除")
	}
	h.Forget("ghost") // 不存在:不炸
}

func TestPersistedFileRoundTrips(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-health.json")
	h := NewHealth(file)
	h.MarkProbe("n1", aliveRes(100, "1.1.1.1", "US"))
	h.MarkTierProbe("n1", "available")
	if err := h.Persist(); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	// 落盘形状 = {"nodes":{tag:row}},与 data/node-health.json 实测逐键一致,
	// JS 版能直接读回
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("top shape: %v", err)
	}
	if _, ok := top["nodes"]; !ok {
		t.Fatalf("missing \"nodes\" key, got %s", raw)
	}
	var nodes map[string]map[string]any
	if err := json.Unmarshal(top["nodes"], &nodes); err != nil {
		t.Fatalf("nodes shape: %v", err)
	}
	r1, ok := nodes["n1"]
	if !ok {
		t.Fatalf("missing row n1, got %s", top["nodes"])
	}
	if r1["state"] != "alive" || r1["tier"] != "B" || r1["latencyMs"] != float64(100) {
		t.Fatalf("row n1 = %v, want state=alive tier=B latencyMs=100", r1)
	}
	h2 := NewHealth(file)
	if got := h2.TierOf("n1"); got != TierB {
		t.Fatalf("tier after reload = %q, want B", got)
	}
	if got := h2.HealthOf("n1"); got != StateAlive {
		t.Fatalf("state after reload = %q, want alive", got)
	}
}

func TestLoadIgnoresUnknownStateRows(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-health.json")
	content := `{"nodes":{
		"alive1":{"state":"alive","latencyMs":100,"latencyMin":90,"tier":"A","lastProbeAt":123},
		"dead1":{"state":"dead","latencyMs":-1,"latencyMin":-1},
		"unk1":{"state":"unknown","latencyMs":-1,"lastQuotaAt":999}
	}}`
	if err := os.WriteFile(file, []byte(content), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	h := NewHealth(file)
	snap := h.NodeSnapshot()
	if len(snap) != 2 {
		t.Fatalf("loaded rows = %d, want 2: state=unknown 的行不进内存表", len(snap))
	}
	if _, ok := snap["unk1"]; ok {
		t.Fatal("unknown 行被载入:它没有可达性结论,只会让「上次没探完」冒充「这次不用探」")
	}
	if snap["alive1"].Tier != TierA {
		t.Fatalf("alive1 tier = %q, want A", snap["alive1"].Tier)
	}
	if got := h.TierCounts(); got.Alive != 1 {
		t.Fatalf("alive count = %d, want 1", got.Alive)
	}
}

func TestConcurrentPickAndNotesDoNotRace(t *testing.T) {
	h := NewHealth("")
	tags := []string{"n1", "n2", "n3", "n4"}
	ips := map[string]string{"n1": "1.1.1.1", "n2": "1.1.1.1", "n3": "2.2.2.2", "n4": "3.3.3.3"}
	pool := make([]PoolNode, 0, len(tags))
	for i, tag := range tags {
		lat := int64(10 * (i + 1))
		h.MarkProbe(tag, aliveRes(lat, ips[tag], "US"))
		pool = append(pool, PoolNode{Tag: tag, Country: "US"})
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				h.Pick(PickRequest{Pool: pool, StickyNode: tags[g%len(tags)]})
				h.NoteTtft(tags[i%len(tags)], int64(50+i%50))
				h.NoteQuota(tags[i%len(tags)])
				ip := h.NoteExitBusy("1.1.1.1")
				h.ReleaseExitBusy(ip)
			}
		}(g)
	}
	wg.Wait()
}

// TestPruneStaleReclaimsTtftAndCoolingRows 钉住 R17:池子每轮 churn 掉上千个 tag,
// 而 PruneStale 过去只删判决行 —— ttft 与 cool 两张按节点建键的表随历史 tag
// 单调增长,单行很小但没有上界。EnforceCap 淘汰的 tag 更是连 Forget 都不走,
// 所以残留是稳定的而不是暂时的。
func TestPruneStaleReclaimsTtftAndCoolingRows(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("gone", aliveRes(10, "1.1.1.1", "US"))
	h.MarkProbe("stay", aliveRes(20, "2.2.2.2", "US"))
	h.NoteTtft("gone", 50)
	h.NoteTtft("stay", 60)
	h.NoteCooldown("gone", 0)

	h.PruneStale([]string{"stay"})

	h.mu.RLock()
	ttftN, coolN := len(h.ttft), len(h.cool)
	_, stayKept := h.ttft["stay"]
	_, goneKept := h.ttft["gone"]
	h.mu.RUnlock()
	if goneKept {
		t.Fatal("离开池子的节点的 ttft 行还在:旧出口的速度会被记到复用同一个名字的线路上")
	}
	if ttftN != 1 || !stayKept {
		t.Fatalf("ttft 表 = %d 行, want 只剩 stay 一行", ttftN)
	}
	if coolN != 0 {
		t.Fatalf("cool 表剩 %d 行, want 0:冷却行同理", coolN)
	}
}

// TestStickyTableIsBoundedUnderSessionKeyChurn 钉住 R20 的上半:sticky 的键来自
// 客户端可控的 user/conversation,行只在同会话再次被读到时才作废。没有上限的话,
// 一个每请求换会话名的客户端就能让这张表(以及每次 Pick 在独占锁下的全表扫描)
// 无界增长。
func TestStickyTableIsBoundedUnderSessionKeyChurn(t *testing.T) {
	h := NewHealth("")
	h.MarkProbe("n1", aliveRes(10, "1.1.1.1", "US"))
	for i := 0; i < stickyCap*3+17; i++ {
		h.NoteSticky(fmt.Sprintf("flood-%d", i), "n1", false)
	}
	h.mu.RLock()
	n := len(h.sticky)
	h.mu.RUnlock()
	if n > stickyCap {
		t.Fatalf("sticky 表 %d 行, want ≤ %d(超上限按最久未用淘汰)", n, stickyCap)
	}
}

// TestPickSweepsExpiredStickyRows 是下半:过期行必须在 Pick 的那次全表扫描里
// 顺手删掉 —— 既然每次 Pick 都付了这个遍历,不回收就等于让独占锁里的扫描
// 成本永久等于历史会话数。
func TestPickSweepsExpiredStickyRows(t *testing.T) {
	h := NewHealth("")
	for i := 0; i < 4; i++ {
		tag := fmt.Sprintf("n%d", i)
		h.MarkProbe(tag, aliveRes(int64(10+i), fmt.Sprintf("10.0.0.%d", i+1), "US"))
	}
	h.NoteSticky("old", "n1", false)
	h.NoteSticky("live", "n2", false)
	h.NoteStickyFailure("old")
	h.mu.Lock()
	h.sticky["old"].At = 1
	h.sticky["old"].TTLMS = 1 // 远过期
	h.mu.Unlock()

	if p := h.Pick(PickRequest{Pool: []PoolNode{{Tag: "n3", Country: "US"}}}); p == nil {
		t.Fatal("Pick = nil")
	}
	h.mu.RLock()
	_, oldGone := h.sticky["old"]
	_, liveKept := h.sticky["live"]
	failKept := h.stickyFail["old"]
	h.mu.RUnlock()
	if oldGone {
		t.Fatal("过期 sticky 行还在:扫描不顺手删,表就只长不消")
	}
	if !liveKept {
		t.Fatal("未过期的行被误删了")
	}
	if failKept != 0 {
		t.Fatalf("会话连败计数 = %d, want 0:行删了计数也要跟着删,否则同样的键还漏着", failKept)
	}
}

// TestNodeViewMergeIntoMatchesJSONShape 钉住 O2 的替换前提:/api/status 过去对
// **每个节点**做一次 json.Marshal + json.Unmarshal(每 5 秒一轮,池上限 8000),
// 手写 map 的前提是逐键复刻 encoding/json 的输出 —— 包括每个 omitempty 的省略
// 条件(row 上面的注释记过哪三个字段恒出现、哪三个键在 JS 里是 undefined 因此
// 被丢掉)。判据因此不是「看起来对」,而是「手写结果与走一遍 JSON 的结果相等」。
func TestNodeViewMergeIntoMatchesJSONShape(t *testing.T) {
	views := []NodeView{
		{},
		{row: row{State: StateAlive, LatencyMS: 12, LatencyMin: 11, ExitIP: "1.1.1.1", ExitCountry: "US"}},
		{row: row{State: StateDead, LatencyMS: 400, LatencyMin: 400, ExitIPAt: 5, GeoMismatch: true,
			LastProbeAt: 6, Tier: TierB, LastQuotaAt: 7}, CoolingUntil: 8, CoolingFailures: 2},
	}
	for i, v := range views {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var viaJSON map[string]any
		if err := json.Unmarshal(b, &viaJSON); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		viaMerge := map[string]any{"tag": "t", "country": "US"}
		v.MergeInto(viaMerge)
		delete(viaMerge, "tag")
		delete(viaMerge, "country")
		// 比的是**序列化后**的字节,不是 map 本身:走一遍 JSON 的数值会退化成
		// float64,而手写保持 int64 —— Go 侧类型不同,喂给前端的 JSON 完全相同,
		// 而后者才是 /api/status 的契约。(用 reflect.DeepEqual 比 map 会把这条
		// 无关差异判成不等 —— 第一次跑就是这样抓出来的。)
		left, err := json.Marshal(viaJSON)
		if err != nil {
			t.Fatalf("marshal viaJSON: %v", err)
		}
		right, err := json.Marshal(viaMerge)
		if err != nil {
			t.Fatalf("marshal viaMerge: %v", err)
		}
		if string(left) != string(right) {
			t.Fatalf("case %d: 走 JSON = %s, 手写 = %s", i, left, right)
		}
	}
	// 顺手钉住一个更容易写错的点:omitempty 的键必须**缺席**,而不是带零值出现。
	fresh := map[string]any{}
	NodeView{}.MergeInto(fresh)
	for _, key := range []string{"exitIpAt", "lastProbeAt", "tier", "lastQuotaAt", "coolingUntil", "coolingFailures"} {
		if _, has := fresh[key]; has {
			t.Fatalf("%s 应当缺席(JS 侧是 undefined,JSON.stringify 会丢掉这个键),却出现 %v", key, fresh[key])
		}
	}
	for _, key := range []string{"state", "latencyMs", "latencyMin", "exitIp", "exitCountry", "geoMismatch"} {
		if _, has := fresh[key]; !has {
			t.Fatalf("%s 必须恒出现(即使为零值),面板按它取数", key)
		}
	}
}
