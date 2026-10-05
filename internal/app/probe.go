// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 持续健康监测(1.3.0 定稿):节点按健康状态分两档,各自独立成 pass 循环。
//
//	热区(alive)  —— 全部并发复检(单发上游探针),整轮完成后等
//	                hotIntervalSec(默认 60s)再下一轮;失败计 streak,
//	                连续 2 轮失败降冷区。
//	冷区(dead)   —— 同样并发扫一遍,完成后等 coldIntervalSec(默认 5min);
//	                任意通过立即升回热区;连续 3 轮失败 → 彻底删除
//	                (注册表条目 + 健康行,零记录,墓碑已退役)。
//	首探          —— 新入池、还没有健康行的节点走全量三段(liveness +
//	                上游 + echo)+ B 档认证;完成即进入热/冷循环。
//
// 间隔从「上一轮完整结束后」起算(用户裁定):一轮耗时多久都无所谓,
// 绝不堆积。热区与冷区可并发运行,worker 预算对半封顶(96/32,合计不超过
// 现行 128 并发的实测峰值)。
//
// 两个安全闸门贯穿所有 pass(判据分档 —— 1.3.2 定稿):
//   - 热区事故地板:复检**幸存者**时通过率跌破地板(≥20 样本且 <10% 通过)
//     说明通道坏了 —— 上一轮刚证活的节点不该成批同时死光。整轮丢弃并冻结
//     冷区删除(lastHotOK),直到下一个健康的热区 pass 解锁。
//   - 首探零通关闸:首探测的是来路不明的新节点,通过率天然个位数(2026-10-04
//     现场:1010 个新节点 5%–6% 通过是正常形状),地板判据会把每一轮首探都
//     误杀 —— 热区只出不进缩到十几个。首探只在「零通关 **且** 直连也不可达」
//     时丢弃;有通关或零通关但直连正常都照常应用。
//   - 取消丢弃:pass 中途 ctx 取消(关停)→ 整轮结果丢弃(与 C2 同理)。
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/errors"
	"freerouter/internal/health"
	"freerouter/internal/httpclient"
	"freerouter/internal/logger"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/upstream"
)

const (
	tierWorkers = 8  // src/index.js:746
	tierGapMS   = 60 // src/index.js:750

	// probeAccidentMin 是事故判定的样本下限,probeAliveFloor 是热区 pass
	// 的通过率地板。低于地板 = 探测通道本身坏了(本机断网/上游闸门故障),
	// 不是「60 个节点恰好同时死光」。正常换血在 60s 节奏下的单轮失败率
	// 远低于这个地板;真实池的冷区 pass 不参与这个判定(死节点的大多数
	// 失败是常态)。
	probeAccidentMin = 20
	probeAliveFloor  = 0.1

	// probeDirectTimeoutMS 是「本机到 liveness 源」这一跳的预算(src/nodeprobe.js:208)。
	probeDirectTimeoutMS = 8000

	// coldProbeTimeoutMS 是冷区 pass 的单发预算:死节点大多快败(拒连/复位),
	// 挂死型也只吃 7s —— 冷区 sweep 的总时长靠它兜住。
	coldProbeTimeoutMS = 7000

	// hotProbeTimeoutMS 是热区 pass 的单发预算(与全量探针的默认超时同值)。
	hotProbeTimeoutMS = 12000

	// hotWorkersMax / coldWorkersMax 是两个 pass 的并发封顶:合计不超过
	// 现行全量轮的 128 并发实测峰值(sing-box 拨号 + 上游匿名 GET 的压力
	// 形状不变)。两档 pass 可并发运行。
	hotWorkersMax  = 96
	coldWorkersMax = 32

	// tierProbeTimeoutMS 是 B 档(烧真配额的区域探针)的单次预算(src/index.js:911)。
	tierProbeTimeoutMS = 20000

	// firstProbeTick 是首探扫描的节拍:新入池的节点最坏等这么久开始全量首探。
	firstProbeTick = 30 * time.Second
)

// tierUnavailableRe 复刻 src/probe.js:113-128 stateOf 的「模型侧拒绝」判据,
// 但把裸词 `not supported` 收窄到**模型维度**(与 errors.modelRe 同一处收窄,
// 见该处注释:裸词会让 "region not supported" / "this endpoint is not
// supported" 命中,而这一支的结论是把模型判死到下一轮粗探)。
// 唯一的非模型字面量是裸 `unavailable`:上游的模型停服文案一律带它,而
// "temporarily unavailable" 这类更该由 status 分支处理(5xx 已在函数开头
// 归 unknown,403 的裸 unavailable 是上游真实的模型侧拒绝)。
var tierUnavailableRe = regexp.MustCompile(`(?i)unavailable|model is not supported|not supported model|unsupported model|model not found|no such model|unknown model|invalid model`)

// PassSummary 是一轮 pass(hot/cold/first)的结论,喂日志行与测试。
type PassSummary struct {
	Kind     string // "hot" / "cold" / "first"
	Scanned  int
	Alive    int
	Revived  int
	Demoted  int
	Deleted  int
	NewGated int
	Tested   int
	MS       int64
	At       int64

	Skipped  bool // 探测关闭/另一档在跑/无事可做
	Accident bool // 探测通道故障,整轮结果丢弃
}

// Prober 是探测对 nodeprobe 的全部依赖。做成接口是为了让测试注入假实现:
// 真实探测要打外网,而 pass 的逻辑(状态机、事故、删除)必须能在离线环境断言。
// 生产实现是 *nodeprobe.Prober。
type Prober interface {
	ProbeAll(ctx context.Context, items []nodeprobe.Item, workers int) []nodeprobe.Result
	ProbeDirect(ctx context.Context, timeoutMS int) error
}

// tierChains 按出口 IP 串行 B 档探测(src/index.js:770-781 withTierBucket)。
//
// 为什么按 IP 而不是按会话:配额桶归属未定(配额粒度没有裁决),按 IP 串行是
// 两种读法下都不会错的写法 —— 按 IP 计则并发会把同一出口顶成 429,而 429 会
// 被记成 tier 失败,那是自己制造的假阴性;按会话计只是少一点并发。
type tierChains struct {
	mu sync.Mutex
	m  map[string]chan struct{}
}

// with 串行执行 key 上的 fn。finally 里只删自己那一节:删掉整张表会踩掉
// 别人刚挂上来的链,而链断掉就意味着两个 B 档探测同时打同一个出口。
func (t *tierChains) with(key string, fn func()) {
	t.mu.Lock()
	if t.m == nil {
		t.m = map[string]chan struct{}{}
	}
	prev := t.m[key]
	done := make(chan struct{})
	t.m[key] = done
	t.mu.Unlock()

	if prev != nil {
		<-prev
	}
	defer func() {
		// close 先于摘链:等在后面的那一个必须在本节结束时立刻醒,而不是
		// 等我们走完摘链的临界区。
		close(done)
		t.mu.Lock()
		if t.m[key] == done {
			delete(t.m, key)
		}
		t.mu.Unlock()
	}()
	fn()
}

// probeEnabled 报告面板开关。关掉就所有 pass 都不跑,连日志都不打:这不是
// 失败,是用户的选择。
func (p *Parts) probeEnabled() bool {
	return p.settingsSnapshot().ProbeEnabled
}

// nudgeHot / nudgeCold / nudgeFirst 让对应 pass 立即跑一轮(面板按钮、重建
// 之后的补探)。非阻塞:通道缓冲 1,已有待处理的 nudge 时合并。
func (p *Parts) nudgeHot() {
	select {
	case p.hotNudge <- struct{}{}:
	default:
	}
}
func (p *Parts) nudgeCold() {
	select {
	case p.coldNudge <- struct{}{}:
	default:
	}
}
func (p *Parts) nudgeFirst() {
	select {
	case p.firstNudge <- struct{}{}:
	default:
	}
}

// probingActive 供 /api/status 的 probing 位:任意 pass 在跑即为真。
func (p *Parts) probingActive() bool {
	return p.hotRunning.Load() || p.coldRunning.Load() || p.firstRunning.Load()
}

// passWorkers 是单档 pass 的并发预算:设置值与「按节点数摊」取大者,再压到
// 该档的封顶(热 96 / 冷 32,合计不超现行 128 峰值)。与旧公式的差异只在封顶;
// **必须读 s.ProbeWorkers** —— 面板上挂着「探测并发」却不消费它,就是展示值
// 与事实不符的老毛病(第五轮审计同型)。自动档 (n+3)/4 与旧 probeWorkers 的
// 「按节点数摊」公式逐字相同,setting 只抬高、不压低它。
func passWorkers(n, setting, max int) int {
	w := (n + 3) / 4
	if setting > w {
		w = setting
	}
	if w > max {
		w = max
	}
	if w < 1 {
		w = 1
	}
	return w
}

// collectPassItems 把「tags 里能构建拨号器的」打包成探测项。
func (p *Parts) collectPassItems(tags []string, opts nodeprobe.ProbeOptions) []nodeprobe.Item {
	items := make([]nodeprobe.Item, 0, len(tags))
	for _, tag := range tags {
		d, err := p.Host.Dialer(tag)
		if err != nil {
			// 拨不出去的节点本轮测不到:没有证据不等于有罪,不参与状态推进。
			continue
		}
		items = append(items, nodeprobe.Item{Tag: tag, Dial: d, Options: opts})
	}
	return items
}

// applyAccidentGuard 是**热区** pass 的事故判决:复检对象的通过率跌破地板
// 说明探测通道本身坏了(上一轮刚证活的节点不该成批同时死光)。ProbeDirect 在
// 日志里区分「本机断网」与「上游闸门故障」——两种情况都整轮丢弃结果(成功的
// 少数量也不应用:一个把 95% 存活节点判死的 pass 里,那 5% 的「成功」同样
// 不可信)。冻结只在这里做**一处**:清 lastHotOK —— 冷区删除的闸门读的就是
// 这个位,清掉即暂停,直到下一次健康的热区 pass 重新置真。
//
// 本判据**只适用于热区**:复检的是幸存者,地板才有意义。首探的对象来路不明、
// 通过率天然个位数,误用本判据会把每一轮首探都杀光(1.3.2 修正;首探的通道
// 故障判据见 applyFirstProbeGuard)。
func (p *Parts) applyAccidentGuard(ctx context.Context, sum *PassSummary, tested, alive int) bool {
	if tested < probeAccidentMin || float64(alive)/float64(tested) >= probeAliveFloor {
		p.guardDiscards.Store(0)
		return false
	}
	derr := p.Prober.ProbeDirect(ctx, probeDirectTimeoutMS)
	reason := "上游闸门疑似故障"
	if derr != nil {
		reason = "本机断网（直连也不可达）"
	}
	// 2026-10-06:连续丢弃上限。热区节点曾活过，guard 保护它们不被误杀，
	// 但无限丢弃会让热区状态永远冻结。3 轮后强制落地。
	if n := p.guardDiscards.Add(1); n > 3 {
		p.guardDiscards.Store(0)
		p.lastHotOK.Store(false)
		logger.Warn(fmt.Sprintf("热区 guard 连续 %d 轮丢弃，强制落地（%d/%d 存活）", n, alive, tested))
		return false
	}
	p.lastHotOK.Store(false)
	logger.Error(fmt.Sprintf("探测通道疑似故障（%s）：热区 %d 个节点仅 %d 个通过（%.0f%% < %.0f%% 地板）— 本轮结果整体丢弃，冷区删除冻结",
		reason, tested, alive, float64(alive)/float64(tested)*100, probeAliveFloor*100))
	sum.Accident = true
	sum.Skipped = true
	return true
}

// runProbeItems 是 pass 的公共执行段:并发探测 → 取消丢弃。**不在此处判事故**
// —— 事故判据分档不同(热区看地板、首探看零通关),混在这里会让首探拿热区判据
// 误杀自己。返回 (结果, 通关数);取消或空批时 results 为 nil,调用方原样收场。
func (p *Parts) runProbeItems(ctx context.Context, items []nodeprobe.Item, max int) ([]nodeprobe.Result, int) {
	if len(items) == 0 {
		return nil, 0
	}
	// 并发预算 = max(设置值, 按节点数摊),封顶到该档。设置值必须被消费:
	// 面板上挂着「探测并发」却谁都不读它 = 展示值与事实不符(第五轮审计同型)。
	setting := p.settingsSnapshot().ProbeWorkers
	results := p.Prober.ProbeAll(ctx, items, passWorkers(len(items), setting, max))
	if ctx.Err() != nil {
		// C2 同理:取消的一轮不产生任何结论。
		logger.Info("probe pass: 已取消（关停）— 整轮结果丢弃")
		return nil, 0
	}
	alive := 0
	for _, r := range results {
		if r.Result.State == nodeprobe.StateAlive {
			alive++
		}
	}
	return results, alive
}

// applyFirstProbeGuard 是首探通道的事故判据(1.3.2 定稿,替代过去误用的热区
// 地板)。首探测的是**来路不明的新节点**,通过率天然个位数(2026-10-04 现场:
// 1010 个新节点三轮各 5%–6% 通过 —— 与 1.2.x 的 8% 全量基线同形状)。热区地板
// 套在这里会把每一轮首探都误判成通道故障、整轮丢弃:新节点永远拿不到健康行、
// 永远进不了热区,热区只出不进地缩到十几个(用户现场日志实锤的病灶)。
//
// 首探合法的通道故障信号只有一个:**零通关,且本机直连也不可达**。有通关 = 通道
// 活着(结果照常应用,死的新节点进冷区,由 lastHotOK 删除闸保护);零通关但直连
// 正常 = 这批订阅节点真的全是死的,同样是合法观测,照常应用。只有两者同时落空
// 才丢弃整轮 —— 与 1.2.x 的 ProbeDirect 语义逐字同口径。
func (p *Parts) applyFirstProbeGuard(ctx context.Context, sum *PassSummary, tested, alive int) bool {
	if alive > 0 || tested == 0 {
		p.guardDiscards.Store(0)
		return false
	}
	derr := p.Prober.ProbeDirect(ctx, probeDirectTimeoutMS)
	if derr == nil {
		p.guardDiscards.Store(0)
		return false // 零通关但直连正常:这批新节点确实全死,判决照常落地
	}
	// 2026-10-06:首探放宽。新节点从没活过，误判为 dead 的成本低
	// （cold pass 会重试复活）；无限丢弃只会让节点永远 unknown。
	// 连续丢弃 3 轮后强制落地。
	if n := p.guardDiscards.Add(1); n > 3 {
		p.guardDiscards.Store(0)
		logger.Warn(fmt.Sprintf("首探连续 %d 轮被 guard 丢弃，强制落地（%d 个新节点按 dead 处理，cold pass 会重试）", n, tested))
		return false
	}
	logger.Error(fmt.Sprintf("探测通道疑似故障（首探 %d 个新节点零通关，且本机直连也不可达：%v）— 本轮结果整体丢弃，下一拍重探",
		tested, derr))
	sum.Accident = true
	sum.Skipped = true
	return true
}

// hotPass 复检热区(alive)节点。keep alive 节点在 60s 级别的新鲜度:这是
// 流量真正要走的出口。
func (p *Parts) hotPass(ctx context.Context) PassSummary {
	started := p.nowMS()
	sum := PassSummary{Kind: "hot", At: started}
	if !p.probeEnabled() || p.Prober == nil {
		sum.Skipped = true
		return sum
	}
	if !p.hotRunning.CompareAndSwap(false, true) {
		sum.Skipped = true
		return sum
	}
	defer p.hotRunning.Store(false)

	snap := p.Health.NodeSnapshot()
	var tags []string
	for _, n := range p.poolNodes() {
		if view, ok := snap[n.Tag]; ok && view.State == health.StateAlive {
			tags = append(tags, n.Tag)
		}
	}
	items := p.collectPassItems(tags, nodeprobe.ProbeOptions{
		TimeoutMS: hotProbeTimeoutMS, Attempts: 1, UpstreamOnly: true,
	})
	sum.Scanned = len(tags)
	if len(items) == 0 {
		// 热区暂时为空(冷启动/全池已死):没东西可测,打一行方便对账。
		logger.Info(fmt.Sprintf("probe round: 0/%d alive (热区为空,等待冷区/首探复活)", sum.Scanned))
		sum.MS = p.nowMS() - started
		return sum
	}
	results, alive := p.runProbeItems(ctx, items, hotWorkersMax)
	if results == nil {
		// 取消的一轮:什么都不应用,原样收场。
		sum.MS = p.nowMS() - started
		return sum
	}
	if p.applyAccidentGuard(ctx, &sum, len(items), alive) {
		// 事故判决必须原样进本 pass 的 summary(面板/测试读 sum.Accident),
		// 且一个结果都不应用:判据已整轮丢弃。lastHotOK 已由判据清成 false ——
		// 冻结冷区删除正是这道闸的语义。
		sum.MS = p.nowMS() - started
		return sum
	}
	p.lastHotOK.Store(true)

	demoted := 0
	for _, r := range results {
		switch r.Result.State {
		case nodeprobe.StateAlive:
			p.Health.MarkPassSuccess(r.Tag)
			sum.Alive++
		case nodeprobe.StateDead:
			if p.Health.MarkPassFail(r.Tag, true) == "demote" {
				demoted++
			}
		}
	}
	sum.Demoted = demoted
	// B 档认证:进入热区的非 B 节点(新认证/region 恢复)在此烧一次 16-token
	// 认证。已证 B 的跳过 —— 认证是准入,不是监测。
	sum.NewGated = p.runTierPipeline(ctx, items, results)

	counts := p.Health.TierCounts()
	sum.Tested = len(items)
	sum.MS = p.nowMS() - started

	// 前端 probeFromLogs 按 "probe round:" 前缀抓热区摘要,形状必须兼容:
	//   /probe round:\s*(\d+)\/(\d+)\s+alive\s*\(A\s*(\d+)[^0-9]*B\s*(\d+)[^)]*\)\s*in\s*([\d.]+)s/
	logger.Info(fmt.Sprintf(
		"probe round: %d/%d alive (A %d · B %d · 本轮新验 B %d · 降冷 %d) in %.1fs",
		sum.Alive, sum.Scanned, counts.A, counts.B, sum.NewGated, demoted,
		float64(sum.MS)/1000))
	return sum
}

// coldPass 扫冷区(dead)节点:复活立即升热;连续 3 轮失败 → 彻底删除。
// 删除只在 lastHotOK(最近一次热区 pass 健康结束)时执行 —— 探测通道坏了的
// 时候,「冷区连续失败」是通道的锅,不是节点的罪。
func (p *Parts) coldPass(ctx context.Context) PassSummary {
	started := p.nowMS()
	sum := PassSummary{Kind: "cold", At: started}
	if !p.probeEnabled() || p.Prober == nil {
		sum.Skipped = true
		return sum
	}
	if !p.coldRunning.CompareAndSwap(false, true) {
		sum.Skipped = true
		return sum
	}
	defer p.coldRunning.Store(false)

	snap := p.Health.NodeSnapshot()
	var tags []string
	for _, n := range p.poolNodes() {
		if view, ok := snap[n.Tag]; ok && view.State == health.StateDead {
			tags = append(tags, n.Tag)
		}
	}
	// 轮换抽样:死节点多时每轮只扫 1/3,三轮覆盖全池。排序后按游标切,
	// 复活延迟最多多两轮(20 分钟),探测开销降 2/3。
	sort.Strings(tags)
	if len(tags) > 30 {
		slot := int(p.coldCursor.Add(1)-1) % 3
		kept := make([]string, 0, len(tags)/3+1)
		for i, tag := range tags {
			if i%3 == slot {
				kept = append(kept, tag)
			}
		}
		tags = kept
	}
	items := p.collectPassItems(tags, nodeprobe.ProbeOptions{
		TimeoutMS: coldProbeTimeoutMS, Attempts: 1, UpstreamOnly: true,
	})
	sum.Scanned = len(tags)
	if len(items) == 0 {
		sum.MS = p.nowMS() - started
		return sum
	}
	results := p.Prober.ProbeAll(ctx, items, passWorkers(len(items), p.settingsSnapshot().ProbeWorkers, coldWorkersMax))
	if ctx.Err() != nil {
		logger.Info("cold pass: 已取消（关停）— 整轮结果丢弃")
		sum.MS = p.nowMS() - started
		return sum
	}
	// 冷区自己的通道闸:整轮零复活(常态可以是零)且直连也挂 → 丢弃本轮,
	// 别让「本机断网」给冷区节点白记失败;直连正常则照常应用(零复活是
	// 合法观测)。
	aliveCount := 0
	for _, r := range results {
		if r.Result.State == nodeprobe.StateAlive {
			aliveCount++
		}
	}
	if len(items) >= probeAccidentMin && aliveCount == 0 {
		if derr := p.Prober.ProbeDirect(ctx, probeDirectTimeoutMS); derr != nil {
			logger.Error(fmt.Sprintf("冷区 sweep 期间本机断网（%v）— 本轮结果丢弃", derr))
			sum.Accident = true
			sum.Skipped = true
			sum.MS = p.nowMS() - started
			return sum
		}
	}

	deletionsAllowed := p.lastHotOK.Load()
	deletedTags := make([]string, 0, 8)
	for _, r := range results {
		switch r.Result.State {
		case nodeprobe.StateAlive:
			p.Health.MarkPassSuccess(r.Tag)
			sum.Revived++
		case nodeprobe.StateDead:
			// 删除判决只在热区健康时执行;冻结期间这次失败不计数
			// (下一轮再算),避免「通道抖三下 = 白删一池子」。
			switch p.Health.MarkPassFail(r.Tag, deletionsAllowed) {
			case "delete":
				p.Registry.Remove(r.Tag)
				p.Health.Forget(r.Tag)
				deletedTags = append(deletedTags, r.Tag)
				sum.Deleted++
			case "demote":
				// 冷区节点不会再降档(已经是冷区);防御性忽略。
			}
		}
	}
	if len(deletedTags) > 0 {
		// 落盘与 box 同步是**两件事**,顺序也定死为「先落盘、后同步」:
		//
		// 落盘必须无条件执行(第七轮复审更正)。旧写法把 Flush/Persist 挂在
		// SyncOutbounds 成功分支里,而候选集来自**内存 registry**,被删 tag 当场
		// 就离开了候选 —— 下一轮 coldPass 的 deletedTags **必然为空**,永远不再
		// 有第二次机会补写。订阅长期全挂时 rebuildOnce 也不跑 → 重启后这批已删
		// 节点从盘上复活继续占位,要再凑一次冷区三振才清得掉。成员账是内存事实
		// 的镜像,与 box 有没有同步成功无关(box 留在旧出站也只是多跑一轮,
		// 重启后 registry 载入即自洽)。
		if err := p.Registry.Flush(); err != nil {
			logger.Warn(fmt.Sprintf("[app] cold pass 注册表落盘失败: %v", err))
		}
		if err := p.Health.Persist(); err != nil {
			logger.Warn(fmt.Sprintf("[app] cold pass 健康表落盘失败: %v", err))
		}
		// 删掉的出站必须同步摘掉 sing-box 内的出站,否则残留出站占着
		// 端口段与内存直到下次 Rebuild,而 Rebuild 只在订阅刷新时跑。
		if p.Host != nil {
			if _, _, err := p.Host.SyncOutbounds(p.Registry.All()); err == nil {
				// 换代必须 bump(六审):缓存 client 的拨号闭包绑着被删出站,
				// 不换代的话 noteEgressChanged 的契约(O3:旧代 client 不得再服务
				// 请求)对这批 tag 失效,请求会在已撤出站上白烧一次 transport 失败。
				p.noteEgressChanged()
			} else {
				logger.Warn(fmt.Sprintf("[app] cold pass 同步出站失败: %v", err))
			}
		}
	}
	sum.Tested = len(items)
	sum.MS = p.nowMS() - started
	logger.Info(fmt.Sprintf(
		"cold pass: %d scanned · 复活 %d · 删除 %d（删除闸门 %s）in %.1fs",
		sum.Scanned, sum.Revived, sum.Deleted, map[bool]string{true: "开", false: "冻结"}[deletionsAllowed],
		float64(sum.MS)/1000))
	if !deletionsAllowed && sum.Scanned > 0 {
		// 六审 F3-2:闸门冻结 + 有冷区待清 = 静默吸收态(全池皆死、热区无
		// 样本可开窗),每轮冷区失败只计数不删 —— 用 Warn 喊出来并给出
		// 解锁路径,运维不必从两行 Info 里自己拼因果。
		logger.Warn(fmt.Sprintf(
			"[app] 冷区删除闸门持续冻结(热区无健康样本),%d 个死节点只计不删 — 解锁需热区出一轮 alive 或首探通关;长期如此请检查订阅质量或点「立即探测」", sum.Scanned))
	}
	return sum
}

// firstProbePass 给「还没有健康行」的节点做全量首探(三段 + echo),通关即
// 进入热/冷双循环。B 档认证不在这里烧 —— hot 复检补验(见下)。
func (p *Parts) firstProbePass(ctx context.Context) PassSummary {
	started := p.nowMS()
	sum := PassSummary{Kind: "first", At: started}
	if !p.probeEnabled() || p.Prober == nil {
		sum.Skipped = true
		return sum
	}
	if !p.firstRunning.CompareAndSwap(false, true) {
		sum.Skipped = true
		return sum
	}
	defer p.firstRunning.Store(false)

	snap := p.Health.NodeSnapshot()
	var tags []string
	for _, n := range p.poolNodes() {
		// 圈定「还没有判决」的节点：无行，**或** unknown 行。unknown 行的
		// 唯一生产来源是 NoteQuota（节点在被首探前撞了一发真流量 429，
		// health.go 的 NoteQuota 给它造了 {state:unknown} 临时行），以及粗探
		// 自己判出 StateUnknown 的那些形状。hotPass 只收 alive、coldPass 只收
		// dead，若首探也只收「无行」，带 unknown 行的节点就是三档 pass 的
		// **联合盲区**：永不探测、v1.4.1 起也永不计数删除，一直挂到进程重启
		// （Load 白名单丢行）才回队 —— 期间它照常可被 Pick 选中。
		//
		// 注意这里**没有**「刚探过就跳过」的节流：unknown 判决会被 MarkProbe
		// 开头的早退整行丢弃（连 LastProbeAt 都不刷），所以这类节点每一拍都会
		// 重新进候选，直到探出 alive/dead 为止 —— 这正是自愈路径，不是漏洞
		// （代价与一个从未探过的节点相同）。
		if view, ok := snap[n.Tag]; !ok || view.State == health.StateUnknown {
			tags = append(tags, n.Tag)
		}
	}
	items := p.collectPassItems(tags, nodeprobe.ProbeOptions{})
	sum.Scanned = len(tags)
	if len(items) == 0 {
		sum.MS = p.nowMS() - started
		return sum
	}
	results, alive := p.runProbeItems(ctx, items, hotWorkersMax)
	if results == nil {
		sum.MS = p.nowMS() - started
		return sum
	}
	if p.applyFirstProbeGuard(ctx, &sum, len(items), alive) {
		sum.Accident = true
		sum.Skipped = true
		sum.MS = p.nowMS() - started
		return sum
	}
	for _, r := range results {
		// MarkProbe:既有判决写入(含 echo 的出口 IP/国家);unknown 自动跳过。
		p.Health.MarkProbe(r.Tag, r.Result)
		switch r.Result.State {
		case nodeprobe.StateAlive:
			sum.Alive++
		case nodeprobe.StateDead:
			// 首探判死:进冷区,连败从 0 开始数。
		}
	}
	// 首探不烧 B 档认证:刚通关的节点还没证明稳定性,活不过 60 秒的照样烧
	// 真配额。B 是准入,hot 复检(hotPass 的 runTierPipeline)会在它活过一轮
	// 后补验 —— 不稳定的节点在验 B 之前就被降冷,配额不浪费。
	sum.NewGated = 0
	sum.Tested = len(items)
	sum.MS = p.nowMS() - started
	logger.Info(fmt.Sprintf(
		"first probe: %d 个新节点, %d 通关 (新验 B %d) in %.1fs",
		sum.Scanned, sum.Alive, sum.NewGated, float64(sum.MS)/1000))
	return sum
}

// runTierPipeline 对 A 档通关的节点跑 B 档区域探针,返回本轮新验证为 B 的节点数。
//
// B 档探测是真会话、真烧配额,所以它挂在两个闸门后面:全局错峰闸门
// (tierGate,60ms 一个)和按出口 IP 的串行链。已经验明 B 的节点永不重测 ——
// 认证是准入,不是监测(1.3.0 定稿);稳态成本跟着新进热区的节点数走。
func (p *Parts) runTierPipeline(ctx context.Context, items []nodeprobe.Item, results []nodeprobe.Result) int {
	alive := make([]string, 0, len(results))
	for _, r := range results {
		if r.Result.State == nodeprobe.StateAlive {
			alive = append(alive, r.Tag)
		}
	}
	sort.Strings(alive) // Go map 无序;固定顺序让闸门的排队可复现

	var gated atomic.Int64
	sem := make(chan struct{}, tierWorkers)
	var wg sync.WaitGroup
	for _, tag := range alive {
		if p.Health.TierOf(tag) == health.TierB {
			// 已证 B:不重测,也**不计入**返回值 —— 日志字段是「本轮新验 B」,
			// 把存量算进去会让面板数字虚高。
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(tag string) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				// 一个探针不能杀死整轮。B 档在真网络上跑,单个节点的
				// 意外不该把 A 档的结果一起带走。
				if rec := recover(); rec != nil {
					logger.Warn(fmt.Sprintf("[app] B 档探测异常（%s）: %v", tag, rec))
				}
			}()
			if err := p.tierGate.Wait(ctx); err != nil {
				return
			}
			key := p.Health.ExitIPOf(tag, p.nowMS())
			if key == "" {
				// 没有出口 IP 的节点各自成桶:共用一把锁会把所有未知出口
				// 串成一条链,白白拖长整轮(src/index.js:758-768)。
				key = "tag:" + tag
			}
			verdict := ""
			p.tierBuckets.with(key, func() { verdict = p.probeTierModel(ctx, tag) })
			if verdict == "" {
				return
			}
			p.Health.MarkTierProbe(tag, verdict)
			if verdict == "available" {
				gated.Add(1)
			}
		}(tag)
	}
	wg.Wait()
	return int(gated.Load())
}

// tierPingBody 复刻 src/probe.js:90-109 buildPing:一个 16 token 的 ping。
func tierPingBody(modelID string, wire upstream.Wire) map[string]any {
	switch wire {
	case upstream.WireResponses:
		return map[string]any{
			"model": modelID,
			"input": []any{map[string]any{
				"type": "message", "role": "user",
				"content": []any{map[string]any{"type": "input_text", "text": "ping"}},
			}},
			"stream":            true,
			"store":             false,
			"max_output_tokens": 16,
		}
	default: // chat 与 messages 同形
		return map[string]any{
			"model":      modelID,
			"messages":   []any{map[string]any{"role": "user", "content": "ping"}},
			"stream":     true,
			"max_tokens": 16,
		}
	}
}

// probeTierModel 是 B 档探针的生产实现:经该节点的出口问一次区域模型。
//
// 它经 host.Dialer(tag) 出去 —— 零端口架构里没有别的出口形式。返回空串表示
// 「没有判决」(连不上、或状态码说不出所以然),调用方据此跳过 MarkTierProbe:
// 不确定的探针不得降级一个很可能没问题的节点。
func (p *Parts) probeTierModel(ctx context.Context, tag string) string {
	d, err := p.Host.Dialer(tag)
	if err != nil {
		return ""
	}
	model := health.RegionProbeModel
	wire := upstream.WireFor(model)
	body := tierPingBody(model, wire)
	upstream.ApplyFingerprint(body, wire)
	payload, err := json.Marshal(body)
	if err != nil {
		return ""
	}
	pctx, cancel := context.WithTimeout(ctx, time.Duration(tierProbeTimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodPost,
		p.base()+upstream.EndpointFor(model), bytes.NewReader(payload))
	if err != nil {
		return ""
	}
	// 一个共享会话是唯一不会美化结果的选择:按出口新建会话会在「配额按会话
	// 计」时掩盖真实耗尽,在「按 IP 计」时又买不到任何余量(src/probe.js:41-58)。
	session := upstream.SessionForConversation("probe:our-free-model")
	for name, value := range upstream.GatewayHeaders(upstream.HeaderOptions{
		Session:   session,
		RequestID: upstream.RequestIDFor(session, ""),
		Stream:    true,
	}) {
		req.Header.Set(name, value)
	}
	// OneShotClient:每 shot 一个全新 Transport,keep-alive 会留一条 idle 连接
	// 挂满 60s —— 一轮 B 档几十到几百 shot 就是同量级的瞬时 fd 尖峰。
	client := httpclient.NewOneShotClient(d, time.Duration(tierProbeTimeoutMS)*time.Millisecond)
	resp, err := client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "available"
	}
	fail := errors.Classify(resp.StatusCode, raw, errors.RetryAfter(resp.Header.Get("Retry-After")))
	return tierVerdictOf(resp.StatusCode, fail.Code, string(raw))
}

// tierVerdictOf 复刻 src/probe.js:113-128 stateOf 的分岔。
//
// 分岔顺序是承重的:上游 1.2.2 的 503 是网关侧故障,不是模型判决;400/404/422
// 才是路由层拒绝。把前者读成后者会把一个活模型判死到下一轮。
func tierVerdictOf(status int, code string, body string) string {
	switch {
	case code == check.CodeRegion:
		return "region-blocked"
	case code == check.CodeQuota:
		// 429 说的是「这个出口现在被限流」,和出口健康无关 —— 记成节点失败
		// 就是把一次上游限流翻译成一次节点死亡。
		return "throttled"
	case status >= 500:
		return "unknown"
	case status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusUnprocessableEntity:
		return "unavailable"
	case tierUnavailableRe.MatchString(body):
		return "unavailable"
	default:
		return "unknown"
	}
}
