// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 一个探测轮次决定哪些出口还留在池子里。它单独成文件是因为这一轮有十几条
// 分支 —— 结果缓存窗口、直连可达闸门、事故判定、B 档流水线 —— 每一条都有
// 一个会付出代价的失败模式:要么赔上整个池子(WAN 抖十秒给每个节点写 dead
// 行),要么赔上订阅预算(重复探测一个判决还很新鲜的节点)。
package app

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
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
	"freerouter/internal/parse"
	"freerouter/internal/registry"
	"freerouter/internal/upstream"
)

// 常量与 src/index.js 逐行对应。数值来自实测,不是猜的:
const (
	tierWorkers = 8  // src/index.js:746
	tierGapMS   = 60 // src/index.js:750

	probeAccidentRate = 0.5 // src/index.js:795
	probeAccidentMin  = 20  // src/index.js:796 的 8 在真实池上太小:alive 集只有 50-93 个时,
	// 固有抖动就能超过 50% 流失(2026-10-03 实测三轮连触发,淘汰被永久压制)——
	// 样本下限提到 20,并配合 collapse 双条件,见 ProbeNow。
	probeCacheRatio = 0.5 // src/index.js:816

	// probeDirectTimeoutMS 是「本机到 liveness 源」这一跳的预算(src/nodeprobe.js:208)。
	probeDirectTimeoutMS = 8000

	// probeDegradedTimeoutMS 是连败节点(registry.failCount > 0)的降级探测预算:
	// 一次尝试 + 更短超时。首轮实测 2373 个节点里 2246 个没通关,双次 12s
	// 超时把单轮拖到 493s,而这些节点只是占着位置,不会因为多等一次就活过来
	// (src/index.js:927-930)。
	probeDegradedTimeoutMS = 7000

	// tierProbeTimeoutMS 是 B 档(烧真配额的区域探针)的单次预算(src/index.js:911)。
	tierProbeTimeoutMS = 20000
)

// tierUnavailableRe 复刻 src/probe.js:113-128 stateOf 的「模型侧拒绝」判据。
var tierUnavailableRe = regexp.MustCompile(`(?i)unavailable|not supported|no such model|unknown model|invalid model`)

// ProbeSummary 是一轮探测的结论。它同时喂给日志行(前端按正则抓)、面板和测试。
type ProbeSummary struct {
	Alive     int
	Scanned   int
	TierA     int
	TierB     int
	NewGated  int
	Tested    int
	Cached    int
	Removed   int
	Probation int
	Pool      int
	MS        int64
	At        int64

	Skipped  bool // 探测源不通,整轮跳过
	Accident bool // 探测源事故(alive 掉一半以上)
	// SourceAddr 删掉了(审计 O6):它恒被写成 "direct"、从未被读过 —— 零端口架构
	// 下「这一轮从哪个地址探的」不是一个事实,JS 版的 sourceAddr 同理。
}

// Prober 是探测轮次对 nodeprobe 的全部依赖。做成接口是为了让测试注入假实现:
// 真实探测要打外网,而这一轮的逻辑(缓存、事故、淘汰)必须能在离线环境断言。
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

// markRerun 记下「本轮结束后再来一轮」。并发调 ProbeNow 时,后来的调用者不是
// 排队等锁,而是把意图记在这里 —— 这样连按两次面板按钮不会产生两轮并发探测,
// 也就不会把同一批节点测两遍、让淘汰判决互相覆盖(src/index.js:831-834)。
func (p *Parts) markRerun(force bool) {
	p.probeRerunMu.Lock()
	p.probeRerun = true
	p.probeRerunForce = p.probeRerunForce || force
	p.probeRerunMu.Unlock()
}

// finishProbeRound 是本轮唯一的出口:清 running 标志,并在有人排队时安排
// 一秒后的重跑。注释照抄 src/index.js:987-995 —— 本轮探测被取消时不要留
// 半个池子。
func (p *Parts) finishProbeRound() {
	p.probing.Store(false)
	p.probeRerunMu.Lock()
	rerun := p.probeRerun
	rerunForce := p.probeRerunForce
	p.probeRerun = false
	p.probeRerunForce = false
	p.probeRerunMu.Unlock()
	if !rerun {
		return
	}
	p.afterFunc(time.Second, func() {
		// B9:重跑必须挂在 lifeCtx 上。从前这里写死 context.Background(),
		// 是四个复活点里唯一连 ctx.Err() 都不查的:关停之后这一秒的定时器
		// 照样触发,以全新 context 跑完一整轮探测。
		ctx := p.ctx()
		if ctx.Err() != nil {
			return
		}
		if _, err := p.ProbeNow(ctx, rerunForce); err != nil {
			logger.Warn(fmt.Sprintf("[app] 探测重跑失败: %v", err))
		}
	})
}

// cacheWindowMS 是结果缓存窗口:周期的一半,下限两分钟(src/index.js:818-821)。
//
// 取一半而不是整周期:窗口等于周期时,now-lastProbeAt 恰好落在边界上,而判据
// 是 <=,于是每隔一轮就整轮跳过,刷新频率被静默腰斩。下限两分钟是因为
// probeIntervalMin=5 时半周期只剩 2.5 分钟。
func (p *Parts) cacheWindowMS() int64 {
	cycleMin := p.settingsSnapshot().ProbeIntervalMin
	if cycleMin < 5 {
		cycleMin = 5 // src/index.js:818 的 Math.max(5, probeIntervalMin)
	}
	window := time.Duration(float64(time.Duration(cycleMin)*time.Minute) * probeCacheRatio)
	if window < 2*time.Minute {
		window = 2 * time.Minute
	}
	return window.Milliseconds()
}

// ProbeNow 跑一轮探测。force=true 跳过结果缓存窗口,每个节点都实测。
//
// 返回 error 只有一种情况:已有一轮在跑。其它失败(直连不通、单节点失败)都
// 表达在 ProbeSummary 里 —— 探测轮次是后台任务,把「外网抖动」升级成调用方
// 必须处理的 error 只会让面板的按钮弹出一个没人能处理的对话框。
func (p *Parts) ProbeNow(ctx context.Context, force bool) (ProbeSummary, error) {
	started := p.nowMS()
	summary := ProbeSummary{At: started}

	cur := p.settingsSnapshot()
	if !cur.ProbeEnabled {
		// 面板关掉探测就整轮不跑,连日志都不打:这不是失败,是用户的选择。
		return summary, nil
	}
	if p.Prober == nil {
		return summary, stderrors.New("app: 探测实现未装配")
	}
	if !p.probing.CompareAndSwap(false, true) {
		p.markRerun(force)
		return ProbeSummary{}, stderrors.New("probe already running")
	}
	defer p.finishProbeRound()

	pool := p.Registry.All()
	summary.Scanned = len(pool)
	summary.Pool = len(pool)
	if len(pool) == 0 {
		return summary, nil
	}

	// 结果缓存窗口内的节点跳过实测,但缓存里 alive 的节点仍要计入本轮存活:
	// 漏了这一步,事故判定会把「因为新鲜所以没测」误当成「掉线」,于是一轮
	// 完全正常的探测被标成事故(src/index.js:840-844)。
	windowMS := p.cacheWindowMS()
	aliveTags := make(map[string]bool, len(pool))
	toProbe := make([]parse.Outbound, 0, len(pool))
	// O32:窗口判定过去对每个节点各拿两次锁(ProbedWithin + HealthOf);一次
	// 快照拿全,判据逐字等价(state 必须 alive/dead 才算「量过」,LastProbeAt
	// 在窗口内)。
	snap := p.Health.NodeSnapshot()
	nowMS := p.nowMS()
	for _, o := range pool {
		if view, ok := snap[o.Tag]; ok &&
			(view.State == health.StateAlive || view.State == health.StateDead) &&
			view.LastProbeAt > 0 && nowMS-view.LastProbeAt <= windowMS {
			if !force {
				summary.Cached++
				if view.State == health.StateAlive {
					aliveTags[o.Tag] = true
				}
				continue
			}
		}
		toProbe = append(toProbe, o)
	}
	if len(toProbe) == 0 {
		logger.Info(fmt.Sprintf("probe round: 整轮跳过（%d 个节点都在 %d 分钟结果缓存窗口内）",
			summary.Cached, int(windowMS/60000)))
		summary.Alive = len(aliveTags)
		summary.MS = p.nowMS() - started
		return summary, nil
	}

	// 探测源直连门。不通就整轮跳过 —— 不是「照测但不淘汰」:照测会给每个节点
	// 写 dead 行,而 pickExit 看的是行状态,一次十秒的 WAN 抖动会造成三十分钟
	// 全网 503(src/index.js:870-878)。
	if err := p.Prober.ProbeDirect(ctx, probeDirectTimeoutMS); err != nil {
		logger.Error(fmt.Sprintf("探测源直连不可达：本机到 liveness 源全失败（%v）— 本轮跳过，不探测也不淘汰（现有 %d 个节点与健康行原样保留）",
			err, len(pool)))
		summary.Skipped = true
		summary.MS = p.nowMS() - started
		return summary, nil
	}

	// prevAlive 必须在探测开始前取:事故判定的分母是「上一轮还活着」的节点,
	// 而不是「这一轮有多少节点通过」。整池失败率是常态(首轮 2373 个节点里
	// 2246 个不通),只有一批已证存活的同时掉线才指向探测源本身。
	prevAlive := map[string]bool{}
	for tag, view := range p.Health.NodeSnapshot() {
		if view.State == health.StateAlive {
			prevAlive[tag] = true
		}
	}

	items := make([]nodeprobe.Item, 0, len(toProbe))
	for _, o := range toProbe {
		d, derr := p.Host.Dialer(o.Tag)
		if derr != nil {
			// 拨不出去的节点本轮测不到:它不进 probedTags,于是淘汰判决也
			// 碰不到它 —— 「没证据」不等于「有罪」。
			continue
		}
		opts := nodeprobe.ProbeOptions{}
		if p.Registry.FailCount(o.Tag) > 0 {
			opts = nodeprobe.ProbeOptions{Attempts: 1, TimeoutMS: probeDegradedTimeoutMS}
		}
		items = append(items, nodeprobe.Item{Tag: o.Tag, Dial: d, Options: opts})
	}

	if len(items) == 0 {
		// 所有候选的拨号器都构建失败:这一轮其实什么都没测。旧判据
		// (Scanned>0 && alive==0) 会把这当成「探测源事故」打一行误导排障的
		// 日志 —— 直连门通过说明本机没断网,但真相是本轮零测量。
		logger.Warn(fmt.Sprintf("[app] 本轮没有可测节点：%d 个候选的拨号器全部构建失败 — 不淘汰、不判事故", len(toProbe)))
		summary.MS = p.nowMS() - started
		return summary, nil
	}
	summary.Tested = len(items)
	probedTags := make(map[string]bool, len(items))
	for _, it := range items {
		probedTags[it.Tag] = true
	}
	results := p.Prober.ProbeAll(ctx, items, p.probeWorkers(len(items)))
	if ctx.Err() != nil {
		// C2(关键):取消发生在探测途中 —— 面板「立即探测」一轮要跑数分钟,
		// 期间刷新/关页、或关停掐断了请求 ctx。此后的每个 shot 都会立刻失败、
		// samples 恒为空,而 nodeprobe 的「量不到 = dead」判决加上下面无条件的
		// MarkProbe 会把**整个池子**写成 dead 并落盘:pick 排除全部 dead,每个
		// 请求 503,而且这些 dead 行在缓存窗口内被当「新鲜结论」跳过不重测,
		// 故障持续到窗口过期。取消的一轮不产生任何结论:结果整体丢弃,
		// 健康表与淘汰账原样保留。
		logger.Info("probe round: 已取消（请求方离开或关停）— 整轮结果丢弃，健康表原样保留")
		summary.MS = p.nowMS() - started
		return summary, nil
	}
	// unknownTags 是 backstop 兜底点火的节点(本轮没量出来)。nodeprobe 的契约
	// (nodeprobe.go:440-452)与 health.MarkProbe 都写着「拿到 unknown 应当跳过它
	// 这一轮」:它既不是通关也不是判决。跳过在两个地方都要兑现 —— 记连败会
	// 三轮后够到淘汰门槛(探测源越抖,池子越缩),而 RetainOnly 看 probedTags,
	// 留在这里同样会被判死。
	unknownTags := make(map[string]bool, len(items))
	for _, r := range results {
		p.Health.MarkProbe(r.Tag, r.Result)
		switch r.Result.State {
		case nodeprobe.StateAlive:
			aliveTags[r.Tag] = true
		case nodeprobe.StateDead:
		default:
			unknownTags[r.Tag] = true
			delete(probedTags, r.Tag) // 与拨不出去的节点同一待遇:没证据不等于有罪
		}
	}

	// B 档流水线。JS 是在 A 档每个节点通关时立刻触发(onPass 回调),Go 的
	// nodeprobe.ProbeAll 按输入顺序整批返回、没有回调,所以这里等 A 档收完再
	// 统一编排 —— 时序不同,结论一致(只有通关的节点才值得花配额)。
	summary.NewGated = p.runTierPipeline(ctx, items, results)

	lostAlive := 0
	for tag := range prevAlive {
		if !aliveTags[tag] {
			lostAlive++
		}
	}
	ratioHit := len(prevAlive) >= probeAccidentMin &&
		float64(lostAlive)/float64(len(prevAlive)) > probeAccidentRate
	// collapse 是第二把闸:存活塌到上一轮的 1/4 以下才算「绝对值也塌方」。
	// 真实数据(2026-10-03):alive 50-59 的池子每轮随机换血 60-80% 却仍是
	// 同一个可服务规模 —— 只看比率,事故轮每轮都误触发,淘汰被永久压制
	// (观察期涨到全池 97%,池子只增不减)。双条件后:随机换血(比率超 50%
	// 但绝对值持平)不再误报;真正的探测源断供(alive 塌到 1/4 以下/0)
	// 两条同时满足,保护仍在。
	collapse := len(prevAlive) >= probeAccidentMin && len(aliveTags)*4 < len(prevAlive)
	zeroAlive := summary.Tested > 0 && len(aliveTags) == 0
	accident := (ratioHit && collapse) || zeroAlive
	summary.Accident = accident
	if ratioHit {
		logger.Error(fmt.Sprintf("探测源疑似事故：上一轮存活的 %d 个节点本轮掉了 %d 个（%.0f%% > %.0f%%）— 本轮不淘汰任何节点，保留现有池子",
			len(prevAlive), lostAlive,
			float64(lostAlive)/float64(len(prevAlive))*100, probeAccidentRate*100))
	} else if zeroAlive {
		logger.Error(fmt.Sprintf("探测源疑似事故：本轮实测 %d 个节点 0 个通关（直连门已通过，说明不是本机断网）— 本轮不淘汰任何节点，保留现有池子",
			summary.Tested))
	}

	// 计败:本轮真的测出结论、又没通关的节点各记一次。缓存跳过的不在此列 ——
	// 缓存的语义是「同一个观测只算一次」,否则 MAX_FAILS=3 会被缓存加速一倍。
	// unknown 也不在此列(R16):兜底超时是「本轮没量出来」,不是「量到了不通」。
	now := p.now()
	for _, it := range items {
		if aliveTags[it.Tag] || unknownTags[it.Tag] {
			continue
		}
		p.Registry.NoteFail(it.Tag, now)
	}

	// alive 名单 = 本轮通关的节点;连败未到门槛的是「观察期」名单。两者必须分开:
	// RetainOnly 对 alive 做「清零连败 + 清墓碑」,把观察期节点塞进 alive 会让
	// 计数每轮 1→0,门槛永远够不到(现场表现:淘汰恒为 0、观察期长期钉死)。
	// Go 把 JS retainOnly 里耦在一起的「计败」与「判决」拆成两步,正值的门槛由这里
	// 用两个名单执行。事故轮不能传空的 Protected —— 那会把本轮真通关的节点也记成
	// 一次失败,所以只把 MaxFails 抬成 -1(Infinity,只记账不淘汰)。
	aliveList := make([]string, 0, len(aliveTags))
	for tag := range aliveTags {
		aliveList = append(aliveList, tag)
	}
	protected := make(map[string]bool, len(items))
	maxFails := registry.MaxFails
	if accident {
		maxFails = -1
	} else {
		for _, it := range items {
			if aliveTags[it.Tag] {
				continue
			}
			if p.Registry.FailCount(it.Tag) < registry.MaxFails {
				protected[it.Tag] = true
			}
		}
	}
	dropped := p.Registry.RetainOnly(aliveList, registry.RetainOpts{
		ProbedTags: probedTags,
		Protected:  protected,
		MaxFails:   maxFails,
	})
	for _, tag := range dropped {
		p.Health.Forget(tag)
	}
	// 池子超上限的淘汰同样要清健康行(D-C1):RetainOnly 的 dropped 只覆盖
	// 「本轮测过且没活」的,容量挤出的是另一批人。
	for _, tag := range p.Registry.EnforceCap(registry.PoolCap) {
		p.Health.Forget(tag)
	}
	if err := p.Health.Persist(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 健康表落盘失败: %v", err))
	}

	counts := p.Health.TierCounts()
	summary.Alive = len(aliveTags)
	summary.TierA = counts.A
	summary.TierB = counts.B
	summary.Removed = len(dropped)
	summary.Pool = p.Registry.Len()
	summary.Probation = p.probationCount()
	summary.MS = p.nowMS() - started

	// 形状必须与 web/app.js 的 probeFromLogs 正则一致（全局约束 7）：
	//   /probe round:\s*(\d+)\/(\d+)\s+alive\s*\(A\s*(\d+)[^0-9]*B\s*(\d+)[^)]*\)\s*in\s*([\d.]+)s/
	logger.Info(fmt.Sprintf(
		"probe round: %d/%d alive (A %d · B %d · 本轮新验 B %d · 实测 %d · 缓存复用 %d · 淘汰 %d · 观察期 %d · 池内剩余 %d) in %.1fs",
		summary.Alive, summary.Scanned, summary.TierA, summary.TierB, summary.NewGated,
		summary.Tested, summary.Cached, summary.Removed, summary.Probation, summary.Pool,
		float64(summary.MS)/1000))
	return summary, nil
}

// probeWorkers 复刻 src/index.js:893 的 workers 公式。
func (p *Parts) probeWorkers(n int) int {
	workers := p.settingsSnapshot().ProbeWorkers
	if q := (n + 3) / 4; q > workers {
		workers = q
	}
	if workers > 128 {
		workers = 128
	}
	if workers < 1 {
		workers = 1
	}
	return workers
}

// probationCount 是还在观察期的节点数(连败 > 0)。走 Registry 的一次性聚合:
// 逐 tag 调 FailCount 是每 tag 一次 RLock 往返(引擎审计 M5),池子几千个
// 节点时一轮探测白付几千次锁竞争。
func (p *Parts) probationCount() int {
	return p.Registry.ProbationCount()
}

// runTierPipeline 对 A 档通关的节点跑 B 档区域探针,返回本轮新验证为 B 的节点数。
//
// B 档探测是真会话、真烧配额,所以它挂在两个闸门后面:全局错峰闸门
// (tierGate,60ms 一个)和按出口 IP 的串行链。已经验明 B 的节点永不重测 ——
// 稳态成本跟着新增节点数走,而不是跟着池子大小走(src/index.js:894-899)。
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
			// 已证 B:不重测(稳态成本跟新增节点走),也**不计入**返回值 ——
			// 日志字段是「本轮新验 B」,把存量也算进去会让面板数字虚高,
			// 观察不到 B 档增长是否真的发生了。
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
	upstream.ApplyFingerprint(body, wire == upstream.WireResponses)
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
	client := httpclient.NewClient(d, time.Duration(tierProbeTimeoutMS)*time.Millisecond)
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
