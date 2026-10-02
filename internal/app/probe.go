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
	probeAccidentMin  = 8   // src/index.js:796 样本太小时不判事故
	probeCacheRatio   = 0.5 // src/index.js:816

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

	Skipped    bool   // 探测源不通,整轮跳过
	Accident   bool   // 探测源事故(alive 掉一半以上)
	SourceAddr string // 探测用的直连出口,诊断用
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
		if _, err := p.ProbeNow(context.Background(), rerunForce); err != nil {
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
	cycleMin := p.Settings.ProbeIntervalMin
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
	summary := ProbeSummary{At: started, SourceAddr: "direct"}

	if p.Settings == nil || !p.Settings.ProbeEnabled {
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
	for _, o := range pool {
		if !force && p.Health.ProbedWithin(o.Tag, windowMS) {
			summary.Cached++
			if p.Health.HealthOf(o.Tag) == health.StateAlive {
				aliveTags[o.Tag] = true
			}
			continue
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

	summary.Tested = len(items)
	probedTags := make(map[string]bool, len(items))
	for _, it := range items {
		probedTags[it.Tag] = true
	}
	results := p.Prober.ProbeAll(ctx, items, p.probeWorkers(len(items)))
	for _, r := range results {
		p.Health.MarkProbe(r.Tag, r.Result)
		if r.Result.State == nodeprobe.StateAlive {
			aliveTags[r.Tag] = true
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
	zeroAlive := summary.Scanned > 0 && len(aliveTags) == 0
	accident := ratioHit || zeroAlive
	summary.Accident = accident
	if ratioHit {
		logger.Error(fmt.Sprintf("探测源疑似事故：上一轮存活的 %d 个节点本轮掉了 %d 个（%.0f%% > %.0f%%）— 本轮不淘汰任何节点，保留现有池子",
			len(prevAlive), lostAlive,
			float64(lostAlive)/float64(len(prevAlive))*100, probeAccidentRate*100))
	} else if zeroAlive {
		logger.Error(fmt.Sprintf("探测源疑似事故：本轮 %d 个节点 0 个通关（直连门已通过，说明不是本机断网）— 本轮不淘汰任何节点，保留现有池子",
			summary.Scanned))
	}

	// 计败:本轮真的测过、又没通关的节点各记一次。缓存跳过的不在此列 ——
	// 缓存的语义是「同一个观测只算一次」,否则 MAX_FAILS=3 会被缓存加速一倍。
	now := p.now()
	for _, it := range items {
		if aliveTags[it.Tag] {
			continue
		}
		p.Registry.NoteFail(it.Tag, now)
	}

	// alive 名单 = 通关的 + 连败还没到门槛的。Go 把 JS retainOnly 里耦在一起的
	// 「计败」与「判决」拆成两步,正值的门槛必须由调用方在这里执行;事故轮传
	// 真正的 aliveTags(不能传空集,那会把本轮真通关的节点也记成一次失败),
	// 只把 MaxFails 抬成 -1(Infinity,只记账不淘汰)。
	aliveList := make([]string, 0, len(aliveTags))
	for tag := range aliveTags {
		aliveList = append(aliveList, tag)
	}
	maxFails := registry.MaxFails
	if accident {
		maxFails = -1
	} else {
		for _, it := range items {
			if aliveTags[it.Tag] {
				continue
			}
			if p.Registry.FailCount(it.Tag) < registry.MaxFails {
				aliveList = append(aliveList, it.Tag)
			}
		}
	}
	dropped := p.Registry.RetainOnly(aliveList, registry.RetainOpts{
		ProbedTags: probedTags,
		MaxFails:   maxFails,
	})
	for _, tag := range dropped {
		p.Health.Forget(tag)
	}
	p.Registry.EnforceCap(registry.PoolCap)
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
	workers := p.Settings.ProbeWorkers
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

// probationCount 是还在观察期的节点数(连败 > 0)。registry 没有
// ProbationCount(),因为「观察期」是探测轮的措辞,不是池子的固有属性。
func (p *Parts) probationCount() int {
	n := 0
	for _, o := range p.Registry.All() {
		if p.Registry.FailCount(o.Tag) > 0 {
			n++
		}
	}
	return n
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
			gated.Add(1) // 已证 B:计入本轮新验数,但不重测
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
