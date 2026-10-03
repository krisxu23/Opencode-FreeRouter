// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 状态快照、设置读写与三个周期定时器。单独成文件是因为形状即契约:这里的每
// 一个 JSON 键名都会被逐字搬运来的 web/app.js 消费,拼错一个字母,面板上就
// 是一块安静的空白而不是一行报错。

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"time"

	"freerouter/internal/health"
	"freerouter/internal/logger"
	"freerouter/internal/panel"
	"freerouter/internal/parse"
	"freerouter/internal/stats"
	"freerouter/internal/upstream"
)

// Version 是控制台显示的产品版本。它由构建脚本从 package.json 的 version
// 字段注入(-X freerouter/internal/app.Version=…,见 scripts/go-build.mjs),
// 所以面板标题永远跟着 package.json 走,不会再像 B12 那样停在手写的 0.4.5。
//
// 必须是 var 而不是 const:-X 只作用于变量。裸 `go build`/`go test`(不经过
// 构建脚本)拿到的是下面的开发默认值。
var Version = "0.0.0-dev"

// ---- 时钟与定时器接缝 ----
//
// 测试不能真的等 5 分钟或 6 小时:now/afterFunc/wait 走 Parts 上的可替换字段,
// 生产路径零开销(time.Now / time.AfterFunc / time.After)。

// ctx 是后台任务的根 context(B9)。定时器回调里绝不能再写
// context.Background():关停之后那样会以全新 context 重入,继续写盘、打日志。
// 零值 Parts(Load 失败后)没有 lifeCtx,回落到 Background 只为让调用方不必
// 到处判空 —— 那种 Parts 上不会有定时器被排出来。
func (p *Parts) ctx() context.Context {
	if p.lifeCtx != nil {
		return p.lifeCtx
	}
	return context.Background()
}

func (p *Parts) now() time.Time {
	if p.clockFn != nil {
		return p.clockFn()
	}
	return time.Now()
}

func (p *Parts) nowMS() int64 { return p.now().UnixMilli() }

// timerSlot 是 p.timers 的一格。所有字段只在 p.timersMu 下读写。
//
// 三个位而不是一个 *time.Timer,是因为「谁负责归还 timersWG 计数」有三种终局:
//   - 回调跑了(wrapped 自己 Done)→ fired;
//   - 被关停抢先 Stop 掉(Stop 返回 true,回调不会再跑)→ 由 stopPendingTimers
//     归还 → stopped;
//   - 接缝吞掉了定时器(没有 timer 可停、也没有回调会来)→ 由 afterFunc 归还。
//
// 谁归还只能有一个答案,否则 timersWG 要么挂死要么被 Done 超次(panic)。
type timerSlot struct {
	t       *time.Timer
	fired   bool
	stopped bool
	dropped bool
}

// afterFunc 排一个延时回调,并把它登记进 p.timers 以便关停时统一停掉。
//
// 登记用 timerSlot 这一层间接(整分支评审 RISK-5),不是直接把 *time.Timer 存进
// 表里,原因有两个:
//   - 旧写法里回调读的是主线程那个尚未被同步赋值的 `self` 变量 —— 与
//     rememberTimer 的写之间没有任何同步边,是形式上的数据竞态;而
//     internal/app 恰好是唯一跑不了 -race 的包(sing-box v1.14 自身的竞态淹没
//     了它),所以这条竞态在 CI 里永远不可见。
//   - 更要紧的是次序:回调若在登记之前跑完,forgetTimer(self) 拿到 nil 直接早退,
//     随后 rememberTimer 把一个**已经触发过**的定时器追加进表 —— 它再没有摘除
//     点,p.timers 就随探测/重建轮次只长不消,正是 B9 要防的那类不回收。
//
// slot 在排程之前就进表,回调按格摘除,attach 时若发现这一格已被摘掉(回调跑过
// 或已被关停停掉)就不再登记;关停之后排程的一律当场 Stop,绝不留悬格。
func (p *Parts) afterFunc(d time.Duration, fn func()) *time.Timer {
	p.timersWG.Add(1)
	slot := &timerSlot{}
	p.timersMu.Lock()
	p.timers = append(p.timers, slot)
	closed := p.timersClosed
	p.timersMu.Unlock()

	wrapped := func() {
		defer p.timersWG.Done()
		p.timersMu.Lock()
		slot.fired = true
		p.dropTimerLocked(slot)
		p.timersMu.Unlock()
		fn()
	}

	var t *time.Timer
	if closed {
		// 关停已经收过表:这一发排下去也没人再停它,直接判为不再跑。
		p.timersWG.Done()
		p.timersMu.Lock()
		p.dropTimerLocked(slot)
		p.timersMu.Unlock()
		return nil
	}
	if p.afterFuncFn != nil {
		t = p.afterFuncFn(d, wrapped)
	} else {
		t = time.AfterFunc(d, wrapped)
	}

	p.timersMu.Lock()
	if slot.fired || slot.stopped {
		// 回调已经跑过(接缝同步调用、或 0 延时抢先),或者这一格已经被摘除:
		// 表里不能再留它。nil 接缝(swallowTimers)走的是同一条路 —— 没有可 Stop
		// 的东西,也没有回调来摘它。
		if t == nil {
			slot.stopped = true // 计数已由 wrapped 归还(接缝同步跑过)或无人归还
		}
		p.timersMu.Unlock()
		return t
	}
	if t == nil {
		// 测试接缝吞掉了定时器。**计数归还权始终属于回调那一侧**:接缝可能像
		// swallowTimers 那样把 fn 扣下来稍后手工调用,在这里抢先 Done 会让那次
		// 手工调用二次归还 → panic(negative WaitGroup counter)。
		// 所以只摘格子(没有任何东西能再 Stop 它),不碰计数。
		slot.stopped = true
		p.dropTimerLocked(slot)
		p.timersMu.Unlock()
		return nil
	}
	slot.t = t
	p.timersMu.Unlock()
	return t
}

// dropTimerLocked 把一格从登记表里摘掉(调用方已持 timersMu)。摘掉即封口:
// 之后 attach 不会把它放回去。
func (p *Parts) dropTimerLocked(slot *timerSlot) {
	if slot.dropped {
		return
	}
	slot.dropped = true
	for i, cur := range p.timers {
		if cur == slot {
			p.timers = append(p.timers[:i], p.timers[i+1:]...)
			return
		}
	}
}

// stopPendingTimers 停掉所有还没触发的定时器,并替它们把 timersWG 的计数还上。
//
// 关键点:Stop 返回 true 表示回调不会再跑,于是 wrapped 里的 Done 永远不会执行。
// 计数是 Add 在排定时器时加的,所以必须由这里补 Done,否则随后的 timersWG.Wait
// 会一直挂到 ctx 超时为止(B9 的另一半)。
//
// 顺手把表标成已关闭:关停之后再有代码排定时器(重跑回调、漏网的 goroutine),
// afterFunc 会当场把它停掉并归还计数,而不是留一格永远没人摘的悬位。
func (p *Parts) stopPendingTimers() {
	p.timersMu.Lock()
	pending := p.timers
	p.timers = nil
	p.timersClosed = true
	p.timersMu.Unlock()
	for _, slot := range pending {
		if slot.t == nil {
			continue
		}
		if slot.t.Stop() {
			// Stop 抢到在回调之前:这一格的 Done 由这里归还。
			p.timersWG.Done()
		}
	}
}

// wait 返回一个在 d 后闭合的通道。probeTicker 间隔每轮重读
// (面板改 probeIntervalMin 立即生效),所以是逐轮 wait 而不是固定 Ticker。
// wait 返回一个在 d 后闭合的通道。probeTicker 间隔每轮重读
// (面板改 probeIntervalMin 立即生效),所以是逐轮 wait 而不是固定 Ticker。
// waitFn 是测试接缝:注入后三个循环的节拍完全由测试驱动。
func (p *Parts) wait(d time.Duration) <-chan time.Time {
	if p.waitFn != nil {
		return p.waitFn(d)
	}
	return time.After(d)
}

// base 是上游根地址。OUR_FREE_MODEL_BASE 覆盖它 —— 与 JS 的
// upstream.js:24 同一开关;catalog 刷新与 B 档探针都从这里拼 URL,
// 测试把它指向本地 httptest 服务。
func (p *Parts) base() string {
	if v := os.Getenv("OUR_FREE_MODEL_BASE"); v != "" {
		return v
	}
	return upstream.UpstreamBase
}

// probeInterval 是探测周期:max(5, probeIntervalMin) 分钟。下限 5 分钟来自
// src/index.js:1102 —— 更密的探测只会烧配额、把出口 IP 打成 429,不会让
// 池子更健康。
func (p *Parts) probeInterval() time.Duration {
	minutes := p.settingsSnapshot().ProbeIntervalMin
	if minutes < 5 {
		minutes = 5
	}
	return time.Duration(minutes) * time.Minute
}

// StartTimers 起三个周期任务:探测、订阅重建、限额覆盖层刷新
// (间隔照抄 src/index.js:1102-1104)。它们都阻塞在 firstFetch 上再进第一轮:
// 宁可晚几秒,也不要对着空池子空转。
//
// 传入的 ctx 是调用方的生命周期信号(生产里是 main 的 signal.NotifyContext)。
// Load 已经建好了 lifeCtx,这里只把它桥接起来:调用方 ctx 一旦取消,p.cancel
// 就取消 lifeCtx,三个循环与所有重跑定时器一起退出。测试/嵌入场景直接造 Parts
// (没有经过 Load)时 lifeCtx 为空,这里就地建一个,行为一致。
func (p *Parts) StartTimers(ctx context.Context) {
	if p.lifeCtx == nil {
		life, cancel := context.WithCancel(ctx)
		p.lifeCtx = life
		p.cancel = cancel
	} else if p.cancel != nil {
		// Stop 函数随 AfterFunc 一起留着 —— 丢掉它会让 ctx 一直持有这个回调,
		// 直到 ctx 自己被回收为止。
		_ = context.AfterFunc(ctx, p.cancel)
	}
	life := p.lifeCtx

	if p.firstFetch != nil {
		p.timersWG.Add(1)
		go func() {
			defer p.timersWG.Done()
			select {
			case <-life.Done():
				return
			case <-p.firstFetch:
			}
			p.warmUp(life)
			p.probeLoop(life)
		}()

		p.timersWG.Add(1)
		go func() {
			defer p.timersWG.Done()
			select {
			case <-life.Done():
				return
			case <-p.firstFetch:
			}
			p.rebuildLoop(life)
		}()
	} else {
		p.timersWG.Add(2)
		go func() { defer p.timersWG.Done(); p.probeLoop(life) }()
		go func() { defer p.timersWG.Done(); p.rebuildLoop(life) }()
	}
	p.timersWG.Add(1)
	go func() { defer p.timersWG.Done(); p.limitsLoop(life) }()
}

// warmUp 是开场订阅落定与周期循环之间的那段:刷新模型目录,并在 firstProbeDelay
// 后跑首轮探测。JS 版这两步挂在启动那次 rebuild 的尾巴上(src/index.js:1099 的
// refreshCatalog、:1101 await rebuild() 走到 :736 的 setTimeout(probeNow, 3000));
// Go 版把开场拉取内联进了 Build(app.go 步骤 4 —— 监听端口不该等订阅),那条路径
// 不经过 Rebuild,于是两个尾巴一起丢了:面板只吃 data/catalog-ids.json 的磁盘缓存,
// 节点状态要等满一个探测周期(实测 probeIntervalMin=30 就是 30 分钟)才出现第一轮实测。
//
// 用 wait 而不是 afterFunc:后者在 timersWG 上记一笔、只有回调真跑完才 Done,于是
// 一个被 ctx 取消掉的定时器会把 Wait 挂到超时为止。
func (p *Parts) warmUp(ctx context.Context) {
	// 首探的定时器先挂上,再刷目录。原先 refreshCatalog 同步跑在定时器之前,而它
	// 最坏要等 3 个节点出口各 20s 再加直连 12s(约 72s),期间 StartTimers 里紧跟
	// 其后的 probeLoop 也被一并挡住 —— 面板因此在开机后一分多钟里什么都不显示。
	p.timersWG.Add(1)
	go func() {
		defer p.timersWG.Done()
		select {
		case <-ctx.Done():
			return
		case <-p.wait(firstProbeDelay):
		}
		// 与 Rebuild 的首探同口径:force=false,新鲜结果不重测。已有轮在跑就放弃
		// —— 首探是尽力而为,排队只会让它变成紧接着的第二轮全量实测。
		_, _ = p.ProbeNow(ctx, false)
	}()
	p.timersWG.Add(1)
	go func() {
		defer p.timersWG.Done()
		p.refreshCatalog(ctx)
	}()
}

func (p *Parts) probeLoop(ctx context.Context) {
	for {
		interval := p.probeInterval()
		select {
		case <-ctx.Done():
			return
		case <-p.wait(interval):
		}
		if ctx.Err() != nil {
			return
		}
		// ProbeNow 自己用 probing 标志串行化:上一轮没跑完时这一轮只记
		// 重跑意图,绝不并发 —— 两轮并发会让淘汰判决互相覆盖。
		if _, err := p.ProbeNow(ctx, false); err != nil {
			logger.Info(fmt.Sprintf("[app] 定时探测跳过: %v", err))
		}
	}
}

func (p *Parts) rebuildLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wait(rebuildInterval):
		}
		if ctx.Err() != nil {
			return
		}
		if err := p.Rebuild(ctx); err != nil {
			logger.Warn(fmt.Sprintf("[app] 定时重建失败: %v", err))
		}
	}
}

func (p *Parts) limitsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.wait(limitsInterval):
		}
		if ctx.Err() != nil {
			return
		}
		if err := p.refreshLimitsOverlay(ctx); err != nil {
			logger.Warn(fmt.Sprintf("[app] 定时限额刷新失败: %v", err))
		}
	}
}

// ---- 状态快照 ----
//
// 形状逐字段对照 src/index.js:1060-1085 的 status() —— web/app.js 的 derive()
// 按这些键名取数,多一个少一个都是面板上的静默空白。

// Status 是喂给面板与 /api/status 的完整快照。
//
// forward.key 会出现在这里:控制台监听 127.0.0.1,JS 版同样把它交给本地面板
// (src/index.js:1072),设置页的「测试请求」按钮靠它带 Authorization。计划
// 正文写「绝不回显 key」并称 JS 也没回显 —— 那半句与 JS 源码相反,按 JS 裁决
// (修正案阶段 4 条目)。
func (p *Parts) Status() any {
	// 一次加锁读全部重建状态:lastRebuild.added/removed 原先在锁外裸读,和
	// setRebuildResult 的写构成数据竞争(-race 下会报)。
	lastRebuildAt, lastOK, lastErr, dropped, lastAdded, lastRemoved, mode := func() (int64, bool, string, int, int, int, string) {
		p.rebuildStateMu.Lock()
		defer p.rebuildStateMu.Unlock()
		mode := ""
		// 纯直连兜底:跑过重建但池子空着,且本轮没有热插任何出站。
		if p.lastRebuildAt != 0 && p.Registry.Len() == 0 && p.lastAdded == 0 && p.lastRemoved == 0 {
			mode = "direct"
		}
		return p.lastRebuildAt, p.lastRebuildOK, p.lastRebuildErr, p.lastDropped, p.lastAdded, p.lastRemoved, mode
	}()

	// lastCheck 的形状对齐 src/index.js:1070:ok 为 null 表示「还没跑过重建」,
	// 前端 checkBadge 对 ok==null 返回空串(不显示任何徽标)。冷启动路径现在会
	// 记这一次(app.go 步骤 6.5),所以开机几秒后这里就有值了。
	var okValue any
	if lastRebuildAt != 0 {
		okValue = lastOK
	}

	singbox := map[string]any{
		"running": true, // 零端口架构:sing-box 与本进程同生死,进程在即 running
		"pid":     os.Getpid(),
		"lastCheck": map[string]any{
			"ok":        okValue,
			"dropped":   dropped,
			"nodes":     p.Registry.Len(), // 前端 checkAlert 的「N 个节点正常启用」读它
			"probation": p.probationCount(),
			"at":        lastRebuildAt,
			"mode":      mode,
			"error":     lastErr,
		},
		"lastRebuild": map[string]any{
			"ok":      lastOK,
			"added":   lastAdded,
			"removed": lastRemoved,
			"at":      lastRebuildAt,
		},
	}

	models := p.catalog.get()
	ids := make([]string, 0, len(models))
	caps := map[string]any{}
	for _, m := range models {
		ids = append(ids, m.ID)
		caps[m.ID] = map[string]any{
			"contextWindow": m.ContextWindow,
			"maxOutput":     m.MaxOutput,
		}
	}

	p.limitsMu.Lock()
	limits := map[string]any{
		"rows":      p.limitsRows,
		"fetchedAt": p.limitsFetchedAt,
		"stale":     p.limitsStale,
	}
	p.limitsMu.Unlock()

	region := p.Health.RegionSnapshot()
	regionModels := make([]any, 0, len(region.Models))
	for _, m := range region.Models {
		regionModels = append(regionModels, m)
	}

	// nodes = 注册表(=本代池子)逐节点一行,健康行从内存表取,缺席按 unknown。
	// JS 逐字搬运的前端按 n.state/n.latencyMs/n.country/n.tier 取数;零端口
	// 架构没有 per-node 端口,NodeRow 不带 port(节点表的 port 列恒显示 —,
	// 这是任务 23 登记过的已知差异)。
	snap := p.Health.NodeSnapshot()
	nodes := make([]any, 0, p.Registry.Len())
	for _, o := range p.Registry.All() {
		row := map[string]any{
			"tag":       o.Tag,
			"country":   parse.CountryOf(o.Tag),
			"state":     string(health.StateUnknown),
			"latencyMs": -1,
		}
		if view, ok := snap[o.Tag]; ok {
			// O2:这里从前对每个节点做一次 Marshal + Unmarshal(每 5 秒一轮,
			// 池上限 8000 个节点 = 一万六千次 JSON 往返),只为把行并进 map。
			// MergeInto 逐键复刻 json 的输出,相等性钉在 health 包的测试里。
			view.MergeInto(row)
		}
		nodes = append(nodes, row)
	}

	// B7:端口与密钥必须来自同一份快照,不能两次裸读 —— 两次读之间隔着一次
	// 面板 PUT 就会回出「旧端口配新 key」这种从未存在过的组合。
	cur := p.settingsSnapshot()

	// R7 + O9:落盘失败此前是死信息。stats 的 LastError() 在注释里被承诺给
	// /api/status(:186 与 :366 两处),但状态里根本没有它的位置;注册表/健康表
	// 的 Flush 失败也只进日志。这里给它们一个统一的出口,面板与运维脚本就能
	// 在网关「看起来一切正常」时看到「写盘一直在失败」。
	diagnostics := map[string]any{
		"stats":        map[string]any{"lastError": ""},
		"subscription": map[string]any{"lastError": lastErr},
	}
	if p.StatsStore != nil {
		diagnostics["stats"] = map[string]any{"lastError": p.StatsStore.LastError()}
	}

	return map[string]any{
		"singbox":      singbox,
		"forward":      map[string]any{"running": true, "port": cur.ForwardPort, "key": cur.ForwardKey},
		"models":       ids,
		"modelCaps":    caps,
		"limits":       limits,
		"regionModels": regionModels,
		"nodes":        nodes,
		"usage":        p.usageView(),
		"diagnostics":  diagnostics,
	}
}

// usageView 换算 stats 落盘形状 → 前端形状(src/index.js:1078-1083):
// today 是 UTC 今天,history 是升序 7 天。stats.Bucket 的 JSON 标签
// (req/in/out)与前端一致,直接复用。
func (p *Parts) usageView() map[string]any {
	var snap stats.Snapshot
	var history []stats.HistoryRow
	if p.StatsStore != nil {
		snap = p.StatsStore.Snapshot()
		// usageHistory(st, 7):升序 7 天,缺日补零(src/index.js:1027-1037)。
		history = p.StatsStore.History(usageHistoryDays, p.nowMS())
	}
	if history == nil {
		history = []stats.HistoryRow{}
	}
	today := p.now().UTC().Format("2006-01-02")
	todayBucket, ok := snap.Days[today]
	if !ok {
		todayBucket = stats.Bucket{}
	}
	return map[string]any{
		"today":    todayBucket,
		"requests": snap.Requests,
		"byModel":  snap.Models,
		"history":  history,
	}
}

// usageHistoryDays 照抄 src/index.js:178 USAGE_HISTORY_DAYS。
const usageHistoryDays = 7

// ---- 设置视图与写入 ----

// settingsSnapshot 返回当前设置的**副本**。所有读点都必须走这里,不能直接
// 解引用 p.Settings(B7):面板 PUT 与托盘 Reload 会整结构覆写它,裸读会看到
// 撕裂值 —— 半个 int、旧端口配新 key、字符串头半更新。副本是值语义,
// 调用方拿到后随便读多久都不会再变。
func (p *Parts) settingsSnapshot() Settings {
	p.settingsMu.RLock()
	defer p.settingsMu.RUnlock()
	if p.Settings == nil {
		// 零值 Parts:Load 失败后的 Shutdown 与测试里的空结构体都会走到。
		return Settings{}
	}
	return *p.Settings
}

// hasSettings 报告设置是否已装配。Load 失败后的 Parts 与零值 Parts 上是 false,
// 调用方靠它区分「没装配」与「装配了但都是零值」。
func (p *Parts) hasSettings() bool {
	p.settingsMu.RLock()
	defer p.settingsMu.RUnlock()
	return p.Settings != nil
}

// setSettings 是设置整结构覆写的唯一入口(ApplySettings 与 Reload)。
// Settings 为 nil 时分配一块,否则原地覆盖 —— 保留指针身份,夹具里
// `p.Settings.X = …` 的直接赋值仍然有效。
func (p *Parts) setSettings(next Settings) {
	p.settingsMu.Lock()
	defer p.settingsMu.Unlock()
	if p.Settings == nil {
		p.Settings = &next
		return
	}
	*p.Settings = next
}

// SettingsView 是 /api/settings 的响应:settings 的面板子集(照抄
// src/panel.js:171-184 的键清单,但删掉 portBase/portSpan/catchAllPort)。
// forwardKey 绝不进这个视图 —— 设置页没有显示它的需求,测试请求走的是
// Status() 的 forward.key。
func (p *Parts) SettingsView() map[string]any {
	s := p.settingsSnapshot()
	return map[string]any{
		"subUrls":          s.SubURLs,
		"countries":        s.Countries,
		"probeEnabled":     s.ProbeEnabled,
		"probeWorkers":     s.ProbeWorkers,
		"probeIntervalMin": s.ProbeIntervalMin,
		"effortLevel":      s.EffortLevel,
		"defaultMaxTokens": s.DefaultMaxTokens,
		"forwardPort":      s.ForwardPort,
		"panelPort":        s.PanelPort,
	}
}

// ApplySettings 应用面板发来的设置补丁并落盘。
//
// 只收白名单键:forwardKey 不在清单里,伪造的补丁改不掉密钥。defaultMaxTokens
// 的 null 是「不额外设限」的线上形式,必须原样存进 store 而不是删键 —— 浅层
// merge 下删键会让旧值活过整个往返,字段永远清不掉(src/index.js:1047-1054)。
// 端口字段的改动要重启后生效(转发端口已绑定,运行中重绑会断在途连接);
// 其余字段下一轮探测/重建立即可见。
//
// B10:校验必须发生在落盘之前。面板不认证(panel.go 头注释自认),本机任何进程
// 都能 PUT 一个畸形补丁;旧实现先 Update+Flush 再 settingsFromStore,于是
// {"probeWorkers":"abc"} 会把 settings.json 写成不可解析 —— 下次启动 Load 失败,
// 网关拒绝启动,只能手改文件。现在的顺序是「构造候选 → 逐键类型校验 → 候选能解
// 成 Settings → 才 Update+Flush」,任一步失败都直接返回,一个字节都不写盘。
func (p *Parts) ApplySettings(patch map[string]any) (Settings, error) {
	if len(patch) == 0 {
		return p.settingsSnapshot(), nil
	}
	clean, err := validateSettingsPatch(patch)
	if err != nil {
		return p.settingsSnapshot(), err
	}
	// 候选校验:补丁 merge 进当前快照,先确认结果能被解成 Settings。类型
	// 断言挡不住的组合(例如超大整数)在这里落网。
	before, _ := p.settingsStore.Get().(map[string]any)
	candidate := make(map[string]any, len(before)+len(clean))
	for k, v := range before {
		candidate[k] = v
	}
	for k, v := range clean {
		candidate[k] = v
	}
	next, err := settingsFromMap(candidate)
	if err != nil {
		return p.settingsSnapshot(), err
	}
	p.settingsStore.Update(clean)
	if err := p.settingsStore.Flush(); err != nil {
		// 落盘失败要把内存里的补丁撤回,否则内存与磁盘各说各话。
		p.settingsStore.Update(before)
		return p.settingsSnapshot(), fmt.Errorf("app: 写设置: %w", err)
	}
	p.setSettings(next)
	return next, nil
}

// validateSettingsPatch 对白名单键做逐键类型校验并归一化。任何非法类型都是
// 错误,绝不静默跳过 —— 静默跳过正是 B10 的前半段(非法原值留在 clean 里被写进
// store)。返回的 map 只含通过校验的键,值已归一化成 Settings 字段的 Go 类型。
func validateSettingsPatch(patch map[string]any) (map[string]any, error) {
	clean := map[string]any{}
	for _, key := range []string{
		"subUrls", "countries", "probeEnabled", "probeWorkers", "probeIntervalMin",
		"effortLevel", "defaultMaxTokens", "forwardPort", "panelPort",
	} {
		v, ok := patch[key]
		if !ok {
			continue
		}
		switch key {
		case "subUrls", "countries":
			list, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("app: 设置 %s 必须是字符串数组", key)
			}
			out := make([]string, 0, len(list))
			for _, e := range list {
				s, ok := e.(string)
				if !ok {
					return nil, fmt.Errorf("app: 设置 %s 的元素必须是字符串", key)
				}
				out = append(out, s)
			}
			clean[key] = trimAll(out)
		case "probeEnabled":
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("app: 设置 probeEnabled 必须是布尔值")
			}
			clean[key] = b
		case "effortLevel":
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("app: 设置 effortLevel 必须是字符串")
			}
			clean[key] = s
		case "defaultMaxTokens":
			// null 是「不额外设限」;非正数照 src/index.js:1047-1054 归一成
			// null,而不是把 0 或负数留在盘上。
			if v == nil {
				clean[key] = nil
				continue
			}
			n, ok := settingsInt(v)
			if !ok {
				return nil, fmt.Errorf("app: 设置 defaultMaxTokens 必须是数字或 null")
			}
			if n <= 0 {
				clean[key] = nil
			} else {
				clean[key] = n
			}
		case "probeWorkers", "probeIntervalMin", "forwardPort", "panelPort":
			n, ok := settingsInt(v)
			if !ok {
				return nil, fmt.Errorf("app: 设置 %s 必须是整数", key)
			}
			if n < 0 {
				return nil, fmt.Errorf("app: 设置 %s 不能是负数", key)
			}
			if key == "forwardPort" || key == "panelPort" {
				if n > 65535 {
					return nil, fmt.Errorf("app: 设置 %s 超出端口范围", key)
				}
			}
			clean[key] = n
		}
	}
	return clean, nil
}

// settingsInt 把 JSON 数字转成 int。只接受可无损转换的整数:48.5 会被
// json.Unmarshal 拒绝进 Settings 的 int 字段,正是 B10 的崩溃路径之一,必须在
// 这里就拦下。
func settingsInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) {
			return 0, false
		}
		if n < -(1<<53) || n > 1<<53 {
			return 0, false
		}
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

// LimitsView 是 /api/limits 的摘要面板读回的那一半(刷新在 refreshLimitsOverlay)。
func (p *Parts) LimitsView() panel.LimitsView {
	p.limitsMu.Lock()
	defer p.limitsMu.Unlock()
	return panel.LimitsView{Rows: p.limitsRows, Stale: p.limitsStale}
}
