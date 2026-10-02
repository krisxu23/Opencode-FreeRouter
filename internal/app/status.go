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

// Version 是控制台显示的产品版本。来源与 package.json 的 version 字段一致
// (阶段 5 任务 29 改为 1.0.0 后同步这里)。
const Version = "0.4.5"

// ---- 时钟与定时器接缝 ----
//
// 测试不能真的等 5 分钟或 6 小时:now/afterFunc/wait 走 Parts 上的可替换字段,
// 生产路径零开销(time.Now / time.AfterFunc / time.After)。

func (p *Parts) now() time.Time {
	if p.clockFn != nil {
		return p.clockFn()
	}
	return time.Now()
}

func (p *Parts) nowMS() int64 { return p.now().UnixMilli() }

func (p *Parts) afterFunc(d time.Duration, fn func()) *time.Timer {
	p.timersWG.Add(1)
	wrapped := func() {
		defer p.timersWG.Done()
		fn()
	}
	if p.afterFuncFn != nil {
		return p.afterFuncFn(d, wrapped)
	}
	return time.AfterFunc(d, wrapped)
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
	minutes := p.Settings.ProbeIntervalMin
	if minutes < 5 {
		minutes = 5
	}
	return time.Duration(minutes) * time.Minute
}

// StartTimers 起三个周期任务:探测、订阅重建、限额覆盖层刷新
// (间隔照抄 src/index.js:1102-1104)。它们都阻塞在 firstFetch 上再进第一轮:
// 宁可晚几秒,也不要对着空池子空转。
func (p *Parts) StartTimers(ctx context.Context) {
	if p.firstFetch != nil {
		p.timersWG.Add(1)
		go func() {
			defer p.timersWG.Done()
			select {
			case <-ctx.Done():
				return
			case <-p.firstFetch:
			}
			p.warmUp(ctx)
			p.probeLoop(ctx)
		}()

		p.timersWG.Add(1)
		go func() {
			defer p.timersWG.Done()
			select {
			case <-ctx.Done():
				return
			case <-p.firstFetch:
			}
			p.rebuildLoop(ctx)
		}()
	} else {
		p.timersWG.Add(2)
		go func() { defer p.timersWG.Done(); p.probeLoop(ctx) }()
		go func() { defer p.timersWG.Done(); p.rebuildLoop(ctx) }()
	}
	p.timersWG.Add(1)
	go func() { defer p.timersWG.Done(); p.limitsLoop(ctx) }()
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
			b, err := json.Marshal(view)
			if err == nil {
				var m map[string]any
				if json.Unmarshal(b, &m) == nil {
					for k, v := range m {
						row[k] = v
					}
				}
			}
		}
		nodes = append(nodes, row)
	}

	return map[string]any{
		"singbox":      singbox,
		"forward":      map[string]any{"running": true, "port": p.Settings.ForwardPort, "key": p.Settings.ForwardKey},
		"models":       ids,
		"modelCaps":    caps,
		"limits":       limits,
		"regionModels": regionModels,
		"nodes":        nodes,
		"usage":        p.usageView(),
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

// SettingsView 是 /api/settings 的响应:settings 的面板子集(照抄
// src/panel.js:171-184 的键清单,但删掉 portBase/portSpan/catchAllPort)。
// forwardKey 绝不进这个视图 —— 设置页没有显示它的需求,测试请求走的是
// Status() 的 forward.key。
func (p *Parts) SettingsView() map[string]any {
	s := *p.Settings
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
		return *p.Settings, nil
	}
	clean, err := validateSettingsPatch(patch)
	if err != nil {
		return *p.Settings, err
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
		return *p.Settings, err
	}
	p.settingsStore.Update(clean)
	if err := p.settingsStore.Flush(); err != nil {
		// 落盘失败要把内存里的补丁撤回,否则内存与磁盘各说各话。
		p.settingsStore.Update(before)
		return *p.Settings, fmt.Errorf("app: 写设置: %w", err)
	}
	*p.Settings = next
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
