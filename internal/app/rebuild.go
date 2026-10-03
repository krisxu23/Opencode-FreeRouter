// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 订阅重建与目录刷新。这一份里有四条降级路径,每一条都对应一次真实翻车:
// 订阅全挂时不能清空池子、节点全死时不能拿死节点的出口去拉订阅、目录刷新
// 必须先用节点出口再用直连、限额快照拉不到时要留着上一份而不是显示成零。
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/check"
	"freerouter/internal/health"
	"freerouter/internal/httpclient"
	"freerouter/internal/limits"
	"freerouter/internal/logger"
	"freerouter/internal/parse"
	"freerouter/internal/registry"
	"freerouter/internal/sub"
)

const (
	// subFetchBudget 是订阅拉取的总预算,照抄 src/index.js:412 的
	// AbortSignal.timeout(180_000)。单源 20s × 两轮 × 12 个出口可以远超它,
	// 所以必须有总闸:否则一次全网抖动会把重建拖成几分钟。
	subFetchBudget = 180 * time.Second
	// subExitClientTimeout 不再声明(审计 O6):一个从未被读的预算常量比一个
	// 没接线的旋钮更糟 —— 单源超时实际由 sub.attemptTimeout(20s)与上面的
	// subFetchBudget 总闸决定。
	// subRetryDelay / subRetryLimit 照抄 src/index.js:461-469 的补偿重试。
	subRetryDelay = 20 * time.Second
	subRetryLimit = 2

	// rebuildInterval / limitsInterval 照抄 src/index.js:1103-1104。
	rebuildInterval = 6 * time.Hour
	limitsInterval  = 24 * time.Hour
	// firstProbeDelay 照抄 src/index.js:739-740:重建完 3 秒后立刻补一轮探测,
	// 让面板上的「—」尽快变成一次真实测量。
	firstProbeDelay = 3 * time.Second

	// catalogRetryDelay / catalogRetryLimit 照抄 src/index.js:296-302。
	catalogRetryDelay = 60 * time.Second
	catalogRetryLimit = 5
	// catalogPicks 照抄 src/index.js:266-273 的「随机取 3 个出口」。
	catalogPicks = 3
	// catalogNodeTimeoutMS / catalogDirectTimeoutMS 照抄 src/index.js:280/291。
	catalogNodeTimeoutMS   = 20000
	catalogDirectTimeoutMS = 12000
)

// catalogBox 持有当前一代模型目录。engine 的 State 回调每轮路由读一次它,
// 所以目录换代对在途请求是不可见的 —— 它们用旧的一代跑完。
type catalogBox struct {
	mu   sync.RWMutex
	list []catalog.Model
}

func (b *catalogBox) get() []catalog.Model {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.list
}

func (b *catalogBox) set(list []catalog.Model) {
	b.mu.Lock()
	b.list = list
	b.mu.Unlock()
}

// bootTimeoutMS 不移植(审计 O6):JS 开机等第一轮探测的预算
// Math.min(60000, 5000 + n*10)(src/index.js:658)在 Go 侧没有对应等待 ——
// 开机路径用的是 firstProbeDelay(3s)+ 周期循环,Load 失败路径用的是
// bootJoinTimeout(5s,R12)。留一个算好却没人用的数字,只会让人以为开机有那条
// 上限。

// Rebuild 重拉订阅、热插出站、刷新目录。它不重启任何东西:sing-box 的
// SyncOutbounds 本身就是热插,换出口不需要重启进程,在途连接因此不断。
func (p *Parts) Rebuild(ctx context.Context) error {
	if p.Host == nil || p.Registry == nil {
		return nil
	}
	p.rebuildMu.Lock()
	if p.rebuilding {
		// 已经在重建:记下这次意图,等本轮结束后立刻补一次,而不是并发跑
		// 两轮 —— 两轮并发会让出站集合在 SyncOutbounds 里互相覆盖。
		p.rebuildQueued = true
		p.rebuildMu.Unlock()
		return nil
	}
	p.rebuilding = true
	p.rebuildMu.Unlock()
	defer func() {
		p.rebuildMu.Lock()
		again := p.rebuildQueued
		p.rebuildQueued = false
		p.rebuilding = false
		p.rebuildMu.Unlock()
		if again && ctx.Err() == nil {
			if err := p.Rebuild(ctx); err != nil {
				logger.Warn(fmt.Sprintf("[app] 补重建失败: %v", err))
			}
		}
	}()

	settings := p.settingsSnapshot()
	before := p.Registry.Len()

	picked, fetchErr, dropped := p.fetchSubscriptions(ctx, settings)
	if fetchErr != nil {
		p.noteSubFailure(ctx)
	} else {
		p.subFetchRetries = 0
	}
	if len(picked) == 0 {
		// 订阅一个都没成:沿用注册表里的历史节点,池子原样保留。清空池子
		// 会把一次网络抖动升级成「网关没有出口」。
		picked = p.Registry.All()
		if before == 0 && len(picked) == 0 {
			logger.Info("[app] 未配置订阅或全部拉取失败 — 使用注册表历史节点（0 个）；注册表为空则以纯直连兜底模式启动")
		}
	} else {
		added := p.Registry.Merge(picked)
		p.Registry.EnforceCap(registry.PoolCap)
		if err := p.Registry.Flush(); err != nil {
			logger.Warn(fmt.Sprintf("[app] 注册表落盘失败: %v", err))
		}
		logger.Info(fmt.Sprintf("[app] 订阅：合并 %d 个出口（新增 %d），池内现有 %d 个", len(picked), added, p.Registry.Len()))
	}
	if dropped > 0 {
		logger.Warn(fmt.Sprintf("[app] 订阅里有 %d 个节点 sing-box 无法使用（非法 uuid / 不认的 cipher / 未知传输），未入池", dropped))
	}

	// 出站集合没变就不刷新目录:目录刷新会打一次上游,而「什么都没变」
	// 是稳态下最常见的情况(每 6 小时一次重建)。
	added, removed, syncErr := p.Host.SyncOutbounds(p.Registry.All())
	if syncErr == nil && (added != 0 || removed != 0) {
		// 出站集合被换掉:旧代 client 里绑的拨号闭包指向的是已经被撤下的出站
		// (O3)。失败的 sync 什么都没换,零增删的 sync 同样什么都没换
		// (SyncOutbounds 对已存在的 tag 是纯 no-op,旧 client 的拨号闭包依然
		// 有效)—— 两种情形都不该白丢一整批温热连接池。6 小时一次的周期重建
		// 在稳态下(订阅没变)正是零增删,旧实现每次都把最多 32 个出口的
		// 空闲连接清零,之后的请求全部重新 TCP+TLS 握手。
		p.noteEgressChanged()
	}
	// B11:两路失败都要冒泡 —— 订阅拉不到、出站热插失败。注册表本身已经
	// 按「沿用历史节点」降级处理过,调用方拿到的是「这轮重建有没有全须全尾
	// 地成功」,面板据此给 toast,托盘据此给提示。
	rebuildErr := errors.Join(fetchErr, syncErr)
	p.setRebuildResult(added, removed, dropped, rebuildErr)
	if syncErr != nil {
		logger.Warn(fmt.Sprintf("[app] 热插出站部分失败: %v", syncErr))
	}

	tags := make([]string, 0, p.Registry.Len())
	for _, o := range p.Registry.All() {
		tags = append(tags, o.Tag)
	}
	p.Health.PruneStale(tags)
	if err := p.Health.Persist(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 健康表落盘失败: %v", err))
	}
	if added != 0 || removed != 0 {
		logger.Info(fmt.Sprintf("rebuild ok: %d nodes(热插 %d, 撤下 %d)", p.Registry.Len(), added, removed))
		p.refreshCatalog(ctx)
	}
	p.afterFunc(firstProbeDelay, func() {
		if ctx.Err() != nil {
			return
		}
		if _, err := p.ProbeNow(ctx, false); err != nil {
			// 已有一轮在跑:首探是尽力而为,不排队。
			return
		}
	})
	return rebuildErr
}

// fetchSubscriptions 拉取并筛选订阅。返回值:picked 是整形后的出站、fetchErr
// 表示「一个源都没拉到」(要用缓存/历史节点;非 nil 才算失败)、dropped 是被
// sing-box 拒收的节点数。
//
// B11:从前这里回的是 bool,而 Rebuild 无论订阅成不成、出站热插有没有报错都
// `return nil`。于是面板「刷新」永远 toast 成功、托盘 Reload 永远静默 ——
// 哪怕订阅全军覆没。现在把失败原样交出去,由 Rebuild 聚合后上报。
func (p *Parts) fetchSubscriptions(ctx context.Context, settings Settings) ([]parse.Outbound, error, int) {
	if len(settings.SubURLs) == 0 {
		return nil, nil, 0
	}
	exits := p.subExits()
	budget, cancel := context.WithTimeout(ctx, subFetchBudget)
	defer cancel()
	res, err := sub.Fetch(budget, settings.SubURLs, exits)
	if err != nil {
		logger.Warn(fmt.Sprintf("[app] 订阅失败，沿用 %d 个已知出口: %v", p.Registry.Len(), err))
		return nil, err, 0
	}
	picked := parse.FilterByGroups(res.Outbounds, settings.Countries)
	clean := make([]parse.Outbound, 0, len(picked))
	dropped := 0
	for _, o := range picked {
		sanitized, ok := parse.SanitizeOutbound(o)
		if !ok {
			dropped++
			continue
		}
		clean = append(clean, sanitized)
	}
	return clean, nil, dropped
}

// subExits 是拉订阅时可以借用的出口。只取健康表判活的节点,且最多
// check.SubRetryExits 个。
//
// 为什么不用「所有已知出口」兜底:那些出口属于已经被判死的节点,12 次尝试
// 会各等 20s(最多四分钟),然后日志把责任推给订阅源,而真正的原因是
// 「一个活出口都没有」。池子空的时候正确答案是纯直连
// (src/index.js:393-400)。
func (p *Parts) subExits() []sub.Exit {
	exits := make([]sub.Exit, 0, check.SubRetryExits)
	for _, o := range p.Registry.All() {
		if len(exits) >= check.SubRetryExits {
			break
		}
		if p.Health.HealthOf(o.Tag) != health.StateAlive {
			continue
		}
		d, err := p.Host.Dialer(o.Tag)
		if err != nil {
			continue
		}
		exits = append(exits, sub.Exit{Name: o.Tag, Dial: d})
	}
	return exits
}

// noteSubFailure 是订阅失败的补偿重试:20 秒后再来一次,最多两次。
// 首次启动时订阅常常比出口池先就绪,等本实例就绪后经健康出口复拉往往就通了
// (src/index.js:458-469)。
func (p *Parts) noteSubFailure(ctx context.Context) {
	if p.subFetchRetries >= subRetryLimit {
		return
	}
	p.subFetchRetries++
	attempt := p.subFetchRetries
	logger.Warn(fmt.Sprintf("[app] 订阅拉取失败 — %s 后自动重试（第 %d/%d 次，等本实例就绪后经健康出口复拉）",
		subRetryDelay, attempt, subRetryLimit))
	p.afterFunc(subRetryDelay, func() {
		if ctx.Err() != nil {
			return
		}
		if err := p.Rebuild(ctx); err != nil {
			logger.Warn(fmt.Sprintf("[app] 订阅补偿重试失败: %v", err))
		}
	})
}

// setRebuildResult 记录最近一次热插的结果,面板的「上次重建」与「上次检查」都显示
// 它。dropped 是订阅里被 sing-box 拒收的节点数 —— 前端 checkBadge 的「剔除 N 个
// 坏节点」和 checkAlert 的整句都读它,不记就等于那条告警永远不出现。
func (p *Parts) setRebuildResult(added, removed, dropped int, err error) {
	p.rebuildStateMu.Lock()
	defer p.rebuildStateMu.Unlock()
	p.lastAdded = added
	p.lastRemoved = removed
	p.lastDropped = dropped
	p.lastRebuildAt = p.nowMS()
	p.lastRebuildOK = err == nil
	if err != nil {
		p.lastRebuildErr = err.Error()
	} else {
		p.lastRebuildErr = ""
	}
}

// refreshCatalog 刷新模型目录,顺序是承重的:先用节点出口试,再走直连。
// 2026-10-03 之后的第三条路(models.dev 覆盖层兜底)已删:那张 24h 快照混着
// 上游已下架的 id,曾把目录毒成 33 个免费模型;上游全挂时目录保持现状并重试。
//
// 翻车史(src/index.js:274-287):原来的顺序是反的,于是直连被墙的机器上
// 每一轮都要先白等 12 秒,而且一旦直连成功就再也不试节点出口 —— 控制面
// 永久绕过了代理,日志里留下过 `catalog: 12 free models via direct`。
func (p *Parts) refreshCatalog(ctx context.Context) {
	if p.refreshCatalogAttempt(ctx, 0) {
		return
	}
}

// refreshCatalogAttempt 返回是否成功。失败时按 catalogRetryLimit 递归重试。
func (p *Parts) refreshCatalogAttempt(ctx context.Context, attempt int) bool {
	if ctx.Err() != nil {
		return true // 关停中:不算失败,也不再重试
	}
	ids := p.catalogViaNodeExit(ctx)
	via := ""
	if len(ids) > 0 {
		via = "node exit"
	} else {
		ids = p.catalogViaDirect(ctx)
		if len(ids) > 0 {
			via = "direct"
		} else {
			// 不再用 models.dev 覆盖层兜底(2026-10-03 裁决):那张 24h 快照
			// 混着上游已下架的 id,曾把目录毒成 33 个免费模型。上游全挂时
			// 目录保持现状(冷启动即静态表),只排重试。
			logger.Warn(fmt.Sprintf("[app] catalog refresh failed（节点出口与直连都不可用）; retry %d/%d in %s",
				minInt(attempt+1, catalogRetryLimit), catalogRetryLimit, catalogRetryDelay))
			if attempt < catalogRetryLimit {
				p.afterFunc(catalogRetryDelay, func() {
					p.refreshCatalogAttempt(ctx, attempt+1)
				})
			}
			return false
		}
	}
	p.applyIDs(ids, via)
	return true
}

// catalogViaNodeExit 随机取几个存活出口去问上游模型列表。
func (p *Parts) catalogViaNodeExit(ctx context.Context) []string {
	all := p.Registry.All()
	if len(all) == 0 {
		return nil
	}
	order := rand.Perm(len(all))
	if len(order) > catalogPicks {
		order = order[:catalogPicks]
	}
	for _, i := range order {
		tag := all[i].Tag
		d, err := p.Host.Dialer(tag)
		if err != nil {
			continue
		}
		if ids := p.fetchIDs(ctx, d, catalogNodeTimeoutMS); len(ids) > 0 {
			logger.Info(fmt.Sprintf("[app] catalog: %d free models via node %s", len(ids), shortTag(tag)))
			return ids
		}
	}
	return nil
}

// catalogViaDirect 走直连问一次。它是第二选择,不是第一选择。
func (p *Parts) catalogViaDirect(ctx context.Context) []string {
	d, err := p.Host.Dialer(p.Host.DirectTag())
	if err != nil {
		return nil
	}
	if ids := p.fetchIDs(ctx, d, catalogDirectTimeoutMS); len(ids) > 0 {
		logger.Info(fmt.Sprintf("[app] catalog: %d free models via direct", len(ids)))
		return ids
	}
	return nil
}

// fetchIDs 拉一次上游模型清单。失败返回 nil,让调用方去试下一个出口。
func (p *Parts) fetchIDs(ctx context.Context, d httpclient.Dialer, timeoutMS int) []string {
	pctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, p.base()+"/zen/v1/models", nil)
	if err != nil {
		return nil
	}
	client := httpclient.NewClient(d, time.Duration(timeoutMS)*time.Millisecond)
	resp, err := client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil
	}
	ids, err := catalog.ParseListing(raw)
	if err != nil {
		return nil
	}
	return ids
}

// applyIDs 是目录换代的唯一入口:对着上一份基线打增删对比日志,记住这一代
// 的 id 列表(限额覆盖层要拿它重套),换掉 engine 读到的那一代,把基线落盘,
// 然后打一行日志。覆盖层在 Build 之后套上 —— JS 的 applyIds 同序
// (src/index.js:263-266):models.dev 的 -free 行才带真实额度,canonical 行
// 在某些模型上大 5 倍。
//
// 基线(data/catalog-ids.json)在 2026-10-03 之后只有一个职责:下次启动的
// 对比基准。它不是目录来源 —— 冷启动恒播静态表;models.dev 快照曾借兜底
// 路径把 20+ 个上游已下架的 id 写进目录(面板恒 33 个免费模型),那条路已删。
func (p *Parts) applyIDs(ids []string, via string) {
	next := append([]string(nil), ids...)
	p.baselineMu.Lock()
	prev := p.baselineIDs
	p.baselineIDs = next
	p.baselineMu.Unlock()
	if prev == nil {
		logger.Info(fmt.Sprintf("[app] catalog 基线首次建立: %d 个 id", len(next)))
	} else if added, removed := diffIDs(prev, next); len(added) > 0 || len(removed) > 0 {
		logger.Info(fmt.Sprintf("[app] catalog 对比上次运行: 新增 %s, 摘除 %s",
			summarizeIDs(added), summarizeIDs(removed)))
	}
	list := p.applyOverlay(catalog.Build(ids))
	p.catalog.set(list)
	p.upstreamMu.Lock()
	p.lastUpstreamIDs = next
	p.upstreamMu.Unlock()
	// 基线落盘,下次启动才有 diff 基准(JS saveCatalogCache,src/index.js:248
	// + :264)。写失败只 warn:目录换代本身已经成功,基线进不去不该让正在跑
	// 的网关报错 —— 最坏代价是下次启动少打一行对比。
	if err := catalog.SaveCache(p.catalogCacheFile(), ids, time.Now().UnixMilli()); err != nil {
		logger.Warn(fmt.Sprintf("[app] 目录基线落盘失败（继续运行）: %v", err))
	}
	logger.Info(fmt.Sprintf("[app] catalog: %d free models via %s", len(list), via))
}

// diffIDs 以集合语义对比两代 id 列表:added 在 next 不在 prev,removed 相反;
// 各自保持输入里的首次出现顺序,同一列表内部的重复元素按一次计。
func diffIDs(prev, next []string) (added, removed []string) {
	prevSet := make(map[string]bool, len(prev))
	for _, id := range prev {
		prevSet[id] = true
	}
	nextSet := make(map[string]bool, len(next))
	for _, id := range next {
		nextSet[id] = true
	}
	seen := make(map[string]bool)
	for _, id := range next {
		if !prevSet[id] && !seen[id] {
			seen[id] = true
			added = append(added, id)
		}
	}
	seen = make(map[string]bool)
	for _, id := range prev {
		if !nextSet[id] && !seen[id] {
			seen[id] = true
			removed = append(removed, id)
		}
	}
	return added, removed
}

// summarizeIDs 把 diff 列表压进一行日志:最多列 8 个,多的报个数。
func summarizeIDs(ids []string) string {
	const maxListed = 8
	if len(ids) <= maxListed {
		return strings.Join(ids, ", ")
	}
	return fmt.Sprintf("%s …等共 %d 个", strings.Join(ids[:maxListed], ", "), len(ids))
}

// applyOverlay 把覆盖层的容量值按精确 id 套到目录行上
// (src/limits.js applyLimitsOverlay 逐字语义):
//   - contextWindow / maxOutput:覆盖层有该 id 就覆盖(Zen 的 -free 行);
//   - reasoning:覆盖层声明了布尔就跟随;
//   - vision:本地探测过,不动 —— 除非该行是本地表没见过的回退行
//     (131072/32768 且 vision=false),此时覆盖层的 attachment 位更好。
func (p *Parts) applyOverlay(list []catalog.Model) []catalog.Model {
	p.overlayMu.Lock()
	byID := p.overlayByID
	p.overlayMu.Unlock()
	return ApplyOverlay(list, byID)
}

// ApplyOverlay 是 Parts.applyOverlay 的纯函数内核:同样的覆盖语义,但不读任何
// Parts 状态。独立成导出函数是因为差分验收(difftest,任务 27)必须把 JS 版
// applyLimitsOverlay 的同份输入喂给这条**生产**逻辑 —— 而装配一个完整 Parts
// 才能走到私有方法,会让差分固件背上整个网关的启动条件。
//
// 回退行的判定必须读**覆盖前**的容量值(src/limits.js:137 的 isFallback 读的是
// entry 而不是 next):normalizeRow 要求覆盖行恒带正的 context/maxOutput,若先
// 覆盖再判定,131072/32768 这个「本地表没见过」的标记就永远不成立,vision 位
// 对恰恰最需要它的 models.dev-only 模型永不生效(差分验收前夜单测抓到的实错)。
func ApplyOverlay(list []catalog.Model, byID map[string]limits.OverlayRow) []catalog.Model {
	if len(byID) == 0 {
		return list
	}
	out := make([]catalog.Model, 0, len(list))
	for _, m := range list {
		if over, ok := byID[m.ID]; ok {
			isFallback := limits.FallbackMarked(m.ContextWindow, m.MaxOutput, m.Vision)
			if over.ContextWindow > 0 {
				m.ContextWindow = over.ContextWindow
			}
			if over.MaxOutput > 0 {
				m.MaxOutput = over.MaxOutput
			}
			if over.Reasoning != nil {
				m.Reasoning = *over.Reasoning
			}
			if over.Vision != nil && isFallback {
				m.Vision = *over.Vision
			}
		}
		out = append(out, m)
	}
	return out
}

// refreshLimitsOverlay 刷新 models.dev 覆盖层并重套目录数值。JS 的两条分派
// (src/index.js:327-352)在 2026-10-03 剪掉了一条 —— 覆盖层快照混着上游已
// 下架的 id,不再能撑目录:
//   - 有 upstream 列表 → build 后套覆盖层数值;
//   - 没有 upstream 列表 → 目录保持现状(冷启动即静态表),等下一轮目录刷新;
//   - 抓取失败 → 保留上一份覆盖层只把 stale 置真(清空会让面板把
//     「暂时查不到」显示成「额度全满」)。
//
// 抓取**只走直连**,从不经节点(src/limits.js 文件头:「direct, never via a
// node」——这张表是公开静态资源,烧一个出口的配额去换它毫无收益)。
// 返回值恒 nil:JS 的 refreshLimitsOverlay 也从不向面板抛错,失败语义全部
// 表达在 stale 与日志里。
func (p *Parts) refreshLimitsOverlay(ctx context.Context) error {
	now := p.now()
	p.overlayMu.Lock()
	byID := p.overlayByID
	p.overlayMu.Unlock()
	// B8:fetchedAt 与 limitsRows/limitsStale 是同一份状态,必须同一把锁 ——
	// 从前这里在 overlayMu 下读、在 limitsMu 下写,两把锁各自「正确」合起来
	// 不构成互斥,面板与 OverlayStale 判定会读到撕裂的 fetchedAt。
	p.limitsMu.Lock()
	fetchedAt := p.limitsFetchedAt
	p.limitsMu.Unlock()

	var fetchErr string
	if limits.OverlayStale(fetchedAt, now) {
		payload, err := p.fetchOverlay(ctx)
		if err != nil {
			fetchErr = err.Error()
		} else if extracted := limits.ExtractOpencodeOverlay(payload); len(extracted) > 0 {
			byID = extracted
			fetchedAt = now.UnixMilli()
			if serr := limits.SaveOverlayCache(p.overlayCacheFile(), byID, fetchedAt); serr != nil {
				logger.Warn(fmt.Sprintf("[app] 覆盖层缓存落盘失败: %v", serr))
			}
		} else {
			fetchErr = "empty opencode overlay"
		}
	}

	p.overlayMu.Lock()
	p.overlayByID = byID
	p.overlayMu.Unlock()
	p.limitsMu.Lock()
	p.limitsRows = len(byID)
	p.limitsFetchedAt = fetchedAt
	p.limitsStale = fetchErr != ""
	p.limitsMu.Unlock()

	if fetchErr != "" && len(byID) == 0 {
		logger.Warn(fmt.Sprintf("[app] limits overlay refresh failed (%s) — serving local CAPABILITIES", fetchErr))
		return nil
	}
	p.upstreamMu.Lock()
	haveUpstream := len(p.lastUpstreamIDs) > 0
	ids := append([]string(nil), p.lastUpstreamIDs...)
	p.upstreamMu.Unlock()
	if haveUpstream {
		p.catalog.set(p.applyOverlay(catalog.Build(ids)))
	}
	// 没有 upstream 列表时到此为止:目录 id 的唯一来源是上游列表(2026-10-03
	// 裁决),覆盖层的快照 id 不再能撑目录 —— 保持现状(静态表),等下一轮
	// 目录刷新成功后整体换代。
	logger.Info(fmt.Sprintf("[app] limits overlay: %d opencode rows%s%s",
		len(byID), staleSuffix(fetchErr), errSuffix(fetchErr)))
	return nil
}

func staleSuffix(fetchErr string) string {
	if fetchErr == "" {
		return ""
	}
	return " (stale cache)"
}

func errSuffix(fetchErr string) string {
	if fetchErr == "" {
		return ""
	}
	return fmt.Sprintf(" (fetch failed: %s)", fetchErr)
}

// fetchOverlay 直连抓一次 models.dev 快照(30s 预算,照 src/limits.js timeoutMs)。
func (p *Parts) fetchOverlay(ctx context.Context) ([]byte, error) {
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodGet, limits.OverlayURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	client := httpclient.NewClient(nil, 30*time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 8<<20))
}

// overlayCacheFile 是覆盖层磁盘缓存(data/modelsdev.json,JS
// LIMITS_CACHE_FILE 同名)。
func (p *Parts) overlayCacheFile() string {
	return filepath.Join(p.Root, "data", "modelsdev.json")
}

// catalogCacheFile 是「上次运行见过的 id」基线(data/catalog-ids.json,
// JS CATALOG_CACHE_FILE 同名同形状;2026-10-03 起只作换代对比,不作目录来源)。
func (p *Parts) catalogCacheFile() string {
	return filepath.Join(p.Root, "data", "catalog-ids.json")
}

// PanelURL 是托盘「打开面板」与单实例守卫共用的唯一 URL 来源。
// 端口优先取**实际绑定**的监听地址:报告「正在服务」的那个端口,而不是设置
// 里声称的端口 —— 运行中改端口已被 ApplySettings 拒收,但两者曾经过一轮
// 「设置已写、监听未动」的分裂期,以 listener 为准永远不会再错。没有 listener
// (Load 未走完/测试夹具)时回落设置值。
func (p *Parts) PanelURL() string {
	if p == nil || !p.hasSettings() {
		return ""
	}
	if p.panelLn != nil {
		return fmt.Sprintf("http://127.0.0.1:%d/", portOf(p.panelLn))
	}
	return fmt.Sprintf("http://127.0.0.1:%d/", p.settingsSnapshot().PanelPort)
}

// Reload 是托盘「重启网关」的动作:重读 settings.json(用户可能在面板上
// 改过订阅与间隔),重建出站,刷新目录。它**不重启进程**、不重绑转发端口,
// 因此在途连接不断。sing-box 的 SyncOutbounds 已经是热插,换出口不需要重启。
func (p *Parts) Reload(ctx context.Context) error {
	if p.settingsStore == nil {
		return fmt.Errorf("app: 设置存储未装配")
	}
	if err := p.settingsStore.Load(); err != nil {
		// 解析失败时绝不写回 p.Settings:把运行中的网关降级成默认配置比
		// 保留一份旧设置糟得多。
		return fmt.Errorf("app: 重读设置: %w", err)
	}
	next, err := settingsFromStore(p.settingsStore)
	if err != nil {
		return err
	}
	p.setSettings(next)
	// 顺序固定:先设置,再重建出站,最后刷新目录。反过来会让目录与新出站
	// 代际错配一轮 —— 目录决定哪些模型可用,而出站决定它们经谁出去。
	if err := p.Rebuild(ctx); err != nil {
		return err
	}
	p.refreshCatalog(ctx)
	return nil
}

// shortTag 把出口 tag 截到 32 字符:出口 tag 是整条 URI,日志行会被它淹没。
func shortTag(tag string) string {
	if len(tag) <= 32 {
		return tag
	}
	return tag[:32]
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// jsonNumberOrNil 把 settings 里的 defaultMaxTokens 变成 adapter 要的 int。
// null 是「不加额外上限」的线上形式,必须存成 nil 而不是删掉键
// (src/index.js:1039-1059)。
func jsonNumberOrNil(v any) (int, bool) {
	switch n := v.(type) {
	case nil:
		return 0, false
	case float64:
		if n <= 0 {
			return 0, false
		}
		return int(n), true
	case int:
		if n <= 0 {
			return 0, false
		}
		return n, true
	case json.Number:
		i, err := n.Int64()
		if err != nil || i <= 0 {
			return 0, false
		}
		return int(i), true
	default:
		return 0, false
	}
}

// trimAll 去掉空白并丢掉空串。面板上多打一个回车不该变成一个空订阅源。
func trimAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}
