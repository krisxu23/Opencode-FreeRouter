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
	"freerouter/internal/sub"
)

const (
	// subRetryDelay / subRetryLimit 照抄 src/index.js:461-469 的补偿重试。
	subRetryDelay = 20 * time.Second
	subRetryLimit = 2

	// limitsInterval 照抄 src/index.js:1103-1104。订阅刷新间隔 1.3.0 起改为
	// 面板可设的 refreshIntervalMin(默认 30 分钟),不再是写死的 6 小时。
	limitsInterval = 24 * time.Hour
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

// subFetchBudget 是订阅拉取的总预算,照抄 src/index.js:412 的
// AbortSignal.timeout(180_000)。单源 20s × 两轮 × 12 个出口可以远超它,
// 所以必须有总闸:否则一次全网抖动会把重建拖成几分钟。
// 是 var 而非 const 只为测试可收缩(app_opening_test.go 把它钳到毫秒级,验证
// 总闸真的接在 sub.Fetch 上;八审 M8),生产代码不得改写。
var subFetchBudget = 180 * time.Second

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

// errRebuildQueued 是「重建已在进行,本轮请求已排队补跑」的哨兵。它必须是
// error 而不是 nil:排上的这一手还没跑过,回 nil 会让面板把「已排队」显示
// 成「刷新成功」(第六轮审计)。调用方(面板 /api/refresh、托盘 Reload)把
// 它当非致命提示处理:文案自带答案。
var errRebuildQueued = errors.New("重建已在进行中,本轮请求已排队补跑")

// Rebuild 重拉订阅、热插出站、刷新目录。它不重启任何东西:sing-box 的
// SyncOutbounds 本身就是热插,换出口不需要重启进程,在途连接因此不断。
func (p *Parts) Rebuild(ctx context.Context) error {
	if p.Host == nil || p.Registry == nil {
		return nil
	}
	// queued 短路不谎报成功(第六轮审计):排上队的这一手**还没有跑过**,
	// 把它直接回成 nil 会让面板把「已排队」显示成「刷新成功」。返回一个
	// 可识别的错误,调用方(app 面板 Actions.Refresh、托盘 Reload)照旧记
	// 日志/提示,语义诚实。
	p.rebuildMu.Lock()
	if p.rebuilding {
		p.rebuildQueued = true
		p.rebuildMu.Unlock()
		// 八审 L11:关停窗口里的「已排队补跑」是谎报。面板动作跑在 lifeCtx 上
		// (rebuildOnce 的关停短路注释),ctx 已取消意味着下面循环 :132 的关停
		// 检查注定把这一手丢弃 —— 哨兵承诺的「已排队,会补跑」永远不会兑现,
		// 而调用方(面板/托盘)把哨兵按已受理的提示处理。此时必须回普通错误,
		// 不能回哨兵。
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("app: 关停中,排队的一轮重建未执行: %w", cerr)
		}
		return errRebuildQueued
	}
	p.rebuilding = true
	p.rebuildMu.Unlock()
	// 补重建用循环不用递归:持续被 queue 时 defer 内递归栈深度无界。
	//
	// 返回值以**末轮**为准(第七轮复审更正):此前把各轮错误 Join 累积,结果是
	// 「首轮失败 + 补跑轮成功」时 Rebuild 抛错而 setRebuildResult 已被补跑轮
	// 覆写成 OK —— 同一事实在 /api/status(成功)与 /api/refresh(500 红字)
	// 两处结论相反。轮换语义下补跑轮重做了同一件事,世界已收敛,返回值必须与
	// 落盘的状态记录同源。前轮失败**不静默**:noteSubFailure 已经为每一失败轮
	// 打过 Warn,这里再补一条说明它已被后续轮覆盖。
	var lastErr error
	for {
		if err := p.rebuildOnce(ctx); err != nil {
			if lastErr != nil {
				logger.Warn(fmt.Sprintf("[app] 上一轮重建失败但已被本轮的排队补跑覆盖: %v", lastErr))
			}
			lastErr = err
		} else {
			lastErr = nil
		}
		p.rebuildMu.Lock()
		again := p.rebuildQueued
		p.rebuildQueued = false
		if again && ctx.Err() != nil {
			// 哨兵承诺的是「已排队补跑」,而关停把它丢了 —— 不能再让调用方
			// 按已受理处理(托盘/面板会显示成功,实际这一手没跑)。
			p.rebuilding = false
			p.rebuildMu.Unlock()
			return errors.Join(lastErr, fmt.Errorf("app: 关停中,排队的一轮重建未执行: %w", ctx.Err()))
		}
		if !again {
			p.rebuilding = false
			p.rebuildMu.Unlock()
			break
		}
		// rebuilding 保持 true,下一轮继续。
		p.rebuildMu.Unlock()
	}
	return lastErr
}

// rebuildOnce 是一轮重建本体(旧 Rebuild 去掉递归外壳)。
//
// R2:不拿 rebuildMu 做「已在重建」短路。上一轮把递归改循环时在这里加了一道
// 「p.rebuilding → 记 queued → return nil」,但 Rebuild 循环本身已持有
// rebuilding(第 109 行置 true)、本轮 rebuildOnce 是自己人 —— 于是第二轮及
// 之后的每一轮都走这条短路直接返回,循环永远只跑第一轮:补重建悄悄不补,
// 面板/托盘的「刷新」在并发触发时静默丢轮。互斥语义全部收进 Rebuild,本函数
// 只管跑一轮,不做任何状态判断。
func (p *Parts) rebuildOnce(ctx context.Context) error {
	if p.Host == nil || p.Registry == nil {
		return nil
	}

	settings := p.settingsSnapshot()
	before := p.Registry.Len()

	picked, present, fetchErr, dropped, filtered, sourcesOK, sourcesTotal := p.fetchSubscriptions(ctx, settings)
	if fetchErr != nil {
		p.noteSubFailure(ctx)
	} else {
		p.subFetchRetries.Store(0)
	}
	// 关停窗口短路:面板动作跑在 lifeCtx 上,让它们止步的是 p.cancel() ——
	// Shutdown 注释里「Panel.Close 取消在途动作」对它们不成立(Close 只取消
	// 以 r.Context() 运行的请求)。SyncOutbounds/Flush/Persist 都不看 ctx,
	// 不在这里拦:托盘退出会被大池热插拖住几十秒,退出后仍在写盘。
	if err := ctx.Err(); err != nil {
		return errors.Join(fetchErr, err)
	}
	if len(picked) == 0 {
		// 订阅一个都没成:沿用注册表里的历史节点,池子原样保留。清空池子
		// 会把一次网络抖动升级成「网关没有出口」。
		picked = p.Registry.All()
		if before == 0 && len(picked) == 0 {
			logger.Info("[app] 未配置订阅或全部拉取失败 — 使用注册表历史节点（0 个）；注册表为空则以纯直连兜底模式启动")
		}
	} else {
		// 成员资格跟随订阅:**全员到齐才允许差集删**。九审(2026-10-05):
		// 部分源失败时 present 只是残缺名单,拿它判「不在任何源里=已下架」,
		// 一轮「30 源只成 1 个」的拉取就把 4328 个节点(含全部活节点)清了
		// 场——健康行一起 Forget,热区归零后 subExits 借不到出口复拉,下一
		// 轮源更拉不到,恶性循环直到「几千节点零活」。下架判定必须建立在
		// 「名单完整」之上;缺源这轮只合并,下架留给名单完整的一轮。
		//
		// 订阅下架的节点平时只靠 coldPass 三振,关探测则永久残留占池位和
		// 探测预算 —— 所以不是干脆不删,而是**只在可信名单下删**。
		//
		// **先删、后 Merge** —— 与 boot 路径(app.go 的 629→649)逐字同序,
		// 这个次序是承重的:Merge 按**身份**去重而差集删按 **tag** 判,订阅商
		// 改名节点(US-01→US-02,免费池常态)时,若先 Merge,新 tag 因身份被
		// 还占着池的旧 tag 挡住、被当别名跳过不入池;随后旧 tag 不在 present
		// 被删 —— 该节点整轮从池中消失,健康判决(B 档/延迟/streak)一起清零,
		// 下轮才以新 tag 复活重走首探。先删则旧身份当场让位,新 tag 同一轮
		// 入池,成员不断。
		// 判据仍是对**过滤前**的原始源算 present:被用户地区选择滤掉的节点
		// 仍在订阅里,不删 —— 与 boot 同口径。
		pruned := 0
		if present != nil && sourcesOK == sourcesTotal {
			for _, o := range p.Registry.All() {
				if !present[o.Tag] {
					if p.Registry.Remove(o.Tag) {
						p.Health.Forget(o.Tag)
						pruned++
					}
				}
			}
		}
		added := p.Registry.Merge(picked)
		// 2026-10-06:池上限取消,不再做健康感知淘汰 —— 有多少节点进多少。
		// 烂水回收靠 coldPass 三振 + NeverAlive 早删。EnforceCapRanked 机制
		// 保留在 registry 包里备用。
		if err := p.Registry.Flush(); err != nil {
			logger.Warn(fmt.Sprintf("[app] 注册表落盘失败: %v", err))
		}
		// 九审:部分源失败的合并轮要留下可诊断的痕迹,不然「池子没缩水但
		// 名单残缺」这件事在日志里无影无踪。
		if present != nil && sourcesOK != sourcesTotal {
			logger.Warn(fmt.Sprintf("[app] 订阅部分源失败（%d/%d 个源成功）— 本轮只合并不下架,池内现有 %d 个", sourcesOK, sourcesTotal, p.Registry.Len()))
		}
		logger.Info(fmt.Sprintf("[app] 订阅：合并 %d 个出口（新增 %d，下架 %d，过滤丢弃 %d），池内现有 %d 个", len(picked), added, pruned, filtered, p.Registry.Len()))
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
	// 热插进行中关停的第二道闸:上面那道拦不住「SyncOutbounds 已在跑」的
	// 窗口,后续的 PruneStale/Persist 同样不该在退出流程里继续写盘。
	if err := ctx.Err(); err != nil {
		return errors.Join(fetchErr, err)
	}
	rebuildErr := errors.Join(fetchErr, syncErr)
	p.setRebuildResult(added, removed, dropped, filtered, rebuildErr)
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
	// 1.3.0:重建完成后 nudge 首探循环 —— 新入池节点(没有健康行的)立即
	// 走全量首探,不必等 firstProbeLoop 的 30s 节拍。
	p.afterFunc(firstProbeDelay, func() {
		if ctx.Err() != nil {
			return
		}
		p.nudgeFirst()
	})
	return rebuildErr
}

// fetchSubscriptions 拉取并筛选订阅。返回值:picked 是整形后的出站、fetchErr
// 表示「一个源都没拉到」(要用缓存/历史节点;非 nil 才算失败)、dropped 是被
// sing-box 拒收的节点数、filtered 是地区/类型过滤丢弃的节点数(面板可见,免得
// "几万变几千"无迹可查);sourcesOK/sourcesTotal 是成功/配置的**源**数 ——
// 九审:差集删只在 sourcesOK == sourcesTotal(全员到齐)时执行,部分源失败
// 时 present 残缺,只能合并不能下架。
//
// B11:从前这里回的是 bool,而 Rebuild 无论订阅成不成、出站热插有没有报错都
// `return nil`。于是面板「刷新」永远 toast 成功、托盘 Reload 永远静默 ——
// 哪怕订阅全军覆没。现在把失败原样交出去,由 Rebuild 聚合后上报。
func (p *Parts) fetchSubscriptions(ctx context.Context, settings Settings) ([]parse.Outbound, map[string]bool, error, int, int, int, int) {
	if len(settings.SubURLs) == 0 {
		return nil, nil, nil, 0, 0, 0, 0
	}
	exits := p.subExits()
	budget, cancel := context.WithTimeout(ctx, subFetchBudget)
	defer cancel()
	res, err := sub.Fetch(budget, settings.SubURLs, exits)
	if err != nil {
		logger.Warn(fmt.Sprintf("[app] 订阅失败，沿用 %d 个已知出口: %v", p.Registry.Len(), err))
		return nil, nil, err, 0, 0, 0, len(settings.SubURLs)
	}
	// 2026-10-06:重名 tag 消歧。免费聚合订阅里成千上万个不同节点共用
	// 同一个名字（"EPODONIOS" x7207），Registry 以 tag 为主键会原位覆盖，
	// 1.5 万节点只剩 214 进池。消歧必须在 present 名单构建之前，
	// 否则下架判据（按 tag 差集）会把消歧后的条目当"不在订阅里"删掉。
	res.Outbounds = parse.DisambiguateTags(res.Outbounds)
	// present 是订阅原始全量的 tag 集(过滤前):差集删除的判据。被用户地区
	// 选择过滤掉的节点仍在订阅里,不删 —— 与 boot 路径同口径。
	present := make(map[string]bool, len(res.Outbounds))
	for _, o := range res.Outbounds {
		present[o.Tag] = true
	}
	// 九审:present 的可信度取决于源覆盖面。部分源失败时它只是残缺名单 ——
	// 拿残缺名单判「不在任何源里=已下架」,一次网络抖动就会把整个池子清场
	// (2026-10-05 实测:30 源只成 1 个,4328 节点含全部活节点被删,热区
	// 归零后借不到出口复拉,恶性循环)。所以源成功数必须透传给调用方裁决。
	picked := parse.FilterByGroups(res.Outbounds, settings.Countries)
	// filtered:过滤丢了多少。res.Outbounds 与 picked 都按同一 IdentityOf 去重,
	// 差值就是地区/类型过滤的丢弃数 —— 面板 lastCheck.filtered 展示它。
	filtered := len(res.Outbounds) - len(picked)
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
	return clean, present, nil, dropped, filtered, res.SourcesOK, res.SourcesTotal
}

// openingSubscription 是 Load 第 6.5 步的 goroutine 体:开机后台拉一轮订阅,
// 结果记进 lastCheck/lastRebuild,并按订阅成员资格整理池子。firstFetch 的
// close 与 bootWG.Done 仍归 goroutine 的 defer 管,本方法不碰。
//
// 八审 M8:拉取段此前是内联副本(裸 ctx + 注册表全量拨号出口 + 自抄一份
// 过滤/清洗),与周期轮的 fetchSubscriptions 三处漂移:
//   - 没有 subFetchBudget 总闸:订阅源挂死时这一轮跟着挂死,bootJoin 只是不再
//     等它,goroutine 照烧 12 出口 × 20s × 源数;
//   - 出口不筛活:死出口逐一烧满 attemptTimeout —— 正是 subExits 注释里写明
//     不做的形状;
//   - 过滤/清洗逻辑第二份拷贝,改一处漏一处。
//
// 现在整段复用 fetchSubscriptions:开场轮与周期轮对「订阅拉取」只有落点不同
// —— 这里把结果记进开机账本并按成员资格整理池子,周期轮走 rebuildOnce。
func (p *Parts) openingSubscription(ctx context.Context, cur Settings) {
	if len(cur.SubURLs) == 0 {
		logger.Info(fmt.Sprintf("[app] 未配置订阅或全部拉取失败 — 使用注册表历史节点（%d 个）；注册表为空则以纯直连兜底模式启动", p.Registry.Len()))
		p.setRebuildResult(0, 0, 0, 0, nil)
		return
	}
	clean, present, ferr, dropped, filtered, sourcesOK, sourcesTotal := p.fetchSubscriptions(ctx, cur)
	if ferr != nil {
		p.setRebuildResult(0, 0, 0, 0, ferr)
		return
	}
	// 成员资格跟随订阅(1.3.0):拉取成功后,不在**任何源**里的节点
	// 删干净(注册表 + 健康行,零记录)。删除判定对原始源算 —— 被
	// 用户地区选择过滤掉的节点仍在订阅里,不删。
	// 九审:与 rebuildOnce 同一条闸门 —— **全员到齐才允许差集删**。
	// 部分源失败时 present 残缺,拿它判下架会一次清空整个池子
	// (boot 轮清场比周期轮更狠:它没有任何「沿用历史」护栏以外的
	// 恢复点,健康行一起没)。缺源这轮只合并。
	if present != nil && sourcesOK == sourcesTotal {
		for _, o := range p.Registry.All() {
			if !present[o.Tag] {
				_ = p.Registry.Remove(o.Tag)
				p.Health.Forget(o.Tag)
			}
		}
	} else if present != nil {
		logger.Warn(fmt.Sprintf("[app] 开机订阅部分源失败（%d/%d 个源成功）— 本轮只合并不下架", sourcesOK, sourcesTotal))
	}
	// 落盘前复核 ctx(生命周期审计 #3):sub.Fetch 是网络等待,这期间 Load
	// 可能已因端口占用而失败,fail() 会 cancel() 让 ctx 进入取消态。Fetch
	// 在取消前恰好成功返回时,旧的写法照样往下走 —— reg.Flush 会把这一轮
	// 订阅结果写进注册表文件,而进程随即退出:一次**从未成功启动**的运行
	// 在磁盘上留下了注册表,下次开机继承它。joinBoot 只保证「返回之后不再
	// 写」,管不了「失败之后仍在写」。取消的一轮什么都不写:池子原样留给
	// 下一次真正的开机。
	if ctx.Err() != nil {
		logger.Info("[app] 开机订阅已完成拉取但启动已中止 — 本轮不写注册表,池子保持磁盘现状")
		return
	}
	merged := p.Registry.Merge(clean)
	// 2026-10-06:池上限取消,开场不再做健康感知淘汰(与周期 Rebuild 同口径)。
	if err := p.Registry.Flush(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 注册表落盘失败: %v", err))
	}
	syncAdded, syncRemoved, serr := p.Host.SyncOutbounds(p.Registry.All())
	if serr == nil && (syncAdded != 0 || syncRemoved != 0) {
		// 出站换了代:旧代 client 里绑的是上一代的拨号闭包(O3)。零增删
		// 的 sync 是纯 no-op(SyncOutbounds 对已存在的 tag 不 Remove 不
		// 重建,旧 client 的拨号闭包依然有效),换代只会白扔全部出口的
		// 温热连接池 —— 与 noteEgressChanged 注释声明的语义一致。
		p.noteEgressChanged()
	}
	if dropped > 0 {
		logger.Warn(fmt.Sprintf("[app] 订阅里有 %d 个节点 sing-box 无法使用（非法 uuid / 不认的 cipher / 未知传输），未入池", dropped))
	}
	if serr != nil {
		logger.Warn(fmt.Sprintf("[app] 热插出站失败: %v", serr))
		p.setRebuildResult(syncAdded, syncRemoved, dropped, filtered, serr)
		return
	}
	p.setRebuildResult(syncAdded, syncRemoved, dropped, filtered, nil)
	logger.Info(fmt.Sprintf("[app] 订阅：合并 %d 个出口（新增 %d），池内现有 %d 个（热插 %d，撤下 %d）",
		len(clean), merged, p.Registry.Len(), syncAdded, syncRemoved))
}

// subExits 是拉订阅时可以借用的出口。只取健康表判活的节点,且最多
// check.SubRetryExits 个。
//
// 为什么不用「所有已知出口」兜底:那些出口属于已经被判死的节点,12 次尝试
// 会各等 20s(最多四分钟),然后日志把责任推给订阅源,而真正的原因是
// 「一个活出口都没有」。池子空的时候正确答案是纯直连
// (src/index.js:393-400)。
//
// 取哪几个:先收齐**全部**活节点,再随机抽样到上限(生命周期审计 D-C3)。
// 旧写法边遍历边 break 在第一个凑够的 N 个上 —— Registry.All() 是 Go map
// 序,看似随机实则每轮偏一份,同一批节点会被反复选中而其余活出口永远轮不到
// (一个坏出口反复烧同一个订阅源,12 次重试全花在它身上)。快照 + 洗牌让每个
// 活出口被借到的概率相同;拨号器只对**中选者**构建,不给全池付 O(pool) 的
// Host.Dialer。窗口判定也走一次 NodeSnapshot,不再每 tag 一次 RLock(M5
// 同源)。
func (p *Parts) subExits() []sub.Exit {
	snap := p.Health.NodeSnapshot()
	all := p.Registry.All()
	alive := make([]string, 0, len(all))
	for _, o := range all {
		if view, ok := snap[o.Tag]; ok && view.State == health.StateAlive {
			alive = append(alive, o.Tag)
		}
	}
	rand.Shuffle(len(alive), func(i, j int) { alive[i], alive[j] = alive[j], alive[i] })
	exits := make([]sub.Exit, 0, check.SubRetryExits)
	for _, tag := range alive {
		if len(exits) >= check.SubRetryExits {
			break
		}
		d, err := p.Host.Dialer(tag)
		if err != nil {
			continue
		}
		exits = append(exits, sub.Exit{Name: tag, Dial: d})
	}
	return exits
}

// noteSubFailure 是订阅失败的补偿重试:20 秒后再来一次,最多两次。
// 首次启动时订阅常常比出口池先就绪,等本实例就绪后经健康出口复拉往往就通了
// (src/index.js:458-469)。
func (p *Parts) noteSubFailure(ctx context.Context) {
	if p.subFetchRetries.Load() >= int64(subRetryLimit) {
		return
	}
	attempt := p.subFetchRetries.Add(1)
	logger.Warn(fmt.Sprintf("[app] 订阅拉取失败 — %s 后自动重试（第 %d/%d 次，等本实例就绪后经健康出口复拉）",
		subRetryDelay, attempt, subRetryLimit))
	p.afterFunc(subRetryDelay, func() {
		if ctx.Err() != nil {
			return
		}
		if err := p.Rebuild(ctx); err != nil && !errors.Is(err, errRebuildQueued) {
			logger.Warn(fmt.Sprintf("[app] 订阅补偿重试失败: %v", err))
		}
	})
}

// setRebuildResult 记录最近一次热插的结果,面板的「上次重建」与「上次检查」都显示
// 它。dropped 是订阅里被 sing-box 拒收的节点数 —— 前端 checkBadge 的「剔除 N 个
// 坏节点」和 checkAlert 的整句都读它,不记就等于那条告警永远不出现。
// filtered 是地区/类型过滤丢弃的节点数(2026-10-06 新增):过去"几万变几千"在
// 面板上无迹可查,现在 lastCheck.filtered 展示它。
func (p *Parts) setRebuildResult(added, removed, dropped, filtered int, err error) {
	p.rebuildStateMu.Lock()
	defer p.rebuildStateMu.Unlock()
	p.lastAdded = added
	p.lastRemoved = removed
	p.lastDropped = dropped
	p.lastFiltered = filtered
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
	client := httpclient.NewOneShotClient(d, time.Duration(timeoutMS)*time.Millisecond) // 每 shot 即弃,不留 idle 连接
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
//
// 读盘 + 发布这段必须在 applyMu 里(生命周期审计 #1):ApplySettings 的
// 「候选校验 → store.Update → store.Flush → setSettings」整段靠这把锁串行,
// 而 store 自己的 mu 只保证**单次调用**原子。Reload 原先不持锁直接 Load():
// 面板 PUT 走到 Update 之后、Flush 之前时,reload 的 Load 会把 store 覆回
// 磁盘上的旧内容,PUT 随后 Flush 的就是这份**被回退的** map —— 最终
// 活设置 ≠ store ≠ 磁盘,用户这次保存静默消失,下次关停还会把它钉死。
//
// 锁只包住「读盘 + setSettings」,**不包住 Rebuild**:重建要跑几分钟
// (订阅 + 探测排队),持 applyMu 跨它会冻结面板保存;而 Rebuild 有自己的
// rebuildMu 串行。锁序上不存在环:没有任何路径先持 rebuildMu 再取 applyMu
// (Rebuild 全程不碰 applyMu),所以 applyMu→(释放)→rebuildMu 是唯一方向。
func (p *Parts) Reload(ctx context.Context) error {
	if p.settingsStore == nil {
		return fmt.Errorf("app: 设置存储未装配")
	}
	p.applyMu.Lock()
	err := p.settingsStore.Load()
	var next Settings
	if err == nil {
		// 解析失败时绝不写回 p.Settings:把运行中的网关降级成默认配置比
		// 保留一份旧设置糟得多。
		next, err = settingsFromStore(p.settingsStore)
	}
	if err == nil {
		p.setSettings(next)
	}
	p.applyMu.Unlock()
	if err != nil {
		return fmt.Errorf("app: 重读设置: %w", err)
	}
	// 顺序固定:先设置,再重建出站,最后刷新目录。反过来会让目录与新出站
	// 代际错配一轮 —— 目录决定哪些模型可用,而出站决定它们经谁出去。
	//
	// errRebuildQueued 在这一点**不是失败**(六审):设置已经 setSettings
	// 落地,排队的补跑轮用的正是新设置 —— 面板收到错误会 toast「保存失败」,
	// 而改动其实全在。吞掉它,其余错误原样上抛。
	if err := p.Rebuild(ctx); err != nil && !errors.Is(err, errRebuildQueued) {
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
