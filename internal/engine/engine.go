// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package engine owns exit selection and rotation. It turns one client
// request into a sequence of upstream attempts, each on a different exit,
// until one produces content, becomes provably unretryable, or the pool runs
// out. It also records why each attempt happened.
package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"

	"freerouter/internal/adapter"
	"freerouter/internal/catalog"
	"freerouter/internal/check"
	"freerouter/internal/effort"
	"freerouter/internal/errors"
	"freerouter/internal/health"
	"freerouter/internal/logger"
	"freerouter/internal/messages"
	"freerouter/internal/stream"
	"freerouter/internal/tracelog"
	"freerouter/internal/upstream"
)

// Deps is the engine's whole world. Every field is a callback rather than a
// value because the pool, the settings and the catalog are all replaced by a
// rebuild that can land mid-turn; taking them by value would pin a snapshot
// at construction time and mix generations inside a single request.
type Deps struct {
	// State is read once per Complete: one turn is routed entirely against
	// one generation of the catalog.
	State func() State
	// Pool returns the candidate exits for the current rebuild.
	Pool func() []health.PoolNode
	// Settings returns the live routing settings.
	Settings func() Settings
	// RecordUsage folds one successful turn into the stats store. Task 20
	// (stats) installs it; until then it may be nil. model 是 base id:
	// JS 的 recordUsage({model, ...})(src/index.js:185)按模型分列用量,
	// 丢了它 byModel 表就没有行。
	RecordUsage func(model string, u stream.Usage)
	// Dialer builds the per-exit dialer. Zero-port architecture: the engine
	// hands the adapter an http.Client whose DialContext dials the in-process
	// sing-box outbound for the picked tag, so no local port is ever opened.
	Dialer func(tag string) (*http.Client, error)
	// AdapterDeps is the template for one attempt. The engine copies it and
	// fills in Client and NodeKey per exit, because those two are the only
	// fields that differ between attempts. Passing a ready-made *Adapter
	// instead would make the engine share one NodeKey across exits and
	// attribute every TTFT sample to whichever node was built first.
	AdapterDeps adapter.Deps
	// RecordTrace persists one route record per request. Defaults to
	// tracelog.Record; injected so tests can capture without a disk dir.
	RecordTrace func(tracelog.Route)
	// Log receives the one-line rotation summary. Defaults to logger.Info.
	Log func(msg string)
}

// State is one immutable read of the world.
type State struct {
	Catalog []catalog.Model
	// Health is the live scheduler. The engine mutates it through notes.
	Health *health.Health
}

// Settings are the fields the hot path reads, not the settings file itself:
// a struct of only what routing needs, so a settings migration cannot break
// rotation.
type Settings struct {
	Countries      []string
	EffortLevel    string
	MaxAttempts    int
	MaxWallClockMS int64
}

// Request is one client turn, already normalized by the forward layer.
type Request struct {
	// Model is the client-facing id, before BaseModelId strips any provider
	// suffix.
	Model string
	// OpenAI is the raw parsed body: messages or input, tools, temperature,
	// max_tokens, reasoning_effort, user or conversation.
	OpenAI map[string]any
	// Responses selects the Responses-API spelling of the same body.
	Responses bool
}

// Chunk is what onChunk receives. Kind discriminates; the JS version used a
// tagged object with the same five kinds.
type Chunk struct {
	Kind ChunkKind
	Text string
	// Usage is set only on KindUsage, and is the accumulated accounting.
	Usage stream.Usage
	// Index is the tool-call slot, set only on KindToolCallDelta.
	Index int
	// Finish is set only on KindFinish.
	Finish FinishReason
}

type ChunkKind int

const (
	ChunkText ChunkKind = iota
	ChunkReasoning
	ChunkToolCallDelta
	ChunkUsage
	ChunkFinish
)

type FinishReason int

const (
	FinishStop FinishReason = iota
	FinishToolCalls
	FinishMaxTokens
	// FinishAbort means the stream ended without a terminal event: a cut
	// stream. It is a Finish so the forward layer always closes its frame, and
	// it never counts as success.
	FinishAbort
)

// Outcome is the whole turn. Text and ToolCalls are the answer; Error and
// Retryable describe a turn that produced content and then broke.
type Outcome struct {
	Text      string
	ToolCalls []ToolCall
	Usage     stream.Usage
	Truncated bool
	// Error is set when the turn failed. A non-empty Error alongside non-empty
	// Text is a cut stream: the prefix was already forwarded to the client and
	// must be kept, because retrying on another exit would resend it.
	Error     string
	Retryable bool
}

type ToolCall struct {
	ID   string
	Name string
	// Arguments is raw JSON text. It may be truncated mid-object, which is
	// exactly what FinishMaxTokens means.
	Arguments string
	// slot 是 JS 折叠账目里的 candidate.slot(tool-call 增量按它归并,js :527)。
	// 计划类型块没有这个字段,但按 Index 归并的语义需要把槽位记在条目上;
	// 未导出字段不进任何 JSON/API 形状,对类型块的公开面零影响。
	slot int
}

// Row is one /v1/models entry.
type Row struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ---- 常量与判据(照抄 src/engine.js 文件头与 :36-70,数值不得改动) ----

var retryOn = map[string]bool{
	check.CodeRegion:    true,
	check.CodeTransport: true,
	check.CodeTimeout:   true,
	check.CodeEmpty:     true,
	check.CodeQuota:     true,
	check.CodeServer:    true,
}

// cooldownOn 是「只有节点自身可归因的失败才冷却节点」的白名单(js :47)。
//
// 刻意不含 region:地区封锁是**按模型**判定的(health 的 region 矩阵按模型记
// 判决,Pick 会为该模型跳过它),一个出口只对 muse-spark 被拒时就全局冷却它,
// 会误伤这个节点服务的其它模型。
//
// 也不含 quota/empty/server:这三类可能是全局性的(上游整体限流、模型侧返回
// 空、指纹回归导致的 403),对它们冷却一个节点没用,冷却整个池子会把几百个
// 出口全冻住,网关对所有请求不可用 —— 那比偶尔重试一个坏节点严重得多。
// 它们仍然会进本次请求的 excluded 集合。
var cooldownOn = map[string]bool{
	check.CodeTransport: true,
	check.CodeTimeout:   true,
}

// maxAttemptsDefault 是尝试次数硬上限,防止 picker 异常导致死循环。设置项
// maxAttempts 可覆盖(js :50)。
const maxAttemptsDefault = 20

// attemptCapByCode 是各失败码各自的尝试上限,未列出的用 attemptCap 扫池。
//
// EMPTY_RESPONSE 给 6 次,是两次实测之间夹出来的区间(2026-09-28 两轮真实会话,
// js :55-70):
//
//	第一轮(全池扫描):EMPTY 单次耗时中位 9.6s;有一次连续 20 个出口全空、
//	182s 后才放弃 ⇒ 扫池对它没有意义,只是把失败拖长。
//	第二轮(上限 2):最坏降到 ~20s,但 8 个并发请求挂了 4 个 ⇒ 2 次太紧,
//	把延迟问题换成了可靠性问题。
//
// 第二轮日志里的节点标识还回答了一个关键问题:**同一个出口上一秒返回空、下一
// 秒成功** —— 所以 EMPTY 是上游瞬态、不随出口变化,这正是 cooldownOn 不含它的
// 依据,也让「多试几次」比「换个好出口」更贴切。6 次 = 最坏约 60s(6×10s),
// 按观测到的单次成功率能把大部分失败捞回来,仍显著短于扫池的 20 次(~200s)。
var attemptCapByCode = map[string]int{
	check.CodeEmpty: 6,
}

// maxWallClockDefault = 0 即「不限」(js :225)。没有墙钟上限是明确的选择:
// 设了上限就会在长 prompt 上放弃已经烧掉的 token,不设则靠每轮一行汇总日志
// 兜住可观测性。
const maxWallClockDefault = 0

// ---- 编排器 ----

// Engine 是轮换编排器;它自身无状态,一轮的全部可变量都在 Complete 的栈上,
// 因此并发请求互不串账(TestConcurrentCompletesDoNotMixRouteRecords)。
type Engine struct{ deps Deps }

// NewEngine 绑定依赖。Deps 全是回调:池子、设置与目录都可能被一次 rebuild
// 整体替换,快照在 Complete 里取一次(见 Deps 注释)。
func NewEngine(deps Deps) *Engine { return &Engine{deps: deps} }

func (e *Engine) logf(msg string) {
	if e.deps.Log != nil {
		e.deps.Log(msg)
		return
	}
	logger.Info(msg)
}

func (e *Engine) recordTraceFn() func(tracelog.Route) {
	if e.deps.RecordTrace != nil {
		return e.deps.RecordTrace
	}
	return tracelog.Record
}

func nowMS() int64 { return time.Now().UnixMilli() }

// Complete performs one client turn: rotate exits until content, an
// unretryable verdict, or the pool/attempt/wall-clock budget runs out.
func (e *Engine) Complete(ctx context.Context, req Request, onChunk func(Chunk) error) (Outcome, error) {
	if onChunk == nil {
		onChunk = func(Chunk) error { return nil }
	}
	if e.deps.State == nil || e.deps.Pool == nil || e.deps.Dialer == nil {
		return Outcome{}, errors.Failure{Code: check.CodeServer, Status: 500,
			Message: "engine: Deps.State/Pool/Dialer 未装配"}
	}
	snapshot := e.deps.State() // 一次快照:整轮只对一个目录代际路由
	openAi := req.OpenAI
	if openAi == nil {
		openAi = map[string]any{}
	}
	base := upstream.BaseModelID(req.Model)
	entry := findEntry(snapshot.Catalog, base)
	if entry == nil && catalog.IsFreeLane(base) {
		// 未见过的新免费 id 也可直接建目录行(未知模型旁路)。计划骨架写
		// 「limits overlay 建行」:State 的权威类型块只携带 Catalog/Health,
		// models.dev overlay 的 byId 映射无处可挂;overlay 缺席时 JS 的
		// applyLimitsOverlay(entries, {}) 就是原样返回,等价于只用能力表行。
		built := catalog.Build([]string{base})
		if len(built) > 0 {
			entry = &built[0]
		}
	}
	if entry == nil {
		return Outcome{}, errors.Failure{Code: check.CodeServer, Status: 400,
			Message: fmt.Sprintf("unknown model %q", req.Model)}
	}

	session := "forward:" + sessionKeyOf(openAi)
	msgs := FromOpenAI(openAi, req.Responses)
	tools := NormalizeTools(openAi["tools"]) // 名字为空的直接丢
	// 轮内判定在换算之后就地做:FromOpenAI 的产物才是可比形状(OpenAI 的
	// role:"tool" 与 Responses 的 function_call_output 都被归一成 role:"tool")。
	withinTurn := withinToolTurn(msgs)

	settings := Settings{MaxWallClockMS: maxWallClockDefault}
	if e.deps.Settings != nil {
		settings = e.deps.Settings()
	}
	// js :223:Math.max(1, Math.floor(Number(x)) || 20) —— 0/缺失回落默认,
	// 负数被 max(1,…) 夹到 1。
	attemptCap := settings.MaxAttempts
	switch {
	case attemptCap == 0:
		attemptCap = maxAttemptsDefault
	case attemptCap < 0:
		attemptCap = 1
	}
	wallClock := settings.MaxWallClockMS
	if wallClock < 0 {
		wallClock = 0 // 0 = 不限(默认),见 maxWallClockDefault
	}

	// 会前失败(首字节之前)就换下一个出口,直到健康池扫完。为什么不是固定
	// 2 次:无标识的调用方所有请求原本会落进同一个 session、粘在同一个出口
	// 上,一次失败就没有第二次机会;改成扫池之后,一个出口不行不会让整个
	// 请求失败。终止条件取先到:pick 返回 nil(池里没有可用出口了)或尝试
	// 次数达到 attemptCap。**没有墙钟上限**(默认)—— 这是明确的选择,代价
	// 是连续命中慢超时节点时总耗时可能很长,所以每次轮换都会打一行汇总日志。
	excluded := map[string]bool{}
	// 各失败码已用掉的次数,用于 attemptCapByCode 的独立额度。
	codeAttempts := map[string]int{}
	var lastFailure *errors.Failure
	var trail []string
	startedAt := nowMS()
	tr := &routeTrace{model: base, withinTurn: withinTurn, startedAt: startedAt, recordFn: e.recordTraceFn()}
	// sticky 熔断只统计「从 sticky 出口吃到的会前失败」:同会话在同一 sticky
	// 上连跪 2 次就换出口,而不是 30min 内每请求稳定多一次失败延迟。
	startedSticky := snapshot.Health.ExitForSession(session)
	pool := e.deps.Pool() // 池子随 State 同一代际快照,中途 rebuild 不换轮内候选
	turnSeed := mintTurnSeed()

	attempt := 0
	for {
		attempt++
		attemptStartedAt := nowMS()
		if attempt > attemptCap {
			// 放弃路径也必须打汇总 —— 这正是「只限次数、不设墙钟」这个决定
			// 需要的数据(试了几个、各花多久)。漏了它,那个决定就没有依据
			// 可回头评估。
			e.logRotation(trail, fmt.Sprintf("放弃（cap %d）", attemptCap), startedAt, tr)
			return Outcome{}, errors.Failure{
				Code:    codeOrServer(lastFailure),
				Status:  503,
				Message: fmt.Sprintf("gave up after %d exits (cap %d%s)", attempt-1, attemptCap, lastSuffix(lastFailure)),
			}
		}
		if wallClock > 0 && nowMS()-startedAt > wallClock {
			e.logRotation(trail, fmt.Sprintf("超出墙钟预算 %dms", wallClock), startedAt, tr)
			return Outcome{}, errors.Failure{
				Code:    codeOrServer(lastFailure),
				Status:  503,
				Message: fmt.Sprintf("gave up after %d exits (wall-clock %dms%s)", attempt-1, wallClock, lastSuffix(lastFailure)),
			}
		}
		// 粘性出口在本轮已烧掉:进排除集(engine.js:249-250)。连跪两次的
		// 会话不再给它任何机会,哪怕 Pick 的 sticky 分支还会拉它。
		if startedSticky != "" && snapshot.Health.StickyBurned(session) {
			excluded[startedSticky] = true
		}
		restricted := health.IsRestrictedModel(base) || entry.RegionSensitive
		candidates := make([]health.PoolNode, 0, len(pool))
		for _, node := range pool {
			if !excluded[node.Tag] {
				candidates = append(candidates, node)
			}
		}
		// sticky 只对首个出口生效(engine.js:133):一旦排除集非空,说明这个
		// 会话的粘性出口刚失败,继续粘它没有意义,否则会绕回刚失败的出口。
		stickyNode := startedSticky
		if len(excluded) > 0 {
			stickyNode = ""
		}
		picked := snapshot.Health.Pick(health.PickRequest{
			Model:      base,
			Restricted: restricted,
			Countries:  settings.Countries,
			Pool:       candidates,
			StickyNode: stickyNode,
		})
		// 排序快照只取第一次 pick 的(见 routeTrace 注释)。
		if attempt == 1 && picked != nil {
			tr.order = picked.Order
		}
		if picked == nil {
			if len(trail) > 0 {
				e.logRotation(trail, "池子扫完", startedAt, tr)
			} else {
				// JS 对这条路径既不打日志也不落记录;计划的记录语义是
				// 「每请求恰一条 Route」,这里补记录(人类可读行仍不打)。
				tr.record("池子扫完", "")
			}
			message := "no healthy exit for the selected countries"
			if lastFailure != nil {
				message = fmt.Sprintf("no other healthy exit (last: %s)", lastFailure.Message)
			}
			return Outcome{}, errors.Failure{Code: codeOrServer(lastFailure), Status: 503, Message: message}
		}
		snapshot.Health.NoteSticky(session, picked.NodeKey, withinTurn)
		// 在途计数拿 IP 当令牌,acquire 时取一次、release 还同一个
		// (engine.js:269/:312)。不能在 release 时重新解析 IP:探测轮可能
		// 在这个请求跑着的时候把节点量到另一个 IP,那样会还错对象、把另一个
		// 出口的计数清掉(TestExitBusyTokenIsTheIPAtAcquireTime)。
		busyIP := snapshot.Health.NoteExitBusy(snapshot.Health.ExitIPOf(picked.NodeKey, attemptStartedAt))

		var outcome Outcome
		var finish FinishReason
		sawContent := false
		var failure *errors.Failure
		func() {
			// 唯一归还点:defer 保证 attempt 的每条出路(含 panic)都不泄漏
			// 在途计数 —— engine.js:308-313 的 finally 语义,continue(换出口)
			// 和三条 throw(额度用满/不可重试/无候选)全从这里走过。
			defer snapshot.Health.ReleaseExitBusy(busyIP)
			outcome, finish, sawContent, failure = e.attempt(ctx, attemptInput{
				snapshot:   snapshot,
				entry:      entry,
				messages:   msgs,
				tools:      tools,
				picked:     picked,
				settings:   settings,
				withinTurn: withinTurn,
				session:    session,
				openAi:     openAi,
				turnSeed:   turnSeed,
				onChunk:    onChunk,
			})
		}()

		code := ""
		if failure != nil {
			code = failure.Code
		}
		attemptMS := nowMS() - attemptStartedAt
		if code == check.CodeRegion {
			snapshot.Health.NoteRegionError(entry.ID, picked.NodeKey)
		}
		// 每次尝试落一条结构化结果:出口、耗时、失败码、有没有出内容。
		// served 单独留着,因为「失败但已经出了内容」(断流)与会前失败在
		// 选路账上是两回事 —— 前者不换出口,重试它会重发前缀。
		tr.tries = append(tr.tries, tracelog.TryRow{
			Tag:     picked.NodeKey,
			Country: picked.Country,
			IP:      snapshot.Health.ExitIPOf(picked.NodeKey, nowMS()),
			Code:    tryCodeOf(failure),
			MS:      attemptMS,
			Served:  sawContent,
		})
		if failure == nil && (finish == FinishStop || finish == FinishToolCalls || finish == FinishMaxTokens) {
			// 真实流量是地区矩阵的权威判决,与主动探针同一待遇:gated 模型
			// 在这里答上了,就证明这个出口能服务它(tier B)。
			snapshot.Health.NoteRegionOK(entry.ID, picked.NodeKey)
		}

		// ---- 失败分流的三条分支(js :335-407,顺序不可换) ----

		// A) 可换出口的会前失败:进排除集、按码记账、换下一个出口。
		if failure != nil && !sawContent && retryOn[code] {
			used := codeAttempts[code] + 1
			codeAttempts[code] = used
			trail = append(trail, fmt.Sprintf("%s(%dms,%s)", code, attemptMS, shortTag(picked.NodeKey)))
			// 只对显式配了额度的码生效(目前只有 EMPTY)。其余码继续由主
			// attemptCap 兜底 —— 这样它们的失败信息与日志格式不变。
			if cap, ok := attemptCapByCode[code]; ok && used >= cap {
				e.logRotation(trail, fmt.Sprintf("%s 用满 %d 次", code, cap), startedAt, tr)
				if startedSticky != "" && picked.NodeKey == startedSticky {
					snapshot.Health.NoteStickyFailure(session)
				}
				return Outcome{}, *failure
			}
			excluded[picked.NodeKey] = true
			lastFailure = failure
			if cooldownOn[code] {
				// 只有节点自身可归因的失败才冷却(见 cooldownOn 注释)。
				snapshot.Health.NoteCooldown(picked.NodeKey, failure.RetryAfterMS)
			}
			if code == check.CodeQuota {
				// 配额墙只降级、不冷却:429 说的是上游的计费决定,不是这个
				// 出口坏了。Pick 会在 90s 里把它排在「没撞过墙」的出口后面。
				snapshot.Health.NoteQuota(picked.NodeKey)
			}
			if startedSticky != "" && picked.NodeKey == startedSticky {
				// 失败来自本轮进入时的 sticky 出口 → 记一次熔断分。
				snapshot.Health.NoteStickyFailure(session)
			}
			continue // 同一个 session,换下一个出口
		}
		// B) 不可重试的会前失败(凭证错、未知模型…):立刻抛出,别拿全池
		// 去试一个换出口也解决不了的问题。
		if failure != nil && !sawContent {
			if startedSticky != "" && picked.NodeKey == startedSticky {
				snapshot.Health.NoteStickyFailure(session)
			}
			e.logRotation(trail, fmt.Sprintf("不可重试 %s", code), startedAt, tr)
			return Outcome{}, *failure
		}
		// C) 成功,或已出内容后的失败(断流)。
		if sawContent || !retryOn[code] {
			// 出内容即清熔断分:sticky 出口恢复正常。分支 C 里 failure!=nil
			// 必然伴随 sawContent(A/B 已接走其余情形),两个判据在这里合流。
			snapshot.Health.ClearStickyFailures(session)
		}
		if failure == nil {
			// 成功出口解除冷却,连败计数清零。
			snapshot.Health.ClearCooldown(picked.NodeKey)
		}
		if len(trail) > 0 {
			e.logRotation(trail, "成功", startedAt, tr)
		} else {
			// 没发生轮换时不打人类可读那行(每个请求一行会把日志淹掉),
			// 但照样落一条结构化记录:一次就成的请求恰恰是最需要解释的那种。
			// record 幂等,上面已落过就什么都不做。
			tr.record("成功", "")
		}
		// 把「这次从厂商缓存读到了多少」交回 health:下一次这个会话的粘性
		// TTL 由它定档。只在真正成功的那次出口上记,且在 usage 转 OpenAI
		// 形状之前(harness 形状)。轮内**不**重新定档:轮内出口不会换,而
		// 工具结果回传那一次的 prompt 必然比上一轮大得多、却必然不在上一轮
		// 的缓存前缀里,拿这个去改档等于每轮把出口踢掉一次。
		if failure == nil && outcome.Usage.HasUsage && !withinTurn {
			snapshot.Health.NoteStickyUsage(session, health.StickyUsage{
				CacheReadTokens: float64(outcome.Usage.CacheRead),
				InputTokens:     float64(outcome.Usage.In),
			})
		}
		if e.deps.RecordUsage != nil && failure == nil {
			e.deps.RecordUsage(base, outcome.Usage)
		}
		if failure != nil {
			// 断流发生在已出内容之后:换出口重试会把前缀重发一遍,整条丢弃
			// 又把已成功的半截也烧掉。保留已转发文本,把失败标注给转发层 ——
			// SSE 头已发出的场景里,流内 error 事件(配合 FinishAbort 关帧)
			// 是把失败告诉客户端的唯一通道。
			outcome.Error = failure.Message
			outcome.Retryable = retryOn[code]
		}
		if outcome.Truncated {
			// max-tokens 收尾意味着 arguments 被截在 JSON 半截上;保留它会让
			// OpenAI 答案与 finish_reason 不一致(js :412-416)。
			dropBrokenToolCalls(&outcome)
		}
		// usage → OpenAI 形状(js :419-428)由 OpenAIUsage 提供,转发层在
		// 序列化时调用;Outcome.Usage 保持 harness 形状,RecordUsage 与
		// NoteStickyUsage 都按它记账。
		return outcome, nil
	}
}

// attemptInput 收拢一次尝试的全部素材。
type attemptInput struct {
	snapshot   State
	entry      *catalog.Model
	messages   []messages.Message
	tools      []messages.Tool
	picked     *health.Picked
	settings   Settings
	withinTurn bool
	session    string
	openAi     map[string]any
	turnSeed   string
	onChunk    func(Chunk) error
}

// attempt runs one attempt on one exit. Returns the folded Outcome, the
// finish reason of a successful end, whether any content was seen, and the
// classified failure (nil on success).
//
// adapter.Complete 的回调只吐文本增量 —— reasoning/tool-call 增量与 usage/
// finish 词汇由本层包装成 Chunk 的五种 Kind 交给转发层,转发层因此不必知道
// stream/adapter 的存在。reasoning 与 tool-call 的细分以 adapter 实际能拿到的
// 信息为准:Go adapter 的上行通道只有文本(其内部 feed 只投影 text delta),
// 所以真实链路里只会产生 ChunkText 与收尾的 ChunkUsage/ChunkFinish;
// ChunkReasoning / ChunkToolCallDelta 的折叠语义在 foldChunks 里照 JS 全量
// 实现并配有直接单测,adapter 通道扩展时零改动接入。
func (e *Engine) attempt(ctx context.Context, in attemptInput) (Outcome, FinishReason, bool, *errors.Failure) {
	var out Outcome
	sawContent := false
	emit := func(c Chunk) error {
		if _, saw := foldChunks(c, &out); saw {
			sawContent = true
		}
		return in.onChunk(c)
	}

	client, err := e.deps.Dialer(in.picked.NodeKey)
	if err != nil {
		// 零端口架构里 dialer 失败 = 出口没被装配/拨不通,等价传输层失败:
		// 计入一次尝试、毫秒级返回,交给分支 A 换出口,不等上游超时。
		return out, FinishStop, sawContent,
			&errors.Failure{Code: check.CodeTransport, Message: err.Error(), Retryable: true}
	}

	deps := e.deps.AdapterDeps
	deps.Client = client
	deps.NodeKey = in.picked.NodeKey
	deps.Model = in.entry.ID
	deps.Wire = in.entry.Wire
	deps.SessionID = in.session
	deps.Tools = in.tools
	deps.Effort = in.settings.EffortLevel
	if deps.OnFirstToken == nil {
		// TTFT 回灌(magpie internal/gateway/ttft.go):adapter 在第一个
		// token 到达的那一刻就知道首字延迟,比等整条流读完再算准得多。
		deps.OnFirstToken = in.snapshot.Health.NoteTtft
	}

	clientEffort, _ := in.openAi["reasoning_effort"].(string)
	// 默认档来自设置(上游语义:harness 缺省 balanced),调用方可用
	// reasoning_effort 逐请求覆盖 —— 这条车道真正生效的旋钮是它导出的预算。
	deps.Entry, clientEffort = buildEffortEntry(*in.entry, clientEffort, in.settings.EffortLevel)

	areq := adapter.Request{
		Messages:        in.messages,
		Stream:          true,
		TurnSeed:        in.turnSeed,
		ReasoningEffort: clientEffort,
	}
	if v, ok := in.openAi["temperature"].(float64); ok {
		areq.Temperature = v
	}
	if v, ok := in.openAi["max_tokens"].(float64); ok && v > 0 {
		areq.MaxTokens = int(v)
	}

	usage, err := adapter.NewAdapter(deps).Complete(ctx, areq, func(text string) error {
		return emit(Chunk{Kind: ChunkText, Text: text})
	})
	if usage.HasUsage {
		if err2 := emit(Chunk{Kind: ChunkUsage, Usage: usage}); err2 != nil && err == nil {
			err = err2
		}
	}
	if err != nil {
		failure := classifyAttemptError(err)
		if sawContent {
			// 断流:流被消费到一半。转发层靠这个 Finish 关帧,并且它绝不
			// 算成功 —— Complete 的分支 C 会把失败写进 Outcome。
			_ = emit(Chunk{Kind: ChunkFinish, Finish: FinishAbort})
			return out, FinishAbort, sawContent, failure
		}
		return out, FinishStop, sawContent, failure
	}
	finish := FinishStop
	if len(out.ToolCalls) > 0 {
		finish = FinishToolCalls
	}
	// FinishMaxTokens 经本 adapter 不可达:上行通道只有文本,max-tokens 收尾
	// 没有信源;foldChunks 的 Truncated 折叠与 dropBrokenToolCalls 过滤保留
	// 全量语义,由直接单测钉住。
	if err2 := emit(Chunk{Kind: ChunkFinish, Finish: finish}); err2 != nil {
		// 收尾帧都发不出去 = 客户端已走,按出内容后的失败收场。
		return out, finish, sawContent,
			&errors.Failure{Code: check.CodeServer, Message: err2.Error()}
	}
	return out, finish, sawContent, nil
}

// classifyAttemptError 把 adapter 吐回的错误归类成 Failure:adapter 自己的
// 失败都是 Failure;onChunk 的回调错误(客户端断开的替身)按 js :307 的
// `error?.llmCode ?? error?.code ?? CODE.server` 兜底成 SERVER。
func classifyAttemptError(err error) *errors.Failure {
	var f errors.Failure
	if stderrors.As(err, &f) {
		return &f
	}
	return &errors.Failure{Code: check.CodeServer, Message: err.Error()}
}

// buildEffortEntry 把目录行投影成 effort.Entry,并在此处理「思考关不掉」家族
// 的预算补偿(修正案 §3 任务 17 条:JS 的 ALWAYS_THINKING_FACTOR 由任务 18
// 在构造 Entry 时补偿)。
//
// JS 的 effort.js:74(ceilingOf)对 canDisableThinking === false 的模型把每一档
// 的档位上限翻倍(ALWAYS_THINKING_FACTOR = 2,src/effort.js:42):这类模型关不
// 掉思考,推理先从同一块输出上限里扣,不翻倍的话 balanced 只给答案留约 1500
// token,长轮次每三跑就撞一次 length(实测 mimo-v2.6-flash-free 一天 92 次调用
// 82% 的输出是推理)。Go 的 effort.Entry 不携带该位,而档位上限(2048/8192)
// 是 effort 包的私有常量,从外面抬不上去 —— 所以在这里等价复现:
//
//  1. 用 effort.BudgetFor 对未补偿的 Entry 探出当前档位的实际上限(上限不
//     约束容量时探出的就是 MaxOutput 本身,此时无可翻倍之物也无须翻倍);
//  2. 把 Entry.MaxOutput 压到 min(模型上限, 2×上限),并给请求词一个解析为
//     「无档位上限」的值(extra ↔ JS deep,修正案 §3 的词汇对照),让
//     adapter 里 BudgetFor 自己的上限不再二次收紧。
//
// 两步之后 budget = max(512, min(模型上限, 2×上限, requested, settingsDefault)),
// 与 JS budgetFor(src/effort.js:90-103)在 canDisableThinking===false 时的算术
// 逐项相同。档位词从不出现在请求体上(Go adapter 只用它算 max_tokens),覆盖
// client 词不改变线上语义。
func buildEffortEntry(m catalog.Model, clientWord, settingsWord string) (effort.Entry, string) {
	plain := effort.Entry{
		ID:                m.ID,
		ContextWindow:     m.ContextWindow,
		MaxOutput:         m.MaxOutput,
		SupportsReasoning: m.Reasoning,
	}
	if m.CanDisableThinking || !m.Reasoning {
		return plain, clientWord
	}
	level := effort.ResolveLevel(clientWord, settingsWord, plain)
	ceiling := int64(effort.BudgetFor(level, plain, 0, nil))
	if ceiling >= plain.MaxOutput {
		// 档位没有上限(deep/none)或上限本就不低于模型容量:JS 的翻倍
		// 乘在更小的那一项上,结果不变,原样通过。
		return plain, clientWord
	}
	compensated := plain
	compensated.MaxOutput = 2 * ceiling
	if compensated.MaxOutput > plain.MaxOutput {
		compensated.MaxOutput = plain.MaxOutput
	}
	return compensated, string(effort.LevelExtra)
}

// logRotation 打轮换汇总:一行收拢出口数、各次失败码与耗时。不逐次打 ——
// 一轮探测几百个死节点曾经刷出 7000+ 行把日志和面板淹掉(js :119-124),
// 这一行也够用来判断「只限次数、不设墙钟」的决定是否需要调整。
func (e *Engine) logRotation(trail []string, result string, startedAt int64, tr *routeTrace) {
	shown := trail
	if len(trail) > 6 {
		// Keep the first four and the last two with an elision marker between.
		// A 20-exit sweep is unreadable as one line, and the elided exits are
		// the interchangeable ones; the first four explain why we started
		// rotating and the last two explain where we ended up.
		shown = append(append(append([]string{}, trail[:4]...),
			fmt.Sprintf("…%d个…", len(trail)-6)), trail[len(trail)-2:]...)
	}
	// Build the body first and only add the separator when there is
	// something to separate. The JS version always joined a separator, so a
	// rotation-free turn printed "出口轮换 0 次后不可重试 …:  共 1012ms" with
	// a double space.
	body := ""
	if len(shown) > 0 {
		body = strings.Join(shown, " → ") + " "
	}
	msg := fmt.Sprintf("出口轮换 %d 次后%s: %s共 %dms", len(trail), result, body, nowMS()-startedAt)
	e.logf(msg)
	tr.record(result, body)
}

// shortTag 是日志里的出口标识(js :84-93)。JS 版是 `名称前缀@端口`,因为
// 端口才是唯一键(实测 218 个节点端口无重复);零端口架构里没有端口,唯一键
// 就是完整 tag —— 所以这里只剥区域旗帜(区域指示符 rune:两个码点却不带
// 信息,国家在名称里也有,留着会把 12 字符的名称预算吃掉三分之一)、去掉
// 所有空白、截到 12 字符,不再接 @? 后缀。tracelog 的 OrderRow/TryRow 存
// 完整 tag,由面板折叠显示,信息不丢。
func shortTag(tag string) string {
	var b strings.Builder
	for _, r := range tag {
		if unicode.Is(unicode.Regional_Indicator, r) || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(r)
	}
	runes := []rune(b.String())
	if len(runes) > 12 {
		runes = runes[:12]
	}
	return string(runes)
}

// ModelRows 是 /v1/models 的行。上游 issue #3:在所有已探测的存活出口上都测得
// 地区受限的模型,不再对外列出(面板仍展示并标记,便于观察 region 矩阵恢复)。
// free-lane 过滤是 Go 版的防御性补充:Build 已经过滤过,这里再挡一道手工拼进
// State 的目录行,付费 id 不可能从网关的列表里泄漏出去。
func (e *Engine) ModelRows() []Row {
	if e.deps.State == nil {
		return nil
	}
	snapshot := e.deps.State()
	created := time.Now().Unix()
	rows := make([]Row, 0, len(snapshot.Catalog))
	for _, entry := range snapshot.Catalog {
		if !catalog.IsFreeLane(entry.ID) || snapshot.Health.UnavailableEverywhere(entry.ID) {
			continue
		}
		rows = append(rows, Row{ID: entry.ID, Object: "model", Created: created, OwnedBy: "lite-gateway"})
	}
	return rows
}

// ---- 小工具 ----

// routeTrace 收拢一条请求的结构化路由记录(magpie internal/gateway/trace.go 的
// Route)。人类可读的 logRotation 那行回答「试了几个、花了多久」,回答不了
// **为什么是这个顺序** —— 判据全在 Pick 的排序里算完就丢。order 只在第一次
// pick 时是完整的决策依据:之后每一轮都带着排除集重排,那个顺序是「上一次
// 残余」而不是决定。取第一次的那一份 + 全部尝试结果,正好能重建整条决策链。
type routeTrace struct {
	model      string
	order      []tracelog.OrderRow
	tries      []tracelog.TryRow
	withinTurn bool
	recorded   bool
	startedAt  int64
	recordFn   func(tracelog.Route)
}

// record 一次请求只落一条记录。logRotation 有 6 条调用路径(cap / 墙钟 /
// 池子扫完 / 码额度用满 / 不可重试 / 成功),同一次请求里理论上有两条都可达
// (先撞 specificCap 再抛是唯一的情形),所以幂等性放在这里而不是调用点
// (js :206-218)。
func (t *routeTrace) record(result, text string) {
	if t.recorded {
		return
	}
	t.recorded = true
	if t.recordFn == nil {
		return
	}
	now := nowMS()
	t.recordFn(tracelog.Route{
		Kind:       "route",
		Model:      t.model,
		Text:       text,
		Result:     result,
		WithinTurn: t.withinTurn,
		Order:      t.order,
		Tries:      t.tries,
		MS:         now - t.startedAt,
		At:         now,
	})
}

// sessionKeyOf 是 js 的 openAi.user ?? openAi.conversation ?? 'shared'
// (nullish 回退:键存在但为空串不回退,照样作为键使用)。
func sessionKeyOf(openAi map[string]any) string {
	for _, key := range []string{"user", "conversation"} {
		if v, ok := openAi[key]; ok && v != nil {
			return fmt.Sprint(v)
		}
	}
	return "shared"
}

// mintTurnSeed 给一轮 Complete 发一个种子:同一轮的重试钉在同一个
// x-opencode-request 上 —— 上游按请求 id 做会话内画像,每试一次换一个 id 就是
// 每分钟 N 个新身份,会换来「retry-after 递增」的升级待遇(js upstream.js:186)。
func mintTurnSeed() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// findEntry 按目录行的 id(即 BaseModelId 之后的 base id)查条目。
func findEntry(list []catalog.Model, base string) *catalog.Model {
	for i := range list {
		if list[i].ID == base {
			return &list[i]
		}
	}
	return nil
}

// tryCodeOf 对应 js 的 `failure === undefined ? 'ok' : String(code ?? 'unknown')`。
func tryCodeOf(failure *errors.Failure) string {
	if failure == nil {
		return "ok"
	}
	if failure.Code == "" {
		return "unknown"
	}
	return failure.Code
}

func codeOrServer(lastFailure *errors.Failure) string {
	if lastFailure != nil && lastFailure.Code != "" {
		return lastFailure.Code
	}
	return check.CodeServer
}

func lastSuffix(lastFailure *errors.Failure) string {
	if lastFailure != nil {
		return "; last: " + lastFailure.Message
	}
	return ""
}
