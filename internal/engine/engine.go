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
	"sync"
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
	RecordUsage func(model, exit string, u stream.Usage)
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
	Countries   []string
	EffortLevel string
	// DefaultMaxTokens 是 settings.defaultMaxTokens 的解析结果:<=0 表示
	// 「未设置」(面板存 null),与 adapter.Deps.MaxTokens 的约定一致(B2)。
	DefaultMaxTokens int
	MaxAttempts      int
	MaxWallClockMS   int64
	// ExitConcurrency 是同一出口 IP 上允许的最大在途请求数(magpie 的
	// lanes 闸门):超出的请求按先来后到排队,等到槽位再发出 —— 排队不
	// 是失败,不换出口、不记错误。<=0 表示不限。
	ExitConcurrency int
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
	// tool-call 增量的结构化载荷(替代旧的 JSON-in-Text 信封):Delta 帧带
	// ToolDelta,block-end 完整帧带 ToolArguments —— 判别符是 ToolArguments
	// 是否非空(见 translate.go 的折叠语义)。
	ToolID        string
	ToolName      string
	ToolDelta     string
	ToolArguments string
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
	//
	// 增量期它可能滞后于 argsSB:foldChunks 的增量分支只往 argsSB 追加(O(1)
	// 摊还),Arguments 在物化点(materializeToolCalls)才回填 —— `+=` 在长
	// 参数 × 碎增量下是 O(n²)。读 Arguments 前必须经过物化点;生产侧的物化点
	// 是 attempt 的 materialize() 与 dropBrokenToolCalls 入口。
	Arguments string
	// slot 是 JS 折叠账目里的 candidate.slot(tool-call 增量按它归并,js :527)。
	// 计划类型块没有这个字段,但按 Index 归并的语义需要把槽位记在条目上;
	// 未导出字段不进任何 JSON/API 形状,对类型块的公开面零影响。
	slot int
	// argsSB 累积参数增量,见 Arguments 的注释。指针:strings.Builder 禁止
	// 值拷贝,而 ToolCalls 切片的 append/扩容/range 都会拷贝元素值。
	argsSB *strings.Builder
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
// freeTierShapeWarned 把「形状门禁疑变」的诊断行压到每进程一次(见 Complete
// 里的调用点)。
var freeTierShapeWarned sync.Once

var attemptCapByCode = map[string]int{
	check.CodeEmpty: 6,
}

// maxWallClockDefault 是单轮请求的墙钟上限(毫秒)。过去是 0(不限),代价是
// 连续命中慢超时节点时单请求占 goroutine+SSE+车道槽最长 ~200s(20 次 ×
// EMPTY 中位 9.6s)。180s 覆盖正常长尾(大 prompt 多轮重试),只掐病态扫池;
// settings 显式 0 仍表示不限(面板可设,存量配置不受影响)。
const maxWallClockDefault = 180000

// ---- 编排器 ----

// Engine 是轮换编排器。lanes 是出口 IP 车道闸门(ExitConcurrency),它
// 的状态跨请求存活 —— 除它之外引擎自身无状态,一轮的全部可变量都在
// Complete 的栈上,因此并发请求互不串账(TestConcurrentCompletesDoNotMixRouteRecords)。
type Engine struct {
	deps Deps
	// exitLanes 按「acquire 时的出口 IP」记车道;视图走 Lanes() 给面板。
	exitLanes lanes
}

// NewEngine 绑定依赖。Deps 全是回调:池子、设置与目录都可能被一次 rebuild
// 整体替换,快照在 Complete 里取一次(见 Deps 注释)。
func NewEngine(deps Deps) *Engine { return &Engine{deps: deps} }

// Lanes 是全部出口车道视图(busy/waiting/limit),给面板 /api/status 用。
func (e *Engine) Lanes() map[string]Lane { return e.exitLanes.snapshot() }

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
		wallClock = 0 // 显式负数归 0=不限;出厂默认见 maxWallClockDefault(180s)
	}

	// 会前失败(首字节之前)就换下一个出口,直到健康池扫完。为什么不是固定
	// 2 次:无标识的调用方所有请求原本会落进同一个 session、粘在同一个出口
	// 上,一次失败就没有第二次机会;改成扫池之后,一个出口不行不会让整个
	// 请求失败。终止条件取先到:pick 返回 nil(池里没有可用出口了)、尝试
	// 次数达到 attemptCap、或墙钟超过 maxWallClockDefault(180s —— v1.4.1 从
	// 「不限」改来,只掐病态长扫池,正常长尾在大 prompt 多轮重试下仍远够用;
	// settings 显式 0 仍是不限)。代价是连续命中慢超时节点时总耗时受这个上限
	// 约束,所以每次轮换都打一行汇总日志留证。
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

	// 轮首预建:build+指纹+Marshal 只做一次(同轮重试 body 不变),attempt
	// 只换 Client/NodeKey/Session 头。stale-reasoning 重放走 adapter 内的
	// 二次 Marshal(就地剥字段后重序列化),不走这里。
	clientEffort, _ := openAi["reasoning_effort"].(string)
	var wheelEntry effort.Entry
	wheelEntry, clientEffort = buildEffortEntry(*entry, clientEffort, settings.EffortLevel)
	wheelReq := buildAttemptRequest(attemptInput{
		messages: msgs,
		tools:    tools,
		settings: settings,
		session:  session,
		openAi:   openAi,
		turnSeed: turnSeed,
	}, clientEffort)
	var wheelPrebuilt *adapter.PrebuiltBody
	{
		probeDeps := e.deps.AdapterDeps
		probeDeps.Model = entry.ID
		probeDeps.Wire = entry.Wire
		probeDeps.SessionID = session
		probeDeps.Tools = tools
		probeDeps.Effort = settings.EffortLevel
		if settings.DefaultMaxTokens > 0 {
			probeDeps.MaxTokens = settings.DefaultMaxTokens
		}
		probeDeps.Entry = wheelEntry
		if pb, err := adapter.NewAdapter(probeDeps).BuildBody(wheelReq); err == nil {
			wheelPrebuilt = pb
		} else {
			// 预建失败不致命:attempt 回落本地组装(与旧语义一致)。但也不能
			// 静默 —— 失败意味着这一轮每次 attempt 都白付全量 build+Marshal
			// (正是本优化要消灭的税),零观测就永远没人知道它在退化(六审)。
			e.logf(fmt.Sprintf("engine: 轮首预建失败,本轮退回逐 attempt 组装: %v", err))
		}
	}

	attempt := 0
	for {
		attempt++
		// 客户端取消的快速生效点:排队/慢出口场景下,上一发还在途时客户端
		// 已离开 —— 不在这里拦,下一发拨号/请求照发,出口 quota 照扣。
		if err := ctx.Err(); err != nil {
			tr.record("客户端离开", "")
			return Outcome{}, errors.Failure{Code: check.CodeAborted, Message: "client gone: " + err.Error()}
		}
		attemptStartedAt := nowMS()
		if attempt > attemptCap {
			// 放弃路径也必须打汇总 —— 轮换终止(次数帽/墙钟帽)需要的数据(试了
			// 几个、各花多久)全在这一行里。漏了它,上限怎么定的就永远没有
			// 依据可回头评估。
			e.logRotation(trail, fmt.Sprintf("放弃（cap %d）", attemptCap), startedAt, tr)
			// 给粘性出口记一次熔断分:每轮 attempt 都会把 sticky 重钉在最新
			// 的出口上,放弃路径不清账,会话就钉在最后一个已知失败的出口上,
			// 下一条请求必然先在它上面白付一次失败。记一次分,连跪两次即换。
			snapshot.Health.NoteStickyFailure(session)
			return Outcome{}, errors.Failure{
				Code:    codeOrServer(lastFailure),
				Status:  503,
				Message: fmt.Sprintf("gave up after %d exits (cap %d%s)", attempt-1, attemptCap, lastSuffix(lastFailure)),
			}
		}
		if wallClock > 0 && nowMS()-startedAt > wallClock {
			e.logRotation(trail, fmt.Sprintf("超出墙钟预算 %dms", wallClock), startedAt, tr)
			snapshot.Health.NoteStickyFailure(session)
			return Outcome{}, errors.Failure{
				Code:    codeOrServer(lastFailure),
				Status:  503,
				Message: fmt.Sprintf("gave up after %d exits (wall-clock %dms%s)", attempt-1, wallClock, lastSuffix(lastFailure)),
			}
		}
		// 粘性出口在本轮已烧掉:进排除集(engine.js:249-250)。连跪两次的
		// 会话不再给它任何机会 —— 不过这个判断实际由 ExitForSession 承担
		// (stickyFail 达到 2 时它已把行删掉、返回空串),循环内不会再进到
		// 烧掉的 sticky 上,这里没有可补的排除。
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
			// 与 cap/墙钟放弃路径同理:清一次粘性账,别把会话钉死在
			// 最后一个失败的出口上。
			snapshot.Health.NoteStickyFailure(session)
			message := "no healthy exit for the selected countries"
			if lastFailure != nil {
				message = fmt.Sprintf("no other healthy exit (last: %s)", lastFailure.Message)
			}
			return Outcome{}, errors.Failure{Code: codeOrServer(lastFailure), Status: 503, Message: message}
		}
		// 出口车道闸门(magpie lanes):同一出口 IP 最多 ExitConcurrency 个
		// 在途,超出的排队等槽 —— 排队不是失败,不换出口、不记错误。排队
		// 时不占在途 busyIP 计数(那是发给上游后的量),轮到才取 IP。
		// 没有可信出口 IP 的节点**不进闸**(exitIP 空串):按 rankLocked 的
		// 同一原则,「未知」不是「与谁共享」—— 让所有未量测节点共用一条名
		// 为 "" 的车道,会把物理上互不相干的出口串行成一条队。
		exitIP := snapshot.Health.ExitIPOf(picked.NodeKey, attemptStartedAt)
		releaseLane := func() {}
		if exitIP != "" {
			var ok bool
			releaseLane, ok = e.exitLanes.acquire(ctx, exitIP, settings.ExitConcurrency)
			if !ok {
				// 客户端在排队时离开了:这一轮作废,什么都没发出去。两个账
				// 必须在这里补:(a) NoteSticky 挪到 acquire 成功之后 —— 排队
				// 即取消的会话不该被钉到一个从未发出过请求的出口上白拿 30min
				// 粘性;(b) 「每请求恰一条 Route」的不变量在六条 record 路径
				// 之外不能有旁路。
				tr.record("客户端离开(车道排队)", "")
				return Outcome{}, ctx.Err()
			}
		}
		// sticky 在车道闸门之后落钉:只有真正拿到出口的轮次才配改会话粘性。
		snapshot.Health.NoteSticky(session, picked.NodeKey, withinTurn)
		// 在途计数拿 IP 当令牌,acquire 时取一次、release 还同一个
		// (engine.js:269/:312)。不能在 release 时重新解析 IP:探测轮可能
		// 在这个请求跑着的时候把节点量到另一个 IP,那样会还错对象、把另一个
		// 出口的计数清掉(TestExitBusyTokenIsTheIPAtAcquireTime)。车道闸门
		// 同理:release 还的是 acquire 时的那个 IP。
		busyIP := snapshot.Health.NoteExitBusy(exitIP)

		var outcome Outcome
		var finish FinishReason
		sawContent := false
		var failure *errors.Failure
		func() {
			// 唯一归还点:defer 保证 attempt 的每条出路(含 panic)都不泄漏
			// 在途计数与车道槽位 —— engine.js:308-313 的 finally 语义,
			// continue(换出口)和三条 throw(额度用满/不可重试/无候选)全
			// 从这里走过。defer 逆序执行:先还 busyIP 再还车道槽(与获取
			// 顺序相反,LIFO 正确)。
			defer releaseLane()
			defer snapshot.Health.ReleaseExitBusy(busyIP)
			outcome, finish, sawContent, failure = e.attempt(ctx, attemptInput{
				snapshot:     snapshot,
				entry:        entry,
				messages:     msgs,
				tools:        tools,
				picked:       picked,
				settings:     settings,
				withinTurn:   withinTurn,
				session:      session,
				openAi:       openAi,
				turnSeed:     turnSeed,
				onChunk:      onChunk,
				prebuilt:     wheelPrebuilt,
				clientEffort: clientEffort,
				effortEntry:  wheelEntry,
			})
		}()

		code := ""
		if failure != nil {
			code = failure.Code
		}
		if failure != nil && failure.Type == "FreeTierError" {
			// 形状门禁疑变的诊断锚(dsh harness 仓库的教训:这一行能把排障从
			// 「盲扫五回」缩到一次定位 —— FreeTierError 是会话形状被上游闸门
			// 拒绝,不是配额、也不是这个出口坏了;闸门 2026-09-16/09-27 动过两次)。
			// 每进程只打一次,避免扫池时刷屏。
			freeTierShapeWarned.Do(func() {
				e.logf("engine: 上游回 FreeTierError —— 会话形状门禁疑似收紧" +
					"(session 形状 / 工具名 / stream 标志),若全池持续出现请核对上游闸门变化;本轮仍按可重试处理")
			})
		}
		// 数据面信号(1.3.0 定稿):真实对话的成功/失败就是最真实的健康探测
		// —— 零成本、零延迟。成功视同通过一轮健康检测(刷新 alive 并清连败);
		// transport/timeout 计一次失败(可降冷区)。region/quota/empty 等
		// 「节点活着但被拒」的判决不参与连通性状态机;删除是冷区 pass 的
		// 专属判决,数据面失败只降档(allowDelete=false)。
		switch {
		case failure == nil:
			snapshot.Health.MarkPassSuccess(picked.NodeKey)
		case code == check.CodeTransport || code == check.CodeTimeout:
			snapshot.Health.MarkPassFail(picked.NodeKey, false)
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
			IP:      exitIP, // 复用 attempt 初的 exitIP,不二次查询:探测轮可能
			// 在请求跑着时改判,二次查会把同一轮记成两个 IP;trace 要的是
			// 「这一轮走的出口」,不是记录时刻的最新量测。
			Code:   tryCodeOf(failure),
			MS:     attemptMS,
			Served: sawContent,
		})
		if failure == nil && (finish == FinishStop || finish == FinishToolCalls || finish == FinishMaxTokens) {
			// 真实流量是地区矩阵的权威判决,与主动探针同一待遇:gated 模型
			// 在这里答上了,就证明这个出口能服务它(tier B)。
			snapshot.Health.NoteRegionOK(entry.ID, picked.NodeKey)
		}

		// ---- 失败分流的三条分支(js :335-407,顺序不可换) ----

		// A) 可换出口的会前失败:进排除集、按码记账、换下一个出口。
		// Unavailable 位必须把门:供应商不再服务的模型是服务端事实,换多少
		// 个出口都一样 —— 过去这个位没有消费者,死模型让每个请求都白扫满
		// attemptCap 个出口、还向调用方报告「可重试」(errors 包注释的承诺
		// 就是在这里兑现的)。
		if failure != nil && !sawContent && retryOn[code] && !failure.Unavailable {
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
				// 与 cap/墙钟/池子扫完三条放弃路径同一状态码:这是「轮换耗尽」,
				// Failure 没带 Status 会让转发层落到 500,调用方的重试策略
				// 在同一类失败上分叉。
				exhausted := *failure
				exhausted.Status = 503
				return Outcome{}, exhausted
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
			// exit 一并交给记账:面板按出口分列用量(README 承诺的「模型 ×
			// 出口」维度),断流/轮换的失败轮不走这里。
			e.deps.RecordUsage(base, picked.NodeKey, outcome.Usage)
		}
		if failure != nil {
			// 断流发生在已出内容之后:换出口重试会把前缀重发一遍,整条丢弃
			// 又把已成功的半截也烧掉。保留已转发文本,把失败标注给转发层 ——
			// SSE 头已发出的场景里,流内 error 事件(配合 FinishAbort 关帧)
			// 是把失败告诉客户端的唯一通道。Retryable 同样要过 Unavailable
			// 位:别让一个死模型被报成「值得重试」。
			outcome.Error = failure.Message
			outcome.Retryable = retryOn[code] && !failure.Unavailable
		}
		if outcome.Truncated || failure != nil {
			// max-tokens 收尾意味着 arguments 被截在 JSON 半截上;保留它会让
			// OpenAI 答案与 finish_reason 不一致(js :412-416)。断流同享这一
			// 道(过去只有 Truncated 路径清洗):折到一半的调用,其参数同样
			// 可能停在非法 JSON 上,原样出境会被客户端当合法形状执行。
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
	// prebuilt 是轮首预建的请求体(build+指纹+Marshal 一次,见 Complete):
	// 同轮重试 body 不变,每 attempt 重建是最多 20 倍白算。nil 时 attempt
	// 回落本地组装(测试直调 attempt 时)。
	prebuilt *adapter.PrebuiltBody
	// clientEffort 与 effortEntry 是轮首算好的 effort 推导(同轮不变),
	// attempt 不再每轮重算 buildEffortEntry。
	clientEffort string
	effortEntry  effort.Entry
}

// attempt runs one attempt on one exit. Returns the folded Outcome, the
// finish reason of a successful end, whether any content was seen, and the
// classified failure (nil on success).
//
// adapter 的上行通道交出的是一组 Delta 事件(正文 / 推理 / tool-call 增量),
// 本函数是它到 Chunk 五种 Kind 的唯一翻译点:adapter 在 L3、engine 在 L4,
// adapter 不能反向 import engine,所以两侧各有一套词汇,在这里对齐。
//
// 收尾词汇有三个信源,优先级从高到低:投影层判出的「参数截在半截 JSON 上」
// (→ max-tokens)、上游自己报的收尾 token、以及「确实折出了工具调用」这个事实。
// 第一个信源只能来自投影层 —— 上游把被输出上限截断的一轮照样报成
// finish "tool_calls"(实测 2026-09-25),只看 token 会把一个不可执行的调用
// 交给 harness。
// buildAttemptRequest 把一次尝试的 adapter.Request 组装出来:与 attempt 内
// 原先的内联组装逐字一致,抽出来是为了让轮首预建与 attempt 共用同一组装,
// 不出现两处真相。
func buildAttemptRequest(in attemptInput, clientEffort string) adapter.Request {
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
	} else if v, ok := in.openAi["max_completion_tokens"].(float64); ok && v > 0 {
		// OpenAI 的新参数名(chat completions 的 max_completion_tokens):与
		// max_tokens 同义。过去只有老名字被读,新名字的输出上限被静默丢弃、
		// 回落到面板默认 —— 新 SDK 是会发新名字的。
		areq.MaxTokens = int(v)
	}
	// R19:客户端的停止序列要上线。adapter 写 payload["stop"] 的分支一直在,但全仓
	// 没有生产赋值点(JS 的 engine.js 同样不生产 options.stop),于是「调用方指定
	// stop」这件事在两条线上都是静默无效的。
	if stops := stopSequences(in.openAi["stop"]); len(stops) > 0 {
		areq.Stop = stops
	}
	if in.prebuilt != nil {
		areq.Prebuilt = in.prebuilt
	}
	return areq
}

func (e *Engine) attempt(ctx context.Context, in attemptInput) (Outcome, FinishReason, bool, *errors.Failure) {
	var out Outcome
	sawContent := false
	// 正文增量在这里用 Builder 攒,foldChunks 不再摸 Outcome.Text(长答案上的
	// `+=` 是 O(n²));每个返回点先 materialize 回填。
	var textSB strings.Builder
	materialize := func() { out.Text = textSB.String(); materializeToolCalls(&out) }
	emit := func(c Chunk) error {
		if c.Kind == ChunkText {
			textSB.WriteString(c.Text)
		}
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
	// 面板上的「默认输出上限」是设置里的活值,装配期的 AdapterDeps 只是模板
	// (B2):每请求覆盖,<=0 视同 null,保留模板兜底。
	if in.settings.DefaultMaxTokens > 0 {
		deps.MaxTokens = in.settings.DefaultMaxTokens
	}
	if deps.OnFirstToken == nil {
		// TTFT 回灌(magpie internal/gateway/ttft.go):adapter 在第一个
		// token 到达的那一刻就知道首字延迟,比等整条流读完再算准得多。
		deps.OnFirstToken = in.snapshot.Health.NoteTtft
	}

	clientEffort := in.clientEffort
	entry := in.effortEntry
	// 测试直调 attempt 时轮首字段为空:回落本地推导,与旧内联语义一致。
	if in.prebuilt == nil {
		var cw string
		cw, _ = in.openAi["reasoning_effort"].(string)
		entry, cw = buildEffortEntry(*in.entry, cw, in.settings.EffortLevel)
		clientEffort = cw
	}
	// 默认档来自设置(上游语义:harness 缺省 balanced),调用方可用
	// reasoning_effort 逐请求覆盖 —— 这条车道真正生效的旋钮是它导出的预算。
	deps.Entry = entry

	areq := buildAttemptRequest(in, clientEffort)

	res, err := adapter.NewAdapter(deps).Complete(ctx, areq, func(d adapter.Delta) error {
		return emit(chunkOfDelta(d))
	})
	if err != nil {
		failure := classifyAttemptError(err)
		if sawContent {
			// 断流:流被消费到一半。转发层靠这个 Finish 关帧,并且它绝不
			// 算成功 —— Complete 的分支 C 会把失败写进 Outcome。
			_ = emit(Chunk{Kind: ChunkFinish, Finish: FinishAbort})
			materialize()
			// 断流轮的 usage 不发给客户端也不进 Outcome:它属于一条失败
			// 的尝试,计数不可信;转发层的延迟发头因此也保得住。
			return out, FinishAbort, sawContent, failure
		}
		materialize()
		return out, FinishStop, sawContent, failure
	}
	// 三个 saw 位全空 + 正常收尾:退化完成。交给调用方一个「成功的空回合」是最
	// 难诊断的失败形态 —— 归成 EMPTY 才会走退避重试(js adapter.js:253-261)。
	// EMPTY 有自己的额度(engine.attemptCapByCode),扫池的 20 次预算不由它占用。
	if finishReasonOf(res.Finish) == FinishStop && !res.SawText && !res.SawToolCall && !res.SawReasoning && !res.BrokenToolCall {
		return out, FinishStop, sawContent,
			&errors.Failure{Code: check.CodeEmpty,
				Message: "our free model returned an empty response", Retryable: true}
	}
	finish := FinishStop
	switch {
	case res.BrokenToolCall:
		// 参数被截在 JSON 半截上的调用是不可执行的:降级成 max-tokens 让
		// Complete 分支 C 剪掉它(js adapter.js:239-252)。
		finish = FinishMaxTokens
	default:
		finish = finishReasonOf(res.Finish)
		if finish == FinishStop && len(out.ToolCalls) > 0 {
			// 上游没报收尾 token(或报的是 stop)却确实折出了调用:调用方要的
			// 信号比 token 诚实。转发层本来也按 ToolCalls 重新推导 finish。
			finish = FinishToolCalls
		}
	}
	if res.Usage.HasUsage {
		// usage 帧只在这一轮被判定为「最终交付」时才发给客户端:过去它在
		// 失败分类之前无条件 emit,而 usage 不算内容(sawContent 不为真),
		// 于是上游空回合的 usage 尾包会先把 SSE 头花掉、随后 EMPTY/超时
		// 被判可重试继续换出口 —— 「出口切换只在首字节前」的承诺被打破,
		// 客户端还会收到两帧 usage。失败轮的 usage 同样不可信,直接不发。
		if err2 := emit(Chunk{Kind: ChunkUsage, Usage: res.Usage}); err2 != nil {
			// 写失败 = 客户端已断开。必须用 Aborted(不在 retryOn):usage 不算
			// 内容,CodeServer 会让分支 A 换出口重试一整次,重试在首个增量处
			// 才被 stream.failed() 掐断 —— 白烧一次上游请求。
			materialize()
			return out, finish, sawContent,
				&errors.Failure{Code: check.CodeAborted, Message: "client gone: " + err2.Error()}
		}
	}
	if err2 := emit(Chunk{Kind: ChunkFinish, Finish: finish}); err2 != nil {
		// 收尾帧都发不出去 = 客户端已走。usage-only 轮(无内容块)上
		// sawContent=false,CodeServer 会进分支 A 白烧一整轮;统一 Aborted:
		// 有内容时分支 C 照旧(轮换本来就不发生),无内容时立即收场。
		materialize()
		return out, finish, sawContent,
			&errors.Failure{Code: check.CodeAborted, Message: "client gone: " + err2.Error()}
	}
	materialize()
	return out, finish, sawContent, nil
}

// chunkOfDelta 把 adapter 的投影事件翻译成本层的 Chunk 词汇。
func chunkOfDelta(d adapter.Delta) Chunk {
	switch d.Kind {
	case adapter.DeltaReasoning:
		return Chunk{Kind: ChunkReasoning, Text: d.Text}
	case adapter.DeltaToolCall:
		return ToolCallDeltaChunk(d.Index, d.ID, d.Name, d.Text)
	case adapter.DeltaToolCallEnd:
		return ToolCallBlockEndChunk(d.Index, d.ID, d.Name, d.Arguments)
	default:
		return Chunk{Kind: ChunkText, Text: d.Text}
	}
}

// finishReasonOf 对应 js stream.js:242-246 的 finishReason:三条线的收尾 token
// 词表不同(OpenAI 的 tool_calls/length、Anthropic 的 tool_use/max_tokens、
// Responses 的 function_call/incomplete),在这里一次性归一。缺失的 token 与
// 不认识的 token 都算正常收尾 —— JS 就是这么落的 default 分支。
func finishReasonOf(token string) FinishReason {
	switch token {
	case "tool_calls", "tool_use", "function_call":
		return FinishToolCalls
	case "length", "max_tokens", "max_output_tokens", "incomplete":
		return FinishMaxTokens
	default:
		return FinishStop
	}
}

// stopSequences 把请求体里的 `stop` 归一成字符串切片(R19)。OpenAI 允许裸字符串
// 与字符串数组两种写法;非字符串项与空串逐项丢掉 —— 停止序列少一个是行为差异,
// 为一个畸形项把整轮弄坏更不是。全空当「没给」,交给 adapter 的 len>0 判据省略。
func stopSequences(v any) []string {
	switch t := v.(type) {
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
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
		shown = make([]string, 0, 7)
		shown = append(shown, trail[:4]...)
		shown = append(shown, fmt.Sprintf("…%d个…", len(trail)-6))
		shown = append(shown, trail[len(trail)-2:]...)
	}
	// Build the body first and only add the separator when there is
	// something to separate. The JS version always joined a separator, so a
	// rotation-free turn printed "出口轮换 0 次后不可重试 …:  共 1012ms" with
	// a double space.
	body := ""
	if len(shown) > 0 {
		body = strings.Join(shown, " → ") + " "
	}
	elapsed := nowMS() - startedAt
	msg := fmt.Sprintf("出口轮换 %d 次后%s: %s共 %dms", len(trail), result, body, elapsed)
	// 慢轮标记:单轮超阈值(默认 15s)的请求在汇总行里显眼,排障不用离线翻
	// tracelog 的 jsonl。阈值内的成功轮换不加噪音。
	if elapsed > slowTurnThresholdMS {
		msg += " [慢轮]"
	}
	e.logf(msg)
	tr.record(result, body)
}

// slowTurnThresholdMS 是「慢轮」的阈值:单轮耗时超过它,轮换汇总行带标记。
const slowTurnThresholdMS = 15000

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

// bootUnix 是本进程启动的那一刻(O4)。/v1/models 的 created 用它,而不是每次调用
// 各取一次 time.Now():后者让同一份列表在两次轮询之间"刚更新过",按 created 做
// 缓存判断的客户端会反复认为是新数据。它表达的是「这份目录被本进程见到的时间」,
// 那本来就是一个进程生命周期内的常量。
var bootUnix = time.Now().Unix()

// ModelRows 是 /v1/models 的行。上游 issue #3:在所有已探测的存活出口上都测得
// 地区受限的模型,不再对外列出(面板仍展示并标记,便于观察 region 矩阵恢复)。
// free-lane 过滤是 Go 版的防御性补充:Build 已经过滤过,这里再挡一道手工拼进
// State 的目录行,付费 id 不可能从网关的列表里泄漏出去。
func (e *Engine) ModelRows() []Row {
	if e.deps.State == nil {
		return nil
	}
	snapshot := e.deps.State()
	// O13:「有没有任何一个 B 出口」在这一整轮里是常量,而 GatedUsable() 每次都要
	// 扫一遍 h.nodes(池上限 8000)。过去它藏在目录行的循环里 ⇒ /v1/models 每请求
	// 付 catalog × nodes 次扫描,拿到的还是同一个答案。
	gatedUsable := snapshot.Health.GatedUsable()
	rows := make([]Row, 0, len(snapshot.Catalog))
	for _, entry := range snapshot.Catalog {
		if !catalog.IsFreeLane(entry.ID) {
			continue
		}
		// 判据与 health.UnavailableEverywhere 逐字相同(单模型问答的那个 API),
		// 只是这里必须**在循环外**取 GatedUsable(O13):它对受限模型的每一次
		// 调用都要扫一遍 h.nodes,放在循环里就是 catalog × pool 次全表扫描,
		// 而这 `/v1/models` 一轮里答案根本不会变。
		if health.IsRestrictedModel(entry.ID) && !gatedUsable {
			continue
		}
		rows = append(rows, Row{ID: entry.ID, Object: "model", Created: bootUnix, OwnedBy: "lite-gateway"})
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
