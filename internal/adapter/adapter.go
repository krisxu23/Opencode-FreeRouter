// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package adapter speaks the three upstream wires the free lane exposes:
// chat/completions, responses, and the Anthropic-shaped messages path. They
// differ only in request encoding and response parsing, so they share one
// transport, one fingerprint and one failure classification.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/effort"
	"freerouter/internal/errors"
	"freerouter/internal/httpclient"
	"freerouter/internal/messages"
	"freerouter/internal/stream"
	"freerouter/internal/upstream"
)

// maxBodyBytes 封顶**错误信封**的读取:失败路径要的是错误信封,不是整个
// 响应体(计划:封顶 1MB)。readAllPrefix 的静默截断在这两条路上是正确语义
// —— 拿到开头就够分类了。
const maxBodyBytes = 1 << 20

// maxAnswerBodyBytes 封顶 2xx **整包 JSON 回答**的读取。与信封上限分开是
// 因为截断在这里有害无益:非流式的整包回答大小由模型的 maxOutput 决定。
// 最坏合法形状算得出来:space-bunny-free 的 maxOutput = 524288 token,一个
// token 在 JSON 里最坏摊 6 字节(CJK 单字转义成 \uXXXX),524288 × 6 ≈ 3MB;
// 8MB 给这个上界留 2.5× 余量,同时把恶意/被劫持上游能塞进**每一个在途请求**
// 的内存封顶住(审计纪律:限额只放宽到真实需要,不为「大一点更保险」放行)。
// 旧实现 readAllPrefix 在 1MB 处静默砍断 → json.Unmarshal 失败 → readJSON
// 报「unexpected non-SSE response」→ SERVER:分类撒谎(上游明明回了 JSON)、
// 不进 cooldownOn(坏出口不被冷却)、还在 retryOn 里 —— 一轮白扫 20 个出口,
// 每个出口把整篇回答**重新生成**一遍(上游为此计费!)。超过 8MB 的 2xx 体
// 不是合法回答,按 TRANSPORT 拒掉才是诚实的判决。
const maxAnswerBodyBytes = 8 << 20

// Deps is everything one adapter needs. A struct rather than a dozen arguments
// because app builds it once and hands it to the rotation engine per exit.
type Deps struct {
	Client    *http.Client
	Base      string
	Model     string
	Effort    string
	MaxTokens int
	Tools     []messages.Tool
	SessionID string
	Wire      upstream.Wire
	// NodeKey identifies the exit this adapter dials; it is what the
	// first-token report is attributed to.
	NodeKey string
	// OnFirstToken reports first-token latency at the instant the first delta
	// lands, not at the end of the stream: a 40s reply that answered in 300ms
	// must not delay the routing table learning that. Failures never report —
	// the time to a refused connection says nothing about token speed.
	OnFirstToken func(nodeKey string, ms int64)
	// Entry 是目录行的 effort 投影(计划骨架的最小扩展):build 要在本地算
	// max_tokens,而 budgetFor/resolveLevel 都需要模型自己的上限与推理支持位。
	// 不放 Request 是因为模型身份固定在 adapter 上,一轮之内不随请求变。
	Entry effort.Entry
	// Tools 是**原始**工具模式(计划骨架写 []messages.ToolDef,但 ToolDef 的
	// 线形状由构造时的 style 定死且不可从包外改写;JS 的 toToolDefs 是逐请求
	// 带着当前 wire 的 style 调的,所以 Deps 持原始模式、build 时套样式)。
}

// Request is one turn as handed in by the rotation engine.
type Request struct {
	Messages []messages.Message
	Stream   bool
	Model    string
	Stop     []string
	// Temperature:JS 只在「是有限数字」时写;Go 的零值兼作「未设」——显式 0
	// 与未设不可区分,统一按未设省略(实测它不在指纹闸门内,不惩罚省略)。
	Temperature float64
	// TurnSeed 把同一轮的重试钉在同一个 x-opencode-request 上:上游按请求 id
	// 做会话内画像,每试一次换一个 id 就是每分钟 N 个身份(js upstream.js:186)。
	TurnSeed string
	// MaxTokens 是客户端本轮显式要的输出上限(requested;计划骨架的最小扩展,
	// 注释见 Deps)。<=0 视为「未指定」,回落 Deps.MaxTokens(即
	// settings.defaultMaxTokens),再回落 Entry.MaxOutput —— effort.BudgetFor
	// 的三段回落链。
	MaxTokens int
	// ReasoningEffort 是客户端 reasoning_effort 的原词("" 视同未指定),与
	// Deps.Effort(设置档)一起在 build 时经 effort.ResolveLevel 折叠。
	ReasoningEffort string
}

// Adapter holds no mutable state; Complete is safe for concurrent use.
type Adapter struct{ deps Deps }

// NewAdapter returns an adapter bound to deps.
func NewAdapter(deps Deps) *Adapter { return &Adapter{deps: deps} }

// Wire reports which upstream shape this adapter speaks.
func (a *Adapter) Wire() upstream.Wire { return a.deps.Wire }

// turn 携带一次尝试在传输辅助函数之间穿行的全部状态。body 保留成 map 是因为
// stale-reasoning 重放要就地剥字段后再重序列化。
type turn struct {
	req     Request
	body    map[string]any
	payload []byte
	renames map[string]string
	t0      time.Time
}

// Complete performs one turn. onChunk receives the projected deltas — 正文、
// 推理、tool-call 三类增量,按到达顺序;从它返回 error 会中止这一轮并关掉上游
// body。返回值里的 Result 除了 usage,还带着只有投影层看得见的收尾事实。
func (a *Adapter) Complete(ctx context.Context, req Request, onChunk func(Delta) error) (Result, error) {
	t0 := time.Now()

	body, err := a.build(req)
	if err != nil {
		return Result{}, err
	}
	renames := upstream.ApplyFingerprint(body, a.deps.Wire == upstream.WireResponses)
	payload, err := json.Marshal(body)
	if err != nil {
		return Result{}, err
	}

	t := &turn{req: req, body: body, payload: payload, renames: renames, t0: t0}
	s := newSink(onChunk, renames)
	// 骨架的发送/状态处理段落落在 exchange:stale-reasoning 的重放路径必须能
	// 拿到剥过字段的 body map 与原 400 的分类素材,整段搬过去才可测。
	res, err := a.exchange(ctx, t, s)
	if err != nil {
		return res, err
	}
	// 收尾帧只在流被完整消费后补:出错帧/空闲截止会让这一轮从异常路径收场,
	// 半截的 tool-call 块此时不该被登记成一个调用(js readStream:327)。
	if err := s.closeAll(); err != nil {
		return s.result(), err
	}
	return s.result(), nil
}

// exchange 发一次请求并处理状态分岔:非 2xx 分类(400 命中过期推理引用时剥字段
// 重放一次),2xx 交 readReply。调用方保证 payload 与 body 同源。
func (a *Adapter) exchange(ctx context.Context, t *turn, s *sink) (Result, error) {
	httpReq, err := a.newRequest(ctx, t)
	if err != nil {
		return s.result(), err
	}
	resp, err := a.deps.Client.Do(httpReq)
	if err != nil {
		// 空闲截止先于 ctx 检查：NewStreamClient 的头阶段超时就是靠取消
		// 自己的 context 实现的，先看 ctx.Err() 会把它误判成「客户端中止」
		// （CodeAborted 不可重试、不冷却），而它其实是一个该换出口的 TIMEOUT。
		if stderrors.Is(err, httpclient.ErrIdleTimeout) {
			return s.result(), errors.Failure{Code: check.CodeTimeout, Message: err.Error(), Retryable: true}
		}
		// JS 对中止与传输失败分开记码(http.js:110-111):客户端取消不是
		// 「换个出口」能治的传输抖动,引擎对 CodeAborted 也不冷却。
		if ctx.Err() != nil {
			return s.result(), errors.Failure{Code: check.CodeAborted, Message: "request aborted"}
		}
		return s.result(), errors.Failure{Code: check.CodeTransport, Message: err.Error(), Retryable: true}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, rerr := readAllPrefix(resp.Body, maxBodyBytes)
		// A 400 naming a reasoning item the server no longer knows is not a
		// client error: drop the server-issued references and replay once.
		// Replaying unconditionally would double every genuine 400, and a
		// doubled 400 looks like the provider refusing the model.
		if resp.StatusCode == http.StatusBadRequest && upstream.IsStaleReasoningReference(string(raw)) {
			if upstream.StripStaleReasoningInputs(t.body) {
				return a.replay(ctx, t, s, resp.StatusCode, raw,
					errors.RetryAfter(resp.Header.Get("Retry-After")))
			}
		}
		return s.result(), classifyErrorBody(resp.StatusCode, raw, rerr,
			errors.RetryAfter(resp.Header.Get("Retry-After")))
	}
	if err := a.readReply(resp, t, s); err != nil {
		return s.result(), idleOrPassthrough(err)
	}
	return s.result(), nil
}

// classifyErrorBody 分类一个非 2xx 响应(R18)。
//
// 读错误不能丢:errors.Classify 里 **403 的 FreeTier 判据依赖 body 文案**
// (errors.go 的 freeRe),文案没读全就按「读到的是空文案」分类,会掉进
// `status == 401 || status == 403` 那条不可重试的凭证判决 —— 于是本应换出口的
// 403 变成对客户端的立即失败(engine 的 B 分支直接 return)。
//
// 所以:先拿已经读到的前缀分类(429/Region 这类可重试判决不依赖尾部,照抄);
// 只有当前缀不足以支撑一个**可重试**判决时,才把「没读完」本身当成故障 ——
// TRANSPORT 在引擎的 retryOn 与 cooldownOn 里,换出口并冷却这个出口是对的应对。
func classifyErrorBody(status int, raw []byte, readErr error, retryAfter int64) error {
	f := errors.Classify(status, raw, retryAfter)
	if readErr == nil || f.Retryable {
		return f
	}
	// 只有**判决依赖 body 文案**的那条改判才成立(整分支评审 BUG-3)。401 的判决
	// 只看状态码(errors.go:`case status == 401 || status == 403` 之前是 403 的
	// freeRe 文案判据),读全也不会改变它;把它升级成可重试的 TRANSPORT 恰好违反
	// 本仓库写在 errors.go 里的那条纪律 ——「401 是明确的凭证问题,把它降级成
	// 『可重试』会让一个配置错误变成扫全池的慢失败」:引擎会按 attemptCap=20、
	// 无墙钟把整个池子扫一遍并冷却 20 个出口,而客户端最后只看到 502。
	if status == http.StatusUnauthorized {
		return f
	}
	return errors.Failure{Code: check.CodeTransport, Status: status,
		Message:   "our-free-model: upstream error body unreadable: " + readErr.Error(),
		Retryable: true}
}

// idleOrPassthrough 把 httpclient 的空闲截止翻译成 TIMEOUT。
//
// 引擎的 retryOn 与 cooldownOn 都含 CodeTimeout：一个卡死的出口该被冷却并换掉。
// 不翻译的话它会掉进 classifyAttemptError 的 SERVER 兜底 —— 那等于把「这个出口
// 不回话了」记成「供应商故障」，同一个出口会被反复选中（B1 的另一半）。
func idleOrPassthrough(err error) error {
	if err == nil {
		return nil
	}
	if stderrors.Is(err, httpclient.ErrIdleTimeout) {
		return errors.Failure{Code: check.CodeTimeout, Message: err.Error(), Retryable: true}
	}
	return err
}

// replay 把剥过字段的 body 重发**一次**(js http.js:128-136)。重放又被拒时按
// 重放的响应分类,不再剥第二次;重放连传输都没走通时回落**原始** 400 的分类
// —— 第二条重试连不上,不改变第一条失败的形状。
func (a *Adapter) replay(ctx context.Context, t *turn, s *sink, status int, raw []byte, retryAfter int64) (Result, error) {
	payload, err := json.Marshal(t.body)
	if err != nil {
		return s.result(), err
	}
	retry := &turn{req: t.req, body: t.body, payload: payload, renames: t.renames, t0: t.t0}
	httpReq, err := a.newRequest(ctx, retry)
	if err != nil {
		return s.result(), err
	}
	resp, err := a.deps.Client.Do(httpReq)
	if err != nil {
		// 重放那一发**卡死**时必须先认空闲截止(整分支评审 BUG-2):回落成原始
		// 400 的判决(SERVER,不可重试、不冷却)等于把「这个出口不回话了」记成
		// 「请求本身有问题」,同一个卡死的出口下一轮还会被选中 —— exchange 里
		// 同一条翻译写在 Client.Do 的失败分支上,replay 漏了。
		if stderrors.Is(err, httpclient.ErrIdleTimeout) {
			return s.result(), errors.Failure{Code: check.CodeTimeout, Message: err.Error(), Retryable: true}
		}
		// 其余传输失败回落**原始** 400 的分类:第二条重试连不上,不改变第一条
		// 失败的形状。
		return s.result(), errors.Classify(status, raw, retryAfter)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		replayRaw, replayRErr := readAllPrefix(resp.Body, maxBodyBytes)
		return s.result(), classifyErrorBody(resp.StatusCode, replayRaw, replayRErr,
			errors.RetryAfter(resp.Header.Get("Retry-After")))
	}
	if err := a.readReply(resp, retry, s); err != nil {
		// 与 exchange 同一层翻译:重放期间的空闲截止同样是 TIMEOUT(retryOn +
		// cooldownOn),不是 SERVER。
		return s.result(), idleOrPassthrough(err)
	}
	return s.result(), nil
}

// newRequest 组装 POST:端点按模型选线(js adapter.js:190),指纹头由 upstream
// 出,会话与请求 id 分别由会话 id 与轮次种子派生 —— 同一轮重试复用同一 id。
func (a *Adapter) newRequest(ctx context.Context, t *turn) (*http.Request, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.deps.Base+upstream.EndpointFor(a.deps.Model), bytes.NewReader(t.payload))
	if err != nil {
		return nil, err
	}
	session := upstream.SessionForConversation(a.deps.SessionID)
	for name, value := range upstream.GatewayHeaders(upstream.HeaderOptions{
		Session:   session,
		RequestID: upstream.RequestIDFor(session, t.req.TurnSeed),
		Stream:    t.req.Stream,
	}) {
		httpReq.Header.Set(name, value)
	}
	// anthropic-version 只在 messages 线上带。JS 把 ANTHROPIC_API_VERSION 定义
	// 并导出却从未挂到任何请求上(死导出);真实的 Anthropic Messages API 要求
	// 该头,计划的端点测试也断言它,故在此按线补挂。
	if a.deps.Wire == upstream.WireMessages {
		httpReq.Header.Set("anthropic-version", upstream.AnthropicAPIVersion)
	}
	return httpReq, nil
}

// build 按三条线构造请求体(js adapter.js:278-309 buildPayload + :176-185 的
// 工具/温度/stop/指纹周边)。
func (a *Adapter) build(req Request) (map[string]any, error) {
	// repairToolPairing 在 wire 转换之前(js adapter.js:172):一轮被打断会在
	// 持久历史里留下孤立的 tool_result,回放它不只是难看 —— 免费车道会答 400
	// 并且拖死该会话之后的每一轮。在这里修一次,三条线同时覆盖。
	repaired := messages.RepairToolPairing(req.Messages)
	level := effort.ResolveLevel(req.ReasoningEffort, a.deps.Effort, a.deps.Entry)
	var settingsDefault *int
	if a.deps.MaxTokens > 0 { // Deps.MaxTokens 就是 settings.defaultMaxTokens 的解析结果;<=0 视同 null
		v := a.deps.MaxTokens
		settingsDefault = &v
	}
	budget := effort.BudgetFor(level, a.deps.Entry, req.MaxTokens, settingsDefault)

	var payload map[string]any
	var tools []messages.ToolDef
	switch a.deps.Wire {
	case upstream.WireResponses:
		input, _, err := messages.ToResponseInput(repaired, nil)
		if err != nil {
			return nil, err
		}
		if len(input) == 0 {
			// 全被投影丢掉时给一个占位轮:空 input 会被上游拒
			input = []messages.ResponseItem{{Type: "message", Role: "user",
				Content: []messages.RespPart{{Type: "input_text", Text: "..."}}}}
		}
		payload = map[string]any{
			"model":             a.deps.Model,
			"input":             input,
			"stream":            true,
			"store":             false,
			"max_output_tokens": budget,
		}
		tools = messages.ToolDefs(a.deps.Tools, messages.ToolStyleFlat)
	case upstream.WireMessages:
		shape, _, err := messages.ToClaudeMessages(repaired, nil)
		if err != nil {
			return nil, err
		}
		if len(shape.Messages) == 0 {
			// 历史被投影全部丢掉时空 messages 会被上游拒 400;400 落进
			// errors.Classify 的 default ⇒ SERVER,而 SERVER 在 engine 的 retryOn
			// 里 ⇒ 同一颗注定失败的请求被真实重发 20 次才变成 503(R6)。
			// responses 线早就有这一层占位轮(上面的 input 守卫),这里补齐两条。
			// "text" 是 messages 包内部 blockText 常量的线上形状。
			shape.Messages = []messages.ClaudeMessage{{Role: "user",
				Content: []messages.ClaudeBlock{{Type: "text", Text: "..."}}}}
		}
		payload = map[string]any{
			"model":      a.deps.Model,
			"messages":   shape.Messages,
			"stream":     true,
			"max_tokens": budget,
		}
		if shape.System != "" {
			payload["system"] = shape.System
		}
		tools = messages.ToolDefs(a.deps.Tools, messages.ToolStyleClaude)
	default: // chat
		chat, _, err := messages.ToChatMessages(repaired, nil)
		if err != nil {
			return nil, err
		}
		if len(chat) == 0 {
			// 同 R6:空 messages 数组换 400,400 被当 5xx 重试满 20 次。
			chat = []messages.ChatMessage{{Role: "user", Content: "..."}}
		}
		payload = map[string]any{
			"model":      a.deps.Model,
			"messages":   chat,
			"stream":     true,
			"max_tokens": budget,
		}
		tools = messages.ToolDefs(a.deps.Tools, messages.ToolStyleChat)
	}
	// 免费档的闸门是对声明工具集的指纹校验,调用方一个工具都不带也要过
	// (js adapter.js:181-183)。
	if len(tools) > 0 {
		payload["tools"] = tools
	}
	if req.Temperature != 0 && !math.IsNaN(req.Temperature) && !math.IsInf(req.Temperature, 0) {
		payload["temperature"] = req.Temperature
	}
	// responses 线没有 stop 形状;chat 与 messages 线照写(js adapter.js:179)。
	if a.deps.Wire != upstream.WireResponses && len(req.Stop) > 0 {
		payload["stop"] = req.Stop
	}
	// chat 线强制 usage 尾包:折叠回非流式的回复也要报得出 token
	// (js adapter.js:184-185,移植自 opencode2api 的 ensureAnonymousChatUsage)。
	if a.deps.Wire == upstream.WireChat {
		upstream.EnsureChatUsage(payload)
	}
	return payload, nil
}

var (
	// 首块形状判定(js http.js:264-270):SSE 帧头与 JSON 首字符各一条,
	// 形状不明才回落 Content-Type。
	headSSERe  = regexp.MustCompile(`^\s*(data:|event:|id:|retry:|:)`)
	headJSONRe = regexp.MustCompile(`^\s*[[{]`)
)

// readReply 分两条分支消费 2xx 响应体。
//
// 上游 issue #6:高负载下网关会返回非 event-stream 的 Content-Type 但 SSE 形状
// 的响应体 —— 仅凭 Content-Type 分支会把整条流当 JSON 误杀。所以先嗅探首块
// 字节按形状分类,Content-Type 只作兜底,且永不把流式响应整条读进内存
// (js http.js:143-174 的实现语义)。
func (a *Adapter) readReply(resp *http.Response, t *turn, s *sink) error {
	head := make([]byte, 8)
	n, _ := io.ReadFull(resp.Body, head)
	head = head[:n]
	if n == 0 {
		// JS:body 为空的 2xx 是「上游什么都没产」,归 EMPTY_RESPONSE —— 它在
		// 引擎的可重试码表里,换出口是对的应对。
		return errors.Failure{Code: check.CodeEmpty, Message: "our-free-model: upstream returned no body", Retryable: true}
	}
	body := io.MultiReader(bytes.NewReader(head), resp.Body)
	if headSSERe.Match(head) || (!headJSONRe.Match(head) && strings.Contains(resp.Header.Get("Content-Type"), "event-stream")) {
		return a.readSSE(body, t, s)
	}
	// 整包回答用**严格**读取(上游层审计):readAllPrefix 在 1MB 处静默砍断,
	// 一份合法的大 JSON 回答会被截成不可解析,再被 readJSON 的守卫报成
	// 「unexpected non-SSE response」→ SERVER。分类在这里撒了三次谎:上游
	// 明明回了 JSON;SERVER 不进 cooldownOn(坏出口不会被冷却,下一轮还会
	// 选中它);SERVER 在 retryOn 里 —— 一轮白扫 20 个出口,每个出口把这
	// 一整篇回答**重新生成**一遍,上游按次计费。超过 maxAnswerBodyBytes
	// 的 2xx 体不是合法回答,按 bodyReadFailure 既有的 R18 语义归
	// TRANSPORT(轮换且冷却),而不是继续骗成 SERVER。
	raw, readErr := httpclient.ReadCapped(body, maxAnswerBodyBytes)
	if readErr != nil {
		return bodyReadFailure(readErr)
	}
	return a.readJSON(raw, resp.StatusCode, errors.RetryAfter(resp.Header.Get("Retry-After")), t, s)
}

// bodyReadFailure 把一次 2xx 响应体读故障翻译成引擎认识的码(R18 的第三个点)。
//
// 空闲截止仍按 B1 的语义交给 idleOrPassthrough(它认这个哨兵);其余都是
// TRANSPORT —— 整包没读全不等于「上游回了奇怪的东西」,而 readJSON 的守卫会把
// 截断的 JSON 报成 SERVER,那条既不说这条连接不可信,也不给冷却表任何线索。
func bodyReadFailure(err error) error {
	if stderrors.Is(err, httpclient.ErrIdleTimeout) {
		return err
	}
	return errors.Failure{Code: check.CodeTransport,
		Message: "our-free-model: upstream body unreadable: " + err.Error(), Retryable: true}
}

// readSSE 逐事件喂 feed;[DONE] 与非 JSON 帧在 feed 的入口被跳过。
func (a *Adapter) readSSE(r io.Reader, t *turn, s *sink) error {
	return stream.ReadSSE(r, func(ev stream.Event) error {
		return a.feed(t, []byte(ev.Data), 0, s)
	})
}

// readJSON 消费非流式的整包回复(js http.js:157-170 的 JSON 分支)。usage 的
// 字段名以 JS readStream 的非流式分支为准(mapUsage 的双拼写);全文一次性
// 交出 —— 这是 Go 版对 JS 的一个刻意补齐:JS 的 feedChat 只读 delta,非流式
// JSON 的正文会被丢掉,而计划要求上层拿到全文。
func (a *Adapter) readJSON(raw []byte, status int, retryAfter int64, t *turn, s *sink) error {
	var p map[string]any
	if err := json.Unmarshal(raw, &p); err != nil || p == nil {
		return errors.Failure{Code: check.CodeServer,
			Message: "our-free-model: unexpected non-SSE response: " + snippet(raw)}
	}
	if e, has := p["error"]; has && e != nil {
		// 显式 "error": null 的 2xx 体不是错误信封 —— 按它分类会把一次
		// 正常响应记成一次上游失败。
		return errors.Classify(status, raw, retryAfter)
	}
	if err := a.feed(t, raw, status, s); err != nil {
		return err
	}
	// 终止原因也要补:feedChat 在 delta==nil 时提前 continue(读不到
	// finish_reason),feedClaude/feedResponses 按 type 分派对整包体 no-op ——
	// 截断的整包回答过去被判 FinishStop,客户端拿到 "stop"/"completed" 的
	// 腰斩回答不会续写(H1/BUG-1 同症状的第三扇门)。
	if token := finishOfFullBody(p, a.deps.Wire); token != "" {
		s.setFinish(token)
	}
	if text := fullTextOf(p, a.deps.Wire); text != "" {
		if !s.first {
			s.first = true
			s.acc.TTFTMS = time.Since(t.t0).Milliseconds()
			a.reportTtft(s.acc.TTFTMS)
		}
		// 走 sink 而不是直接回调:正文增量也得进 SawText 的账,否则一条合法的
		// 非流式回复会被上层判成「空响应」。
		if err := s.text("t", text); err != nil {
			return err
		}
	}
	// 非流式整包里的工具调用同样要补齐(过去只补了正文):feed 只处理 delta
	// 形状,整包回复里的 message.tool_calls / tool_use / function_call 会被
	// 丢光 —— 调用方拿到一条带 finish 的回复却没有任何可执行的调用。经 sink
	// 投影,start 记 SawToolCall 的账、closeAll 的 block-end 帧把参数交出去。
	for i, call := range fullToolCallsOf(p, a.deps.Wire) {
		key := "j" + strconv.Itoa(i)
		s.toolStart(key, call.ID, upstream.RestoreToolName(call.Name, s.renames))
		if call.Arguments != "" {
			if err := s.toolArgs(key, call.Arguments); err != nil {
				return err
			}
		}
	}
	return nil
}

// fullToolCall 是从非流式 JSON 里取出的一个完整工具调用。
type fullToolCall struct{ ID, Name, Arguments string }

// fullToolCallsOf 从非流式 JSON 里取出全部工具调用(逐线形状;feed 只处理
// delta 形状,这是它的非流式补集)。Arguments 统一成 JSON 文本:chat/responses
// 线上本来就是字符串,claude 的 input 是对象,序列化一次。没有 id 也没有名字
// 的条目是畸形数据,跳过。
func fullToolCallsOf(p map[string]any, wire upstream.Wire) []fullToolCall {
	var calls []fullToolCall
	appendCall := func(id, name, args string) {
		if name == "" && id == "" {
			return
		}
		calls = append(calls, fullToolCall{ID: id, Name: name, Arguments: args})
	}
	switch wire {
	case upstream.WireChat:
		choices, _ := p["choices"].([]any)
		for _, choice := range choices {
			cm, ok := choice.(map[string]any)
			if !ok {
				continue
			}
			msg, _ := cm["message"].(map[string]any)
			if msg == nil {
				continue
			}
			list, _ := msg["tool_calls"].([]any)
			for _, raw := range list {
				call, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				fn, _ := call["function"].(map[string]any)
				id, _ := call["id"].(string)
				name, _ := fn["name"].(string)
				args, _ := fn["arguments"].(string)
				appendCall(id, name, args)
			}
		}
	case upstream.WireMessages:
		blocks, _ := p["content"].([]any)
		for _, block := range blocks {
			bm, ok := block.(map[string]any)
			if !ok || bm["type"] != "tool_use" {
				continue
			}
			id, _ := bm["id"].(string)
			name, _ := bm["name"].(string)
			args := "{}"
			if input, has := bm["input"]; has && input != nil {
				if b, err := json.Marshal(input); err == nil {
					args = string(b)
				}
			}
			appendCall(id, name, args)
		}
	default: // responses
		items, _ := p["output"].([]any)
		for _, item := range items {
			im, ok := item.(map[string]any)
			if !ok || im["type"] != "function_call" {
				continue
			}
			id, _ := im["call_id"].(string)
			if id == "" {
				id, _ = im["id"].(string)
			}
			name, _ := im["name"].(string)
			args, _ := im["arguments"].(string)
			appendCall(id, name, args)
		}
	}
	return calls
}

// feed 是三条线共用的逐帧投影入口:认错误帧、锚 TTFT、折 usage,然后把这一帧
// 交给所属线对的投影函数。投影本身(开块、累积 tool-call 参数、还原工具名)住在
// project.go —— 上行交出的是 Delta 事件(正文/推理/tool-call 增量),而不是
// 只有正文:reasoning 与 function call 都在同一批帧里,只搬 delta.content 会让
// 调用方的工具永远执行不到(审计 B13)。
func (a *Adapter) feed(t *turn, chunk []byte, status int, s *sink) error {
	var p map[string]any
	if err := json.Unmarshal(chunk, &p); err != nil {
		return nil // 畸形帧与 [DONE] 一律跳过(js readStream:300-302 的 try/catch)
	}
	// 流内错误帧按错误**信封**同款分类(js stream.js:303-315):分类错成
	// 可重试码会让 harness 重发一个已流出半个答案的轮次。
	if p["type"] == "error" || p["error"] != nil {
		return errors.Classify(status, chunk, 0)
	}
	// TTFT 锚在第一个 delta 上,而不是第一个可见文本上:推理重的模型几分钟
	// 后才吐可见文本(js carriesDelta:333-353)。
	if a.carriesDelta(p) && !s.first {
		s.first = true
		s.acc.TTFTMS = time.Since(t.t0).Milliseconds()
		a.reportTtft(s.acc.TTFTMS)
	}
	prior := s.acc
	// ScanUsage 折叠顶层 usage(chat 线的双拼写 + cache 细节)与 claude 文本
	// 增量检测;三条线的嵌套落点与合并缺口在下面各线的补充里修。
	//
	// O14 的一半**不做**：同一帧确实被解了两遍(上面一次进 map 供 carriesDelta 与
	// 三条线的投影用,这里一次进带指针的结构体)。合并不是免费的 —— B4/R14 依赖
	// `*int64` 区分「键缺席」与「显式 0」,而 map 里读出来只有 any,那条区分要么
	// 在 map 侧重写一遍、要么丢掉编译期的字段约束。两趟解码都是 O(帧大小),帧本身
	// 被 SSE 上限钉在 8MB 内、常态只有几百字节;相比之下把判据写坏的代价是面板用量
	// 与出口粘性定档整体失真 —— B4/R14 正是这类缺陷。
	stream.ScanUsage(chunk, &s.acc, &s.first, t.t0)
	switch a.deps.Wire {
	case upstream.WireChat:
		return feedChat(p, s)
	case upstream.WireResponses:
		feedResponsesUsage(p, &s.acc)
		return feedResponses(p, s)
	default:
		feedClaudeUsage(p, prior, &s.acc)
		return feedClaude(p, s)
	}
}

// carriesDelta 对应 js stream.js:333-353。`choices` 是外部数据:可能是 [null],
// 也可能根本不是数组 —— 这条判断跑在流的最开头,是畸形帧最容易打到的地方。
func (a *Adapter) carriesDelta(p map[string]any) bool {
	switch a.deps.Wire {
	case upstream.WireChat:
		choices, ok := p["choices"].([]any)
		if !ok {
			return false
		}
		for _, choice := range choices {
			cm, ok := choice.(map[string]any)
			if !ok {
				continue
			}
			delta, _ := cm["delta"].(map[string]any)
			if delta == nil {
				continue
			}
			if s, _ := delta["content"].(string); s != "" {
				return true
			}
			if s, _ := delta["reasoning"].(string); s != "" {
				return true
			}
			if details, ok := delta["reasoning_details"].([]any); ok {
				for _, part := range details {
					pm, _ := part.(map[string]any)
					if s, _ := pm["text"].(string); s != "" {
						return true
					}
				}
			}
			if calls, ok := delta["tool_calls"].([]any); ok && len(calls) > 0 {
				return true
			}
		}
		return false
	case upstream.WireResponses:
		if s, _ := p["delta"].(string); s != "" {
			return true
		}
		return p["type"] == "response.output_item.added"
	default:
		return p["type"] == "content_block_delta" || p["type"] == "content_block_start"
	}
}

// feedChat 提取 chat 线的文本增量。畸形 choice([null]/"nope")跳过而不是
// 抛 TypeError —— 上游帧是外部数据,那个 TypeError 曾让调用方拿到裸错误
// (js feedChat:143-146 的实测注释)。
//
// feedChat/feedClaude/feedResponses 的逐帧投影住在 project.go:那里按槽位开块、
// 累积 tool-call 参数,并把推理增量一并交出。

// feedClaudeUsage 修 claude 线 usage 的两个 ScanUsage 盲区:
//   - message_start 的 usage 藏在 message 下,且缓存计数字段名是
//     cache_read_input_tokens(ScanUsage 只认顶层与 OpenAI 细节拼写);
//   - message_delta 只带 {output_tokens},ScanUsage 折叠时会把 In 清零 ——
//     js stream.js:284-293 的合并规则:只带输出侧的报告并入既有值。
func feedClaudeUsage(p map[string]any, prior stream.Usage, acc *stream.Usage) {
	usage, _ := p["usage"].(map[string]any)
	switch p["type"] {
	case "message_start":
		msg, _ := p["message"].(map[string]any)
		if msg != nil {
			usage, _ = msg["usage"].(map[string]any)
		}
		if usage == nil {
			return
		}
		acc.HasUsage = true
		if v, ok := numOf(usage["cache_read_input_tokens"]); ok && v > 0 {
			acc.CacheRead = v
		}
		if v, ok := numOf(usage["input_tokens"]); ok {
			// disjoint-count:input_tokens 是毛值,减去缓存命中才是 harness 的
			// 未缓存输入(js stream.js:122 的 Math.max(0, prompt - cached))。
			acc.In = v - acc.CacheRead
			if acc.In < 0 {
				acc.In = 0
			}
		}
		if v, ok := numOf(usage["output_tokens"]); ok {
			acc.Out = v
		}
	case "message_delta":
		if usage == nil {
			return
		}
		if _, has := numOf(usage["input_tokens"]); !has {
			acc.In = prior.In
			acc.CacheRead = prior.CacheRead
		}
	default:
		// 非流式 JSON:cache_read_input_tokens 在顶层 usage 里
		if usage == nil {
			return
		}
		if v, ok := numOf(usage["cache_read_input_tokens"]); ok && v > 0 {
			// ScanUsage 只认 OpenAI 的 cached_tokens 拼写,所以它刚把 In 记成
			// 了毛值:这里补做那次减法,只减新学到的差额以免重复扣。
			delta := v - acc.CacheRead
			acc.CacheRead = v
			if delta > 0 {
				acc.In -= delta
				if acc.In < 0 {
					acc.In = 0
				}
			}
		}
	}
}

// feedResponsesUsage 修 responses 线的 usage 盲区:usage 挂在 response 下,
// ScanUsage 的顶层折叠看不到它(js feedResponses:221-236)。completed 与
// incomplete 都要认 —— 被截断的轮次上游发的是独立终止事件 incomplete,载荷
// 同形且带 usage;只认 completed 的话,恰恰是最该记账的长输出截断轮
// (muse-spark 家族)usage 恒 0,RecordUsage/NoteStickyUsage 全部漏账。
func feedResponsesUsage(p map[string]any, acc *stream.Usage) {
	if p["type"] != "response.completed" && p["type"] != "response.incomplete" {
		return
	}
	resp, _ := p["response"].(map[string]any)
	if resp == nil {
		return
	}
	usage, _ := resp["usage"].(map[string]any)
	if usage == nil {
		return
	}
	acc.HasUsage = true
	if details, ok := usage["input_tokens_details"].(map[string]any); ok {
		if v, ok := numOf(details["cached_tokens"]); ok && v > 0 {
			acc.CacheRead = v
		}
	}
	if v, ok := numOf(usage["input_tokens"]); ok {
		// disjoint-count:input_tokens 是毛值(含缓存命中),harness 的
		// inputTokens 只记未缓存部分(js stream.js:122)。
		acc.In = v - acc.CacheRead
		if acc.In < 0 {
			acc.In = 0
		}
	}
	if v, ok := numOf(usage["output_tokens"]); ok {
		acc.Out = v
	}
}

// fullTextOf 从非流式 JSON 里取出全文(逐线形状;feed 只处理 delta 形状,这条
// 是它的非流式补集)。
func fullTextOf(p map[string]any, wire upstream.Wire) string {
	var parts []string
	appendText := func(s string) {
		if s != "" {
			parts = append(parts, s)
		}
	}
	switch wire {
	case upstream.WireChat:
		choices, _ := p["choices"].([]any)
		for _, choice := range choices {
			cm, ok := choice.(map[string]any)
			if !ok {
				continue
			}
			msg, _ := cm["message"].(map[string]any)
			if msg == nil {
				continue
			}
			if s, ok := msg["content"].(string); ok {
				appendText(s)
				continue
			}
			if blocks, ok := msg["content"].([]any); ok {
				for _, block := range blocks {
					bm, _ := block.(map[string]any)
					if bm != nil && bm["type"] == "text" {
						s, _ := bm["text"].(string)
						appendText(s)
					}
				}
			}
		}
	case upstream.WireMessages:
		blocks, _ := p["content"].([]any)
		for _, block := range blocks {
			bm, ok := block.(map[string]any)
			if ok && bm["type"] == "text" {
				s, _ := bm["text"].(string)
				appendText(s)
			}
		}
	default:
		items, _ := p["output"].([]any)
		for _, item := range items {
			im, ok := item.(map[string]any)
			if !ok || im["type"] != "message" {
				continue
			}
			blocks, _ := im["content"].([]any)
			for _, block := range blocks {
				bm, _ := block.(map[string]any)
				if bm != nil && bm["type"] == "output_text" {
					s, _ := bm["text"].(string)
					appendText(s)
				}
			}
		}
	}
	return strings.Join(parts, "")
}

// reportTtft 把首 token 延迟报给路由层。panic 必须被吞掉:路由信号绝不能打断
// 一轮请求(js adapter.js:61-66);OnFirstToken 为 nil 时静默(引擎任务 18 才
// 会装配它)。
func (a *Adapter) reportTtft(ms int64) {
	if a.deps.OnFirstToken == nil {
		return
	}
	defer func() { _ = recover() }()
	a.deps.OnFirstToken(a.deps.NodeKey, ms)
}

// numOf 取 JSON 数字(json.Unmarshal 把数字解成 float64)。
func numOf(v any) (int64, bool) {
	f, ok := v.(float64)
	if !ok {
		return 0, false
	}
	return int64(f), true
}

// snippet 把非 JSON 响应体的开头截进错误消息(js http.js:166 的 slice(0,200);
// 字节截断即可 —— 它只进错误消息,不再被解析)。
func snippet(raw []byte) string {
	if len(raw) > 200 {
		return string(raw[:200])
	}
	return string(raw)
}

// readAllPrefix 读**前缀**:至多 limit 字节,超出的部分静默丢弃、不报错
// (非 2xx 响应体封顶 1MB —— 失败路径要的是错误信封,不是整个响应体)。
// 与 httpclient.ReadCapped(严格模式:超限一个字节就报错)语义不同、名字相近
// 曾是误用陷阱,改名以示区分:前缀用于「只需要开头就能分类」的场合,严格版
// 用于「多读一个字节都算违约」的场合。
func readAllPrefix(r io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, limit))
}

// finishOfFullBody 从非流式整包里读终止原因(feed 只处理 delta 形状,这是它的
// 非流式补集):chat 的 choices[].finish_reason、claude 的顶层 stop_reason、
// responses 的顶层 status + incomplete_details —— 映射与 feedResponses 流式
// 分支同一套(reason=max_output_tokens → "length")。空串与 "stop" 同价
// (engine.finishReasonOf 的 default 分支)。
func finishOfFullBody(p map[string]any, wire upstream.Wire) string {
	switch wire {
	case upstream.WireChat:
		token := ""
		choices, _ := p["choices"].([]any)
		for _, choice := range choices {
			cm, ok := choice.(map[string]any)
			if !ok {
				continue
			}
			if s, ok := cm["finish_reason"].(string); ok && s != "" {
				token = s
			}
		}
		return token
	case upstream.WireMessages:
		s, _ := p["stop_reason"].(string)
		return s
	default: // responses:整包就是 response 对象本身(流式才包在 response 键下)
		status, _ := p["status"].(string)
		details, _ := p["incomplete_details"].(map[string]any)
		reason, _ := details["reason"].(string)
		switch {
		case reason == "max_output_tokens":
			return "length"
		case status == "completed":
			return "stop"
		default:
			return status
		}
	}
}
