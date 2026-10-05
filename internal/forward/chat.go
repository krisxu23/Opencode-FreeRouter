// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package forward

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"freerouter/internal/engine"
	frerrors "freerouter/internal/errors"
	"freerouter/internal/stream"
)

// 本文件是转发层的**线路形状**(wire shapes)。一律用带 json tag 的结构体而不是
// map[string]any:Go 的 encoding/json 对 map 按字母序排键,而 JS 的
// JSON.stringify 按插入序 —— 用 map 会让 `{id,object,created,model,choices}`
// 变成 `{choices,created,id,model,object}`,字节不同、语义相同。任务 27 的差分
// 验收要逐字节比两个版本的响应,所以这里的字段顺序是照 src/forward.js 抄的。

// ---- 错误体 ----

// openAIErrorBody 照 js :84-86。param/code 恒为 null:这个网关没有它们的语义,
// 给个 null 比编一个值诚实。
type openAIErrorBody struct {
	Error openAIErrorDetail `json:"error"`
}

type openAIErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   any    `json:"param"`
	Code    any    `json:"code"`
}

// sseErrorBody 照 js :42-44:流内的错误事件。retryable 只在为真时出现 ——
// JS 用的是 `...(retryable ? {retryable:true} : {})`,不是 `retryable:false`。
type sseErrorBody struct {
	Error sseErrorDetail `json:"error"`
}

type sseErrorDetail struct {
	Message   string `json:"message"`
	Type      string `json:"type"`
	Retryable bool   `json:"retryable,omitempty"`
}

// ---- 探活与模型清单 ----

type healthBody struct {
	OK      bool   `json:"ok"`
	Service string `json:"service"`
}

type modelsBody struct {
	Object string       `json:"object"`
	Data   []engine.Row `json:"data"`
}

// ---- usage ----

// promptTokensDetails / completionTokensDetails 的值恒出现(js 不省零),
// 而它们所在的父键只在**真有 usage** 时出现 —— 所以父键用指针 + omitempty。
type promptTokensDetails struct {
	CachedTokens int64 `json:"cached_tokens"`
}

type completionTokensDetails struct {
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

// openAIUsage 是 chat 线路的 usage 形状,字段顺序照 js :335-341。
type openAIUsage struct {
	PromptTokens            int64                    `json:"prompt_tokens"`
	CompletionTokens        int64                    `json:"completion_tokens"`
	TotalTokens             int64                    `json:"total_tokens"`
	PromptTokensDetails     *promptTokensDetails     `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *completionTokensDetails `json:"completion_tokens_details,omitempty"`
}

// openAIUsageOf 把 harness 形状的 usage 转成 OpenAI 形状。
//
// 算术在 engine.OpenAIUsageTotalsOf(单一事实来源),这里直接逐字段抄进有序
// 结构体 —— 旧路径先在 engine 侧造 map、再按字符串键抄回来,热路径上每个
// 响应多一轮分配。第二返回值对应 `usage === undefined`:没有 usage 时
// **整个 details 段都不出现**,调用方拿到的就是三个 0 —— 与 js :231 的
// `outcome.usage ?? {...}` 一致。
func openAIUsageOf(u stream.Usage) (openAIUsage, bool) {
	if !u.HasUsage {
		return openAIUsage{}, false
	}
	t := engine.OpenAIUsageTotalsOf(u)
	return openAIUsage{
		PromptTokens:            t.PromptTokens,
		CompletionTokens:        t.CompletionTokens,
		TotalTokens:             t.TotalTokens,
		PromptTokensDetails:     &promptTokensDetails{CachedTokens: t.CachedTokens},
		CompletionTokensDetails: &completionTokensDetails{ReasoningTokens: 0},
	}, true
}

// ---- chat.completion(非流式) ----

type openAIToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type openAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIToolFunction `json:"function"`
}

type chatMessage struct {
	Role      string           `json:"role"`
	Content   *string          `json:"content"`
	ToolCalls []openAIToolCall `json:"tool_calls,omitempty"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatCompletionBody struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   openAIUsage  `json:"usage"`
	// Error 只在「已出内容后断流」的非流式响应上出现:半截回答仍按 200 交付
	// (重试会重发前缀,js 同构),但没有这个标记的话,调用方看到的是与完整
	// 回答不可区分的 finish_reason:stop。成功路径恒不出现(omitempty)。
	Error *openAIErrorDetail `json:"error,omitempty"`
}

// ---- chat.completion.chunk(流式) ----

type toolFuncDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments"`
}

type toolCallDelta struct {
	Index    int            `json:"index"`
	ID       string         `json:"id,omitempty"`
	Function *toolFuncDelta `json:"function,omitempty"`
}

type chunkDelta struct {
	Role      string          `json:"role,omitempty"`
	Content   *string         `json:"content,omitempty"`
	Reasoning *string         `json:"reasoning,omitempty"`
	ToolCalls []toolCallDelta `json:"tool_calls,omitempty"`
}

type chunkChoice struct {
	Index        int        `json:"index"`
	Delta        chunkDelta `json:"delta"`
	FinishReason *string    `json:"finish_reason"`
}

type chunkFrame struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []chunkChoice `json:"choices"`
	Usage   *openAIUsage  `json:"usage,omitempty"`
}

// ---- responses ----

type responsesContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type responsesOutput struct {
	Type      string `json:"type"`
	Role      string `json:"role,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	// Content/Summary 声明为 any 而不是 []T + omitempty:空切片在 omitempty 下
	// 会连键一起消失(协议审计 M3),而 reasoning 行的公开形状是
	// {"type":"reasoning","summary":[]};null 又不是合法的缺席表示。any 的
	// omitempty 只看接口本身是否 nil —— 装着空切片的非 nil 接口恒发键,
	// 不相关的行(如 function_call)传 nil 仍干净地不带键。
	Content any `json:"content,omitempty"`
	// Summary 是 reasoning 输出项的字段(流式与最终体都会出现);非 reasoning
	// 项不携带。这条车道没有推理摘要流,恒为空数组。
	Summary any `json:"summary,omitempty"`
}

type responsesUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	TotalTokens  int64 `json:"total_tokens"`
}

type responsesBody struct {
	ID        string            `json:"id"`
	Object    string            `json:"object"`
	CreatedAt int64             `json:"created_at"`
	Model     string            `json:"model"`
	Status    string            `json:"status"`
	Output    []responsesOutput `json:"output"`
	Usage     responsesUsage    `json:"usage"`
	// IncompleteDetails 只在 status:"incomplete" 时出现:镜像上游
	// response.incomplete 事件的 max_output_tokens 截断形状,让流式客户端
	// 与非流式(finish_reason:length)一样能区分「正常完成」和「被截断」。
	IncompleteDetails *incompleteDetails `json:"incomplete_details,omitempty"`
	// FinishReason 是本轮的公开结束原因(带 response 事件时都出现,带 item 的
	// 事件仍不携带):截断轮必须发 "length",与 chat 线的
	// finish_reason:"length" 对齐 —— 过去非流式恒发 completed 且没有这个字段,
	// 被腰斩的回答与完整回答在线路上完全不可区分(协议审计 H1)。
	FinishReason string `json:"finish_reason,omitempty"`
	// Error 只在「已出内容后断流」的非流式响应上出现(与 chat 线的顶层 error
	// 标记同理):让半截回答与完整回答可区分。成功路径恒不出现。
	Error *openAIErrorDetail `json:"error,omitempty"`
}

// incompleteDetails 是 responsesBody 里截断原因那一小块;具名是为了两处
// (流式收尾 / 非流式收尾)共用同一形状而不用各写一遍匿名 struct 再让
// 类型系统对「完全一致的结构」较真。
type incompleteDetails struct {
	Reason string `json:"reason"`
}

// responsesEvent 是生命周期事件(created/in_progress/output_item.added/
// output_item.done/failed/completed)的信封:要么带 response,要么带 item。
// OutputIndex 是指针:created/completed 这类**没有** output_index 语义的事件
// 不发这个键(裸 int 会在零值处静默丢键),而 output_item.added/done 恒发
// —— 哪怕是 0 号项。严格客户端靠 added 事件里的 output_index 把后续增量
// 关联到 item,键在 index 0 处蒸发等于第一项永不归位(协议审计 M1)。
type responsesEvent struct {
	Type        string         `json:"type"`
	Response    *responsesBody `json:"response,omitempty"`
	Item        *responsesItem `json:"item,omitempty"`
	OutputIndex *int           `json:"output_index,omitempty"`
}

// responsesDeltaEvent 是增量事件(output_text.delta / reasoning_summary_text.delta /
// function_call_arguments.delta / content_part.added)的信封:item_id 与
// output_index 是定位键,恒出现。content_index 同样恒出现(去掉了 omitempty):
// 公开 API 的 schema 把它列为 output_text.delta/.done/content_part.* 的必填
// 字段,零值 0 被 omit 掉时按 item_id 归位、按 part 数组内序累积的严格客户端
// 会读不到定位键(协议审计 M2)。function_call 增量多带一个值为 0 的键是
// 无害噪音,一致性优先。
type responsesDeltaEvent struct {
	Type         string `json:"type"`
	ItemID       string `json:"item_id"`
	OutputIndex  int    `json:"output_index"`
	ContentIndex int    `json:"content_index"`
	Delta        string `json:"delta,omitempty"`
	// done 事件的终值字段:公开 schema 里 output_text.done 叫 **text**、
	// function_call_arguments.done 叫 **arguments**、reasoning_summary_text.done
	// 叫 **text** —— delta 是增量事件的字段名,终值放它上面,按公开 schema
	// 读终值的严格客户端会拿到 undefined。
	Text      string            `json:"text,omitempty"`
	Arguments string            `json:"arguments,omitempty"`
	Part      *responsesContent `json:"part,omitempty"`
}

// summaryText 是 reasoning 项 summary 数组的元素形状(公开 API)。
type summaryText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// responsesItem 是流式 output_item.added/done 里的 item 形状(与 responsesOutput
// 分开:id/status 只属于流式项)。Content/Summary 是 any + omitempty(同
// responsesOutput 的理由,协议审计 M3):message 恒发数组(added 时是刻意的
// 空 [],done 时带全文),reasoning 恒发 summary 数组,function_call 两者都不带。
type responsesItem struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Role      string `json:"role,omitempty"`
	Status    string `json:"status,omitempty"`
	Content   any    `json:"content,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
	Summary   any    `json:"summary,omitempty"`
}

// sseStream 是延迟发头的 SSE 通道。
//
// 为什么延迟:js 在调 complete **之前**就 openStreamHeaders(:236),于是一个
// 「第一次拨号就失败」的流式请求在 JS 里是 200 + 一个只有 role 骨架的流 ——
// 在 OpenAI SDK 眼里那是「成功返回了 0 个 token」,调用方无从知道该不该重试。
// 本实现把头发在第一帧真正到达时:在那之前失败,走的是和非流式完全一样的
// 502 JSON 错误。头一旦出去,失败就只能走 in-band error 事件(js :281-300)。
// 这是对 JS 的一处**有意偏离**,记录在提交信息里。
type sseStream struct {
	w       *writer
	started bool
	// writeErr 记录第一次写失败:客户端断开时,把错误从 onChunk 回传给
	// adapter,让它的中止通道生效 —— 否则断开只能等 server 的后台读检测
	// 到,期间上游流继续被消费、出口 quota 照扣。
	writeErr error
	// buf/enc 是每帧复用的序列化缓冲(O14)。一条流可以吐几百个增量帧,过去
	// marshalNoEscape 每帧新建一个 bytes.Buffer 与一个 json.Encoder,再把结果
	// 拷进第三个缓冲里 —— 全在热路径上。
	buf bytes.Buffer
	enc *json.Encoder
}

// newSSEStream 建一条 SSE 输出流。enc 绑在流自己的 buf 上,所以它必须在 sseStream
// 已经落位之后才能构造(字段地址要稳定)。
func newSSEStream(w *writer) *sseStream {
	s := &sseStream{w: w}
	s.enc = json.NewEncoder(&s.buf)
	// 与 marshalNoEscape 同一理由:json 默认把 < > & 转成 \u003c 之类,而
	// JSON.stringify 不转;模型输出里出现 </script> 是常事。
	s.enc.SetEscapeHTML(false)
	return s
}

// failed 报告这条流是否已经写失败(客户端断开的替身)。
func (s *sseStream) failed() bool { return s.writeErr != nil }

// rawFrame 把 buf 里的完整帧原样写出去(调用方已拼好 "data: …\n\n")。
func (s *sseStream) rawFrame() {
	s.ensure()
	if s.writeErr == nil {
		if _, err := s.w.Write(s.buf.Bytes()); err != nil {
			s.writeErr = err
		}
		s.w.Flush()
	}
	s.buf.Reset()
}

func (s *sseStream) ensure() {
	if s.started {
		return
	}
	s.started = true
	h := s.w.Header()
	// 与 js :187-195 一致;只有 SSE 响应带 CORS,普通 JSON 响应不带(js 的
	// json() 不设 CORS 头)。
	applyCORS(h)
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
}

// sendFrame 写一帧 `data: <json>\n\n`。整帧拼进复用缓冲后**一次** Write:
// 旧实现每帧三次 Write(前缀/payload/分隔),SSE 高帧率时 syscall 翻三倍。
func (s *sseStream) send(event any) {
	s.ensure()
	if s.writeErr != nil {
		return
	}
	s.buf.Reset()
	s.buf.WriteString("data: ")
	if err := s.enc.Encode(event); err != nil {
		// 序列化失败:一个字节都没写出去,丢弃这一帧即可。
		s.buf.Reset()
		return
	}
	// Encoder 在值后面补了一个换行,再补一个正好凑成空行分隔。
	s.buf.WriteByte('\n')
	s.rawFrame()
}

// sendEvent 写一帧带 `event:` 行的 SSE(Responses API 的形状)。
func (s *sseStream) sendEvent(eventType string, payload any) {
	s.ensure()
	if s.writeErr != nil {
		return
	}
	s.buf.Reset()
	s.buf.WriteString("event: ")
	s.buf.WriteString(eventType)
	s.buf.WriteString("\ndata: ")
	if err := s.enc.Encode(payload); err != nil {
		s.buf.Reset()
		return
	}
	s.buf.WriteByte('\n')
	s.rawFrame()
}

func (s *sseStream) done() {
	s.ensure()
	s.buf.Reset()
	s.buf.WriteString("data: [DONE]\n\n")
	s.rawFrame()
}

func ptr[T any](v T) *T { return &v }

// chatCompletions 是 POST /v1/chat/completions(js :198-307)。
func (s *Server) chatCompletions(w *writer, r *http.Request, body map[string]any) {
	model := baseModelID(stringField(body, "model"))
	if model == "" {
		openAIError(w, http.StatusBadRequest, "invalid_request_error", "`model` is required")
		return
	}
	id := "chatcmpl-" + randHex(8)
	created := nowSeconds()
	wantsStream, _ := body["stream"].(bool)

	if !wantsStream {
		s.chatCompletionOnce(w, r, body, model, id, created)
		return
	}
	s.chatCompletionStream(w, r, body, model, id, created)
}

// refused 是「这一轮被拒了,而且没有任何内容可以给」的判据(js :215/:315)。
// 少了它,调用方拿到的是 200 + content:null + finish_reason:stop —— 与「模型
// 选择什么都不说」在协议上完全不可区分,用户的提问会被静默丢掉。
func refused(out engine.Outcome) bool {
	return out.Error != "" && out.Text == "" && len(out.ToolCalls) == 0
}

func (s *Server) chatCompletionOnce(w *writer, r *http.Request, body map[string]any, model, id string, created int64) {
	out, err := s.complete(r.Context(), engine.Request{Model: model, OpenAI: body}, nil)
	if err != nil {
		// Complete 的 error 返回对应 JS 的「complete 抛错」:轮换耗尽/无健康
		// 出口在 src/engine.js:233 是 throw,一路冒到 forward.js:103 的顶层
		// catch → **500**。outcome.error(回合被拒但已 resolve)才是 js :216
		// 的 502 —— 两者在 JS 里是两条不同的路,不能都压成 502(差分 B7
		// 实测:同样的耗尽错误 JS 回 500、Go 回 502)。Failure 自带 Status
		// 且合法时优先(unknown model 是 400:客户端不该把它当服务端 500
		// 去重试)。
		openAIError(w, statusOf(err, http.StatusInternalServerError), "server_error", messageOf(err))
		return
	}
	if refused(out) {
		openAIError(w, http.StatusBadGateway, "server_error", out.Error)
		return
	}

	// js :219 的 `text` 是 outcome.text 与 toolCalls.map(()=>'') 的拼接,而后者
	// 只贡献空串 —— 所以它就是 outcome.Text。
	msg := chatMessage{Role: "assistant"}
	// 空回答与「说了话」必须可区分:Content 保持 nil → 序列化成 null,
	// 而不是 content:"" —— 客户端把 "" 当正文读,把 null 当「没有正文」。
	if out.Text != "" {
		msg.Content = ptr(out.Text)
	}
	for i, call := range out.ToolCalls {
		cid := call.ID
		if cid == "" {
			cid = "call_" + itoa(i)
		}
		msg.ToolCalls = append(msg.ToolCalls, openAIToolCall{
			ID:       cid,
			Type:     "function",
			Function: openAIToolFunction{Name: call.Name, Arguments: call.Arguments},
		})
	}
	finish := "stop"
	if len(out.ToolCalls) > 0 {
		finish = "tool_calls"
	} else if out.Truncated {
		finish = "length"
	}
	usage, _ := openAIUsageOf(out.Usage) // 无 usage 时就是三个 0,details 段不出现
	resp := chatCompletionBody{
		ID: id, Object: "chat.completion", Created: created, Model: model,
		Choices: []chatChoice{{Index: 0, Message: msg, FinishReason: finish}},
		Usage:   usage,
	}
	if out.Error != "" {
		// 断流且已出内容:半截回答按 200 交付(换出口重试会重发前缀),但
		// 顶层补一个 error 标记 —— 不加它,这条被掐断的回答与完整回答在
		// 协议上不可区分,调用方无从得知 finish_reason:stop 背后是一刀两断。
		resp.Error = &openAIErrorDetail{Message: out.Error, Type: "server_error", Param: nil, Code: nil}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) chatCompletionStream(w *writer, r *http.Request, body map[string]any, model, id string, created int64) {
	stream := newSSEStream(w)
	skeleton := chunkFrame{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{Role: "assistant", Content: ptr("")}}},
	}
	// 骨架帧必须排在所有内容帧之前(OpenAI SDK 靠它建立 assistant 消息对象),
	// 但它自己也属于「已经发了东西」—— 所以它和头一起延迟到第一帧真正到达。
	started := false
	start := func() {
		if started {
			return
		}
		started = true
		stream.send(skeleton)
	}

	// seenToolStart 记录已向客户端发过首帧的 slot;toolOrdinal 把 engine 的
	// 全局块序号(slot,正文/推理/工具统一编号)映射成 OpenAI 线上的
	// tool_calls[].index(按调用出现顺序从 0 连续编号)。 reasoning 块先到
	// 是常态,不重编号的话线上几乎每个带调用的回复 index 都从 1 起,
	// 按数组下标归并的客户端(LiteLLM/LangChain 等)会产出稀疏数组。
	seenToolStart := map[int]bool{}
	toolOrdinal := map[int]int{}
	// 该 slot 的 function.name 是否已上过线:首帧名字为空(先参后名的反常帧序)
	// 时,晚到名字只补一次,后续帧继续吞(第八轮 R4 低-2)。
	toolNameSent := map[int]bool{}
	nextToolIndex := 0
	forwarded := false

	onChunk := func(c engine.Chunk) error {
		if stream.failed() {
			return stream.writeErr // 客户端已断:让 adapter 的中止通道生效
		}
		switch c.Kind {
		case engine.ChunkText:
			if c.Text == "" {
				// 与 reasoning 同一守卫:空文本增量也不发帧。部分上游把
				// content_block_start 的空 text 或纯 keepalive 走成空增量,
				// 严格客户端把它当成一次新的内容块开始。start() 不调 ——
				// 空帧不值得花掉骨架帧。
				return nil
			}
			start()
			forwarded = true
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{Content: ptr(c.Text)}}},
			})
		case engine.ChunkReasoning:
			if c.Text == "" {
				// 空思考增量不发帧(magpie chattidy 修的四类形状之一):某些
				// 上游在思考结束时补一个空 delta,渲染成 "reasoning":"" 后,
				// 严格客户端(Qoder 一类)会把它当成一次新的思考开始,回复
				// 被拆成一字一行。start() 也不调 —— 空帧不值得花掉骨架帧。
				return nil
			}
			start()
			forwarded = true
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{Reasoning: ptr(c.Text)}}},
			})
		case engine.ChunkToolCallDelta:
			first := !seenToolStart[c.Index]
			if first && c.ToolArguments != "" && c.ToolDelta == "" {
				// 只有 block-end、从没有过增量帧:零参数调用(或上游整段
				// 补发)的唯一登记途径就是这一帧。旧实现无条件丢弃它,客户端
				// 收不到任何 tool_calls delta 却在收尾看到 finish_reason:
				// tool_calls —— SDK 组装出的 assistant 消息没有任何调用,
				// 下一轮回放即错。这里合成首帧,把 ID+Name+完整参数一次发出。
				start()
				forwarded = true
				seenToolStart[c.Index] = true
				toolOrdinal[c.Index] = nextToolIndex
				nextToolIndex++
				if c.ToolName != "" {
					toolNameSent[c.Index] = true
				}
				stream.send(chunkFrame{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{{
						Index:    toolOrdinal[c.Index],
						ID:       c.ToolID,
						Function: &toolFuncDelta{Name: c.ToolName, Arguments: c.ToolArguments},
					}}}}},
				})
				return nil
			}
			if c.ToolArguments != "" {
				// 已发过增量的 block-end 完整帧:参数忽略(增量已拼齐,重发会让
				// SDK 当成两个调用)。但晚到的名字要补:先参后名的上游把 name 只
				// 放在收尾帧,旧形状整帧吞掉,客户端拿到 Name:"" 的调用无法执行。
				// OpenAI 线允许后续 delta 只带 function.name。
				if c.ToolName != "" && !toolNameSent[c.Index] {
					toolNameSent[c.Index] = true
					start()
					forwarded = true
					stream.send(chunkFrame{
						ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
						Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{{
							Index:    toolOrdinal[c.Index],
							Function: &toolFuncDelta{Name: c.ToolName, Arguments: ""},
						}}}}},
					})
				}
				return nil
			}
			start()
			forwarded = true
			if first {
				seenToolStart[c.Index] = true
				toolOrdinal[c.Index] = nextToolIndex
				nextToolIndex++
			}
			entry := toolCallDelta{Index: toolOrdinal[c.Index]}
			if first {
				entry.ID = c.ToolID
				entry.Function = &toolFuncDelta{Name: c.ToolName, Arguments: ""}
				if c.ToolName != "" {
					toolNameSent[c.Index] = true
				}
			} else if c.ToolName != "" && !toolNameSent[c.Index] {
				// 增量帧上的晚到名字(同上:先参后名),补一发 name-only delta。
				entry.Function = &toolFuncDelta{Name: c.ToolName, Arguments: ""}
				toolNameSent[c.Index] = true
			}
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{entry}}}},
			})
			if c.ToolDelta != "" {
				stream.send(chunkFrame{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{
						{Index: toolOrdinal[c.Index], Function: &toolFuncDelta{Arguments: c.ToolDelta}},
					}}}},
				})
			}
		case engine.ChunkUsage:
			u, ok := openAIUsageOf(c.Usage)
			if !ok {
				return nil
			}
			// 先判 ok 再发头:空 usage 也烧头的话,后续失败只能走 in-band,
			// 丢掉延迟发头的红利。
			start()
			// usage 帧按 js :274 带 choices: [],不是收尾帧。
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{}, Usage: &u,
			})
		}
		return nil
	}

	out, err := s.complete(r.Context(), engine.Request{Model: model, OpenAI: body}, onChunk)
	if err != nil {
		if !stream.started {
			// 头还没出去:这正是延迟发头换来的东西 —— 第一次拨号就失败时,
			// 调用方拿到的是一个真正的错误(默认 502;Failure 自带 Status
			// 且合法时优先,如 unknown model 的 400),而不是一个「成功但
			// 0 token」的流。
			openAIError(w, statusOf(err, http.StatusBadGateway), "server_error", messageOf(err))
			return
		}
		// 头已经花掉了(js :281-287):流内 error 事件是把失败告诉客户端的
		// 唯一通道。不发它,调用方只会在自己的读取端看到 "stream read failed:
		// terminated",既没有错误详情也没有重试依据。
		stream.send(sseErrorBody{Error: sseErrorDetail{
			Message:   messageOf(err),
			Type:      "server_error",
			Retryable: retryableCodes[frerrors.CodeOf(err)],
		}})
		stream.done()
		return
	}
	if out.Error != "" {
		if !stream.started {
			// 池子全灭、一个 token 都没出来:与非流式分支同样回 502。JS 这里
			// 只能发 in-band error,因为它的 200 早就花掉了(见 sseStream 注释)。
			openAIError(w, http.StatusBadGateway, "server_error", out.Error)
			return
		}
		// 200 已经花掉,但一个被拒的回合仍必须说出来:用干净的
		// finish_reason:stop 收尾就是同一个「空 200」从另一扇门进来。
		stream.send(sseErrorBody{Error: sseErrorDetail{
			Message:   out.Error,
			Type:      "server_error",
			Retryable: out.Retryable,
		}})
		if !forwarded {
			stream.done()
			return
		}
	}
	start()
	// 与非流式分支同一套判定(差分一致):finish 看折进 Outcome 的 ToolCalls
	// 而不是 seenToolStart —— 截断轮的残缺调用已被 engine 剪掉,流上却已发
	// 过增量帧;拿 seenToolStart 判会把 finish_reason:tool_calls 发给一条
	// 参数截在半截、无法执行的调用上(非流式同场景回的是 length)。
	finish := "stop"
	if len(out.ToolCalls) > 0 {
		finish = "tool_calls"
	} else if out.Truncated {
		finish = "length"
	}
	stream.send(chunkFrame{
		ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
		Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{}, FinishReason: ptr(finish)}},
	})
	stream.done()
}

// responsesEndpoint 是 POST /v1/responses(js :310-331),给 Codex 形状的本地
// 客户端用。注意它**不检查 model 是否为空**(与 chat 线路不同),照搬。
func (s *Server) responsesEndpoint(w *writer, r *http.Request, body map[string]any) {
	model := baseModelID(stringField(body, "model"))
	id := "resp-" + randHex(8)

	openAI := normalizeResponsesBody(body)

	// stream:true 的请求必须真的流回去。旧实现无视它恒回整包 JSON —— Codex
	// 类客户端按 Responses API 默认发流式请求,拿到一次性 JSON 后读流会失败。
	if v, _ := body["stream"].(bool); v {
		s.responsesStream(w, r, openAI, model, id)
		return
	}

	out, err := s.complete(r.Context(), engine.Request{Model: model, OpenAI: openAI, Responses: true}, nil)
	if err != nil {
		// 与 chatCompletionOnce 同理:JS 的 throw 走顶层 catch → 500(js :316
		// 的 502 只属于 resolve 成 outcome.error 的拒绝);Failure 自带 Status
		// 且合法时优先。
		openAIError(w, statusOf(err, http.StatusInternalServerError), "server_error", messageOf(err))
		return
	}
	if refused(out) {
		openAIError(w, http.StatusBadGateway, "server_error", out.Error)
		return
	}

	output, usage := responsesOutputOf(out)
	// 截断判决与流式分支、chat 线共用同一个 Outcome 上的同一个标志:过去这里
	// 硬编码 status:"completed",一个被 max_output_tokens 腰斩的回答在非流式
	// 线路上与完整回答**完全不可区分**(协议审计 H1 —— chat 线会把
	// out.Truncated 映成 finish_reason:"length",responses 线却没有对应物,
	// Codex 类非流式客户端因此永远不会续写)。
	status, details, finish := "completed", (*incompleteDetails)(nil), "stop"
	if out.Truncated {
		status = "incomplete"
		details = &incompleteDetails{Reason: "max_output_tokens"}
		finish = "length"
	}
	resp := responsesBody{
		ID: id, Object: "response", CreatedAt: nowSeconds(), Model: model, Status: status,
		Output: output, Usage: usage, IncompleteDetails: details, FinishReason: finish,
	}
	if out.Error != "" {
		// 与 chat 线同理:半截回答按 200 交付,顶层 error 标记让它可区分。
		resp.Error = &openAIErrorDetail{Message: out.Error, Type: "server_error", Param: nil, Code: nil}
	}
	writeJSON(w, http.StatusOK, resp)
}

// normalizeResponsesBody 把 Responses API 的请求体归一成 engine 能读的形状。
// js :314 的 `{...body, input: body.input ?? body.messages ?? []}` 之外,补齐
// 三件曾被静默丢弃的参数(engine 只读 chat 拼写的顶层键,而 Responses 客户端
// 发的是自己的拼写):
//   - reasoning.effort → reasoning_effort(推理档位)
//   - max_output_tokens → max_tokens(输出上限)
//   - instructions → 折成 input 首条 system(Responses 的系统提示不叫 messages)
func normalizeResponsesBody(body map[string]any) map[string]any {
	openAI := make(map[string]any, len(body)+1)
	for k, v := range body {
		openAI[k] = v
	}
	if v, ok := openAI["input"]; !ok || v == nil {
		if m, ok := openAI["messages"]; ok && m != nil {
			openAI["input"] = m
		} else {
			openAI["input"] = []any{}
		}
	}
	if reasoning, ok := openAI["reasoning"].(map[string]any); ok {
		if eff, ok := reasoning["effort"].(string); ok && eff != "" {
			if _, exists := openAI["reasoning_effort"]; !exists {
				openAI["reasoning_effort"] = eff
			}
		}
	}
	if _, exists := openAI["max_tokens"]; !exists {
		if v, ok := openAI["max_output_tokens"].(float64); ok && v > 0 {
			openAI["max_tokens"] = v
		}
	}
	if instr, ok := openAI["instructions"].(string); ok && instr != "" {
		openAI["input"] = prependInstructions(openAI["input"], instr)
	}
	return openAI
}

// prependInstructions 把系统指令插到 input 列表最前面;字符串 input 先拆成单条
// user。非字符串非数组的形状原样返回,交给 engine 的归一兜底。
func prependInstructions(input any, instructions string) any {
	items, ok := input.([]any)
	if !ok {
		if s, isStr := input.(string); isStr {
			items = []any{map[string]any{"role": "user", "content": s}}
		} else {
			return input
		}
	}
	rows := make([]any, 0, len(items)+1)
	rows = append(rows, map[string]any{"role": "system", "content": instructions})
	return append(rows, items...)
}

// responsesOutputOf 把折好的 Outcome 变成 Responses 的 output 数组与 usage。
func responsesOutputOf(out engine.Outcome) ([]responsesOutput, responsesUsage) {
	var output []responsesOutput
	if out.Text != "" {
		output = append(output, responsesOutput{
			Type:    "message",
			Role:    "assistant",
			Content: []responsesContent{{Type: "output_text", Text: out.Text}},
		})
	}
	for i, call := range out.ToolCalls {
		cid := call.ID
		if cid == "" {
			cid = "call_" + itoa(i)
		}
		output = append(output, responsesOutput{
			Type: "function_call", CallID: cid, Name: call.Name, Arguments: call.Arguments,
		})
	}
	if output == nil {
		output = []responsesOutput{}
	}
	// js :325-329 读的是已经转成 OpenAI 形状的 outcome.usage 的三个键。
	var usage responsesUsage
	if u, ok := openAIUsageOf(out.Usage); ok {
		usage = responsesUsage{InputTokens: u.PromptTokens, OutputTokens: u.CompletionTokens, TotalTokens: u.TotalTokens}
	}
	return output, usage
}

// respStreamItem 是 Responses 流式输出里的一个打开中的 item。
type respStreamItem struct {
	itemID string
	kind   string // "reasoning" | "message" | "function_call"
	outIdx int
	callID string
	name   string
	text   strings.Builder
}

// responsesStream 是 /v1/responses 的流式分支。事件形状对齐 OpenAI Responses
// SSE:created/in_progress → output_item.added → 增量事件 → output_item.done →
// completed。与 chat 线同理,头发在第一个事件真正要发时:在那之前失败,调用方
// 拿到的是 JSON 错误而不是「成功但 0 token」的流。usage 不发独立事件,由
// response.completed 携带(Responses API 没有独立的 usage 事件)。
func (s *Server) responsesStream(w *writer, r *http.Request, openAI map[string]any, model, id string) {
	sse := newSSEStream(w)
	created := nowSeconds()

	var items []*respStreamItem
	itemBySlot := map[int]*respStreamItem{}
	seenToolSlot := map[int]bool{}
	// curMessage / curReasoning 是**当前打开着**的同类 item:文本块只往当前
	// 那个 message 里拼,而不是「items 里第一个 message」。旧实现用后者,
	// 于是 text → tool_call → text 这种真实形状(先说一句、再调工具、然后继续
	// 说)会把第二段话拼进第 0 个 item:item 数少一个、output_index 与流上的
	// 创建序矛盾、done/completed 里的 output 也少一段。function_call 开新项
	// 时把两者置 nil,下一段文本自然开一个新 item —— 一个内容块被另一个
	// 种类打断就是新块。
	var curMessage, curReasoning *respStreamItem
	nextItemIdx := 0
	var finalUsage stream.Usage
	forwarded := false

	skeleton := func(status string, output []responsesOutput, usage *responsesUsage) *responsesBody {
		rb := responsesBody{
			ID: id, Object: "response", CreatedAt: created, Model: model,
			Status: status, Output: output,
		}
		if usage != nil {
			rb.Usage = *usage
		}
		return &rb
	}
	createdSent := false
	sendCreated := func() {
		if createdSent {
			return
		}
		createdSent = true
		empty := []responsesOutput{}
		sse.sendEvent("response.created", responsesEvent{Type: "response.created", Response: skeleton("in_progress", empty, nil)})
		sse.sendEvent("response.in_progress", responsesEvent{Type: "response.in_progress", Response: skeleton("in_progress", empty, nil)})
	}
	openItem := func(kind, itemID string) *respStreamItem {
		it := &respStreamItem{itemID: itemID, kind: kind, outIdx: nextItemIdx}
		nextItemIdx++
		items = append(items, it)
		ev := responsesEvent{Type: "response.output_item.added", OutputIndex: ptr(it.outIdx), Item: &responsesItem{
			ID: it.itemID, Type: kind, Status: "in_progress",
		}}
		switch kind {
		case "message":
			ev.Item.Role = "assistant"
			ev.Item.Content = []responsesContent{}
		case "reasoning":
			ev.Item.Summary = []any{}
		}
		sse.sendEvent(ev.Type, ev)
		return it
	}

	onChunk := func(c engine.Chunk) error {
		if sse.failed() {
			return sse.writeErr
		}
		switch c.Kind {
		case engine.ChunkText:
			if c.Text == "" {
				// 与 reasoning 同守卫:空文本增量不发帧(某些上游把 role
				// 骨架或纯 keepalive 走成 text 块),发了就是一字一行。
				return nil
			}
			sendCreated()
			// 思考项还开着:文本必须另开 message item。旧形状只在 function_call
			// 处清两项,text↔reasoning 交错(Anthropic interleaved thinking)时
			// B 段正文挂回 A 段的 message item:文本不丢,但线序与创建序交叉,
			// 严格状态机客户端把内容挂错 item(第八轮 R4 中-3)。
			if curReasoning != nil {
				curMessage, curReasoning = nil, nil
			}
			it := curMessage
			if it == nil {
				it = openItem("message", "msg_"+itoa(nextItemIdx))
				curMessage = it
				sse.sendEvent("response.content_part.added", responsesDeltaEvent{
					Type: "response.content_part.added", ItemID: it.itemID, OutputIndex: it.outIdx,
					Part: &responsesContent{Type: "output_text", Text: ""},
				})
			}
			it.text.WriteString(c.Text)
			forwarded = true
			sse.sendEvent("response.output_text.delta", responsesDeltaEvent{
				Type: "response.output_text.delta", ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.Text,
			})
		case engine.ChunkReasoning:
			if c.Text == "" {
				// 与 chat 线同一守卫:空思考增量不发帧(chattidy 形状 1 在
				// responses 线上同样会一字一行)。不发帧就不花 created 头 ——
				// 一整轮只有空思考的回合留给 usage-only 的兜底。
				return nil
			}
			sendCreated()
			// 文本项还开着:推理另开 reasoning item(对称同上)。
			if curMessage != nil {
				curMessage, curReasoning = nil, nil
			}
			it := curReasoning
			if it == nil {
				it = openItem("reasoning", "rs_"+itoa(nextItemIdx))
				curReasoning = it
			}
			it.text.WriteString(c.Text)
			forwarded = true
			// 公开 API 的推理增量事件名是 reasoning_summary_text(与 adapter
			// 认上游两个名字的表一致);reasoning_text 不是公开事件,监听公开
			// 事件名的客户端会把它当未知事件静默丢掉。
			sse.sendEvent("response.reasoning_summary_text.delta", responsesDeltaEvent{
				Type: "response.reasoning_summary_text.delta", ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.Text,
			})
		case engine.ChunkToolCallDelta:
			// 先判归属再发头:孤儿 delta(it==nil、无首帧信息)本来要丢弃,
			// 先 sendCreated 等于为一次注定丢弃的增量烧掉 response.created。
			it := itemBySlot[c.Index]
			first := !seenToolSlot[c.Index]
			if it == nil && !first {
				return nil // 没开过项也没有首帧信息:无从归属,丢弃
			}
			sendCreated()
			if first {
				seenToolSlot[c.Index] = true
				if it == nil {
					// function_call 项不走 openItem:added 事件必须一次带上
					// call_id/name,拆成两发客户端会看到两个裸项。
					it = &respStreamItem{itemID: "fc_" + itoa(nextItemIdx), kind: "function_call", outIdx: nextItemIdx, callID: c.ToolID, name: c.ToolName}
					nextItemIdx++
					items = append(items, it)
					itemBySlot[c.Index] = it
					// 工具块打断了当前文本/推理块:它们到此收尾,之后的
					// 文本另开一个 message item(见 curMessage 的注释)。
					curMessage, curReasoning = nil, nil
					sse.sendEvent("response.output_item.added", responsesEvent{Type: "response.output_item.added", OutputIndex: ptr(it.outIdx), Item: &responsesItem{
						ID: it.itemID, Type: "function_call", Status: "in_progress",
						CallID: c.ToolID, Name: c.ToolName, Arguments: "",
					}})
				}
			}
			if c.ToolArguments != "" {
				// block-end:增量已经拼齐就忽略;零参调用(整段参数随
				// block-end 到达)在这里一次发完。晚到的名字(先参后名:首帧
				// 增量为空名、name 只随收尾帧到)补进 item 快照 —— added 事件
				// 已按空名字发出收不回来,done/completed 按 it.name 重建,
				// 至少要带真名(第八轮 R4 低-2)。
				if it.name == "" && c.ToolName != "" {
					it.name = c.ToolName
				}
				if it.text.Len() > 0 {
					return nil
				}
				it.text.WriteString(c.ToolArguments)
				forwarded = true
				sse.sendEvent("response.function_call_arguments.delta", responsesDeltaEvent{
					Type: "response.function_call_arguments.delta", ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.ToolArguments,
				})
				return nil
			}
			if c.ToolDelta != "" {
				it.text.WriteString(c.ToolDelta)
				forwarded = true
				sse.sendEvent("response.function_call_arguments.delta", responsesDeltaEvent{
					Type: "response.function_call_arguments.delta", ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.ToolDelta,
				})
			}
		case engine.ChunkUsage:
			// usage-only 轮次(finish=length/incomplete 且无内容块)也会走到
			// completed:不先发 created 的话,客户端收到的第一个事件就是
			// completed —— chat 线同场景有骨架帧兜底,这里同样要补生命周期头。
			sendCreated()
			finalUsage = c.Usage
		}
		return nil
	}

	out, err := s.complete(r.Context(), engine.Request{Model: model, OpenAI: openAI, Responses: true}, onChunk)
	if err != nil {
		if !sse.started {
			openAIError(w, statusOf(err, http.StatusBadGateway), "server_error", messageOf(err))
			return
		}
		resp := skeleton("failed", []responsesOutput{}, nil)
		resp.Error = &openAIErrorDetail{Message: messageOf(err), Type: "server_error", Param: nil, Code: nil}
		sse.sendEvent("response.failed", responsesEvent{Type: "response.failed", Response: resp})
		return
	}
	if out.Error != "" && !forwarded && !sse.started {
		// 一个被拒的回合且一个事件都没发过:与非流式分支同样回 502。
		// (started 是保险:头花掉之后再回 JSON 会产出 SSE 头+JSON 体的畸形
		// 响应;usage-only 轮次会 started 而 forwarded——那只出现在成功路径,
		// 与 out.Error 互斥,这里把不变量钉死。)
		openAIError(w, http.StatusBadGateway, "server_error", out.Error)
		return
	}
	if out.Error != "" {
		resp := skeleton("failed", []responsesOutput{}, nil)
		resp.Error = &openAIErrorDetail{Message: out.Error, Type: "server_error", Param: nil, Code: nil}
		sse.sendEvent("response.failed", responsesEvent{Type: "response.failed", Response: resp})
		return
	}

	// 收尾:按打开顺序逐个 item 发 done,再发 completed。completed 的 output
	// 直接从**已流出的 items** 投影 —— 从 Outcome 重投影会丢掉 reasoning 项,
	// 且 message/function_call 的固定排序与流上 output_index 的创建序矛盾。
	// sendCreated 幂等:一整轮没有任何可发帧时(只有被守卫吃掉的空增量),
	// completed 仍不能当流上的第一个事件(C-新2 的不变量,与 usage-only 兜底同理)。
	sendCreated()
	finalOutput := make([]responsesOutput, 0, len(items))
	for _, it := range items {
		done := responsesItem{ID: it.itemID, Type: it.kind, Status: "completed"}
		switch it.kind {
		case "message":
			text := it.text.String()
			done.Role = "assistant"
			done.Content = []responsesContent{{Type: "output_text", Text: text}}
			// 正文 item 的 part/text 收尾事件:严格的状态机客户端在等它们。
			sse.sendEvent("response.content_part.done", responsesDeltaEvent{
				Type: "response.content_part.done", ItemID: it.itemID, OutputIndex: it.outIdx,
				Part: &responsesContent{Type: "output_text", Text: text},
			})
			sse.sendEvent("response.output_text.done", responsesDeltaEvent{
				Type: "response.output_text.done", ItemID: it.itemID, OutputIndex: it.outIdx, Text: text,
			})
			finalOutput = append(finalOutput, responsesOutput{
				Type: "message", Role: "assistant",
				Content: []responsesContent{{Type: "output_text", Text: text}},
			})
		case "reasoning":
			// 流出去的推理文本要在自己的收尾里带全:过去 done/completed 的
			// summary 恒为空数组 —— 按 item.done 重建(而非按 delta 累积)的
			// 客户端会丢掉全部推理。reasoning_summary_text.done 同理补上。
			sum := []any{summaryText{Type: "summary_text", Text: it.text.String()}}
			sse.sendEvent("response.reasoning_summary_text.done", responsesDeltaEvent{
				Type: "response.reasoning_summary_text.done", ItemID: it.itemID,
				OutputIndex: it.outIdx, Text: it.text.String(),
			})
			done.Summary = sum
			finalOutput = append(finalOutput, responsesOutput{Type: "reasoning", Summary: sum})
		case "function_call":
			args := it.text.String()
			if args == "" {
				args = "{}"
			}
			done.CallID, done.Name, done.Arguments = it.callID, it.name, args
			finalOutput = append(finalOutput, responsesOutput{
				Type: "function_call", CallID: it.callID, Name: it.name, Arguments: args,
			})
		}
		// M4:function_call 项在 output_item.done **之前**先发表单事件,再关项。
		if it.kind == "function_call" {
			sse.sendEvent("response.function_call_arguments.done", responsesDeltaEvent{
				Type: "response.function_call_arguments.done", ItemID: it.itemID,
				OutputIndex: it.outIdx, Arguments: done.Arguments,
			})
		}
		sse.sendEvent("response.output_item.done", responsesEvent{
			Type: "response.output_item.done", OutputIndex: ptr(it.outIdx), Item: &done,
		})
	}
	if len(finalOutput) == 0 {
		finalOutput = []responsesOutput{}
	}
	u := responsesUsage{}
	if tu, ok := openAIUsageOf(finalUsage); ok {
		u = responsesUsage{InputTokens: tu.PromptTokens, OutputTokens: tu.CompletionTokens, TotalTokens: tu.TotalTokens}
	}
	// 截断轮镜像上游 response.incomplete 的形状:status=incomplete + 截断原因
	// + finish_reason。流式与非流式在同一个 Outcome 上做同一个判决(协议审计
	// H1:过去只有这条线上有这套映射,非流式分支恒发 completed)。
	status, details, finish := "completed", (*incompleteDetails)(nil), "stop"
	if out.Truncated {
		status = "incomplete"
		details = &incompleteDetails{Reason: "max_output_tokens"}
		finish = "length"
	}
	resp := skeleton(status, finalOutput, &u)
	resp.IncompleteDetails = details
	resp.FinishReason = finish
	sse.sendEvent("response.completed", responsesEvent{Type: "response.completed", Response: resp})
}

// nowSeconds 照 js 的 `Math.floor(Date.now()/1000)`。
func nowSeconds() int64 { return time.Now().Unix() }

// itoa 是 strconv.Itoa 的短名,只在本文件用,避免为一个 `call_%d` 再引一次包。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
