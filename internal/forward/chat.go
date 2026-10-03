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
	Type      string             `json:"type"`
	Role      string             `json:"role,omitempty"`
	Content   []responsesContent `json:"content,omitempty"`
	CallID    string             `json:"call_id,omitempty"`
	Name      string             `json:"name,omitempty"`
	Arguments string             `json:"arguments,omitempty"`
	// Summary 是 reasoning 输出项的字段(流式与最终体都会出现);非 reasoning
	// 项不携带。这条车道没有推理摘要流,恒为空数组。
	Summary []any `json:"summary,omitempty"`
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
	// Error 只在「已出内容后断流」的非流式响应上出现(与 chat 线的顶层 error
	// 标记同理):让半截回答与完整回答可区分。成功路径恒不出现。
	Error *openAIErrorDetail `json:"error,omitempty"`
}

// responsesEvent 是 Responses SSE 的事件信封:一个结构体装全部事件形状,
// 各事件只填自己用到的键(omitempty 保证不出现空壳字段)。
type responsesEvent struct {
	Type     string         `json:"type"`
	Response *responsesBody `json:"response,omitempty"`
	Item     *responsesItem `json:"item,omitempty"`
	// 增量事件的定位与载荷。
	ItemID       string            `json:"item_id,omitempty"`
	OutputIndex  int               `json:"output_index"`
	ContentIndex int               `json:"content_index,omitempty"`
	Delta        string            `json:"delta,omitempty"`
	Part         *responsesContent `json:"part,omitempty"`
}

// responsesItem 是流式 output_item.added/done 里的 item 形状(与 responsesOutput
// 分开:id/status 只属于流式项)。
type responsesItem struct {
	ID        string             `json:"id"`
	Type      string             `json:"type"`
	Role      string             `json:"role,omitempty"`
	Status    string             `json:"status,omitempty"`
	Content   []responsesContent `json:"content,omitempty"`
	CallID    string             `json:"call_id,omitempty"`
	Name      string             `json:"name,omitempty"`
	Arguments string             `json:"arguments,omitempty"`
	Summary   []any              `json:"summary,omitempty"`
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
	nextToolIndex := 0
	forwarded := false

	onChunk := func(c engine.Chunk) error {
		if stream.failed() {
			return stream.writeErr // 客户端已断:让 adapter 的中止通道生效
		}
		switch c.Kind {
		case engine.ChunkText:
			start()
			if c.Text != "" {
				forwarded = true
			}
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{Content: ptr(c.Text)}}},
			})
		case engine.ChunkReasoning:
			start()
			if c.Text != "" {
				forwarded = true
			}
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
				// 已发过增量的 block-end 完整帧:忽略。js 的 onChunk 没有这个
				// 分支(增量已经把 arguments 拼齐了),转发层同样必须忽略它 ——
				// 否则同一个 tool call 会被发两遍,SDK 会当成两个调用。
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
			start()
			u, ok := openAIUsageOf(c.Usage)
			if !ok {
				return nil
			}
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
	finish := "stop"
	if len(out.ToolCalls) > 0 || len(seenToolStart) > 0 {
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
	resp := responsesBody{
		ID: id, Object: "response", CreatedAt: nowSeconds(), Model: model, Status: "completed",
		Output: output, Usage: usage,
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
	sendCreated := func() {
		empty := []responsesOutput{}
		sse.sendEvent("response.created", responsesEvent{Type: "response.created", Response: skeleton("in_progress", empty, nil)})
		sse.sendEvent("response.in_progress", responsesEvent{Type: "response.in_progress", Response: skeleton("in_progress", empty, nil)})
	}
	openItem := func(kind, itemID string) *respStreamItem {
		it := &respStreamItem{itemID: itemID, kind: kind, outIdx: nextItemIdx}
		nextItemIdx++
		items = append(items, it)
		ev := responsesEvent{Type: "response.output_item.added", OutputIndex: it.outIdx, Item: &responsesItem{
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
			sendCreated()
			var it *respStreamItem
			for _, cand := range items {
				if cand.kind == "message" {
					it = cand
					break
				}
			}
			if it == nil {
				it = openItem("message", "msg_"+itoa(nextItemIdx))
				sse.sendEvent("response.content_part.added", responsesEvent{
					ItemID: it.itemID, OutputIndex: it.outIdx, ContentIndex: 0,
					Part: &responsesContent{Type: "output_text", Text: ""},
				})
			}
			it.text.WriteString(c.Text)
			forwarded = true
			sse.sendEvent("response.output_text.delta", responsesEvent{
				ItemID: it.itemID, OutputIndex: it.outIdx, ContentIndex: 0, Delta: c.Text,
			})
		case engine.ChunkReasoning:
			sendCreated()
			var it *respStreamItem
			for _, cand := range items {
				if cand.kind == "reasoning" {
					it = cand
					break
				}
			}
			if it == nil {
				it = openItem("reasoning", "rs_"+itoa(nextItemIdx))
			}
			it.text.WriteString(c.Text)
			forwarded = true
			sse.sendEvent("response.reasoning_text.delta", responsesEvent{
				ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.Text,
			})
		case engine.ChunkToolCallDelta:
			sendCreated()
			it := itemBySlot[c.Index]
			first := !seenToolSlot[c.Index]
			if first {
				seenToolSlot[c.Index] = true
				if it == nil {
					// function_call 项不走 openItem:added 事件必须一次带上
					// call_id/name,拆成两发客户端会看到两个裸项。
					it = &respStreamItem{itemID: "fc_" + itoa(nextItemIdx), kind: "function_call", outIdx: nextItemIdx, callID: c.ToolID, name: c.ToolName}
					nextItemIdx++
					items = append(items, it)
					itemBySlot[c.Index] = it
					sse.sendEvent("response.output_item.added", responsesEvent{Type: "response.output_item.added", OutputIndex: it.outIdx, Item: &responsesItem{
						ID: it.itemID, Type: "function_call", Status: "in_progress",
						CallID: c.ToolID, Name: c.ToolName, Arguments: "",
					}})
				}
			}
			if it == nil {
				return nil // 没开过项也没有首帧信息:无从归属,丢弃
			}
			if c.ToolArguments != "" {
				// block-end:增量已经拼齐就忽略;零参调用(整段参数随
				// block-end 到达)在这里一次发完。
				if it.text.Len() > 0 {
					return nil
				}
				it.text.WriteString(c.ToolArguments)
				forwarded = true
				sse.sendEvent("response.function_call_arguments.delta", responsesEvent{
					ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.ToolArguments,
				})
				return nil
			}
			if c.ToolDelta != "" {
				it.text.WriteString(c.ToolDelta)
				forwarded = true
				sse.sendEvent("response.function_call_arguments.delta", responsesEvent{
					ItemID: it.itemID, OutputIndex: it.outIdx, Delta: c.ToolDelta,
				})
			}
		case engine.ChunkUsage:
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
	if out.Error != "" && !forwarded {
		// 一个被拒的回合且一个事件都没发过:与非流式分支同样回 502。
		openAIError(w, http.StatusBadGateway, "server_error", out.Error)
		return
	}
	if out.Error != "" {
		resp := skeleton("failed", []responsesOutput{}, nil)
		resp.Error = &openAIErrorDetail{Message: out.Error, Type: "server_error", Param: nil, Code: nil}
		sse.sendEvent("response.failed", responsesEvent{Type: "response.failed", Response: resp})
		return
	}

	// 收尾:按打开顺序逐个 item 发 done,再发 completed(带 usage 与最终 output)。
	finalOutput, _ := responsesOutputOf(out)
	for _, it := range items {
		done := responsesItem{ID: it.itemID, Type: it.kind, Status: "completed"}
		switch it.kind {
		case "message":
			done.Role = "assistant"
			done.Content = []responsesContent{{Type: "output_text", Text: it.text.String()}}
		case "reasoning":
			done.Summary = []any{}
		case "function_call":
			done.CallID, done.Name = it.callID, it.name
			args := it.text.String()
			if args == "" {
				args = "{}"
			}
			done.Arguments = args
		}
		sse.sendEvent("response.output_item.done", responsesEvent{
			Type: "response.output_item.done", OutputIndex: it.outIdx, Item: &done,
		})
	}
	u := responsesUsage{}
	if tu, ok := openAIUsageOf(finalUsage); ok {
		u = responsesUsage{InputTokens: tu.PromptTokens, OutputTokens: tu.CompletionTokens, TotalTokens: tu.TotalTokens}
	}
	sse.sendEvent("response.completed", responsesEvent{Type: "response.completed", Response: skeleton("completed", finalOutput, &u)})
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
