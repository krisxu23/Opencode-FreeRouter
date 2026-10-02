// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package forward

import (
	"encoding/json"
	"net/http"
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
	Object string        `json:"object"`
	Data   []engine.Row  `json:"data"`
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
// 算术本身留在 engine.OpenAIUsage 里(单一事实来源,engine 的注释明确把这一步
// 交给转发层调用),这里只是把它的 map 抄进有序结构体。第二返回值对应
// `usage === undefined`:没有 usage 时**整个 details 段都不出现**,
// 调用方拿到的就是三个 0 —— 与 js :231 的 `outcome.usage ?? {...}` 一致。
func openAIUsageOf(u stream.Usage) (openAIUsage, bool) {
	if !u.HasUsage {
		return openAIUsage{}, false
	}
	m := engine.OpenAIUsage(u)
	num := func(key string) int64 {
		switch v := m[key].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		default:
			return 0
		}
	}
	details := func(key string) int64 {
		sub, ok := m[key].(map[string]any)
		if !ok {
			return 0
		}
		switch v := sub["cached_tokens"].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		}
		switch v := sub["reasoning_tokens"].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		}
		return 0
	}
	return openAIUsage{
		PromptTokens:            num("prompt_tokens"),
		CompletionTokens:        num("completion_tokens"),
		TotalTokens:             num("total_tokens"),
		PromptTokensDetails:     &promptTokensDetails{CachedTokens: details("prompt_tokens_details")},
		CompletionTokensDetails: &completionTokensDetails{ReasoningTokens: details("completion_tokens_details")},
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
	Index        int         `json:"index"`
	Delta        chunkDelta  `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
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
}

// ---- tool-call 信封解码 ----

// toolEnvelope 是 engine.toolPayload 的对侧。engine 把 tool-call 增量打包成
// JSON 塞进 Chunk.Text(见 engine/translate.go:347-361 的注释:转发层用同一对
// 构造器/信封解码,不必再发明形状),这里按同一组 json tag 解开。
//
//	Delta 非空     → 增量帧(js 的 tool-call-delta)
//	Arguments 非空 → 完整帧(js 的 block-end)
type toolEnvelope struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Delta     string `json:"delta"`
	Arguments string `json:"arguments"`
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

func (s *sseStream) send(event any) {
	s.ensure()
	raw, err := marshalNoEscape(event)
	if err != nil {
		return
	}
	// 与 js :183-185 的 `data: ${JSON.stringify(event)}\n\n` 逐字节一致。
	buf := make([]byte, 0, len(raw)+8)
	buf = append(buf, "data: "...)
	buf = append(buf, raw...)
	buf = append(buf, '\n', '\n')
	_, _ = s.w.Write(buf)
	s.w.Flush()
}

func (s *sseStream) done() {
	s.ensure()
	_, _ = s.w.Write([]byte("data: [DONE]\n\n"))
	s.w.Flush()
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
		// 实测:同样的耗尽错误 JS 回 500、Go 回 502)。
		openAIError(w, http.StatusInternalServerError, "server_error", messageOf(err))
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
	writeJSON(w, http.StatusOK, chatCompletionBody{
		ID: id, Object: "chat.completion", Created: created, Model: model,
		Choices: []chatChoice{{Index: 0, Message: msg, FinishReason: finish}},
		Usage:   usage,
	})
}

func (s *Server) chatCompletionStream(w *writer, r *http.Request, body map[string]any, model, id string, created int64) {
	stream := &sseStream{w: w}
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

	seenToolStart := map[int]bool{}
	forwarded := false

	onChunk := func(c engine.Chunk) error {
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
			var env toolEnvelope
			if err := json.Unmarshal([]byte(c.Text), &env); err != nil {
				return nil // 畸形信封:跳过,不影响已经发出去的内容
			}
			if env.Arguments != "" {
				// block-end 完整帧。js 的 onChunk 没有这个分支(增量已经把
				// arguments 拼齐了),转发层同样必须忽略它 —— 否则同一个
				// tool call 会被发两遍,SDK 会当成两个调用。
				return nil
			}
			start()
			forwarded = true
			first := !seenToolStart[c.Index]
			seenToolStart[c.Index] = true
			entry := toolCallDelta{Index: c.Index}
			if first {
				entry.ID = env.ID
				entry.Function = &toolFuncDelta{Name: env.Name, Arguments: ""}
			}
			stream.send(chunkFrame{
				ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
				Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{entry}}}},
			})
			if env.Delta != "" {
				stream.send(chunkFrame{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []chunkChoice{{Index: 0, Delta: chunkDelta{ToolCalls: []toolCallDelta{
						{Index: c.Index, Function: &toolFuncDelta{Arguments: env.Delta}},
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
			// 调用方拿到的是一个真正的 502,而不是一个「成功但 0 token」的流。
			openAIError(w, http.StatusBadGateway, "server_error", messageOf(err))
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

	// js :314 `{ ...body, input: body.input ?? body.messages ?? [] }`。
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

	out, err := s.complete(r.Context(), engine.Request{Model: model, OpenAI: openAI, Responses: true}, nil)
	if err != nil {
		// 与 chatCompletionOnce 同理:JS 的 throw 走顶层 catch → 500(js :316
		// 的 502 只属于 resolve 成 outcome.error 的拒绝)。
		openAIError(w, http.StatusInternalServerError, "server_error", messageOf(err))
		return
	}
	if refused(out) {
		openAIError(w, http.StatusBadGateway, "server_error", out.Error)
		return
	}

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
	writeJSON(w, http.StatusOK, responsesBody{
		ID: id, Object: "response", CreatedAt: nowSeconds(), Model: model, Status: "completed",
		Output: output, Usage: usage,
	})
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

