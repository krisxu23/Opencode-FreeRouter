// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package adapter

// 上行投影:三条线的帧 → 增量事件 + 收尾事实。
//
// 为什么这一层存在:引擎要的**不是**文本字符串。一次模型回合里除了正文还有推理
// 增量与 tool-call 增量,而 function calling 的全部信息就在后者里 —— 只把
// delta.content 交出去,网关看起来「能连、能出字」,调用方的工具却永远执行不到
// (审计 B13)。投影层把 js stream.js 的 BlockSink 与 readStream 状态合起来移植,
// 因为有两件事只有「按槽位累积」才做得到:
//
//   - 一个 tool-call 的参数是否解析得动(决定 finish 是 tool_calls 还是
//     max-tokens);
//   - 这一轮是否真的产出过任何东西(决定空响应)。
//
// adapter 在 L3、engine 在 L4,adapter 不能反向 import engine,所以这里自带一套
// 事件词汇(Delta),由 engine 的 attempt 翻译成 engine.Chunk;Chunk 与
// foldChunks 因此零改动。

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"freerouter/internal/stream"
	"freerouter/internal/upstream"
)

// DeltaKind 是投影层交给上层的增量种类。
type DeltaKind int

const (
	// DeltaText 是可见正文增量。
	DeltaText DeltaKind = iota
	// DeltaReasoning 是推理增量:chat 的 delta.reasoning / reasoning_details、
	// messages 的 thinking_delta、responses 的 reasoning 摘要。
	DeltaReasoning
	// DeltaToolCall 是 tool-call 的参数增量(Text)。
	DeltaToolCall
	// DeltaToolCallEnd 是 tool-call 块的收尾帧,Arguments 是拼齐的参数。
	// engine 的 foldChunks 用它登记「一个参数增量都没有的调用」。
	DeltaToolCallEnd
)

// Delta 是一条上行增量事件。
type Delta struct {
	Kind DeltaKind
	// Index 是块序号,由投影层按到达顺序分配(engine 的 Chunk.Index、转发层的
	// tool_calls[].index 都以它归并,所以分配顺序必须与 JS 一致)。
	Index int
	// ID/Name 只在 tool-call 两类事件上有值;Name 已经过 RestoreToolName 还原
	// 成调用方自己的拼写。
	ID   string
	Name string
	// Text 是正文/推理增量,或 tool-call 的参数增量。
	Text string
	// Arguments 只在 DeltaToolCallEnd 上有值:拼齐后的完整参数(空参数按 "{}")。
	Arguments string
}

// Result 是一轮 Complete 的收尾事实。
//
// 三个 saw 位与 BrokenToolCall 只有投影层看得见:上层收到的是折过的文本,分不出
// 「上游报了 stop 但一个块都没开」和「上游报了 stop 且出了正文」。这两件事正是
// finish 词汇的两个分支 —— 参数截在半截 JSON 上要降级成 max-tokens,全空的正常
// 收尾要归成 CodeEmpty(js adapter.js:238-262)。
type Result struct {
	stream.Usage
	// Finish 是上游报的收尾 token 原文:chat 的 finish_reason、messages 的
	// stop_reason、responses 的 status(JS 在 response.completed 处就把
	// max_output_tokens 归成 "length")。整条流没报过时是空串,与 "stop" 同价。
	Finish string
	// SawText/SawReasoning/SawToolCall:这一轮产出过哪几类块。SawToolCall 只要
	// 上游开过 tool-call 块就为真,哪怕一个参数增量都没来。
	SawText      bool
	SawReasoning bool
	SawToolCall  bool
	// BrokenToolCall:至少一个 tool-call 块的拼齐参数不是合法 JSON。截断的一轮
	// 上游照样报 finish "tool_calls"(实测 2026-09-25),所以收尾 token 单独
	// 判不出来,只能靠累积的参数。
	BrokenToolCall bool
}

// 块种类(与 js BlockSink 的 kind 字符串同义)。
const (
	blockText = iota
	blockReasoning
	blockToolCall
)

// block 是一个已开启的流块。
type block struct {
	index int
	kind  int
	// text 是正文/推理块的累积值(只为与 JS 形状对齐,投影层的判定不读它)。
	text string
	// args 是 tool-call 块拼齐中的参数。
	args string
	id   string
	name string
}

// sink 是 js stream.js:27-101 的 BlockSink 加上 readStream 的每轮流状态。
//
// 槽位键与 JS 逐字对应:chat 的 t / r / c<call.index>,messages 的
// b<event.index>,responses 的 t<output_index> / r<item_id> /
// i<output_index>。键必须稳定,否则同一个调用的参数会被折进两个块。
type sink struct {
	emit    func(Delta) error
	renames map[string]string

	// 流状态(对应 js readStream 的 state)。
	acc    stream.Usage
	first  bool
	finish string

	next  int
	order []*block
	byKey map[string]*block

	sawText        bool
	sawReasoning   bool
	sawToolCall    bool
	brokenToolCall bool
}

func newSink(emit func(Delta) error, renames map[string]string) *sink {
	if emit == nil {
		emit = func(Delta) error { return nil }
	}
	return &sink{emit: emit, renames: renames, byKey: map[string]*block{}}
}

// result 交出当前的收尾事实。任何时刻调用都安全:error 路径也要把已经折出的
// usage 带出去(engine 收到错误时仍然上报 usage)。
func (s *sink) result() Result {
	return Result{
		Usage:          s.acc,
		Finish:         s.finish,
		SawText:        s.sawText,
		SawReasoning:   s.sawReasoning,
		SawToolCall:    s.sawToolCall,
		BrokenToolCall: s.brokenToolCall,
	}
}

// slot 取(必要时按到达顺序开)一个槽位对应的块。
//
// tool-call 块在开出时就铸一个 id:没有 id 的调用下一轮会带着空 toolCallId 回来,
// 配对修复会把 assistant 与 tool 两侧一起丢掉,模型永远看不到自己的工具结果,
// 于是反复重发同一个调用(js stream.js:42-45)。上游稍后给的真 id 覆盖它。
func (s *sink) slot(key string, kind int) *block {
	if b, ok := s.byKey[key]; ok {
		return b
	}
	b := &block{index: s.next, kind: kind}
	s.next++
	if kind == blockToolCall {
		b.id = mintToolCallID()
		s.sawToolCall = true
	}
	s.byKey[key] = b
	s.order = append(s.order, b)
	return b
}

func (s *sink) text(key, delta string) error {
	if delta == "" {
		return nil
	}
	s.sawText = true
	b := s.slot(key, blockText)
	b.text += delta
	return s.emit(Delta{Kind: DeltaText, Index: b.index, Text: delta})
}

func (s *sink) reasoning(key, delta string) error {
	if delta == "" {
		return nil
	}
	s.sawReasoning = true
	b := s.slot(key, blockReasoning)
	b.text += delta
	return s.emit(Delta{Kind: DeltaReasoning, Index: b.index, Text: delta})
}

// toolStart 只登记 id/name,不发帧(JS 的 BlockSink.toolStart 同样不发):
// 调用方要看到的是一个带参数的调用,单独一帧「有个调用开始了」不是可用信息。
func (s *sink) toolStart(key, id, name string) {
	b := s.slot(key, blockToolCall)
	if id != "" {
		b.id = id
	}
	if name != "" {
		b.name = name
	}
}

func (s *sink) toolArgs(key, delta string) error {
	if delta == "" {
		return nil
	}
	b := s.slot(key, blockToolCall)
	b.args += delta
	return s.emit(Delta{Kind: DeltaToolCall, Index: b.index, ID: b.id, Name: b.name, Text: delta})
}

// closeAll 收尾每一个还开着的块。
//
// 只发 tool-call 的收尾帧:JS 也给正文/推理块发 block-end,但 engine 的
// foldChunks 与转发层都不消费那两帧(正文已经由增量帧折完),在这里发只会让
// 上行事件表里多出没人读的东西。tool-call 的收尾帧必须发 —— 它是「零参数
// 调用」唯一的登记途径。
//
// 顺序:JS 的 open 是 Map,按插入序迭代;Go 的 map 无序,所以另开一个 order
// 切片,保证多调用轮次的 ToolCalls 次序与 JS 一致。
func (s *sink) closeAll() error {
	for _, b := range s.order {
		if b.kind != blockToolCall {
			continue
		}
		args := b.args
		if args == "" {
			args = "{}"
		}
		if !json.Valid([]byte(args)) {
			s.brokenToolCall = true
		}
		if err := s.emit(Delta{Kind: DeltaToolCallEnd, Index: b.index, ID: b.id, Name: b.name, Arguments: args}); err != nil {
			return err
		}
	}
	s.byKey = map[string]*block{}
	s.order = nil
	return nil
}

// setFinish 记下游方的收尾 token。后到的覆盖先到的(JS 的 state.finish 同理)。
func (s *sink) setFinish(token string) {
	if token == "" {
		return
	}
	s.finish = token
}

// mintToolCallID 对应 js stream.js:23-25 的
// `call_${crypto.randomBytes(12).toString('hex')}`。
func mintToolCallID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		// 熵源坏掉时退回时间派生的字节:形状仍然合法(24 个十六进制字符),
		// 代价是唯一性变弱 —— 比 panic 拖垮整条转发链路好。同包内的
		// upstream.mintID 是同样的取舍。
		v := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(v >> (8 * (i % 8)))
		}
	}
	return "call_" + hex.EncodeToString(b[:])
}

// ---- 三条线的逐帧投影 ----

// feedChat 投影 chat 线的一帧(js stream.js:140-163)。
//
// 畸形 choice([null]、字符串)跳过而不是抛:上游帧是外部数据,
// JS 那边靠可选链把它读成 undefined 后静默跳过,Go 的断言必须同样宽容。
func feedChat(p map[string]any, s *sink) error {
	choices, _ := p["choices"].([]any)
	for _, choice := range choices {
		cm, ok := choice.(map[string]any)
		if !ok {
			continue
		}
		delta, _ := cm["delta"].(map[string]any)
		if delta == nil {
			// 非流式的 message 体走 readJSON 的整包分支,不在这里处理。
			continue
		}
		// if/else if 是刻意的(JS 同形):reasoning 只要**是字符串**(哪怕是空串)
		// 就不再读 reasoning_details,否则一帧里两路来源会把同一段推理折两遍。
		if text, ok := delta["reasoning"].(string); ok {
			if err := s.reasoning("r", text); err != nil {
				return err
			}
		} else if details, ok := delta["reasoning_details"].([]any); ok {
			for _, part := range details {
				pm, _ := part.(map[string]any)
				if text, ok := pm["text"].(string); ok {
					if err := s.reasoning("r", text); err != nil {
						return err
					}
				}
			}
		}
		if text, ok := delta["content"].(string); ok {
			if err := s.text("t", text); err != nil {
				return err
			}
		}
		calls, _ := delta["tool_calls"].([]any)
		for _, raw := range calls {
			call, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			idx, _ := numOf(call["index"])
			key := "c" + strconv.FormatInt(idx, 10)
			fn, _ := call["function"].(map[string]any)
			id, _ := call["id"].(string)
			name, _ := fn["name"].(string)
			switch {
			case name != "":
				// 还原调用方拼写:闸门把线上名规范成小写,renames 里存着原名,
				// 回程不还原的话调用方收到的工具名与它声明的不是同一个。
				s.toolStart(key, id, upstream.RestoreToolName(name, s.renames))
			case id != "":
				s.toolStart(key, id, "")
			}
			if args, _ := fn["arguments"].(string); args != "" {
				if err := s.toolArgs(key, args); err != nil {
					return err
				}
			}
		}
		if token, ok := cm["finish_reason"].(string); ok {
			s.setFinish(token)
		}
	}
	return nil
}

// feedClaude 投影 messages 线的一帧(js stream.js:166-199)。usage 由
// feedClaudeUsage 单独处理,这里只管内容增量与收尾 token。
func feedClaude(p map[string]any, s *sink) error {
	switch p["type"] {
	case "content_block_start":
		cb, _ := p["content_block"].(map[string]any)
		if cb == nil || cb["type"] != "tool_use" {
			return nil
		}
		id, _ := cb["id"].(string)
		name, _ := cb["name"].(string)
		s.toolStart("b"+indexKey(p, "index"), id, upstream.RestoreToolName(name, s.renames))
		return nil
	case "content_block_delta":
		part, _ := p["delta"].(map[string]any)
		if part == nil {
			return nil
		}
		key := "b" + indexKey(p, "index")
		switch part["type"] {
		case "text_delta":
			text, _ := part["text"].(string)
			return s.text(key, text)
		case "thinking_delta":
			text, _ := part["thinking"].(string)
			return s.reasoning(key, text)
		case "input_json_delta":
			text, _ := part["partial_json"].(string)
			return s.toolArgs(key, text)
		}
		return nil
	case "message_delta":
		// 收尾 token 在 delta.stop_reason 上。
		part, _ := p["delta"].(map[string]any)
		if part == nil {
			return nil
		}
		token, _ := part["stop_reason"].(string)
		s.setFinish(token)
		return nil
	}
	return nil
}

// feedResponses 投影 responses 线的一帧(js stream.js:202-239)。usage 由
// feedResponsesUsage 单独处理。
func feedResponses(p map[string]any, s *sink) error {
	switch p["type"] {
	case "response.output_item.added":
		item, _ := p["item"].(map[string]any)
		if item == nil || item["type"] != "function_call" {
			return nil
		}
		// call_id 优先于 id:两个键都出现过,语义一样。
		id, _ := item["call_id"].(string)
		if id == "" {
			id, _ = item["id"].(string)
		}
		name, _ := item["name"].(string)
		s.toolStart("i"+indexKey(p, "output_index"), id, upstream.RestoreToolName(name, s.renames))
		return nil
	case "response.output_text.delta":
		text, _ := p["delta"].(string)
		return s.text("t"+indexKey(p, "output_index"), text)
	case "response.reasoning_summary_text.delta", "response.output_reasoning.text.delta":
		text, _ := p["delta"].(string)
		key, _ := p["item_id"].(string)
		if key == "" {
			key = "r"
		}
		return s.reasoning("r"+key, text)
	case "response.function_call_arguments.delta":
		text, _ := p["delta"].(string)
		return s.toolArgs("i"+indexKey(p, "output_index"), text)
	case "response.completed", "response.incomplete", "response.failed":
		// 收尾 token 在 JS 侧就被归一:被输出上限截断的一律叫 "length"
		//(engine 的 finishReason 认这个名字),正常完成叫 "stop"。
		//
		// 事件名要认全(整分支评审 BUG-1):responses 线有三个并列的终止事件,
		// 被 max_output_tokens 截断走的是 **response.incomplete**,上游故障走的是
		// `response.failed`,而这里过去只有 `response.completed` —— 截断的一轮因此
		// finish 恒为空串,engine 落 default 报 `stop`:客户端拿到一条腰斩的回答却
		// 被告知「正常结束」,不会去续写(muse-spark 家族就是 responses 线)。
		// 三个事件的载荷形状相同(response.status / incomplete_details.reason),
		// 共用这一支即可;failed 会带着 status:"failed" 落到 default,
		// finishReasonOf 认不出它 ⇒ 收尾 stop,但 BrokenToolCall/EMPTY 两道判据
		// 仍然在前面兜住(不在这里发明新的错误路径,没有固件支撑它)。
		resp, _ := p["response"].(map[string]any)
		if resp == nil {
			return nil
		}
		details, _ := resp["incomplete_details"].(map[string]any)
		reason, _ := details["reason"].(string)
		status, _ := resp["status"].(string)
		switch {
		case reason == "max_output_tokens":
			s.setFinish("length")
		case status == "completed":
			s.setFinish("stop")
		default:
			s.setFinish(status)
		}
		return nil
	}
	return nil
}

// indexKey 把帧里的序号投影成槽位名的一部分。缺失按 0 —— JS 会拼出
// "undefined",但那只在畸形帧上出现,且同一条调用的相邻帧拼出的键仍然相同。
func indexKey(p map[string]any, field string) string {
	v, _ := numOf(p[field])
	return strconv.FormatInt(v, 10)
}
