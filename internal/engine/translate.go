// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package engine

// OpenAI ↔ harness 翻译与 chunk 折叠。全部无状态纯函数,移植自 src/engine.js
// 的 :440-548(JS 源是行为的最终事实),不 import 本包其余部分。

import (
	"encoding/json"
	"fmt"
	"strings"

	"freerouter/internal/messages"
	"freerouter/internal/stream"
)

// withinToolTurn 报告这是不是**同一轮的工具结果回传**(js :458-462,magpie
// `internal/gateway/affinity.go` turnIn 判据的取反用法)。
//
// 判「最后一条是 tool」而不是「数组里有 tool」:一轮结束后用户又发来新消息,
// 数组里**同时**有上轮的工具结果和新的 user text(正常对话里极常见),按
// 「有 tool」判会把新一轮也钉死在旧出口上,粘性再也不会过期 —— 所以必须看
// 末尾。轮内前缀每一步都在变长,提示词缓存最值钱、换出口代价也最大,这一段
// 强粘(粘性 TTL 不许被缩到基线以下)。
func withinToolTurn(msgs []messages.Message) bool {
	if len(msgs) == 0 {
		return false
	}
	return msgs[len(msgs)-1].Role == "tool"
}

// FromOpenAI 把 OpenAI 请求体换算成 harness 消息(js fromOpenAiMessages :465)。
// responses 为 true 时读 input(Responses API 的 item 列表),否则读 messages。
func FromOpenAI(openAi map[string]any, responses bool) []messages.Message {
	if openAi == nil {
		openAi = map[string]any{}
	}
	var rows []map[string]any
	if responses {
		rows = normaliseResponsesInput(openAi["input"])
	} else if arr, ok := openAi["messages"].([]any); ok {
		rows = make([]map[string]any, 0, len(arr))
		for _, raw := range arr {
			if row, ok := raw.(map[string]any); ok {
				rows = append(rows, row)
			}
		}
	}
	out := make([]messages.Message, 0, len(rows))
	for _, row := range rows {
		role, _ := row["role"].(string)
		if role == "" {
			role = "user" // JS: row.role ?? 'user'
		}
		var blocks []messages.Block
		var toolCalls []messages.ToolCall
		switch content := row["content"].(type) {
		case string:
			if content != "" {
				blocks = append(blocks, messages.Block{Type: "text", Text: content})
			}
		case []any:
			blocks = appendOpenAIContentParts(blocks, content)
		}
		if role == "tool" {
			// role:"tool" 的正文是字符串原样,其余形状 JSON 化(js :487 的
			// typeof row.content === 'string' ? row.content : JSON.stringify(...))。
			// 三个分支都必须保留:content 是空串 → 正文是空串(投影端兜底成
			// "(no output)");content 缺失/null → JS 的 row.content ?? '' 被
			// stringify 成 '""' 字面量;其余 JSON 值原样序列化。
			text := ""
			if s, ok := row["content"].(string); ok {
				text = s
			} else if row["content"] == nil {
				text = `""`
			} else if b, err := json.Marshal(row["content"]); err != nil {
				text = `""`
			} else {
				text = string(b)
			}
			callID, _ := row["tool_call_id"].(string)
			out = append(out, messages.Message{
				Role:       "tool",
				Blocks:     []messages.Block{{Type: "text", Text: text}},
				ToolCallID: callID,
			})
			continue
		}
		if role == "assistant" {
			if calls, ok := row["tool_calls"].([]any); ok {
				for _, raw := range calls {
					call, ok := raw.(map[string]any)
					if !ok {
						continue
					}
					var fn map[string]any
					if v, ok := call["function"].(map[string]any); ok {
						fn = v
					}
					toolCalls = append(toolCalls, messages.ToolCall{
						ID:   stringOf(call["id"]),
						Name: stringOf(fn["name"]),
						// JS: call.function?.arguments ?? '{}'。arguments 在线上
						// 是 JSON 文本字符串,原样直传;非字符串值退化为 {}。
						Arguments: rawJSONOf(fn["arguments"]),
					})
				}
			}
		}
		if len(blocks) == 0 && len(toolCalls) == 0 {
			continue // JS: content.length === 0 → 整条丢弃
		}
		// 角色归一(js :497):developer/system/assistant 保留,其余一律 user。
		switch role {
		case "developer", "system", "assistant":
		default:
			role = "user"
		}
		out = append(out, messages.Message{Role: role, Blocks: blocks, ToolCalls: toolCalls})
	}
	return out
}

// appendOpenAIContentParts 处理 content 数组(js :476-484):字符串部分直接收;
// 对象部分收 text/input_text/output_text 三种拼写的文本,以及 image_url
// (可能是 {url} 对象,也可能是裸字符串)。
func appendOpenAIContentParts(blocks []messages.Block, parts []any) []messages.Block {
	for _, partAny := range parts {
		switch part := partAny.(type) {
		case string:
			if part != "" {
				blocks = append(blocks, messages.Block{Type: "text", Text: part})
			}
		case map[string]any:
			// JS: part?.text ?? part?.input_text ?? part?.output_text —— ??
			// 只在 nullish 时才看下一个,所以键存在但为空串时**不**回退
			// (空串文本本来也会被 !== '' 挡掉,两读等价)。
			for _, key := range []string{"text", "input_text", "output_text"} {
				if v, present := part[key]; present && v != nil {
					if s, ok := v.(string); ok && s != "" {
						blocks = append(blocks, messages.Block{Type: "text", Text: s})
					}
					break // 第一个非 nullish 键即生效,无论它是否空串
				}
			}
			if image := imageURLOf(part); image != "" {
				blocks = append(blocks, messages.Block{Type: "image", Attachment: &messages.Attachment{
					// js :482:{attachmentId:'url:'+前 64 字符, mediaType,
					// bytes:0, width:0, height:0, url}。AttachmentID 按字节截
					// (计划裁决):它只是个不透明去重键,不参与任何解析。
					AttachmentID: "url:" + head64(image),
					MediaType:    "image/png",
					URL:          image,
				}})
			}
		}
	}
	return blocks
}

// imageURLOf 对应 js 的 part?.image_url?.url ?? part?.image_url:对象取 .url,
// 字符串原样。
func imageURLOf(part map[string]any) string {
	v, ok := part["image_url"]
	if !ok || v == nil {
		return ""
	}
	switch image := v.(type) {
	case string:
		return image
	case map[string]any:
		if u, ok := image["url"].(string); ok {
			return u
		}
	}
	return ""
}

// head64 取前 64 字节(计划措辞「前 64 字节」;JS 的 slice(0,64) 是 UTF-16
// 单位,对 ASCII 等价,这里不追求多字节字符上的逐位一致 —— 键只要稳定即可)。
func head64(s string) string {
	if len(s) > 64 {
		return s[:64]
	}
	return s
}

// stringOf 是 JS 的 `x ?? ”` 宽松读法:nil/非字符串都归空串。
func stringOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// rawJSONOf 把 OpenAI 的 arguments 值变成 json.RawMessage:字符串原样(契约:
// 永不过 map 往返,见 messages 包的裁决),缺失/空串退化为 {},其余 JSON 值
// 序列化一次。
func rawJSONOf(v any) json.RawMessage {
	switch x := v.(type) {
	case nil:
		return json.RawMessage("{}")
	case string:
		if x == "" {
			return json.RawMessage("{}")
		}
		return json.RawMessage(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return json.RawMessage("{}")
		}
		return b
	}
}

// normaliseResponsesInput 把 Responses API 的 input 归一成 OpenAI messages 的
// 行形状(js :505-514):字符串 → 单条 user;function_call → assistant +
// tool_calls;function_call_output → role:"tool"。
func normaliseResponsesInput(input any) []map[string]any {
	if s, ok := input.(string); ok {
		return []map[string]any{{"role": "user", "content": s}}
	}
	arr, ok := input.([]any)
	if !ok {
		return nil
	}
	rows := make([]map[string]any, 0, len(arr))
	for _, raw := range arr {
		switch row := raw.(type) {
		case string:
			rows = append(rows, map[string]any{"role": "user", "content": row})
		case map[string]any:
			switch row["type"] {
			case "function_call":
				rows = append(rows, map[string]any{
					"role":    "assistant",
					"content": []any{},
					"tool_calls": []any{map[string]any{
						"id":       row["call_id"],
						"function": map[string]any{"name": row["name"], "arguments": row["arguments"]},
					}},
				})
			case "function_call_output":
				rows = append(rows, map[string]any{
					"role":         "tool",
					"content":      responsesOutputString(row["output"]),
					"tool_call_id": row["call_id"],
				})
			default:
				rows = append(rows, row)
			}
		}
	}
	return rows
}

// responsesOutputString 对应 js 的 String(row.output ?? ”):null/缺失归空串,
// 其余按 fmt.Sprint 的近似(JS 的 String() 对对象给 '[object Object]',Go 给
// map 字面量 —— 两者都只是降级路径,正常链路 output 恒为字符串)。
func responsesOutputString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

// NormalizeTools 把 OpenAI 的 tools 列表读成 harness 工具模式
// (js normalizeTool :516-521):读 tool.name ?? tool.function.name,名字为空
// (或纯空白)的直接丢弃。
//
// **必须**保持 harness 形状直通,不得在这里套一层 toToolDefs —— 历史上套过
// 一层,adapter 自己的 toToolDefs 再读 tool.name 时读到的是 {function:{name}}
// 的外壳,取不到值,于是把全部真实工具丢光:上游只看到指纹四件套(或
// tool_choice: none),整个会话里没有一次模型调过工具(js :170-171 的事故
// 注释,原样搬运)。真正的线形状投影发生在 adapter 按当前 wire 做
// (messages.ToolDefs),本函数只做「OpenAI 两种形状 → harness 一种形状」的
// 归一与丢弃。
func NormalizeTools(list any) []messages.Tool {
	arr, ok := list.([]any)
	if !ok {
		return nil
	}
	out := make([]messages.Tool, 0, len(arr))
	for _, raw := range arr {
		tool, ok := raw.(map[string]any)
		if !ok {
			continue // JS: tool?.name 对标量是 undefined → null → 丢弃
		}
		var fn map[string]any
		if v, ok := tool["function"].(map[string]any); ok {
			fn = v
		}
		name := openAIToolString(tool["name"], fn["name"])
		if strings.TrimSpace(name) == "" {
			continue // js :518:空名返回 null,被 map+filter 丢弃
		}
		params, hasParams := toolParamsOf(tool["parameters"])
		if !hasParams {
			params, hasParams = toolParamsOf(fn["parameters"])
		}
		if !hasParams {
			// js :519 的兜底:{type:'object',properties:{}}
			params = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		description := openAIToolString(tool["description"], fn["description"])
		out = append(out, messages.Tool{Name: name, Description: description, Params: params})
	}
	return out
}

// openAIToolString 是 js 的 `tool?.x ?? tool?.function?.x ?? ”`,非字符串值
// 走 String() 近似。
func openAIToolString(primary, fallback any) string {
	switch v := primary.(type) {
	case string:
		return v
	case nil:
	default:
		return fmt.Sprint(v)
	}
	switch v := fallback.(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// toolParamsOf 只认对象形状的 parameters(js 的 truthy 判定:对象才收)。
func toolParamsOf(v any) (map[string]any, bool) {
	if m, ok := v.(map[string]any); ok {
		return m, true
	}
	return nil, false
}

// ---- chunk 折叠 ----

// toolPayload 是 KindToolCallDelta 的 Chunk.Text 载荷。Chunk 的字段块是计划
// 定死的(五种 Kind 共用 Text 一个通道),而 JS 的 tool-call 增量本携带
// {index, id, name, argumentsDelta} 四元信息 —— 所以 engine 把它打包成 JSON
// 信封塞进 Text;ToolCallDeltaChunk / ToolCallBlockEndChunk 是两个构造器,
// 转发层(from engine 的 Chunk 渲染 OpenAI SSE)用同一对构造器/信封解码,
// 不必再发明形状。
//
//	Delta 非空     → 增量帧:按 Chunk.Index 归并 arguments(js tool-call-delta)
//	Arguments 非空 → 完整帧:按 ID 匹配已有条目,匹配不到才追加(js block-end)
type toolPayload struct {
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Delta     string `json:"delta,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// ToolCallDeltaChunk 构造一个 tool-call 增量 Chunk。
func ToolCallDeltaChunk(index int, id, name, delta string) Chunk {
	b, _ := json.Marshal(toolPayload{ID: id, Name: name, Delta: delta})
	return Chunk{Kind: ChunkToolCallDelta, Index: index, Text: string(b)}
}

// ToolCallBlockEndChunk 构造一个「块结束」的完整 tool-call Chunk:载荷是
// 完整 arguments,foldChunks 按 ID 匹配。
func ToolCallBlockEndChunk(index int, id, name, arguments string) Chunk {
	b, _ := json.Marshal(toolPayload{ID: id, Name: name, Arguments: arguments})
	return Chunk{Kind: ChunkToolCallDelta, Index: index, Text: string(b)}
}

// foldChunks 把一个 Chunk 折进 Outcome(js foldForwardOutcome :523-548)。
// 返回 (FinishReason, 是否出过内容)。文本增量追加、reasoning 增量单独留
// (JS 对它无分支 —— 它是给转发层的内容,不进答案文本)、tool-call 增量按
// Index 归并 arguments、block-end 的 tool-call 用 ID 匹配已有条目(不是
// Index),匹配不到才追加。
func foldChunks(c Chunk, into *Outcome) (FinishReason, bool) {
	switch c.Kind {
	case ChunkText:
		into.Text += c.Text
		return FinishStop, true
	case ChunkReasoning:
		// reasoning 不折进 Text(它不是答案的一部分),但算「出了内容」:
		// 推理增量一样意味着这个出口开始吐 token 了。
		return FinishStop, true
	case ChunkToolCallDelta:
		var p toolPayload
		if err := json.Unmarshal([]byte(c.Text), &p); err != nil {
			return FinishStop, true // 畸形信封按「有内容」处理,不影响轮换账
		}
		if p.Arguments != "" {
			// block-end 完整帧(Arguments 字段是它的判别符,见 toolPayload):
			// 按 **ID** 匹配已有条目(js :535-538),不是 Index。命中即保持
			// 原条目(增量已把 arguments 拼齐,JS 的命中分支什么都不做);
			// 不命中才追加。
			for i := range into.ToolCalls {
				if p.ID != "" && into.ToolCalls[i].ID == p.ID {
					return FinishStop, true
				}
			}
			into.ToolCalls = append(into.ToolCalls, ToolCall{ID: p.ID, Name: p.Name, Arguments: p.Arguments, slot: c.Index})
			return FinishStop, true
		}
		// 增量帧:按 Index 找条目(js :527-531)。
		for i := range into.ToolCalls {
			call := &into.ToolCalls[i]
			if call.slot == c.Index {
				call.Arguments += p.Delta
				if p.Name != "" {
					call.Name = p.Name
				}
				if p.ID != "" {
					call.ID = p.ID
				}
				return FinishStop, true
			}
		}
		into.ToolCalls = append(into.ToolCalls, ToolCall{ID: p.ID, Name: p.Name, Arguments: p.Delta, slot: c.Index})
		return FinishStop, true
	case ChunkUsage:
		into.Usage = c.Usage // JS 是整体替换(:540),adapter 交来的是终值
		return FinishStop, false
	case ChunkFinish:
		if c.Finish == FinishMaxTokens {
			into.Truncated = true // js :542
		}
		return c.Finish, false
	default:
		return FinishStop, false
	}
}

// dropBrokenToolCalls 过滤掉 arguments 不是合法 JSON 的 tool call
// (js :412-416)。max-tokens 收尾意味着 adapter 判定某个调用的参数被截在
// JSON 半截上;保留它会让 OpenAI 答案与 finish_reason 不一致。空 arguments
// 按 {} 参与判定(JS 的 call.arguments === ” ? '{}' : call.arguments)。
func dropBrokenToolCalls(out *Outcome) {
	kept := make([]ToolCall, 0, len(out.ToolCalls))
	for _, call := range out.ToolCalls {
		args := call.Arguments
		if args == "" {
			args = "{}"
		}
		if json.Valid([]byte(args)) {
			kept = append(kept, call)
		}
	}
	out.ToolCalls = kept
}

// OpenAIUsage 把 harness usage 形状换算成 OpenAI 形状(js :419-428)。非流式
// 响应与 /v1/responses 的 usage 都直接透传换算结果,OpenAI 客户端只认
// prompt_tokens/completion_tokens:
//
//	prompt_tokens = inputTokens + cacheReadTokens(上游把缓存读也算进 prompt)
//	prompt_tokens_details.cached_tokens = cacheReadTokens
//
// Go 的 stream.Usage 不携带 totalTokens/reasoningTokens 两个可选量(JS 的 ??
// 缺省分支):total 恒按三项之和,reasoning 恒 0。
func OpenAIUsage(u stream.Usage) map[string]any {
	prompt := u.In + u.CacheRead
	return map[string]any{
		"prompt_tokens":     prompt,
		"completion_tokens": u.Out,
		"total_tokens":      prompt + u.Out,
		"prompt_tokens_details": map[string]any{
			"cached_tokens": u.CacheRead,
		},
		"completion_tokens_details": map[string]any{
			"reasoning_tokens": int64(0),
		},
	}
}
