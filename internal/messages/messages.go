// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package messages 把 harness 消息词汇表投影到三条上游线(移植自 src/messages.js,
// JS 源是行为的最终事实):OpenAI Chat Completions(chat)、OpenAI Responses
// (responses)与 Anthropic Messages(messages/claude)。哪条模型走哪条线由
// internal/upstream 的端点表固定,本包不做选路。
//
// 导出名取 Go 的 To* 惯例,与 JS 的对应关系:
//
//	repairToolPairing → RepairToolPairing
//	toChatMessages    → ToChatMessages
//	toClaudeMessages  → ToClaudeMessages
//	toResponseInput   → ToResponseInput
//	toToolDefs        → ToolDefs(JS 带 style 参数,三种 wire 三种形状,照搬)
//	needsVision       → NeedsVision
//
// 计划骨架里的 ToOpenAI/ToAnthropic 与 Messages 信封(model/max_tokens/stream)
// 不在本包:JS 里请求体信封由 adapter 的 buildPayload 组装,与 JS 的函数方向
// 一一对应即可,不强行凑总纲 §6 的名字(修正案 §3 的裁决)。
//
// 两处有意偏离 JS(任务 13 的裁决):
//
//  1. 未知 role 返回 error,不静默丢弃/兜底 —— 静默丢消息会让模型"忘事"而
//     无人知晓;正常链路的 role 只有五种(engine.FromOpenAI 已归一),这是 tripwire。
//  2. tool arguments 全程 json.RawMessage 原样保留,绝不过 map[string]any 往返
//     (参数顺序与 1.0 这类数字字面量会被改写,上游签名校验会失败)。chat 线
//     与 JS 逐字节一致(字符串直传);claude 线的 input 因此不再经过
//     JSON.parse→stringify 重排,比 JS 更忠实于模型产出,且 Anthropic 只要求
//     input 是合法 JSON。
//
// max_tokens 的 4096 缺省不在本包,JS 的 messages.js 也根本不碰 max_tokens:
// 预算只出自 src/effort.js 的 budgetFor(adapter.js:173 调用,回落
// settings.defaultMaxTokens,面板留空 = 各模型自己的上限);计划测试表里的
// 4096 是 effort 测试中 light(2048)× 思考不可关闭(×2)的算术结果,不是
// 任何层的字面缺省。Go 侧归属 internal/effort + adapter(阶段 3 任务 16)。
package messages

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"freerouter/internal/upstream"
)

// 消息角色:JS 中间表示只有这五种,转发链路里其它 role 已被 engine 归一。
const (
	roleSystem    = "system"
	roleDeveloper = "developer"
	roleUser      = "user"
	roleAssistant = "assistant"
	roleTool      = "tool"
)

// 内容块类型(JS block.type)。tool-call 块提升为 Message.ToolCalls,不在此列。
const (
	blockText  = "text"
	blockImage = "image"
)

// Attachment 对应 JS 的 attachment 记录(engine.FromOpenAI 用客户端的 image_url
// 构造:{attachmentId:'url:'+前 64 字符, mediaType, bytes, width, height, url})。
// 图片是否出境由 adapter 判断(丢弃/编码),本包只透传 —— image_url→attachment
// 的构造逻辑在 engine 侧,不在这里做两份。URL 空串视同"无 URL"
// (JS 的 attachment.url 为 undefined → 图片被丢弃并记警告)。
type Attachment struct {
	AttachmentID string
	MediaType    string
	URL          string
	Bytes        int64
	Width        int
	Height       int
}

// Block 是有序内容块,只承载 text/image 两种(JS 的 tool-call 块提升为
// Message.ToolCalls);type 之外的零值字段按 type 取舍。构造方负责把
// JS blocksOf 兜底的 input_text/output_text/input_image 等外来形状归一成
// 这两种 —— 那是 engine 侧解码的事,本包的 blocksOf 不再做形状猜测。
type Block struct {
	Type       string // "text" | "image"
	Text       string // text 块(JS block.text)
	Offloaded  bool   // image 块:已落盘不出境(JS block.offloaded === true)
	Attachment *Attachment
}

// ToolCall 是 assistant 的工具调用(计划骨架:ToolCalls[]{Name, Args json.RawMessage})。
//
// Arguments 是模型写出的 arguments JSON 原文,不是转义后的字符串字面量:
// chat/responses 线按字符串原样直传,claude 线按原样嵌入 input —— 两条路都
// 不经过 map[string]any。契约:构造方把 OpenAI 的 arguments 字符串原样存入,
// 非字符串值退化为 {};投影端对空值再兜底一次。
type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

// Message 是三条上游线之间的中间表示,对应 JS 的 harness 消息
// {role, content: string|Block[], toolCallId?, isError?, source?}。
type Message struct {
	// Role:system/developer/user/assistant/tool 之一,其余投影时报错。
	Role string
	// Content 与 Blocks 二选一,对应 JS content: string | Block[] 联合类型;
	// Blocks 非 nil 时是权威形态(JS 数组分支),Content 是裸字符串分支
	// (转发链路的 OpenAI 调用方会发裸字符串)。content:null 反序列化后
	// 就是这里的零值 "",天然不会以 null 形态出境。
	Content string
	Blocks  []Block
	// ToolCalls:assistant 的 tool-call 块(JS content 里与 text 同序存放;
	// 提升成独立字段是计划骨架的形状,claude 线因此固定按 text→tool_use
	// 发序 —— 真实数据里模型先文本后调用,转发链路也是 content 先于
	// tool_calls,故只有理论上才可能出现调用先于文本的块序)。
	ToolCalls []ToolCall
	// ToolCallID:tool 结果回答的调用 id(JS toolCallId ?? source.callId;
	// source 兜底由构造方并入,本包只读这一个字段)。
	ToolCallID string
	// IsError:tool 结果是否错误(JS isError === true → tool_result.is_error)。
	IsError bool
	// Name:计划骨架字段。JS 中间表示不携带它(转发链路也不透传),保留
	// 只为对得上骨架 API;投影端从不读。
	Name string
}

// blocksOf 对应 JS blocksOf:把消息内容归一成块列表。
func blocksOf(m Message) []Block {
	if m.Blocks != nil {
		return m.Blocks
	}
	if m.Content == "" {
		return nil
	}
	return []Block{{Type: blockText, Text: m.Content}}
}

// textOf 对应 JS textOf:连接每个带文本的块,以 "\n" 分隔(含空文本块,照抄)。
func textOf(blocks []Block) string {
	parts := make([]string, 0, len(blocks))
	for _, b := range blocks {
		if b.Type == blockText {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// toolOutput 是 tool 结果的正文:textOf 或 '(no output)' 占位 —— 空结果
// 原样出境会让部分上游拒绝,占位是 JS 定下的行为。
func toolOutput(blocks []Block) string {
	if text := textOf(blocks); text != "" {
		return text
	}
	return "(no output)"
}

// ResolveImage 对应 adapter 的 deps.resolveImage:把附件换成可出境的 URL。
// ok=false 等价 JS 返回 undefined(回落 attachment.url,再不行就丢弃+警告)。
type ResolveImage func(Attachment) (string, bool)

// outboundImageURL 复刻 JS 三条投影里同一段取 URL 表达式:offloaded 一票否决;
// resolveImage 优先,未给出(ok=false)时回落 attachment.url。URL 为空串视同
// 无 URL(JS 的 undefined → 丢弃+警告)。
func outboundImageURL(b Block, resolve ResolveImage) (string, bool) {
	if b.Type != blockImage || b.Offloaded {
		return "", false
	}
	if resolve != nil && b.Attachment != nil {
		if url, ok := resolve(*b.Attachment); ok {
			return url, true
		}
	}
	if b.Attachment != nil && b.Attachment.URL != "" {
		return b.Attachment.URL, true
	}
	return "", false
}

// unknownRole:JS 在三条线上对未知 role 一律静默(chat 丢弃,claude/responses
// 兜底成 user);Go 按任务 13 裁决显式报错。见包注释第 1 条偏离。
func unknownRole(wire string, index int, role string) error {
	return fmt.Errorf("messages: unknown role %q at message %d cannot be projected to the %s wire", role, index, wire)
}

// ---- chat 线(OpenAI Chat Completions) ----

// ChatMessage 是 ToChatMessages 产出的上游消息形状。Content 承载 JS 的
// string | parts数组 | null 三态(assistant 仅调用时是 null —— OpenAI 形状
// 允许的空缺;user/system 恒为 string 或 parts)。字段序即 JS 对象插入序
// (role, tool_call_id, content, tool_calls),差分验收按字节比对。
type ChatMessage struct {
	Role       string         `json:"role"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	Content    any            `json:"content"`
	ToolCalls  []ChatToolCall `json:"tool_calls,omitempty"`
}

// ChatToolCall 对应 JS {id, type:'function', function:{name, arguments}}。
type ChatToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function ChatFunction `json:"function"`
}

// ChatFunction 的 Arguments 是 JSON 文本字符串(与 JS 直传一致,逐字节保真)。
type ChatFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ChatPart 是 chat 线的 content part:text 或 image_url,与 JS 键序一致。
type ChatPart struct {
	Type     string        `json:"type"`
	Text     string        `json:"text,omitempty"`
	ImageURL *ChatImageURL `json:"image_url,omitempty"`
}

// ChatImageURL 对应 JS {image_url:{url}}。
type ChatImageURL struct {
	URL string `json:"url"`
}

// argsText 把 arguments 原文作为 chat/responses 线的字符串值;空值兜底成 "{}"
// (JS 对非字符串 arguments 的退化,构造方契约之外的第二道防线)。
func argsText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	return string(raw)
}

// ToChatMessages 对应 JS toChatMessages:投影到 OpenAI Chat Completions。
// assistant 的 reasoning 不回放(中间表示里本来就没有);tool 结果是一等
// role:"tool" 消息,按 call id 键合。
func ToChatMessages(messages []Message, resolve ResolveImage) ([]ChatMessage, []string, error) {
	out := make([]ChatMessage, 0, len(messages))
	var warnings []string
	for i, m := range messages {
		blocks := blocksOf(m)
		switch m.Role {
		case roleSystem, roleDeveloper:
			if text := textOf(blocks); text != "" {
				out = append(out, ChatMessage{Role: roleSystem, Content: text})
			}
		case roleUser:
			parts := make([]ChatPart, 0, len(blocks))
			for _, b := range blocks {
				switch {
				case b.Type == blockText && b.Text != "":
					parts = append(parts, ChatPart{Type: "text", Text: b.Text})
				case b.Type == blockImage:
					if url, ok := outboundImageURL(b, resolve); ok {
						parts = append(parts, ChatPart{Type: "image_url", ImageURL: &ChatImageURL{URL: url}})
					} else {
						warnings = append(warnings, "image-dropped")
					}
				}
			}
			if len(parts) == 0 {
				// JS 同款回落:图片全出局时用文本兜底,文本也没有就整条丢弃
				if fallback := textOf(blocks); fallback != "" {
					out = append(out, ChatMessage{Role: roleUser, Content: fallback})
				}
				continue
			}
			var content any = parts
			if len(parts) == 1 && parts[0].Type == "text" {
				content = parts[0].Text
			}
			out = append(out, ChatMessage{Role: roleUser, Content: content})
		case roleAssistant:
			text := textOf(blocks)
			calls := make([]ChatToolCall, 0, len(m.ToolCalls))
			for _, c := range m.ToolCalls {
				calls = append(calls, ChatToolCall{ID: c.ID, Type: "function", Function: ChatFunction{Name: c.Name, Arguments: argsText(c.Arguments)}})
			}
			if text == "" && len(calls) == 0 {
				continue
			}
			var content any
			if text != "" {
				content = text // JS 的 text || null:仅调用时是 null,不是 ""
			}
			entry := ChatMessage{Role: roleAssistant, Content: content}
			if len(calls) > 0 {
				entry.ToolCalls = calls
			}
			out = append(out, entry)
		case roleTool:
			out = append(out, ChatMessage{Role: roleTool, ToolCallID: m.ToolCallID, Content: toolOutput(blocks)})
		default:
			return nil, nil, unknownRole("chat", i, m.Role)
		}
	}
	return out, warnings, nil
}

// ---- claude 线(Anthropic Messages) ----

// ClaudeImageSource 是 base64 图块的数据源(JS {type:'base64', media_type, data})。
type ClaudeImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

// ClaudeBlock 是 claude 线的内容块,一个结构承载四种块型。字段序取各块型的
// JS 对象插入序(omitempty 跳过无关字段后逐型一致):
// text: type,text;tool_use: type,id,name,input;image: type,source;
// tool_result: type,tool_use_id,content,is_error。
// IsError 用 *bool 是因为 JS 对 tool_result 恒写 is_error(false 也在),
// 其它块型则完全不写该键。
type ClaudeBlock struct {
	Type      string             `json:"type"`
	Text      string             `json:"text,omitempty"`
	ID        string             `json:"id,omitempty"`
	Name      string             `json:"name,omitempty"`
	Input     json.RawMessage    `json:"input,omitempty"`
	Source    *ClaudeImageSource `json:"source,omitempty"`
	ToolUseID string             `json:"tool_use_id,omitempty"`
	Content   string             `json:"content,omitempty"`
	IsError   *bool              `json:"is_error,omitempty"`
}

// ClaudeMessage 是 claude 线的消息。
type ClaudeMessage struct {
	Role    string        `json:"role"`
	Content []ClaudeBlock `json:"content"`
}

// ClaudeShape 对应 JS toClaudeMessages 的返回 {system, messages}:system 是
// 顶层字段而非消息(system/developer 在此合并,"" 等价 JS 的 undefined,即
// 请求体里不带 system 键)。
type ClaudeShape struct {
	System   string          `json:"system,omitempty"`
	Messages []ClaudeMessage `json:"messages"`
}

// imageMedia 是图片线可承载的媒体类型,与 JS IMAGE_MEDIA 同表。
var imageMedia = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/webp": true,
	"image/gif":  true,
}

// dataMediaRe 对应 JS 的 /data:([^;]+)/(不加锚,JS 的 match 也不是)。
var dataMediaRe = regexp.MustCompile(`data:([^;]+)`)

// claudeInput 把 arguments 原文作为 tool_use 的 input 值原样嵌入 —— 任务 13
// 的裁决:绝不过 map[string]any 往返。JS 在此 JSON.parse 后由 JSON.stringify
// 重新序列化,空白/1.0 会被改写;Go 保留原文更忠实于模型产出。非法 JSON
// 退化 "{}"(JS 的 try/catch 同款;空串走 JS 的 `arguments || '{}'` 分支)。
func claudeInput(raw json.RawMessage) json.RawMessage {
	if len(raw) > 0 && json.Valid(raw) {
		out := make(json.RawMessage, len(raw))
		copy(out, raw)
		return out
	}
	return json.RawMessage("{}")
}

// ToClaudeMessages 对应 JS toClaudeMessages:投影到 Anthropic Messages。
// system/developer 合并进顶层 System;tool 变成 user + tool_result;只有
// 受支持的 data URL 才成为 base64 图块,其余只留警告。
func ToClaudeMessages(messages []Message, resolve ResolveImage) (ClaudeShape, []string, error) {
	var shape ClaudeShape
	shape.Messages = make([]ClaudeMessage, 0, len(messages))
	var warnings []string
	for i, m := range messages {
		blocks := blocksOf(m)
		if m.Role == roleSystem || m.Role == roleDeveloper {
			if text := textOf(blocks); text != "" {
				if shape.System != "" {
					shape.System += "\n\n"
				}
				shape.System += text
			}
			continue
		}
		if m.Role == roleTool {
			isError := m.IsError
			shape.Messages = append(shape.Messages, ClaudeMessage{Role: roleUser, Content: []ClaudeBlock{{
				Type:      "tool_result",
				ToolUseID: m.ToolCallID,
				Content:   toolOutput(blocks),
				IsError:   &isError,
			}}})
			continue
		}
		if m.Role != roleUser && m.Role != roleAssistant {
			return ClaudeShape{}, nil, unknownRole("messages", i, m.Role)
		}
		content := make([]ClaudeBlock, 0, len(blocks)+len(m.ToolCalls))
		for _, b := range blocks {
			switch {
			case b.Type == blockText && b.Text != "":
				content = append(content, ClaudeBlock{Type: "text", Text: b.Text})
			case b.Type == blockImage:
				url, ok := outboundImageURL(b, resolve)
				if !ok {
					warnings = append(warnings, "image-dropped")
					continue
				}
				// 只收受支持的 data URL;https 图床 URL 在这条线上出境会被拒
				comma := strings.Index(url, ",")
				head := ""
				if comma >= 0 {
					head = url[:comma]
				}
				match := dataMediaRe.FindStringSubmatch(head)
				if len(match) > 0 && imageMedia[match[1]] {
					data := url // JS 的 slice(comma+1):comma=-1 时取整串(不可达路径,照抄)
					if comma >= 0 {
						data = url[comma+1:]
					}
					content = append(content, ClaudeBlock{Type: "image", Source: &ClaudeImageSource{
						Type: "base64", MediaType: match[1], Data: data,
					}})
				} else {
					warnings = append(warnings, "image-dropped")
				}
			}
		}
		for _, c := range m.ToolCalls {
			content = append(content, ClaudeBlock{Type: "tool_use", ID: c.ID, Name: c.Name, Input: claudeInput(c.Arguments)})
		}
		if len(content) == 0 {
			continue
		}
		role := roleUser
		if m.Role == roleAssistant {
			role = roleAssistant
		}
		shape.Messages = append(shape.Messages, ClaudeMessage{Role: role, Content: content})
	}
	return shape, warnings, nil
}

// ---- responses 线(OpenAI Responses) ----

// RespPart 是 Responses 输入的 content part:input_text/output_text/input_image。
type RespPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
}

// ResponseItem 是 Responses 的 input item(message / function_call /
// function_call_output),字段序即 JS 插入序。Arguments 与 chat 线同款:
// JSON 文本字符串原样直传。
type ResponseItem struct {
	Type      string     `json:"type"`
	Role      string     `json:"role,omitempty"`
	Content   []RespPart `json:"content,omitempty"`
	CallID    string     `json:"call_id,omitempty"`
	Name      string     `json:"name,omitempty"`
	Arguments string     `json:"arguments,omitempty"`
	Output    string     `json:"output,omitempty"`
}

// ToResponseInput 对应 JS toResponseInput:投影到 OpenAI Responses 的 item 列表。
// 早期轮次的 reasoning item 只在签发账号可解,池化免密凭证会轮换账号,回显
// 必然 400 —— 它们在本中间表示里就不存在,结构上不可能出境(与 JS 同理由)。
func ToResponseInput(messages []Message, resolve ResolveImage) ([]ResponseItem, []string, error) {
	out := make([]ResponseItem, 0, len(messages))
	var warnings []string
	for i, m := range messages {
		blocks := blocksOf(m)
		switch m.Role {
		case roleSystem, roleDeveloper:
			if text := textOf(blocks); text != "" {
				out = append(out, ResponseItem{Type: "message", Role: roleSystem,
					Content: []RespPart{{Type: "input_text", Text: text}}})
			}
		case roleTool:
			out = append(out, ResponseItem{Type: "function_call_output", CallID: m.ToolCallID, Output: toolOutput(blocks)})
		case roleAssistant:
			if text := textOf(blocks); text != "" {
				out = append(out, ResponseItem{Type: "message", Role: roleAssistant,
					Content: []RespPart{{Type: "output_text", Text: text}}})
			}
			for _, c := range m.ToolCalls {
				out = append(out, ResponseItem{Type: "function_call", CallID: c.ID, Name: c.Name, Arguments: argsText(c.Arguments)})
			}
		case roleUser:
			parts := make([]RespPart, 0, len(blocks))
			for _, b := range blocks {
				switch {
				case b.Type == blockText && b.Text != "":
					parts = append(parts, RespPart{Type: "input_text", Text: b.Text})
				case b.Type == blockImage:
					if url, ok := outboundImageURL(b, resolve); ok {
						parts = append(parts, RespPart{Type: "input_image", ImageURL: url})
					} else {
						warnings = append(warnings, "image-dropped")
					}
				}
			}
			if len(parts) > 0 {
				out = append(out, ResponseItem{Type: "message", Role: roleUser, Content: parts})
			}
		default:
			return nil, nil, unknownRole("responses", i, m.Role)
		}
	}
	return out, warnings, nil
}

// RepairToolPairing 对应 JS repairToolPairing:摘掉没被回答的调用与没有调用
// 的回答。每条上游线都强制"调用之后必跟结果",一轮被打断(工具没起来、用户
// 中止、进程崩了)就会在持久历史里留下那个形状;回放它不只是难看 —— 免费车道
// 会答 400 [invalid_request_error],并且拖死该会话之后的每一轮。在这里修复
// 一次,三条线同时覆盖。
func RepairToolPairing(messages []Message) []Message {
	answered := map[string]bool{}
	for _, m := range messages {
		if m.Role == roleTool && m.ToolCallID != "" {
			answered[m.ToolCallID] = true
		}
	}
	keptCalls := map[string]bool{}
	out := make([]Message, 0, len(messages))
	for _, m := range messages {
		if m.Role == roleTool {
			// 调用先于自己的结果出现,这里 keptCalls 必已可解析
			if keptCalls[m.ToolCallID] {
				out = append(out, m)
			}
			continue
		}
		if m.Role != roleAssistant {
			out = append(out, m)
			continue
		}
		hasText := false
		for _, b := range blocksOf(m) {
			if b.Type == blockText && b.Text != "" {
				hasText = true
				break
			}
		}
		kept := make([]ToolCall, 0, len(m.ToolCalls))
		for _, c := range m.ToolCalls {
			if answered[c.ID] {
				kept = append(kept, c)
				keptCalls[c.ID] = true
			}
		}
		if len(kept) == len(m.ToolCalls) {
			// 什么都没被摘(含纯文本轮)时原样保留;无调用无文本的空轮丢弃
			if len(kept) > 0 || hasText {
				out = append(out, m)
			}
			continue
		}
		if len(kept) == 0 {
			// 只剩未落地调用的轮次:模型说过的话(文本)要留,但必须留在
			// 摘掉调用后的副本上 —— 推原消息会把 tool_use-without-tool_result
			// 的 400 形状原样回放出去,正是本函数要消灭的东西
			if hasText {
				copied := m
				copied.ToolCalls = nil
				out = append(out, copied)
			}
			continue
		}
		copied := m
		copied.ToolCalls = kept
		out = append(out, copied)
	}
	return out
}

// ---- 工具定义 ----

// 工具定义的三种线形状,对应 JS adapter 的 STYLE_FOR_WIRE
// {chat:'chat', responses:'flat', messages:'claude'}。
const (
	ToolStyleChat   = "chat"   // {type:'function', function:{...}}
	ToolStyleFlat   = "flat"   // {type:'function', name, ..., parameters}
	ToolStyleClaude = "claude" // {name, description, input_schema}
)

// Tool 是 harness 侧的工具模式({name, description, parameters})。
// Params 是 nil 视同"没给参数表"(补默认),空非 nil map 视同 JS 的 truthy
// {}(原样保留)。
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Params      map[string]any `json:"parameters"`
}

// ToolDef 是待序列化的线形状(计划骨架 {Type, Name, Description, Params})。
// 三种 wire 的 JSON 形状不同,由 style 决定 MarshalJSON 的渲染分支 —— JS 的
// toToolDefs 直接拼对象,Go 用自定义 MarshalJSON 达成同一效果。
type ToolDef struct {
	Type        string
	Name        string
	Description string
	Params      map[string]any

	style string
}

// toolFunctionShape 是 chat 线嵌套的 function 对象。
type toolFunctionShape struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// MarshalJSON 按 style 渲染三种形状,键序与 JS 对象字面量插入序一致。
// 空样式走 chat 分支(JS toToolDefs 的 else 分支同款)。
func (d ToolDef) MarshalJSON() ([]byte, error) {
	switch d.style {
	case ToolStyleClaude:
		return json.Marshal(struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			InputSchema map[string]any `json:"input_schema"`
		}{d.Name, d.Description, d.Params})
	case ToolStyleFlat:
		return json.Marshal(struct {
			Type        string         `json:"type"`
			Name        string         `json:"name"`
			Description string         `json:"description"`
			Parameters  map[string]any `json:"parameters"`
		}{"function", d.Name, d.Description, d.Params})
	default:
		return json.Marshal(struct {
			Type     string            `json:"type"`
			Function toolFunctionShape `json:"function"`
		}{"function", toolFunctionShape{d.Name, d.Description, d.Params}})
	}
}

// truncateName 按 rune 截到上游上限(JS 的 slice(0, MAX_TOOL_NAME_LEN) 是
// UTF-16 单位,rune 对 BMP 等价,且避免把多字节字符切成非法 UTF-8)。
func truncateName(name string) string {
	runes := []rune(name)
	if len(runes) <= upstream.MaxToolNameLen {
		return name
	}
	return string(runes[:upstream.MaxToolNameLen])
}

// ToolDefs 对应 JS toToolDefs(tools, style)。计划骨架的 ToolDefs(tools) 没有
// style 参数,但 JS 的三种 wire 三种形状是单一结构表达不了的,故照 JS 签名带上。
// name 去空白、空名跳过、超长截断、缺参数表补 {type:'object',properties:{}}。
func ToolDefs(tools []Tool, style string) []ToolDef {
	list := make([]ToolDef, 0, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		params := tool.Params
		if params == nil {
			params = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		list = append(list, ToolDef{
			Type:        "function",
			Name:        truncateName(name),
			Description: tool.Description,
			Params:      params,
			style:       style,
		})
	}
	return list
}

// NeedsVision 对应 JS needsVision:只有真正带图(且未 offload)的消息才要求
// 视觉模型。offloaded 的块即便带着 URL 也不再出境,不构成视觉需求。
func NeedsVision(messages []Message) bool {
	for _, m := range messages {
		for _, b := range blocksOf(m) {
			if b.Type == blockImage && !b.Offloaded {
				return true
			}
		}
	}
	return false
}
