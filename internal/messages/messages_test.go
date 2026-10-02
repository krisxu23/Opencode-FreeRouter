// SPDX-License-Identifier: GPL-3.0-or-later
package messages

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"freerouter/internal/upstream"
)

// ---- 测试工具 ----

func mustChat(t *testing.T, msgs []Message, resolve ResolveImage) ([]ChatMessage, []string) {
	t.Helper()
	out, warnings, err := ToChatMessages(msgs, resolve)
	if err != nil {
		t.Fatalf("ToChatMessages: %v", err)
	}
	return out, warnings
}

func mustClaude(t *testing.T, msgs []Message, resolve ResolveImage) (ClaudeShape, []string) {
	t.Helper()
	shape, warnings, err := ToClaudeMessages(msgs, resolve)
	if err != nil {
		t.Fatalf("ToClaudeMessages: %v", err)
	}
	return shape, warnings
}

func mustResponses(t *testing.T, msgs []Message, resolve ResolveImage) ([]ResponseItem, []string) {
	t.Helper()
	out, warnings, err := ToResponseInput(msgs, resolve)
	if err != nil {
		t.Fatalf("ToResponseInput: %v", err)
	}
	return out, warnings
}

// rawJSON 返回确定性紧凑 JSON,用于钉死键序(差分验收按字节比对请求体)。
func rawJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// jsonEq 做语义级比对(反序列化后 DeepEqual),键序无关 —— 用于含
// map[string]any 参数表的形状(Go 的 map 序列化按键排序,与 JS 插入序不同)。
func jsonEq(t *testing.T, got any, want string) {
	t.Helper()
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var g, w any
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("JSON mismatch:\n got: %s\nwant: %s", raw, want)
	}
}

// chatBackToMessages 把 chat 线输出手工映射回中间表示(round trip 测试用)。
func chatBackToMessages(t *testing.T, chat []ChatMessage) []Message {
	t.Helper()
	var out []Message
	for _, cm := range chat {
		switch cm.Role {
		case "system":
			out = append(out, Message{Role: "system", Content: cm.Content.(string)})
		case "user":
			switch c := cm.Content.(type) {
			case string:
				out = append(out, Message{Role: "user", Content: c})
			case []ChatPart:
				out = append(out, Message{Role: "user", Blocks: []Block{{Type: "text", Text: c[0].Text}}})
			default:
				t.Fatalf("unexpected user content type %T", cm.Content)
			}
		case "assistant":
			m := Message{Role: "assistant"}
			if s, ok := cm.Content.(string); ok {
				m.Content = s
			}
			for _, tc := range cm.ToolCalls {
				m.ToolCalls = append(m.ToolCalls, ToolCall{ID: tc.ID, Name: tc.Function.Name, Arguments: json.RawMessage(tc.Function.Arguments)})
			}
			out = append(out, m)
		case "tool":
			out = append(out, Message{Role: "tool", ToolCallID: cm.ToolCallID, Content: cm.Content.(string)})
		default:
			t.Fatalf("unexpected role %q", cm.Role)
		}
	}
	return out
}

// ---- 计划任务 13 的 8 条测试 ----

// OpenAI messages[0].role=="system" → Anthropic 顶层 system,不进 Messages;
// chat 线的 system 仍是首条消息(JS toChatMessages 不上提,developer 同法合成)。
func TestSystemBecomesTopLevelField(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "be brief"},
		{Role: "developer", Content: "be terse"},
		{Role: "user", Content: "hi"},
	}
	shape, _ := mustClaude(t, msgs, nil)
	if got := rawJSON(t, shape); got != `{"system":"be brief\n\nbe terse","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}` {
		t.Fatalf("claude system 不在顶层: %s", got)
	}
	chat, _ := mustChat(t, msgs, nil)
	if got := rawJSON(t, chat); got != `[{"role":"system","content":"be brief"},{"role":"system","content":"be terse"},{"role":"user","content":"hi"}]` {
		t.Fatalf("chat 线 system 形状不对: %s", got)
	}
}

// tool_calls 的 arguments 必须原文到达两条线:claude 的 input 原样嵌入,
// chat 的 arguments 字符串原样直传 —— 全程不过 map[string]any 往返。
func TestToolCallsToToolUse(t *testing.T) {
	msgs := []Message{{Role: "assistant", Content: "calling", ToolCalls: []ToolCall{
		{ID: "call_1", Name: "read", Arguments: json.RawMessage(`{"path":"a.txt"}`)},
	}}}
	shape, _ := mustClaude(t, msgs, nil)
	block := shape.Messages[0].Content[1]
	if block.Type != "tool_use" || block.ID != "call_1" || block.Name != "read" {
		t.Fatalf("tool_use 形状不对: %+v", block)
	}
	// 1.0 / 键序这类非规范化文本必须原样到达 —— 改写了上游签名校验就会失败
	if got := string(block.Input); got != `{"path":"a.txt"}` {
		t.Fatalf("input 不再是原文: %s", got)
	}
	if got := rawJSON(t, shape.Messages[0]); got != `{"role":"assistant","content":[{"type":"text","text":"calling"},{"type":"tool_use","id":"call_1","name":"read","input":{"path":"a.txt"}}]}` {
		t.Fatalf("claude assistant 形状/键序不对: %s", got)
	}
	chat, _ := mustChat(t, msgs, nil)
	if got := rawJSON(t, chat); got != `[{"role":"assistant","content":"calling","tool_calls":[{"id":"call_1","type":"function","function":{"name":"read","arguments":"{\"path\":\"a.txt\"}"}}]}]` {
		t.Fatalf("chat tool_calls 形状/键序不对: %s", got)
	}
	// arguments 缺失(构造方对非字符串值的契约退化)→ "{}";仅调用无文本 →
	// content 是 JS 的 null(OpenAI 形状本身允许的空缺)
	chat2, _ := mustChat(t, []Message{{Role: "assistant", ToolCalls: []ToolCall{{ID: "call_2", Name: "bash"}}}}, nil)
	if got := rawJSON(t, chat2); got != `[{"role":"assistant","content":null,"tool_calls":[{"id":"call_2","type":"function","function":{"name":"bash","arguments":"{}"}}]}]` {
		t.Fatalf("缺 arguments 的退化不对: %s", got)
	}
}

// role:"tool" → chat 线 role:"tool"+tool_call_id;claude 线 user+tool_result
// (is_error 恒在,false 也要在);responses 线 function_call_output。
func TestToolResultsToToolMessages(t *testing.T) {
	msgs := []Message{
		{Role: "tool", ToolCallID: "call_1", Content: "body"},
		{Role: "tool", ToolCallID: "call_2", IsError: true},
	}
	chat, _ := mustChat(t, msgs, nil)
	if got := rawJSON(t, chat); got != `[{"role":"tool","tool_call_id":"call_1","content":"body"},{"role":"tool","tool_call_id":"call_2","content":"(no output)"}]` {
		t.Fatalf("chat tool 结果形状不对: %s", got)
	}
	shape, _ := mustClaude(t, msgs, nil)
	if got := rawJSON(t, shape); got != `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"body","is_error":false}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_2","content":"(no output)","is_error":true}]}]}` {
		t.Fatalf("claude tool_result 形状不对(is_error:false 必须在): %s", got)
	}
	resp, _ := mustResponses(t, msgs, nil)
	if got := rawJSON(t, resp); got != `[{"type":"function_call_output","call_id":"call_1","output":"body"},{"type":"function_call_output","call_id":"call_2","output":"(no output)"}]` {
		t.Fatalf("responses function_call_output 形状不对: %s", got)
	}
}

// 多模态:有可用 URL 的图片按 JS 行为出境(image_url part),offloaded/无 URL
// 的只留 image-dropped 警告;图片全部出局时消息塌缩成纯文本 "a"。
func TestMultiModalCollapsesToText(t *testing.T) {
	resolve := ResolveImage(func(a Attachment) (string, bool) {
		if a.URL == "https://example.com/a.png" {
			return "data:image/png;base64,AAAA", true
		}
		return "", false
	})
	msgs := []Message{
		{Role: "user", Blocks: []Block{
			{Type: "text", Text: "look"},
			{Type: "image", Attachment: &Attachment{URL: "https://example.com/a.png"}},
		}},
		{Role: "user", Blocks: []Block{{Type: "image", Offloaded: true, Attachment: &Attachment{URL: "https://example.com/b.png"}}}},
		{Role: "user", Blocks: []Block{{Type: "image", Attachment: &Attachment{}}}},
	}
	chat, warnings := mustChat(t, msgs, resolve)
	if got := rawJSON(t, chat); got != `[{"role":"user","content":[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]` {
		t.Fatalf("chat 多模态形状不对: %s", got)
	}
	if !reflect.DeepEqual(warnings, []string{"image-dropped", "image-dropped"}) {
		t.Fatalf("warnings 不对: %v", warnings)
	}
	// 计划正文的方向(图片丢弃 → 塌缩成文本)对应"无可用 URL"的分支;
	// 剩余单文本 part 折叠成字符串
	chat2, warnings2 := mustChat(t, []Message{{Role: "user", Blocks: []Block{
		{Type: "text", Text: "a"},
		{Type: "image", Attachment: &Attachment{}},
	}}}, nil)
	if got := rawJSON(t, chat2); got != `[{"role":"user","content":"a"}]` {
		t.Fatalf("塌缩结果不对: %s", got)
	}
	if !reflect.DeepEqual(warnings2, []string{"image-dropped"}) {
		t.Fatalf("warnings2 不对: %v", warnings2)
	}
}

// content:null / 空内容进来落成 ""(Go string 零值),投影端绝不把 JSON null
// 内容发给上游(上游 400):user/system 的空文本整条跳过,tool 空结果落
// '(no output)' 占位。assistant 仅调用时的 content:null 是 OpenAI 形状本身
// 允许的空缺(见 TestToolCallsToToolUse),与"空内容丢成 null"是两回事。
func TestEmptyContentBecomesEmptyStringNotNull(t *testing.T) {
	for _, empty := range [][]Message{{{Role: "user"}}, {{Role: "user", Content: ""}}} {
		chat, _ := mustChat(t, empty, nil)
		if len(chat) != 0 {
			t.Fatalf("空 user 不该产生 chat 消息: %s", rawJSON(t, chat))
		}
		shape, _ := mustClaude(t, empty, nil)
		if len(shape.Messages) != 0 || shape.System != "" {
			t.Fatalf("空 user 不该产生 claude 消息: %s", rawJSON(t, shape))
		}
		resp, _ := mustResponses(t, empty, nil)
		if len(resp) != 0 {
			t.Fatalf("空 user 不该产生 responses item: %s", rawJSON(t, resp))
		}
	}
	chatTool, _ := mustChat(t, []Message{{Role: "tool", ToolCallID: "call_1"}}, nil)
	if got := rawJSON(t, chatTool); got != `[{"role":"tool","tool_call_id":"call_1","content":"(no output)"}]` {
		t.Fatalf("空 tool 结果该落 '(no output)': %s", got)
	}
}

// OpenAI → 中间表示 → 各线往返语义等价;arguments 的非规范化文本(1.0、键序)
// 全程不被改写。
func TestRoundTripIsStable(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "sys"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "calling", ToolCalls: []ToolCall{
			{ID: "call_1", Name: "read", Arguments: json.RawMessage(`{"path":"a.txt","n":1.0}`)},
		}},
		{Role: "tool", ToolCallID: "call_1", Content: "body"},
	}
	first, _ := mustChat(t, msgs, nil)
	second, _ := mustChat(t, chatBackToMessages(t, first), nil)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("round trip 不稳定:\nfirst: %s\nsecond: %s", rawJSON(t, first), rawJSON(t, second))
	}
	// claude 线的 input 也没经过重排:1.0 仍是 1.0(system 上提后 assistant 在 Messages[1])
	shape, _ := mustClaude(t, msgs, nil)
	if got := string(shape.Messages[1].Content[1].Input); got != `{"path":"a.txt","n":1.0}` {
		t.Fatalf("claude input 被改写: %s", got)
	}
}

// 未知 role 返回 error,不静默丢弃 —— 静默丢消息会让模型"忘事"而无人知晓。
// 有意偏离 JS(chat 线 default:break 丢弃、claude/responses 兜底成 user):
// 正常链路的 role 只有五种(engine.FromOpenAI 已归一),这是 tripwire。
func TestUnknownRoleIsRejected(t *testing.T) {
	_, _, err := ToChatMessages([]Message{{Role: "narrator", Content: "x"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "narrator") {
		t.Fatalf("chat 线该拒绝未知 role: %v", err)
	}
	if _, _, err := ToClaudeMessages([]Message{{Role: "narrator", Content: "x"}}, nil); err == nil {
		t.Fatal("claude 线该拒绝未知 role")
	}
	if _, _, err := ToResponseInput([]Message{{Role: "narrator", Content: "x"}}, nil); err == nil {
		t.Fatal("responses 线该拒绝未知 role")
	}
}

// ---- 对照 tests/messages.test.js 补漏 ----

func TestRepairToolPairingKeepsAnsweredPairs(t *testing.T) {
	out := RepairToolPairing([]Message{
		{Role: "user", Content: "read the file"},
		{Role: "assistant", Content: "sure", ToolCalls: []ToolCall{{ID: "call_1", Name: "read", Arguments: json.RawMessage(`{"path":"a.txt"}`)}}},
		{Role: "tool", ToolCallID: "call_1", Content: "contents"},
	})
	if len(out) != 3 {
		t.Fatalf("该保留 3 条: %s", rawJSON(t, out))
	}
	if out[1].Content != "sure" || len(out[1].ToolCalls) != 1 || out[1].ToolCalls[0].ID != "call_1" {
		t.Fatalf("有答案的调用被动过: %+v", out[1])
	}
	if out[2].ToolCallID != "call_1" {
		t.Fatalf("被答的结果被动过: %+v", out[2])
	}
}

func TestRepairToolPairingDropsUnlandedCalls(t *testing.T) {
	out := RepairToolPairing([]Message{
		{Role: "user", Content: "go"},
		{Role: "assistant", Content: "thinking", ToolCalls: []ToolCall{{ID: "call_1", Name: "read"}, {ID: "call_2", Name: "bash"}}},
		{Role: "tool", ToolCallID: "call_1", Content: "ok"},
	})
	var assistants, tools int
	for _, m := range out {
		if m.Role == "assistant" {
			assistants++
			if m.Content != "thinking" || len(m.ToolCalls) != 1 || m.ToolCalls[0].ID != "call_1" {
				t.Fatalf("该只留已落地的 call_1: %+v", m)
			}
		}
		if m.Role == "tool" {
			tools++
		}
	}
	if assistants != 1 || tools != 1 {
		t.Fatalf("条数不对: assistants=%d tools=%d", assistants, tools)
	}
}

func TestRepairToolPairingDropsOrphanResults(t *testing.T) {
	out := RepairToolPairing([]Message{
		{Role: "user", Content: "hi"},
		{Role: "tool", ToolCallID: "call_ghost", Content: "from a turn that never happened"},
		{Role: "tool", Content: "no id at all"},
	})
	if len(out) != 1 || out[0].Role != "user" {
		t.Fatalf("孤儿结果该被丢弃: %s", rawJSON(t, out))
	}
}

func TestRepairToolPairingDropsCalllessAssistantTurn(t *testing.T) {
	out := RepairToolPairing([]Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "call_1", Name: "read"}}},
		{Role: "assistant", Content: "just text"},
	})
	if len(out) != 2 || out[0].Role != "user" || out[1].Role != "assistant" {
		t.Fatalf("角色序列不对: %s", rawJSON(t, out))
	}
	if out[1].Content != "just text" || len(out[1].ToolCalls) != 0 {
		t.Fatalf("纯文本轮该原样留下: %+v", out[1])
	}
}

// 文本 + 未落地调用 → 只留文本,调用被摘掉(JS 修过的缺陷:推原消息会把
// tool_use-without-tool_result 的 400 形状原样回放出去)。
func TestRepairToolPairingKeepsTextStripsUnlandedCalls(t *testing.T) {
	out := RepairToolPairing([]Message{
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "let me check", ToolCalls: []ToolCall{{ID: "call_1", Name: "read"}}},
	})
	if len(out) != 2 || out[1].Content != "let me check" || len(out[1].ToolCalls) != 0 {
		t.Fatalf("该只留文本: %+v", out[1])
	}
}

func TestRepairToolPairingToleratesZeroValues(t *testing.T) {
	if len(RepairToolPairing(nil)) != 0 {
		t.Fatal("nil 输入该得空切片")
	}
	out := RepairToolPairing([]Message{{}, {Role: "tool"}, {Role: "assistant"}})
	if len(out) != 1 || !reflect.DeepEqual(out[0], Message{}) {
		t.Fatalf("零值消息该原样通过: %s", rawJSON(t, out))
	}
}

// 单块文本折叠成字符串,多块保持 parts 数组;空内容不产生空 user 消息。
func TestChatCollapsesSingleTextKeepsMultiParts(t *testing.T) {
	chat, _ := mustChat(t, []Message{{Role: "user", Content: "hi"}}, nil)
	if got := rawJSON(t, chat); got != `[{"role":"user","content":"hi"}]` {
		t.Fatalf("单文本该折叠成字符串: %s", got)
	}
	chat, _ = mustChat(t, []Message{{Role: "user", Blocks: []Block{{Type: "text", Text: "a"}, {Type: "text", Text: "b"}}}}, nil)
	if got := rawJSON(t, chat); got != `[{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]}]` {
		t.Fatalf("多文本该保持 parts 数组: %s", got)
	}
	for _, empty := range [][]Message{{{Role: "user", Content: ""}}, {{Role: "user", Blocks: []Block{}}}} {
		chat, _ := mustChat(t, empty, nil)
		if len(chat) != 0 {
			t.Fatalf("空内容不该产生消息: %s", rawJSON(t, chat))
		}
	}
}

// 坏 JSON 的 arguments 退化为 {}(JS 的 try/catch 同款),空内容的消息整条跳过。
func TestClaudeBadJSONArgsDegradeAndEmptySkipped(t *testing.T) {
	shape, _ := mustClaude(t, []Message{
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "call_1", Name: "read", Arguments: json.RawMessage("{not json")}}},
		{Role: "user", Blocks: []Block{}},
		{Role: "assistant", Content: ""},
	}, nil)
	if shape.System != "" {
		t.Fatalf("system 该缺省: %q", shape.System)
	}
	if len(shape.Messages) != 1 {
		t.Fatalf("空内容消息该整条跳过: %s", rawJSON(t, shape))
	}
	if got := string(shape.Messages[0].Content[0].Input); got != "{}" {
		t.Fatalf("坏 JSON 该退化为 {{}}: %s", got)
	}
}

// 只有受支持的 data URL 才成为 base64 图块,其余只留警告(JS 同款:
// webp 进,svg+xml 与 https 出)。
func TestClaudeImageDataURLGate(t *testing.T) {
	shape, warnings := mustClaude(t, []Message{
		{Role: "user", Blocks: []Block{{Type: "image", Attachment: &Attachment{URL: "data:image/webp;base64,ZZZZ"}}}},
		{Role: "user", Blocks: []Block{{Type: "image", Attachment: &Attachment{URL: "data:image/svg+xml;base64,ZZZZ"}}}},
		{Role: "user", Blocks: []Block{{Type: "image", Attachment: &Attachment{URL: "https://example.com/a.png"}}}},
	}, nil)
	if len(shape.Messages) != 1 {
		t.Fatalf("只有 webp 该出境: %s", rawJSON(t, shape))
	}
	if got := rawJSON(t, shape.Messages[0]); got != `{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/webp","data":"ZZZZ"}}]}` {
		t.Fatalf("base64 图块形状不对: %s", got)
	}
	if !reflect.DeepEqual(warnings, []string{"image-dropped", "image-dropped"}) {
		t.Fatalf("warnings 不对: %v", warnings)
	}
}

func TestResponsesItemsMapping(t *testing.T) {
	msgs := []Message{
		{Role: "system", Content: "be brief"},
		{Role: "user", Content: "hi"},
		{Role: "assistant", Content: "calling", ToolCalls: []ToolCall{{ID: "call_1", Name: "read", Arguments: json.RawMessage(`{"path":"a.txt"}`)}}},
		{Role: "tool", ToolCallID: "call_1", Content: "body"},
	}
	out, _ := mustResponses(t, msgs, nil)
	want := `[{"type":"message","role":"system","content":[{"type":"input_text","text":"be brief"}]}` +
		`,{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}` +
		`,{"type":"message","role":"assistant","content":[{"type":"output_text","text":"calling"}]}` +
		`,{"type":"function_call","call_id":"call_1","name":"read","arguments":"{\"path\":\"a.txt\"}"}` +
		`,{"type":"function_call_output","call_id":"call_1","output":"body"}]`
	if got := rawJSON(t, out); got != want {
		t.Fatalf("responses item 形状/键序不对:\n got: %s\nwant: %s", got, want)
	}
}

// 空内容不产生 item,图片走 input_image( Responses 线不做 data URL 过滤)。
func TestResponsesEmptyNoItemsAndInputImage(t *testing.T) {
	out, _ := mustResponses(t, []Message{
		{Role: "assistant"},
		{Role: "user"},
		{Role: "user", Blocks: []Block{
			{Type: "image", Attachment: &Attachment{URL: "data:image/png;base64,AAAA"}},
			{Type: "text", Text: "caption"},
		}},
	}, nil)
	want := `[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"},{"type":"input_text","text":"caption"}]}]`
	if got := rawJSON(t, out); got != want {
		t.Fatalf("responses 图片 item 不对:\n got: %s\nwant: %s", got, want)
	}
}

// 三种 wire 三种形状,name 去空白、空名跳过、超长截断、缺参数表补默认。
func TestToolDefsThreeStyles(t *testing.T) {
	long := strings.Repeat("x", upstream.MaxToolNameLen+20)
	params := map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}
	tools := []Tool{
		{Name: "  read_file  ", Description: "read a file", Params: params},
		{Name: "", Description: "no name"},
		{Name: "glob"},
		{Name: long},
	}
	claude := ToolDefs(tools, ToolStyleClaude)
	if len(claude) != 3 {
		t.Fatalf("空名该被跳过: %d", len(claude))
	}
	jsonEq(t, claude[0], `{"name":"read_file","description":"read a file","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}`)
	if got := rawJSON(t, claude[0]); !strings.HasPrefix(got, `{"name":"read_file","description":"read a file","input_schema":`) {
		t.Fatalf("claude 形状键序不对: %s", got)
	}
	jsonEq(t, claude[1], `{"name":"glob","description":"","input_schema":{"type":"object","properties":{}}}`)
	if len(claude[2].Name) != upstream.MaxToolNameLen {
		t.Fatalf("超长名该截到 %d: %d", upstream.MaxToolNameLen, len(claude[2].Name))
	}
	flat := ToolDefs(tools, ToolStyleFlat)
	jsonEq(t, flat[0], `{"type":"function","name":"read_file","description":"read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}`)
	chat := ToolDefs(tools, ToolStyleChat)
	jsonEq(t, chat[0], `{"type":"function","function":{"name":"read_file","description":"read a file","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}}`)
	if len(ToolDefs(nil, ToolStyleClaude)) != 0 {
		t.Fatal("nil 工具表该得空切片")
	}
	// 空非 nil 参数表 = JS 的 truthy {} → 原样保留,不补默认
	jsonEq(t, ToolDefs([]Tool{{Name: "x", Params: map[string]any{}}}, ToolStyleClaude)[0], `{"name":"x","description":"","input_schema":{}}`)
}

// 只有真正带图(且未 offload)的消息才要求视觉模型;裸字符串内容不炸。
func TestNeedsVision(t *testing.T) {
	if !NeedsVision([]Message{{Role: "user", Blocks: []Block{{Type: "image", Attachment: &Attachment{URL: "x"}}}}}) {
		t.Fatal("带图该要求视觉模型")
	}
	if NeedsVision([]Message{{Role: "user", Blocks: []Block{{Type: "image", Offloaded: true}}}}) {
		t.Fatal("offloaded 的块不该要求视觉模型")
	}
	if NeedsVision([]Message{{Role: "user", Content: "hi"}}) {
		t.Fatal("纯文本不该要求视觉模型")
	}
	if NeedsVision(nil) {
		t.Fatal("空输入不该要求视觉模型")
	}
}
