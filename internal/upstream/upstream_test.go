// SPDX-License-Identifier: GPL-3.0-or-later
package upstream

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// 计划任务 15 步骤 1 的 22 条测试逐条落实;另有三条对照 tests/upstream.test.js
// 的镜像测试(UserIDFor 每轮新鲜、FingerprintConfig 形状、DeclaredToolNames),
// 在各自函数头注明。

// 计划表第 1 条:1000 个 seed(含空串、"global"、带空格、中文、256 字节长串)
// 的输出全部匹配 SessionRe。
func TestSessionOutputMatchesSessionRe(t *testing.T) {
	seeds := []string{"", "global", " with spaces ", "中文会话", strings.Repeat("x", 256)}
	for i := 0; len(seeds) < 1000; i++ {
		seeds = append(seeds, fmt.Sprintf("conv-%d", i))
	}
	for _, seed := range seeds {
		out := SessionForConversation(seed)
		if !SessionRe.MatchString(out) {
			t.Fatalf("SessionForConversation(%q) = %q, 不匹配 SessionRe", seed, out)
		}
	}
}

// 计划表第 2 条:同一 seed 两次调用结果相同 —— 会话亲和跨轮次、跨重启
// (SessionForConversation 是 id 的纯函数;src/upstream.js:164-178:每请求一个
// 新会话实测换来 429 + 递增 retry-after)。
func TestSessionIsStableAcrossCalls(t *testing.T) {
	for _, seed := range []string{"conv-1", "", "中文会话"} {
		a := SessionForConversation(seed)
		b := SessionForConversation(seed)
		if a != b {
			t.Fatalf("seed %q: %q != %q", seed, a, b)
		}
	}
}

// 计划表第 3 条:两个 seed 的输出不同,否则所有会话挤进同一个亲和桶。
// 空串与 "global" 共享同一会话是设计(js :180 空串回落 'global'),不算冲突。
func TestDifferentSeedsDiffer(t *testing.T) {
	if SessionForConversation("conv-1") == SessionForConversation("conv-2") {
		t.Fatal("distinct seeds collapsed into one session")
	}
	if SessionForConversation("") != SessionForConversation("global") {
		t.Fatal("empty seed must fall back to the global bucket (js :180)")
	}
}

// 计划表第 4 条:1000 个 (session, seed) 组合的输出全部匹配 RequestRe。
func TestRequestIDOutputMatchesRequestRe(t *testing.T) {
	for i := 0; i < 1000; i++ {
		session := fmt.Sprintf("ses_%04d", i)
		seed := fmt.Sprintf("turn-%d", i%7)
		out := RequestIDFor(session, seed)
		if !RequestRe.MatchString(out) {
			t.Fatalf("RequestIDFor(%q,%q) = %q 不匹配 RequestRe", session, seed, out)
		}
	}
}

// 计划表第 5 条:空 turnSeed 走即时铸造回退(src/upstream.js:187),输出仍是
// 网关形状。
func TestRequestIDFallsBackWhenSeedIsEmpty(t *testing.T) {
	for i := 0; i < 8; i++ {
		out := RequestIDFor("s", "")
		if !RequestRe.MatchString(out) {
			t.Fatalf("fallback id %q 不匹配 RequestRe", out)
		}
	}
}

// 计划表第 6 条:同 (session, seed) 两次调用相同 —— 重试必须复用同一
// request id;不同 turn 必须不同。
func TestRequestIDIsStableForTheSameTurn(t *testing.T) {
	a := RequestIDFor("ses_abc", "turn-1")
	if a != RequestIDFor("ses_abc", "turn-1") {
		t.Fatalf("same turn produced two ids: %q vs %q", a, RequestIDFor("ses_abc", "turn-1"))
	}
	if a == RequestIDFor("ses_abc", "turn-2") {
		t.Fatal("different turns share one request id")
	}
}

// 计划表第 7 条:剥尾部 "(level)" 思考前缀(src/upstream.js:213-215)。
// 实测(node,2026-10-01):正则 \([^()]+\)\s*$ 只剥尾部的**简单**括号组 ——
// 嵌套串 "a(b(c))" 尾部不是简单组,整体不匹配,原样返回;多个尾组
// "a(b)(c)" 只剥最后一层。计划测试表里 "a(b(c)) 只去掉最后一层" 的表述
// 按 JS 实测行为修正(修正案 §6.5:JS 源码行为是最终事实)。
func TestBaseModelIDStripsTheThinkingSuffix(t *testing.T) {
	cases := []struct{ in, want string }{
		{"muse-spark-1.3 (high)", "muse-spark-1.3"},
		{"a(b(c))", "a(b(c))"}, // 嵌套括号:尾组不是简单组,不剥
		{"a(b)(c)", "a(b)"},    // 只去掉最后一层
		{"  muse (high)  ", "muse"},
		{"", ""},
	}
	for _, c := range cases {
		if got := BaseModelID(c.in); got != c.want {
			t.Fatalf("BaseModelID(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// 计划表第 8 条:muse-spark 家族(含思考后缀、路径前缀、下划线变体、大写)
// 走 responses;union-alpha 走 messages;其余一律 chat。muse 正则大小写
// 不敏感且只看路径最后一段;"spark" 后跟字母的不算(sparkly)。
func TestWireForEachModelFamily(t *testing.T) {
	cases := []struct {
		model string
		want  Wire
	}{
		{"muse-spark-1.2-contributor-free", WireResponses},
		{"muse-spark-1.3-contributor-free", WireResponses},
		{"muse-spark-1.3-contributor-free (high)", WireResponses},
		{"vendor/muse_spark-custom", WireResponses},
		{"Muse-Spark-9", WireResponses},
		{"union-alpha", WireMessages},
		{"union-alpha (thinking)", WireMessages},
		{"mimo-v2.6-flash-free", WireChat},
		{"muse-sparkly-free", WireChat},
		{"", WireChat},
	}
	for _, c := range cases {
		if got := WireFor(c.model); got != c.want {
			t.Fatalf("WireFor(%q) = %q, want %q", c.model, got, c.want)
		}
	}
}

// 计划表第 9 条:三条线路的路径与 src/upstream.js:233-235 逐字相同
// (tests/upstream.test.js:11-15)。
func TestEndpointForMatchesTheWire(t *testing.T) {
	if got := EndpointFor("muse-spark-1.3-contributor-free"); got != "/zen/v1/responses" {
		t.Fatalf("responses endpoint = %q", got)
	}
	if got := EndpointFor("union-alpha"); got != "/zen/v1/messages" {
		t.Fatalf("messages endpoint = %q", got)
	}
	if got := EndpointFor("mimo-v2.6-flash-free"); got != "/zen/v1/chat/completions" {
		t.Fatalf("chat endpoint = %q", got)
	}
}

// 计划表第 10 条:头集合逐字等于 src/upstream.js:277-286 —— 恰好 8 个键
// (7 个指纹头 + accept),stream 推导 accept,显式 Accept 优先。
// 先清空两个覆盖环境变量,保证测的是默认指纹而不是环境残留。
func TestGatewayHeadersCarryExactlyTheMeasuredSet(t *testing.T) {
	t.Setenv("OUR_FREE_MODEL_UA", "")
	t.Setenv("OUR_FREE_MODEL_HEADERS_JSON", "")
	h := GatewayHeaders(HeaderOptions{Session: "ses_abc", RequestID: "msg_def", Stream: true})
	want := map[string]string{
		"content-type":       "application/json",
		"authorization":      "Bearer public",
		"user-agent":         "opencode/1.18.31",
		"x-opencode-client":  "desktop",
		"x-opencode-session": "ses_abc",
		"x-opencode-request": "msg_def",
		"x-opencode-project": "global",
		"accept":             "text/event-stream",
	}
	if !reflect.DeepEqual(h, want) {
		t.Fatalf("streaming headers = %v, want %v", h, want)
	}
	h = GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: false})
	if h["accept"] != "*/*" {
		t.Fatalf("non-stream accept = %q", h["accept"])
	}
	h = GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: true, Accept: "application/json"})
	if h["accept"] != "application/json" {
		t.Fatalf("explicit accept = %q", h["accept"])
	}
}

// 计划表第 11 条:ParentSession 为空时不得出现 x-parent-session-id;非空时
// 带上该值(js :290)。
func TestParentSessionHeaderOnlyWhenSet(t *testing.T) {
	t.Setenv("OUR_FREE_MODEL_HEADERS_JSON", "")
	h := GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: false})
	if _, ok := h["x-parent-session-id"]; ok {
		t.Fatal("x-parent-session-id must be absent when ParentSession is empty")
	}
	h = GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: false, ParentSession: "ses_parent"})
	if h["x-parent-session-id"] != "ses_parent" {
		t.Fatalf("parent header = %q", h["x-parent-session-id"])
	}
}

// 计划表第 12 条(Go 特有):OUR_FREE_MODEL_HEADERS_JSON 畸形时返回默认头、
// Overridden 为空,不 panic、不返回 error(src/upstream.js:70-72、:82 的
// 中文注释:覆盖项写错时退回默认值,而不是拒绝启动)。
func TestHeaderOverridesIgnoreMalformedJSON(t *testing.T) {
	t.Setenv("OUR_FREE_MODEL_UA", "")
	t.Setenv("OUR_FREE_MODEL_HEADERS_JSON", "{oops")
	h := GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: false})
	if len(h) != 8 {
		t.Fatalf("malformed override changed the header set: %d keys: %v", len(h), h)
	}
	if cfg := FingerprintConfig(); len(cfg.Overridden) != 0 {
		t.Fatalf("Overridden = %v, want empty", cfg.Overridden)
	}
}

// 计划表第 13 条:覆盖键逐字 toLowerCase(js :87)后合并;值为 null 的键与
// 空键跳过(:88)。
func TestHeaderOverrideKeysAreLowercased(t *testing.T) {
	t.Setenv("OUR_FREE_MODEL_UA", "")
	t.Setenv("OUR_FREE_MODEL_HEADERS_JSON", `{"User-Agent":"x","X-Trace":null,"":"skip"}`)
	h := GatewayHeaders(HeaderOptions{Session: "s", RequestID: "r", Stream: false})
	if h["user-agent"] != "x" {
		t.Fatalf("user-agent = %q, want %q", h["user-agent"], "x")
	}
	if _, ok := h["User-Agent"]; ok {
		t.Fatal("override key was not lowercased")
	}
	if _, ok := h["x-trace"]; ok {
		t.Fatal("null-valued override must be skipped")
	}
	if _, ok := h[""]; ok {
		t.Fatal("empty override key must be skipped")
	}
	if len(h) != 8 {
		t.Fatalf("override added unexpected keys: %v", h)
	}
}

// 计划表第 14 条:缺 bash/read 时补两个"自我禁用诱饵",description 逐字
// 抄 src/upstream.js:351-352,参数是空 object 形状。
func TestApplyFingerprintAppendsTheDecoy(t *testing.T) {
	body := map[string]any{"model": "m", "messages": []any{}, "tools": []any{}}
	renames := ApplyFingerprint(body, false)
	tools, _ := body["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools = %d entries, want 2 decoys: %v", len(tools), tools)
	}
	wantDesc := "This tool is currently unavailable and must not be used."
	wantParams := map[string]any{"type": "object", "properties": map[string]any{}}
	for i, name := range RequiredTools {
		tm, _ := tools[i].(map[string]any)
		fn, _ := tm["function"].(map[string]any)
		if fn["name"] != name {
			t.Fatalf("decoy %d name = %v, want %q", i, fn["name"], name)
		}
		if fn["description"] != wantDesc {
			t.Fatalf("decoy description = %v, want verbatim %q", fn["description"], wantDesc)
		}
		if !reflect.DeepEqual(fn["parameters"], wantParams) {
			t.Fatalf("decoy parameters = %v, want %v", fn["parameters"], wantParams)
		}
	}
	if len(renames) != 0 {
		t.Fatalf("renames = %v, want empty", renames)
	}
}

// 计划表第 15 条:大小写归一(js :339-345)—— 调用方发 Bash,线上是小写
// bash;renames 记录「线上名 → 调用方原名」,RestoreToolName 恢复拼写
// (:364-367);且调用方的原 tool map 不得被就地改写。
func TestApplyFingerprintCanonicalisesCase(t *testing.T) {
	body := map[string]any{"tools": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "Bash", "parameters": map[string]any{}}},
	}}
	renames := ApplyFingerprint(body, false)
	tools, _ := body["tools"].([]any)
	tm, _ := tools[0].(map[string]any)
	fn, _ := tm["function"].(map[string]any)
	if fn["name"] != "bash" {
		t.Fatalf("sent name = %v, want bash", fn["name"])
	}
	if got := RestoreToolName("bash", renames); got != "Bash" {
		t.Fatalf("RestoreToolName(bash) = %q, want Bash", got)
	}
	if got := RestoreToolName("glob", renames); got != "glob" {
		t.Fatalf("RestoreToolName(glob) = %q, want glob", got)
	}
	if got := RestoreToolName("bash", nil); got != "bash" {
		t.Fatalf("RestoreToolName with nil map = %q, want bash", got)
	}
	orig := map[string]any{"type": "function", "function": map[string]any{"name": "Bash"}}
	b2 := map[string]any{"tools": []any{orig}}
	ApplyFingerprint(b2, false)
	if orig["function"].(map[string]any)["name"] != "Bash" {
		t.Fatal("caller's tool map was mutated in place")
	}
}

// 计划表第 16 条:重名去重(js :337 的 seen 集合)—— bash 与 Bash 同时声明,
// 线上只有一个 bash(第一个保留,变体整条丢弃),renames 为空。
func TestApplyFingerprintDropsDuplicates(t *testing.T) {
	body := map[string]any{"tools": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "bash"}},
		map[string]any{"type": "function", "function": map[string]any{"name": "Bash"}},
	}}
	renames := ApplyFingerprint(body, false)
	if got := toolNamesOf(body); !reflect.DeepEqual(got, []string{"bash", "read"}) {
		t.Fatalf("names = %v, want [bash read](Bash 整条丢弃 + read 诱饵)", got)
	}
	if len(renames) != 0 {
		t.Fatalf("renames = %v, want empty", renames)
	}
}

// 计划表第 17 条:tool_choice 语义(js :356-359)—— 已有值一律不覆盖;
// flat 补 "auto";非 flat 仅在调用方零工具时补 "none"。
func TestApplyFingerprintToolChoice(t *testing.T) {
	chatEmpty := map[string]any{"tools": []any{}}
	ApplyFingerprint(chatEmpty, false)
	if chatEmpty["tool_choice"] != "none" {
		t.Fatalf("chat without tools: tool_choice = %v, want none", chatEmpty["tool_choice"])
	}
	chatWithTools := map[string]any{"tools": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "glob"}},
	}}
	ApplyFingerprint(chatWithTools, false)
	if _, ok := chatWithTools["tool_choice"]; ok {
		t.Fatal("chat with caller tools must not set tool_choice")
	}
	flatEmpty := map[string]any{"tools": []any{}}
	ApplyFingerprint(flatEmpty, true)
	if flatEmpty["tool_choice"] != "auto" {
		t.Fatalf("flat: tool_choice = %v, want auto", flatEmpty["tool_choice"])
	}
	kept := map[string]any{"tools": []any{}, "tool_choice": "required"}
	ApplyFingerprint(kept, true)
	if kept["tool_choice"] != "required" {
		t.Fatalf("existing tool_choice was overwritten: %v", kept["tool_choice"])
	}
}

// 计划表第 18 条:调用方声明的 glob/grep 原样保留(js :313-315、
// tests/upstream.test.js:54-57:额外名字无害,改写它们什么也买不到);
// 诱饵只补缺失的两个名字,且排在调用方工具之后。
func TestApplyFingerprintPassesOtherToolsThrough(t *testing.T) {
	body := map[string]any{"tools": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "glob", "parameters": map[string]any{}}},
		map[string]any{"type": "function", "function": map[string]any{"name": "grep", "parameters": map[string]any{}}},
	}}
	ApplyFingerprint(body, false)
	if got := toolNamesOf(body); !reflect.DeepEqual(got, []string{"glob", "grep", "bash", "read"}) {
		t.Fatalf("names = %v", got)
	}
}

// 计划表第 19 条:仅 stream == true 的 chat 体注入 stream_options.include_usage
// (src/upstream.js:376-387,移植自 opencode2api 的 ensureAnonymousChatUsage);
// 幂等;非流式不动(forward 负责折叠)。
func TestEnsureChatUsageOnlyTouchesStreamingBodies(t *testing.T) {
	a := map[string]any{"model": "m", "stream": true}
	if EnsureChatUsage(a) != true {
		t.Fatal("streaming body must gain include_usage")
	}
	if !reflect.DeepEqual(a["stream_options"], map[string]any{"include_usage": true}) {
		t.Fatalf("stream_options = %v", a["stream_options"])
	}
	if EnsureChatUsage(a) != false {
		t.Fatal("second call must be a no-op (idempotent)")
	}
	b := map[string]any{"model": "m", "stream": false}
	if EnsureChatUsage(b) != false {
		t.Fatal("non-streaming body must not be touched")
	}
	if _, ok := b["stream_options"]; ok {
		t.Fatal("non-streaming body gained stream_options")
	}
	// 已有 stream_options 但缺 include_usage → 就地补,不整体替换
	c := map[string]any{"model": "m", "stream": true, "stream_options": map[string]any{"foo": 1}}
	if EnsureChatUsage(c) != true {
		t.Fatal("existing stream_options must be patched in place")
	}
	if c["stream_options"].(map[string]any)["include_usage"] != true {
		t.Fatal("include_usage not forced to true")
	}
	if c["stream_options"].(map[string]any)["foo"] != 1 {
		t.Fatal("existing stream_options keys were lost")
	}
}

// 计划表第 20 条:「reasoning 提及」与「gone 标记」必须同时命中
// (src/upstream.js:395-400);只含 "reasoning item" 而无 gone 标记的
// 通用校验错不误杀。
func TestIsStaleReasoningReferenceNeedsBothSignals(t *testing.T) {
	if !IsStaleReasoningReference("Referenced reasoning item 'rs_abc' was not found or has expired") {
		t.Fatal("measured stale-reasoning body must match")
	}
	if !IsStaleReasoningReference("reasoning reference does not exist") {
		t.Fatal("gone marker 'does not exist' must match")
	}
	if !IsStaleReasoningReference("Reasoning Item is no longer available") {
		t.Fatal("case-insensitive phrase + 'no longer' must match")
	}
	if IsStaleReasoningReference("reasoning item is invalid") {
		t.Fatal("phrase without a gone marker must NOT match")
	}
	if IsStaleReasoningReference("unknown reasoning field") {
		t.Fatal("generic reasoning error must NOT match")
	}
	if IsStaleReasoningReference("boom") {
		t.Fatal("unrelated error must NOT match")
	}
}

// 计划表第 21 条:删 previous_response_id + input[] 里 type=="reasoning" 项,
// 返回是否有改动(src/upstream.js:408-423);无改动必须报告 false 且不动原状。
func TestStripStaleReasoningInputsReportsChange(t *testing.T) {
	payload := map[string]any{
		"model":                "m",
		"previous_response_id": "resp_x",
		"input": []any{
			map[string]any{"type": "reasoning", "id": "rs_1"},
			map[string]any{"type": "message", "role": "user", "content": "hi"},
		},
	}
	if StripStaleReasoningInputs(payload) != true {
		t.Fatal("strip must report the change")
	}
	want := map[string]any{
		"model": "m",
		"input": []any{map[string]any{"type": "message", "role": "user", "content": "hi"}},
	}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload = %v, want %v", payload, want)
	}
	if StripStaleReasoningInputs(map[string]any{"model": "m", "input": []any{}}) != false {
		t.Fatal("empty input is not a change")
	}
	if StripStaleReasoningInputs(map[string]any{"model": "m"}) != false {
		t.Fatal("payload without the fields is not a change")
	}
	untouched := map[string]any{"input": []any{map[string]any{"type": "message"}}}
	if StripStaleReasoningInputs(untouched) != false {
		t.Fatal("input without reasoning items is not a change")
	}
	if len(untouched["input"].([]any)) != 1 {
		t.Fatal("input was modified although nothing matched")
	}
}

// 计划表第 22 条:300 字符截到 256(MAX_SESSION_LENGTH,js :56、:435-439);
// 顺带钉住首尾去空白与 rune 计数(JS 按 UTF-16 码元,Go 按 rune,BMP 内一致,
// 且不会在多字节序列中间切出半个字符)。
func TestTruncateSessionBoundsTheLength(t *testing.T) {
	if got := TruncateSession(strings.Repeat("a", 300)); len(got) != 256 {
		t.Fatalf("truncated length = %d, want 256", len(got))
	}
	if got := TruncateSession("  ses_x  "); got != "ses_x" {
		t.Fatalf("trimmed = %q, want ses_x", got)
	}
	if got := TruncateSession(strings.Repeat("会", 300)); got != strings.Repeat("会", 256) {
		t.Fatalf("rune-bound truncation broken: %d runes", len([]rune(got)))
	}
}

// 镜像 tests/upstream.test.js:38-43:x-opencode-request 每轮新 id —— 稳定的
// per-session id 实测被闸门拒绝(src/upstream.js:193-200)。
func TestUserIDForIsFreshPerTurn(t *testing.T) {
	a := UserIDFor()
	b := UserIDFor()
	if a == b {
		t.Fatal("per-turn id repeated")
	}
	if !RequestRe.MatchString(a) || !RequestRe.MatchString(b) {
		t.Fatalf("ids %q / %q do not match RequestRe", a, b)
	}
}

// 镜像 tests/upstream.test.js:69-76:诊断配置如实反映生效指纹。
func TestFingerprintConfigReportsTheEffectiveFingerprint(t *testing.T) {
	t.Setenv("OUR_FREE_MODEL_UA", "")
	t.Setenv("OUR_FREE_MODEL_HEADERS_JSON", "")
	cfg := FingerprintConfig()
	if cfg.UserAgent != "opencode/1.18.31" {
		t.Fatalf("cfg.UserAgent = %q", cfg.UserAgent)
	}
	if cfg.ClientKind != "desktop" {
		t.Fatalf("cfg.ClientKind = %q", cfg.ClientKind)
	}
	if !reflect.DeepEqual(cfg.Tools, []string{"bash", "read"}) {
		t.Fatalf("cfg.Tools = %v", cfg.Tools)
	}
	if cfg.Headers == nil || len(cfg.Headers) != 0 {
		t.Fatalf("cfg.Headers = %v, want an empty (non-nil) map", cfg.Headers)
	}
	if len(cfg.Overridden) != 0 {
		t.Fatalf("cfg.Overridden = %v, want empty", cfg.Overridden)
	}
}

// 补一条覆盖:DeclaredToolNames 是响应侧把诱饵从真实工具调用里认出来的依据
// (src/upstream.js:425-433)—— 按声明顺序去重返回,无名工具跳过。
func TestDeclaredToolNamesSplitsDecoysFromRealCalls(t *testing.T) {
	body := map[string]any{"tools": []any{
		map[string]any{"type": "function", "function": map[string]any{"name": "bash"}},
		map[string]any{"type": "function", "function": map[string]any{"name": "bash"}},
		map[string]any{"type": "function"},
		map[string]any{"type": "function", "name": " grep "},
	}}
	if got := DeclaredToolNames(body); !reflect.DeepEqual(got, []string{"bash", "grep"}) {
		t.Fatalf("DeclaredToolNames = %v, want [bash grep]", got)
	}
	if got := DeclaredToolNames(map[string]any{}); got != nil {
		t.Fatalf("no tools → nil, got %v", got)
	}
}

// toolNamesOf 取非 flat 体里每个工具的 function.name,测试辅助。
func toolNamesOf(body map[string]any) []string {
	tools, _ := body["tools"].([]any)
	var names []string
	for _, tool := range tools {
		tm, _ := tool.(map[string]any)
		fn, _ := tm["function"].(map[string]any)
		name, _ := fn["name"].(string)
		names = append(names, name)
	}
	return names
}
