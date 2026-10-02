// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/effort"
	"freerouter/internal/errors"
	"freerouter/internal/httpclient"
	"freerouter/internal/messages"
	"freerouter/internal/stream"
	"freerouter/internal/upstream"
)

// captured 是伪造上游收到的每一个请求。
type captured struct {
	path    string
	headers http.Header
	body    []byte
}

// fakeUpstream 按脚本回应:script 收到请求序号(0 起),返回 (status,
// content-type, body)。大多数测试用同一段 200/SSE 剧本;按序号分支即可表达
// 「第一次 400、重放成功」这类时序。
type fakeUpstream struct {
	srv *httptest.Server
	mu  sync.Mutex
	got []captured
}

func (f *fakeUpstream) requests() []captured {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]captured, len(f.got))
	copy(out, f.got)
	return out
}

func newFakeUpstream(t *testing.T, script func(n int) (int, string, string)) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		n := len(f.got)
		f.got = append(f.got, captured{path: r.URL.Path, headers: r.Header.Clone(), body: raw})
		f.mu.Unlock()
		status, ctype, body := script(n)
		w.Header().Set("Content-Type", ctype)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// entryFor 与 src/catalog.js 的三个夹具模型同形状(测试只关心 maxOutput 与
// 推理支持位)。
func entryFor(model string) effort.Entry {
	switch model {
	case "union-alpha":
		return effort.Entry{ID: model, ContextWindow: 262144, MaxOutput: 131072}
	case "muse-spark-1.3-contributor-free":
		return effort.Entry{ID: model, ContextWindow: 1048576, MaxOutput: 131072, SupportsReasoning: true}
	default: // big-pickle:chat 线
		return effort.Entry{ID: model, ContextWindow: 200000, MaxOutput: 32000, SupportsReasoning: true}
	}
}

func newAdapter(base string, client *http.Client, model string) *Adapter {
	return NewAdapter(Deps{
		Client:    client,
		Base:      base,
		Model:     model,
		Effort:    "balanced",
		Entry:     entryFor(model),
		SessionID: "conversation-7",
		Wire:      upstream.WireFor(model),
		NodeKey:   "n1",
	})
}

func sseBody(lines ...string) string {
	var sb strings.Builder
	for _, line := range lines {
		sb.WriteString("data: " + line + "\n\n")
	}
	return sb.String()
}

func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, raw)
	}
	return m
}

func toolNamesOf(body map[string]any) []string {
	var names []string
	tools, _ := body["tools"].([]any)
	for _, tool := range tools {
		tm, ok := tool.(map[string]any)
		if !ok {
			continue
		}
		if name, ok := tm["name"].(string); ok {
			names = append(names, name)
			continue
		}
		if fn, ok := tm["function"].(map[string]any); ok {
			name, _ := fn["name"].(string)
			names = append(names, name)
		}
	}
	return names
}

func collect(chunks *strings.Builder) func(string) error {
	return func(s string) error {
		chunks.WriteString(s)
		return nil
	}
}

var okChatSSE = sseBody(
	`{"choices":[{"delta":{"role":"assistant"}}]}`,
	`{"choices":[{"delta":{"content":"Hel"}}]}`,
	`{"choices":[{"delta":{"content":"lo"}}]}`,
	// include_usage 的尾包是**顶层** usage + 空 choices(OpenAI 规范,
	// js feedChat 读的就是 payload.usage,不是 choices[].usage)
	`{"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":2}}`,
	`[DONE]`,
)

// TestChatWireCollapsesStreamIntoOneChunk:客户端要的是非流式(Request.Stream
// 为 false),上游仍然被强制成流式 —— build 恒写 stream:true 并由
// ensureChatUsage 强制 usage 尾包。adapter 只负责把增量按到达顺序交给
// onChunk;拼不拼成一条是转发层的职责,所以这里断言的是「收到完整文本与
// usage」而不是回调次数。
func TestChatWireCollapsesStreamIntoOneChunk(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", okChatSSE
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var chunks strings.Builder
	usage, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, collect(&chunks))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if chunks.String() != "Hello" {
		t.Fatalf("onChunk received %q, want %q", chunks.String(), "Hello")
	}
	if !usage.HasUsage || usage.In != 10 || usage.Out != 2 {
		t.Fatalf("usage = %+v, want HasUsage with in=10 out=2", usage)
	}
	// 「上游强制 SSE」的机制本身:payload 恒 stream:true + include_usage
	sent := decodeBody(t, f.requests()[0].body)
	if sent["stream"] != true {
		t.Fatalf("payload stream = %v, want true(上游只服务流式请求)", sent["stream"])
	}
	opts, _ := sent["stream_options"].(map[string]any)
	if opts == nil || opts["include_usage"] != true {
		t.Fatalf("payload stream_options = %v, want include_usage:true(折叠回非流式后 usage 才不丢)", sent["stream_options"])
	}
}

func TestResponsesWireUsesTheRightEndpoint(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "application/json",
			`{"id":"resp_1","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":3,"output_tokens":1}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "muse-spark-1.3-contributor-free")

	var chunks strings.Builder
	usage, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&chunks))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got := f.requests()
	if len(got) != 1 || got[0].path != "/zen/v1/responses" {
		t.Fatalf("path = %v, want /zen/v1/responses", got[0].path)
	}
	sent := decodeBody(t, got[0].body)
	if _, has := sent["messages"]; has {
		t.Fatalf("responses wire must not carry a messages key: %v", sent)
	}
	if _, has := sent["input"]; !has {
		t.Fatalf("responses wire carries input, got %v", sent)
	}
	if !usage.HasUsage {
		t.Fatalf("usage lost on the responses wire: %+v", usage)
	}
}

func TestMessagesWireUsesTheRightEndpoint(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			`{"type":"message_start","message":{"usage":{"input_tokens":12,"output_tokens":1,"cache_read_input_tokens":5}}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hola"}}`,
			`{"type":"message_delta","usage":{"output_tokens":9}}`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "union-alpha")

	var chunks strings.Builder
	usage, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&chunks))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	got := f.requests()
	if got[0].path != "/zen/v1/messages" {
		t.Fatalf("path = %s, want /zen/v1/messages", got[0].path)
	}
	if v := got[0].headers.Get("anthropic-version"); v != upstream.AnthropicAPIVersion {
		t.Fatalf("anthropic-version = %q, want %q", v, upstream.AnthropicAPIVersion)
	}
	if chunks.String() != "hola" {
		t.Fatalf("text = %q, want %q", chunks.String(), "hola")
	}
	// message_start 给输入侧(12,其中 cacheRead 5),message_delta 只带输出 9:
	// 合并规则要求 9 并入既有值,而不是把输入侧清零(js stream.js:284-293)。
	// 输入侧按 disjoint-count 规则记**未缓存**部分:12-5=7
	// (js stream.js:122;对照 tests/stream.test.js:174-184 的 50/10 → 40)。
	if usage.In != 7 || usage.CacheRead != 5 || usage.Out != 9 || !usage.HasUsage {
		t.Fatalf("usage = %+v, want in=7(12-5) cacheRead=5 out=9", usage)
	}
}

func TestHeadersCarryTheFingerprint(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", okChatSSE
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	h := f.requests()[0].headers
	if !upstream.SessionRe.MatchString(h.Get("x-opencode-session")) {
		t.Fatalf("x-opencode-session = %q, want a %v match", h.Get("x-opencode-session"), upstream.SessionRe)
	}
	if h.Get("x-opencode-client") != "desktop" {
		t.Fatalf("x-opencode-client = %q, want desktop", h.Get("x-opencode-client"))
	}
	if h.Get("authorization") != "Bearer public" {
		t.Fatalf("authorization = %q, want the pooled public credential", h.Get("authorization"))
	}
	if !strings.Contains(h.Get("user-agent"), "opencode/") {
		t.Fatalf("user-agent = %q, want the opencode fingerprint token", h.Get("user-agent"))
	}
	if !strings.Contains(h.Get("accept"), "text/event-stream") {
		t.Fatalf("accept = %q, want text/event-stream for a streaming turn", h.Get("accept"))
	}
}

func TestRequestIDIsReusedAcrossRetries(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", okChatSSE
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	req := Request{Messages: []messages.Message{{Role: "user", Content: "hi"}}, Stream: true, TurnSeed: "turn-42"}
	if _, err := a.Complete(context.Background(), req, collect(&strings.Builder{})); err != nil {
		t.Fatalf("first Complete: %v", err)
	}
	if _, err := a.Complete(context.Background(), req, collect(&strings.Builder{})); err != nil {
		t.Fatalf("second Complete: %v", err)
	}
	got := f.requests()
	first, second := got[0].headers.Get("x-opencode-request"), got[1].headers.Get("x-opencode-request")
	if first == "" || first != second {
		t.Fatalf("x-opencode-request = %q then %q, want the same stable id per turn", first, second)
	}
	if !upstream.RequestRe.MatchString(first) {
		t.Fatalf("x-opencode-request = %q, want a %v match", first, upstream.RequestRe)
	}
	req.TurnSeed = "turn-43"
	if _, err := a.Complete(context.Background(), req, collect(&strings.Builder{})); err != nil {
		t.Fatalf("third Complete: %v", err)
	}
	if third := f.requests()[2].headers.Get("x-opencode-request"); third == first {
		t.Fatalf("a different turn seed must mint a different request id, got %q twice", third)
	}
}

// TestCallbackErrorStopsTheTurn:onChunk 返回 error → 停读、关 body、原样返回
// 该 error。「上游连接被关闭」的观察点:handler 在首个事件后阻塞,客户端断开
// 会让服务端请求上下文取消。
func TestCallbackErrorStopsTheTurn(t *testing.T) {
	firstSeen := make(chan struct{})
	serverDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `data: {"choices":[{"delta":{"content":"a"}}]}`+"\n\n")
		w.(http.Flusher).Flush()
		close(firstSeen)
		<-r.Context().Done()
		close(serverDone)
	}))
	t.Cleanup(srv.Close)
	a := newAdapter(srv.URL, srv.Client(), "big-pickle")

	errBoom := errors.Failure{Code: check.CodeAborted, Message: "client went away"}
	calls := 0
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, func(string) error {
		calls++
		return errBoom
	})
	if err != errBoom {
		t.Fatalf("Complete error = %v, want the callback error verbatim", err)
	}
	if calls != 1 {
		t.Fatalf("onChunk called %d times, want 1 (turn stops at the first refusal)", calls)
	}
	select {
	case <-firstSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream never delivered the first event")
	}
	select {
	case <-serverDone:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream connection was not closed after the callback error")
	}
}

// TestOnFirstTokenPanicIsSwallowed:路由信号绝不能打断一轮请求(src/adapter.js:65)。
func TestOnFirstTokenPanicIsSwallowed(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", okChatSSE
	})
	a := NewAdapter(Deps{
		Client: f.srv.Client(), Base: f.srv.URL, Model: "big-pickle",
		Entry: entryFor("big-pickle"), SessionID: "conversation-7",
		Wire: upstream.WireFor("big-pickle"), NodeKey: "n1",
		OnFirstToken: func(string, int64) { panic("routing signal") },
	})

	var chunks strings.Builder
	usage, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&chunks))
	if err != nil {
		t.Fatalf("Complete: %v (an OnFirstToken panic must never break the turn)", err)
	}
	if chunks.String() != "Hello" || !usage.HasUsage {
		t.Fatalf("turn did not complete normally: text=%q usage=%+v", chunks.String(), usage)
	}
}

// TestStaleReasoningIsStrippedAndReplayed:第一次 400 且 body 命中
// IsStaleReasoningReference → 剥掉 previous_response_id 与 reasoning 输入项后
// 重放一次,第二次成功。
//
// 走 exchange 而不是 Complete:build 的三条投影本来就**不发** reasoning 项与
// previous_response_id(它们在中间表示里不存在,见 messages 包注释),注入
// 只能发生在 body 组装之后 —— 这正是 js http.js 的调用位(它在 postStreamed
// 里剥,不在 buildPayload 里)。
func TestStaleReasoningIsStrippedAndReplayed(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		if n == 0 {
			return 400, "application/json", `{"error":{"message":"Reasoning item rs_9 not found"}}`
		}
		return 200, "application/json",
			`{"id":"resp_2","output":[{"type":"message","content":[{"type":"output_text","text":"fresh"}]}],"usage":{"input_tokens":4,"output_tokens":2}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "muse-spark-1.3-contributor-free")

	req := Request{Messages: []messages.Message{{Role: "user", Content: "hi"}}, Stream: true}
	body, err := a.build(req)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	renames := upstream.ApplyFingerprint(body, a.deps.Wire == upstream.WireResponses)
	body["previous_response_id"] = "resp_stale"
	input, _ := body["input"].([]any)
	body["input"] = append(input, map[string]any{"type": "reasoning", "summary": []any{}})
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tu := &turn{req: req, body: body, payload: payload, renames: renames, t0: time.Now()}
	var acc stream.Usage
	first := false
	usage, err := a.exchange(context.Background(), tu, collect(&strings.Builder{}), &acc, &first)
	if err != nil {
		t.Fatalf("exchange: %v (a stale reasoning reference must be stripped and replayed once)", err)
	}
	if !usage.HasUsage {
		t.Fatalf("usage lost across the replay: %+v", usage)
	}
	got := f.requests()
	if len(got) != 2 {
		t.Fatalf("sent %d requests, want exactly 2 (original + one replay)", len(got))
	}
	replayed := decodeBody(t, got[1].body)
	if _, has := replayed["previous_response_id"]; has {
		t.Fatal("replayed payload still carries previous_response_id")
	}
	items, _ := replayed["input"].([]any)
	for _, item := range items {
		if tm, ok := item.(map[string]any); ok && tm["type"] == "reasoning" {
			t.Fatal("replayed payload still carries a reasoning input item")
		}
	}
}

// TestStaleReasoningIsNotRetriedTwice:普通 400 不命中过期推理引用 → 只发一次。
func TestStaleReasoningIsNotRetriedTwice(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 400, "application/json", `{"error":{"message":"invalid_request: messages is malformed"}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if err == nil {
		t.Fatal("want a failure for a genuine 400")
	}
	if errors.CodeOf(err) == "" {
		t.Fatalf("error %v is not classified", err)
	}
	if got := len(f.requests()); got != 1 {
		t.Fatalf("sent %d requests, want 1 (a genuine 400 is never replayed)", got)
	}
}

func TestFailureIsClassifiedNotLeakedRaw(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 500, "application/json", `{"error":{"message":"upstream exploded"}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if err == nil {
		t.Fatal("want a failure")
	}
	if errors.CodeOf(err) != check.CodeServer {
		t.Fatalf("code = %q, want %q", errors.CodeOf(err), check.CodeServer)
	}
	failure, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("error %T is not an errors.Failure: %v", err, err)
	}
	if failure.Status != 500 || failure.Message != "upstream exploded" {
		t.Fatalf("failure = %+v, want the classified envelope", failure)
	}
}

// TestUsageSurvivesANonStreamingReply:上游不回 SSE 而回整包 JSON 时,usage
// 仍要从非流式形状解析出来(字段名按 JS readStream 的非流式分支:
// prompt_tokens / completion_tokens),全文一次性交给 onChunk。
func TestUsageSurvivesANonStreamingReply(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "application/json",
			`{"choices":[{"message":{"content":"plain answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var chunks strings.Builder
	calls := 0
	usage, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, func(s string) error {
		calls++
		chunks.WriteString(s)
		return nil
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if chunks.String() != "plain answer" {
		t.Fatalf("text = %q, want the full reply body", chunks.String())
	}
	if calls != 1 {
		t.Fatalf("onChunk called %d times, want exactly once for a non-streaming reply", calls)
	}
	if !usage.HasUsage || usage.In != 7 || usage.Out != 3 {
		t.Fatalf("usage = %+v, want HasUsage in=7 out=3", usage)
	}
}

// TestInputTokensAreUncachedAcrossEveryWire 把 harness 的 disjoint-count 规则
// 钉在三条线的入口上:上游给的是**毛**输入总数(prompt_tokens / input_tokens
// 已含缓存命中),而 harness 的 inputTokens 只记未缓存部分
// (js stream.js:11-13 的模块注释、mapUsage:122 的 Math.max(0, prompt - cached))。
//
// 夹具与 tests/stream.test.js 同形,期望值逐字取自那份 JS 测试:
// chat 100/40 → 60(:51,63)、responses 10/2 → 8(:213,222)、
// messages 50/10 → 40(:174,184)。这三条曾经都记成毛值,阶段 5 的 A 组
// 逐字节差分会在第一帧就挂。
func TestInputTokensAreUncachedAcrossEveryWire(t *testing.T) {
	cases := []struct {
		name string
		body string
		want int64
	}{
		{"chat", `{"choices":[],"usage":{"prompt_tokens":100,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":40}}}`, 60},
		{"responses", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":10,"output_tokens":4,"input_tokens_details":{"cached_tokens":2}}}}`, 8},
		{"messages", `{"type":"message_start","message":{"usage":{"input_tokens":50,"output_tokens":1,"cache_read_input_tokens":10}}}`, 40},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeUpstream(t, func(n int) (int, string, string) {
				return 200, "text/event-stream", sseBody(tc.body)
			})
			model := "big-pickle"
			if tc.name == "responses" {
				model = "muse-spark-1.3-contributor-free"
			} else if tc.name == "messages" {
				model = "union-alpha"
			}
			a := newAdapter(f.srv.URL, f.srv.Client(), model)
			usage, err := a.Complete(context.Background(), Request{
				Messages: []messages.Message{{Role: "user", Content: "hi"}},
				Stream:   true,
			}, collect(&strings.Builder{}))
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if !usage.HasUsage || usage.In != tc.want {
				t.Fatalf("usage = %+v, want in=%d (uncached input, js stream.js:122)", usage, tc.want)
			}
		})
	}
}

// TestIdleUpstreamIsClassifiedAsTimeout 钉住 B1 的另一半：空闲截止必须被认成
// TIMEOUT，而不是掉进 classifyAttemptError 的 SERVER 兜底。
//
// 引擎的 retryOn 与 cooldownOn 都含 CodeTimeout —— 一个停发不关流的出口该被
// 冷却并换掉。归成 SERVER 的话引擎会把「这个出口不回话了」记成「供应商故障」，
// 下一次还会挑同一个出口，客户端则收到 502 而看不到真正的病因。
func TestIdleUpstreamIsClassifiedAsTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-release // 只停发，不关流
	}))
	defer srv.Close()
	defer close(release)

	// 空闲窗口 120ms，上游停发：adapter 必须报 TIMEOUT。
	a := newAdapter(srv.URL, httpclient.NewStreamClient(nil, 120*time.Millisecond), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if err == nil {
		t.Fatal("want a failure from the idle upstream")
	}
	if code := errors.CodeOf(err); code != check.CodeTimeout {
		t.Fatalf("code = %q, want %q (idle deadline must be a retryable TIMEOUT)", code, check.CodeTimeout)
	}
	if failure, ok := err.(errors.Failure); !ok || !failure.Retryable {
		t.Fatalf("failure = %#v, want Retryable", err)
	}
}

func TestDecoyToolsAreInjectedOnEveryWire(t *testing.T) {
	for _, model := range []string{"big-pickle", "muse-spark-1.3-contributor-free", "union-alpha"} {
		f := newFakeUpstream(t, func(n int) (int, string, string) {
			return 200, "application/json", `{"usage":{"input_tokens":1,"output_tokens":1}}`
		})
		a := newAdapter(f.srv.URL, f.srv.Client(), model)
		if _, err := a.Complete(context.Background(), Request{
			Messages: []messages.Message{{Role: "user", Content: "hi"}},
			Stream:   true,
		}, collect(&strings.Builder{})); err != nil {
			t.Fatalf("%s: Complete: %v", model, err)
		}
		names := toolNamesOf(decodeBody(t, f.requests()[0].body))
		has := func(want string) bool {
			for _, name := range names {
				if name == want {
					return true
				}
			}
			return false
		}
		if !has("bash") || !has("read") {
			t.Fatalf("%s: decoys missing, tools = %v(免费闸门要求两个名字同时声明)", model, names)
		}
	}
}
