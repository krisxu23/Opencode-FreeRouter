// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package adapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/effort"
	"freerouter/internal/errors"
	"freerouter/internal/httpclient"
	"freerouter/internal/messages"
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

// collect 只把**文本**增量拼进 builder:多数既有用例断言的就是正文。
func collect(chunks *strings.Builder) func(Delta) error {
	return func(d Delta) error {
		if d.Kind == DeltaText {
			chunks.WriteString(d.Text)
		}
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
	}, func(Delta) error {
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
	res, err := a.exchange(context.Background(), tu, newSink(collect(&strings.Builder{}), renames))
	if err != nil {
		t.Fatalf("exchange: %v (a stale reasoning reference must be stripped and replayed once)", err)
	}
	if !res.Usage.HasUsage {
		t.Fatalf("usage lost across the replay: %+v", res.Usage)
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
	}, func(d Delta) error {
		if d.Kind != DeltaText {
			return nil
		}
		calls++
		chunks.WriteString(d.Text)
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
	if !usage.Usage.HasUsage || usage.Usage.In != 7 || usage.Usage.Out != 3 {
		t.Fatalf("usage = %+v, want HasUsage in=7 out=3", usage.Usage)
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

// collectAll 收全部上行事件,断言 tool-call / reasoning 增量用(B13 之前根本没有
// 这两类事件,只有文本能走通)。
func collectAll(got *[]Delta) func(Delta) error {
	return func(d Delta) error {
		*got = append(*got, d)
		return nil
	}
}

// pickOf 按种类取事件,断言顺序与数量用。
func pickOf(got []Delta, kind DeltaKind) []Delta {
	var out []Delta
	for _, d := range got {
		if d.Kind == kind {
			out = append(out, d)
		}
	}
	return out
}

// chatToolFrame 拼一个 chat 线的 delta.tool_calls 帧。空字段按上游的省略行为处理:
// 只有非空的 id/name/arguments 才写进帧里。arguments 传**未转义**的 JSON 片段,
// 由 json.Marshal 负责转义。
func chatToolFrame(idx int, id, name, args string) string {
	call := map[string]any{"index": idx}
	if id != "" {
		call["id"] = id
	}
	fn := map[string]any{}
	if name != "" {
		fn["name"] = name
	}
	if args != "" {
		fn["arguments"] = args
	}
	if len(fn) > 0 {
		call["function"] = fn
	}
	raw, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{
		"delta": map[string]any{"tool_calls": []any{call}}}}})
	return string(raw)
}

// TestChatWireProjectsToolCallDeltas 钉住 B13 的核心缺口:chat 线的
// delta.tool_calls 必须作为增量上行。改造前 feedChat 只读 delta.content,函数
// 调用在网关这一侧被整段吞掉,调用方永远收不到 tool_calls。
func TestChatWireProjectsToolCallDeltas(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			chatToolFrame(0, "call_7", "glob", ""),
			chatToolFrame(0, "", "", `{"q":`),
			chatToolFrame(0, "", "", `"src/**"}`),
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	deltas := pickOf(got, DeltaToolCall)
	ends := pickOf(got, DeltaToolCallEnd)
	// 首帧只登记 id/name(JS 的 toolStart 不发帧),所以参数增量是两条。
	if len(deltas) != 2 {
		t.Fatalf("tool-call 增量 = %d, want 2(%+v)", len(deltas), got)
	}
	if deltas[0].ID != "call_7" || deltas[0].Name != "glob" || deltas[0].Text != `{"q":` {
		t.Fatalf("首帧增量 = %+v", deltas[0])
	}
	if deltas[1].Text != `"src/**"}` {
		t.Fatalf("次帧增量 = %+v", deltas[1])
	}
	if deltas[0].Index != deltas[1].Index {
		t.Fatalf("同一 tool-call 的两帧必须同槽位: %d vs %d", deltas[0].Index, deltas[1].Index)
	}
	if len(ends) != 1 || ends[0].Arguments != `{"q":"src/**"}` || ends[0].ID != "call_7" || ends[0].Name != "glob" {
		t.Fatalf("收尾帧 = %+v, want 拼齐的 arguments", ends)
	}
	if !res.SawToolCall || res.SawText || res.SawReasoning || res.BrokenToolCall {
		t.Fatalf("result = %+v, want 只有 SawToolCall", res)
	}
	if res.Finish != "tool_calls" {
		t.Fatalf("finish = %q, want tool_calls", res.Finish)
	}
}

// TestChatWireProjectsReasoningDeltas 钉住推理增量:chat 线有 **delta.reasoning
// 与 delta.reasoning_details[].text 两条来源**,过去都落在文本契约之外。
func TestChatWireProjectsReasoningDeltas(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			`{"choices":[{"delta":{"reasoning":"think"}}]}`,
			`{"choices":[{"delta":{"reasoning_details":[{"type":"reasoning.text","text":" more"}]}}]}`,
			`{"choices":[{"delta":{"content":"answer"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	reasoning := pickOf(got, DeltaReasoning)
	text := pickOf(got, DeltaText)
	if len(reasoning) != 2 || reasoning[0].Text != "think" || reasoning[1].Text != " more" {
		t.Fatalf("reasoning 增量 = %+v", reasoning)
	}
	if len(text) != 1 || text[0].Text != "answer" {
		t.Fatalf("text 增量 = %+v", text)
	}
	// 推理与正文是两个块,槽位序号必须分开(engine 按槽位归并)。
	if reasoning[0].Index == text[0].Index {
		t.Fatalf("推理与正文不得共用槽位: %d", text[0].Index)
	}
	if !res.SawReasoning || !res.SawText {
		t.Fatalf("result = %+v, want SawReasoning 与 SawText 同时为真", res)
	}
}

// TestMessagesWireProjectsThinkingAndToolUse 钉住 messages 线:thinking_delta
// 与 input_json_delta 过去都不在 feedClaude 的读取范围内。
func TestMessagesWireProjectsThinkingAndToolUse(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			`{"type":"message_start","message":{"usage":{"input_tokens":3,"output_tokens":0}}}`,
			`{"type":"content_block_start","index":0,"content_block":{"type":"thinking"}}`,
			`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"hmm"}}`,
			`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_9","name":"glob"}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}`,
			`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"x\"}"}}`,
			`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "union-alpha")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	reasoning := pickOf(got, DeltaReasoning)
	deltas := pickOf(got, DeltaToolCall)
	ends := pickOf(got, DeltaToolCallEnd)
	if len(reasoning) != 1 || reasoning[0].Text != "hmm" {
		t.Fatalf("thinking 增量 = %+v", reasoning)
	}
	if len(deltas) != 2 || deltas[0].ID != "toolu_9" || deltas[0].Name != "glob" {
		t.Fatalf("tool-call 增量 = %+v", deltas)
	}
	if len(ends) != 1 || ends[0].Arguments != `{"q":"x"}` {
		t.Fatalf("收尾帧 = %+v", ends)
	}
	if deltas[0].Index != deltas[1].Index || deltas[0].Index == reasoning[0].Index {
		t.Fatalf("槽位分配错乱: reasoning=%d tool=%d/%d", reasoning[0].Index, deltas[0].Index, deltas[1].Index)
	}
	if !res.SawReasoning || !res.SawToolCall || res.BrokenToolCall {
		t.Fatalf("result = %+v", res)
	}
	if res.Finish != "tool_use" {
		t.Fatalf("finish = %q, want tool_use", res.Finish)
	}
}

// TestResponsesWireProjectsFunctionCallAndReasoning 钉住 responses 线:
// function_call 项、arguments 增量与推理摘要过去都被丢掉。
func TestResponsesWireProjectsFunctionCallAndReasoning(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_1"}}`,
			`{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","delta":"because"}`,
			`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","call_id":"call_3","name":"grep"}}`,
			`{"type":"response.function_call_arguments.delta","output_index":1,"delta":"{\"pat\":"}`,
			`{"type":"response.function_call_arguments.delta","output_index":1,"delta":"\"y\"}"}`,
			`{"type":"response.output_text.delta","output_index":2,"delta":"hi"}`,
			`{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":4,"output_tokens":2}}}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "muse-spark-1.3-contributor-free")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	deltas := pickOf(got, DeltaToolCall)
	ends := pickOf(got, DeltaToolCallEnd)
	reasoning := pickOf(got, DeltaReasoning)
	if len(deltas) != 2 || deltas[0].ID != "call_3" || deltas[0].Name != "grep" {
		t.Fatalf("function_call 增量 = %+v", deltas)
	}
	if len(ends) != 1 || ends[0].Arguments != `{"pat":"y"}` {
		t.Fatalf("收尾帧 = %+v", ends)
	}
	if len(reasoning) != 1 || reasoning[0].Text != "because" {
		t.Fatalf("推理摘要 = %+v", reasoning)
	}
	if pickOf(got, DeltaText)[0].Text != "hi" {
		t.Fatalf("正文增量 = %+v", got)
	}
	// response.completed 的 status 归一成 JS 的收尾 token 原文。
	if res.Finish != "stop" {
		t.Fatalf("finish = %q, want stop", res.Finish)
	}
	if !res.SawToolCall || !res.SawReasoning || !res.SawText {
		t.Fatalf("result = %+v", res)
	}
}

// TestBrokenToolCallIsReported 钉住 B13 的第二半:参数被输出上限截在半截 JSON
// 上时,上游仍然报 finish_reason:"tool_calls"(实测 2026-09-25),所以只有拼齐的
// 参数能判出「这轮被截断」。
func TestBrokenToolCallIsReported(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			chatToolFrame(0, "call_1", "glob", `{"q":"unclosed`),
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !res.BrokenToolCall {
		t.Fatalf("result = %+v, want BrokenToolCall", res)
	}
	// 半截参数原样交出:剪枝是 engine 侧的事,投影层不隐瞒内容。
	ends := pickOf(got, DeltaToolCallEnd)
	if len(ends) != 1 || ends[0].Arguments != `{"q":"unclosed` {
		t.Fatalf("收尾帧 = %+v", ends)
	}
}

// TestToolCallWithNoArgumentsStillEndsTheBlock:一个参数增量都没有的调用,收尾帧
// 必须补 "{}" —— 否则 engine 折不出这个调用,调用方看不见它。
func TestToolCallWithNoArgumentsStillEndsTheBlock(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			chatToolFrame(0, "call_z", "now", ""),
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(pickOf(got, DeltaToolCall)) != 0 {
		t.Fatalf("没有参数增量时不该有增量帧: %+v", got)
	}
	ends := pickOf(got, DeltaToolCallEnd)
	if len(ends) != 1 || ends[0].Arguments != "{}" || ends[0].ID != "call_z" {
		t.Fatalf("收尾帧 = %+v, want arguments=\"{}\"", ends)
	}
	if !res.SawToolCall || res.BrokenToolCall {
		t.Fatalf("result = %+v", res)
	}
}

// TestToolCallIDIsMintedWhenTheProviderOmitsIt:没有 id 的调用下一轮会带着空
// toolCallId 回来,pairing 修复会把两侧一起丢掉,模型永远看不到自己的结果而无限
// 重发同一个调用 —— 投影层要先铸一个稳定替身(js stream.js:23-25、:42-45)。
func TestToolCallIDIsMintedWhenTheProviderOmitsIt(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			chatToolFrame(0, "", "now", "{}"),
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	if _, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	deltas := pickOf(got, DeltaToolCall)
	if len(deltas) != 1 {
		t.Fatalf("增量 = %+v", got)
	}
	re := regexp.MustCompile(`^call_[0-9a-f]{24}$`)
	if !re.MatchString(deltas[0].ID) {
		t.Fatalf("铸出的 tool-call id 形状不对: %q, want call_<24hex>", deltas[0].ID)
	}
}

// TestClientToolsSurviveTheFingerprintGate 钉住 B13 的第二处 Go 独有回归:
// adapter 交出去的是 []messages.ToolDef,而指纹闸门只断言 []any —— 断言永不
// 成立,hadClientTools 恒 false:调用方的工具被两个诱饵整份顶掉,chat 线还被强写
// tool_choice:"none",函数调用从请求侧就不可能发生。
//
// 同时钉住名字还原:线上小写归一(闸门要求),回程必须换回调用方的拼写。
func TestClientToolsSurviveTheFingerprintGate(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			chatToolFrame(0, "call_b", "bash", `{"cmd":"ls"}`),
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`[DONE]`,
		)
	})
	a := NewAdapter(Deps{
		Client: f.srv.Client(), Base: f.srv.URL, Model: "big-pickle",
		Effort: "balanced", Entry: entryFor("big-pickle"), SessionID: "conversation-7",
		Wire: upstream.WireFor("big-pickle"), NodeKey: "n1",
		Tools: []messages.Tool{
			{Name: "Bash", Description: "run"},
			{Name: "glob", Description: "match"},
		},
	})

	var got []Delta
	if _, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	sent := decodeBody(t, f.requests()[0].body)
	names := toolNamesOf(sent)
	count := func(want string) int {
		n := 0
		for _, name := range names {
			if name == want {
				n++
			}
		}
		return n
	}
	if got := count("bash"); got != 1 {
		t.Fatalf("线上 bash 出现 %d 次, want 1(调用方的 Bash 归一后不得与诱饵重复): %v", got, names)
	}
	if count("read") != 1 {
		t.Fatalf("诱饵 read 缺席: %v", names)
	}
	if count("glob") != 1 {
		t.Fatalf("调用方的 glob 被顶掉了: %v", names)
	}
	if _, has := sent["tool_choice"]; has {
		t.Fatalf("声明了工具的请求不得被强写 tool_choice: %v", sent["tool_choice"])
	}
	deltas := pickOf(got, DeltaToolCall)
	if len(deltas) != 1 || deltas[0].Name != "Bash" {
		t.Fatalf("回程的 tool 名没还原: %+v, want Bash", deltas)
	}
}

// TestProviderFinishTokenIsReported 钉住收尾 token 的上行:上游说 length 时
// 整轮是被输出上限截断的,engine 据此才报得出 finish_reason:"length"。
func TestProviderFinishTokenIsReported(t *testing.T) {
	cases := []struct {
		wire string
		body string
		want string
	}{
		{"chat", sseBody(
			`{"choices":[{"delta":{"content":"cut"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"length"}]}`,
			`[DONE]`), "length"},
		{"responses", sseBody(
			`{"type":"response.output_text.delta","delta":"cut"}`,
			`{"type":"response.completed","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
			`[DONE]`), "length"},
	}
	for _, tc := range cases {
		t.Run(tc.wire, func(t *testing.T) {
			model := "big-pickle"
			if tc.wire == "responses" {
				model = "muse-spark-1.3-contributor-free"
			}
			f := newFakeUpstream(t, func(n int) (int, string, string) {
				return 200, "text/event-stream", tc.body
			})
			a := newAdapter(f.srv.URL, f.srv.Client(), model)
			res, err := a.Complete(context.Background(), Request{
				Messages: []messages.Message{{Role: "user", Content: "hi"}},
				Stream:   true,
			}, collect(&strings.Builder{}))
			if err != nil {
				t.Fatalf("Complete: %v", err)
			}
			if res.Finish != tc.want {
				t.Fatalf("finish = %q, want %q", res.Finish, tc.want)
			}
		})
	}
}

// TestEmptyTurnReportsNoContentFlags:只出角色骨架与 usage 的一轮,三个 saw 位
// 全空 —— engine 据此把它归成 CodeEmpty,而不是交给调用方一个静默的空回合。
func TestEmptyTurnReportsNoContentFlags(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseBody(
			`{"choices":[{"delta":{"role":"assistant"},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":1}}`,
			`[DONE]`,
		)
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("退化的一轮不该有任何上行帧: %+v", got)
	}
	if res.SawText || res.SawToolCall || res.SawReasoning || res.BrokenToolCall {
		t.Fatalf("result = %+v, want 三个 saw 位全空", res)
	}
	if res.Finish != "stop" {
		t.Fatalf("finish = %q, want stop", res.Finish)
	}
	if !res.Usage.HasUsage {
		t.Fatalf("usage 应当仍然在: %+v", res.Usage)
	}
}

// TestNonStreamingReplyMarksSawText:非流式整包的正文也走投影通道,所以它不算
// 退化的一轮(否则 B13 的 CodeEmpty 会把补齐的正文判成空)。
func TestNonStreamingReplyMarksSawText(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 200, "application/json",
			`{"choices":[{"message":{"content":"plain"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")

	var got []Delta
	res, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, collectAll(&got))
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(pickOf(got, DeltaText)) != 1 || !res.SawText {
		t.Fatalf("result = %+v deltas = %+v", res, got)
	}
}

// TestResponsesIncompleteEventIsRecognized 是整分支评审的 BUG-1:responses 线被
// 输出上限截断时,真实上游发的是**独立终止事件** `response.incomplete`(带
// status:"incomplete" 与 incomplete_details.reason),而投影层的 switch 只认
// `response.completed` —— 于是 finish 保持空串,engine 的 finishReasonOf("") 落
// default ⇒ 客户端收到 `finish_reason:"stop"` 的一条腰斩回答,不会去续写。
// `response.failed` 同理被无声丢掉。分支体里的映射早就是对的,只是走不到。
//
// 对照组把旧固件的形状也钉住(completed + status:"incomplete" —— 真实上游不会发
// 这种组合,但既然已经测过它,就让它继续测得对)。
func TestResponsesIncompleteEventIsRecognized(t *testing.T) {
	tests := []struct {
		name  string
		frame string
		want  string
	}{
		{"incomplete 事件",
			`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
			"length"},
		{"failed 事件",
			`{"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","message":"boom"}}}`,
			"failed"},
		{"completed 事件", `{"type":"response.completed","response":{"status":"completed"}}`, "stop"},
		{"completed 带 incomplete status(旧形状)",
			`{"type":"response.completed","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}`,
			"length"},
	}
	for _, tc := range tests {
		f := newFakeUpstream(t, func(n int) (int, string, string) {
			return 200, "text/event-stream", sseBody(
				`{"type":"response.output_text.delta","delta":"partial"}`, tc.frame)
		})
		a := newAdapter(f.srv.URL, f.srv.Client(), "muse-spark-1.3-contributor-free")
		res, err := a.Complete(context.Background(), Request{
			Messages: []messages.Message{{Role: "user", Content: "hi"}}, Stream: true,
		}, collect(&strings.Builder{}))
		if err != nil {
			t.Fatalf("%s: Complete: %v", tc.name, err)
		}
		if res.Finish != tc.want {
			t.Errorf("%s: Finish = %q, want %q", tc.name, res.Finish, tc.want)
		}
	}
}

// TestReplayIdleTimeoutIsATimeout 是整分支评审的 BUG-2:`exchange` 把 readReply /
// 拨号错误过一层 idleOrPassthrough 翻成 TIMEOUT,`replay` 两条路都没过。于是
// stale-reasoning 重放期间出口卡死时,engine 拿到的是裸 ErrIdleTimeout(或回落成
// 原始 400 的判决)⇒ 归入 SERVER:不在 retryOn 也不在 cooldownOn —— 卡死的出口
// 既不会被换掉也不会被冷却。这正是 idleOrPassthrough 注释里写明不能犯的事
// (「同一个出口会被反复选中,B1 的另一半」)。
func TestReplayIdleTimeoutIsATimeout(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		if n == 0 {
			return 400, "application/json", `{"error":{"message":"Reasoning item rs_9 not found"}}`
		}
		// 重放那一发:收下连接但永远不发头 —— 出口卡死的形状。
		time.Sleep(2 * time.Second)
		return 200, "application/json", `{}`
	})
	// 空闲截止只有 60ms:头阶段超时由 httpclient 的 idleTransport 取消请求。
	a := newAdapter(f.srv.URL, httpclient.NewStreamClient(nil, 60*time.Millisecond),
		"muse-spark-1.3-contributor-free")

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
	_, err = a.exchange(context.Background(), tu, newSink(collect(&strings.Builder{}), renames))
	if err == nil {
		t.Fatal("want a failure when the replay stalls")
	}
	failure, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("err = %#v, want errors.Failure", err)
	}
	if failure.Code != check.CodeTimeout {
		t.Fatalf("重放期卡死被归成 %q, want TIMEOUT:SERVER 既不重试也不冷却,出口会被反复选中", failure.Code)
	}
	if !failure.Retryable {
		t.Fatal("TIMEOUT 必须可重试")
	}
}

// TestUnreadable401BodyStaysACredentialFailure 是整分支评审的 BUG-3:本仓库自己在
// errors.go 里写着「401 是明确的凭证问题,把它降级成『可重试』会让一个配置错误
// 变成扫全池的慢失败」。R18 给 classifyErrorBody 加的「读不全就改判 TRANSPORT」
// 只对**判决依赖文案**的状态码成立(403 要靠文案区分 FreeTier 配额);401 的判决
// 与 body 无关,却被一起升级 ⇒ 引擎按 attemptCap=20、无墙钟扫全池并冷却 20 个
// 出口,客户端最后只看到 502,连「凭证错了」都丢了。
func TestUnreadable401BodyStaysACredentialFailure(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 401, "application/json", `{"error":{"type":"authentication_error","message":"bad key"}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}}, Stream: true,
	}, collect(&strings.Builder{}))
	if err == nil {
		t.Fatal("want a failure")
	}
	failure, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("err = %#v", err)
	}
	if failure.Code != check.CodeCredential || failure.Retryable {
		t.Fatalf("401 被判成 %q(retryable=%v), want 不可重试的凭证判决", failure.Code, failure.Retryable)
	}

	// 同一判决在「body 读不全」时也必须保持不变:401 不依赖文案。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "4000")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"type":"auth`)
	}))
	t.Cleanup(srv.Close)
	a2 := newAdapter(srv.URL, srv.Client(), "big-pickle")
	_, err = a2.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}}, Stream: true,
	}, collect(&strings.Builder{}))
	failure2, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("截断的 401 没有产出 Failure: %#v", err)
	}
	if failure2.Code != check.CodeCredential || failure2.Retryable {
		t.Fatalf("截断的 401 被升级成 %q(retryable=%v):一个配置错误会变成扫全池的慢失败",
			failure2.Code, failure2.Retryable)
	}
}

// TestEveryWireGetsAPlaceholderWhenHistoryProjectsToNothing 是整分支评审补记的
// R6:一条历史被投影全部丢掉时(`messages.go` 的空 content 会 `continue`,持久
// 历史里的纯 system 轮、全空正文都会走到这里),payload 的 messages 数组是空的,
// 上游回 400;`errors.Classify` 的 default 把 400 判成 SERVER,而 SERVER 在
// engine 的 retryOn 里 ⇒ 同一颗必然失败的请求被真实重发 20 次才变成 503。
// responses 线早就有占位轮(那段注释还在),messages 与 chat 两条线没有。
func TestEveryWireGetsAPlaceholderWhenHistoryProjectsToNothing(t *testing.T) {
	cases := []struct {
		name  string
		model string
		key   string
	}{
		{"chat 线", "big-pickle", "messages"},
		{"messages 线", "union-alpha", "messages"},
		{"responses 线(已有守卫,当对照组)", "muse-spark-1.3-contributor-free", "input"},
	}
	for _, tc := range cases {
		a := newAdapter("http://127.0.0.1:1", http.DefaultClient, tc.model)
		payload, err := a.build(Request{
			// 一条会被投影全部丢掉的轮:正文为空。
			Messages: []messages.Message{{Role: "user", Content: ""}},
			Stream:   true,
		})
		if err != nil {
			t.Fatalf("%s: build: %v", tc.name, err)
		}
		// 三条线的 payload 值是各自的强类型切片,判「有没有占位轮」按线上形状判:
		// 过一遍 JSON 再看数组长度。
		raw, err := json.Marshal(payload[tc.key])
		if err != nil {
			t.Fatalf("%s: marshal %s: %v", tc.name, tc.key, err)
		}
		var list []any
		if err := json.Unmarshal(raw, &list); err != nil {
			t.Fatalf("%s: %s 不是数组: %s", tc.name, tc.key, raw)
		}
		if len(list) == 0 {
			t.Fatalf("%s: 空 %s 发给上游只会换来 400,然后被当 5xx 重试 20 次(R6)", tc.name, tc.key)
		}
	}
}

// TestUnreadableErrorBodyIsRetriedAsTransport 钉住 R18:非 2xx 的 body 读失败时,
// 旧代码把错误连同「文案没读到」这件事一起丢了。403 的 FreeTier 判据依赖 body
// 文本 —— 文本没了就退化成不可重试的凭证错误,本应换出口的 403 变成对客户端的
// 立即失败。读不断才是判决,读断了只是这条连接不可信。
//
// 夹具用「声明的 Content-Length 比写出的字节多」制造真读错误(客户端 unexpected
// EOF),而不是手工喂前缀 —— 要钉的就是传输层这一步。
func TestUnreadableErrorBodyIsRetriedAsTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "4000")
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"type":"FreeTierError","message":"free tier limit reached for`)
	}))
	t.Cleanup(srv.Close)

	a := newAdapter(srv.URL, srv.Client(), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if err == nil {
		t.Fatal("want a failure from the truncated error body")
	}
	failure, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("err = %#v, want errors.Failure", err)
	}
	if failure.Code != check.CodeTransport {
		t.Fatalf("code = %q, want %q:文案没读全就不该下凭证判决", failure.Code, check.CodeTransport)
	}
	if !failure.Retryable {
		t.Fatal("读断的错误必须可重试(换出口是对的应对)")
	}
}

// TestReadable403FreeTierBodyStillClassifiesAsQuota 是上一条的对照组:读得到的
// 文本必须照旧分类,不能因为 R18 的改动把所有 403 都推成 transport。
func TestReadable403FreeTierBodyStillClassifiesAsQuota(t *testing.T) {
	f := newFakeUpstream(t, func(n int) (int, string, string) {
		return 403, "application/json", `{"error":{"type":"FreeTierError","message":"free tier limit reached"}}`
	})
	a := newAdapter(f.srv.URL, f.srv.Client(), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	}, collect(&strings.Builder{}))
	if code := errors.CodeOf(err); code != check.CodeQuota {
		t.Fatalf("code = %q, want %q", code, check.CodeQuota)
	}
}

// TestUnreadableSuccessBodyIsRetriedAsTransport 钉住 R18 的第三个点:2xx 的
// JSON 分支也吞了读错误 —— 截断的整包会退化成「unexpected non-SSE response」的
// SERVER 判决,而不是一次该换出口的传输故障。
func TestUnreadableSuccessBodyIsRetriedAsTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "4000")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"par`)
	}))
	t.Cleanup(srv.Close)

	a := newAdapter(srv.URL, srv.Client(), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, collect(&strings.Builder{}))
	if code := errors.CodeOf(err); code != check.CodeTransport {
		t.Fatalf("code = %q, want %q(读断的 2xx 不是「上游回了奇怪的东西」)", code, check.CodeTransport)
	}
	if failure, ok := err.(errors.Failure); !ok || !failure.Retryable {
		t.Fatalf("err = %#v, want Retryable", err)
	}
}

// TestLargeValidAnswerIsNotTruncatedIntoServerSweep 是上游层审计「1MiB 静默
// 截断」的钉:一份**合法**的大 JSON 回答(>1MiB,非流式的整包)过去被
// readAllPrefix 在 1MB 处砍断 → json.Unmarshal 失败 → readJSON 的守卫报
// 「unexpected non-SSE response」→ SERVER。三条谎言:上游明明回了可解析的
// JSON;SERVER 不在 cooldownOn(坏出口不冷却);SERVER 在 retryOn —— 于是一
// 个能答的出口被判坏、白扫 20 个出口把整篇回答重新生成一遍(上游按次计费)。
//
// 修复把 2xx 整包读取从「静默前缀」换成严格读 + 16MiB 上限,这条回答现在
// 必须解析成功、文本完整交出。用 io.Pipe 流式喂,测试自己先分配 2MB 字符串
// 就测不到被测代码的内存了(与 :230 的手法一致)。
func TestLargeValidAnswerIsNotTruncatedIntoServerSweep(t *testing.T) {
	const contentLen = 2 << 20 // 2MiB 正文,> 旧的 1MiB 静默截断点
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// {"choices":[{"message":{"content":"aaaa…2MiB…"}}]}
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"`)
		chunk := strings.Repeat("a", 64*1024)
		for sent := 0; sent < contentLen; sent += len(chunk) {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
		_, _ = io.WriteString(w, `"}}]}`)
	}))
	t.Cleanup(srv.Close)

	a := newAdapter(srv.URL, srv.Client(), "big-pickle")
	var out strings.Builder
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, collect(&out))
	if err != nil {
		t.Fatalf("合法大回答被判失败(截断→SERVER→扫池的复发): code=%q err=%v", errors.CodeOf(err), err)
	}
	if out.Len() < contentLen {
		t.Fatalf("正文 = %d 字节, want ≥%d(必须完整不被截)", out.Len(), contentLen)
	}
}

// TestOversizeAnswerFailsAsTransportNotServer 钉住修复的另一半:超过
// maxAnswerBodyBytes(8MiB)严格上限的 2xx 体不是合法回答,必须走
// bodyReadFailure 的 TRANSPORT(在 retryOn 里、换出口),而不是继续骗成
// SERVER。读断的整包与读断的错误信封同一条 R18 语义:「整包没读全」不等于
// 「上游回了奇怪的东西」。
func TestOversizeAnswerFailsAsTransportNotServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"`)
		chunk := strings.Repeat("a", 64*1024)
		// 无限吐,直到读侧在 8MiB+1 处报错返回、pipe 关闭。
		for {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	a := newAdapter(srv.URL, srv.Client(), "big-pickle")
	_, err := a.Complete(context.Background(), Request{
		Messages: []messages.Message{{Role: "user", Content: "hi"}},
		Stream:   false,
	}, collect(&strings.Builder{}))
	if code := errors.CodeOf(err); code != check.CodeTransport {
		t.Fatalf("code = %q, want TRANSPORT(超限整包是传输故障,不是「奇怪响应」的 SERVER)", code)
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
