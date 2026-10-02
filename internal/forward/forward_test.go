// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package forward

import (
	"bufio"
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
	"freerouter/internal/engine"
	frerrors "freerouter/internal/errors"
	"freerouter/internal/stream"
)

const testKey = "ofm-0123456789abcdefghijklmnopqrstuv"

// stub 是转发层唯一的上游。转发层不认识池子、sing-box、health,所以整个测试面
// 只有一个可编程的 Complete —— 这也正是这个包能被单测穷尽的原因。
type stub struct {
	mu       sync.Mutex
	enabled  bool
	key      string
	rows     []engine.Row
	complete func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error)
	calls    int
	lastReq  engine.Request
}

func newStub() *stub { return &stub{enabled: true, key: testKey} }

func (s *stub) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := New(Config{
		Enabled:    func() bool { return s.enabled },
		ForwardKey: func() string { return s.key },
		Complete: func(ctx context.Context, req engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
			s.mu.Lock()
			s.calls++
			s.lastReq = req
			fn := s.complete
			s.mu.Unlock()
			if fn == nil {
				return engine.Outcome{}, nil
			}
			return fn(ctx, req, onChunk)
		},
		ModelRows: func() []engine.Row { return s.rows },
		Log:       func(string) {},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func (s *stub) callsN() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func auth() map[string]string { return map[string]string{"Authorization": "Bearer " + testKey} }

func do(t *testing.T, ts *httptest.Server, method, path string, hdr map[string]string, body string) (*http.Response, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	rq, err := http.NewRequest(method, ts.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, v := range hdr {
		rq.Header.Set(k, v)
	}
	res, err := ts.Client().Do(rq)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return res, string(raw)
}

// sseFrames 拆出所有 data: 行(跳过 [DONE])。SSE 是行协议,断言就该按行做。
func sseFrames(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			t.Fatalf("bad SSE frame %q: %v", payload, err)
		}
		out = append(out, obj)
	}
	return out
}

func firstChoice(t *testing.T, frame map[string]any) map[string]any {
	t.Helper()
	choices, ok := frame["choices"].([]any)
	if !ok || len(choices) == 0 {
		t.Fatalf("frame has no choices: %v", frame)
	}
	c, ok := choices[0].(map[string]any)
	if !ok {
		t.Fatalf("choice is not an object: %v", choices[0])
	}
	return c
}

func deltaOf(t *testing.T, frame map[string]any) map[string]any {
	t.Helper()
	d, ok := firstChoice(t, frame)["delta"].(map[string]any)
	if !ok {
		t.Fatalf("choice has no delta: %v", frame)
	}
	return d
}

func decode(t *testing.T, body string) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal([]byte(body), &obj); err != nil {
		t.Fatalf("body is not JSON (%v): %s", err, body)
	}
	return obj
}

// ---- 密钥 ----

func TestGenerateKeyRoundTripsConstantTime(t *testing.T) {
	k := GenerateKey()
	if !strings.HasPrefix(k, keyPrefix) {
		t.Fatalf("key %q lacks the %q prefix", k, keyPrefix)
	}
	if len(k) < len(keyPrefix)+30 {
		t.Fatalf("key %q is shorter than 24 random bytes of base64url", k)
	}
	if !KeyMatches(k, k) {
		t.Fatal("a key must match itself")
	}
	if KeyMatches(k+"x", k) || KeyMatches(k[:len(k)-1], k) {
		t.Fatal("a key must not match a longer or shorter string")
	}
	if KeyMatches("", k) {
		t.Fatal("the empty string must not match a real key")
	}
	if !KeyMatches("", "") {
		t.Fatal("two empty strings are equal (js: byteLength 0 === 0)")
	}
}

func TestKeyComparisonIsLengthInsensitiveInTime(t *testing.T) {
	// 粗筛,不是证明:crypto/subtle 在长度不等时会立刻返回 0,所以实现必须
	// 先填充到等长再比较。谁把填充删了,这里的时间会随长度差出现台阶 ——
	// 常数时间的**语义**由 TestGenerateKeyRoundTripsConstantTime 钉住。
	start := time.Now()
	for i := 0; i < 20000; i++ {
		KeyMatches(strings.Repeat("x", i%64), testKey)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("20000 comparisons took %v — the comparison is doing something expensive", d)
	}
}

// ---- 路由与鉴权 ----

func TestHealthProbeNeedsNoKey(t *testing.T) {
	ts := newStub().serve(t)
	for _, path := range []string{"/", "/health"} {
		res, body := do(t, ts, http.MethodGet, path, nil, "")
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d body %s", path, res.StatusCode, body)
		}
		got := decode(t, body)
		if got["ok"] != true || got["service"] != serviceName {
			t.Fatalf("%s: body %s", path, body)
		}
	}
	// 探活免鉴权,模型清单不免 —— 它会把可用模型暴露给未授权的调用方。
	if res, _ := do(t, ts, http.MethodGet, "/v1/models", nil, ""); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("models without a key: status %d, want 401", res.StatusCode)
	}
}

func TestDisabledGatewayAnswers503EverywhereExceptProbe(t *testing.T) {
	st := newStub()
	st.enabled = false
	ts := st.serve(t)

	// 关闭判定排在一切之前(js :112-115),连 /health 与 CORS 预检都是 503。
	// 这不是「探活也要密钥」那类错误 —— 是网关被用户主动关掉了,此时对它
	// 回答 200 会让负载均衡器把一个不干活的实例判成健康。
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/health"},
		{http.MethodGet, "/"},
		{http.MethodGet, "/v1/models"},
		{http.MethodOptions, "/v1/chat/completions"},
		{http.MethodPost, "/v1/chat/completions"},
		{http.MethodGet, "/nope"},
	} {
		res, body := do(t, ts, c.method, c.path, auth(), "{}")
		if res.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s %s: status %d, want 503", c.method, c.path, res.StatusCode)
		}
		if got := decode(t, body)["error"].(map[string]any); got["type"] != "service_unavailable" {
			t.Fatalf("%s %s: error type %v", c.method, c.path, got["type"])
		}
	}
	if st.callsN() != 0 {
		t.Fatalf("a disabled gateway must not reach Complete (%d calls)", st.callsN())
	}
}

func TestMissingKeyIs401AndWrongKeyIs401(t *testing.T) {
	ts := newStub().serve(t)
	resA, bodyA := do(t, ts, http.MethodGet, "/v1/models", nil, "")
	resB, bodyB := do(t, ts, http.MethodGet, "/v1/models", map[string]string{"Authorization": "Bearer ofm-wrong"}, "")
	if resA.StatusCode != http.StatusUnauthorized || resB.StatusCode != http.StatusUnauthorized {
		t.Fatalf("statuses %d / %d, want 401 / 401", resA.StatusCode, resB.StatusCode)
	}
	// 「没带」和「带错了」必须不可区分:否则攻击者能拿响应差异探测密钥空间。
	if bodyA != bodyB {
		t.Fatalf("bodies differ:\n missing: %s\n wrong:   %s", bodyA, bodyB)
	}
	for _, h := range []string{"Content-Type", "Cache-Control"} {
		if resA.Header.Get(h) != resB.Header.Get(h) {
			t.Fatalf("header %s differs: %q vs %q", h, resA.Header.Get(h), resB.Header.Get(h))
		}
	}
}

func TestKeyIsAcceptedFromBearerOrHeader(t *testing.T) {
	ts := newStub().serve(t)
	for _, hdr := range []map[string]string{
		{"Authorization": "Bearer " + testKey},
		{"Authorization": "bearer " + testKey},
		{"X-Api-Key": testKey},
	} {
		res, body := do(t, ts, http.MethodGet, "/v1/models", hdr, "")
		if res.StatusCode != http.StatusOK {
			t.Fatalf("headers %v: status %d body %s", hdr, res.StatusCode, body)
		}
	}
}

func TestAuthIsCheckedBeforeRouting(t *testing.T) {
	// js :128-131 的次序:鉴权在路由之前。没有密钥的人不该从 404/401 的差别
	// 里看出哪些路径存在。
	ts := newStub().serve(t)
	res, _ := do(t, ts, http.MethodPost, "/v1/embeddings", nil, "{}")
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unknown path without a key: status %d, want 401", res.StatusCode)
	}
}

func TestUnknownPathIs404(t *testing.T) {
	ts := newStub().serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/embeddings", auth(), "{}")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", res.StatusCode)
	}
	got := decode(t, body)
	if got["error"].(map[string]any)["message"] != "no route for POST /v1/embeddings" {
		t.Fatalf("body %s", body)
	}
}

func TestOptionsReturnsNoContentWithCORS(t *testing.T) {
	ts := newStub().serve(t)
	res, _ := do(t, ts, http.MethodOptions, "/v1/chat/completions", nil, "")
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("status %d, want 204", res.StatusCode)
	}
	for k, want := range map[string]string{
		"Access-Control-Allow-Origin":  "*",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Max-Age":       "600",
	} {
		if got := res.Header.Get(k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestModelsWorksWithAndWithoutV1Prefix(t *testing.T) {
	st := newStub()
	st.rows = []engine.Row{{ID: "m1", Object: "model", Created: 7, OwnedBy: "freerouter"}}
	ts := st.serve(t)
	for _, path := range []string{"/v1/models", "/models", "/v1/models/"} {
		res, body := do(t, ts, http.MethodGet, path, auth(), "")
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s: status %d body %s", path, res.StatusCode, body)
		}
		got := decode(t, body)
		if got["object"] != "list" {
			t.Fatalf("%s: object %v", path, got["object"])
		}
		data, _ := got["data"].([]any)
		if len(data) != 1 || data[0].(map[string]any)["id"] != "m1" {
			t.Fatalf("%s: data %v", path, got["data"])
		}
	}
}

func TestEmptyModelListIsAnArrayNotNull(t *testing.T) {
	// 客户端普遍对 data 直接做 map;null 会炸在它们那边,不是我们这边。
	ts := newStub().serve(t)
	_, body := do(t, ts, http.MethodGet, "/v1/models", auth(), "")
	if !strings.Contains(body, `"data":[]`) {
		t.Fatalf("empty list must serialize as [], got %s", body)
	}
}

// ---- 请求体 ----

func TestBodyOverEightMegabytesIs413(t *testing.T) {
	st := newStub()
	ts := st.serve(t)
	big := `{"model":"m","pad":"` + strings.Repeat("a", 9<<20) + `"}`
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), big)
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", res.StatusCode)
	}
	if got := decode(t, body)["error"].(map[string]any)["message"]; got != "request body too large" {
		t.Fatalf("message %v", got)
	}
	if st.callsN() != 0 {
		t.Fatalf("an oversized body must never reach Complete (%d calls)", st.callsN())
	}
}

func TestModelIsRequired(t *testing.T) {
	ts := newStub().serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"stream":false}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", res.StatusCode)
	}
	if got := decode(t, body)["error"].(map[string]any)["message"]; got != "`model` is required" {
		t.Fatalf("message %v", got)
	}
}

// ---- 非流式 ----

func TestNonStreamingThrownErrorIs500(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{}, frerrors.Failure{Code: check.CodeCredential, Status: 401, Message: "bad credential"}
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	// JS 的 complete 对「轮换耗尽/无健康出口」是 throw(src/engine.js:233),
	// 一路冒到 forward.js:103 的顶层 catch → 500。502 只属于 resolve 成
	// outcome.error 的拒绝(下一个测试)。差分 B7 实测同款耗尽错误两版分流。
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", res.StatusCode)
	}
	got := decode(t, body)
	e := got["error"].(map[string]any)
	// 客户端文案不该带内部码(errors.Failure.Error() 是 "CODE: message")。
	if e["message"] != "bad credential" || e["type"] != "server_error" {
		t.Fatalf("error %v", e)
	}
	if _, ok := got["choices"]; ok {
		t.Fatalf("a refused turn must not come back as a completion: %s", body)
	}
}

func TestNonStreamingOutcomeErrorWithNoContentIs502(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Error: "upstream stream ended early", Retryable: true}, nil
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", res.StatusCode)
	}
	if got := decode(t, body)["error"].(map[string]any)["message"]; got != "upstream stream ended early" {
		t.Fatalf("message %v", got)
	}
}

func TestNonStreamingPartialContentIs200(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Text: "半句", Error: "断流", Retryable: true}, nil
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 (内容已经出来了,丢掉它比报错更糟): %s", res.StatusCode, body)
	}
	msg := firstChoice(t, decode(t, body))["message"].(map[string]any)
	if msg["content"] != "半句" {
		t.Fatalf("content %v", msg["content"])
	}
}

func TestNonStreamingSuccessShape(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{
			Text:    "hi",
			Usage:   stream.Usage{In: 10, Out: 5, CacheRead: 3, HasUsage: true},
			ToolCalls: []engine.ToolCall{
				{ID: "call_1", Name: "search", Arguments: `{"q":"x"}`},
				{Name: "echo", Arguments: `{}`}, // 没有 id 时用 call_<i> 兜底
			},
		}, nil
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m (high)"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	got := decode(t, body)
	if got["object"] != "chat.completion" || got["model"] != "m" {
		t.Fatalf("object/model %v/%v (模型名必须去掉 effort 后缀)", got["object"], got["model"])
	}
	if !strings.HasPrefix(got["id"].(string), "chatcmpl-") {
		t.Fatalf("id %v", got["id"])
	}
	ch := firstChoice(t, got)
	if ch["finish_reason"] != "tool_calls" {
		t.Fatalf("finish_reason %v", ch["finish_reason"])
	}
	calls := ch["message"].(map[string]any)["tool_calls"].([]any)
	if len(calls) != 2 {
		t.Fatalf("tool_calls %v", calls)
	}
	if calls[0].(map[string]any)["id"] != "call_1" || calls[1].(map[string]any)["id"] != "call_1" {
		t.Fatalf("tool call ids %v", calls)
	}
	u := got["usage"].(map[string]any)
	if u["prompt_tokens"].(float64) != 13 || u["completion_tokens"].(float64) != 5 || u["total_tokens"].(float64) != 18 {
		t.Fatalf("usage %v", u)
	}
}

func TestNonStreamingTruncatedIsLength(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Text: "cut off", Truncated: true}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	got := decode(t, body)
	if ch := firstChoice(t, got); ch["finish_reason"] != "length" {
		t.Fatalf("finish_reason %v", ch["finish_reason"])
	}
	// 没有 usage 时用零三元组兜底(js :231 的 `outcome.usage ?? {...}`)。
	u := got["usage"].(map[string]any)
	if u["prompt_tokens"].(float64) != 0 || u["total_tokens"].(float64) != 0 {
		t.Fatalf("usage %v", u)
	}
}

// ---- 流式 ----

func TestStreamingErrorBeforeFirstChunkIs502(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{}, frerrors.Failure{Code: check.CodeTransport, Status: 502, Message: "dial tcp 1.2.3.4:443: connection refused"}
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502 (一个空 SSE 流在 SDK 眼里是「成功返回 0 个 token」)", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type %q — 头不该已经作为 SSE 发出", ct)
	}
	if got := decode(t, body)["error"].(map[string]any)["message"]; !strings.Contains(got.(string), "connection refused") {
		t.Fatalf("message %v", got)
	}
}

func TestStreamingErrorAfterHeadersSendsInBandError(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "partial "}); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{}, frerrors.Failure{Code: check.CodeTransport, Message: "our-free-model: stream read failed: terminated"}
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 — 头在第一帧之后已经花掉了", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type %q", ct)
	}
	if !strings.Contains(body, `"content":"partial "`) {
		t.Fatalf("已出的内容必须保留: %s", body)
	}
	if !strings.Contains(body, `"type":"server_error"`) || !strings.Contains(body, `"retryable":true`) {
		t.Fatalf("缺少 in-band error 事件: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("流必须以 data: [DONE] 收尾: %s", body)
	}
}

func TestStreamingOutcomeErrorBeforeFirstChunkIs502(t *testing.T) {
	// 计划 §2159 硬约束 1:首个 chunk 之前不发头。引擎在吐第一个字之前就
	// 报告「全部出口都不可用」时,调用方该拿到一个真正的 502,而不是一个
	// 只有 role 骨架的空 SSE 流 —— 后者在 OpenAI SDK 眼里是「成功返回了
	// 0 个 token」,调用方无从判断要不要重试。
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Error: "全部出口都不可用", Retryable: true}, nil
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502: %s", res.StatusCode, body)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type %q — 头不该已经作为 SSE 发出", ct)
	}
	e := decode(t, body)["error"].(map[string]any)
	if e["message"] != "全部出口都不可用" || e["type"] != "server_error" {
		t.Fatalf("error %v", e)
	}
}

func TestStreamingPartialContentWithOutcomeErrorKeepsFinishFrame(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		_ = onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "partial "})
		return engine.Outcome{Text: "partial ", Error: "断流", Retryable: true}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("缺少 error 事件: %s", body)
	}
	if !strings.Contains(body, `"finish_reason":"stop"`) {
		t.Fatalf("已经出过内容就必须照常收尾: %s", body)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("缺少 [DONE]: %s", body)
	}
}

func TestStreamingSuccessShape(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "he"}); err != nil {
			return engine.Outcome{}, err
		}
		if err := onChunk(engine.Chunk{Kind: engine.ChunkReasoning, Text: "think"}); err != nil {
			return engine.Outcome{}, err
		}
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "llo"}); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{Text: "hello"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	frames := sseFrames(t, body)

	// 第一帧必须是 role 骨架:OpenAI SDK 靠它建立 assistant 消息对象。
	if d := deltaOf(t, frames[0]); d["role"] != "assistant" || d["content"] != "" {
		t.Fatalf("first frame %v", frames[0])
	}
	var text, reasoning strings.Builder
	var finish string
	for _, f := range frames {
		if _, ok := f["error"]; ok {
			t.Fatalf("unexpected error frame: %v", f)
		}
		ch, ok := f["choices"].([]any)
		if !ok || len(ch) == 0 {
			continue
		}
		c := ch[0].(map[string]any)
		if fr, ok := c["finish_reason"].(string); ok {
			finish = fr
			continue
		}
		d, _ := c["delta"].(map[string]any)
		if s, ok := d["content"].(string); ok {
			text.WriteString(s)
		}
		if s, ok := d["reasoning"].(string); ok {
			reasoning.WriteString(s)
		}
	}
	if text.String() != "hello" || reasoning.String() != "think" {
		t.Fatalf("text %q reasoning %q", text.String(), reasoning.String())
	}
	if finish != "stop" {
		t.Fatalf("finish_reason %q", finish)
	}
	if !strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]") {
		t.Fatalf("缺少 [DONE]: %s", body)
	}
}

func TestToolCallFirstFrameCarriesIDAndName(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		chunks := []engine.Chunk{
			engine.ToolCallDeltaChunk(0, "call_1", "search", `{"q":`),
			engine.ToolCallDeltaChunk(0, "", "", `"x"}`),
			// block-end 完整帧:js 的 onChunk 没有这个分支(增量已经把
			// arguments 拼齐了),所以它必须被忽略,否则同一个调用发两遍。
			engine.ToolCallBlockEndChunk(0, "call_1", "search", `{"q":"x"}`),
		}
		for _, c := range chunks {
			if err := onChunk(c); err != nil {
				return engine.Outcome{}, err
			}
		}
		return engine.Outcome{ToolCalls: []engine.ToolCall{{ID: "call_1", Name: "search", Arguments: `{"q":"x"}`}}}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)

	var calls []map[string]any
	var finish string
	for _, f := range sseFrames(t, body) {
		ch, ok := f["choices"].([]any)
		if !ok || len(ch) == 0 {
			continue
		}
		c := ch[0].(map[string]any)
		if fr, ok := c["finish_reason"].(string); ok {
			finish = fr
			continue
		}
		d, _ := c["delta"].(map[string]any)
		tc, ok := d["tool_calls"].([]any)
		if !ok {
			continue
		}
		calls = append(calls, tc[0].(map[string]any))
	}
	// js :251-271 每个 tool-call-delta chunk 恒发**两帧**:一帧带 index
	// (首帧另带 id/function.name),delta 非空时再补一帧只带 arguments。
	// 所以两个增量 chunk = 4 帧,其中第 3 帧是第二个 chunk 的 index 帧,
	// 形状就是 `{"index":0}` —— 没有 function。这是 JS 的原样行为,不是
	// 我们把帧发重了;调用方按 index 归并,所以它无害。
	if len(calls) != 4 {
		t.Fatalf("want 4 tool frames (2 chunk × 2 帧), got %d: %v", len(calls), calls)
	}
	if calls[0]["id"] != "call_1" || calls[0]["function"].(map[string]any)["name"] != "search" {
		t.Fatalf("首帧必须带 id 与 name: %v", calls[0])
	}
	if calls[0]["function"].(map[string]any)["arguments"] != "" {
		t.Fatalf("首帧的 arguments 必须是空串: %v", calls[0])
	}
	for i, c := range calls[1:] {
		if _, ok := c["id"]; ok {
			t.Fatalf("增量帧 %d 不该重复 id(OpenAI SDK 会当成多个 tool call): %v", i+1, c)
		}
		if fn, ok := c["function"].(map[string]any); ok {
			if _, ok := fn["name"]; ok {
				t.Fatalf("增量帧 %d 不该重复 name: %v", i+1, c)
			}
		}
	}
	argsOf := func(c map[string]any) string {
		fn, ok := c["function"].(map[string]any)
		if !ok {
			return ""
		}
		s, _ := fn["arguments"].(string)
		return s
	}
	if argsOf(calls[1]) != `{"q":` || argsOf(calls[3]) != `"x"}` {
		t.Fatalf("arguments 增量被拆错: %v", calls)
	}
	if _, ok := calls[2]["function"]; ok {
		t.Fatalf("第二个 chunk 的 index 帧不该带 function: %v", calls[2])
	}
	if finish != "tool_calls" {
		t.Fatalf("finish_reason %q, want tool_calls", finish)
	}
}

func TestUsageFrameCarriesOpenAIShapedUsage(t *testing.T) {
	st := newStub()
	usage := stream.Usage{In: 10, Out: 5, CacheRead: 3, HasUsage: true}
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "hi"}); err != nil {
			return engine.Outcome{}, err
		}
		if err := onChunk(engine.Chunk{Kind: engine.ChunkUsage, Usage: usage}); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{Text: "hi", Usage: usage}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)

	var found map[string]any
	for _, f := range sseFrames(t, body) {
		if u, ok := f["usage"].(map[string]any); ok {
			found = u
			// usage 帧按 js :274 带 choices: [],不是收尾帧。
			if ch, ok := f["choices"].([]any); !ok || len(ch) != 0 {
				t.Fatalf("usage frame choices %v", f["choices"])
			}
		}
	}
	if found == nil {
		t.Fatalf("流里没有 usage 帧: %s", body)
	}
	if found["prompt_tokens"].(float64) != 13 || found["completion_tokens"].(float64) != 5 || found["total_tokens"].(float64) != 18 {
		t.Fatalf("usage %v", found)
	}
	if found["prompt_tokens_details"].(map[string]any)["cached_tokens"].(float64) != 3 {
		t.Fatalf("cached_tokens %v", found["prompt_tokens_details"])
	}
	if found["completion_tokens_details"].(map[string]any)["reasoning_tokens"].(float64) != 0 {
		t.Fatalf("reasoning_tokens %v", found["completion_tokens_details"])
	}
}

func TestContextCancelStopsTheStream(t *testing.T) {
	st := newStub()
	released := make(chan struct{})
	st.complete = func(ctx context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		_ = onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "x"})
		select {
		case <-ctx.Done():
			close(released)
			return engine.Outcome{}, ctx.Err()
		case <-time.After(10 * time.Second):
			return engine.Outcome{}, nil
		}
	}
	ts := st.serve(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rq, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m","stream":true}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	rq.Header.Set("Authorization", "Bearer "+testKey)
	res, err := ts.Client().Do(rq)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	// 等到第一个 data: 行,确认流真的开始了,再掐断。
	if _, err := bufio.NewReader(res.Body).ReadString('\n'); err != nil {
		t.Fatalf("read first line: %v", err)
	}
	cancel()
	_ = res.Body.Close()

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("客户端断开后 Complete 的 ctx 没有被取消 —— 上游请求会一直挂着")
	}
}

// ---- responses 线路 ----

func TestResponsesEndpointAcceptsTheResponsesBody(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, req engine.Request, _ func(engine.Chunk) error) (engine.Outcome, error) {
		if !req.Responses {
			t.Fatalf("responses 线路必须把 Responses 置为 true")
		}
		return engine.Outcome{Text: "hi", Usage: stream.Usage{In: 2, Out: 1, HasUsage: true}}, nil
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/responses", auth(),
		`{"model":"m","input":[{"role":"user","content":"hi"}]}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	got := decode(t, body)
	if got["object"] != "response" || got["status"] != "completed" {
		t.Fatalf("object/status %v/%v", got["object"], got["status"])
	}
	out := got["output"].([]any)
	if len(out) != 1 || out[0].(map[string]any)["type"] != "message" {
		t.Fatalf("output %v", out)
	}
	if out[0].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] != "hi" {
		t.Fatalf("output text %v", out[0])
	}
	if got["usage"].(map[string]any)["input_tokens"].(float64) != 2 {
		t.Fatalf("usage %v", got["usage"])
	}

	st.mu.Lock()
	sent := st.lastReq.OpenAI
	st.mu.Unlock()
	if _, ok := sent["input"]; !ok {
		t.Fatalf("input 形状必须透传: %v", sent)
	}
}

func TestResponsesEndpointFallsBackToMessages(t *testing.T) {
	st := newStub()
	ts := st.serve(t)
	do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	st.mu.Lock()
	sent := st.lastReq.OpenAI
	st.mu.Unlock()
	// js :314 `input: body.input ?? body.messages ?? []`。
	if _, ok := sent["input"]; !ok {
		t.Fatalf("缺 input 时应当回落 messages: %v", sent)
	}
	if _, ok := sent["messages"]; !ok {
		t.Fatalf("原始 messages 也要留着: %v", sent)
	}
}

func TestResponsesEndpointToolCallShape(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{ToolCalls: []engine.ToolCall{{ID: "c1", Name: "search", Arguments: `{}`}, {Name: "echo", Arguments: `{}`}}}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m"}`)
	out := decode(t, body)["output"].([]any)
	if len(out) != 2 {
		t.Fatalf("output %v", out)
	}
	first := out[0].(map[string]any)
	if first["type"] != "function_call" || first["call_id"] != "c1" || first["name"] != "search" {
		t.Fatalf("function_call %v", first)
	}
	if out[1].(map[string]any)["call_id"] != "call_1" {
		t.Fatalf("没有 id 时用 call_<i> 兜底: %v", out[1])
	}
}

func TestResponsesEndpointErrorIs502(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Error: "全部出口都不可用"}, nil
	}
	ts := st.serve(t)
	res, _ := do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m"}`)
	if res.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", res.StatusCode)
	}
}

// TestNotWiredCompleteIs500NotPanic:Config 只给了 ForwardKey 时,handler 必须
// 答 500 而不是 nil 解引用崩掉整个进程。这是"没接线"与"接线了但坏了"的分界。
func TestNotWiredCompleteIs500NotPanic(t *testing.T) {
	srv := New(Config{ForwardKey: func() string { return testKey }})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	// 未接线的 Complete 走的也是 error 返回路径(= JS 的 throw → 顶层 catch),
	// 与差分 B7 的修正一致:这条路径是 500。
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status %d body %s", res.StatusCode, body)
	}
	if !strings.Contains(body, "Complete is not wired") {
		t.Fatalf("未接线的 Complete 必须给出可归因的错误: %s", body)
	}
}

// TestServerTimeoutsAreBounded 是 R1 的钉子:Go 的 http.Server 零值等于无限,
// 一个连上不发头的 slowloris 连接会一直占着 goroutine 和 fd。
//
// 同时钉住 ReadTimeout/WriteTimeout **必须保持 0**:转发端口吐 SSE,一个正常
// 回复可以吐几十秒,整请求死线会把它腰斩 —— 那不是疏忽,是刻意的(空闲截止
// 由 httpclient.NewStreamClient 在响应体上实现)。谁把这两项设上,这个测试红。
func TestServerTimeoutsAreBounded(t *testing.T) {
	srv := New(Config{})
	if srv.http.ReadHeaderTimeout != serverReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", srv.http.ReadHeaderTimeout, serverReadHeaderTimeout)
	}
	if srv.http.IdleTimeout != serverIdleTimeout {
		t.Errorf("IdleTimeout = %v, want %v", srv.http.IdleTimeout, serverIdleTimeout)
	}
	if srv.http.ReadHeaderTimeout <= 0 || srv.http.IdleTimeout <= 0 {
		t.Fatal("两个超时都必须为正:零值就是无限,slowloris 能一直占着连接")
	}
	if srv.http.ReadTimeout != 0 || srv.http.WriteTimeout != 0 {
		t.Fatalf("ReadTimeout=%v WriteTimeout=%v,必须为 0 —— SSE 回复会被整请求死线腰斩",
			srv.http.ReadTimeout, srv.http.WriteTimeout)
	}
}
