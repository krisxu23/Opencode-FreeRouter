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
	if KeyMatches("", "") {
		// 对 JS 的**有意偏离**(js 的 byteLength 0 === 0 为真):空密钥不
		// 匹配任何呈现,包括空呈现 —— GenerateKey 坏熵源时返回空串,这里的
		// 语义必须与 authorized() 的「空串拒绝一切」同向,否则它会成为一个
		// 「拿空 expected 比较就全放行」的陷阱默认。
		t.Fatal("two empty strings must not match: an empty key grants nothing")
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
			Text:  "hi",
			Usage: stream.Usage{In: 10, Out: 5, CacheRead: 3, HasUsage: true},
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

// TestResponsesNonStreamTruncatedIsIncomplete 是协议审计 H1 的钉:非流式
// /v1/responses 被 max_output_tokens 腰斩(out.Truncated)时必须报
// status:"incomplete" + incomplete_details.reason + finish_reason:"length",
// 不能像过去那样硬编码 completed —— 否则被截断的回答与完整回答在线路上完全
// 不可区分,Codex 类非流式客户端永远不知道该续写。chat 线对同一个 Outcome
// 早已映射成 finish_reason:"length"(chatCompletionOnce),两条线现在同判据。
func TestResponsesNonStreamTruncatedIsIncomplete(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Text: "半截回答", Truncated: true}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m","stream":false}`)
	got := decode(t, body)
	if got["status"] != "incomplete" {
		t.Fatalf("status = %v, want incomplete(截断轮绝不能谎报 completed)", got["status"])
	}
	if got["finish_reason"] != "length" {
		t.Errorf("finish_reason = %v, want length", got["finish_reason"])
	}
	det, ok := got["incomplete_details"].(map[string]any)
	if !ok || det["reason"] != "max_output_tokens" {
		t.Errorf("incomplete_details = %v, want {reason: max_output_tokens}", got["incomplete_details"])
	}
}

// TestResponsesNonStreamCompletedHasStopFinishReason 钉住成功轮的对称面:
// 未截断时 status=completed、finish_reason=stop,且**不带** incomplete_details
// (omitempty 在 nil 指针上生效)。
func TestResponsesNonStreamCompletedHasStopFinishReason(t *testing.T) {
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{Text: "完整回答"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m","stream":false}`)
	got := decode(t, body)
	if got["status"] != "completed" || got["finish_reason"] != "stop" {
		t.Fatalf("status/finish = %v/%v, want completed/stop", got["status"], got["finish_reason"])
	}
	if _, present := got["incomplete_details"]; present {
		t.Errorf("成功轮不该有 incomplete_details: %v", got["incomplete_details"])
	}
}

// responsesSSEEvents 把一段 responses SSE 文本拆成 [{type, 原始 JSON}] 行,
// 供形状断言逐事件消费(data: 行按事件边界聚合,与客户端看到的帧一致)。
func responsesSSEEvents(t *testing.T, body string) []map[string]any {
	t.Helper()
	var out []map[string]any
	// 每个 `data:` 就是一帧;本实现的帧不带 event: 行,类型在 JSON 的 type 键里。
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimRight(line, "\r")
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			t.Fatalf("SSE 帧不是 JSON: %q err=%v", data, err)
		}
		out = append(out, ev)
	}
	return out
}

// TestResponsesStreamEventShapePins 是协议审计 M1–M4 的合并钉:走一遍完整的
// responses 流(一个 message 项 + 一个 reasoning 项 + 一个 function_call 项),
// 逐事件核对形状不变量。修复前的具体失败:
//   - M1: output_item.added 的 output_index 在 0 号项上**整个键消失**;
//   - M2: output_text.delta 的 content_index 同理蒸发;
//   - M3: added 的 message 项没有 content:[](reasoning 项没有 summary:[]);
//   - M4: function_call 项的 output_item.done 之前缺 arguments.done。
func TestResponsesStreamEventShapePins(t *testing.T) {
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		// 顺序:先 reasoning(0 号项),再正文(1 号项),最后工具调用(2 号项)。
		if err := onChunk(engine.Chunk{Kind: engine.ChunkReasoning, Text: "想一想"}); err != nil {
			return engine.Outcome{}, err
		}
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "你好"}); err != nil {
			return engine.Outcome{}, err
		}
		if err := onChunk(engine.Chunk{Kind: engine.ChunkToolCallDelta, Index: 0, ToolID: "c1", ToolName: "search", ToolDelta: `{"q":"x"}`}); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{Text: "你好"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/responses", auth(), `{"model":"m","stream":true}`)
	evs := responsesSSEEvents(t, body)

	// 首事件必须是 created,completed 必须收尾。
	if len(evs) == 0 || evs[0]["type"] != "response.created" {
		t.Fatalf("首事件 = %v, 期望 response.created", evs)
	}
	if evs[len(evs)-1]["type"] != "response.completed" {
		t.Fatalf("末事件 = %v, 期望 response.completed", evs[len(evs)-1]["type"])
	}

	byType := map[string][]map[string]any{}
	for _, ev := range evs {
		byType[ev["type"].(string)] = append(byType[ev["type"].(string)], ev)
	}

	// M1:每个 output_item.added 必须带一个**数字** output_index(含 0 号项)。
	added := byType["response.output_item.added"]
	if len(added) != 3 {
		t.Fatalf("added 事件 = %d, want 3(reasoning/message/function_call)", len(added))
	}
	for i, ev := range added {
		idx, ok := ev["output_index"]
		if !ok {
			t.Fatalf("added[%d] 缺 output_index(M1:0 号项的键不能蒸发): %v", i, ev)
		}
		if idx.(float64) != float64(i) {
			t.Errorf("added[%d].output_index = %v, want %d", i, idx, i)
		}
	}

	// M3:message 的 added 带 content:[]、reasoning 的 added 带 summary:[]。
	var msgAdded, reAdded map[string]any
	for _, ev := range added {
		item := ev["item"].(map[string]any)
		switch item["type"] {
		case "message":
			msgAdded = item
		case "reasoning":
			reAdded = item
		}
	}
	if c, ok := msgAdded["content"].([]any); !ok || len(c) != 0 {
		t.Errorf("message.added.content = %v, want [](空数组,不是缺键)", msgAdded["content"])
	}
	if s, ok := reAdded["summary"].([]any); !ok || len(s) != 0 {
		t.Errorf("reasoning.added.summary = %v, want []", reAdded["summary"])
	}

	// M2:每个 output_text.delta 必须带数字 content_index(含 0)。
	for i, ev := range byType["response.output_text.delta"] {
		if _, ok := ev["content_index"]; !ok {
			t.Fatalf("output_text.delta[%d] 缺 content_index(M2): %v", i, ev)
		}
	}

	// M4:function_call 的 output_item.done 之前必须先发 arguments.done,
	// 且 done 的 arguments 已定稿。
	if len(byType["response.function_call_arguments.done"]) != 1 {
		t.Fatalf("function_call_arguments.done 事件数 = %d, want 1(M4 曾被整体省略)",
			len(byType["response.function_call_arguments.done"]))
	}
	// done 事件顺序:arguments.done 必须紧邻在该 function_call 的 item.done 前。
	var argDoneIdx, fcItemDoneIdx = -1, -1
	for i, ev := range evs {
		switch ev["type"] {
		case "response.function_call_arguments.done":
			argDoneIdx = i
		case "response.output_item.done":
			if it, ok := ev["item"].(map[string]any); ok && it["type"] == "function_call" {
				fcItemDoneIdx = i
			}
		}
	}
	if !(argDoneIdx >= 0 && fcItemDoneIdx == argDoneIdx+1) {
		t.Errorf("arguments.done(%d) 必须紧接 function_call 的 item.done(%d)", argDoneIdx, fcItemDoneIdx)
	}
}

// TestJSONResponsesCarryCORS 是协议审计 M5 的钉:OPTIONS 预检承诺了跨源可用,
// 实际 JSON 响应(非流式)必须带 Access-Control-Allow-Origin,否则浏览器在
// 预检通过之后仍把响应拦在 CORS 之外,跨源 harness 连错误体都读不到。
func TestJSONResponsesCarryCORS(t *testing.T) {
	st := newStub()
	ts := st.serve(t)
	// /v1/models 的 200 JSON
	res, _ := do(t, ts, http.MethodGet, "/v1/models", auth(), "")
	if res.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Errorf("/v1/models JSON 缺 ACAO(M5): %v", res.Header)
	}
	// 401 错误体也必须带(客户端要能读到错误形状)
	hdr := map[string]string{"Authorization": "Bearer wrong"}
	res2, _ := do(t, ts, http.MethodPost, "/v1/chat/completions", hdr, `{"model":"m"}`)
	if res2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("错误路径 status = %d, want 401", res2.StatusCode)
	}
	if res2.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Errorf("401 JSON 缺 ACAO(M5): %v", res2.Header)
	}
}

// TestResponsesEndpointFallsBackToMessages 的占位已由上方用例覆盖,保留原测试。

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
// 钉住读死线的两段式与 WriteTimeout=0:ReadTimeout 只覆盖读体阶段(slowloris
// 的第三个洞:发头之后无限慢速喂 body),readBody 读完解除;WriteTimeout 必
// 须保持 0 —— 转发端口吐 SSE,一个正常回复可以吐几十秒,写方向的整请求死线
// 会把它腰斩(空闲截止由 httpclient.NewStreamClient 在响应体上实现)。谁把
// WriteTimeout 设上、或把 ReadTimeout 的解除逻辑删了,这个测试与流式测试都会红。
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
	// ReadTimeout 是两段式的第一段:只管读体阶段(防无限慢速喂 body),
	// readBody 读完即用 ResponseController 解除读死线 —— 不解除的话,
	// net/http 的后台读会在死线到期时取消请求 context,长回答的 SSE 照样
	// 被腰斩。WriteTimeout 必须保持 0:写方向的整请求死线没有解除手段。
	if srv.http.ReadTimeout != serverReadTimeout {
		t.Fatalf("ReadTimeout=%v, want %v(读体阶段的上限)", srv.http.ReadTimeout, serverReadTimeout)
	}
	if srv.http.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout=%v,必须为 0 —— SSE 回复会被整请求死线腰斩", srv.http.WriteTimeout)
	}
}

func TestZeroArgumentToolCallIsForwardedAsStream(t *testing.T) {
	// C1:零参数调用的唯一上行事件是 block-end 完整帧(参数增量为空、
	// toolStart 不发帧)。旧实现无条件丢弃 block-end,客户端收不到任何
	// tool_calls delta 却在收尾看到 finish_reason:"tool_calls" —— SDK
	// 组装出的 assistant 消息没有任何调用,下一轮回放即错。
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		if err := onChunk(engine.ToolCallBlockEndChunk(0, "call_1", "get_time", "{}")); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{ToolCalls: []engine.ToolCall{{ID: "call_1", Name: "get_time", Arguments: "{}"}}}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)

	var sawCall bool
	var args, id, name string
	for _, f := range sseFrames(t, body) {
		ch, ok := f["choices"].([]any)
		if !ok || len(ch) == 0 {
			continue
		}
		c := ch[0].(map[string]any)
		d, _ := c["delta"].(map[string]any)
		if d == nil {
			continue
		}
		tc, ok := d["tool_calls"].([]any)
		if !ok || len(tc) == 0 {
			continue
		}
		call := tc[0].(map[string]any)
		sawCall = true
		id, _ = call["id"].(string)
		fn, _ := call["function"].(map[string]any)
		if fn != nil {
			name, _ = fn["name"].(string)
			args, _ = fn["arguments"].(string)
		}
	}
	if !sawCall {
		t.Fatalf("零参数调用必须以 tool_calls delta 帧出现在流上: %s", body)
	}
	if id != "call_1" || name != "get_time" || args != "{}" {
		t.Fatalf("合成首帧应一次带齐 id/name/完整参数: id=%q name=%q args=%q", id, name, args)
	}
}

func TestUnknownModelMapsTo400(t *testing.T) {
	// I2:Failure.Status=400 的请求错误不该回 500 —— 客户端把「模型名写错」
	// 当服务端故障去重试是纯浪费。400 之外的 Status 不透传(JS 差分 B7 的
	// 500/502 约定不动)。
	st := newStub()
	st.complete = func(context.Context, engine.Request, func(engine.Chunk) error) (engine.Outcome, error) {
		return engine.Outcome{}, frerrors.Failure{Code: check.CodeServer, Status: 400, Message: `unknown model "m"`}
	}
	ts := st.serve(t)
	res, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400: %s", res.StatusCode, body)
	}
}

// ---- chattidy 形状钉测试(移植自 magpie chattidy.go 的教训) ----
//
// magpie 是字节转发架构,要在流上现修四类真实客户端兼容形状;FreeRouter 是
// 解析+重渲染架构,天然不产这些形状 —— 这里的测试把「永不产出」钉死,防止
// 未来的渲染改动悄悄退化。四类形状:
//
//	(1) "reasoning_content":"" / "reasoning":"" 空思考增量 → 一字一行回复
//	(2) "finish_reason":"" 空串 → OpenAI 契约是 null,严格客户端拒绝空串
//	(3) tool-call 增量重复 "name":"" → 覆盖式合并组装出名为 "" 的工具调用
//	(4) "tool_calls":[] 空数组 → 按存在性判断的客户端反复关闭文本块

// streamAssert 沿流断言:每个 delta 帧的形状由 assert 把关,违规即 Fail。
func streamAssert(t *testing.T, body string, assert func(t *testing.T, raw []byte, delta map[string]any, finish any)) {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			continue
		}
		raw := []byte(payload)
		var f struct {
			Choices []struct {
				Delta        map[string]any `json:"delta"`
				FinishReason *string        `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("bad SSE JSON %q: %v", payload, err)
		}
		for _, c := range f.Choices {
			var fin any
			if c.FinishReason != nil {
				fin = *c.FinishReason
			}
			assert(t, raw, c.Delta, fin)
		}
	}
}

func TestChatTidyNeverEmitsEmptyReasoningDelta(t *testing.T) {
	// 形状(1):思考结束后的空 delta 不发帧。客户端把 "reasoning":""
	// 当成一次新的思考开始,回复被拆成一字一行(Qoder 实测)。
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		for _, c := range []engine.Chunk{
			{Kind: engine.ChunkReasoning, Text: "想"},
			{Kind: engine.ChunkReasoning, Text: ""}, // 上游补的空增量
			{Kind: engine.ChunkText, Text: "答"},
		} {
			if err := onChunk(c); err != nil {
				return engine.Outcome{}, err
			}
		}
		return engine.Outcome{Text: "答"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	sawReasoning := false
	streamAssert(t, body, func(t *testing.T, raw []byte, delta map[string]any, _ any) {
		if r, ok := delta["reasoning"]; ok {
			sawReasoning = true
			if s, _ := r.(string); s == "" {
				t.Fatalf("流上出现空 reasoning 增量(chattidy 形状1): %s", raw)
			}
		}
	})
	if !sawReasoning {
		t.Fatalf("非空 reasoning 增量丢失: %s", body)
	}
}

func TestChatTidyFinishReasonIsNeverEmptyString(t *testing.T) {
	// 形状(2):收尾帧的 finish_reason 要么是语义值要么缺省成 null,永远
	// 不发 ""(OpenAI 契约是 null;AI SDK 严格拒绝空串)。
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		// 非流式线 onChunk 是 nil(chatCompletionOnce 不传),先护住。
		if onChunk != nil {
			if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "hi"}); err != nil {
				return engine.Outcome{}, err
			}
		}
		return engine.Outcome{Text: "hi"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	streamAssert(t, body, func(t *testing.T, raw []byte, _ map[string]any, finish any) {
		if s, ok := finish.(string); ok && s == "" {
			t.Fatalf("finish_reason 空串(chattidy 形状2): %s", raw)
		}
	})
	// 非流式同款:choice.finish_reason 不为 ""。
	_, body2 := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m"}`)
	if s, ok := firstChoice(t, decode(t, body2))["finish_reason"].(string); ok && s == "" {
		t.Fatalf("非流式 finish_reason 空串: %s", body2)
	}
}

func TestChatTidyToolCallNameCarriedOnce(t *testing.T) {
	// 形状(3):只有每个调用的首帧带 id+name,后续增量帧绝不重复 —— 覆盖式
	// 合并的客户端(grok CLI)会把重复的 "name":"" 组装成名为 "" 的工具调用,
	// 报 "Tool not found"。(TestToolCallFirstFrameCarriesIDAndName 已断言
	// 帧数与首帧形状;这里从原始字节角度再钉一次「name 只出现一次」。)
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		for _, c := range []engine.Chunk{
			engine.ToolCallDeltaChunk(0, "call_1", "search", `{"q":`),
			engine.ToolCallDeltaChunk(0, "", "", `"x"}`),
		} {
			if err := onChunk(c); err != nil {
				return engine.Outcome{}, err
			}
		}
		return engine.Outcome{ToolCalls: []engine.ToolCall{{ID: "call_1", Name: "search", Arguments: `{"q":"x"}`}}}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	nameFrames := 0
	streamAssert(t, body, func(t *testing.T, raw []byte, delta map[string]any, _ any) {
		tc, ok := delta["tool_calls"].([]any)
		if !ok {
			return
		}
		call := tc[0].(map[string]any)
		fn, _ := call["function"].(map[string]any)
		if fn == nil {
			return
		}
		if n, ok := fn["name"].(string); ok {
			if n == "" {
				t.Fatalf("工具增量帧携带空 name(chattidy 形状3): %s", raw)
			}
			nameFrames++
		}
	})
	if nameFrames != 1 {
		t.Fatalf("name 应只出现在首帧一次, got %d 次: %s", nameFrames, body)
	}
}

func TestChatTidyNoEmptyToolCallsArray(t *testing.T) {
	// 形状(4):delta 里不出现 "tool_calls":[] —— 按存在性(不看内容)判断
	// 的客户端(Qoder)会认为文本块结束了,正文被反复关闭。
	st := newStub()
	st.complete = func(_ context.Context, _ engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
		if err := onChunk(engine.Chunk{Kind: engine.ChunkText, Text: "hi"}); err != nil {
			return engine.Outcome{}, err
		}
		return engine.Outcome{Text: "hi"}, nil
	}
	ts := st.serve(t)
	_, body := do(t, ts, http.MethodPost, "/v1/chat/completions", auth(), `{"model":"m","stream":true}`)
	streamAssert(t, body, func(t *testing.T, raw []byte, delta map[string]any, _ any) {
		if tc, ok := delta["tool_calls"].([]any); ok && len(tc) == 0 {
			t.Fatalf("delta 出现空 tool_calls 数组(chattidy 形状4): %s", raw)
		}
	})
}
