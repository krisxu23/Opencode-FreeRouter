// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"freerouter/internal/adapter"
	"freerouter/internal/catalog"
	"freerouter/internal/engine"
	"freerouter/internal/forward"
	"freerouter/internal/health"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/tracelog"
)

// 这一组是**跨层接线**的验收位。forward 的测试面故意只有一个可编程的 Complete 桩
// (那样才能穷尽它的分支),engine 的测试面只到自己的 Outcome,adapter 的只到
// Delta —— 于是「客户端发了 tools ⇒ 上游真的收到工具 ⇒ 回流的 tool_calls 真的到达
// 客户端」这条链在三个包里各测一半,合起来那一段从来没有一次被跑通过。B13
// (三条线丢弃全部 tool-call 与 reasoning 增量 ⇒ function calling 完全不可用)
// 正是藏在这个缝里:每一层单测都绿,端到端是断的。审计 T16 的全分支评审把这个
// 缺位补在 app —— app 是唯一的接线包。
//
// 这里不启动 sing-box:被测对象是 engine→adapter→forward 的接线本身,出站拨号
// 由 sbx 包自己的测试覆盖。

const e2eKey = "ofm-e2e-key"

type e2eUpstream struct {
	mu      sync.Mutex
	got     []map[string]any
	replies func(n int) (int, string, string)
}

func (f *e2eUpstream) requests() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.got))
	copy(out, f.got)
	return out
}

// chatFrame 拼一帧 chat 增量;arguments 里的引号交给 json.Marshal,不手工转义。
func chatFrame(t *testing.T, delta map[string]any, finish string) string {
	t.Helper()
	choice := map[string]any{"index": 0, "delta": delta}
	if finish != "" {
		choice["finish_reason"] = finish
	}
	body, err := json.Marshal(map[string]any{"choices": []any{choice}})
	if err != nil {
		t.Fatalf("frame: %v", err)
	}
	return "data: " + string(body) + "\n\n"
}

func toolCallFrames(t *testing.T) string {
	first := chatFrame(t, map[string]any{"tool_calls": []any{map[string]any{
		"index": 0, "id": "call_ls", "type": "function",
		"function": map[string]any{"name": "bash", "arguments": `{"cmd":"ls"`},
	}}}, "")
	second := chatFrame(t, map[string]any{"tool_calls": []any{map[string]any{
		"index":    0,
		"function": map[string]any{"arguments": `,"cmd":"ls -la"}`},
	}}}, "")
	usage := `data: {"choices":[],"usage":{"prompt_tokens":120,"completion_tokens":7,` +
		`"prompt_tokens_details":{"cached_tokens":100}}}` + "\n\n"
	// **故意不发 finish_reason**:收尾的 tool_calls 必须由「折出了调用」这一路推出来。
	return first + second + usage + "data: [DONE]\n\n"
}

func newE2E(t *testing.T, replies func(n int) (int, string, string)) (*e2eUpstream, *engine.Engine) {
	t.Helper()
	f := &e2eUpstream{replies: replies}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var parsed map[string]any
		_ = json.Unmarshal(raw, &parsed)
		f.mu.Lock()
		n := len(f.got)
		f.got = append(f.got, parsed)
		f.mu.Unlock()
		status, ctype, payload := f.replies(n)
		w.Header().Set("Content-Type", ctype)
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, payload)
	}))
	t.Cleanup(srv.Close)

	h := health.NewHealth("")
	for i, tag := range []string{"node-0", "node-1"} {
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 5, LatencyMin: 5,
			ExitIP: fmt.Sprintf("10.0.0.%d", i+1), ExitCountry: "US"})
	}
	eng := engine.NewEngine(engine.Deps{
		State: func() engine.State {
			return engine.State{Catalog: catalog.Build([]string{"big-pickle"}), Health: h}
		},
		Pool: func() []health.PoolNode {
			return []health.PoolNode{{Tag: "node-0", Country: "US"}, {Tag: "node-1", Country: "US"}}
		},
		Settings:    func() engine.Settings { return engine.Settings{Countries: []string{"US"}} },
		Dialer:      func(tag string) (*http.Client, error) { return srv.Client(), nil },
		AdapterDeps: adapter.Deps{Base: srv.URL},
		RecordTrace: func(tracelog.Route) {},
		Log:         func(string) {},
	})
	return f, eng
}

func postChat(t *testing.T, eng *engine.Engine, body string) string {
	t.Helper()
	srv := forward.New(forward.Config{
		Enabled:    func() bool { return true },
		ForwardKey: func() string { return e2eKey },
		Complete:   eng.Complete,
		ModelRows:  eng.ModelRows,
		Log:        func(string) {},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("content-type", "application/json")
	req.Header.Set("authorization", "Bearer "+e2eKey)
	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.StatusCode != 200 {
		t.Fatalf("status = %d, body = %s", res.StatusCode, raw)
	}
	return string(raw)
}

const toolsBody = `{"model":"big-pickle","stream":true,"messages":[{"role":"user","content":"list files"}],` +
	`"tools":[{"type":"function","function":{"name":"bash","description":"run a command",` +
	`"parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}}]}`

// joinToolArgs 按 SDK 的做法把 SSE 流里的 tool_calls 增量拼回参数串:按
// tool_calls[].index 累积 `function.arguments`。顺带钉住两条下游形状契约:
//   - 首帧必须带 id 与非空 name(client 靠它认出这是哪个调用);
//   - 除首帧外的每一帧都必须带 `function`(一个只有 index 的帧对客户端没有任何
//     信息,JS 的 forward.js:260-269 也从不发这种帧)。
func joinToolArgs(t *testing.T, sse string) string {
	t.Helper()
	byIndex := map[float64]string{}
	var firstID string
	var firstName any
	seen := map[float64]bool{}
	for _, frame := range strings.Split(sse, "\n\n") {
		line := strings.TrimPrefix(strings.TrimSpace(frame), "data: ")
		if line == "" || line == "[DONE]" {
			continue
		}
		var evt map[string]any
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			t.Fatalf("解不开下行帧 %q: %v", line, err)
		}
		choices, _ := evt["choices"].([]any)
		for _, raw := range choices {
			ch, _ := raw.(map[string]any)
			delta, _ := ch["delta"].(map[string]any)
			calls, _ := delta["tool_calls"].([]any)
			for _, rawCall := range calls {
				call, _ := rawCall.(map[string]any)
				idx, ok := call["index"].(float64)
				if !ok {
					t.Fatalf("tool_calls 条目没有数值 index: %v", call)
				}
				if id, _ := call["id"].(string); id != "" {
					firstID = id
					fn, _ := call["function"].(map[string]any)
					firstName = fn["name"]
				} else if !seen[idx] {
					t.Fatalf("第 %v 号调用在第一帧之后到达,却没带 id: %v", idx, call)
				}
				fn, hasFn := call["function"].(map[string]any)
				if !seen[idx] && !hasFn {
					t.Fatalf("调用的首帧必须带 function(JS 也是 {id, function:{name,arguments:\"\"}}): %v", call)
				}
				if hasFn {
					if s, _ := fn["arguments"].(string); s != "" {
						byIndex[idx] += s
					}
				}
				seen[idx] = true
			}
		}
	}
	if firstID == "" {
		t.Fatal("流里没有任何带 id 的 tool_call 帧")
	}
	if name, _ := firstName.(string); name != "bash" {
		t.Fatalf("函数名 = %q, want bash", name)
	}
	if len(byIndex) != 1 {
		t.Fatalf("tool call 条数 = %d, want 1(同一 index 的两段要拼在一起): %v", len(byIndex), byIndex)
	}
	for _, joined := range byIndex {
		return joined
	}
	return ""
}

// TestEndToEndToolCallReachesTheClient 是 B13 的端到端验收:客户端带 tools 发一次
// 流式请求 ⇒ 上游必须真的收到 tools(且 tool_choice 不许被打成 "none")⇒ 上游回流的
// tool_calls 必须带着 id/name 与拼接完整的 arguments 到达客户端,收尾必须是
// "finish_reason":"tool_calls"。
func TestEndToEndToolCallReachesTheClient(t *testing.T) {
	up, eng := newE2E(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", toolCallFrames(t)
	})
	frames := postChat(t, eng, toolsBody)

	// ① 增量本身到了客户端。
	if !strings.Contains(frames, `"tool_calls"`) {
		t.Fatalf("客户端没有收到任何 tool_calls 帧:\n%s", frames)
	}
	if !strings.Contains(frames, `"id":"call_ls"`) || !strings.Contains(frames, `"name":"bash"`) {
		t.Fatalf("tool_calls 帧缺 id/name(call 身份在链上丢了):\n%s", frames)
	}
	// ② 两段 arguments 必须按到达顺序拼回一个完整 JSON。
	//
	// 形状不是"一帧带完整参数":archive/node/src/forward.js:252-270 就是两帧发
	// —— 首帧带 id/name 与空 arguments,随后一帧只带这一段的增量。Go 逐字照抄
	// (forward_test.go 的 TestToolCallFirstFrameCarriesIDAndName 钉着它),所以
	// 这里断言的是**拼接结果**,而不是某一帧的内容。
	if got := joinToolArgs(t, frames); got != `{"cmd":"ls","cmd":"ls -la"}` {
		t.Fatalf("arguments 拼回来是 %q, want {\"cmd\":\"ls\",\"cmd\":\"ls -la\"}:\n%s", got, frames)
	}
	// ③ 收尾:上游没发 finish token,收尾的 tool_calls 只能由「折出了调用」推出。
	if !strings.Contains(frames, `"finish_reason":"tool_calls"`) {
		t.Fatalf("收尾不是 tool_calls(finish 判定链断在某处):\n%s", frames)
	}
	// ④ 出向闸门:工具定义要真的上线,tool_choice 不许被强设 none。
	reqs := up.requests()
	if len(reqs) != 1 {
		t.Fatalf("上游收到 %d 个请求, want 1", len(reqs))
	}
	if tools, _ := reqs[0]["tools"].([]any); len(tools) == 0 {
		t.Fatalf("出向 tools 丢了:上游没收到任何工具定义")
	}
	if tc, ok := reqs[0]["tool_choice"].(string); ok && tc == "none" {
		t.Fatalf("tool_choice 被强设成 none,模型将永远不回工具调用")
	}
}

// TestEndToEndTextStreamStillWorks 是同一根链路的对照组:纯文本流不能因为工具链
// 的改造而变形(正文、收尾 stop、usage 三段都要在)。
func TestEndToEndTextStreamStillWorks(t *testing.T) {
	_, eng := newE2E(t, func(n int) (int, string, string) {
		return 200, "text/event-stream",
			chatFrame(t, map[string]any{"content": "hello"}, "") +
				chatFrame(t, map[string]any{}, "stop") +
				`data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":3}}` + "\n\n" +
				"data: [DONE]\n\n"
	})
	frames := postChat(t, eng, `{"model":"big-pickle","stream":true,`+
		`"messages":[{"role":"user","content":"hi"}]}`)
	if !strings.Contains(frames, `"content":"hello"`) {
		t.Fatalf("正文没到客户端:\n%s", frames)
	}
	if !strings.Contains(frames, `"finish_reason":"stop"`) {
		t.Fatalf("收尾 stop 丢了:\n%s", frames)
	}
	if !strings.Contains(frames, `"total_tokens":13`) {
		t.Fatalf("usage 没上行:\n%s", frames)
	}
}
