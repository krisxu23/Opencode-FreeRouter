// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package engine

// 任务 18 的 30 条测试表(计划)+ 1 条 mimo 补偿验证。夹具按计划:真实
// health.NewHealth("")(空路径 = 不落盘,每个测试白拿隔离)、catalog 两行、
// 8 节点池、按脚本回状态码的 httptest 上游、RecordTrace 打桩捕获。

import (
	"context"
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
	"freerouter/internal/check"
	"freerouter/internal/errors"
	"freerouter/internal/health"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/stream"
	"freerouter/internal/tracelog"
)

// ---- 伪造上游(与 adapter_test 同一形状;测试包之间不能共享 helper) ----

type captured struct {
	headers http.Header
	body    []byte
}

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

func (f *fakeUpstream) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

// newFakeUpstream 按脚本回应:script 收到请求序号(0 起)。onReq 钩子在脚本
// 之前跑,用来在「请求在途」时改动 health 状态(busy 令牌语义测试)。
func newFakeUpstream(t *testing.T, script func(n int) (int, string, string), onReq ...func(n int)) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		n := len(f.got)
		f.got = append(f.got, captured{headers: r.Header.Clone(), body: readAll(r)})
		f.mu.Unlock()
		for _, hook := range onReq {
			hook(n)
		}
		status, ctype, body := script(n)
		w.Header().Set("Content-Type", ctype)
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func readAll(r *http.Request) []byte {
	raw, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	return raw
}

// sseChat 是 big-pickle(chat 线)的一段成功 SSE:一个文本增量 + [DONE]。
func sseChat(text string) string {
	delta := `{"choices":[{"delta":{"content":"` + text + `"}}]}`
	return "data: " + delta + "\n\ndata: [DONE]\n\n"
}

// sseChatWithUsage 在文本增量之后追加一帧 chat 顶层 usage(ScanUsage 的折叠
// 落点),带 prompt_tokens_details.cached_tokens。
func sseChatWithUsage(text string, prompt, cached, completion int64) string {
	delta := `{"choices":[{"delta":{"content":"` + text + `"}}]}`
	usage := fmt.Sprintf(`{"choices":[],"usage":{"prompt_tokens":%d,"completion_tokens":%d,`+
		`"prompt_tokens_details":{"cached_tokens":%d}}}`, prompt, completion, cached)
	return "data: " + delta + "\n\ndata: " + usage + "\n\ndata: [DONE]\n\n"
}

// sseResponses 是 responses 线(muse-spark 家族)的一段成功 SSE。
func sseResponses(text string) string {
	delta := `{"type":"response.output_text.delta","delta":"` + text + `"}`
	return "data: " + delta + "\n\ndata: [DONE]\n\n"
}

// sseCutAfter 先吐一个字再流内报错:已出内容后的失败(断流)形状。
func sseCutAfter(text, errMsg string) string {
	delta := `{"choices":[{"delta":{"content":"` + text + `"}}]}`
	boom := `{"type":"error","error":{"type":"ConsoleError","message":"` + errMsg + `"}}`
	return "data: " + delta + "\n\ndata: " + boom + "\n\n"
}

const quotaBody = `{"error":{"type":"FreeUsageLimitError","message":"usage limit"}}`
const regionBody = `{"error":{"type":"RegionError","message":"Model is not available in your country"}}`

// ---- 夹具 ----

// pool8 是 8 个节点的候选池,tag 字典序 = Pick 的确定性次序(无健康行时同
// bucket 同 cost 按 tag 排序)。
func pool8() []health.PoolNode {
	pool := make([]health.PoolNode, 0, 8)
	for i := 0; i < 8; i++ {
		pool = append(pool, health.PoolNode{Tag: fmt.Sprintf("node-%d", i), Country: "US"})
	}
	return pool
}

// seedTierB 把 8 个节点都量成 alive + B(受限模型只认 B 出口)。
func seedTierB(h *health.Health) {
	for i := 0; i < 8; i++ {
		tag := fmt.Sprintf("node-%d", i)
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 5, LatencyMin: 5, ExitCountry: "US"})
		h.MarkTierProbe(tag, "available")
	}
}

// seedAliveIPs 把 8 个节点量成 alive 并各带一个独立出口 IP,让 NoteExitBusy
// 真正开始计数(ExitIPOf 为空串时 NoteExitBusy 是 no-op)。
func seedAliveIPs(h *health.Health) {
	for i := 0; i < 8; i++ {
		h.MarkProbe(fmt.Sprintf("node-%d", i), nodeprobe.ProbeResult{
			State: nodeprobe.StateAlive, LatencyMS: 5, LatencyMin: 5,
			ExitIP: fmt.Sprintf("10.0.0.%d", i+1), ExitCountry: "US",
		})
	}
}

type fixture struct {
	t   *testing.T
	h   *health.Health
	up  *fakeUpstream
	eng *Engine

	mu       sync.Mutex
	settings Settings
	catalog  []catalog.Model
	dialFail map[string]bool
	routes   []tracelog.Route
	logs     []string
}

func newFixture(t *testing.T, script func(n int) (int, string, string), onReq ...func(n int)) *fixture {
	t.Helper()
	f := &fixture{
		t:        t,
		h:        health.NewHealth(""),
		catalog:  catalog.Build([]string{"big-pickle", "muse-spark-1.3-contributor-free"}),
		dialFail: map[string]bool{},
		settings: Settings{Countries: []string{"US"}, EffortLevel: "balanced"},
	}
	f.up = newFakeUpstream(t, script, onReq...)
	f.eng = NewEngine(Deps{
		State: func() State {
			f.mu.Lock()
			defer f.mu.Unlock()
			return State{Catalog: f.catalog, Health: f.h}
		},
		Pool:     func() []health.PoolNode { return pool8() },
		Settings: func() Settings { return f.settings },
		Dialer: func(tag string) (*http.Client, error) {
			f.mu.Lock()
			fail := f.dialFail[tag]
			f.mu.Unlock()
			if fail {
				return nil, fmt.Errorf("engine test: no dialer for tag %q", tag)
			}
			return f.up.srv.Client(), nil
		},
		AdapterDeps: adapter.Deps{
			Base:      f.up.srv.URL,
			MaxTokens: 0, // 设置缺省 max_tokens:回落目录行自己的上限
		},
		RecordTrace: func(r tracelog.Route) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.routes = append(f.routes, r)
		},
		Log: func(msg string) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.logs = append(f.logs, msg)
		},
	})
	return f
}

func (f *fixture) routesN() []tracelog.Route {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]tracelog.Route, len(f.routes))
	copy(out, f.routes)
	return out
}

func (f *fixture) logsN() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.logs))
	copy(out, f.logs)
	return out
}

func (f *fixture) setMaxAttempts(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settings.MaxAttempts = n
}

func (f *fixture) setCatalog(rows []catalog.Model) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalog = rows
}

func (f *fixture) failDial(tag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dialFail[tag] = true
}

// simpleReq 是最普通的请求:单条 user 消息、会话键 user。
func simpleReq(model, user string) Request {
	return Request{Model: model, OpenAI: map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		"user":     user,
	}}
}

// toolTurnReq 的消息以 role:"tool" 结尾(withinToolTurn 的判据)。
func toolTurnReq(model, user string) Request {
	return Request{Model: model, OpenAI: map[string]any{
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
			map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{
				"id": "c1", "type": "function",
				"function": map[string]any{"name": "lookup", "arguments": "{}"},
			}}},
			map[string]any{"role": "tool", "tool_call_id": "c1", "content": "result"},
		},
		"user": user,
	}}
}

// failAtText 的 onChunk:收到第一个文本增量就报错(客户端断开的替身)。
func failAtText() func(Chunk) error {
	return func(c Chunk) error {
		if c.Kind == ChunkText {
			return fmt.Errorf("client gone")
		}
		return nil
	}
}

func wantFailure(t *testing.T, err error) errors.Failure {
	t.Helper()
	if err == nil {
		t.Fatalf("Complete 应当失败,却返回了成功")
	}
	f, ok := err.(errors.Failure)
	if !ok {
		t.Fatalf("错误应是 errors.Failure,得到 %T: %v", err, err)
	}
	return f
}

// ---- 主循环:轮换三分支 ----

func TestCompleteRotatesOnAPreContentFailure(t *testing.T) {
	script := func(n int) (int, string, string) {
		if n == 0 {
			return 451, "application/json", regionBody
		}
		return 200, "text/event-stream", sseChat("ok")
	}
	f := newFixture(t, script)
	out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err != nil {
		t.Fatalf("换出口后应成功: %v", err)
	}
	if out.Text != "ok" {
		t.Fatalf("Text = %q, want ok", out.Text)
	}
	routes := f.routesN()
	if len(routes) != 1 {
		t.Fatalf("Route 记录应恰一条,得到 %d", len(routes))
	}
	tries := routes[0].Tries
	if len(tries) != 2 {
		t.Fatalf("Tries 应两条,得到 %d: %+v", len(tries), tries)
	}
	if tries[0].Code != check.CodeRegion {
		t.Fatalf("第一条 Try 的 code = %q, want %q", tries[0].Code, check.CodeRegion)
	}
	if tries[0].Served {
		t.Fatalf("会前失败不应标 served")
	}
	if tries[1].Code != "ok" {
		t.Fatalf("第二条 Try 的 code = %q, want ok", tries[1].Code)
	}
}

func TestCompleteNeverRotatesAfterContent(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseCutAfter("A", "boom")
	})
	out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err != nil {
		t.Fatalf("断流应以 Outcome.Error 报告而不是 error: %v", err)
	}
	if out.Text != "A" {
		t.Fatalf("已转发的前缀必须保留: Text = %q, want A", out.Text)
	}
	if out.Error == "" {
		t.Fatalf("断流必须带 Outcome.Error")
	}
	tries := f.routesN()[0].Tries
	if len(tries) != 1 {
		t.Fatalf("出内容后不得换出口: Tries = %d 条", len(tries))
	}
	if !tries[0].Served {
		t.Fatalf("断流那条 Try 应标 served")
	}
}

func TestCompleteStopsAtMaxAttempts(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 429, "application/json", quotaBody
	})
	f.setMaxAttempts(4)
	_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	fail := wantFailure(t, err)
	if fail.Code != check.CodeQuota {
		t.Fatalf("code = %q, want %q", fail.Code, check.CodeQuota)
	}
	if fail.Status != 503 {
		t.Fatalf("放弃路径应回 503,得到 %d", fail.Status)
	}
	if got := f.up.count(); got != 4 {
		t.Fatalf("恰好 4 次尝试,得到 %d", got)
	}
	if tries := f.routesN()[0].Tries; len(tries) != 4 {
		t.Fatalf("Tries 应 4 条,得到 %d", len(tries))
	}
}

func TestEmptyResponseHasItsOwnBudget(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", "" // 空体 2xx → CodeEmpty
	})
	_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	fail := wantFailure(t, err)
	if fail.Code != check.CodeEmpty {
		t.Fatalf("code = %q, want %q", fail.Code, check.CodeEmpty)
	}
	if got := f.up.count(); got != 6 {
		t.Fatalf("EMPTY 的独立额度是 6 次,得到 %d 次(不是扫池的 20)", got)
	}
}

func TestSessionIDNeverChangesAcrossExits(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 429, "application/json", quotaBody
	})
	f.setMaxAttempts(6)
	_, _ = f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	reqs := f.up.requests()
	if len(reqs) != 6 {
		t.Fatalf("应 6 次尝试,得到 %d", len(reqs))
	}
	first := reqs[0].headers.Get("x-opencode-session")
	if first == "" {
		t.Fatalf("上游应收到会话头")
	}
	for i, req := range reqs {
		if got := req.headers.Get("x-opencode-session"); got != first {
			t.Fatalf("第 %d 次尝试的会话 id 变了: %q != %q(伪造新 session 会让上游把退避当成新用户)", i, got, first)
		}
	}
}

func TestUnknownModelIsNotRetried(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		t.Errorf("未知模型不应发出任何上游请求")
		return 200, "text/event-stream", sseChat("nope")
	})
	_, err := f.eng.Complete(context.Background(), simpleReq("gpt-4", "u1"), nil)
	fail := wantFailure(t, err)
	if fail.Code != check.CodeServer || fail.Status != 400 {
		t.Fatalf("unknown model 应 (SERVER, 400),得到 (%s, %d)", fail.Code, fail.Status)
	}
	if got := f.up.count(); got != 0 {
		t.Fatalf("上游被请求了 %d 次, want 0", got)
	}
	if routes := f.routesN(); len(routes) != 0 {
		t.Fatalf("进不了主循环的请求不应有 Route 记录,得到 %d", len(routes))
	}
}

func TestUnknownFreeLaneModelGetsACatalogRow(t *testing.T) {
	// muse-spark 家族走 responses 线,响应体按那条线的形状给。
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseResponses("ok")
	})
	seedTierB(f.h) // muse-spark 家族是受限模型,只认 B 出口
	out, err := f.eng.Complete(context.Background(), simpleReq("muse-spark-1.9-contributor-free", "u1"), nil)
	if err != nil {
		t.Fatalf("未知免费 id 应建目录行并跑完: %v", err)
	}
	if out.Text != "ok" {
		t.Fatalf("Text = %q, want ok", out.Text)
	}
	routes := f.routesN()
	if len(routes) != 1 || routes[0].Model != "muse-spark-1.9-contributor-free" {
		t.Fatalf("Route = %+v, want Model muse-spark-1.9-contributor-free", routes)
	}
}

func TestNonRetryableFailureThrowsImmediately(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 401, "application/json", `{"error":{"message":"bad key"}}`
	})
	_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	fail := wantFailure(t, err)
	if fail.Code != check.CodeCredential {
		t.Fatalf("code = %q, want %q", fail.Code, check.CodeCredential)
	}
	if got := f.up.count(); got != 1 {
		t.Fatalf("不可重试失败不得扫池: 上游被请求 %d 次, want 1", got)
	}
}

// ---- health 记账:quota / region / cooldown / busy ----

func TestQuotaOnlyDemotesNeverCools(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		if n == 0 {
			return 429, "application/json", quotaBody
		}
		return 200, "text/event-stream", sseChat("ok")
	})
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("第一请求应成功: %v", err)
	}
	if snap := f.h.CooldownSnapshot(); len(snap) != 0 {
		t.Fatalf("429 只降级不冷却: CooldownSnapshot = %+v", snap)
	}
	// 全新会话、无粘性:node-0 必须仍出现在候选快照里 —— 429 只是把它
	// 降级(Throttled),从不排除(Pick 只重排、永不排除)。
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u2"), nil); err != nil {
		t.Fatalf("第二请求应成功: %v", err)
	}
	order := f.routesN()[1].Order
	found := false
	for _, row := range order {
		if row.Tag == "node-0" {
			found = true
			if !row.Throttled {
				t.Fatalf("node-0 刚撞过配额墙,应带 Throttled 降级标记: %+v", order)
			}
		}
	}
	if !found {
		t.Fatalf("node-0 应仍在候选快照(只降级、不排除): %+v", order)
	}
}

func TestRegionFailureDoesNotCoolTheNode(t *testing.T) {
	script := func(n int) (int, string, string) {
		if n == 0 {
			return 451, "application/json", regionBody
		}
		return 200, "text/event-stream", sseChat("ok")
	}
	f := newFixture(t, script)
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if snap := f.h.CooldownSnapshot(); len(snap) != 0 {
		t.Fatalf("cooldownOn 不含 region: CooldownSnapshot = %+v", snap)
	}
}

func TestTransportFailureCoolsTheNode(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	f.failDial("node-0")
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("拨号失败应换出口成功: %v", err)
	}
	snap := f.h.CooldownSnapshot()
	if _, ok := snap["node-0"]; !ok {
		t.Fatalf("拨号被拒(transport)应冷却该节点: %+v", snap)
	}
}

func assertAllBusyZero(t *testing.T, f *fixture) {
	t.Helper()
	for i := 1; i <= 8; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		if n := f.h.ExitBusyCount(ip); n != 0 {
			t.Fatalf("出口 IP %s 的在途计数 = %d, want 0(每条路径都要归还)", ip, n)
		}
	}
}

func TestExitBusyIsReleasedOnEveryPath(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := newFixture(t, func(n int) (int, string, string) {
			return 200, "text/event-stream", sseChat("ok")
		})
		seedAliveIPs(f.h)
		if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
			t.Fatalf("应成功: %v", err)
		}
		assertAllBusyZero(t, f)
	})
	t.Run("giveup-503", func(t *testing.T) {
		f := newFixture(t, func(n int) (int, string, string) {
			return 429, "application/json", quotaBody
		})
		seedAliveIPs(f.h)
		f.setMaxAttempts(2)
		if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err == nil {
			t.Fatalf("应当 503")
		}
		assertAllBusyZero(t, f)
	})
	t.Run("onChunk-error", func(t *testing.T) {
		f := newFixture(t, func(n int) (int, string, string) {
			return 200, "text/event-stream", sseChat("ok")
		})
		seedAliveIPs(f.h)
		out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), failAtText())
		if err != nil {
			t.Fatalf("onChunk 错误按断流处理,不走 error 通道: %v", err)
		}
		if out.Error == "" {
			t.Fatalf("onChunk 错误应进 Outcome.Error")
		}
		assertAllBusyZero(t, f)
	})
}

func TestExitBusyTokenIsTheIPAtAcquireTime(t *testing.T) {
	// 请求在途时探测轮把 node-0 量到新 IP:release 若重新解析就会还错对象
	// (旧 IP 永久泄漏一条计数,新 IP 被多减一次)。钩子在夹具构造之后挂上,
	// 因为闭包引用 f 而 f 还没初始化完成。
	var onFirstRequest func(n int)
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	}, func(n int) {
		if onFirstRequest != nil {
			onFirstRequest(n)
		}
	})
	onFirstRequest = func(n int) {
		if n == 0 {
			f.h.MarkProbe("node-0", nodeprobe.ProbeResult{
				State: nodeprobe.StateAlive, LatencyMS: 5, LatencyMin: 5,
				ExitIP: "10.9.9.9", ExitCountry: "US",
			})
		}
	}
	seedAliveIPs(f.h)
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if n := f.h.ExitBusyCount("10.0.0.1"); n != 0 {
		t.Fatalf("acquire 时刻的令牌 IP 10.0.0.1 未归还: count = %d", n)
	}
	if n := f.h.ExitBusyCount("10.9.9.9"); n != 0 {
		t.Fatalf("release 后的新 IP 不应有计数: count = %d", n)
	}
}

// ---- 粘性 ----

func TestStickyIsRecordedForTheSession(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if got := f.h.ExitForSession("forward:u1"); got != "node-0" {
		t.Fatalf("ExitForSession = %q, want node-0", got)
	}
}

func TestWithinTurnSkipsStickyUsageRegrading(t *testing.T) {
	body := sseChatWithUsage("ok", 5000, 2000, 10)
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", body
	})
	// 轮内(消息以 tool 结果收尾):成功但不得用 usage 重新定档 —— 轮内追加
	// 的内容必然不在上一轮的前缀里,拿本轮的命中去改档会在缓存最值钱的时刻
	// 踢掉出口。
	if _, err := f.eng.Complete(context.Background(), toolTurnReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("轮内请求应成功: %v", err)
	}
	if ttl, ok := f.h.StickyTTL("forward:u1"); !ok || ttl != 30*60*1000 {
		t.Fatalf("轮内成功不得改档: TTL = %d/%v, want 基线 %d", ttl, ok, 30*60*1000)
	}
	// 对照组:非轮内的同一会话,usage(cacheRead=2000 ≥ 1024)把 TTL 放大到 2h。
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("非轮内请求应成功: %v", err)
	}
	if ttl, ok := f.h.StickyTTL("forward:u1"); !ok || ttl != 2*60*60*1000 {
		t.Fatalf("非轮内成功应按 cacheRead 放大 TTL: %d/%v, want %d", ttl, ok, 2*60*60*1000)
	}
}

func TestStickyFailsTwiceThenRotates(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		if n == 0 {
			return 429, "application/json", quotaBody
		}
		return 200, "text/event-stream", sseChat("ok")
	})
	// 预置:会话粘在 node-0,且已有一次 sticky 连败(上一条请求在同一 sticky
	// 上会前失败)。本请求第一次尝试再吃一次失败即连跪两次 → 换出口。
	f.h.NoteSticky("forward:u1", "node-0", false)
	f.h.NoteStickyFailure("forward:u1")
	out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err != nil {
		t.Fatalf("第二次应换出口成功: %v", err)
	}
	if out.Text != "ok" {
		t.Fatalf("Text = %q, want ok", out.Text)
	}
	tries := f.routesN()[0].Tries
	if len(tries) < 2 {
		t.Fatalf("应至少两次尝试: %+v", tries)
	}
	if tries[0].Tag != "node-0" {
		t.Fatalf("第一次尝试应仍在 sticky 上: %q", tries[0].Tag)
	}
	if tries[1].Tag == "node-0" {
		t.Fatalf("连跪两次后必须换出口,第二次还在 %q", tries[1].Tag)
	}
	if f.h.StickyBurned("forward:u1") {
		t.Fatalf("成功后熔断分应清零")
	}
}

// ---- 结构化路由记录 ----

func TestTraceRecordsOncePerRequest(t *testing.T) {
	script := func(n int) (int, string, string) {
		switch n {
		case 0:
			return 429, "application/json", quotaBody
		case 1:
			return 451, "application/json", regionBody
		}
		return 200, "text/event-stream", sseChat("ok")
	}
	f := newFixture(t, script)
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if routes := f.routesN(); len(routes) != 1 {
		t.Fatalf("每请求恰一条 Route,得到 %d(幂等在 trace 内,不在调用点)", len(routes))
	}
}

func TestTraceKeepsTheFirstOrderSnapshot(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		if n < 2 {
			return 429, "application/json", quotaBody
		}
		return 200, "text/event-stream", sseChat("ok")
	})
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	route := f.routesN()[0]
	if len(route.Tries) != 3 {
		t.Fatalf("Tries 应 3 条,得到 %d", len(route.Tries))
	}
	if len(route.Order) == 0 || route.Order[0].Tag != "node-0" {
		t.Fatalf("Order 应是第 1 次候选快照(首行 node-0),得到 %+v", route.Order)
	}
	if len(route.Order) != 8 {
		t.Fatalf("第一次快照应含全部 8 个候选,得到 %d", len(route.Order))
	}
}

func TestNoRotationPrintsNoSummaryLine(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	if logs := f.logsN(); len(logs) != 0 {
		t.Fatalf("没轮换就不打人类可读汇总: %+v", logs)
	}
	routes := f.routesN()
	if len(routes) != 1 || routes[0].Result != "成功" {
		t.Fatalf("无轮换也要落一条结构化记录: %+v", routes)
	}
}

func TestRotationSummaryElidesTheMiddle(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 429, "application/json", quotaBody
	})
	_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err == nil {
		t.Fatalf("全池 429 应当失败")
	}
	logs := f.logsN()
	if len(logs) != 1 {
		t.Fatalf("汇总日志恰一行,得到 %d: %+v", len(logs), logs)
	}
	msg := logs[0]
	if !strings.Contains(msg, "出口轮换 8 次后池子扫完") {
		t.Fatalf("汇总行形状不对: %q", msg)
	}
	if !strings.Contains(msg, "…2个…") {
		t.Fatalf("8 次轮换应折叠中段 2 个: %q", msg)
	}
	for _, keep := range []string{"node-0", "node-1", "node-2", "node-3", "node-6", "node-7"} {
		if !strings.Contains(msg, keep) {
			t.Fatalf("汇总行应含首4/尾2 中的 %s: %q", keep, msg)
		}
	}
	for _, elided := range []string{"node-4", "node-5"} {
		if strings.Contains(msg, elided) {
			t.Fatalf("中段 %s 应被折叠: %q", elided, msg)
		}
	}
}

func TestSummaryHasNoDoubleSpaceWhenNothingRotated(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 401, "application/json", `{"error":{"message":"bad key"}}`
	})
	_, _ = f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	logs := f.logsN()
	if len(logs) != 1 {
		t.Fatalf("不可重试路径也打一行汇总,得到 %d: %+v", len(logs), logs)
	}
	msg := logs[0]
	if strings.Contains(msg, "  ") {
		t.Fatalf("汇总行不应有任何连续空格(JS 版缺陷): %q", msg)
	}
	if !strings.Contains(msg, ": 共") {
		t.Fatalf("汇总行应保留 ': 共' 段: %q", msg)
	}
}

// ---- shortTag / 拨号失败 / ModelRows / usage 形状 ----

func TestShortTagDropsFlagsAndWhitespace(t *testing.T) {
	got := shortTag("🇺🇸  Node A")
	if got != "NodeA" {
		t.Fatalf("shortTag = %q, want NodeA", got)
	}
	if strings.ContainsRune(got, '@') {
		t.Fatalf("零端口版不接 @后缀: %q", got)
	}
	long := shortTag("🇩🇪 Example-Air-DE_3")
	if runes := []rune(long); len(runes) > 12 {
		t.Fatalf("应截到 12 字符: %q", long)
	}
	if strings.ContainsAny(long, "@ \t") {
		t.Fatalf("不应含 @ 或空白: %q", long)
	}
}

func TestUnknownExitIsDialedAndFailsFast(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	f.failDial("node-0")
	out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err != nil {
		t.Fatalf("未知 tag 按传输失败换出口: %v", err)
	}
	tries := f.routesN()[0].Tries
	if len(tries) != 2 {
		t.Fatalf("应恰两次尝试: %+v", tries)
	}
	if tries[0].Code != check.CodeTransport {
		t.Fatalf("dialer 错误应记 TRANSPORT,得到 %q", tries[0].Code)
	}
	if tries[0].MS > 2000 {
		t.Fatalf("拨号失败应毫秒级返回,得到 %dms", tries[0].MS)
	}
	if out.Text != "ok" {
		t.Fatalf("Text = %q, want ok", out.Text)
	}
}

func TestModelRowsHidesModelsWithNoBExit(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) { return 200, "text/event-stream", sseChat("ok") })
	rows := f.eng.ModelRows()
	if len(rows) != 1 || rows[0].ID != "big-pickle" {
		t.Fatalf("无 B 出口时 muse-spark 应隐藏、big-pickle 应在: %+v", rows)
	}
	if rows[0].Object != "model" || rows[0].OwnedBy != "lite-gateway" || rows[0].Created == 0 {
		t.Fatalf("Row 形状不对: %+v", rows[0])
	}
}

func TestModelRowsHidesNonFreeModels(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) { return 200, "text/event-stream", sseChat("ok") })
	seedTierB(f.h) // muse-spark 需 B 出口才会被列出
	f.setCatalog(append(catalog.Build([]string{"big-pickle", "muse-spark-1.3-contributor-free"}),
		catalog.Model{ID: "gpt-4", Name: "GPT 4", MaxOutput: 32768, Reasoning: true, CanDisableThinking: true}))
	rows := f.eng.ModelRows()
	for _, row := range rows {
		if row.ID == "gpt-4" {
			t.Fatalf("非 free lane 的行不得出现在 /v1/models: %+v", rows)
		}
	}
	if len(rows) != 2 {
		t.Fatalf("big-pickle 与 muse-spark(有 B 出口时)都应在: %+v", rows)
	}
}

func TestUsageIsConvertedToOpenAIShape(t *testing.T) {
	got := OpenAIUsage(stream.Usage{In: 100, CacheRead: 20, Out: 5, HasUsage: true})
	if got["prompt_tokens"] != int64(120) {
		t.Fatalf("prompt_tokens = %v, want 120(input+cacheRead)", got["prompt_tokens"])
	}
	details, ok := got["prompt_tokens_details"].(map[string]any)
	if !ok || details["cached_tokens"] != int64(20) {
		t.Fatalf("prompt_tokens_details.cached_tokens = %v, want 20", got["prompt_tokens_details"])
	}
	if got["completion_tokens"] != int64(5) {
		t.Fatalf("completion_tokens = %v, want 5", got["completion_tokens"])
	}
	if got["total_tokens"] != int64(125) {
		t.Fatalf("total_tokens = %v, want 125", got["total_tokens"])
	}
}

// ---- chunk 折叠(translate 层) ----

func TestTruncatedToolCallsAreDropped(t *testing.T) {
	var out Outcome
	foldChunks(ToolCallDeltaChunk(0, "c1", "search", `{"q":"x`), &out)
	foldChunks(ToolCallDeltaChunk(1, "c2", "echo", `{}`), &out)
	foldChunks(Chunk{Kind: ChunkFinish, Finish: FinishMaxTokens}, &out)
	if !out.Truncated {
		t.Fatalf("FinishMaxTokens 应置 Truncated")
	}
	if len(out.ToolCalls) != 2 {
		t.Fatalf("过滤前应两个调用: %+v", out.ToolCalls)
	}
	dropBrokenToolCalls(&out)
	if len(out.ToolCalls) != 1 || out.ToolCalls[0].ID != "c2" {
		t.Fatalf("半截 JSON 的调用应被丢弃: %+v", out.ToolCalls)
	}
}

func TestToolDeltasFoldByIndex(t *testing.T) {
	var out Outcome
	foldChunks(ToolCallDeltaChunk(0, "c1", "search", `{"`), &out)
	foldChunks(ToolCallDeltaChunk(0, "", "", `q":1`), &out)
	foldChunks(ToolCallDeltaChunk(0, "", "", `}`), &out)
	foldChunks(ToolCallDeltaChunk(1, "c2", "echo", `{}`), &out)
	if len(out.ToolCalls) != 2 {
		t.Fatalf("同一 Index 的增量应归并: %+v", out.ToolCalls)
	}
	first := out.ToolCalls[0]
	if first.ID != "c1" || first.Name != "search" || first.Arguments != `{"q":1}` {
		t.Fatalf("arguments 应首尾相接: %+v", first)
	}
}

func TestBlockEndToolCallMatchesByID(t *testing.T) {
	var out Outcome
	// 增量先建出 id=c1 的条目
	foldChunks(ToolCallDeltaChunk(0, "c1", "search", `{"a":1}`), &out)
	// block-end 用 block.id 命中 → 合并(不产生第二个条目)
	foldChunks(ToolCallBlockEndChunk(7, "c1", "search", `{"a":1}`), &out)
	if len(out.ToolCalls) != 1 {
		t.Fatalf("命中的 block-end 不得追加: %+v", out.ToolCalls)
	}
	// 不命中 → 追加新条目
	foldChunks(ToolCallBlockEndChunk(8, "c2", "echo", `{}`), &out)
	if len(out.ToolCalls) != 2 {
		t.Fatalf("未命中的 block-end 应追加: %+v", out.ToolCalls)
	}
	if out.ToolCalls[1].ID != "c2" || out.ToolCalls[1].Arguments != "{}" {
		t.Fatalf("追加条目形状不对: %+v", out.ToolCalls[1])
	}
}

// ---- 并发 ----

func TestConcurrentCompletesDoNotMixRouteRecords(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	const sessions, perSession = 4, 3
	var wg sync.WaitGroup
	for s := 0; s < sessions; s++ {
		for r := 0; r < perSession; r++ {
			wg.Add(1)
			go func(s, r int) {
				defer wg.Done()
				user := fmt.Sprintf("s%d", s)
				req := simpleReq("big-pickle", user)
				if r%2 == 1 {
					req = toolTurnReq("big-pickle", user)
				}
				if _, err := f.eng.Complete(context.Background(), req, nil); err != nil {
					t.Errorf("并发请求失败: %v", err)
				}
			}(s, r)
		}
	}
	wg.Wait()
	routes := f.routesN()
	if len(routes) != sessions*perSession {
		t.Fatalf("应 %d 条 Route,得到 %d", sessions*perSession, len(routes))
	}
	within, plain := 0, 0
	for _, route := range routes {
		if route.Model != "big-pickle" {
			t.Fatalf("Route 混入了别的请求的模型: %+v", route)
		}
		if len(route.Tries) != 1 || route.Tries[0].Code != "ok" {
			t.Fatalf("Route 的 tries 被混入了他请求的结果: %+v", route.Tries)
		}
		if route.WithinTurn {
			within++
		} else {
			plain++
		}
	}
	if within != sessions || plain != sessions*(perSession-1) {
		t.Fatalf("WithinTurn 标记与请求不对应: within=%d plain=%d", within, plain)
	}
}

// ---- mimo 家族补偿(修正案 §3 任务 17 条) ----

func TestMimoAlwaysThinkingCeilingIsDoubled(t *testing.T) {
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	f.setCatalog(catalog.Build([]string{"big-pickle", "mimo-v2.6-flash-free"}))
	// mimo-v2.6(canDisableThinking=false):balanced 的 8192 上限应翻倍成 16384。
	if _, err := f.eng.Complete(context.Background(), simpleReq("mimo-v2.6-flash-free", "u1"), nil); err != nil {
		t.Fatalf("mimo 请求应成功: %v", err)
	}
	// 对照:big-pickle(可关思考)同档保持 8192。
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u2"), nil); err != nil {
		t.Fatalf("big-pickle 请求应成功: %v", err)
	}
	reqs := f.up.requests()
	if len(reqs) != 2 {
		t.Fatalf("应捕获两次请求,得到 %d", len(reqs))
	}
	var got []int64
	for _, req := range reqs {
		var body map[string]any
		if err := json.Unmarshal(req.body, &body); err != nil {
			t.Fatalf("请求体不是 JSON: %v", err)
		}
		if v, ok := body["max_tokens"].(float64); ok {
			got = append(got, int64(v))
		}
	}
	if len(got) != 2 {
		t.Fatalf("两次请求体都应带 max_tokens: %v", got)
	}
	if got[0] != 16384 {
		t.Fatalf("mimo balanced 预算 = %d, want 16384(8192 × ALWAYS_THINKING_FACTOR,src/effort.js:74)", got[0])
	}
	if got[1] != 8192 {
		t.Fatalf("big-pickle balanced 预算 = %d, want 8192", got[1])
	}
}
