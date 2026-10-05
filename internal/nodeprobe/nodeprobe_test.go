// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package nodeprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"freerouter/internal/httpclient"
)

// TestProbeAllReportsEveryItemOnce 对照 tests/nodeprobe.test.js 第 1 条：12 个必
// 失败的 item，每项恰好一条结果、State 全为 dead。拨号器立刻报错而不是挂起，
// 所以即便走默认预算（12s × 2 次）也能瞬间失败。
func TestProbeAllReportsEveryItemOnce(t *testing.T) {
	refused := httpclient.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		return nil, errors.New("dial: connection refused")
	})
	items := make([]Item, 12)
	for i := range items {
		items[i] = Item{Tag: fmt.Sprintf("n%02d", i), Dial: refused}
	}
	p := NewProber(nil)
	res := p.ProbeAll(context.Background(), items, 3)
	if len(res) != len(items) {
		t.Fatalf("got %d results, want %d", len(res), len(items))
	}
	seen := map[string]int{}
	for i, r := range res {
		if r.Tag != items[i].Tag {
			t.Fatalf("result[%d].Tag = %q, want %q（结果必须与输入同序）", i, r.Tag, items[i].Tag)
		}
		seen[r.Tag]++
		if r.Result.State != StateDead {
			t.Errorf("item %s: State = %q, want %q", r.Tag, r.Result.State, StateDead)
		}
	}
	for tag, n := range seen {
		if n != 1 {
			t.Errorf("item %s reported %d times, want exactly once", tag, n)
		}
	}
}

func TestGateURLIsHTTPS(t *testing.T) {
	// 闸门探测打的是主链路真实上游，明文会话会被门户/中间盒改写——https 是底线。
	if !strings.HasPrefix(GateURL, "https://") {
		t.Errorf("GateURL = %q, want https prefix", GateURL)
	}
}

// TestLivenessURLsAreDistinctAndAtLeastFour 对照 tests/nodeprobe.test.js 第 3 条。
// 「可注册域」用最后两段近似（不引公共后缀库）：IP 字面量（1.1.1.1 是刻意的
// DNS 污染免疫源，见 src/nodeprobe.js:48 注释）没有可注册域，它自己就算一个
// 网络主体。断言的是"至少 4 个互不相同的主体"，不是精确的后缀表。
func TestLivenessURLsAreDistinctAndAtLeastFour(t *testing.T) {
	if len(LivenessURLs) < 4 {
		t.Fatalf("liveness 目标太少（%d），主体冗余不足", len(LivenessURLs))
	}
	hosts := make([]string, 0, len(LivenessURLs))
	for _, u := range LivenessURLs {
		if !strings.HasPrefix(u, "https://") {
			t.Errorf("liveness must be https-only: %s", u)
		}
		parsed, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad liveness url %s: %v", u, err)
		}
		hosts = append(hosts, parsed.Hostname())
	}
	seenHost := map[string]bool{}
	for _, h := range hosts {
		if seenHost[h] {
			t.Errorf("liveness host 重复: %s", h)
		}
		seenHost[h] = true
	}
	principals := map[string]bool{}
	for _, h := range hosts {
		if net.ParseIP(h) != nil {
			principals[h] = true // IP 字面量自己就是一个主体
			continue
		}
		labels := strings.Split(h, ".")
		if len(labels) > 2 {
			labels = labels[len(labels)-2:]
		}
		principals[strings.Join(labels, ".")] = true
	}
	if len(principals) < 4 {
		t.Errorf("至少要有 4 个不同网络主体，实际 %d: %v", len(principals), principals)
	}
	// echo 源必须全 https：出口 IP 不得明文出境（src/nodeprobe.js:69-72）。
	if len(EchoURLs) < 3 {
		t.Fatalf("echo 候选太少（%d），单点故障时拿不到出口国家", len(EchoURLs))
	}
	for _, u := range EchoURLs {
		if !strings.HasPrefix(u, "https://") {
			t.Errorf("echo must be https-only: %s", u)
		}
	}
	for _, want := range []string{"https://api.ip.sb/geoip", "https://ipwho.is/", "https://ifconfig.co/json"} {
		if !slices.Contains(EchoURLs, want) {
			t.Errorf("缺少已验证可用的 echo 源 %s", want)
		}
	}
}

// TestDeadProbeReturnsFast 对照 tests/nodeprobe.test.js 第 5 条：拨号器指向
// 127.0.0.1:1（覆盖传入的 addr），本机必然立刻 ECONNREFUSED——dead 判定不许
// 等满堆叠的超时。
func TestDeadProbeReturnsFast(t *testing.T) {
	loopbackRefused := httpclient.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, "127.0.0.1:1")
	})
	p := NewProber(nil)
	start := time.Now()
	res := p.ProbeNode(context.Background(), loopbackRefused, 3000, 2)
	elapsed := time.Since(start)
	if res.State != StateDead {
		t.Fatalf("State = %q, want dead", res.State)
	}
	if elapsed >= 15*time.Second {
		t.Fatalf("dead verdict took %v, want < 15s（不许等满堆叠超时）", elapsed)
	}
}

// TestOptionsPerItem 钉住 probeAll 的逐项 options（JS 第 6 条 optionsOf 语义）：
// 每个 item 用自己的 TimeoutMS。拨号器阻塞到 ctx.Done 为止——ctx 上的期限就是
// http.Client 从 Options.TimeoutMS 算出的超时，阻塞时长即逐项证据。
func TestOptionsPerItem(t *testing.T) {
	p := NewProber(nil)
	p.liveness = []string{"http://probe.test/hang"}
	p.gate = ""
	var mu sync.Mutex
	blocked := map[string]time.Duration{}
	mkDial := func(tag string) httpclient.Dialer {
		return func(ctx context.Context, network, addr string) (net.Conn, error) {
			start := time.Now()
			<-ctx.Done()
			mu.Lock()
			blocked[tag] = time.Since(start)
			mu.Unlock()
			return nil, ctx.Err()
		}
	}
	items := []Item{
		{Tag: "short", Dial: mkDial("short"), Options: ProbeOptions{TimeoutMS: 400, Attempts: 1}},
		{Tag: "long", Dial: mkDial("long"), Options: ProbeOptions{TimeoutMS: 1500, Attempts: 1}},
	}
	res := p.ProbeAll(context.Background(), items, 2)
	for _, r := range res {
		if r.Result.State != StateDead {
			t.Errorf("item %s: State = %q, want dead", r.Tag, r.Result.State)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, tag := range []string{"short", "long"} {
		if _, ok := blocked[tag]; !ok {
			t.Fatalf("item %s was never probed", tag)
		}
	}
	if blocked["short"] >= 900*time.Millisecond {
		t.Errorf("short item waited %v, want ~400ms（没有用上自己的 Options）", blocked["short"])
	}
	if blocked["long"] < 1200*time.Millisecond {
		t.Errorf("long item waited %v, want ~1500ms（short 的预算串到了 long 上）", blocked["long"])
	}
}

// hangListener 起一个"只接受连接、永不回话"的 TCP 服务：客户端的每一次尝试
// 都必须等满自己的超时预算（对照 JS 版 attempts 预算用例的 hang server）。
func hangListener(t *testing.T) net.Addr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu     sync.Mutex
		conns  []net.Conn
		closed bool
	)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				c.Close()
				continue
			}
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		closed = true
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		_ = ln.Close()
	})
	return ln.Addr()
}

// TestAttemptsBudget 对照 tests/nodeprobe.test.js 第 7 条：attempts=1 只花一个
// 超时预算（连败节点的降级路径），attempts=2 必须真的等满两个预算。
func TestAttemptsBudget(t *testing.T) {
	addr := hangListener(t)
	dial := httpclient.Dialer(func(ctx context.Context, network, a string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, network, addr.String())
	})
	p := NewProber(nil)
	p.liveness = []string{"http://" + addr.String() + "/"}
	p.gate = ""

	t1 := time.Now()
	one := p.ProbeNode(context.Background(), dial, 800, 1)
	d1 := time.Since(t1)
	if one.State != StateDead {
		t.Fatalf("attempts=1: State = %q, want dead", one.State)
	}
	if d1 >= 1500*time.Millisecond {
		t.Fatalf("attempts=1 took %v, want < 1500ms（降级路径没有省下预算）", d1)
	}

	t2 := time.Now()
	two := p.ProbeNode(context.Background(), dial, 800, 2)
	d2 := time.Since(t2)
	if two.State != StateDead {
		t.Fatalf("attempts=2: State = %q, want dead", two.State)
	}
	if d2 < 1500*time.Millisecond {
		t.Fatalf("attempts=2 took %v, want >= 1500ms（两个 800ms 预算必须等满）", d2)
	}
}

// slowServer 返回一个按请求序号给固定延迟的服务：第 1 发延迟 first，之后延迟
// second。ProbeNode 每轮尝试只打一发（测试里把 liveness 覆盖成单 URL、关掉
// 闸门与 echo），序号因此是确定的。
func slowServer(t *testing.T, first, second time.Duration) *httptest.Server {
	t.Helper()
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := first
		if atomic.AddInt32(&n, 1) > 1 {
			d = second
		}
		time.Sleep(d)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestMultiSegmentBodySurvivesFetchVia 是 H1 的回归钉:fetchVia 曾在
// RoundTrip 返回时就 cancel shot ctx,于是「判决与排干都发生在响应体上」的
// 两个消费者(gateVerdict 的 ReadCapped、echoJudge 的 ReadCapped)在 body
// 被拆成多段时恒读不完 —— 实测症状是 context canceled,后果是「到得了上游
// 但到不了 Cloudflare」这类**本应存活**的节点每轮被判 dead,exitIp 大面积空。
//
// 判据用真实链路(ProbeNode),不直接调 fetchVia:把 liveness 源指到一个必败
// 地址,让**闸门**成为 stage-1 里唯一的胜者,这样 gateVerdict 必然执行;闸门
// 体写成一份 300KB 的模型清单并**强制多段**(先 Flush 头段、再逐块 Flush,
// 且关掉 Content-Length 让传输走分块),echo 同理。修复前这里 State=dead;
// 修复后 State=alive 且 ExitIP 非空(两个消费者都读完了各自的多段体)。
func TestMultiSegmentBodySurvivesFetchVia(t *testing.T) {
	mux := http.NewServeMux()
	// 闸门:200 + 一份 data 数组,但故意拆成很多小段刷出去,逼出多段 body。
	mux.HandleFunc("/gate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		// 无 Content-Length → net/http 走 chunked,body 一定会被拆成多段。
		_, _ = io.WriteString(w, `{"data":[`)
		filler := strings.Repeat("x", 4096)
		for i := 0; i < 80; i++ { // ~320KB,远超单段
			if i > 0 {
				_, _ = io.WriteString(w, ",")
			}
			_, _ = io.WriteString(w, `{"id":"`+filler+`"}`)
			if f, ok := w.(http.Flusher); ok {
				f.Flush() // 每一段都刷,确保分段
			}
		}
		_, _ = io.WriteString(w, `]}`)
	})
	// echo:小 body,但同样分两段刷,验证 echoJudge 的 ReadCapped 也读完。
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"ip":"1.2.`)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = io.WriteString(w, `3.4","country_code":"US"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p := NewProber(nil)
	// liveness 指向一个必败端口:闸门因此是 stage-1 的唯一候选胜者。
	p.liveness = []string{"http://127.0.0.1:1/live"} // 端口 1 拒连
	p.gate = srv.URL + "/gate"
	p.echo = []string{srv.URL + "/echo"}

	res := p.ProbeNode(context.Background(), nil, 5000, 1)
	if res.State != StateAlive {
		t.Fatalf("State = %q, want alive —— 多段闸门体没能读完就是 H1 复发(gateVerdict 被 cancel 掐死会判 dead)", res.State)
	}
	if res.ExitIP != "1.2.3.4" {
		t.Errorf("ExitIP = %q, want 1.2.3.4 —— echo 的多段体必须被 echoJudge 完整读出", res.ExitIP)
	}
}

// TestSuccessfulProbeCollectsMinLatency（Go 特有）钉住"多轮采样取最小值"：
// 50/200 → LatencyMS=50 且 LatencyMin=50；200/50 → LatencyMS=200 且 LatencyMin=50。
// 这是 JS 版修不了的历史缺陷（样本按全程累计计时，min 恒等于首样本；Go 按每轮
// 独立计时），测试用区间断言吸收调度抖动，语义靠区间边界钉死。
// URL 覆盖通过 Prober 的字段做（同包测试直接赋值，见 nodeprobe.go 的注释），
// 不为测试改包级常量。
func TestSuccessfulProbeCollectsMinLatency(t *testing.T) {
	newP := func(srv *httptest.Server) *Prober {
		p := NewProber(nil)
		p.liveness = []string{srv.URL}
		p.gate = ""
		p.echo = nil
		return p
	}

	// 第一组：50ms 然后 200ms → 首样本 50，最小值也是 50。
	p1 := newP(slowServer(t, 50*time.Millisecond, 200*time.Millisecond))
	r1 := p1.ProbeNode(context.Background(), nil, 5000, 2)
	if r1.State != StateAlive {
		t.Fatalf("State = %q, want alive", r1.State)
	}
	if r1.LatencyMS < 45 || r1.LatencyMS > 150 {
		t.Errorf("LatencyMS = %d, want ~50（首样本）", r1.LatencyMS)
	}
	if r1.LatencyMin < 45 || r1.LatencyMin > 150 || r1.LatencyMin > r1.LatencyMS {
		t.Errorf("LatencyMin = %d, want ~50 且 <= LatencyMS(%d)", r1.LatencyMin, r1.LatencyMS)
	}

	// 第二组：200ms 然后 50ms → 首样本 200，最小值是 50（JS 版在这里给不出 50）。
	p2 := newP(slowServer(t, 200*time.Millisecond, 50*time.Millisecond))
	r2 := p2.ProbeNode(context.Background(), nil, 5000, 2)
	if r2.State != StateAlive {
		t.Fatalf("State = %q, want alive", r2.State)
	}
	if r2.LatencyMS < 150 || r2.LatencyMS > 400 {
		t.Errorf("LatencyMS = %d, want ~200（首样本）", r2.LatencyMS)
	}
	if r2.LatencyMin < 45 || r2.LatencyMin > 150 {
		t.Errorf("LatencyMin = %d, want ~50（min 必须真的取最小）", r2.LatencyMin)
	}
}

// TestUpstreamOnlyContract 钉住 1.3.0 持续健康监测探针(UpstreamOnly)的三条
// 契约。入口必须走 ProbeAll —— UpstreamOnly 的分叉在 probeOne 里,直接调
// ProbeNode 走的是全三段(含 echo),测不到这条车道。
//  1. 单发闸门成功即 alive,且 LatencyMin == LatencyMS(只有一发,没有第二
//     个样本可取 min);
//  2. **绝不打 echo** —— 出口 IP 是首探(全量三段)的职责;热区 60s 一轮的
//     频率下打第三方 echo 源会把它打成我们自己的热点。echo 指向一个「敢被
//     请求就报错」的陷阱服务器,ExitIP 必须为空。
//  3. 取消的一轮报 unknown 而不是 dead(取消 ≠ 判决)。
func TestUpstreamOnlyContract(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/gate", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("user-agent"); got != GateProbeUA {
			t.Errorf("闸门那发必须用主链路 UA, got %q", got)
		}
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"m"}]}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	echoTrap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("UpstreamOnly 打了 echo —— 热区 pass 必须单发,echo 是首探的职责")
	}))
	t.Cleanup(echoTrap.Close)

	p := NewProber(nil)
	p.liveness = []string{"http://127.0.0.1:1/live"} // 单发模式不该用到它
	p.gate = srv.URL + "/gate"
	p.echo = []string{echoTrap.URL + "/echo"}

	res := p.ProbeAll(context.Background(), []Item{{
		Tag:     "hot-node",
		Options: ProbeOptions{TimeoutMS: 3000, Attempts: 2, UpstreamOnly: true},
	}}, 1)
	if len(res) != 1 {
		t.Fatalf("got %d results, want 1", len(res))
	}
	r := res[0].Result
	if r.State != StateAlive {
		t.Fatalf("State = %q, want alive", r.State)
	}
	if r.LatencyMin != r.LatencyMS {
		t.Errorf("LatencyMin(%d) != LatencyMS(%d): 单发没有第二个样本可取 min", r.LatencyMin, r.LatencyMS)
	}
	if r.ExitIP != "" {
		t.Errorf("ExitIP = %q, want 空: UpstreamOnly 不做 echo", r.ExitIP)
	}

	// 契约 3:闸门挂起、外层 ctx 取消 → unknown。
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	t.Cleanup(hang.Close)
	p2 := NewProber(nil)
	p2.gate = hang.URL + "/gate"
	p2.echo = nil
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	res2 := p2.ProbeAll(ctx, []Item{{
		Tag:     "slow",
		Options: ProbeOptions{TimeoutMS: 5000, Attempts: 1, UpstreamOnly: true},
	}}, 1)
	if res2[0].Result.State != StateUnknown {
		t.Fatalf("取消轮 State = %q, want unknown(取消 ≠ 判决)", res2[0].Result.State)
	}
}

// TestFullProbeDoesUseEcho 是上一条契约 2 的**对照组**:上面那个 echo 陷阱
// 安静,可能只是因为 echo 根本连不通(夹具假象),而不是 UpstreamOnly 省掉了
// 它。这里跑**非** UpstreamOnly 的全量三段,echo 指向一个真会应答的源,断言
// ExitIP 非空 —— 证明 echo 这条路径本身是通的。两边的差异因此只能来自
// UpstreamOnly 那一个开关,陷阱的沉默才是有效判决。
func TestFullProbeDoesUseEcho(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/gate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"data":[{"id":"m"}]}`)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.WriteString(w, `{"ip":"203.0.113.7","country_code":"US"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	p := NewProber(nil)
	p.liveness = []string{srv.URL + "/gate"}
	p.gate = srv.URL + "/gate"
	p.echo = []string{srv.URL + "/echo"}

	res := p.ProbeAll(context.Background(), []Item{{
		Tag:     "full-node",
		Options: ProbeOptions{TimeoutMS: 3000, Attempts: 1}, // 无 UpstreamOnly
	}}, 1)
	if got := res[0].Result.ExitIP; got != "203.0.113.7" {
		t.Fatalf("全量三段的 ExitIP = %q, want 203.0.113.7 —— echo 不可达时,UpstreamOnly 那条对照测试的沉默毫无意义", got)
	}
}

// stubbornConn 是「不响应取消」的那一类连接：头部照发，之后 Read 永久阻塞，
// Close / SetDeadline 都是空操作。它钉的是 easy-proxies 记录过的协议形状 ——
// 拨号能回来，但请求体永远不落地，客户端的 cancel 与 client.Timeout 都无法让
// 这一发收口（只有传输层的 close 能，而这一层不理会）。
//
// R4:Read 必须**跨多次调用**把假头交清、之后永久阻塞(select{})。这条桩换过
// 三版，每一版都是被真实 Transport 的读法打回来的：
//   - v1 `copy(p, …)` 不看 p 有多长：空闲探活 Peek(1) 只取走 1 字节，剩下 63
//     字节留在桩里；下一次请求复用同一连接读到残缺头，readResponse 报
//     unexpected EOF、readLoop 进 peekFailLocked，那一发以
//     readLoopPeekFailLocked 收场(echo 合法失败 → alive)。约 10% 概率。
//   - v2 「头一次性交清」+「buf 小于头长就报错」：把概率 flake 变成了**必然
//     的错误路径** —— 只要 Transport 某次用小于 64 字节的 buf 调 Read，桩当场
//     返回错误，echo 请求在毫秒级合法失败、判成 alive，而用例断言的是 unknown
//     (2026-10-05 CI 上就是这样红的：`--- FAIL: TestIncompleteIsNotDead (0.01s)`)。
//   - v3 现在这版：不猜调用方 buf 有多大，把假头当成字节流，谁要多少给多少，
//     交完才永久阻塞。Read 只能返回 (n>0, nil) 或永远不返回，没有第三条路 ——
//     用例要钉的「拨号后读不回来、Close 也无人理会」才是唯一可能的形状。
type stubbornConn struct {
	sent int // 已交付的假头字节数(必须指针接收者：值接收者会从头重发)
}

var stubbornHead = []byte("HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")

func (c *stubbornConn) Read(p []byte) (int, error) {
	if c.sent < len(stubbornHead) {
		n := copy(p, stubbornHead[c.sent:])
		c.sent += n
		return n, nil // n 可能为 0(len(p)==0)：合法退化，不动状态、不阻塞
	}
	select {} // 永久阻塞：头发完了就永远不返回，也不理会 Close
}

func (*stubbornConn) Write(p []byte) (int, error)      { return len(p), nil }
func (*stubbornConn) Close() error                     { return nil }
func (*stubbornConn) LocalAddr() net.Addr              { return stubAddr{} }
func (*stubbornConn) RemoteAddr() net.Addr             { return stubAddr{} }
func (*stubbornConn) SetDeadline(time.Time) error      { return nil }
func (*stubbornConn) SetReadDeadline(time.Time) error  { return nil }
func (*stubbornConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr struct{}

func (stubAddr) Network() string { return "stub" }
func (stubAddr) String() string  { return "stub" }

// TestStubbornConnDeliversHeadAcrossPartialReads 钉住 R4 桩的契约，直接对
// 2026-10-05 的 CI 红灯：调用方 buf 多小都不许出错。v2 桩对 len(p) < 头长
// 返回 error，于是任何一次小 buf 读都会让 echo 请求在毫秒级合法失败、探针以
// alive 收场，而 TestIncompleteIsNotDead 断言的是 unknown —— 表现为那条用例
// 在 0.01s 内 FAIL（本地跑十次未必复现，CI 2 核必现）。
func TestStubbornConnDeliversHeadAcrossPartialReads(t *testing.T) {
	c := &stubbornConn{}
	var got []byte
	for len(got) < len(stubbornHead) {
		buf := make([]byte, 1) // 一次只要 1 字节：最坏的分片形状
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("已读 %d/%d 字节后出错: %v", len(got), len(stubbornHead), err)
		}
		if n != 1 {
			t.Fatalf("n = %d, want 1（已读 %d 字节）", n, len(got))
		}
		got = append(got, buf[0])
	}
	if string(got) != string(stubbornHead) {
		t.Fatalf("逐字节读到的假头 = %q, want %q", got, stubbornHead)
	}
	// 混合形状：先 3 字节、再一次性 4KB，合起来必须仍是完整假头。
	c2 := &stubbornConn{}
	head := make([]byte, 3)
	if n, err := c2.Read(head); err != nil || n != 3 {
		t.Fatalf("小 buf 读: n=%d err=%v, want 3, nil", n, err)
	}
	if string(head) != string(stubbornHead[:3]) {
		t.Fatalf("前 3 字节 = %q, want %q", head, stubbornHead[:3])
	}
	rest := make([]byte, 4096)
	n, err := c2.Read(rest)
	if err != nil {
		t.Fatalf("大 buf 读剩下: %v", err)
	}
	if string(rest[:n]) != string(stubbornHead[3:]) {
		t.Fatalf("剩余 = %q, want %q", rest[:n], stubbornHead[3:])
	}
	// 头交完必须永久阻塞：50ms 内返回即失败（Close/Deadline 都是空操作）。
	done := make(chan int, 1)
	go func() {
		n, _ := c2.Read(make([]byte, 4096))
		done <- n
	}()
	select {
	case n := <-done:
		t.Fatalf("头交完后 Read 返回了 n=%d，必须永久阻塞", n)
	case <-time.After(50 * time.Millisecond):
	}
}

// hangServer 的 /live 秒回 204，/echo 永远不回（等客户端自己取消）：stage-1
// 快速通过、echo 挂满 8s 的 echoBudgetMS。
//
// 注意：兜底预算（attempts×timeoutMs+echoBudgetMS+backstopSlackMS）按构造
// **大于**这个形状的最坏耗时，所以这里撞不出兜底——NodeProbe 会等 echo 走完
// 再以 alive 收场。兜底要靠真·挂死（拨号器不响应 ctx）才触发，见
// TestBackstopFiresOnAnUncancellableDialer。
func hangServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done(): // 客户端取消即收尾，不留悬挂 handler
		case <-time.After(30 * time.Second):
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// hangPair 是 hangServer 的两实例版：/live 一个监听端口，/echo 另一个。分端口是
// 必需的 —— 同一 host:port 上 stage-1 与 echo 会复用同一条连接，echo 段压根不
// 发起新拨号，「挂死不响应 ctx」的形状就构造不出来。
func hangPair(t *testing.T) (live, echoSrv *httptest.Server) {
	t.Helper()
	liveMux := http.NewServeMux()
	liveMux.HandleFunc("/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	live = httptest.NewServer(liveMux)
	t.Cleanup(live.Close)

	echoMux := http.NewServeMux()
	echoMux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	})
	echoSrv = httptest.NewServer(echoMux)
	t.Cleanup(echoSrv.Close)
	return live, echoSrv
}

// TestIncompleteIsNotDead（Go 特有）：必触发 backstop 的配置下，结果是
// unknown 且 Incomplete，绝不是 dead——dead 会给可能健康的节点记一次连败。
//
// 「必触发 backstop」只能用**真·挂死**的拨号器构造：底层拨号不响应 ctx 取消时，
// 请求 ctx 的 deadline 也救不了它。hangServer 那条形状（服务器不回、客户端能
// 取消）走的是 shot ctx 期限，兜底按构造晚于它，所以那里拿到的是 alive ——
// 这正是兜底预算必须 ≥ attempts×timeoutMs+echoBudget 的理由。
func TestIncompleteIsNotDead(t *testing.T) {
	live, echoSrv := hangPair(t)
	// stage-1 走 live 的真实拨号；echo 指向**另一个**监听端口，所以必然触发一次
	// 新拨号 —— 那一发永久挂起、故意不看 ctx（easy-proxies 记录过的那种协议）。
	// 注意不能把两段放在同一 host:port 上：连接会被复用，压根没有第二次拨号，
	// 用例就退化成「客户端自己的 shot ctx 能取消」的形状，兜底自然不会触发。
	// 拨号器收到的 addr 是 host:port：echo 源与 live 源分端口，所以按 addr 判定
	// 「这一发是 echo 段」，比数拨号次数更贴合真实形状（一次请求可能重拨多次）。
	var echoDials atomic.Int64
	dial := httpclient.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		if strings.HasSuffix(addr, strings.TrimPrefix(echoSrv.URL, "http://")) {
			// 只有「拨号后读不回来、Close 也无人理会」才是真的不响应取消：
			// 光让 DialContext 挂起不够 —— client.Timeout 的定时器会让 do() 提前
			// 返回（它并不需要拨号结束），那一发照样收口。
			echoDials.Add(1)
			return &stubbornConn{}, nil
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	})
	p := NewProber(nil)
	p.liveness = []string{live.URL + "/live"}
	p.gate = ""
	p.echo = []string{echoSrv.URL + "/echo"}
	// 余量必须给「响应头」留够时间：echo 这一发能撞到兜底，靠的是**头已经
	// 到了、判决层卡在读 body 上**（stubbornConn 的 Close 与各 deadline 都是
	// 空操作）。若 echo 的单发预算先到期，transport 会放弃这一发、ProbeNode
	// 立刻以 alive 收场 —— 用例就退化成「客户端自己能取消」的形状。
	//
	// R4:5000ms 只是「别让自己的调度把 echo 头掐了」的保险，判定权在兜底。
	// 2026-10-05 的 CI 红灯不是这里的余量不够，而是桩把「buf 小于头长」当错误
	// 返回（见 stubbornConn 注释）—— 探针 10ms 就拿到确定判决，压根没等预算。
	p.echoBudgetMS = 5000   // echo 单发预算：只要头在这之内到达即可
	p.backstopSlackMS = 500 // 兜底 ≈ 100 + 5000 + 500
	items := []Item{{Tag: "slow", Dial: dial, Options: ProbeOptions{TimeoutMS: 100, Attempts: 1}}}
	res := p.ProbeAll(context.Background(), items, 1)
	if len(res) != 1 {
		t.Fatalf("got %d results, want 1", len(res))
	}
	r := res[0]
	if r.Result.State != StateUnknown {
		t.Fatalf("State = %q, want unknown（unknown 不是 dead）；echo 拨号 %d 次，LatencyMS=%d Incomplete=%v ExitIP=%q",
			r.Result.State, echoDials.Load(), r.Result.LatencyMS, r.Result.Incomplete, r.Result.ExitIP)
	}
	if !r.Result.Incomplete {
		t.Errorf("Incomplete = false, want true")
	}
	if r.Result.LatencyMS != -1 {
		t.Errorf("LatencyMS = %d, want -1", r.Result.LatencyMS)
	}
}

// TestOversizedGateBodyIsRejected 是 R3 的闸门半边:闸门/echo 都是我们不控制
// 的第三方主机。无上限的 io.ReadAll 意味着一个坏掉的(或被接管的)闸门能把
// 探测进程拖进 OOM —— 而探测是并发 48 路的,一份超大体被读 48 次。
//
// 这里直接调 gateVerdict:它就是要钉住的那个判据函数,走 ProbeNode 还要等
// 两次 stage-1 预算,慢且绕。
func TestOversizedGateBodyIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = io.CopyN(w, zeroReader{}, maxGateBodyBytes+1)
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	err = gateVerdict(resp)
	if err == nil {
		t.Fatal("超大体必须判闸门失败,不能默默吃下整份响应")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("错误文案要能归因到上限: %v", err)
	}
}

// zeroReader 产出无限个 'a'。生成器而非 strings.Repeat:测试自己先分配一份
// 8MB 字符串的话,测的就不是被测代码的内存行为了。
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

// TestAbortStopsTheZombie（Go 特有）：兜底到点后，该 worker 的派生 ctx 必须在
// 100ms 内 Done。观测点是拨号器收到的 ctx（ProbeNode 把 worker ctx 一路传进每
// 一次请求），echo 段的拨号 ctx 是 worker ctx 的子孙，cancel 必然传导。
//
// 为什么必须是「拨号不响应 ctx」而不是「服务器不回」：单发预算会作为期限传进
// 请求 ctx（TestOptionsPerItem 钉的就是这条），服务器不回时那一发会在 shot ctx
// 处自己解挂、以 dead/alive 收场，根本走不到兜底 —— 兜底预算（attempts×timeoutMs
// +echoBudget+slack）按构造就晚于它。能活过兜底的只有底层不配合取消的形状，
// 与生产语义一致：easy-proxies 记录过的就是这种协议。TestIncompleteIsNotDead
// 用同一形状钉「unknown 不是 dead」，这里补上「兜底之后不留僵尸 ctx」。
func TestAbortStopsTheZombie(t *testing.T) {
	live, echoSrv := hangPair(t)
	var mu sync.Mutex
	var observed context.Context
	dial := httpclient.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		observed = ctx // 最后一次拨号是 echo 段（stage-1 先发生且只有一发）
		mu.Unlock()
		if strings.HasSuffix(addr, strings.TrimPrefix(echoSrv.URL, "http://")) {
			return &stubbornConn{}, nil // echo 段挂死不响应 ctx
		}
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	})
	p := NewProber(nil)
	p.liveness = []string{live.URL + "/live"}
	p.gate = ""
	p.echo = []string{echoSrv.URL + "/echo"}
	// 同 TestIncompleteIsNotDead：撞兜底的前提是 echo 的头先到、body 读挂死，
	// 预算给到 5000ms 才不会被满载下的调度抖动抢先收口（R3：1000ms 在 12 包
	// 并行 race 下被吃穿，transport 先关 body、echo 合法失败回 alive）。
	p.echoBudgetMS = 5000
	p.backstopSlackMS = 500
	items := []Item{{Tag: "zombie", Dial: dial, Options: ProbeOptions{TimeoutMS: 100, Attempts: 1}}}

	done := make(chan []Result, 1)
	go func() { done <- p.ProbeAll(context.Background(), items, 1) }()

	select {
	case res := <-done:
		if len(res) != 1 || res[0].Result.State != StateUnknown || !res[0].Result.Incomplete {
			t.Fatalf("results = %+v, want one unknown/incomplete", res)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ProbeAll did not return after the backstop budget")
	}
	mu.Lock()
	echoCtx := observed
	mu.Unlock()
	if echoCtx == nil {
		t.Fatal("echo dial never happened")
	}
	// 兜底分支在写结果前先 cancel（nodeprobe.go 的顺序保证），100ms 只吸收调度
	// 延迟——测试钉的是"必须发生"，不是"恰好同时"。
	select {
	case <-echoCtx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("worker ctx still alive 100ms after the backstop fired: zombie")
	}
}
