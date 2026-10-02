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

// hangServer 的 /live 秒回 204，/echo 永远不回（等客户端自己取消）：stage-1
// 快速通过、echo 挂满预算——这正是会撞上兜底的形状（echo 8s > 兜底余量 5s）。
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

// TestIncompleteIsNotDead（Go 特有）：必触发 backstop 的配置下，结果是
// unknown 且 Incomplete，绝不是 dead——dead 会给可能健康的节点记一次连败。
func TestIncompleteIsNotDead(t *testing.T) {
	srv := hangServer(t)
	p := NewProber(nil)
	p.liveness = []string{srv.URL + "/live"}
	p.gate = ""
	p.echo = []string{srv.URL + "/echo"}
	items := []Item{{Tag: "slow", Options: ProbeOptions{TimeoutMS: 100, Attempts: 1}}}
	res := p.ProbeAll(context.Background(), items, 1)
	if len(res) != 1 {
		t.Fatalf("got %d results, want 1", len(res))
	}
	r := res[0]
	if r.Result.State != StateUnknown {
		t.Fatalf("State = %q, want unknown（unknown 不是 dead）", r.Result.State)
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
// 为什么打 echo 段而不是「拨号挂死」：单发预算会作为期限传进请求 ctx
// （TestOptionsPerItem 钉的就是这条），所以拨号挂死的形状会在单发预算处自己
// 解挂、以 dead 收场，根本走不到兜底——按构造，兜底（attempts×timeoutMs+5000）
// 永远晚于单发预算，能活过兜底的只有固定的 8s echo 预算（timeoutMs<3000 时）。
// 这与生产语义一致：能卡死整轮探测的恰恰是 echo 段。TestIncompleteIsNotDead
// 用同一形状钉「unknown 不是 dead」，这里补上「兜底之后不留僵尸 ctx」。
func TestAbortStopsTheZombie(t *testing.T) {
	srv := hangServer(t)
	var mu sync.Mutex
	var observed context.Context
	dial := httpclient.Dialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		observed = ctx // 最后一次拨号是 echo 段（stage-1 先发生且只有一发）
		mu.Unlock()
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
	})
	p := NewProber(nil)
	p.liveness = []string{srv.URL + "/live"}
	p.gate = ""
	p.echo = []string{srv.URL + "/echo"}
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
