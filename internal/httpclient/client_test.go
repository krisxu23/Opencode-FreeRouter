// SPDX-License-Identifier: GPL-3.0-or-later
package httpclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestTransportKeepsEightIdleConnectionsPerHost 钉住 O3:MaxIdleConns 只限**总量**,
// 而每主机的空闲上限是 Go 的默认值 2。一个出口背后就是同一个 host,所以高并发下
// 每个出口只能复用两条空闲连接 —— 第三条起新建、用完丢掉,握手成本按请求数累加。
// transport 本来就是按出口建的(一个 client 一个池),总量与每主机的上限应当一致。
func TestTransportKeepsEightIdleConnectionsPerHost(t *testing.T) {
	tr := transport(nil)
	if tr.MaxIdleConnsPerHost != tr.MaxIdleConns {
		t.Fatalf("MaxIdleConnsPerHost = %d, want 与 MaxIdleConns(%d) 一致(Go 默认 2)",
			tr.MaxIdleConnsPerHost, tr.MaxIdleConns)
	}
}

func TestClientUsesTheInjectedDialer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	used := 0
	c := NewClient(func(ctx context.Context, network, addr string) (net.Conn, error) {
		used++
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}, 5*time.Second)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = resp.Body.Close()
	if used == 0 {
		t.Fatal("injected dialer was never called")
	}
}

func TestClientTimeoutIsEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	c := NewClient(nil, 150*time.Millisecond)
	if _, err := c.Get(srv.URL); err == nil {
		t.Fatal("expected a timeout, got nil")
	}
}

func TestClientRejectsRedirectsToAnotherHost(t *testing.T) {
	// 订阅源偶尔 301 到 CDN。不限制重定向次数就可能被带着跑十跳。
	// 这里只断言不 panic 且最终报错或成功，行为细节由实现说明。
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("final"))
	}))
	defer other.Close()
	c := NewClient(nil, 2*time.Second)
	resp, err := c.Get(other.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
}

// TestStreamClientAllowsAResponseLongerThanTheIdleWindow 钉住流式客户端与
// NewClient 的关键差别：死线是**空闲**截止，不是整请求截止。JS 权威在
// http.js:185（timeoutMs=300000）与 :235（每收到一块就 deadline = now +
// timeoutMs 续期）。整请求死线会让一个正常吐 40 秒的回复在第 20 秒被腰斩。
func TestStreamClientAllowsAResponseLongerThanTheIdleWindow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		for i := 0; i < 6; i++ {
			_, _ = io.WriteString(w, "data: chunk\n\n")
			flusher.Flush()
			time.Sleep(40 * time.Millisecond)
		}
	}))
	defer srv.Close()
	// 空闲窗口 120ms，而整条响应要 ~240ms：整请求死线必然失败，空闲死线必须成功。
	c := NewStreamClient(nil, 120*time.Millisecond)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("stream client rejected a response that kept sending: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("read no body")
	}
}

// TestStreamClientAbortsAnIdleStream 钉住另一半：真正停发时必须在空闲窗口后
// 报错，且错误要能被认出来 —— 引擎把它当 TIMEOUT（可重试、可冷却），而不是
// 掉进 classifyAttemptError 的 SERVER 兜底。
func TestStreamClientAbortsAnIdleStream(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release // 只停发，不关流：JS 注释 http.js:180-184 描述的那种源
	}))
	defer srv.Close()
	defer close(release)

	c := NewStreamClient(nil, 120*time.Millisecond)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	started := time.Now()
	_, err = io.ReadAll(resp.Body)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("expected the idle deadline to abort the read, got nil")
	}
	if !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("want ErrIdleTimeout, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("idle deadline fired far too late: %s", elapsed)
	}
}

// TestStreamClientAbortsAStreamThatNeverSendsHeaders 覆盖响应头阶段的空闲
// 截止。引擎默认没有墙钟上限，一个接受连接却永不发头的上游（或者连 TCP 都
// 没建起来的那种）会让请求挂到天荒；NewStreamClient 必须自己把这一段收掉。
func TestStreamClientAbortsAStreamThatNeverSendsHeaders(t *testing.T) {
	release := make(chan struct{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		<-release // 收下连接，一个字都不写
	}()
	defer close(release)

	c := NewStreamClient(nil, 120*time.Millisecond)
	started := time.Now()
	_, err = c.Get("http://" + ln.Addr().String())
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("expected the header deadline to abort the request, got nil")
	}
	if !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("want ErrIdleTimeout, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("header deadline fired far too late: %s", elapsed)
	}
}

// TestStreamClientStillWorksForAWholeBodyRead 保证空闲读不会被包装器自己打断：
// 一次快速完成的整包读取必须原样返回。
func TestStreamClientStillWorksForAWholeBodyRead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()
	c := NewStreamClient(nil, 5*time.Second)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(raw) != "hello" {
		t.Fatalf("body = %q", raw)
	}
}

// TestIdleReaderDoesNotKillAStreamThatKeepsArriving 是 gen 仲裁位的回归用例。
//
// 形状：每个空闲窗口的 2/3 处送一块数据,连送十余次,总时长远超 idle。旧实现
// (time.Timer.Reset 复用同一枚) 在计时器已触发、回调正排在 r.mu 上等锁的 tick
// 上会把一条正在吐字的流 Close 掉,下一次 Read 报 ErrIdleTimeout —— 引擎按
// 可重试可冷却的 TIMEOUT 处理,**健康出口被误冷却**。
//
// 这条用例对时序敏感,所以把余量拉开:数据每 idle*2/3 一块,即使有几十毫秒的
// 调度抖动也远在窗口内;判定看的是「十几轮续期之后流仍然完整」,而不是某一
// 次 Reset 的返回值。
func TestIdleReaderDoesNotKillAStreamThatKeepsArriving(t *testing.T) {
	const (
		idle   = 60 * time.Millisecond
		chunks = 15
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, _ := w.(http.Flusher)
		for i := 0; i < chunks; i++ {
			_, _ = io.WriteString(w, "x")
			if f != nil {
				f.Flush()
			}
			time.Sleep(idle * 2 / 3)
		}
	}))
	defer srv.Close()

	c := NewStreamClient(nil, idle)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("持续吐字的流被空闲读掐断了: %v（want nil）", err)
	}
	if len(raw) != chunks {
		t.Fatalf("读到 %d 字节, want %d", len(raw), chunks)
	}
}

// TestHeaderGuardSettleWaitsForTheCallback 钉第八轮 R3 中-2:计时器 Stop()
// 返回 false 只保证回调**已触发**(被排进 goroutine 队列),不保证它已执行到
// expired.Store(true) —— 旧形状在这里直接读标志,晚到的回调随后才 cancel(),
// 流在 body 阶段死于裸 "context canceled"。契约:settle 必须等回调收尾,
// 返回后 triggered() 是最终值。
func TestHeaderGuardSettleWaitsForTheCallback(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	cancel := func() {
		close(entered) // 回调已进门,但还没落 expired —— 把它钉在这里
		<-release
	}
	g := newHeaderGuard(time.Millisecond, cancel)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("前置:回调未触发")
	}
	if g.triggered() {
		t.Fatal("前置:cancel 阻塞期间 expired 不应为真")
	}
	settleDone := make(chan struct{})
	go func() { g.settle(); close(settleDone) }()
	// 旧形状(不等回调)此刻已经返回;新形状必须还卡着。
	select {
	case <-settleDone:
		t.Fatal("settle 在回调收尾前就返回了:TOCTOU 窗口没有关上")
	case <-time.After(80 * time.Millisecond):
	}
	close(release)
	select {
	case <-settleDone:
	case <-time.After(2 * time.Second):
		t.Fatal("release 后 settle 未返回")
	}
	if !g.triggered() {
		t.Fatal("settle 返回后 triggered() 必须是最终值 true")
	}
}

// settle 对未触发的计时器照旧即时返回,不引入额外等待。
func TestHeaderGuardSettleOnLiveTimerReturnsImmediately(t *testing.T) {
	g := newHeaderGuard(time.Hour, func() {})
	done := make(chan struct{})
	go func() { g.settle(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("未触发的计时器不应等待")
	}
	if g.triggered() {
		t.Fatal("前置未满足:计时器不应已触发")
	}
}

// TestNewStreamClientClampsNonPositiveIdle 钉第八轮 R3 低-1:idle<=0 的旧
// 形状把头阶段截止与 body 空闲截止**双双静默关闭**(「挂到天荒」复活)。
// 构造器必须钳到默认值,不得退化。
func TestNewStreamClientClampsNonPositiveIdle(t *testing.T) {
	for _, idle := range []time.Duration{0, -1, -time.Hour} {
		c := NewStreamClient(nil, idle)
		tr, ok := c.Transport.(*idleTransport)
		if !ok {
			t.Fatalf("idle=%v: Transport 不是 *idleTransport", idle)
		}
		if tr.idle != defaultStreamIdle {
			t.Fatalf("idle=%v 被静默接受: 构造器必须钳到 defaultStreamIdle(%v)", idle, defaultStreamIdle)
		}
	}
	c := NewStreamClient(nil, 300*time.Millisecond)
	if tr := c.Transport.(*idleTransport); tr.idle != 300*time.Millisecond {
		t.Fatalf("正的 idle 被改写: %v", tr.idle)
	}
}
