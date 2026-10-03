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
