// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package httpclient builds the two HTTP clients the program needs: requests
// that go straight out, and requests that go through a specific proxy node.
package httpclient

import (
	"context"
	stderrors "errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Dialer opens a connection to addr. It is a type alias on purpose: sbx.Dialer
// and the default dialer both match this shape, so either can be passed
// without a conversion at every call site.
type Dialer = func(ctx context.Context, network, addr string) (net.Conn, error)

// ErrIdleTimeout 是空闲截止被触发的哨兵。它必须能被 errors.Is 认出：引擎把
// 超时当成 TIMEOUT（可重试、可冷却），而不是掉进 classifyAttemptError 的
// SERVER 兜底 —— 那样一个卡死的上游会被当成供应商故障而不是出口故障。
var ErrIdleTimeout = stderrors.New("httpclient: stream idle past its deadline")

// transport 建出两个客户端共用的手工 Transport。手工而不是克隆
// http.DefaultTransport 的理由见 NewClient 的注释。
func transport(d Dialer) *http.Transport {
	tr := &http.Transport{
		DialContext:           d,
		MaxIdleConns:          8,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// node's pipeline: pipelining 0 in the JS build became this flag. Keeping
		// it explicit matters because the default is 1 and a pipelined request
		// that fails mid-flight is attributed to the wrong node.
		ForceAttemptHTTP2: false,
	}
	if d == nil {
		tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return tr
}

// NewClient returns an HTTP client whose connections are opened by d. A nil d
// means the host's own network.
//
// The transport is built by hand rather than by cloning http.DefaultTransport
// for one reason: a node must keep working while the pool is being reloaded,
// and http.DefaultTransport pools connections across the change. A per-node
// client is cheap; a stale pooled connection to a deleted node is not.
//
// timeout is a **whole-request** deadline, which is what the probe, catalog and
// subscription paths want. Streaming turns must use NewStreamClient instead:
// a 40-second answer is normal there, and a whole-request deadline kills it.
func NewClient(d Dialer, timeout time.Duration) *http.Client {
	return &http.Client{Transport: transport(d), Timeout: timeout}
}

// NewStreamClient returns a client for streaming upstream turns. It differs
// from NewClient in exactly one way: idle is the deadline, not the whole
// request.
//
// JS 权威（archive/node/src/http.js:103/:185/:235）：timeoutMs 默认 300000，
// 且每收到一块就 deadline = now + timeoutMs 续期。注释 :180-184 记着理由 ——
// 一个只停发不关流的源永远走不到循环体，事后检查 Date.now() 的写法实测
// timeoutMs=20 在 400ms 后仍然 pending。Go 的 http.Client.Timeout 恰恰是事后
// 检查不了的那种：它是整请求截止，一个正常吐 40 秒的回复会在第 20 秒被腰斩
// （原实现 app.go 传 20s）。所以这里把 Timeout 留 0，改由响应体包装器在每次
// Read 返回后重置一枚 time.Timer 来实现空闲截止。
func NewStreamClient(d Dialer, idle time.Duration) *http.Client {
	c := &http.Client{Transport: transport(d)}
	c.Transport = &idleTransport{base: c.Transport, idle: idle}
	return c
}

// idleTransport 把每个响应体包成空闲截止读取器。用 RoundTripper 而不是
// http.Client.Timeout 是因为只有拿到响应体之后才知道「空闲」该怎么算；用
// 包装器而不是 context deadline 是因为 context 一旦超时就不可续期，而那正是
// 整请求死线的形状。
type idleTransport struct {
	base http.RoundTripper
	idle time.Duration
}

func (t *idleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// 响应头阶段也必须有界：一个接受连接却永不发头的上游会让请求挂到天荒
	// （引擎默认没有墙钟上限）。用一枚在头到达时就停掉的定时器取消请求
	// context —— 头一到就交棒给响应体的空闲读，两者不重叠。
	//
	// cancel 必须活到响应体读完：响应体的读就挂在这个 context 上，RoundTrip
	// 一返回就 defer cancel 会把刚拿到的 body 立刻取消掉（实测症状是
	// "context canceled"）。所以正常路径把它交给 idleReader，由 Close 释放。
	ctx, cancel := context.WithCancel(req.Context())
	var headerExpired atomic.Bool
	var headerTimer *time.Timer
	if t.idle > 0 {
		headerTimer = time.AfterFunc(t.idle, func() {
			headerExpired.Store(true)
			cancel()
		})
	}
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if headerTimer != nil {
		headerTimer.Stop()
	}
	if err != nil {
		cancel()
		// 头阶段的超时在底层看起来是 context canceled（我们自己取消的），
		// 对调用方要的是同一个空闲超时语义 —— 否则它会被归成「客户端中止」
		// 而不是可重试的 TIMEOUT。
		if headerExpired.Load() {
			return nil, ErrIdleTimeout
		}
		return resp, err
	}
	if resp.Body != nil {
		resp.Body = newIdleReader(resp.Body, t.idle, cancel)
	} else {
		cancel()
	}
	return resp, nil
}

// idleReader 是「每读到数据就续期」的响应体。计时器在两次 Read 之间跑，超时
// 即 Close 底层连接 —— Close 会让阻塞中的 Read 立刻返回，读取方看到的是
// ErrIdleTimeout（errors.Is 可认）。除了 Close 还要取消 context：DialContext
// 挂在它上面，一个还没建连的请求同样会被空闲计时器收走。
type idleReader struct {
	body   io.ReadCloser
	idle   time.Duration
	cancel context.CancelFunc
	mu     sync.Mutex
	t      *time.Timer
	done   bool
}

func newIdleReader(body io.ReadCloser, idle time.Duration, cancel context.CancelFunc) *idleReader {
	r := &idleReader{body: body, idle: idle, cancel: cancel}
	if idle > 0 {
		r.t = time.AfterFunc(idle, r.expire)
	}
	return r
}

// expire 只关底层连接，不碰 r.done：谁先到（读完成还是超时）由 arm/disarm 的
// 互斥保证不会两边同时动计时器。
func (r *idleReader) expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	// 关掉底层连接让阻塞中的 Read 返回；真正的错误值由 Read 自己判超时。
	_ = r.body.Close()
	r.done = true
}

func (r *idleReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	r.mu.Lock()
	expired := r.done
	if !expired && r.t != nil && err == nil {
		// 收到数据就续期（js http.js:235 的 deadline = Date.now() + timeoutMs）。
		r.t.Reset(r.idle)
	}
	r.mu.Unlock()
	if err != nil && expired {
		// 底层是「读到一半被关掉」的形状，对调用方要的是「空闲超时」这个语义。
		return n, ErrIdleTimeout
	}
	return n, err
}

func (r *idleReader) Close() error {
	r.mu.Lock()
	r.done = true
	if r.t != nil {
		r.t.Stop()
	}
	r.mu.Unlock()
	if r.cancel != nil {
		r.cancel()
	}
	return r.body.Close()
}
