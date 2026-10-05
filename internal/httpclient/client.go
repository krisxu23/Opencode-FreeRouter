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

// defaultStreamIdle 是 NewStreamClient 对 idle<=0 的钳制值,与 app 侧的
// streamIdleTimeout(300s)同值 —— 语义就是「流式默认空闲窗」,不要在此
// 引出第二个事实源:改 app 的常量时必须一起动这里。
const defaultStreamIdle = 5 * time.Minute

// transport 建出两个客户端共用的手工 Transport。手工而不是克隆
// http.DefaultTransport 的理由见 NewClient 的注释。
func transport(d Dialer) *http.Transport {
	// 每出口的空闲连接池。32 = 单请求 attemptCap(20)+ 并发余量:8 的旧值意味着
	// 高并发打同一出口时第 9 条起新建、用完即弃,TCP+TLS 握手成本按请求数累加。
	// transport 本来就是按出口建的(一个 client 一个池),所以每主机的上限与
	// 总量取同一个数才是这里的语义(主机数恒为 1,Go 默认 2 的坑见 O3)。
	tr := &http.Transport{
		DialContext:         d,
		MaxIdleConns:        32,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     60 * time.Second,
		// 握手 5s:免费池的慢出口多,10s 串行 Dial+TLS 最坏 20s 才判死一个
		// 出口;5s 足够覆盖正常握手(P95 远小于此),烂握手早死早换出口。
		TLSHandshakeTimeout:   5 * time.Second,
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

// NewOneShotClient 是 NewClient 的单发变体:DisableKeepAlives 让连接在响应
// 结束后立刻关闭,不留空闲连接慢慢等 IdleConnTimeout。给探测这类「每发一个
// 全新 client」的调用方用 —— 一轮数千个 shot 各自留下 ≤1 条空闲连接,就是
// 探测期的瞬时 fd 尖峰(O4)。
func NewOneShotClient(d Dialer, timeout time.Duration) *http.Client {
	c := NewClient(d, timeout)
	c.Transport.(*http.Transport).DisableKeepAlives = true
	return c
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
	// idle<=0 的旧形状是「头阶段截止与 body 空闲截止双双静默关闭」——
	// 一个只收不发头的上游重新挂到天荒(第八轮 R3 低-1)。生产唯一调用方
	// 传的是 app 的 streamIdleTimeout(300s),这里钳到同值,拒绝退化为无界。
	if idle <= 0 {
		idle = defaultStreamIdle
	}
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
	guard := newHeaderGuard(t.idle, cancel)
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	guard.settle()
	if err == nil && guard.triggered() {
		// 头在 idle 截止的同一 tick 到达:回调已把 ctx cancel —— 不在这里
		// 拦,body 的每次 Read 都会死在 "context canceled",被 adapter 归成
		// EMPTY/SERVER 而不是 TIMEOUT(不进 cooldownOn,坏出口不冷却)。与
		// exchange 侧「空闲截止先于 ctx 检查」同一裁决。
		cancel()
		return nil, ErrIdleTimeout
	}
	if err != nil {
		cancel()
		// 头阶段的超时在底层看起来是 context canceled（我们自己取消的），
		// 对调用方要的是同一个空闲超时语义 —— 否则它会被归成「客户端中止」
		// 而不是可重试的 TIMEOUT。
		if guard.triggered() {
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

// headerGuard 把「头阶段截止」封装成可等价:计时器触发时回调先 cancel 请求
// context、再落 expired 标志、最后关 done(顺序刻意:回归测试用一枚阻塞的
// cancel 就能把回调钉在「已触发、未落定」的位置上)。**settle 是关键** —— 计时器的
// Stop() 返回 false 只保证回调已被触发(runtime 已把它排进 goroutine 队列),
// 不保证它已执行到 expired.Store(true):旧形状在这里直接读标志,晚到的回调
// 随后才 cancel() ⇒ 流在 body 阶段死于裸 "context canceled",被归成
// EMPTY/SERVER 而非 ErrIdleTimeout(不冷却)。settle 在 Stop=false 时等回调
// 收尾再返回,expired 的读数因此是最终值(第八轮 R3 中-2;窗口极窄,短 idle
// 的测试与高负载下概率放大)。
type headerGuard struct {
	expired atomic.Bool
	done    chan struct{}
	timer   *time.Timer
}

func newHeaderGuard(idle time.Duration, cancel context.CancelFunc) *headerGuard {
	g := &headerGuard{done: make(chan struct{})}
	if idle > 0 {
		g.timer = time.AfterFunc(idle, func() {
			cancel()
			g.expired.Store(true)
			close(g.done)
		})
	}
	return g
}

// settle 停掉头阶段计时器;若它已触发,等回调执行完再返回。
func (g *headerGuard) settle() {
	if g == nil || g.timer == nil {
		return
	}
	if !g.timer.Stop() {
		<-g.done
	}
}

func (g *headerGuard) triggered() bool {
	return g != nil && g.expired.Load()
}

// idleReader 是「每读到数据就续期」的响应体。计时器在两次 Read 之间跑，超时
// 即 Close 底层连接并取消请求 context —— Close 让阻塞中的 Read 立刻返回，
// cancel 收走还挂在 DialContext 上的建连（一个还没建连的请求同样该被空闲
// 计时器收走；且超时路径上调用方未必会走到 Close，不能只指望 Close 释放）。
//
// gen 是续期与超时的仲裁位。time.Timer.Reset 对**已触发**的计时器同样「成功」
// 返回 nil，但 AfterFunc 的回调已经被 runtime 排进 goroutine 队列：它正阻塞在
// r.mu 上等锁，而这一侧的 Read 先拿到锁、拿到数据、把表重整完就放锁 —— 回调
// 随后拿到锁，r.done 还是 false，于是把一条正在正常吐字的流 Close 掉，下一次
// Read 报 ErrIdleTimeout。引擎按可重试可冷却的 TIMEOUT 处理它：**健康出口被误
// 冷却**。
//
// 解法是「一次武装一枚计时器，不复用」：每次续期都递增 gen 并新开 AfterFunc，
// 回调带着自己那一代的编号进来，对不上就当作「本次触发已被后续数据作废」直接
// 返回。**不能**用 Reset 复用同一枚 —— 复用后回调携带的是首次武装时的旧 gen，
// 续期越多、被作废的就越多，最后一次真正的超时也一并被作废，流永远不收口
// （这正是它第一次跑挂的形状）。
type idleReader struct {
	body   io.ReadCloser
	idle   time.Duration
	cancel context.CancelFunc
	mu     sync.Mutex
	t      *time.Timer
	gen    uint64
	done   bool
}

func newIdleReader(body io.ReadCloser, idle time.Duration, cancel context.CancelFunc) *idleReader {
	r := &idleReader{body: body, idle: idle, cancel: cancel}
	if idle > 0 {
		r.armLocked()
	}
	return r
}

// armLocked（持有 r.mu）把空闲截止重整到「此刻起 idle 之后」。代价是每读到一块
// 数据就换一枚计时器：流式响应里这是每 chunk 一次 time.AfterFunc，量级可忽略，
// 换来的是「过期回调永不误伤新鲜数据」这条硬保证。
func (r *idleReader) armLocked() {
	r.gen++
	gen := r.gen
	if r.t != nil {
		r.t.Stop() // 已触发时 Stop 返回 false，回调仍会来 —— 由 gen 挡掉
	}
	r.t = time.AfterFunc(r.idle, func() { r.expire(gen) })
}

// expire 只关底层连接并取消请求 ctx。超时必须自我收口：调用方在超时后未必还会
// Close（错误分支直接 return、或把 resp 交给非 defer 的路径），而排队/拨号中的
// 请求只有 ctx 能中止。
func (r *idleReader) expire(gen uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done || gen != r.gen {
		return // 已被后续数据续期作废，或已收尾
	}
	// 关掉底层连接让阻塞中的 Read 返回；真正的错误值由 Read 自己判超时。
	_ = r.body.Close()
	if r.cancel != nil {
		r.cancel() // 幂等：Close 路径也调它
	}
	r.done = true
}

func (r *idleReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	r.mu.Lock()
	expired := r.done
	if !expired && r.t != nil {
		switch {
		case err == nil:
			// 收到数据就续期（js http.js:235 的 deadline = Date.now() + timeoutMs）。
			r.armLocked()
		case stderrors.Is(err, io.EOF):
			// 读完了:停表。旧实现让计时器以剩余时间继续跑满整个 idle 窗口,
			// 期间 expire() 会对已读完的 body 再做一次 Close、把 r 钉在
			// timer 里 —— 无功能损害,但生命周期不对称,Close 路径有的
			// 收尾 EOF 路径没有。
			r.t.Stop()
		}
	}
	r.mu.Unlock()
	if expired && err != nil && n == 0 {
		// 底层是「读到一半被关掉」的形状,对调用方要的是「空闲超时」这个语义。
		// 只覆盖**没有数据**的读:transport 缓冲里最后一块数据与 EOF 同帧返回
		// 时(err != nil 且 n > 0),一条已完整读完的流若恰逢计时器先到,会被
		// 误报成 TIMEOUT —— adapter 把它翻译成可重试可冷却的 TIMEOUT,换出口
		// 重放整轮并错误冷却一个好出口。有数据的最后一次读必须先把数据交出去。
		return 0, ErrIdleTimeout
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
