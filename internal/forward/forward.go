// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package forward 是客户端能看到的全部网关:转发端口上的 OpenAI 形状 HTTP 面。
// 它上游的一切 —— 节点池、sing-box、出口选择 —— 在这里都不可见,这正是 Go
// 重写能替换掉那整套机械、却不用动本包契约一个字节的原因。
//
// 归层 L5(见 internal/LAYERS.md):Config.Complete 直接返回 engine.Outcome,
// 而 engine 是 L4。forward 与 panel/app 一样是组装层职责。
package forward

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/engine"
	frerrors "freerouter/internal/errors"
	"freerouter/internal/logger"
	"freerouter/internal/upstream"
)

const (
	// maxBodyBytes 照 src/forward.js:23。用 http.MaxBytesReader 而非读完再判长:
	// 读完再判意味着攻击者已经喂进来几个 GB,而 8MB 之后的内容一个字节都不该读。
	maxBodyBytes = 8 * 1024 * 1024
	// keyBytes / keyPrefix 是 generateKey 的两个参数(js :47-49 的 24 字节 base64url)。
	keyBytes  = 24
	keyPrefix = "ofm-"
	// serviceName 是探活体里的服务名(js :126)。两个版本必须报同一个名字,
	// 否则依赖探活体的编排会以为换了服务。
	serviceName = "our-free-model"
)

// retryableCodes 是**面向调用方**的「这次失败值得你自己再试一次吗」(js :40)。
//
// 项目里有三个用途不同、**不要求一致**的重试码集合,别把它们混成一个:
//   - 这里 = 已经轮换完整个池子之后,值不值得让调用方自己再试;
//   - engine 的 retryOn = 哪些失败值得换个出口重来(**含 quota**);
//   - adapter 的 providerRetryPolicy = 上游 harness 自己的重试策略,1:1 移植。
//
// 上一版注释把这里写成「与 adapter 保持一致」,那句话是错的:quota 不在本集合
// 里,因为配额按出口 IP 计(用户拍板:换 IP 额度就是全新的),把配额报成可重试
// 只会让调用方在同一个 IP 上原地重试。js :29-38 对此有逐字说明。
var retryableCodes = map[string]bool{
	check.CodeRegion:    true,
	check.CodeTransport: true,
	check.CodeTimeout:   true,
	check.CodeEmpty:     true,
	check.CodeServer:    true,
}

// corsHeaders 照 js :174-181。写成一个有序表而不是 map:HTTP 头本身的语义与
// 顺序无关,但把四处值集中在一处便于和 JS 逐行对照。
var corsHeaders = [][2]string{
	{"Access-Control-Allow-Origin", "*"},
	{"Access-Control-Allow-Headers", "authorization, content-type, x-api-key"},
	{"Access-Control-Allow-Methods", "GET, POST, OPTIONS"},
	{"Access-Control-Max-Age", "600"},
}

// Config 是转发服务的全部依赖,一律以回调注入,好让本包不认识 store/sbx/health。
//
// 与总纲 §6 的一处偏离(记录在提交信息里):§6 把 Complete 压平成
// `func(text string) error`,那会丢掉 tool-call 与 usage 两类帧 —— 而
// engine/translate.go:347-355 已经裁定「转发层用同一对构造器/信封解码」。
// 因此这里直接暴露 engine.Complete 的原签名,转发层拿到的是 engine.Chunk。
type Config struct {
	// Enabled 对应 settings.enabled。false 时除探活外一律 503,这样一个被
	// 关掉的网关报告的是「关着」,而不是「不存在」。
	Enabled func() bool
	// ForwardKey 是共享密钥,比较走常数时间。空串等于拒绝一切鉴权请求。
	ForwardKey func() string
	// Complete 跑一轮对话,就是 engine.Complete。
	Complete func(ctx context.Context, req engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error)
	// ModelRows 服务 /v1/models,就是 engine.ModelRows。
	ModelRows func() []engine.Row
	// Log 收请求级日志行,缺省走 logger.Info。
	Log func(msg string)
}

// Server 是转发端口的监听者。用 New + Serve(ln) 两步式而不是 Start(ctx,cfg):
// 端口错误必须在调用方那一侧同步暴露,否则 app 无法在「端口被占」时回落随机端口。
type Server struct {
	cfg  Config
	http *http.Server
	// busy 是「正在做工作」的请求计数(移植自 magpie busy.go):只数转发
	// 端口上真正跑一轮对话的请求 —— GET/OPTIONS/探活不占数。lastDone 让
	// 面板能区分「工具链间隙」(agent 在跑工具,没发请求但回合没完)与
	// 「回合结束」(inFlight 落到 0 且 lastDone 刚刷新)。
	busy busyCounter
}

// busyCounter 是转发在途请求的原子账本。inFlight 只数已鉴权的 POST 对话
// 请求;lastDone 记最后一个请求完成的 unix 纳秒,0 表示进程启动以来还没
// 完成过。
type busyCounter struct {
	inFlight atomic.Int64
	lastDone atomic.Int64
}

// workBegin/workEnd 是对话请求的进出账;workEnd 记完成时刻。二者之间
// panic 也由 handle 的 recover 兜底调 workEnd,账面不泄漏。
func (s *Server) workBegin() { s.busy.inFlight.Add(1) }

func (s *Server) workEnd() {
	if n := s.busy.inFlight.Add(-1); n < 0 {
		// 多余的 workEnd(理论不可达,防御):别把账压成负数。
		s.busy.inFlight.Add(1)
		return
	}
	s.busy.lastDone.Store(time.Now().UnixNano())
}

// Busy 是转发端口「正在做工作」的实时视图,给面板 /api/status 用。
func (s *Server) Busy() ForwardBusy {
	return ForwardBusy{
		Requests: s.busy.inFlight.Load(),
		Last:     s.busy.lastDone.Load(),
	}
}

// ForwardBusy 是 busy 视图的形状(magpie Busy{Requests, Last} 同款)。
type ForwardBusy struct {
	Requests int64 `json:"requests"`
	Last     int64 `json:"last"`
}

// serverReadHeaderTimeout / serverIdleTimeout / serverReadTimeout 是两个服务器
// 共用的收紧值(R1 + 慢速 body 加固)。
//
// ReadHeaderTimeout/IdleTimeout 堵的是 slowloris 的两个洞:连上来不发头、
// 发完一个请求就挂着不关。这里补第三个洞:发头之后**无限慢速地喂 body** ——
// maxBodyBytes 只限体积不限时间,io.ReadAll 没有时间约束,旧实现里一条连接
// 可以被任意慢的 body 无限期占住。5 分钟对合法请求(8MB 上限)绰绰有余;
// SSE 是写方向,不受读死线影响。
//
// 不设 WriteTimeout:转发端口要吐 SSE,整请求死线会把它腰斩(那正是
// httpclient.NewStreamClient 存在的理由)。
//
// 与 JS 同源(forward.js:100 同样没设),但 JS 那边是 Node 默认值,Go 这边是
// 零值即无限;修起来零成本,所以修。
const (
	serverReadHeaderTimeout = 10 * time.Second
	serverIdleTimeout       = 60 * time.Second
	serverReadTimeout       = 5 * time.Minute
)

// New 组装一个转发服务,此时还没有监听任何端口。
//
// ReadTimeout 是**两段式**的第一段:它只管「读体阶段」(防慢速喂 body 占住
// 连接),readBody 读完就解除读死线再进入可能长时间写 SSE 的处理阶段 ——
// 不解除的话,net/http 的后台读会在死线到期时取消请求 context,长回答的
// SSE 一样会被腰斩(这正是 ReadTimeout 曾被钉死为 0 的原因)。
func New(cfg Config) *Server {
	s := &Server{cfg: cfg}
	s.http = &http.Server{
		Handler:           http.HandlerFunc(s.handle),
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       serverReadTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
	return s
}

// Handler 暴露底层处理器,供测试与需要自行组装的调用方使用。
func (s *Server) Handler() http.Handler { return s.http.Handler }

// Serve 在 ln 上服务直到 Close。正常关闭(ErrServerClosed)返回 nil,其余
// 监听错误原样返回 —— 调用方要能区分「我关的」和「它坏了」。
func (s *Server) Serve(ln net.Listener) error {
	err := s.http.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close 关闭监听并掐断在途连接(SSE 是长连接,不掐断就永远关不干净)。
func (s *Server) Close() error { return s.http.Close() }

func (s *Server) logf(msg string) {
	if s.cfg.Log != nil {
		s.cfg.Log(msg)
		return
	}
	logger.Info(msg)
}

// writer 记住头有没有发出去。顶层 recover 与流式路径都要据此决定「还能不能
// 改状态码」:头一旦出去,唯一还能做的就是把连接收干净。
type writer struct {
	http.ResponseWriter
	wrote bool
}

func (w *writer) WriteHeader(code int) {
	if w.wrote {
		return
	}
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *writer) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Flush 让 SSE 真的逐帧到达客户端。缺了它,net/http 会把整个流攒到缓冲区
// 满了才发,「流式」就只剩个名字。
func (w *writer) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap 让 http.NewResponseController 穿透包装、操作底层连接的读写死线
// (readBody 在读体结束后用它解除 ReadTimeout,长流 SSE 因此不被腰斩)。
func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// handle 是路由链本身。顺序即优先级,照 src/forward.js:108-146 逐条搬。
//
// 这里没用 http.ServeMux 的 Go 1.22 方法与通配符模式(计划步骤 3 的写法):
// ServeMux 会对 `/v1/models/` 发 301、对重复斜杠做 clean,而 JS 是先把尾部
// 斜杠归一掉再匹配;更要紧的是 JS 的优先级是「关闭 → OPTIONS → 探活 → 鉴权
// → 路由」,其中前四步对**未匹配的路径**同样生效,ServeMux 的按模式分发表达
// 不了这个次序。手写链是这里唯一能逐字对齐 JS 的写法。
func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	rw := &writer{ResponseWriter: w}
	defer func() {
		// 顶层安全网(js :101-105):一个请求炸掉不该带走进程,也不该让
		// 调用方收到一个裸断的连接。panic 详情只进日志 —— 它可能携带请求
		// 内容或内部状态,回给客户端的必须是固定文案。
		if p := recover(); p != nil {
			s.logf(fmt.Sprintf("request failed: %v", p))
			if !rw.wrote {
				openAIError(rw, http.StatusInternalServerError, "server_error", "internal error")
			}
		}
	}()

	path := normalizePath(r.URL.Path)

	if !s.enabled() {
		openAIError(rw, http.StatusServiceUnavailable, "service_unavailable",
			"the forward listener is switched off in Our Free Model settings")
		return
	}
	// CORS 预检,好让另一个源上的浏览器 harness 也能用。
	if r.Method == http.MethodOptions {
		applyCORS(rw.Header())
		rw.WriteHeader(http.StatusNoContent)
		return
	}
	// 只回答存活,而且**故意放在密钥检查之前**:探端口通不通的人不该为了拿
	// 一个答案先要密钥。它也只回答「我活着」,模型清单在鉴权路由后面。
	if path == "/" || path == "/health" {
		writeJSON(rw, http.StatusOK, healthBody{OK: true, Service: serviceName})
		return
	}
	if !s.authorized(r) {
		openAIError(rw, http.StatusUnauthorized, "invalid_request_error", "missing or invalid API key")
		return
	}
	if r.Method == http.MethodGet && (path == "/v1/models" || path == "/models") {
		// 空表也要是 [] 而不是 null:客户端普遍对 data 直接做 map,
		// null 会炸在它们那边而不是我们这边。
		rows := []engine.Row{}
		if s.cfg.ModelRows != nil {
			rows = s.cfg.ModelRows()
		}
		// 回调返回 nil 切片时编码器会写成 `"data":null`,而 js :134 的
		// modelRows() 恒为数组。客户端普遍对 data 直接做 map,null 会炸在
		// 它们那边而不是我们这边。
		if rows == nil {
			rows = []engine.Row{}
		}
		writeJSON(rw, http.StatusOK, modelsBody{Object: "list", Data: rows})
		return
	}
	if r.Method == http.MethodPost && (path == "/v1/chat/completions" || path == "/chat/completions") {
		body, ok := s.readBody(rw, r)
		if !ok {
			return
		}
		// busy 计数从 body 读完后开始:排队读 body 的慢连接不算「做工作」,
		// 只数真正进对话处理的那一段。
		s.workBegin()
		defer s.workEnd()
		s.chatCompletions(rw, r, body)
		return
	}
	if r.Method == http.MethodPost && (path == "/v1/responses" || path == "/responses") {
		body, ok := s.readBody(rw, r)
		if !ok {
			return
		}
		s.workBegin()
		defer s.workEnd()
		s.responsesEndpoint(rw, r, body)
		return
	}
	openAIError(rw, http.StatusNotFound, "not_found_error", "no route for "+r.Method+" "+path)
}

// normalizePath 照 js :110 的 `pathname.replace(/\/+$/,”) || '/'`。
func normalizePath(p string) string {
	p = strings.TrimRight(p, "/")
	if p == "" {
		return "/"
	}
	return p
}

func (s *Server) enabled() bool {
	if s.cfg.Enabled == nil {
		return true
	}
	return s.cfg.Enabled()
}

func (s *Server) authorized(r *http.Request) bool {
	if s.cfg.ForwardKey == nil {
		return false
	}
	key := s.cfg.ForwardKey()
	if key == "" {
		return false
	}
	return KeyMatches(bearerOf(r), key)
}

// bearerOf 从 Authorization: bearer <key> 或 x-api-key 取密钥(js :59-64)。
// 两者都收,`bearer` 前缀大小写不敏感。
func bearerOf(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if len(auth) >= 7 && strings.EqualFold(auth[:7], "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return strings.TrimSpace(r.Header.Get("x-api-key"))
}

// GenerateKey 签发一个新密钥:`ofm-` + 24 字节 base64url(js :47-49)。
func GenerateKey() string {
	buf := make([]byte, keyBytes)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand 失败意味着系统熵源坏了,此时签出的密钥不可能安全。
		// 但签名是 `string`,没有地方返回错误 —— 返回空串,而 authorized()
		// 对空串一律拒绝,坏熵源因此表现为「全部拒绝」而不是「全部放行」。
		return ""
	}
	return keyPrefix + base64.RawURLEncoding.EncodeToString(buf)
}

// KeyMatches 常数时间地比较密钥(js :52-57)。
//
// 先比长度是必须的:长度不等本身就是答案,而 crypto/subtle 在长度不等时
// 立刻返回 0 —— 那个提前返回会把「密钥有多长」变成可测的时序侧信道。所以
// 这里先在 max(len) 长的填充副本上走完一次常数时间比较,再合并长度判据。
//
// 双方都为空返回 **false**(空密钥不匹配任何呈现,包括空呈现):GenerateKey
// 坏熵源时返回空串并靠 authorized() 拒绝一切,这里的语义必须与之同向 ——
// 返回 true 会成为一个陷阱默认,任何未来调用方拿空 expected 来比较都会全放行。
func KeyMatches(presented, expected string) bool {
	a, b := []byte(presented), []byte(expected)
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	if n == 0 {
		return false
	}
	x := make([]byte, n)
	y := make([]byte, n)
	copy(x, a)
	copy(y, b)
	equal := subtle.ConstantTimeCompare(x, y) == 1
	return len(a) == len(b) && equal
}

// readBody 读并解析请求体(js :66-76)。超限返回 413 —— JS 那边这条抛出的
// Error 会走顶层 catch 变成 500,Go 版按计划改成 413:这是客户端错误,报 500
// 会让调用方以为网关坏了而去重试同一个超大请求。
//
// 读取失败与 JSON 语法错误同为**客户端可修的** 4xx,错误文案用固定短语:
// 底层错误原文(io 错误、语法偏移)属于内部细节,不回给调用方。
func (s *Server) readBody(w *writer, r *http.Request) (map[string]any, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			openAIError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body too large")
			return nil, false
		}
		openAIError(w, http.StatusBadRequest, "invalid_request_error", "could not read request body")
		return nil, false
	}
	// 读体阶段到此结束:解除 ReadTimeout 设下的读死线,再进入可能长时间
	// 写 SSE 的处理阶段(见 New 的两段式注释)。
	_ = http.NewResponseController(w).SetReadDeadline(time.Time{})
	if len(raw) == 0 {
		return map[string]any{}, true
	}
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		// JS 的 JSON.parse 抛错同样走顶层 catch → 500;客户端错误按 4xx 回,
		// 调用方才不会把语法错误当成服务端故障去重试。
		openAIError(w, http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON")
		return nil, false
	}
	body, _ := parsed.(map[string]any)
	if body == nil {
		body = map[string]any{}
	}
	return body, true
}

// complete 把 Config.Complete 包一层:没接线时报一个可归因的 500,而不是
// 让一个 nil 函数调用变成 panic(那会被 recover 成 500,但丢掉原因)。
func (s *Server) complete(ctx context.Context, req engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
	if s.cfg.Complete == nil {
		return engine.Outcome{}, frerrors.Failure{Code: check.CodeServer, Message: "forward: Complete is not wired"}
	}
	return s.cfg.Complete(ctx, req, onChunk)
}

// messageOf 取错误的**人类可读**部分。JS 用的是 `error?.message`(即
// UpstreamError 的 message),不是 `String(error)`;Go 的 Failure.Error() 是
// "CODE: message",直接拿来当响应体会把内部码泄进客户端文案里。
func messageOf(err error) string {
	var f frerrors.Failure
	if errors.As(err, &f) && f.Message != "" {
		return f.Message
	}
	return err.Error()
}

// statusOf 把错误映射成响应状态码。只采纳 400:unknown model 这类「调用方改
// 一下模型名就能修好」的请求错误,回 500 会诱导它去重试。其余一律走 fallback
// —— JS 差分 B7 的约定是 throw 路径恒 500/502,Failure.Status 携带的上游状态
// (401 凭证、429 配额…)不改变这个映射:那是上游的事,不是调用方修得了的。
func statusOf(err error, fallback int) int {
	var f frerrors.Failure
	if errors.As(err, &f) && f.Status == http.StatusBadRequest {
		return f.Status
	}
	return fallback
}

func applyCORS(h http.Header) {
	for _, kv := range corsHeaders {
		h.Set(kv[0], kv[1])
	}
}

// openAIError 写一个 OpenAI 形状的错误体(js :84-86)。param/code 恒为 null:
// 这个网关没有它们的语义,给个 null 比编一个值诚实。
//
// 用结构体而不是 map:map 会被 encoding/json 按字母序排成
// `{code,message,param,type}`,与 JS 的 `{message,type,param,code}` 不同字节。
func openAIError(w http.ResponseWriter, status int, typ, message string) {
	writeJSON(w, status, openAIErrorBody{Error: openAIErrorDetail{
		Message: message,
		Type:    typ,
		Param:   nil,
		Code:    nil,
	}})
}

// writeJSON 照 js :78-82:content-type / content-length / cache-control: no-store。
func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := marshalNoEscape(payload)
	if err != nil {
		body = []byte(`{"error":{"message":"response could not be encoded","type":"server_error","param":null,"code":null}}`)
		status = http.StatusInternalServerError
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// marshalNoEscape 序列化得和 JSON.stringify 一样。
//
// json.Marshal 默认把 <、>、& 转成 \u003c 之类(为了能安全嵌进 HTML),而
// JS 不转。模型输出里出现 </script> 或 &amp; 是常事,两个版本的响应字节必须
// 一致,否则差分验收会在这些字符上误报。
func marshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder 会补一个换行,JSON.stringify 不会。Content-Length 得对得上。
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// stringField 是 JS 的 `String(body.model ?? ”)`:非字符串值也要能变成字符串。
func stringField(body map[string]any, key string) string {
	v, ok := body[key]
	if !ok || v == nil {
		return ""
	}
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprint(x)
	}
}

// baseModelID 把带 "(level)" 后缀的模型名归一成基名(js 的 baseModelId)。
func baseModelID(model string) string { return upstream.BaseModelID(model) }

// randHex 生成 n 字节的十六进制串,用于 chatcmpl-/resp- 的 id(js :205/:313)。
func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		// id 只用于串联一次响应,熵源坏掉时退化成可预测的值也比中断请求好。
		return strings.Repeat("0", n*2)
	}
	return hex.EncodeToString(buf)
}
