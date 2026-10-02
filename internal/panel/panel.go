// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package panel serves the local console. It holds no domain state at all:
// every answer comes from a function value injected by app, so the panel can
// be exercised against fakes without booting a gateway.
//
// The HTTP surface is a deliberate one-to-one port of src/panel.js, including
// its quirks: `/app.js` missing is a 500 rather than a 404 (a 404 would let the
// client's catch stay silent and leave a blank page that reports nothing), and
// every handler failure is a 500 — never a 403 — because the panel listens on
// 127.0.0.1 only and does not authenticate, so a status code must never let a
// caller infer anything about internal state.
//
// One deliberate exception: PUT /api/settings answers 400 when the patch fails
// validation. That is the caller's own malformed input, not a server fault, and
// the client must see it (web/app.js:941-943 only reacts to `!r.ok`) — before
// this, a bad patch was silently accepted, reported as saved, and left an
// unparseable settings.json behind that stopped the gateway from starting.
package panel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"freerouter/internal/logger"
	"freerouter/internal/tracelog"
	"freerouter/web"
)

// bootMarker is the exact placeholder web/index.html must carry. The name is
// load-bearing: an earlier version used `__BOOT__` as the marker and replaced
// the one inside `window.__BOOT__`, producing `window.{...} = __BOOT__` and a
// SyntaxError on the first frame.
const bootMarker = "/*__BOOT_JSON__*/null"

// R1:面板同样要收紧头阶段与空闲超时(Go 的 http.Server 零值等于无限)。
//
// 不设 WriteTimeout:POST /api/probe 会阻塞一整轮探测(几百个节点、几十秒),
// 整请求死线会把一个正在干活的按钮变成 500。ReadHeaderTimeout 堵 slowloris
// 的「连上不发头」,IdleTimeout 堵 keep-alive 上挂死的连接。
const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 60 * time.Second
)

// panelLoopbackHosts 是面板认的 Host 主机名白名单(R2)。
//
// 面板绑在 127.0.0.1 上不等于「只有本机能访问」:DNS-rebinding 让一个恶意
// 页面把 http://evil.example 解析到 127.0.0.1,浏览器照发请求,Host 是
// evil.example —— 于是恶意页面能读 GET /api/status(status.go 里回显转发
// 密钥)、能 PUT /api/settings、能 POST /api/probe。绑定地址挡不住这一层,
// 只有校验 Host 能。
var panelLoopbackHosts = map[string]bool{
	"127.0.0.1": true,
	"localhost": true,
	"::1":       true,
}

// maxBodyBytes mirrors the 1<<20 ceiling in src/panel.js's readJson.
const maxBodyBytes = 1 << 20

// logsLimit is the ring slice the console asks for. src/index.js:1043 wires
// `logs: () => logger.recent(400)`; the number is part of the contract.
const logsLimit = 400

// PanelActions are the mutating verbs the console can invoke. They are
// function values rather than a package import so panel stays at L5 without
// reaching into engine, health or registry.
type PanelActions struct {
	ProbeNow      func(ctx context.Context, force bool) error
	Refresh       func() error
	RefreshLimits func() error
}

// LimitsView is the summary /api/limits reports. JS splits this across two
// halves — refreshLimitsOverlay writes the overlay, status() reads it back —
// and the Go port keeps that split: RefreshLimits does the work, Limits reads
// the current numbers.
type LimitsView struct {
	Rows  int  `json:"rows"`
	Stale bool `json:"stale"`
}

// PanelDeps is everything the console needs. Nothing here is optional except
// Actions and Limits: a nil callback answers with a zero value.
type PanelDeps struct {
	Status        func() any
	GetSettings   func() any
	ApplySettings func(map[string]any) (any, error)
	Actions       PanelActions
	Logs          func(int) []logger.Line
	RouteRecent   func(int) []tracelog.Route
	Version       string
	// AssetDir is an optional override: when it is set, index.html and app.js
	// are read from that directory instead of the copy compiled into the binary.
	// Production leaves it empty (the exe is self-contained); the tests stage
	// their own shell to exercise the malformed and missing cases, which embed
	// can never produce.
	AssetDir string
	Log      func(string)
	// Limits reports the overlay summary that /api/limits returns. It is read
	// after RefreshLimits runs, matching the JS split between writing the
	// overlay and reading it back from status().
	Limits func() LimitsView
}

// Boot is the one snapshot the browser reads as window.__BOOT__. Its shape is
// identical to what the client builds after a poll, so a single render path
// serves both the first paint and every update.
type Boot struct {
	Version      string         `json:"version"`
	Singbox      any            `json:"singbox"`
	Forward      any            `json:"forward"`
	Models       []any          `json:"models"`
	ModelCaps    map[string]any `json:"modelCaps"`
	Limits       any            `json:"limits"`
	RegionModels []any          `json:"regionModels"`
	Nodes        []any          `json:"nodes"`
	Usage        any            `json:"usage"`
	Settings     any            `json:"settings"`
	Logs         []logger.Line  `json:"logs"`
}

// settingsView is the settings subset the console form needs. The JS version
// also sent portBase, portSpan and catchAllPort; those belonged to the
// per-node-port architecture and are gone (a port table, a port base/span and
// a catch-all port do not exist in a single-process gateway).
type settingsView struct {
	SubURLs          any `json:"subUrls"`
	Countries        any `json:"countries"`
	ProbeEnabled     any `json:"probeEnabled"`
	ProbeWorkers     any `json:"probeWorkers"`
	ProbeIntervalMin any `json:"probeIntervalMin"`
	EffortLevel      any `json:"effortLevel"`
	DefaultMaxTokens any `json:"defaultMaxTokens"`
	ForwardPort      any `json:"forwardPort"`
	PanelPort        any `json:"panelPort"`
}

// Server is the console HTTP service.
type Server struct {
	deps PanelDeps
	srv  *http.Server

	// shell is web/index.html as read at construction. A missing file is not
	// fatal — the request answers 500 "console shell missing" — but a file
	// whose marker count is not exactly 1 is: serving it would ship a page
	// that breaks silently in the browser.
	shell        string
	shellMissing bool
	bootErr      error

	port int
}

// New builds the console. It reads the shell up front so a malformed template
// fails at startup instead of at first paint.
func New(deps PanelDeps) *Server {
	s := &Server{deps: deps}
	raw, err := s.readAsset("index.html")
	if err != nil {
		s.shellMissing = true
	} else {
		s.shell = string(raw)
		if n := strings.Count(s.shell, bootMarker); n != 1 {
			s.bootErr = fmt.Errorf("panel: web/index.html 里 %s 出现 %d 次，必须恰好 1 次", bootMarker, n)
		}
	}
	s.srv = &http.Server{
		Handler:           http.HandlerFunc(s.route),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}
	return s
}

// readAsset 取面板的静态文件：设了 AssetDir 就从磁盘读（测试要造畸形壳和缺文件的
// 情形），没设就用编进 exe 的那份。生产路径不设，所以单文件就能跑。
func (s *Server) readAsset(name string) ([]byte, error) {
	if s.deps.AssetDir != "" {
		return os.ReadFile(filepath.Join(s.deps.AssetDir, name))
	}
	return web.FS.ReadFile(name)
}

// Serve blocks on the listener. It reports the construction error first so a
// bad template never reaches a client, then maps the ordinary shutdown error
// to nil the same way forward.Serve does.
func (s *Server) Serve(ln net.Listener) error {
	if s.bootErr != nil {
		return s.bootErr
	}
	if ln == nil {
		return errors.New("panel: nil listener")
	}
	if tcp, ok := ln.Addr().(*net.TCPAddr); ok {
		s.port = tcp.Port
	}
	err := s.srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Close shuts the server down. http.Server.Close closes active and idle
// connections alike, which is the Go equivalent of the JS version's
// closeAllConnections() followed by close().
func (s *Server) Close() error {
	return s.srv.Close()
}

// Port is the bound port, or 0 before Serve has been called.
func (s *Server) Port() int { return s.port }

func (s *Server) logf(format string, args ...any) {
	if s.deps.Log != nil {
		s.deps.Log(fmt.Sprintf(format, args...))
	}
}

// route is the per-request entry point. The panel is the user's only window
// into a running gateway, so a panic in one injected callback must not take
// the whole console down with it.
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if rec := recover(); rec != nil {
			msg := fmt.Sprint(rec)
			if err, ok := rec.(error); ok {
				msg = err.Error()
			}
			s.logf("panel request failed: %s", msg)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": msg})
		}
	}()
	if !s.localRequest(w, r) {
		return
	}
	s.dispatch(w, r)
}

// localRequest 是 R2 的 DNS-rebinding 闸门:请求必须真的发给本机控制台。
//
// 它**不是鉴权** —— 面板仍然没有密钥、仍然把每个内部错误答成 500。这一层
// 判的是「这个请求是不是浏览器里那个恶意页面代发的」:rebinding 攻击下
// Host/Origin 会是攻击者的域名而不是 127.0.0.1,所以按主机名白名单拒绝即可。
// 因此 403 在这里是诚实的(它说的是「这不是发给控制台的请求」),而
// TestNoStateCanDistinguishAuthorizedFromNot 要防的「用状态码推断内部状态」
// 依旧成立:那个 403 与面板内部状态无关,且在任何 Host 合法的请求上都不会出现。
//
// 只认主机名、不比对端口:面板端口可配置,而攻击者伪造 Host 时同样可以写上
// 正确端口;真正拦住 rebinding 的是「主机名必须是回环名」这一条。
func (s *Server) localRequest(w http.ResponseWriter, r *http.Request) bool {
	if !panelLoopbackHosts[hostOnly(r.Host)] {
		s.logf("panel: 拒绝非本机 Host 的请求: %q", r.Host)
		writeJSON(w, http.StatusForbidden, map[string]any{"error": "panel: host is not the local console"})
		return false
	}
	// 浏览器对跨源的非简单请求才会带上 Origin;同源 PUT 也会带(值同源)。
	// 因此「有 Origin 就必须是回环」,没有 Origin 说明不是浏览器发的 ——
	// curl/托盘/测试都走这条路,不能拒。
	if isWriteMethod(r.Method) {
		if origin := r.Header.Get("Origin"); origin != "" && !loopbackURLHost(origin) {
			s.logf("panel: 拒绝非本机 Origin 的写请求: %q", origin)
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "panel: origin is not the local console"})
			return false
		}
		if ref := r.Header.Get("Referer"); ref != "" && !loopbackURLHost(ref) {
			s.logf("panel: 拒绝非本机 Referer 的写请求: %q", ref)
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "panel: referer is not the local console"})
			return false
		}
	}
	return true
}

// hostOnly 去掉 Host 里的端口,IPv6 的方括号也一并去干净,并折叠大小写
// (主机名大小写不敏感,"LOCALHOST" 与 "localhost" 是同一台机器):
// "127.0.0.1:3458" -> "127.0.0.1"、"[::1]:3458" -> "::1"。
func hostOnly(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return strings.ToLower(strings.Trim(h, "[]"))
	}
	return strings.ToLower(strings.Trim(host, "[]"))
}

// isWriteMethod 列出会改状态的方法。GET/HEAD/OPTIONS 不查 Origin:
// 它们本就是只读的,而浏览器同源的普通导航请求不带 Origin。
func isWriteMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// loopbackURLHost 判一个 Origin/Referer 值的主机是不是回环。
// Origin 只有 scheme://host[:port] 三段,Referer 是完整 URL;同一个解析
// 都能吃下。解析失败一律按「不是回环」处理。
func loopbackURLHost(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return false
	}
	return panelLoopbackHosts[strings.ToLower(u.Hostname())]
}

func (s *Server) dispatch(w http.ResponseWriter, r *http.Request) {
	path := normalizePath(r.URL.RequestURI())
	method := r.Method

	switch {
	case method == http.MethodGet && path == "/":
		s.serveShell(w)

	case method == http.MethodGet && path == "/app.js":
		s.serveClientJS(w)

	case method == http.MethodGet && path == "/api/settings":
		writeJSON(w, http.StatusOK, s.deps.GetSettings())

	case method == http.MethodPut && path == "/api/settings":
		patch, err := readBodyJSON(w, r)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		var out any
		if s.deps.ApplySettings != nil {
			out, err = s.deps.ApplySettings(patch)
			if err != nil {
				// 400,不是 500:畸形补丁是调用方自己的输入问题,与内部状态
				// 无关。纯文本是因为 web/app.js:941-943 把非 2xx 的 body
				// 原样塞进 toast —— JSON 会显示成一坨。
				writeText(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		writeJSON(w, http.StatusOK, out)

	case method == http.MethodGet && path == "/api/status":
		writeJSON(w, http.StatusOK, s.deps.Status())

	case method == http.MethodGet && path == "/api/logs":
		lines := []logger.Line{}
		if s.deps.Logs != nil {
			if got := s.deps.Logs(logsLimit); got != nil {
				lines = got
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"lines": lines})

	case method == http.MethodGet && path == "/api/routes":
		limit := clampRoutesLimit(r.URL.Query().Get("limit"))
		rows := []tracelog.Route{}
		if s.deps.RouteRecent != nil {
			if got := s.deps.RouteRecent(limit); got != nil {
				rows = got
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"rows": rows})

	case method == http.MethodPost && path == "/api/probe":
		if s.deps.Actions.ProbeNow != nil {
			// force is what the button means: probe now, do not answer from
			// the result cache. Without it a click right after an automatic
			// round would be a no-op.
			if err := s.deps.Actions.ProbeNow(r.Context(), true); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case method == http.MethodPost && path == "/api/refresh":
		if s.deps.Actions.Refresh != nil {
			if err := s.deps.Actions.Refresh(); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case method == http.MethodPost && path == "/api/limits":
		if s.deps.Actions.RefreshLimits != nil {
			if err := s.deps.Actions.RefreshLimits(); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
		}
		view := LimitsView{}
		if s.deps.Limits != nil {
			view = s.deps.Limits()
		}
		writeJSON(w, http.StatusOK, struct {
			OK    bool `json:"ok"`
			Rows  int  `json:"rows"`
			Stale bool `json:"stale"`
		}{OK: true, Rows: view.Rows, Stale: view.Stale})

	default:
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": fmt.Sprintf("no route for %s %s", method, path),
		})
	}
}

// serveShell injects the boot snapshot into the shell. A missing shell is a
// loud 500: an empty page would look like a working console with no data.
func (s *Server) serveShell(w http.ResponseWriter) {
	if s.shellMissing {
		writeText(w, http.StatusInternalServerError, "console shell missing")
		return
	}
	page, err := injectBoot(s.shell, s.bootstrap())
	if err != nil {
		s.logf("panel: cannot inject boot: %v", err)
		writeText(w, http.StatusInternalServerError, "console shell missing")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(page)))
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, page)
}

// serveClientJS ships the bundle verbatim. The bytes come from the binary, or
// from the override directory when one is set.
func (s *Server) serveClientJS(w http.ResponseWriter) {
	raw, err := s.readAsset("app.js")
	if err != nil {
		s.logf("panel: cannot read client bundle: %v", err)
		writeText(w, http.StatusInternalServerError, "client bundle missing")
		return
	}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// bootstrap flattens status + settings + logs into the single snapshot the
// browser reads. Everything is read back through a JSON round trip so that a
// struct-shaped Status() from app works as well as a map.
func (s *Server) bootstrap() Boot {
	st := toMap(safeCall(s.deps.Status))
	cfg := toMap(safeCall(s.deps.GetSettings))

	var logs []logger.Line
	if s.deps.Logs != nil {
		if got := s.deps.Logs(logsLimit); got != nil {
			logs = got
		}
	}
	if logs == nil {
		logs = []logger.Line{}
	}

	usage := st["usage"]
	if usage == nil {
		usage = map[string]any{
			"today":    map[string]any{"req": 0, "in": 0, "out": 0},
			"requests": 0,
			"byModel":  map[string]any{},
		}
	}

	return Boot{
		Version:      s.deps.Version,
		Singbox:      st["singbox"],
		Forward:      st["forward"],
		Models:       toSlice(st["models"]),
		ModelCaps:    toMap(st["modelCaps"]),
		Limits:       st["limits"],
		RegionModels: toSlice(st["regionModels"]),
		Nodes:        toSlice(st["nodes"]),
		Usage:        usage,
		Settings: settingsView{
			SubURLs:          orEmptySlice(cfg["subUrls"]),
			Countries:        orEmptySlice(cfg["countries"]),
			ProbeEnabled:     cfg["probeEnabled"],
			ProbeWorkers:     cfg["probeWorkers"],
			ProbeIntervalMin: cfg["probeIntervalMin"],
			EffortLevel:      cfg["effortLevel"],
			DefaultMaxTokens: cfg["defaultMaxTokens"],
			ForwardPort:      cfg["forwardPort"],
			PanelPort:        cfg["panelPort"],
		},
		Logs: logs,
	}
}

// injectBoot replaces the marker exactly once. A functional replacement in JS
// existed because `$&` and friends are special in a replacement string and the
// JSON can contain `$`; strings.Replace has no such hazard, so the only thing
// that matters here is that the marker is escaped for the surrounding <script>.
func injectBoot(shell string, boot Boot) (string, error) {
	raw, err := marshalJSON(boot)
	if err != nil {
		return "", err
	}
	escaped := strings.ReplaceAll(string(raw), "<", `\u003c`)
	escaped = strings.ReplaceAll(escaped, "\u2028", `\u2028`)
	escaped = strings.ReplaceAll(escaped, "\u2029", `\u2029`)
	return strings.Replace(shell, bootMarker, escaped, 1), nil
}

// normalizePath drops the query string and trailing slashes, mapping the empty
// result back to "/" — the same three steps as src/panel.js:51.
func normalizePath(raw string) string {
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	raw = strings.TrimRight(raw, "/")
	if raw == "" {
		return "/"
	}
	return raw
}

// clampRoutesLimit reproduces `Math.min(200, Math.max(1, Number(x) || 50))`
// including the counter-intuitive branch: `Number('0')` is falsy, so `?limit=0`
// yields the default 50 and not the floor 1. A non-numeric value must never
// turn the request into a 500.
func clampRoutesLimit(raw string) int {
	n, err := strconv.Atoi(raw)
	if err != nil || n == 0 {
		n = 50
	}
	if n < 1 {
		return 1
	}
	if n > 200 {
		return 200
	}
	return n
}

// readBodyJSON reads the request body with the 1MB ceiling. An empty body is
// an empty object rather than nil, matching `applySettings(patch ?? {})`; a nil
// map would panic the first time app assigns a key into it.
func readBodyJSON(w http.ResponseWriter, r *http.Request) (map[string]any, error) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, errors.New("request body too large")
		}
		return nil, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

// writeJSON mirrors src/panel.js's json(): the charset is spelled out (unlike
// forward's writeJSON, which sends a bare application/json), the length is
// pinned, and the response is never cached.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := marshalJSON(payload)
	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"response could not be encoded"}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeText(w http.ResponseWriter, status int, text string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	_, _ = io.WriteString(w, text)
}

// marshalJSON encodes without HTML escaping so a response body is byte-for-byte
// what JSON.stringify would have produced. U+2028/U+2029 stay escaped: Go's
// encoder does that unconditionally and JS escapes them for the same reason.
func marshalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// safeCall runs an injected accessor, turning a panic into a nil value so the
// boot snapshot can still be assembled. The request-level recover() would
// otherwise abort the whole page on one bad callback.
func safeCall(fn func() any) any {
	if fn == nil {
		return nil
	}
	var out any
	func() {
		defer func() { _ = recover() }()
		out = fn()
	}()
	return out
}

// toMap normalizes a status/settings value into a map. Accepting any value (not
// just map[string]any) keeps the panel working when app hands it a struct.
func toMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	if m, ok := v.(map[string]any); ok {
		if m == nil {
			return map[string]any{}
		}
		return m
	}
	raw, err := marshalJSON(v)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return map[string]any{}
	}
	return m
}

// toSlice normalizes a list value, always yielding a non-nil slice so the boot
// JSON carries [] where JS did rather than null.
func toSlice(v any) []any {
	if v == nil {
		return []any{}
	}
	if s, ok := v.([]any); ok {
		if s == nil {
			return []any{}
		}
		return s
	}
	raw, err := marshalJSON(v)
	if err != nil {
		return []any{}
	}
	var out []any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return []any{}
	}
	return out
}

func orEmptySlice(v any) any {
	if v == nil {
		return []any{}
	}
	return v
}
