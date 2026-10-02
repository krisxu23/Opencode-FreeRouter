// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package panel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"freerouter/internal/logger"
	"freerouter/internal/tracelog"
)

// goodShell 是最小可用外壳：必须含 /*__BOOT_JSON__*/null 恰好一次、含
// <script src="/app.js">，否则 panel.New 会拒绝启动（见 TestBootMarkerMustAppearExactlyOnce）。
const goodShell = `<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>FreeRouter 控制台</title></head>
<body>
<div id="app"></div>
<script>window.__BOOT__ = /*__BOOT_JSON__*/null;</script>
<script src="/app.js"></script>
</body>
</html>
`

const marker = "/*__BOOT_JSON__*/null"

var testClient = &http.Client{Timeout: 10 * time.Second}

// writeAssets 造一个资产目录。html/js 传空串表示该文件不存在 —— 空目录是
// TestAppJSMissingIsFiveHundred 的夹具，不需要额外特判。
func writeAssets(t *testing.T, html, js string) string {
	t.Helper()
	dir := t.TempDir()
	if html != "" {
		if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(html), 0o644); err != nil {
			t.Fatalf("write index.html: %v", err)
		}
	}
	if js != "" {
		if err := os.WriteFile(filepath.Join(dir, "app.js"), []byte(js), 0o644); err != nil {
			t.Fatalf("write app.js: %v", err)
		}
	}
	return dir
}

// startPanel 起一个真实监听的面板。用真监听而不是 httptest.NewServer 是因为
// Serve(ln) 的两步式正是 app.Load 的用法（先 net.Listen 拿端口再 go Serve），
// 用真监听才能顺带验证端口与 Close 的行为。
func startPanel(t *testing.T, deps PanelDeps) string {
	t.Helper()
	s := New(deps)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("panel listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	t.Cleanup(func() {
		_ = s.Close()
		_ = ln.Close()
	})
	base := "http://" + ln.Addr().String()

	// 等第一次握手成功。资产有问题时 Serve 会立刻返回错误，这里要报的是那个
	// 错误本身，而不是下游一串莫名其妙的连接被拒。握手走一个不存在的路径，
	// 免得预先消费掉某个测试正在计数的回调。
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatalf("Serve 立刻返回: %v", err)
		default:
		}
		resp, err := testClient.Get(base + "/__ping")
		if err == nil {
			_ = resp.Body.Close()
			return base
		}
		if time.Now().After(deadline) {
			t.Fatalf("面板在 5s 内没有开始服务: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func do(t *testing.T, method, url, body string) (int, http.Header, string) {
	t.Helper()
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, resp.Header, string(raw)
}

// baseDeps 是最小可用依赖：所有回调都返回零值，具体测试再覆盖需要的那个。
func baseDeps(dir string) PanelDeps {
	return PanelDeps{
		AssetDir: dir,
		Version:  "0.0.0-test",
		Status:   func() any { return map[string]any{} },
		GetSettings: func() any {
			return map[string]any{}
		},
		ApplySettings: func(map[string]any) any { return map[string]any{} },
		Logs:          func(int) []logger.Line { return nil },
		RouteRecent:   func(int) []tracelog.Route { return nil },
	}
}

// TestRootServesHTMLWithInjectedBoot:第一帧就靠注入的 boot JSON 画出真实数字，
// 所以标记必须被替换掉、且后面跟的是 JSON 而不是字面 null。
func TestRootServesHTMLWithInjectedBoot(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	deps := baseDeps(dir)
	deps.Status = func() any { return map[string]any{"models": []any{"m1", "m2"}} }
	deps.GetSettings = func() any {
		return map[string]any{"forwardPort": 3457, "forwardKey": "super-secret-key"}
	}
	base := startPanel(t, deps)

	status, header, body := do(t, http.MethodGet, base+"/", "")
	if status != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", status)
	}
	if ct := header.Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	if cc := header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}
	if strings.Contains(body, marker) {
		t.Errorf("响应体里还剩字面标记 %s —— 注入没发生", marker)
	}
	if !strings.Contains(body, `window.__BOOT__ = {"version":"0.0.0-test"`) {
		t.Errorf("window.__BOOT__ 后面不是 JSON: %s", firstLineWith(body, "window.__BOOT__"))
	}
	if !strings.Contains(body, `"m1"`) {
		t.Errorf("boot 里没有 status 的 models: %s", body)
	}
	// 任务 24 的 TestStatusNeverLeaksForwardKey 在 panel 这一层同样成立：
	// 面板只回显「是否已设 key」，绝不把 key 本身送进浏览器。
	if strings.Contains(body, "super-secret-key") {
		t.Errorf("boot 泄露了 forwardKey: %s", body)
	}
}

// TestBootMarkerMustAppearExactlyOnce:标记数不对说明模板被复制过或写错了，
// 这时产出的是一个静默坏掉的白屏页面，所以必须在启动时就报错。
func TestBootMarkerMustAppearExactlyOnce(t *testing.T) {
	cases := []struct {
		name string
		html string
		want int
	}{
		{"零次", strings.Replace(goodShell, marker, "null", 1), 0},
		{"两次", strings.Replace(goodShell, marker, marker+"\n"+marker, 1), 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeAssets(t, tc.html, "console.log('app')\n")
			s := New(baseDeps(dir))
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			defer func() { _ = ln.Close() }()
			serveErr := s.Serve(ln)
			if serveErr == nil {
				t.Fatalf("标记出现 %d 次时 Serve 应当报错", tc.want)
			}
			if !strings.Contains(serveErr.Error(), marker) {
				t.Errorf("错误信息里应点明标记: %v", serveErr)
			}
			_ = s.Close()
		})
	}
}

// TestBootEscapesClosingScriptTag:status 里出现 </script> 会把注入的 <script>
// 提前闭合，前端第一帧直接白屏且没有任何报错。
func TestBootEscapesClosingScriptTag(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	deps := baseDeps(dir)
	deps.Status = func() any {
		return map[string]any{"singbox": map[string]any{"note": "</script><script>alert(1)</script>"}}
	}
	base := startPanel(t, deps)

	_, _, body := do(t, http.MethodGet, base+"/", "")
	// 只转义 `<`，与 src/panel.js 的 `.replace(/</g, '\\u003c')` 一致：`>` 在
	// <script> 里没有提前闭合的能力，JS 也没转它，多转一个字节阶段 5 的
	// B10 端点差分就会挂。
	if !strings.Contains(body, `\u003cscript>alert(1)`) {
		t.Errorf("</script> 没有被转义: %s", body)
	}
	if strings.Contains(body, "<script>alert(1)") {
		t.Errorf("注入的脚本没有转义，会提前闭合外层 <script>: %s", body)
	}
}

// TestAppJSServedVerbatim:前端是逐字静态资源，不是模板。任何 `\n`、`${}`
// 都必须原样送到浏览器（src/panel.js:19-21 记录了内联进模板字符串的翻车史）。
func TestAppJSServedVerbatim(t *testing.T) {
	js := "const s = `x${1}y`\n// 行尾 U+2028:\u2028\nlet probeFromLogs = /alive/\n"
	dir := writeAssets(t, goodShell, js)
	base := startPanel(t, baseDeps(dir))

	status, header, body := do(t, http.MethodGet, base+"/app.js", "")
	if status != http.StatusOK {
		t.Fatalf("GET /app.js = %d, want 200", status)
	}
	if ct := header.Get("Content-Type"); ct != "text/javascript; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
	if body != js {
		t.Errorf("app.js 不是逐字节送出:\n got %q\nwant %q", body, js)
	}
}

// TestAppJSMissingIsFiveHundred:500 而不是 404。404 会让前端的 catch 静默，
// 页面停在白屏而不报错 —— 一个不报错的坏页面比一个报错的坏页面难查得多。
func TestAppJSMissingIsFiveHundred(t *testing.T) {
	dir := writeAssets(t, "", "")
	base := startPanel(t, baseDeps(dir))

	status, header, body := do(t, http.MethodGet, base+"/app.js", "")
	if status != http.StatusInternalServerError {
		t.Fatalf("GET /app.js = %d, want 500", status)
	}
	if body != "client bundle missing" {
		t.Errorf("body = %q", body)
	}
	if ct := header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q", ct)
	}
}

// TestRootWithoutShellIsFiveHundred:外壳缺失同样是响亮的 500，而不是把进程
// 拖死或悄悄送一个空页面。
func TestRootWithoutShellIsFiveHundred(t *testing.T) {
	dir := writeAssets(t, "", "")
	base := startPanel(t, baseDeps(dir))

	status, _, body := do(t, http.MethodGet, base+"/", "")
	if status != http.StatusInternalServerError {
		t.Fatalf("GET / = %d, want 500", status)
	}
	if !strings.Contains(body, "console shell missing") {
		t.Errorf("body = %q", body)
	}
}

// TestUnknownRouteIsFourOhFourWithMethodAndPath:兜底 404 必须带上方法与路径，
// 否则前端只能看到一句无信息量的 "not found"。
func TestUnknownRouteIsFourOhFourWithMethodAndPath(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	base := startPanel(t, baseDeps(dir))

	status, _, body := do(t, http.MethodGet, base+"/foo", "")
	if status != http.StatusNotFound {
		t.Fatalf("GET /foo = %d, want 404", status)
	}
	if !strings.Contains(body, "no route for GET /foo") {
		t.Errorf("body = %q", body)
	}

	status, _, body = do(t, http.MethodPost, base+"/bar", "")
	if status != http.StatusNotFound {
		t.Fatalf("POST /bar = %d, want 404", status)
	}
	if !strings.Contains(body, "no route for POST /bar") {
		t.Errorf("body = %q", body)
	}
}

// TestStatusSettingsLogsRoutesAllProxyThrough:面板自己没有任何状态，四个读
// 端点都必须原样穿过注入的函数值。
func TestStatusSettingsLogsRoutesAllProxyThrough(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	var statusCalls, settingsCalls, logsCalls, routesCalls int
	deps := baseDeps(dir)
	deps.Status = func() any {
		statusCalls++
		return map[string]any{"nodes": []any{}}
	}
	deps.GetSettings = func() any {
		settingsCalls++
		return map[string]any{"forwardPort": 3457}
	}
	deps.Logs = func(limit int) []logger.Line {
		logsCalls++
		if limit != 400 {
			t.Errorf("Logs limit = %d, want 400（与 src/index.js:1043 的 logger.recent(400) 一致）", limit)
		}
		return []logger.Line{{T: 1, Level: "info", Msg: "hello"}}
	}
	deps.RouteRecent = func(limit int) []tracelog.Route {
		routesCalls++
		return []tracelog.Route{{At: 2, Kind: "route", Model: "m"}}
	}
	base := startPanel(t, deps)

	if status, _, body := do(t, http.MethodGet, base+"/api/status", ""); status != http.StatusOK || !strings.Contains(body, `"nodes"`) {
		t.Errorf("GET /api/status = %d %s", status, body)
	}
	if status, _, body := do(t, http.MethodGet, base+"/api/settings", ""); status != http.StatusOK || !strings.Contains(body, `"forwardPort":3457`) {
		t.Errorf("GET /api/settings = %d %s", status, body)
	}
	if status, _, body := do(t, http.MethodGet, base+"/api/logs", ""); status != http.StatusOK || body != `{"lines":[{"t":1,"level":"info","msg":"hello"}]}` {
		t.Errorf("GET /api/logs = %d %s", status, body)
	}
	if status, _, body := do(t, http.MethodGet, base+"/api/routes", ""); status != http.StatusOK || !strings.HasPrefix(body, `{"rows":[`) {
		t.Errorf("GET /api/routes = %d %s", status, body)
	}
	if statusCalls != 1 || settingsCalls != 1 || logsCalls != 1 || routesCalls != 1 {
		t.Errorf("回调次数 status=%d settings=%d logs=%d routes=%d, want 各 1 次",
			statusCalls, settingsCalls, logsCalls, routesCalls)
	}
}

// TestRoutesLimitIsClamped:钳位规则逐字取自 src/panel.js:94 的
// `Math.min(200, Math.max(1, Number(x) || 50))`，含 `Number('0') || 50` 这个
// 反直觉分支（'0' 走的是默认值 50，不是下限 1）。非数字参数绝不能让请求 500。
func TestRoutesLimitIsClamped(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", 50},
		{"?limit=0", 50},
		{"?limit=-5", 1},
		{"?limit=99999", 200},
		{"?limit=abc", 50},
		{"?limit=37", 37},
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			dir := writeAssets(t, goodShell, "console.log('app')\n")
			got := -1
			deps := baseDeps(dir)
			deps.RouteRecent = func(limit int) []tracelog.Route {
				got = limit
				return nil
			}
			base := startPanel(t, deps)

			status, _, body := do(t, http.MethodGet, base+"/api/routes"+tc.query, "")
			if status != http.StatusOK {
				t.Fatalf("GET /api/routes%s = %d %s", tc.query, status, body)
			}
			if got != tc.want {
				t.Errorf("limit = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestProbeNowAlwaysForces:面板按钮的语义是「现在给我测一遍」，不能命中结果
// 缓存。少了 force，紧接着自动轮次按下的按钮会变成空操作（src/panel.js:99-102）。
func TestProbeNowAlwaysForces(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	called := 0
	forced := false
	deps := baseDeps(dir)
	deps.Actions.ProbeNow = func(_ context.Context, force bool) error {
		called++
		forced = force
		return nil
	}
	base := startPanel(t, deps)

	status, _, body := do(t, http.MethodPost, base+"/api/probe", "")
	if status != http.StatusOK || body != `{"ok":true}` {
		t.Fatalf("POST /api/probe = %d %s", status, body)
	}
	if called != 1 || !forced {
		t.Errorf("probeNow 调用 %d 次, force=%v；必须恰好一次且 force=true", called, forced)
	}
}

// TestLimitsRefreshReturnsRowsAndStale:前端据 stale 显示「数据可能过期」。
//
// 与计划正文的一处偏离：正文的表里写 fake 返回 `(&Overlay{Len:42}, stale=true)`，
// 但 `Overlay` 这个类型在仓库里不存在（总纲 §6 声明的 ExtractOpencodeOverlay/
// ApplyLimitsOverlay 尚未实现），而 §6 又把 RefreshLimits 定形为 `func() error`
// —— 拿不到 rows/stale。因此按 JS 的分工拆成两半：RefreshLimits 只负责刷新，
// Limits 负责读回当前摘要（对应 src/index.js 的 refreshLimitsOverlay 写全局、
// status() 读全局）。
func TestLimitsRefreshReturnsRowsAndStale(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	refreshed := 0
	deps := baseDeps(dir)
	deps.Actions.RefreshLimits = func() error {
		refreshed++
		return nil
	}
	deps.Limits = func() LimitsView { return LimitsView{Rows: 42, Stale: true} }
	base := startPanel(t, deps)

	status, _, body := do(t, http.MethodPost, base+"/api/limits", "")
	if status != http.StatusOK {
		t.Fatalf("POST /api/limits = %d %s", status, body)
	}
	if body != `{"ok":true,"rows":42,"stale":true}` {
		t.Errorf("body = %s", body)
	}
	if refreshed != 1 {
		t.Errorf("RefreshLimits 调用 %d 次, want 1", refreshed)
	}
}

// TestPutSettingsPassesEmptyObjectForEmptyBody:空 body 必须变成空 map 而不是
// nil。JS 的 `applySettings(patch ?? {})` 是同样的兜底，Go 侧传 nil 会在
// `patch["x"] = v` 上直接 panic。
func TestPutSettingsPassesEmptyObjectForEmptyBody(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	var got map[string]any
	called := 0
	deps := baseDeps(dir)
	deps.ApplySettings = func(patch map[string]any) any {
		called++
		got = patch
		return map[string]any{"ok": true}
	}
	base := startPanel(t, deps)

	status, _, body := do(t, http.MethodPut, base+"/api/settings", "")
	if status != http.StatusOK {
		t.Fatalf("PUT /api/settings = %d %s", status, body)
	}
	if called != 1 {
		t.Fatalf("ApplySettings 调用 %d 次, want 1", called)
	}
	if got == nil {
		t.Fatal("ApplySettings 收到 nil map")
	}
	if len(got) != 0 {
		t.Errorf("ApplySettings 收到 %v, want 空 map", got)
	}
}

// TestPutSettingsRejectsOversizedBody:超过 1MB 的 body 必须被拒绝且绝不落到
// ApplySettings 上。
//
// 与计划正文的一处偏离：正文测试表写「4xx」，但 src/panel.js 的 readJson 是
// 直接 throw，冒泡到 createServer 的顶层 catch → **500**。规则 5 要求正文与
// JS 冲突时以 JS 为准（阶段 5 的 B 组端点差分不覆盖这一条，但两版行为一致
// 才是这次重写的目标）。
func TestPutSettingsRejectsOversizedBody(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	called := 0
	deps := baseDeps(dir)
	deps.ApplySettings = func(map[string]any) any {
		called++
		return map[string]any{}
	}
	base := startPanel(t, deps)

	huge := `{"subUrls":["` + strings.Repeat("a", 2<<20) + `"]}`
	status, _, body := do(t, http.MethodPut, base+"/api/settings", huge)
	if status != http.StatusInternalServerError {
		t.Fatalf("PUT 2MB body = %d %s, want 500（与 src/panel.js 的顶层 catch 一致）", status, body)
	}
	if called != 0 {
		t.Errorf("超限 body 仍然调了 ApplySettings %d 次", called)
	}
}

// TestHandlerRecoversFromPanic:一个回调 panic 不能让整个面板陪葬 —— 面板是
// 用户唯一的观测面，它挂了就什么都看不见了。
func TestHandlerRecoversFromPanic(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	deps := baseDeps(dir)
	deps.Status = func() any { panic(errors.New("boom-status")) }
	deps.Logs = func(int) []logger.Line { return []logger.Line{{T: 1, Level: "info", Msg: "still alive"}} }
	base := startPanel(t, deps)

	status, _, body := do(t, http.MethodGet, base+"/api/status", "")
	if status != http.StatusInternalServerError {
		t.Fatalf("panic 后 GET /api/status = %d %s, want 500", status, body)
	}
	if !strings.Contains(body, "boom-status") {
		t.Errorf("500 的 body 应带上 panic 信息: %s", body)
	}

	// 服务必须继续收下一个请求。
	status, _, body = do(t, http.MethodGet, base+"/api/logs", "")
	if status != http.StatusOK || !strings.Contains(body, "still alive") {
		t.Fatalf("panic 之后面板不再服务: %d %s", status, body)
	}
}

// TestNoStateCanDistinguishAuthorizedFromNot:面板只监听 127.0.0.1、不做鉴权
// （src/panel.js:3 的「all local-only」），所以 403 永远不会是一个诚实的答案。
// 内部出错就必须是 500 —— 不能用状态码让调用方推断出「这个路径存在但你没权限」。
func TestNoStateCanDistinguishAuthorizedFromNot(t *testing.T) {
	dir := writeAssets(t, goodShell, "console.log('app')\n")
	deps := baseDeps(dir)
	deps.Status = func() any { panic(errors.New("boom-status")) }
	deps.GetSettings = func() any { panic(errors.New("boom-settings")) }
	deps.Logs = func(int) []logger.Line { panic(errors.New("boom-logs")) }
	deps.RouteRecent = func(int) []tracelog.Route { panic(errors.New("boom-routes")) }
	base := startPanel(t, deps)

	for _, path := range []string{"/api/status", "/api/settings", "/api/logs", "/api/routes"} {
		status, _, body := do(t, http.MethodGet, base+path, "")
		if status != http.StatusInternalServerError {
			t.Errorf("GET %s = %d %s, want 500", path, status, body)
		}
		if status == http.StatusForbidden || status == http.StatusUnauthorized {
			t.Errorf("GET %s 用状态码泄露了内部状态", path)
		}
	}
}

// depsAssetDirForTest 指向仓库真实的 web/ 目录。测试的工作目录是 internal/panel，
// 所以相对路径两级回到 Go 树根 —— 与 app.resolveWebDir 在生产里解析出的同一目录。
func depsAssetDirForTest(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "web")
}

func firstLineWith(text, needle string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return fmt.Sprintf("(没有含 %q 的行)", needle)
}

// TestShellKeepsTrailingNewline 钉住壳的字节尾巴:JS 版的 SHELL 模板串以
// </html>\n 结尾(src/panel.js),逐字搬运的 web/index.html 必须同样带收尾
// 换行 —— 差分 B10 抓到的唯一一处壳字节差,少这一字节,/ 的响应体与 JS 版
// 就不是逐字节相同。
func TestShellKeepsTrailingNewline(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(depsAssetDirForTest(t), "index.html"))
	if err != nil {
		t.Skipf("web/index.html 不可读: %v", err)
	}
	if !strings.HasSuffix(string(raw), "</html>\n") {
		t.Fatalf("shell 必须以 </html>\n 收尾, got %q", string(raw[len(raw)-12:]))
	}
}
