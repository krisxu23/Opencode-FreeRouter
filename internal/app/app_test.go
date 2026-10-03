// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/limits"
	"freerouter/internal/nodeprobe"
)

// load 起一个完整网关，并把关闭交给 t。
//
// 端口是**每个测试一对、只用一次**的。原来的写法假设「包内测试顺序跑，所以都
// 用默认 3457/3458 也不会互相踩」，那个前提只对**连接**成立（server-first FIN
// 之后 Windows 确实能立刻重绑同一个端口），对**监听套接字**不成立：listener
// 关闭后端口会进 TimeWait（本机实测：跑完一轮后 3457 停在 TimeWait、pid 0），
// 下一个测试的 net.Listen 就可能 EADDRINUSE，而 Load 不回落端口 —— 它直接
// 返回错误。CI 的 4 核共享 runner 周期性踩到这条路径（run 37094536832 的
// "Go tests" 步骤就红在这里，本地 -count=4 -cpu=2 多包并发能复现）。
// 端口一对只用一次，就没有可互相踩的窗口；再兜一层：真被别的进程抢走（Windows
// 还会把某些区段整块保留给 Hyper-V）就换一对重试，但只对「监听失败」这一类错误
// 重试，别的错误不许被重试掩盖。
func load(t *testing.T) *Parts {
	t.Helper()
	parts, err := loadWithPorts(t, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	t.Cleanup(func() { _ = parts.Shutdown(context.Background()) })
	return parts
}

// loadWithPorts 在给定 root 上跑 Load，端口由操作系统挑，并写进 settings.json
// （盘上有值 ⇒ 默认值不生效，测试之间不再共用 3457/3458）。extra 是同一份文件里
// 要一并写死的其它键（比如「盘上的值优先」那条要放的 countries）。
//
// 端口不是一次探完再交出去，而是**每一轮重试都重新让 Load 去绑**：先 bind :0 探
// 到号、关掉、再让 Load 去绑同一个号，是探测-释放-重绑的经典竞态 —— 并排跑的其
// 它包（httptest 也从同一动态区取端口）完全可以在中间把它拿走，实测多包并发下
// 正是这样红的。所以这里对「监听失败」这一类错误整轮重来，而不是假装端口是稳的。
func loadWithPorts(t *testing.T, root string, extra map[string]any) (*Parts, error) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		f, p := freePortPair(t)
		if err := writeSettingsPorts(t, root, f, p, extra); err != nil {
			return nil, err
		}
		parts, err := Load(root)
		if err == nil {
			return parts, nil
		}
		lastErr = err
		if !strings.Contains(err.Error(), "监听") {
			return nil, err // 不是端口冲突：不许用重试把它掩盖掉
		}
	}
	return nil, lastErr
}

// writeSettingsPorts 在 Load 之前把端口与 extra 键写进 data/settings.json。
func writeSettingsPorts(t *testing.T, root string, forwardPort, panelPort int, extra map[string]any) error {
	t.Helper()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	body := map[string]any{"forwardPort": forwardPort, "panelPort": panelPort}
	for k, v := range extra {
		body[k] = v
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataDir, "settings.json"), raw, 0o644)
}

// freePortPair 让操作系统挑两个空闲端口：它给的一定不在本机保留区段内，
// 比「从固定区间猜」可靠。探测完立刻关闭，端口只被随后的 Load 用一次。
func freePortPair(t *testing.T) (int, int) {
	t.Helper()
	a := probeFreePort(t)
	b := probeFreePort(t)
	for b == a {
		b = probeFreePort(t)
	}
	return a, b
}

func probeFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("探空闲端口: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// waitBindable 等一组端口真的可以被重新绑定（TimeWait 消散）。只给那些**必须**
// 用固定默认端口的测试用（它们验的就是默认值本身）。
func waitBindable(t *testing.T, ports ...int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		free := true
		lns := make([]net.Listener, 0, len(ports))
		for _, p := range ports {
			ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p))
			if err != nil {
				free = false
				break
			}
			lns = append(lns, ln)
		}
		for _, ln := range lns {
			_ = ln.Close()
		}
		if free {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("端口 %v 在 10s 内始终不可绑定（上一个测试的 TimeWait 没散？）", ports)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func client() *http.Client {
	return &http.Client{Timeout: 10 * time.Second}
}

func forwardURL(p *Parts, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.Settings.ForwardPort, path)
}

func panelURL(p *Parts, path string) string {
	return fmt.Sprintf("http://127.0.0.1:%d%s", p.Settings.PanelPort, path)
}

func get(t *testing.T, url, key string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := client().Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

func post(t *testing.T, url, key, payload string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(payload))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	res, err := client().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

func readJSON(t *testing.T, file string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s 不是 JSON 对象: %v (%s)", file, err, raw)
	}
	return out
}

// TestLoadCreatesEveryStoreUnderRoot 钉住数据目录的布局。路径以 JS 版
// src/index.js:43-62 为准（gateway.log 直接在 data/ 下、路由目录叫 route
// 而不是 routes/），计划正文里那两处写法是笔误。
func TestLoadCreatesEveryStoreUnderRoot(t *testing.T) {
	root := t.TempDir()
	parts, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer func() { _ = parts.Shutdown(context.Background()) }()

	for _, rel := range []string{
		"data/settings.json",
		"data/node-registry.json",
		"data/node-health.json",
		"data/gateway.log",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
			t.Errorf("%s 应存在: %v", rel, err)
		}
	}
	info, err := os.Stat(filepath.Join(root, "data", "route"))
	if err != nil {
		t.Fatalf("data/route/ 应存在: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("data/route 应是目录")
	}
}

func TestLoadOnAnEmptyRootWritesSettingsDefaults(t *testing.T) {
	root := t.TempDir()
	// 这条验的就是「默认值本身」，所以必须用固定的 3457/3458，不能换成一次性
	// 端口；等上一轮测试的 TimeWait 散去再绑，否则它就成了上面那个 flake 的
	// 又一个受害者。
	waitBindable(t, 3457, 3458)
	parts, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer func() { _ = parts.Shutdown(context.Background()) }()

	cfg := readJSON(t, filepath.Join(root, "data", "settings.json"))
	if got := cfg["forwardPort"]; got != float64(3457) {
		t.Errorf("forwardPort = %v, want 3457", got)
	}
	if got := cfg["panelPort"]; got != float64(3458) {
		t.Errorf("panelPort = %v, want 3458", got)
	}
	cc, ok := cfg["countries"].([]any)
	if !ok || len(cc) == 0 {
		t.Errorf("countries 应是非空数组, got %v", cfg["countries"])
	}
	if parts.Settings.ForwardKey == "" {
		t.Errorf("首次启动应生成 forwardKey")
	}
}

func TestLoadDoesNotOverwriteExistingSettings(t *testing.T) {
	root := t.TempDir()
	// countries 是我们写进去的既有值，端口交给 helper 挑（含端口冲突重试）：这条要
	// 验的是「盘上的值优先、文件不被默认值改写」，而不是「某个号恰好没人用」。
	parts, err := loadWithPorts(t, root, map[string]any{"countries": []string{"JP"}})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer func() { _ = parts.Shutdown(context.Background()) }()

	file := filepath.Join(root, "data", "settings.json")
	got := readJSON(t, file)
	if int(got["forwardPort"].(float64)) != parts.Settings.ForwardPort ||
		int(got["panelPort"].(float64)) != parts.Settings.PanelPort {
		t.Fatalf("盘上端口(%v/%v)与运行时(%d/%d)不一致：Load 改写了盘上的值或没按盘上的值起",
			got["forwardPort"], got["panelPort"], parts.Settings.ForwardPort, parts.Settings.PanelPort)
	}
	if parts.Settings.ForwardPort == 3457 || parts.Settings.PanelPort == 3458 {
		t.Fatalf("盘上的端口被默认值盖掉了: %d/%d", parts.Settings.ForwardPort, parts.Settings.PanelPort)
	}
	if cc, ok := got["countries"].([]any); !ok || len(cc) != 1 || cc[0] != "JP" {
		t.Errorf("countries 被动过: %v", got["countries"])
	}
}

// TestForwardAnswersHealthBeforeAnyNodeExists 探活不要求密钥，也不要求池子里
// 有节点：它是「端口通不通」的唯一答案。
func TestForwardAnswersHealthBeforeAnyNodeExists(t *testing.T) {
	parts := load(t)
	status, body := get(t, forwardURL(parts, "/health"), "")
	if status != http.StatusOK {
		t.Fatalf("/health 状态 = %d, want 200 (%s)", status, body)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("/health 不是 JSON: %v (%s)", err, body)
	}
	if out["ok"] != true || out["service"] != "our-free-model" {
		t.Fatalf("/health 体 = %s", body)
	}
}

// TestForwardRefusesRequestsWithNoExits 空池必须拒绝得干净。
//
// 状态码以 JS 源码为最终事实:engine 对「无健康出口」是 throw(src/engine.js:265),
// 冒到 forward.js:103 的顶层 catch → **500**。任务 20 曾按误读钉成 502,阶段 5
// 差分 B 层实测同一场景 JS 回 500 后改判(见 internal/forward/chat.go:332 的
// 注释)。真正要钉的是「错误体里有 no usable exit 这句话」,好让用户一眼看出
// 是没节点而不是模型坏了。
func TestForwardRefusesRequestsWithNoExits(t *testing.T) {
	parts := load(t)
	status, body := post(t, forwardURL(parts, "/v1/chat/completions"), parts.Settings.ForwardKey,
		`{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}`)
	if status != http.StatusInternalServerError {
		t.Fatalf("空池状态 = %d, want 500 (%s)", status, body)
	}
	if !strings.Contains(body, "no usable exit") {
		t.Fatalf("空池错误体应含 no usable exit, got %s", body)
	}
	var out struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("错误体不是 JSON: %v (%s)", err, body)
	}
	if out.Error.Type != "server_error" {
		t.Errorf("error.type = %q, want server_error", out.Error.Type)
	}
}

// TestForwardRequiresTheKeyOnRealRoutes 鉴权先于池子判断：否则未授权者能靠
// 状态码差异枚举「这台机器有没有节点」。
func TestForwardRequiresTheKeyOnRealRoutes(t *testing.T) {
	parts := load(t)
	status, body := get(t, forwardURL(parts, "/v1/models"), "")
	if status != http.StatusUnauthorized {
		t.Fatalf("无 key /v1/models 状态 = %d, want 401 (%s)", status, body)
	}

	status, body = get(t, forwardURL(parts, "/v1/models"), parts.Settings.ForwardKey)
	if status != http.StatusOK {
		t.Fatalf("带 key /v1/models 状态 = %d, want 200 (%s)", status, body)
	}
	var out struct {
		Object string `json:"object"`
		Data   []any  `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("模型列表不是 JSON: %v (%s)", err, body)
	}
	if out.Object != "list" {
		t.Errorf("object = %q, want list", out.Object)
	}
	if len(out.Data) < 10 {
		t.Errorf("空健康表下仍应给出至少 10 个免费模型, got %d", len(out.Data))
	}
}

func TestShutdownIsSafeTwice(t *testing.T) {
	parts := load(t)
	if err := parts.Shutdown(context.Background()); err != nil {
		t.Fatalf("第一次 Shutdown: %v", err)
	}
	if err := parts.Shutdown(context.Background()); err != nil {
		t.Fatalf("第二次 Shutdown 应无害: %v", err)
	}
	var nilParts *Parts
	if err := nilParts.Shutdown(context.Background()); err != nil {
		t.Fatalf("零值 Parts 上 Shutdown 应无害: %v", err)
	}
}

func TestShutdownFlushesEveryStore(t *testing.T) {
	root := t.TempDir()
	// 这条只关心「Shutdown 把每个存储都落盘」，端口给它一对一次性的：裸 Load
	// 会去绑默认的 3457/3458，而那是上面那个 TimeWait flake 的靶子。
	parts, err := loadWithPorts(t, root, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	parts.Health.MarkProbe("node-a", nodeprobe.ProbeResult{State: nodeprobe.StateDead})
	if err := parts.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	disk := readJSON(t, filepath.Join(root, "data", "node-health.json"))
	nodes, _ := disk["nodes"].(map[string]any)
	if len(nodes) != 1 {
		t.Fatalf("node-health.json 有 %d 行, want 1", len(nodes))
	}
	if _, ok := nodes["node-a"]; !ok {
		t.Fatalf("node-health.json 缺 node-a: %v", nodes)
	}
}

func TestLoadFailureLeavesThePortFree(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:3457")
	if err != nil {
		t.Skipf("3457 已被占用，无法构造冲突: %v", err)
	}
	defer ln.Close()

	parts, err := Load(t.TempDir())
	if err == nil {
		_ = parts.Shutdown(context.Background())
		t.Fatal("转发端口被占时 Load 必须报错")
	}
	if !strings.Contains(err.Error(), "3457") {
		t.Fatalf("错误里应点名端口: %v", err)
	}
}

func TestRootDirPrefersTheEnvironmentVariable(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("FREEROUTER_DATA", dir)
	got, err := RootDir()
	if err != nil {
		t.Fatalf("RootDir: %v", err)
	}
	if got != dir {
		t.Fatalf("RootDir = %q, want %q", got, dir)
	}
}

// TestConcurrentShutdownAndCompleteIsRaceFree 在 -race 下钉住「关停与在途请求
// 并发」：关停要取消 ctx、关监听、落盘，请求要读池子与设置，两条路不许抢同一
// 块内存。
func TestConcurrentShutdownAndCompleteIsRaceFree(t *testing.T) {
	parts := load(t)
	url := forwardURL(parts, "/v1/chat/completions")
	key := parts.Settings.ForwardKey

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodPost, url,
				strings.NewReader(`{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}`))
			if err != nil {
				return
			}
			req.Header.Set("Authorization", "Bearer "+key)
			res, err := client().Do(req)
			if err != nil {
				return // 关停后连接被拒是预期结果之一
			}
			_, _ = io.Copy(io.Discard, res.Body)
			_ = res.Body.Close()
		}()
	}
	time.Sleep(5 * time.Millisecond)
	if err := parts.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	wg.Wait()
}

// TestApplyOverlayPureKernel 钉住 Parts.applyOverlay 抽出的纯函数内核:差分
// 验收(difftest)要从包外喂 JS 版 applyLimitsOverlay 的同份输入,而纯函数是
// 唯一不需要装配整个 Parts 就能调到的生产逻辑。四条规则各一条:
// 覆盖容量、跟随 reasoning、非回退行不动 vision、回退行才吃 attachment 位。
func TestApplyOverlayPureKernel(t *testing.T) {
	no := false
	yes := true
	list := []catalog.Model{
		{ID: "a", Vision: true, Reasoning: false, ContextWindow: 100, MaxOutput: 10},
		{ID: "b", Vision: true, Reasoning: false, ContextWindow: 100, MaxOutput: 10},
		{ID: "c", Vision: false, Reasoning: true, ContextWindow: 131072, MaxOutput: 32768},
		{ID: "d", Vision: true, Reasoning: true, ContextWindow: 131072, MaxOutput: 32768},
		{ID: "e", Vision: false, Reasoning: true, ContextWindow: 200, MaxOutput: 30},
	}
	byID := map[string]limits.OverlayRow{
		"a": {ContextWindow: 999, MaxOutput: 99},
		"b": {ContextWindow: 999, MaxOutput: 99, Reasoning: &yes},
		"c": {ContextWindow: 999, MaxOutput: 99, Vision: &yes},
		"d": {ContextWindow: 999, MaxOutput: 99, Vision: &yes},
		"e": {ContextWindow: 999, MaxOutput: 99, Vision: &no},
	}
	got := ApplyOverlay(list, byID)
	if got[0].ContextWindow != 999 || got[0].MaxOutput != 99 || got[0].Reasoning != false || !got[0].Vision {
		t.Fatalf("row a: %+v", got[0])
	}
	if !got[1].Reasoning {
		t.Fatalf("row b: reasoning must follow the overlay")
	}
	if !got[2].Vision || got[2].ContextWindow != 999 {
		t.Fatalf("row c: fallback row must take the overlay vision bit: %+v", got[2])
	}
	// d 是 131072/32768 但 vision=true —— 不是回退行,vision 必须保持本地探测结论。
	if !got[3].Vision || got[3].ContextWindow != 999 {
		t.Fatalf("row d: %+v", got[3])
	}
	if got[4].Vision {
		t.Fatalf("row e: fallback row with attachment=false must flip vision off")
	}
	if len(got) != len(list) {
		t.Fatalf("overlay must not grow or drop rows")
	}
	if got := ApplyOverlay(list, nil); len(got) != len(list) {
		t.Fatalf("empty overlay must return the input unchanged")
	}
}

// TestJSONNumberOrNilIsTheSettingsBridgeToTheBudget:B2。这个函数是
// settings.defaultMaxTokens(JSON 的 null / 数字)与 adapter 的 int 约定
// (<=0 视同未设置)之间的唯一桥;在 T3 之前它零调用点,settings 里的值
// 根本到不了热路径。四个分支各钉一条。
func TestJSONNumberOrNilIsTheSettingsBridgeToTheBudget(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want int
		ok   bool
	}{
		{"null 是未设置", nil, 0, false},
		{"正数原样取出", float64(4096), 4096, true},
		{"int 也认", 8192, 8192, true},
		{"0 与负数视同未设置", float64(0), 0, false},
		{"非数字是未设置", "4096", 0, false},
	}
	for _, c := range cases {
		got, ok := jsonNumberOrNil(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("%s: jsonNumberOrNil(%v) = (%d,%v), want (%d,%v)", c.name, c.in, got, ok, c.want, c.ok)
		}
	}
}
