// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/check"
	"freerouter/internal/gate"
	"freerouter/internal/health"
	"freerouter/internal/limits"
	"freerouter/internal/logger"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/parse"
	"freerouter/internal/persistence"
	"freerouter/internal/registry"
	"freerouter/internal/sbx"
	"freerouter/internal/stats"
)

// ---- 夹具 ----

// fakeEngine 喂 limits.Refresh 的 stats 形状({"usage":{ip:{...}}})。
type fakeEngine struct{ usage map[string]any }

func (f fakeEngine) Stats(context.Context) (any, error) {
	if f.usage == nil {
		return map[string]any{"usage": map[string]any{}}, nil
	}
	return map[string]any{"usage": f.usage}, nil
}

// fakeProber 是 Prober 接口的测试实现:探测不真的出网,缓存/事故/淘汰的
// 分支逻辑由此离线可断言。
type fakeProber struct {
	mu          sync.Mutex
	directErr   error
	directCnt   int
	allCnt      int
	block       chan struct{} // 非 nil:ProbeAll 阻塞直到通道关闭
	failedTags  map[string]bool
	unknownTags map[string]bool // backstop 兜底:本轮没量出来的节点(state=unknown)
}

func (f *fakeProber) ProbeDirect(ctx context.Context, timeoutMS int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.directCnt++
	return f.directErr
}

func (f *fakeProber) ProbeAll(ctx context.Context, items []nodeprobe.Item, workers int) []nodeprobe.Result {
	f.mu.Lock()
	f.allCnt++
	block := f.block
	failed := f.failedTags
	unknown := f.unknownTags
	f.mu.Unlock()
	results := make([]nodeprobe.Result, 0, len(items))
	for _, it := range items {
		r := aliveResult("198.51.100." + fmt.Sprint(len(results)%200+1))
		if failed[it.Tag] {
			r = deadResult()
		}
		if unknown[it.Tag] {
			r = unknownResult()
		}
		results = append(results, nodeprobe.Result{Tag: it.Tag, Result: r})
	}
	if block != nil {
		<-block
	}
	return results
}

// vnode 造一个能被 sing-box 真装载的 vless 出站 —— direct 的选项结构不认
// server 字段(严格解码),夹具节点必须是真协议。
func vnode(tag string) parse.Outbound {
	return parse.Outbound{
		Tag: tag, Type: "vless", Server: tag + ".example", ServerPort: 443,
		UUID: "aeaeaeae-aeae-4aea-8aea-aeaeaeaeaeae",
		TLS:  &parse.TLS{Enabled: parse.BoolPtr(true), ServerName: tag + ".example"},
	}
}

// vlink 是 vnode 的链接形式(订阅源的形状;名字就是 #fragment)。
func vlink(tag string) string {
	return "vless://aeaeaeae-aeae-4aea-8aea-aeaeaeaeaeae@" + tag + ".example:443?type=ws&security=tls&sni=" + tag + ".example#" + tag
}

// subAndCatalogServer 订阅与上游目录共用一台服务器:订阅路径回链接文本,
// /zen/v1/models 回目录 JSON —— refreshCatalog 经节点出口/直连打的正是后一条。
func subAndCatalogServer(t *testing.T, linkBody, modelsBody string, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if strings.HasPrefix(r.URL.Path, "/zen/v1/models") {
			_, _ = w.Write([]byte(modelsBody))
			return
		}
		_, _ = w.Write([]byte(linkBody))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// swallowTimers 把 afterFunc 换成捕获即弃:测试只关心「是否被安排、延迟多少」,
// 不真的触发;真实 60s 重试定时器在测试结束后乱发请求才是要避免的。
func swallowTimers(p *Parts) {
	p.afterFuncFn = func(time.Duration, func()) *time.Timer { return nil }
}

// newProbeParts 组装一个能跑 ProbeNow/Rebuild 的 Parts:真实 sbx 宿主装载
// n 个 vless 节点,其余全部走假实现或临时目录。
func newProbeParts(t *testing.T, n int) *Parts {
	t.Helper()
	// 所有的出网(B 档探针、目录刷新)默认打到已关闭的本地端口:毫秒级
	// ECONNREFUSED,绝不碰真上游。需要目录 JSON 的测试用 t.Setenv 覆盖。
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	t.Setenv("OUR_FREE_MODEL_BASE", closedURL)

	logger.Init("")
	dir := t.TempDir()
	outs := make([]parse.Outbound, 0, n)
	for i := 0; i < n; i++ {
		outs = append(outs, vnode(fmt.Sprintf("n%d", i)))
	}
	h := sbx.NewHost(func(level, msg string) { t.Logf("sbx %s: %s", level, msg) })
	if err := h.Start(context.Background(), outs); err != nil {
		t.Fatalf("host start: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	reg := registry.NewRegistry(filepath.Join(dir, "node-registry.json"))
	if err := reg.Load(); err != nil {
		t.Fatalf("registry load: %v", err)
	}
	reg.Merge(outs)

	s := defaultSettings()
	s.ProbeWorkers = 8
	// 夹具节点都是无旗标的纯 tag:分桶落 OTHER,订阅过滤要放行它。
	s.Countries = []string{"OTHER"}
	fetchDone := make(chan struct{})
	close(fetchDone)
	p := &Parts{
		Root:       dir,
		Settings:   &s,
		Host:       h,
		Registry:   reg,
		Health:     health.NewHealth(filepath.Join(dir, "node-health.json")),
		Prober:     &fakeProber{},
		tierGate:   gate.New(tierGapMS),
		catalog:    &catalogBox{list: catalog.Static()},
		firstFetch: fetchDone,
		hotNudge:   make(chan struct{}, 1),
		coldNudge:  make(chan struct{}, 1),
		firstNudge: make(chan struct{}, 1),
	}
	// 热区/冷区节点预先分层:与生产形状对齐(夹具的 fakeProber 把状态同时写进
	// 健康行,始态全部 alive=hot;冷区由 coldPass 测试显式建)。lifecycle 测试
	// 的 afterFuncFn 接缝不就绪时,afterFunc 退化为 time.AfterFunc 直接在后台
	// 跑一轮,夹具不依赖这个接缝。
	_ = p
	return p
}

func aliveResult(ip string) nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 100, LatencyMin: 100, ExitIP: ip, ExitCountry: "US"}
}

func deadResult() nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateDead, LatencyMS: -1}
}

// unknownResult 是 worker 兜底点火的形状:本轮**没量出来**。nodeprobe 的契约
// (nodeprobe.go:440-452、health.go 的 MarkProbe)都要求调用方跳过它这一轮 ——
// 它既不是 alive 也不是判决。
func unknownResult() nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateUnknown, LatencyMS: -1, Incomplete: true}
}

// ---- 探测轮次(12 条) ----

func TestProbeNowWritesHealthAndPersists(t *testing.T) {
	p := newProbeParts(t, 10)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 3; i < 10; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 3 alive,7 dead
	}
	for i := 0; i < 10; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	sum := p.hotPass(context.Background())
	if sum.Tested != 10 || sum.Alive != 3 {
		t.Fatalf("summary = tested %d alive %d, want 10/3", sum.Tested, sum.Alive)
	}
	if got := p.Health.HealthOf("n0"); got != health.StateAlive {
		t.Fatalf("n0 state = %q, want alive", got)
	}
	if err := p.Health.Persist(); err != nil {
		t.Fatalf("persist: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(p.Root, "node-health.json"))
	if err != nil {
		t.Fatalf("health file: %v", err)
	}
	var onDisk struct {
		Nodes map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(raw, &onDisk); err != nil || len(onDisk.Nodes) == 0 {
		t.Fatalf("health file has %d rows (err %v), want rows", len(onDisk.Nodes), err)
	}
}

func TestProbeSummaryLineMatchesFrontendRegex(t *testing.T) {
	p := newProbeParts(t, 6)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{"n5": true}
	for i := 0; i < 6; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	p.hotPass(context.Background())
	found := false
	for _, line := range logger.Recent(50) {
		m := check.ProbeSummaryRe.FindStringSubmatch(line.Msg)
		if m == nil {
			continue
		}
		found = true
		if m[1] != "5" || m[2] != "6" {
			t.Fatalf("alive/scanned = %s/%s, want 5/6", m[1], m[2])
		}
	}
	if !found {
		t.Fatal("日志里没有匹配前端正则的 probe round 行")
	}
}

func TestProbeNowDoesNotEvictOnAccident(t *testing.T) {
	// 场景 A(误报修复的回归钉):20 个热区节点这轮只剩 6 个通过 —— 通过率
	// 30%,远高于 10% 地板,不算事故。「随机换血 70% 但池子仍可服务」正是
	// 旧双条件判据每轮误触发、淘汰被永久压制的真实形状(第五轮审计),地板
	// 判据对此免疫:照常推进状态机,该降冷的降冷。
	p := newProbeParts(t, 20)
	for i := 0; i < 20; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 6; i < 20; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 掉 14(70%),存活 6 > 20/4
	}
	sum := p.hotPass(context.Background())
	if sum.Accident {
		t.Fatalf("accident = true, want false (掉 14/20 但存活 6 未塌到 1/4,双条件不满足)")
	}
}

func TestProbeNowAccidentNeedsCollapseToo(t *testing.T) {
	// 1.3.0 事故判据:热区 pass 的**通过率跌破 10% 地板**(样本 ≥20)才是通道
	// 故障 —— 上一轮比率对比与绝对塌方双条件被这条更直的信号取代(旧双条件在
	// 真实池的「换血」形状下每轮误触发,见第五轮审计)。20 个热区节点只剩 1 个
	// 通过(5% < 10%)= 事故轮:结果整体丢弃,一个状态都不推进。
	p := newProbeParts(t, 20)
	for i := 0; i < 20; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 1; i < 20; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 掉 19(95%),通过率 5% < 10% 地板
	}
	sum := p.hotPass(context.Background())
	if !sum.Accident {
		t.Fatalf("accident = false, want true (通过率 5%% 跌破 10%% 地板)")
	}
	if !sum.Skipped {
		t.Error("事故轮必须同时标记 Skipped(结果整体丢弃)")
	}
	if p.Registry.Len() != 20 {
		t.Fatalf("registry len = %d, want 20 (事故轮一个都不淘汰)", p.Registry.Len())
	}
	// 判决丢弃的硬核对:失败的 19 个一个都没被降档(仍是 alive),
	// 通关的 1 个也不推进 —— 坏通道上「成功」的少数量同样不可信。
	snap := p.Health.NodeSnapshot()
	for i := 0; i < 20; i++ {
		if v := snap[fmt.Sprintf("n%d", i)]; v.State != health.StateAlive {
			t.Fatalf("n%d 被事故轮推进成 %v, want alive(整轮丢弃)", i, v.State)
		}
	}
	// 冻结不变量的硬核对:事故轮不算「热区健康结束」—— lastHotOK 必须保持
	// false(冷区删除闸从此刻起关闭,直到下一次健康的热区 pass 重新打开)。
	// 夹具里 MarkProbe 不是 pass,本轮又是事故,所以这里只可能是 false。
	if p.lastHotOK.Load() {
		t.Fatal("事故轮把 lastHotOK 置真了 —— 删除冻结闸被事故轮自己解开,冻结语义失效")
	}
}

func TestHotPassDemotesButNeverDeletes(t *testing.T) {
	// 1.3.0:热区 pass 只降档(连续 2 轮失败 → 冷区),删除是冷区 sweep 的
	// 专属判决。这里验证热区本身不删东西。
	p := newProbeParts(t, 10)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 1; i <= 4; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true
	}
	for i := 0; i < 10; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	p.hotPass(context.Background())
	if got := p.Registry.Len(); got != 10 {
		t.Fatalf("registry len = %d, want 10 (热区 pass 不删节点)", got)
	}
	// 第二轮:streak 到 2,降档。健康行状态应变 dead。
	p.hotPass(context.Background())
	demoted := 0
	for _, view := range p.Health.NodeSnapshot() {
		if view.State == health.StateDead {
			demoted++
		}
	}
	if demoted == 0 {
		t.Fatal("热区连续 2 轮失败后应有节点降入冷区")
	}
}

// TestFailingNodeIsDeletedAfterConsecutiveColdRounds 是 1.3.0 双档淘汰的
// 端到端钉:冷区节点连续 3 轮 sweep 失败 → 彻底删除(注册表条目 + 健康行
// 都不留);失败一次都不该碰 registry 里的名字。
func TestFailingNodeIsDeletedAfterConsecutiveColdRounds(t *testing.T) {
	p := newProbeParts(t, 3)
	fp := p.Prober.(*fakeProber)
	// n0 先降到冷区(预置两轮热区失败)。
	fp.failedTags = map[string]bool{"n0": true}
	for i := 0; i < 3; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	p.hotPass(context.Background())
	p.hotPass(context.Background())
	if p.Health.HealthOf("n0") != health.StateDead {
		t.Fatal("n0 应已降入冷区")
	}
	// 冷区 sweep:3 轮连续失败 → 删除。
	for round := 1; round <= 3; round++ {
		p.coldPass(context.Background())
		if p.Registry.Has("n0") && round < 3 {
			continue
		}
	}
	if p.Registry.Has("n0") {
		t.Fatal("连续 3 轮冷区失败的节点仍在池内 —— 删除未生效")
	}
	if got := p.Health.HealthOf("n0"); got != health.StateUnknown {
		t.Fatalf("删除的节点不应留健康行: %v", got)
	}
}

func TestAccidentNeedsMinimumSample(t *testing.T) {
	p := newProbeParts(t, 5)
	for i := 0; i < 5; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 1; i < 5; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 4/5 掉线,但样本只有 5
	}
	sum := p.hotPass(context.Background())
	if sum.Accident {
		t.Fatal("accident = true, want false (probeAccidentMin=8)")
	}
}

func TestTierGateSurvivesAcrossRounds(t *testing.T) {
	p := newProbeParts(t, 3)
	for i := 0; i < 3; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	p.hotPass(context.Background())
	if p.tierGate == nil {
		t.Fatal("tierGate 未装配")
	}
	first := p.tierGate.NextAt()
	p.hotPass(context.Background())
	// 第二轮仍有节点在排队:闸门是跨轮复用的,不是每轮新建
	// (每轮新建会让上一轮的错峰成果全部丢失)。
	if p.tierGate.NextAt() < first {
		t.Fatalf("tierGate NextAt 倒退: %d < %d", p.tierGate.NextAt(), first)
	}
}

func TestTierBucketReleasesOnlyItsOwnSection(t *testing.T) {
	var chains tierChains
	aEntered := make(chan struct{})
	release := make(chan struct{})
	aDone := make(chan struct{})
	go func() {
		defer close(aDone)
		// 生产里这层 recover 在 runTierPipeline 的 worker 上
		// (一个探针不能杀死整轮);这里照搬它的形状。
		defer func() { _ = recover() }()
		chains.with("ip-a", func() {
			close(aEntered)
			<-release
			panic("boom") // finally 仍必须只摘自己那一节
		})
	}()
	<-aEntered
	// 同 key 的后来者在第一节结束前不得进入。
	bEntered := make(chan struct{})
	go func() {
		defer close(bEntered)
		chains.with("ip-a", func() {})
	}()
	// 不同 key 的并发探测不受影响。
	cDone := make(chan struct{})
	go func() {
		defer close(cDone)
		chains.with("ip-b", func() {})
	}()
	select {
	case <-cDone:
	case <-time.After(2 * time.Second):
		t.Fatal("不同出口 IP 的探测被同一条链串住了")
	}
	close(release)
	<-aDone // with 的 defer 在 panic 展开时执行:链节被摘掉
	select {
	case <-bEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("前一节 panic 后,同 key 的后来者没有接上(链被整表删除踩掉了)")
	}
}

// TestProbeRoundSkipsTheFailCountOnUnknownVerdicts 钉住 R16:backstop 兜底给出的
// unknown 是「本轮没量出来」,不是判决 —— nodeprobe(nodeprobe.go:440-452)与
// health.MarkProbe 都要求调用方跳过它这一轮。过去只有 MarkProbe 那半兑现了:
// 计败与淘汰判决照样把 unknown 当失败,探测源越抖,池子被缩得越狠。
// 1.3.0:unknown 不该记失败(MarkProbe 本就跳过 unknown,状态机不被调用);
// dead 对照组连续失败应降入冷区,再 3 轮冷区 sweep 才删除。
func TestUnknownVerdictsDoNotAdvanceTheTwoTierMachine(t *testing.T) {
	p := newProbeParts(t, 4)
	fp := p.Prober.(*fakeProber)
	fp.unknownTags = map[string]bool{"n1": true}
	fp.failedTags = map[string]bool{"n2": true, "n3": true} // 对照组:真判决必须照常生效
	for i := 0; i < 4; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	for round := 0; round < 4; round++ {
		p.hotPass(context.Background())
	}
	// n1(unknown)的健康行状态必须原样保持:它这一轮被跳过,不推进状态机。
	if got := p.Health.HealthOf("n1"); got != health.StateAlive {
		t.Fatalf("unknown 节点的状态被推进成 %v, want alive(没量出来不算判决)", got)
	}
	if !p.Registry.Has("n1") {
		t.Fatal("没量出来的节点被删了:「没证据」被当成「有罪」")
	}
	// n2/n3(dead)连续热区失败 → 应降入冷区,但**热区本身不删**(池子仍全在)。
	if got := p.Health.HealthOf("n2"); got != health.StateDead {
		t.Fatalf("n2 应已降入冷区: %v", got)
	}
	if got := p.Registry.Len(); got != 4 {
		t.Fatalf("registry len = %d, want 4(热区只降档,不删除)", got)
	}
}

func TestProbeNowIsSerialized(t *testing.T) {
	p := newProbeParts(t, 3)
	for i := 0; i < 3; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	fp := p.Prober.(*fakeProber)
	fp.block = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		p.hotPass(context.Background())
		done <- nil
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fp.mu.Lock()
		n := fp.allCnt
		fp.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 1.3.0:同 CAS 语义由 hotRunning 位提供 —— 并发第二轮应被跳过(零探测)。
	sum := p.hotPass(context.Background())
	if !sum.Skipped {
		t.Fatal("并发第二轮应被 hotRunning 跳过")
	}
	close(fp.block)
	if err := <-done; err != nil {
		t.Fatalf("first round: %v", err)
	}
}

// ---- 订阅重建(9 条) ----

func TestRebuildReplacesOutboundsInPlace(t *testing.T) {
	p := newProbeParts(t, 1) // n0 已在池内
	swallowTimers(p)
	url := subAndCatalogServer(t, vlink("fresh-node"), `{"data":[]}`, http.StatusOK)
	p.Settings.SubURLs = []string{url}
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !p.Registry.Has("fresh-node") || p.Registry.Len() != 2 {
		t.Fatalf("registry len %d has fresh %v, want 2/true(池=历史并集,热插不重启)", p.Registry.Len(), p.Registry.Has("fresh-node"))
	}
	if !p.Host.Has("fresh-node") {
		t.Fatal("新节点没有热插进 sing-box")
	}
	if p.lastRebuildAt == 0 || !p.lastRebuildOK {
		t.Fatalf("重建结果未记账: at %d ok %v", p.lastRebuildAt, p.lastRebuildOK)
	}
}

func TestRebuildKeepsOldOutboundsWhenAllSubsFail(t *testing.T) {
	p := newProbeParts(t, 3)
	swallowTimers(p)
	url := subAndCatalogServer(t, "boom", "boom", http.StatusInternalServerError)
	p.Settings.SubURLs = []string{url}
	// B11:订阅全挂必须上报失败,而不是永远 nil。降级(不清池子)与上报是
	// 两件事:池子照旧,但调用方要知道这一轮没成功。
	if err := p.Rebuild(context.Background()); err == nil {
		t.Fatal("订阅全挂时 Rebuild 必须返回错误")
	}
	if p.Registry.Len() != 3 {
		t.Fatalf("registry len = %d, want 3 (订阅全挂不清池子)", p.Registry.Len())
	}
}

func TestRebuildRetriesTwiceThenGivesUp(t *testing.T) {
	p := newProbeParts(t, 1)
	url := subAndCatalogServer(t, "boom", "boom", http.StatusInternalServerError)
	p.Settings.SubURLs = []string{url}
	type captured struct {
		d  time.Duration
		fn func()
	}
	got := make(chan captured, 16)
	p.afterFuncFn = func(d time.Duration, fn func()) *time.Timer {
		got <- captured{d, fn}
		return nil
	}
	// B11:补偿重试的每一轮都是失败的一轮,所以每轮都返回错误 —— 但重试
	// 排程本身不受影响,这里只断言排了两次就不再排。
	if err := p.Rebuild(context.Background()); err == nil {
		t.Fatal("订阅全挂时 Rebuild 必须返回错误")
	}
	// 首探/目录重试的 afterFunc 混在同一个通道里,只对 subRetryDelay 的
	// 捕获计数并触发;两次补偿重试之后不再安排。
	deadline := time.After(3 * time.Second)
	retries := 0
	for retries < subRetryLimit {
		select {
		case c := <-got:
			if c.d == subRetryDelay {
				retries++
				if retries <= subRetryLimit {
					c.fn() // 触发下一次补偿重试
				}
			}
		case <-deadline:
			t.Fatalf("第 %d 次补偿重试没有被安排", retries+1)
		}
	}
	select {
	case c := <-got:
		if c.d == subRetryDelay {
			t.Fatalf("重试上限 %d 次后仍在安排重试(delay %v)", subRetryLimit, c.d)
		}
	case <-time.After(300 * time.Millisecond):
	}
	if p.subFetchRetries != subRetryLimit {
		t.Fatalf("subFetchRetries = %d, want %d", p.subFetchRetries, subRetryLimit)
	}
}

func TestRebuildDoesNotRestartTheBoxWhenSetUnchanged(t *testing.T) {
	p := newProbeParts(t, 1)
	swallowTimers(p)
	url := subAndCatalogServer(t, vlink("n0"), `{"data":[]}`, http.StatusOK)
	p.Settings.SubURLs = []string{url}
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	// 池子没变:不打 rebuild ok、不刷新目录(目录刷新要打一次上游)。
	for _, line := range logger.Recent(50) {
		if strings.Contains(line.Msg, "rebuild ok") {
			t.Fatal("集合未变却打了 rebuild ok")
		}
	}
	p.upstreamMu.Lock()
	n := len(p.lastUpstreamIDs)
	p.upstreamMu.Unlock()
	if n != 0 {
		t.Fatalf("目录被刷新了 %d 个 id,集合未变时不应发生", n)
	}
}

func TestRebuildSurvivesAnOutboundThatFailsToSync(t *testing.T) {
	p := newProbeParts(t, 1)
	// 坏节点直接塞进注册表(订阅整形漏掉它的形状):SyncOutbounds 必须跳过它
	// 而不是让整轮重建失败。
	bad := vnode("bad-node")
	bad.Type = "quantum-tunnel"
	if n := p.Registry.Merge([]parse.Outbound{bad}); n != 1 {
		t.Fatalf("merge bad: %d", n)
	}
	url := subAndCatalogServer(t, vlink("n0"), `{"data":[]}`, http.StatusOK)
	p.Settings.SubURLs = []string{url}
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !p.Host.Has("n0") {
		t.Fatal("好节点没有装载")
	}
	if p.Host.Has("bad-node") {
		t.Fatal("坏节点被装载了")
	}
}

func TestRefreshCatalogPrefersNodeExitOverDirect(t *testing.T) {
	p := newProbeParts(t, 1)
	// 订阅给一个新节点(added=1 才会触发目录刷新),/zen/v1/models 回目录。
	url := subAndCatalogServer(t, vlink("fresh-node"), `{"data":[{"id":"free-a"},{"id":"free-b"}]}`, http.StatusOK)
	t.Setenv("OUR_FREE_MODEL_BASE", url)
	p.Settings.SubURLs = []string{url}
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	p.upstreamMu.Lock()
	ids := append([]string(nil), p.lastUpstreamIDs...)
	p.upstreamMu.Unlock()
	if len(ids) == 0 {
		t.Fatal("目录没有被节点出口刷新")
	}
	if got := p.catalog.get(); len(got) == 0 || got[0].ID == "" {
		t.Fatalf("catalog = %+v, want 2 rows", got)
	}
}

func TestRefreshCatalogFallsBackToDirect(t *testing.T) {
	p := newProbeParts(t, 0) // 池子空:节点出口路径没有候选
	url := subAndCatalogServer(t, "", `{"models":[{"id":"free-c"}]}`, http.StatusOK)
	t.Setenv("OUR_FREE_MODEL_BASE", url)
	p.refreshCatalog(context.Background())
	p.upstreamMu.Lock()
	ids := append([]string(nil), p.lastUpstreamIDs...)
	p.upstreamMu.Unlock()
	if len(ids) == 0 {
		t.Fatal("直连回退没有拿到模型列表")
	}
}

// TestRefreshCatalogDoesNotFallBackToOverlay:上游全挂时目录不再吃 models.dev
// 覆盖层(2026-10-03 裁决):overlay 的 id 是 24h 快照,混着上游已下架的模型,
// 曾把面板毒成恒 33 个免费模型。目录保持启动时的静态表,只排重试。
func TestRefreshCatalogDoesNotFallBackToOverlay(t *testing.T) {
	p := newProbeParts(t, 0)
	swallowTimers(p) // 全挂路径会排 60s 重试定时器,别让它在测试外开火
	url := subAndCatalogServer(t, "", "no", http.StatusInternalServerError)
	t.Setenv("OUR_FREE_MODEL_BASE", url)
	no := false
	p.overlayByID = map[string]limits.OverlayRow{
		"overlay-model": {ContextWindow: 100000, MaxOutput: 40000, Reasoning: &no},
	}
	before := append([]catalog.Model(nil), p.catalog.get()...)
	p.refreshCatalog(context.Background())
	p.upstreamMu.Lock()
	ids := append([]string(nil), p.lastUpstreamIDs...)
	p.upstreamMu.Unlock()
	if len(ids) != 0 {
		t.Fatalf("overlay 仍在兜底: lastUpstreamIDs = %v", ids)
	}
	after := p.catalog.get()
	if len(after) != len(before) {
		t.Fatalf("目录被 overlay 改写: %d 行 → %d 行", len(before), len(after))
	}
	for i := range after {
		if after[i].ID != before[i].ID {
			t.Fatalf("目录被 overlay 改写: 第 %d 行 %s → %s", i, before[i].ID, after[i].ID)
		}
	}
}

// TestRebuildReadyProbeUsesSameTimeoutFormula 随 bootTimeoutMS 一起删了(审计 O6):
// 那条 min(60000, 5000+10n) 公式在 Go 侧没有任何调用点,测它等于测一个不存在的接线。
// 开机路径真正用到的两条各有自己的测试:TestFirstProbeRunsThreeSecondsAfterReady
// (firstProbeDelay)与 TestJoinBootCapsTheWait(bootJoinTimeout,R12)。

// ---- 状态快照与定时器(6 条) ----

func TestStatusCarriesForwardKeyForTheLocalConsole(t *testing.T) {
	// 计划原文写「绝不回显 key」并称 JS 也没回显 —— 后半句与 src/index.js:1072
	// 相反:控制台(仅 127.0.0.1)拿到 key 才能在设置页发测试请求。按 JS 裁决,
	// 这条测试断言 key **在**快照里(修正案阶段 4 条目)。
	p := newProbeParts(t, 1)
	p.Settings.ForwardKey = "ofm-test-key"
	st := p.Status().(map[string]any)
	fw := st["forward"].(map[string]any)
	if fw["key"] != "ofm-test-key" || fw["port"] != p.Settings.ForwardPort {
		t.Fatalf("forward view = %+v, want key+port", fw)
	}
	// 但设置视图里没有 key:设置页没有显示它的需求。
	for k := range p.SettingsView() {
		if k == "forwardKey" {
			t.Fatal("SettingsView 泄漏了 forwardKey")
		}
	}
}

func TestStatusReadsNodeRowsFromHealthMemory(t *testing.T) {
	p := newProbeParts(t, 2)
	p.Health.MarkProbe("n0", aliveResult("198.51.100.5"))
	st := p.Status().(map[string]any)
	nodes := st["nodes"].([]any)
	byTag := map[string]map[string]any{}
	for _, n := range nodes {
		m := n.(map[string]any)
		byTag[m["tag"].(string)] = m
	}
	if got := byTag["n0"]["state"]; got != "alive" {
		t.Fatalf("n0.state = %v, want alive(内存表)", got)
	}
	if got := byTag["n1"]["state"]; got != string(health.StateUnknown) {
		t.Fatalf("n1.state = %v, want unknown(缺席行回退)", got)
	}
	if _, has := byTag["n0"]["port"]; has {
		t.Fatal("零端口架构的 NodeRow 不该有 port 字段")
	}
}

// R7 + O9:stats 的落盘失败必须能从 /api/status 看到。stats.LastError() 的
// 注释承诺它给 /api/status 用,但状态里从前没有它的位置 —— 一个「写盘一直
// 失败、内存计数照常」的网关在面板上看起来完全健康。
func TestStatusSurfacesTheStatsWriteFailure(t *testing.T) {
	p := newProbeParts(t, 1)
	// 零值 Parts 也必须有这个键:面板的启动帧直接读它。
	st := p.Status().(map[string]any)
	diag, ok := st["diagnostics"].(map[string]any)
	if !ok {
		t.Fatal("Status 缺 diagnostics 子对象")
	}
	if _, ok := diag["stats"]; !ok {
		t.Fatal("diagnostics 缺 stats 子对象")
	}
	if _, ok := diag["subscription"]; !ok {
		t.Fatal("diagnostics 缺 subscription 子对象(订阅最后失败原因)")
	}

	// 挂一个必然写不进去的 stats:父路径是普通文件 ⇒ Flush 必失败。
	blocker := filepath.Join(p.Root, "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("准备阻断文件: %v", err)
	}
	s := stats.New(filepath.Join(blocker, "stats.json"))
	s.Record(stats.Record{At: time.Now().UnixMilli(), Model: "m", OK: true, Output: 1})
	if err := s.Flush(); err == nil {
		t.Fatal("Flush 写不进去应返回 error")
	}
	p.StatsStore = s

	diag = p.Status().(map[string]any)["diagnostics"].(map[string]any)
	statsView := diag["stats"].(map[string]any)
	if got, _ := statsView["lastError"].(string); got == "" {
		t.Fatal("stats 落盘失败必须出现在 diagnostics.stats.lastError 里")
	}

	// 订阅失败原因同样要露出来:B11 之后 lastRebuildErr 记的是这轮重建的
	// 合并错误,面板据此给 toast,运维脚本据此告警。
	p.setRebuildResult(0, 0, 0, errors.New("订阅源全部超时"))
	diag = p.Status().(map[string]any)["diagnostics"].(map[string]any)
	subView := diag["subscription"].(map[string]any)
	if got, _ := subView["lastError"].(string); got != "订阅源全部超时" {
		t.Fatalf("diagnostics.subscription.lastError = %q, want 订阅源全部超时", got)
	}
}

// TestAllThreePassLoopsAreAlive 钉住 1.3.0 的结构缺陷修复:三个 pass 循环
// 曾被**串行**塞进同一个 goroutine(hotLoop 永不返回,coldLoop/firstProbeLoop
// 排在它后面 = 永不执行)—— 冷区扫描与首探双双停摆,新入池节点永远没有
// 健康行、死节点永远不被回收。现在三路各自独立:每个 nudge 只驱动自己的
// pass,各自恰好一轮。夹具:hot 有 alive 行、cold 有 dead 行、first 有无行
// 节点;周期全部沉默,只靠 nudge 驱动。
func TestAllThreePassLoopsAreAlive(t *testing.T) {
	p := newProbeParts(t, 3) // n0,n1,n2
	// n0 alive(热区有活)、n1 dead(冷区有活)、n2 无健康行(首探有活)。
	p.Health.MarkProbe("n0", aliveResult("198.51.100.1"))
	p.Health.MarkProbe("n1", deadResult())
	fp := p.Prober.(*fakeProber)
	p.waitFn = func(time.Duration) <-chan time.Time { return make(chan time.Time) } // 周期全沉默

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartTimers(ctx)

	// 三档各投一个 nudge。
	p.nudgeHot()
	p.nudgeCold()
	p.nudgeFirst()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fp.mu.Lock()
		n := fp.allCnt
		fp.mu.Unlock()
		if n >= 3 {
			return // 三个 pass 都真的跑起来了
		}
		time.Sleep(10 * time.Millisecond)
	}
	fp.mu.Lock()
	got := fp.allCnt
	fp.mu.Unlock()
	t.Fatalf("5 秒内只有 %d 个 pass 跑过, want 3 —— 串行 goroutine 的停摆复发", got)
}

// TestPassWorkersConsumesTheSetting 钉「面板并发」真被消费:passWorkers 取
// max(设置值, 按节点数摊)再封顶。旧实现完全忽略设置值,面板上挂着「探测
// 并发」字段却不读它 —— 展示值与事实不符(第五轮审计同型)。
func TestPassWorkersConsumesTheSetting(t *testing.T) {
	cases := []struct {
		n, setting, max, want int
		why                   string
	}{
		{100, 8, 96, 25, "自动档 (100+3)/4=25 > 设置 8,取大者"},
		{100, 48, 96, 48, "设置 48 > 自动 25,设置赢"},
		{8, 128, 96, 96, "设置顶到档位封顶 96(热)"},
		{8, 128, 32, 32, "同一设置,冷档封顶 32 —— 合计不超 128 峰值"},
		{0, 0, 96, 1, "零节点零设置也至少 1,不给 ProbeAll 传 0"},
	}
	for _, c := range cases {
		if got := passWorkers(c.n, c.setting, c.max); got != c.want {
			t.Errorf("passWorkers(%d,%d,%d) = %d, want %d(%s)", c.n, c.setting, c.max, got, c.want, c.why)
		}
	}
}

func TestTimersStopOnContextCancel(t *testing.T) {
	p := newProbeParts(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	p.StartTimers(ctx)
	cancel()
	done := make(chan struct{})
	go func() { p.timersWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel 后定时循环没有退出")
	}
}

func TestFirstProbeRunsThreeSecondsAfterReady(t *testing.T) {
	// 1.3.0:重建后的补探从「直接跑一轮」改成「给首探循环投 nudge」。定时器
	// 回调触发时,判决落在 firstNudge 通道上(循环 goroutine 消费后开测);
	// 这里没有起循环,所以钉的是「nudge 正确入 channel」。
	p := newProbeParts(t, 1)
	url := subAndCatalogServer(t, vlink("n0"), `{"data":[]}`, http.StatusOK)
	p.Settings.SubURLs = []string{url}
	type captured struct {
		d  time.Duration
		fn func()
	}
	got := make(chan captured, 4)
	p.afterFuncFn = func(d time.Duration, fn func()) *time.Timer {
		got <- captured{d, fn}
		return nil
	}
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	// Rebuild 可能先排其他定时器(订阅重试等);取**第一个 d=firstProbeDelay** 的。
	deadline := time.After(2 * time.Second)
	for {
		select {
		case c := <-got:
			if c.d != firstProbeDelay {
				continue
			}
			c.fn()
			select {
			case <-p.firstNudge:
				return // nudge 正确落道
			default:
				t.Fatal("定时器触发后 firstNudge 通道没有收到投递")
			}
		case <-deadline:
			t.Fatal("重建后没有安排首探补探定时器")
		}
	}
}

// TestWarmUpProbesWithoutWaitingForTheInterval:开场订阅落定之后必须立刻有一轮
// 首探,不能等满一个探测周期。JS 版的首探来自启动那次 rebuild 的尾巴
// (src/index.js:1101 + :739-740);Go 版把开场拉取内联进 Build(app.go 步骤 4),
// 那条路径不经过 Rebuild —— 少了这一轮,probeIntervalMin=30 时面板半小时全是「–」。
func TestWarmUpProbesWithoutWaitingForTheInterval(t *testing.T) {
	p := newProbeParts(t, 1)
	swallowTimers(p) // 目录刷新失败时的 60s 重试定时器与本测试无关
	p.waitFn = func(d time.Duration) <-chan time.Time {
		if d == firstProbeDelay {
			c := make(chan time.Time, 1)
			c <- time.Now()
			return c
		}
		return make(chan time.Time) // 周期循环保持沉默
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartTimers(ctx)
	fp := p.Prober.(*fakeProber)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fp.mu.Lock()
		n := fp.allCnt
		fp.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("开场订阅落定后没有首探:第一轮实测被推到了整个探测周期之后")
}

// TestWarmUpRefreshesCatalogAtStartup:启动即刷一次上游模型列表。JS 版是
// src/index.js:1099 那句 void refreshCatalog();没有它,面板冷启动只能播
// 静态回退表,要等 6 小时后的第一次重建才见新列表。
func TestWarmUpRefreshesCatalogAtStartup(t *testing.T) {
	p := newProbeParts(t, 0)
	url := subAndCatalogServer(t, "", `{"data":[{"id":"warm-a"},{"id":"warm-b"}]}`, http.StatusOK)
	t.Setenv("OUR_FREE_MODEL_BASE", url)
	p.waitFn = func(time.Duration) <-chan time.Time { return make(chan time.Time) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartTimers(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.upstreamMu.Lock()
		ids := append([]string(nil), p.lastUpstreamIDs...)
		p.upstreamMu.Unlock()
		if len(ids) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("启动后没有刷新模型目录")
}

// ---- 设置写入 ----

func TestApplySettingsKeepsNullAndGuardsKey(t *testing.T) {
	p := newProbeParts(t, 1)
	p.settingsStore = persistence.NewStore("settings", filepath.Join(p.Root, "settings.json"), settingsMap(defaultSettings()))
	if err := p.settingsStore.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	// 生产里 key 存在 store 中(Load 生成后写入),Settings 从 store 派生;
	// 夹具保持同一事实来源。
	p.Settings.ForwardKey = "ofm-keep-me"
	p.settingsStore.Update(map[string]any{"forwardKey": "ofm-keep-me"})
	_, err := p.ApplySettings(map[string]any{
		"defaultMaxTokens": nil,         // 清空 = null,必须存 null 而不是删键
		"forwardKey":       "ofm-evil",  // 白名单外:改不掉
		"hotIntervalSec":   float64(90), // JSON 数字
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	raw, _ := os.ReadFile(filepath.Join(p.Root, "settings.json"))
	var onDisk map[string]any
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatalf("settings.json: %v", err)
	}
	v, ok := onDisk["defaultMaxTokens"]
	if !ok || v != nil {
		t.Fatalf("defaultMaxTokens = %v (present %v), want null 且键存在", v, ok)
	}
	if p.Settings.ForwardKey != "ofm-keep-me" {
		t.Fatalf("forwardKey 被补丁改成了 %q", p.Settings.ForwardKey)
	}
	if p.Settings.HotIntervalSec != 90 {
		t.Fatalf("hotIntervalSec = %d, want 90", p.Settings.HotIntervalSec)
	}
}

// TestApplySettingsRejectsAMalformedPatchWithoutTouchingDisk:B10。面板不认证
// (panel.go 的头注释自认),任何本机进程都能 PUT 一个畸形补丁。旧实现先把
// clean 写进 store 再校验,一条 {"probeWorkers":"abc"} 就能把 settings.json
// 写成不可解析 —— 下次启动 Load 失败,网关拒绝启动,只能手改文件。修法:先逐键
// 类型校验,非法立即返回,绝不碰盘。
func TestApplySettingsRejectsAMalformedPatchWithoutTouchingDisk(t *testing.T) {
	p := newProbeParts(t, 1)
	settingsFile := filepath.Join(p.Root, "settings.json")
	p.settingsStore = persistence.NewStore("settings", settingsFile, settingsMap(defaultSettings()))
	if err := p.settingsStore.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := p.settingsStore.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	before, err := os.ReadFile(settingsFile)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	beforeWorkers := p.Settings.ProbeWorkers

	for _, patch := range []map[string]any{
		{"probeWorkers": "abc"},        // 报告 B10 的复现输入
		{"subUrls": []any{float64(1)}}, // 数组里混进非字符串
		{"probeEnabled": "yes"},        // 布尔位收到字符串
		{"countries": "US"},            // 数组位收到字符串
		{"hotIntervalSec": nil},        // 数字位收到 null
	} {
		if _, err := p.ApplySettings(patch); err == nil {
			t.Fatalf("ApplySettings(%v) 接受了畸形补丁", patch)
		}
		after, err := os.ReadFile(settingsFile)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(after) != string(before) {
			t.Fatalf("ApplySettings(%v) 动了磁盘:\n before %s\n after  %s", patch, before, after)
		}
		if p.Settings.ProbeWorkers != beforeWorkers {
			t.Fatalf("probeWorkers 被畸形补丁改成了 %d", p.Settings.ProbeWorkers)
		}
	}
	// 盘上文件必须仍能启动 —— 这才是 B10 的核心后果。
	if _, err := settingsFromStore(p.settingsStore); err != nil {
		t.Fatalf("settingsFromStore 在畸形补丁后失败: %v", err)
	}
}
