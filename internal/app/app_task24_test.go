// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
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
	mu         sync.Mutex
	directErr  error
	directCnt  int
	allCnt     int
	block      chan struct{} // 非 nil:ProbeAll 阻塞直到通道关闭
	failedTags map[string]bool
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
	f.mu.Unlock()
	results := make([]nodeprobe.Result, 0, len(items))
	for _, it := range items {
		r := aliveResult("198.51.100." + fmt.Sprint(len(results)%200+1))
		if failed[it.Tag] {
			r = deadResult()
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
	}
	return p
}

func aliveResult(ip string) nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: 100, LatencyMin: 100, ExitIP: ip, ExitCountry: "US"}
}

func deadResult() nodeprobe.ProbeResult {
	return nodeprobe.ProbeResult{State: nodeprobe.StateDead, LatencyMS: -1}
}

// ---- 探测轮次(12 条) ----

func TestProbeNowWritesHealthAndPersists(t *testing.T) {
	p := newProbeParts(t, 10)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 3; i < 10; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 3 alive,7 dead
	}
	sum, err := p.ProbeNow(context.Background(), false)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if sum.Tested != 10 || sum.Alive != 3 {
		t.Fatalf("summary = tested %d alive %d, want 10/3", sum.Tested, sum.Alive)
	}
	if got := p.Health.HealthOf("n0"); got != health.StateAlive {
		t.Fatalf("n0 state = %q, want alive", got)
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

func TestProbeNowSkipsWholeRoundWhenDirectIsDown(t *testing.T) {
	p := newProbeParts(t, 4)
	fp := p.Prober.(*fakeProber)
	fp.directErr = fmt.Errorf("liveness unreachable")
	healthFile := filepath.Join(p.Root, "node-health.json")
	if err := p.Health.Persist(); err != nil {
		t.Fatalf("seed persist: %v", err)
	}
	st1, err := os.Stat(healthFile)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	sum, err := p.ProbeNow(context.Background(), false)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !sum.Skipped {
		t.Fatal("Skipped = false, want true")
	}
	if p.Registry.Len() != 4 {
		t.Fatalf("registry len = %d, want 4 (一个都不能少)", p.Registry.Len())
	}
	st2, _ := os.Stat(healthFile)
	if !st1.ModTime().Equal(st2.ModTime()) {
		t.Fatal("node-health.json 被改写:跳过的轮次不许写任何健康行")
	}
}

func TestProbeNowKeepsAliveTagsFromResultCache(t *testing.T) {
	p := newProbeParts(t, 8)
	for i := 0; i < 8; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.7"))
	}
	sum, err := p.ProbeNow(context.Background(), false)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if sum.Tested != 0 || sum.Cached != 8 {
		t.Fatalf("tested %d cached %d, want 0/8", sum.Tested, sum.Cached)
	}
	if sum.Alive != 8 {
		t.Fatalf("alive = %d, want 8 (缓存里 alive 的必须计入存活,否则误判事故)", sum.Alive)
	}
}

func TestProbeSummaryLineMatchesFrontendRegex(t *testing.T) {
	p := newProbeParts(t, 6)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{"n5": true}
	if _, err := p.ProbeNow(context.Background(), true); err != nil {
		t.Fatalf("probe: %v", err)
	}
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
	p := newProbeParts(t, 20)
	for i := 0; i < 20; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 6; i < 20; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true // 上一轮 20 alive,这轮掉 14(70%)
	}
	sum, err := p.ProbeNow(context.Background(), true)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !sum.Accident {
		t.Fatalf("accident = false, want true (掉了 14/20)")
	}
	if p.Registry.Len() != 20 {
		t.Fatalf("registry len = %d, want 20 (事故轮一个都不淘汰)", p.Registry.Len())
	}
}

func TestProbeNowEvictsWhenNotAccident(t *testing.T) {
	p := newProbeParts(t, 10)
	now := time.Now()
	// n1..n4 预置 3 次连败:本轮再测不到就够淘汰门槛。
	for i := 1; i <= 4; i++ {
		tag := fmt.Sprintf("n%d", i)
		p.Registry.NoteFail(tag, now)
		p.Registry.NoteFail(tag, now)
		p.Registry.NoteFail(tag, now)
	}
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{}
	for i := 1; i <= 4; i++ {
		fp.failedTags[fmt.Sprintf("n%d", i)] = true
	}
	// prevAlive 只有 5 个(<8)不构成事故样本。
	for i := 5; i < 10; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.9"))
	}
	sum, err := p.ProbeNow(context.Background(), true)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if sum.Accident {
		t.Fatal("accident = true, want false (样本 5 < probeAccidentMin)")
	}
	if sum.Removed != 4 || p.Registry.Len() != 6 {
		t.Fatalf("removed %d len %d, want 4/6", sum.Removed, p.Registry.Len())
	}
}

// TestFailingNodeIsEvictedAfterConsecutiveRounds 跑真实的连续轮次,而不是像
// TestProbeNowEvictsWhenNotAccident 那样手工预置三次 NoteFail。
//
// 这一条是回归测试:观察期的节点过去被塞进 RetainOnly 的 alive 名单,而 alive
// 名单里的节点连败会被清零,于是连败计数每轮 0→1→0→1… 永远到不了
// registry.MaxFails。现场表现就是日志里 `淘汰 0 · 观察期 1601` 长期钉死,
// 整池死节点一个都不处理。手工预置 NoteFail 的测试绕开了累加环节,所以一直是绿的。
func TestFailingNodeIsEvictedAfterConsecutiveRounds(t *testing.T) {
	p := newProbeParts(t, 10)
	fp := p.Prober.(*fakeProber)
	fp.failedTags = map[string]bool{"n1": true} // 只有 n1 一直失败,9 个通关

	// force=true:夹具的健康行每轮都是新的,不绕过结果缓存窗就只会测到一次。
	for round := 1; round <= registry.MaxFails; round++ {
		if _, err := p.ProbeNow(context.Background(), true); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		want := round
		if round < registry.MaxFails && p.Registry.FailCount("n1") != want {
			t.Fatalf("round %d 后连败 = %d, want %d(观察期不能清零计数)",
				round, p.Registry.FailCount("n1"), want)
		}
	}
	if p.Registry.Has("n1") {
		t.Fatalf("连败 %d 轮的节点仍在池内,池子 %d 个 —— 淘汰从未生效",
			registry.MaxFails, p.Registry.Len())
	}
	if p.Registry.Len() != 9 {
		t.Fatalf("池子剩 %d 个, want 9", p.Registry.Len())
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
	sum, err := p.ProbeNow(context.Background(), true)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if sum.Accident {
		t.Fatal("accident = true, want false (probeAccidentMin=8)")
	}
}

func TestTierGateSurvivesAcrossRounds(t *testing.T) {
	p := newProbeParts(t, 3)
	if _, err := p.ProbeNow(context.Background(), true); err != nil {
		t.Fatalf("round1: %v", err)
	}
	if p.tierGate == nil {
		t.Fatal("tierGate 未装配")
	}
	first := p.tierGate.NextAt()
	if _, err := p.ProbeNow(context.Background(), true); err != nil {
		t.Fatalf("round2: %v", err)
	}
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

func TestProbeNowIsSerialized(t *testing.T) {
	p := newProbeParts(t, 3)
	fp := p.Prober.(*fakeProber)
	fp.block = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := p.ProbeNow(context.Background(), false)
		done <- err
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
	_, err2 := p.ProbeNow(context.Background(), false)
	if err2 == nil || !strings.Contains(err2.Error(), "already running") {
		t.Fatalf("并发第二轮 err = %v, want already running", err2)
	}
	close(fp.block)
	if err := <-done; err != nil {
		t.Fatalf("first round: %v", err)
	}
}

func TestForceIgnoresResultCacheWindow(t *testing.T) {
	p := newProbeParts(t, 8)
	for i := 0; i < 8; i++ {
		p.Health.MarkProbe(fmt.Sprintf("n%d", i), aliveResult("198.51.100.7"))
	}
	sum, err := p.ProbeNow(context.Background(), true)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if sum.Cached != 0 || sum.Tested != 8 {
		t.Fatalf("cached %d tested %d, want 0/8", sum.Cached, sum.Tested)
	}
}

func TestProbeRerunsOneSecondAfterCancel(t *testing.T) {
	p := newProbeParts(t, 2)
	type captured struct {
		d  time.Duration
		fn func()
	}
	got := make(chan captured, 4)
	p.afterFuncFn = func(d time.Duration, fn func()) *time.Timer {
		got <- captured{d, fn}
		return nil
	}
	p.markRerun(true)
	p.finishProbeRound()
	select {
	case c := <-got:
		if c.d != time.Second {
			t.Fatalf("rerun delay = %v, want 1s", c.d)
		}
		before := p.Prober.(*fakeProber).allCnt
		c.fn() // 重跑真的发起一轮探测,而不是只打个日志
		if p.Prober.(*fakeProber).allCnt != before+1 {
			t.Fatal("重跑没有发起探测轮")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("finishProbeRound 没有安排重跑")
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
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
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
	if err := p.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild: %v", err)
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

func TestRefreshCatalogFallsBackToOverlay(t *testing.T) {
	p := newProbeParts(t, 0)
	// 上游恒 500:节点出口与直连都拿不到列表,models.dev 覆盖层兜底。
	url := subAndCatalogServer(t, "", "no", http.StatusInternalServerError)
	t.Setenv("OUR_FREE_MODEL_BASE", url)
	no := false
	p.overlayByID = map[string]limits.OverlayRow{
		"overlay-model": {ContextWindow: 100000, MaxOutput: 40000, Reasoning: &no},
	}
	p.refreshCatalog(context.Background())
	p.upstreamMu.Lock()
	ids := append([]string(nil), p.lastUpstreamIDs...)
	p.upstreamMu.Unlock()
	if len(ids) != 1 || ids[0] != "overlay-model" {
		t.Fatalf("overlay 回退 ids = %v, want [overlay-model]", ids)
	}
}

func TestRebuildReadyProbeUsesSameTimeoutFormula(t *testing.T) {
	cases := []struct{ n, want int }{
		{0, 5000}, {100, 6000}, {5500, 60000}, {100000, 60000},
	}
	for _, c := range cases {
		if got := bootTimeoutMS(c.n); got != c.want {
			t.Fatalf("bootTimeoutMS(%d) = %d, want %d (min(60000, 5000+10n))", c.n, got, c.want)
		}
	}
}

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

func TestTimersUseMaxFiveMinuteProbeFloor(t *testing.T) {
	p := newProbeParts(t, 1)
	p.Settings.ProbeIntervalMin = 1
	if got := p.probeInterval(); got != 5*time.Minute {
		t.Fatalf("interval = %v, want 5m 下限", got)
	}
	p.Settings.ProbeIntervalMin = 15
	if got := p.probeInterval(); got != 15*time.Minute {
		t.Fatalf("interval = %v, want 15m", got)
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

func TestTimersDoNotOverlapProbeRounds(t *testing.T) {
	p := newProbeParts(t, 2)
	fp := p.Prober.(*fakeProber)
	fp.block = make(chan struct{})
	p.Settings.ProbeIntervalMin = 5
	interval := p.probeInterval()
	// 缓冲 2:第二拍不许阻塞测试 goroutine —— 它要等 probing 标志挡掉。
	tick := make(chan time.Time, 2)
	p.waitFn = func(d time.Duration) <-chan time.Time {
		if d == interval {
			return tick
		}
		return time.After(1 * time.Hour) // 重建/限额循环在本测试里保持沉默
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.StartTimers(ctx)
	tick <- time.Now()
	tick <- time.Now() // 第二拍:上一轮还堵着,必须被 probing 标志挡掉
	time.Sleep(200 * time.Millisecond)
	fp.mu.Lock()
	n := fp.allCnt
	fp.mu.Unlock()
	if n != 1 {
		t.Fatalf("探测轮并发执行了 %d 次, want 1", n)
	}
	close(fp.block)
	// 第二轮在 tick#2 被消费后开跑;等它落完盘再让 TempDir 清理,
	// 否则 Windows 的 RemoveAll 会撞上正在写的文件。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && p.probing.Load() {
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFirstProbeRunsThreeSecondsAfterReady(t *testing.T) {
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
	select {
	case c := <-got:
		if c.d != firstProbeDelay {
			t.Fatalf("first probe delay = %v, want %v", c.d, firstProbeDelay)
		}
		c.fn()
		if p.Prober.(*fakeProber).allCnt != 1 {
			t.Fatal("首探没有真的跑一轮")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("重建后没有安排首探")
	}
}

// TestWarmUpProbesWithoutWaitingForTheInterval:开场订阅落定之后必须立刻有一轮
// 首探,不能等满一个探测周期。JS 版的首探来自启动那次 rebuild 的尾巴
// (src/index.js:1101 + :739-740);Go 版把开场拉取内联进 Build(app.go 步骤 4),
// 那条路径不经过 Rebuild —— 少了这一轮,probeIntervalMin=30 时面板半小时全是「–」。
func TestWarmUpProbesWithoutWaitingForTheInterval(t *testing.T) {
	p := newProbeParts(t, 1)
	swallowTimers(p) // 目录刷新失败时的 60s 重试定时器与本测试无关
	p.Settings.ProbeIntervalMin = 30
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
// src/index.js:1099 那句 void refreshCatalog();没有它,面板的模型表只能吃
// data/catalog-ids.json 的磁盘缓存,要等 6 小时后的第一次重建才见新列表。
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
		"probeIntervalMin": float64(15), // JSON 数字
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
	if p.Settings.ProbeIntervalMin != 15 {
		t.Fatalf("probeIntervalMin = %d, want 15", p.Settings.ProbeIntervalMin)
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
		{"probeIntervalMin": nil},      // 数字位收到 null
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
