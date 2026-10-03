// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package nodeprobe 是零配额层的节点粗探测，逐字移植 src/nodeprobe.js。三阶段：
//
//	stage 1  liveness — 1.1.1.1/cdn-cgi/trace（IP 字面量：免疫 gstatic/apple 域名
//	                   被实测到的 DNS 污染）与 cp.cloudflare.com/generate_204 等，
//	                   并发打满、首个成功即赢、延迟取最快；
//	stage 2  upstream — GET /zen/v1/models（实测匿名 200、零配额）。liveness 只证明
//	                   「出口有网」，这一发证明「出口到得了我们在乎的上游」。并入
//	                   stage 1 是实测教训：到得了 opencode.ai 但到不了 Cloudflare
//	                   的出口是可用的，旧的串行闸门（先 Cloudflare 再上游）会把这
//	                   类节点在从未试过上游的情况下误判成死。
//	stage 3  echo    — 出口 IP + 国家，取第一个应答源。tag 自称的国家仅供参考，
//	                   这里才是分组用的地面真相。
//
// 过了本探测 = tier A（可达，非门控模型都可用）。它对地区门控模型**只字未提**：
// 免费通道的地区墙是每次出站的运行期判决，不是模型属性——/zen/v1/models 会在
// 一个随后回答 403 RegionError 的出口上照常返回全部模型 id。只有真实对话能分
// 辨，那是 health（tier B）的事，由 index 作为本探测之后的流水线阶段去跑。
package nodeprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"freerouter/internal/httpclient"
)

// LivenessURLs 是 stage-1 候选，逐字照抄 src/nodeprobe.js:48。
//
// 四个 liveness 源分属四个不同的网络主体（Cloudflare / Google / Mozilla /
// Apple），原先只有前两个——而那两个都是 Cloudflare。单一主体故障（区域路由
// 抖动、该 ASN 的任播节点被针对）会让**整轮探测**把全池节点判死，日志里的
// `淘汰 2554`（98.6%）就是这种形状。firstSuccess 是「任一成功即够」，多一个
// 不同主体的候选代价只是一次并发 shot，收益是整轮结果不再押在同一个主体上。
// 全部强制 https（fetchVia 里关重定向）：门户劫持改不了 https 响应，这正是
// 「宁可少一个候选也不用明文源」那条纪律。
var LivenessURLs = []string{
	"https://1.1.1.1/cdn-cgi/trace",
	"https://cp.cloudflare.com/generate_204",
	"https://www.gstatic.com/generate_204",
	"https://detectportal.firefox.com/success.txt",
	"https://captive.apple.com/hotspot-detect.html",
}

// GateURL 是上游闸门，逐字照抄 src/nodeprobe.js:55：实测过匿名 200、零配额。
const GateURL = "https://opencode.ai/zen/v1/models"

// 探测请求的 UA 分两类（纪律照抄 src/nodeprobe.js:56-66 的注释）：
//   - liveness/echo 源都是**别人**的服务，报一个诚实的自述名，不冒充任何客户端；
//   - 上游闸门是**我们真正要过的那道门**，就用主链路同一族 UA。理由：闸门的判决
//     是内容级的（见 gateVerdict），而 OmniRoute 记录过「非 OpenCode CLI 的 UA
//     会被替换/拒绝」——探测必须打在主链路真实的形状上，否则「探测通过」与
//     「实际请求通过」是两件不同的事，闸门就白设了。刻意**不发**会话头：GET
//     /zen/v1/models 不需要它，而探测每轮要跑上千次，塞会话会在上游侧污染会话表
//     （FishBottle7 的 400 MissingSessionID 是 chat/POST 路径的现象，不是这条 GET）。
const ProbeUA = "lite-gateway-probe/0.2"

// GateProbeUA 逐字照抄 src/upstream.js:53-55 的 CLIENT_UA 默认值
// （`opencode/${CLIENT_VERSION}`，CLIENT_VERSION = '1.18.31'）。JS 里的
// OUR_FREE_MODEL_UA 环境覆盖由组装层负责传入，本包不读环境。
const GateProbeUA = "opencode/1.18.31"

// EchoURLs 是 stage-3 候选，逐字照抄 src/nodeprobe.js:73。
//
// echo 源要回一个带 IP 的 JSON。ip-api.com 的免费档只支持 http——明文出境暴露
// 出口 IP，因此不留它。少一个候选的代价可接受：firstSuccess 本来就是「任一成功
// 即够」。ifconfig.co 的字段名是 country_iso（不是 country_code），exitInfoOf
// 一并认下。
var EchoURLs = []string{
	"https://api.ip.sb/geoip",
	"https://ipinfo.io/json",
	"https://ipwho.is/",
	"https://ifconfig.co/json",
}

// 探测三态。unknown 不是 dead：index 阶段靠它区分「没测到」（跳过这一轮，不记
// 连败）与「测到不通」（走退役路径）。把 unknown 当 dead 会给可能健康的节点白记
// 一次连败，几轮就把整池误淘汰。
const (
	StateAlive   = "alive"
	StateDead    = "dead"
	StateUnknown = "unknown"
)

// 预算默认值与 JS probeNode / probeAll 的参数默认一致（src/nodeprobe.js:229/305）。
const (
	defaultTimeoutMS = 12000
	defaultAttempts  = 2
	echoBudgetMS     = 8000
	backstopSlackMS  = 5000

	// maxGateBodyBytes / maxEchoBodyBytes 是两处响应体的上限(R3)。
	//
	// 探测目标是**我们不控制的**远端:一个恶意或被劫持的目标可以无限发流,
	// 而 io.ReadAll 会把它整条读进内存。闸门要的是模型清单(实测几百 KB),
	// echo 要的是 IP+国家(几百字节),给足余量再封顶。
	maxGateBodyBytes = 8 << 20
	maxEchoBodyBytes = 1 << 20
)

// ProbeOptions 是单个探测项的预算。字段只有 probeNode 实际需要的两个
// （JS probeNode 的 timeoutMs/attempts；signal 是 worker 兜底 internals，不是选项）。
// 零值在 ProbeAll 里展开为默认值——调用方可以对已连败节点降到一次尝试 + 更短超时。
type ProbeOptions struct {
	TimeoutMS int
	Attempts  int
}

// ProbeResult 是单个节点的探测结论。
type ProbeResult struct {
	State       string // alive / dead / unknown
	LatencyMS   int64  // 首个成功样本（「这一轮测到的延迟」，面板读它）；dead 时是总耗时；unknown 恒 -1
	LatencyMin  int64  // 所有样本的最小值，只用于选路排序
	ExitIP      string
	ExitCountry string // 两字母大写；echo 源没给就是空串
	Incomplete  bool   // 兜底到点，本轮没量完整
}

// Item 是 ProbeAll 的输入项。Dial 与 sbx.Dialer 是同一个类型别名（见
// internal/LAYERS.md 的方向性说明）：sbx 产出拨号器，app 把它塞进来，本包不
// import sbx。Dial 为 nil 表示直连（ProbeDirect / 测试用）。
type Item struct {
	Tag     string
	Dial    httpclient.Dialer
	Options ProbeOptions
}

// Result 是 ProbeAll 的逐项输出，与输入同序（app 落盘需要确定性顺序）。
type Result struct {
	Tag    string
	Result ProbeResult
}

// Prober 持有探测源表与日志出口。
//
// URL 表做成字段而不是让测试改包级常量：单元测试要把它指到本地 httptest
// （绝不在测试里打公网 URL），同包测试直接整体替换字段；NewProber 装入生产
// 默认值，这就是修正案裁决的「URL 表 = Prober 字段 + New 可覆盖」方案。
type Prober struct {
	logf     func(level, msg string)
	liveness []string
	gate     string // 空串 = 关掉闸门（测试用）
	echo     []string
}

// NewProber 装入生产默认探测源。logf 可为 nil（静默）。
func NewProber(logf func(level, msg string)) *Prober {
	return &Prober{
		logf:     logf,
		liveness: LivenessURLs,
		gate:     GateURL,
		echo:     EchoURLs,
	}
}

func (p *Prober) log(level, msg string) {
	if p.logf != nil {
		p.logf(level, msg)
	}
}

// fetchVia 经 dial 对 url 发一次 GET，单发预算 timeoutMs。
//
// 每次调用新建一个 httpclient，不复用连接池（JS fetchVia 每次现做 dispatcher，
// 同理）：A 节点的复活连接若是池化复用的，会被记到 B 头上。redirect 按 JS 的
// `redirect: 'error'` 关闭——映射为 ErrUseLastResponse，被门户重定向时判决层只
// 会看到 3xx 状态码而自然判负，明文劫持改不了 https 应答。
//
// 拨号阶段的预算要**自己给**：本版本 net/http 的 getConn 刻意用
// `context.WithoutCancel(请求ctx)` 派生拨号 ctx（transport.go 原注释：取消请求
// 不该顺手杀掉还能复用的拨号）——请求 ctx 上的期限与取消都**传不到**拨号阶段。
// 不补这一层的话，挂死的拨号既不吃单发预算、也不吃 worker 兜底的 cancel，正是
// 「僵尸拨号占着 sing-box inbound」事故的 Go 版形状。以 shot 为父重发期限，预算
// 与取消两条线都接回来；代价是放弃了 transport「拨号结果给下一个请求复用」的
// 优化——本就每 shot 一个 client、无池可复用，无所谓。
func fetchVia(ctx context.Context, dial httpclient.Dialer, url string, timeoutMs int, ua string) (*http.Response, error) {
	shot, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()
	bounded := dial
	if bounded != nil { // nil = 直连，交给 httpclient 的默认拨号器
		bounded = func(_ context.Context, network, addr string) (net.Conn, error) {
			dialCtx, dialCancel := context.WithTimeout(shot, time.Duration(timeoutMs)*time.Millisecond)
			defer dialCancel()
			return dial(dialCtx, network, addr)
		}
	}
	client := httpclient.NewOneShotClient(bounded, time.Duration(timeoutMs)*time.Millisecond)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequestWithContext(shot, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("user-agent", ua)
	return client.Do(req)
}

// firstSuccess 并发打满 urls，首个「判出结果」的 shot 赢，其余当场取消，全败
// 返回 nil。移植自 src/nodeprobe.js:109-139，两条实测教训原样保留：
//
//  1. 首个成功必须**取消败者**——否则峰值并发里一半是已无意义的 fetch 跑满超时，
//     占着 sing-box inbound 和本地临时端口（128 并发轮实测）；
//  2. 判决层对「非判决」必须报 error 而不是返回 nil——JS 里 judges THROW，
//     Promise.any 才会等真正的判决；若 nil 能赢，它瞬间胜出，全池判死（实测事故）。
//
// 外层 ctx（worker 兜底）一响，所有 shot 一起被掐：shot ctx 都是它的子 context，
// 没有 this 线，兜底才真的能放弃等待（JS 的 onOuterAbort 同一条线）。
func firstSuccess(
	ctx context.Context,
	dial httpclient.Dialer,
	urls []string,
	timeoutMs int,
	uaOf func(string) string,
	judge func(resp *http.Response, url string) (any, error),
) any {
	if len(urls) == 0 {
		return nil
	}
	type outcome struct {
		val any
		err error
	}
	outcomes := make(chan outcome, len(urls)) // 缓冲满额：败者收尾不阻塞
	cancels := make([]context.CancelFunc, len(urls))
	for i, url := range urls {
		shotCtx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func(shotCtx context.Context, url string) {
			resp, err := fetchVia(shotCtx, dial, url, timeoutMs, uaOf(url))
			var v any
			if err == nil {
				v, err = judge(resp, url)
				// 判决后一律排干并关 body（JS drain / body.cancel）：不关会把
				// 这条连接和它的 goroutine 留在 transport 里。
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			outcomes <- outcome{val: v, err: err}
		}(shotCtx, url)
	}
	remaining := len(urls)
	for remaining > 0 {
		o := <-outcomes
		remaining--
		if o.err == nil && o.val != nil {
			for _, cancel := range cancels {
				cancel() // 首个成功即掐其余
			}
			return o.val
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

// gateVerdict 是上游闸门的内容层判决——200 不等于通过。移植自
// src/nodeprobe.js:150-188，事故注释原样保留：
//
// stage 1 曾把闸门和 liveness 源放进同一个 firstSuccess，判据只有状态码，拿到
// 200 就 drain 掉 body。于是「上游回 200 但 body 是拦截页/错误 JSON」（
// Cloudflare 的 cf_details 拦截、上游把免费通道摘掉后返回的 {"error":…}）被记成
// alive，而这个节点在真实对话里第一轮就失败。判据必须是**正向证据**：body 里
// 确实有一份模型清单。对照 xiecang speedtest/check/gpt_web.go:70-77：状态码必
// 200，且 JSON 里有 data 或 models 数组。代价是每个存活节点多读几 KB——2500
// 节点/30 分钟一轮，可忽略。
//
// 非判决一律返回 error，firstSuccess 依赖这个语义。判据与 JS 逐字对齐：
// Array.isArray(json?.data) || Array.isArray(json?.models)（JS 不要求非空，
// 顶层数组同样判负）。
func gateVerdict(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gate HTTP %d", resp.StatusCode)
	}
	body, err := httpclient.ReadCapped(resp.Body, maxGateBodyBytes)
	if err != nil {
		// 包装而不是吞掉:调用方要能区分「闸门断流」与「闸门吐了个超大体」,
		// 后者说明这个闸门在被滥用(R3)。
		return fmt.Errorf("gate response body unreadable: %w", err)
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("gate response is not JSON")
	}
	obj, ok := parsed.(map[string]any)
	if !ok {
		return fmt.Errorf("gate response carries no model list")
	}
	_, hasData := obj["data"].([]any)
	_, hasModels := obj["models"].([]any)
	if !hasData && !hasModels {
		return fmt.Errorf("gate response carries no model list")
	}
	return nil
}

// exitInfo 承载 echo 源的出口事实。
type exitInfo struct {
	ip      string
	country string
}

// exitInfoOf 照抄 src/nodeprobe.js:141-148 的字段路径：IP 认 ip 或 query；
// 国家认 country_code / countryCode / country_iso / country——字段名各家不同，
// 少认一个就等于少一个可用候选。缺 IP 即非判决。
func exitInfoOf(body []byte) (exitInfo, error) {
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return exitInfo{}, fmt.Errorf("echo response is not JSON")
	}
	obj, ok := parsed.(map[string]any)
	if !ok {
		return exitInfo{}, fmt.Errorf("echo response missing ip")
	}
	ip := firstStr(obj, "ip", "query")
	if ip == "" {
		return exitInfo{}, fmt.Errorf("echo response missing ip")
	}
	cc := strings.ToUpper(firstStr(obj, "country_code", "countryCode", "country_iso", "country"))
	if len(cc) > 2 {
		cc = cc[:2] // JS String(cc).toUpperCase().slice(0, 2)
	}
	return exitInfo{ip: ip, country: cc}, nil
}

func firstStr(obj map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := obj[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// stage1URLs：liveness 全表 + 闸门（闸门并入 stage 1 的原因见包注释）。
func (p *Prober) stage1URLs() []string {
	if p.gate == "" {
		return slices.Clone(p.liveness)
	}
	return slices.Concat(p.liveness, []string{p.gate})
}

// uaOf：闸门那发用主链路 UA，其余诚实自述（见 GateProbeUA 上的纪律注释）。
func (p *Prober) uaOf() func(string) string {
	return func(url string) string {
		if p.gate != "" && url == p.gate {
			return GateProbeUA
		}
		return ProbeUA
	}
}

// ProbeNode 探测单个出口，移植自 src/nodeprobe.js:229。
//
// attempts 循环**不提前 break**——这是对 JS 版的一处故意偏离，理由照搬 JS 注释：
// 每轮成功都记进 samples，成功也不跳出，同一次探测拿两个以上样本才能取最小值。
// 这不是额外预算：池子形状决定了成本几乎为零（2373 个节点里只有约 191 个存活，
// 死节点本来就把 attempts 次超时全部等满，活节点多跑的一轮是毫秒级），收益是
// 延迟不再被单次冷启动污染——VPNpool nodecheck.sh:50-59 记的「同一真实节点上一
// 分钟 ~50%、下一分钟 3/3」说的正是这件事。JS 版曾在这里 break，healthy 节点
// 只产生一个样本；Go 按修正后的语义实现。
//
// 另一处偏离：JS 样本记 Date.now()-t0（全程累计），min 恒等于首样本、等于没有
// min；Go 按每轮独立计时。LatencyMS = 首样本（「这一轮测到的延迟」，面板读它）；
// LatencyMin = 所有轮次最小值，只用于选路排序。取 min 会系统性低估，但选路只要
// 相对次序，而 min 比 mean 抗冷启动（xiecang speedtest/utils.go 并发多发取最小）。
//
// 默认两次尝试：单次瞬时失败不该让一个健康出口掉一格连败计数（实测 1750 节点池
// 里，每轮一次瞬时 echo/gate 失败就是「池子稳定」和「池子在掉健康节点」的区别）。
func (p *Prober) ProbeNode(ctx context.Context, dial httpclient.Dialer, timeoutMs int, attempts int) ProbeResult {
	start := time.Now()
	if attempts < 1 {
		attempts = 1 // JS Math.max(1, attempts)
	}
	urls := p.stage1URLs()
	uaOf := p.uaOf()
	var samples []int64
	for range attempts {
		attemptStart := time.Now()
		judge := func(resp *http.Response, url string) (any, error) {
			// 闸门走内容层判据，liveness 源只判状态码——闸门那 200 必须真的带
			// 一份模型清单，否则「探测通过」和「真实对话能过」是两件事。
			if p.gate != "" && url == p.gate {
				if err := gateVerdict(resp); err != nil {
					return nil, err
				}
			} else if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
				return nil, fmt.Errorf("reachability HTTP %d", resp.StatusCode)
			}
			return time.Since(attemptStart).Milliseconds(), nil
		}
		if v := firstSuccess(ctx, dial, urls, timeoutMs, uaOf, judge); v != nil {
			if ms, ok := v.(int64); ok {
				samples = append(samples, ms)
			}
		}
	}
	if len(samples) == 0 {
		if ctx.Err() != nil {
			// C2:取消导致的「一枪都没打完」不是判决。调用方(app/probe.go 的
			// 取消守卫)会把取消轮整体丢弃,但 MarkProbe 对 unknown 本来就有
			// 「跳过」契约 —— 在这里就报 unknown,让「取消 ≠ dead」由类型
			// 保证,而不只依赖调用方的守卫。dead 会改写健康行并参与淘汰,
			// 一整轮取消就能把全池写成 dead(C2 的事故链)。
			return ProbeResult{State: StateUnknown, LatencyMS: time.Since(start).Milliseconds(), Incomplete: true}
		}
		return ProbeResult{State: StateDead, LatencyMS: time.Since(start).Milliseconds()}
	}
	var echo exitInfo
	if len(p.echo) > 0 {
		echoJudge := func(resp *http.Response, _ string) (any, error) {
			body, err := httpclient.ReadCapped(resp.Body, maxEchoBodyBytes)
			if err != nil {
				return nil, fmt.Errorf("echo body unreadable: %w", err)
			}
			return exitInfoOf(body)
		}
		if v := firstSuccess(ctx, dial, p.echo, echoBudgetMS, func(string) string { return ProbeUA }, echoJudge); v != nil {
			if info, ok := v.(exitInfo); ok {
				echo = info
			}
		}
	}
	return ProbeResult{
		State:       StateAlive,
		LatencyMS:   samples[0],
		LatencyMin:  slices.Min(samples),
		ExitIP:      echo.ip,
		ExitCountry: echo.country,
	}
}

// ProbeAll 有界并发粗探测，移植自 src/nodeprobe.js:305。
//
// 每个 worker 有自己的**超时兜底**。probeNode 内部每一发都有预算，理论上最坏耗时
// 是 attempts×timeoutMs 加上 echo 的 8s；但「理论上」不成立：easy-proxies
// internal/boxmgr/manager.go 记录过某些 sing-box 协议**不响应 ctx 取消**，那个
// worker 就永久挂着——它的 slot 不释放，Promise.all 永不返回，**整轮探测**卡在
// 一个节点上，其余 2499 个节点的结果全部作废。这不是「某个节点判错」，是「整轮
// 不出结果」，所以必须有一层不依赖底层配合的兜底。
//
// 兜底到点后：
//  1. 先 cancel 这个 worker 自己的 ctx（掐掉它底下那批 shot，别留僵尸 goroutine
//     ——ctx 能传导到的都会当场收尾；对真不响应取消的底层，JS 版同样只能放弃
//     等待，cancel 已是能做的全部）；
//  2. 把该节点记为**本轮不完整**：State=unknown、LatencyMS=-1、Incomplete=true
//     ——既不 alive 也**不判 dead**：判 dead 会白白给一个可能健康的节点记一次
//     连败，调用方拿到 unknown 应当跳过它这一轮。
//
// 预算 attempts×timeoutMs+5000：必须**不小于** probeNode 自己允许的最坏耗时，
// 否则兜底会把正常慢节点误判成不完整（tests/nodeprobe.test.js:71-99 钉的就是
// 这个下界）。
//
// 与 JS 的差异：JS 用 onResult 回调乱序回报；Go 按输入顺序整批返回（结果槽位按
// 下标写，worker 间无竞争），app 落盘与断言都需要确定性顺序。
func (p *Prober) ProbeAll(ctx context.Context, items []Item, workers int) []Result {
	out := make([]Result, len(items))
	for i := range items {
		out[i].Tag = items[i].Tag
	}
	if len(items) == 0 {
		return out
	}
	if workers < 1 {
		workers = 1
	}
	if workers > len(items) {
		workers = len(items) // JS Math.max(1, Math.min(workers, items.length))
	}
	var cursor atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(cursor.Add(1)) - 1 // JS cursor++ 的原子版
				if i >= len(items) {
					return
				}
				p.probeOne(ctx, items[i], &out[i])
			}
		}()
	}
	wg.Wait()
	return out
}

// probeOne 带兜底地探测单项。done 缓冲 1：兜底放弃等待后，probeNode 仍能无阻塞
// 收尾退出，不因没人读结果而挂死。
func (p *Prober) probeOne(ctx context.Context, item Item, out *Result) {
	timeoutMs := item.Options.TimeoutMS
	if timeoutMs <= 0 {
		timeoutMs = defaultTimeoutMS
	}
	attempts := item.Options.Attempts
	if attempts <= 0 {
		attempts = defaultAttempts
	}
	budget := time.Duration(attempts*timeoutMs+backstopSlackMS) * time.Millisecond
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	done := make(chan ProbeResult, 1)
	go func() { done <- p.ProbeNode(wctx, item.Dial, timeoutMs, attempts) }()
	select {
	case r := <-done:
		out.Result = r
	case <-timer.C:
		// 先掐 worker ctx 再写结果：底下每发 shot 的 ctx 都是 wctx 的子孙，
		// 这一声 cancel 就是「别留僵尸」的那一刀。
		cancel()
		out.Result = ProbeResult{State: StateUnknown, LatencyMS: -1, Incomplete: true}
		p.log("warn", "probe backstop fired for "+item.Tag)
	}
}

// ProbeDirect 探测**探测源自己**能不能上网——直连，不经任何节点。移植自
// src/nodeprobe.js:208，理由照搬：一个节点探测失败有两种完全不同的原因——那个
// 节点坏了，或者**我们这边**出不去（本机断网、DNS 坏了、上游被本地策略拦了）。
// 第二种情况下整轮会把全部健康节点判成死的，然后被 retainOnly 连败计数淘汰掉。
// 少淘汰一轮只是下一轮再测一次；误淘汰一轮要把几百个健康节点等回订阅刷新。对照
// VPNpool vpnpoold:223-225：探测失败先问 WAN 本身还活着没有；build.sh:129-133
// 规定 0 个可达时一个都不淘汰。用的是与节点探测**同一批** liveness 源：多一个
// 源就多一个「我们和它之间」的未知数，判据就不干净了。
//
// 只测 liveness：无 echo（出口 IP 对「我们自己」无意义）、无闸门（那是节点要过
// 的门）。返回 nil 表示探测源自己没问题。
func (p *Prober) ProbeDirect(ctx context.Context, timeoutMs int) error {
	reachable := firstSuccess(ctx, nil, p.liveness, timeoutMs,
		func(string) string { return ProbeUA },
		func(resp *http.Response, _ string) (any, error) {
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
				return nil, fmt.Errorf("reachability HTTP %d", resp.StatusCode)
			}
			return true, nil
		})
	if reachable == nil {
		return fmt.Errorf("no liveness source reachable over direct network")
	}
	return nil
}
