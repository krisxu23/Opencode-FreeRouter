// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package sub pulls every subscription source, merges them, and caches the
// last-good result for offline boots. The two-tier fetch strategy (direct
// first, live-exit retry second) is ported verbatim from src/sub.js — it is
// measured behavior, not a design to be simplified.
package sub

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"freerouter/internal/check"
	"freerouter/internal/httpclient"
	"freerouter/internal/parse"
)

// Exit is one working node the fetcher can retry a failing source through.
//
// The JS build named this exitAddrs because every node had a local mixed-inbound
// port and "go out through node X" meant "GET http://127.0.0.1:<port>/". With
// sing-box linked in there is no local port: the way out is a dialer. Keeping the
// name Exit and the struct shape explicit means every call site has to be looked
// at during the port removal, instead of silently passing a port list to
// something that now wants a dialer.
type Exit struct {
	// Name identifies the node in the error text, e.g. "🇩🇪 德国 01".
	Name string
	// Dial opens the connection. Reusing it for many requests is fine; it is
	// an http.Transport DialContext, not a one-shot connection.
	Dial httpclient.Dialer
	// Header is added to the retried request. Mostly a test hook: it lets a test
	// tell "went out through the exit" from "went direct".
	Header map[string]string
}

// Result is the merged outcome of one Fetch round, and the exact on-disk shape
// of data/subs_cache.json: {"outbounds","fetchedAt","sources","details"}.
//
// FetchedAt 和 Sources 是修正案 §3 要求补回的字段（正文只列了 Outbounds/Details）：
// 缓存形状必须与 JS 现网文件兼容，fetchedAt 是毫秒时间戳（Date.now 的等价物），
// sources 是清洗（trim、去空）后的源列表。
type Result struct {
	Outbounds []parse.Outbound `json:"outbounds"`
	FetchedAt int64            `json:"fetchedAt"`
	Sources   []string         `json:"sources"`
	Details   []Detail         `json:"details"`
}

// Detail is the per-source fetch record the panel renders as the last line of
// the subscription card. Nodes 只在成功时写（该源去重后贡献的节点数），Error 只在
// 失败时写 —— 与 JS 版 details 条目的键存在性一致。
type Detail struct {
	URL   string `json:"url"`
	OK    bool   `json:"ok"`
	Nodes int    `json:"nodes,omitempty"`
	Error string `json:"error,omitempty"`
}

// attemptTimeout 是"单次尝试"的上限。src/sub.js:313-315 的
// AbortSignal.timeout(20_000) 对直连与经出口的每一次拉取一视同仁：被墙直连的
// ETIMEDOUT、死出口的连接悬挂，都不该吃满整次拉取预算；整体预算由调用方的
// ctx 控制（JS 版的 signal）。计划的"直连 30s"是笔误级偏差，按任务指示以
// src/sub.js 为准，统一 20s。
const attemptTimeout = 20 * time.Second

// subUserAgent / acceptHeader 逐字照抄 src/sub.js:316。UA 不是随手写的：不少
// 订阅源按 UA 下发不同格式（clash UA → Clash YAML，陌生 UA 可能 403 或回纯
// 文本），计划里"换成 freerouter/sub 便于识别"会在真实源上破坏拉取。要改 UA
// 必须先对目标源实测。
const (
	subUserAgent = "clash.meta/1.18.1"
	acceptHeader = "application/json, text/yaml, text/plain, */*"
)

// maxSubBodyBytes caps one subscription response (R3). 实测 data/subs_cache.json
// 是 432KB 量级,所以 32MB 对真实源宽松得离谱,但对一个无限发流的源是硬墙。
const maxSubBodyBytes = 32 << 20

// Fetch 拉取全部订阅源并合并（不再"首个成功即用"），按配置指纹去重。
//
// 两级策略逐字照搬 src/sub.js:276-336（这是实测有效的行为，不是可以"简化"的
// 设计）：
//  1. 第一轮：全部源直连并行（JS 的 Promise.allSettled → 每个源一个结果下标）。
//  2. 第二轮：只对失败源按 exits 顺序复拉，每源至多 check.SubRetryExits=12 个
//     出口，每次新建请求，串行 per 源（避免打爆单节点）。
//  3. 任一源成功即正常返回；失败源只留在 Details[i].Error。
//  4. 仅当没有任何源产出出站（全挂）才返回 error，错误串带上全部 Details。
func Fetch(ctx context.Context, sources []string, exits []Exit) (Result, error) {
	list := make([]string, 0, len(sources))
	for _, s := range sources {
		if t := strings.TrimSpace(s); t != "" {
			list = append(list, t)
		}
	}
	if len(list) == 0 {
		return Result{}, errors.New("no subscription sources configured")
	}

	direct := subHTTPClient(nil)
	raw := make([][]parse.Outbound, len(list)) // nil = 该源还没成功
	details := make([]Detail, len(list))

	// 第一轮：全部直连并行。每个 goroutine 只写自己的下标，无锁；
	// wg.Wait() 返回后才有读方，happens-before 由 WaitGroup 保证。
	var wg sync.WaitGroup
	for i, url := range list {
		wg.Add(1)
		go func(i int, url string) {
			defer wg.Done()
			obs, err := fetchOne(ctx, direct, url, nil)
			if err != nil {
				details[i] = Detail{URL: url, OK: false, Error: detailError(err)}
				return
			}
			raw[i] = obs
		}(i, url)
	}
	wg.Wait()

	// 第二轮：直连失败的源，经健康出口逐个复拉（串行 per 源）。
	// 出口数上限 check.SubRetryExits = 12 —— 2026-09-28 实测从 3 提到 12：
	// 本机直连被墙时（实测 Cloudflare 系域名 ETIMEDOUT），经健康出口复拉是
	// 唯一更新通道，而出口池有 22 个。只试 3 个太看运气——实测前两个出口
	// ECONNRESET、第三个才成功，运气差时三个全坏就等于这个源彻底拉不到，
	// 于是池子只能靠剩下那个能直连的源（354 节点）续命。
	exitClients := make([]*http.Client, len(exits))
	clientsBuilt := false
	for i := range list {
		if raw[i] != nil || len(exits) == 0 {
			continue
		}
		if !clientsBuilt {
			// 客户端按出口缓存一份而不是每次尝试新建：Transport 不 dial 就不占
			// 连接，而复用让同一出口的连接池真的能池化。
			clientsBuilt = true
			for j, ex := range exits {
				if ex.Dial == nil {
					continue
				}
				exitClients[j] = subHTTPClient(ex.Dial)
			}
		}
		for j := range exits {
			if j >= check.SubRetryExits {
				break
			}
			if exitClients[j] == nil {
				continue
			}
			obs, err := fetchOne(ctx, exitClients[j], list[i], exits[j].Header)
			if err == nil {
				raw[i] = obs
				break
			}
			details[i] = Detail{URL: list[i], OK: false, Error: detailError(err)}
			if ctx.Err() != nil {
				break // JS 版的 signal?.aborted：整体已取消，别再烧出口
			}
		}
	}

	// 汇总 + 指纹去重（保序，首个胜出）。去重键是配置指纹而不是节点名：两个
	// 条目只要会拨同一个服务器、用同一套凭据，就是同一个代理，不管它叫什么
	// （src/sub.js:348）。旧键 `${tag}|${type}|${server}:${port}` 有两个反向
	// 缺陷：同一个节点换个名字（US-01 → 🇺🇸 US-01）算新节点，白占一个端口槽；
	// 两个不同节点撞名时后者被整条丢掉 —— 那是真的丢节点。指纹与 registry 的
	// 墓碑键共用 L0 那份 IdentityOf，「轮内去重」和「跨轮继承失败记忆」对同一
	// 物理节点的判断是同一个，不会互相漂移。
	merged := []parse.Outbound{}
	seen := map[string]bool{}
	for i := range list {
		if raw[i] == nil {
			continue
		}
		nodes := 0
		for _, o := range raw[i] {
			key := parse.IdentityOf(o)
			if seen[key] {
				continue
			}
			seen[key] = true
			merged = append(merged, o)
			nodes++
		}
		details[i] = Detail{URL: list[i], OK: true, Nodes: nodes}
	}
	if len(merged) == 0 {
		parts := make([]string, 0, len(details))
		for _, d := range details {
			parts = append(parts, truncate(d.URL, 60)+" → "+d.Error)
		}
		return Result{Details: details}, errors.New("全部订阅源均失败: " + truncate(strings.Join(parts, " | "), 400))
	}
	return Result{
		Outbounds: merged,
		FetchedAt: time.Now().UnixMilli(),
		Sources:   list,
		Details:   details,
	}, nil
}

// fetchOne 拉一个源并解析。extra 是出口身份头（仅第二轮复拉时携带）。
// 对 2xx 之外的状态码、以及"一个节点都没认出来"的响应体，都按源失败处理
// （JS: `if (!r.ok) throw` / `if (!outbounds.length) throw`），不中断其它源。
// subHTTPClient 是订阅拉取专用 client:重定向策略收紧到同 scheme、不落
// 本机/私网地址,最多 3 跳。Go 默认策略是 10 跳跟随到任意 http/https 主机
// (含 127.0.0.1 与内网) —— 一个不可信的订阅服务器回 302 就能把拉取器变成
// 内网触探器,Detail.Error 的文案差异("HTTP 404" vs 「没有识别出任何节点」)
// 还能当内网端口/服务存在性的侧信道。nodeprobe 的 client 早就这么设了,
// 订阅这条车道是漏网之鱼。
func subHTTPClient(dial httpclient.Dialer) *http.Client {
	c := httpclient.NewClient(dial, attemptTimeout)
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("重定向超过 3 跳")
		}
		if len(via) > 0 && req.URL.Scheme != via[0].URL.Scheme {
			return fmt.Errorf("重定向跨 scheme(%s → %s)", via[0].URL.Scheme, req.URL.Scheme)
		}
		if parse.IsUnroutableServer(req.URL.Hostname()) {
			return fmt.Errorf("重定向指向不可路由地址 %s", req.URL.Host)
		}
		return nil
	}
	return c
}

func fetchOne(ctx context.Context, c *http.Client, url string, extra map[string]string) ([]parse.Outbound, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("accept", acceptHeader)
	req.Header.Set("user-agent", subUserAgent)
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := httpclient.ReadCapped(resp.Body, maxSubBodyBytes)
	if err != nil {
		return nil, err
	}
	outbounds, reason := parseSubscriptionBodyWithReason(string(body))
	outbounds = dropUnroutable(outbounds)
	if len(outbounds) == 0 {
		if reason != "" {
			return nil, fmt.Errorf("没有识别出任何节点：%s", reason)
		}
		return nil, errors.New("没有识别出任何节点（格式不受支持？）")
	}
	return outbounds, nil
}

// parseSubscriptionBody 把一段订阅文本解析成出站数组；「指向本机的假节点」
// 这道过滤在四种格式的汇合点收口（src/sub.js parseSubscriptionBody），而不是
// 在各解析器里各写一遍。
func parseSubscriptionBody(text string) []parse.Outbound {
	obs, _ := parseSubscriptionBodyWithReason(text)
	return dropUnroutable(obs)
}

// parseSubscriptionBodyWithReason 是四种订阅格式的分发本体，顺序逐字照抄
// src/sub.js:202-222：sing-box JSON → Clash YAML → 明文/整段 base64 节点链接。
// 某一种格式认输（坏了、键不存在、空数组）就静默落到下一种。
//
// 第二个返回值是「认输的那些格式各自为什么认输」(R25)。回落顺序本身是对的，
// 但过去把 ParseClashYAML 的 err 直接丢弃：一份带重复键的 Clash 订阅（整份文件
// 认输）落到 ParseLinks（没有 ://）返回 nil，运维只能看到「格式不受支持？」，
// 真因完全不可见 —— 而这两种情况在面板上长得一模一样。JS 的
// `try { YAML.parse } catch { /* fall through */ }` 同样吞错（同源），
// Go 侧把原因带上来。
func parseSubscriptionBodyWithReason(text string) ([]parse.Outbound, string) {
	body := strings.TrimSpace(text)
	if body == "" {
		return nil, ""
	}
	var reasons []string
	if strings.HasPrefix(body, "{") {
		obs, err := parse.ParseSingBoxJSON(body)
		if err == nil && len(obs) > 0 {
			return obs, ""
		}
		if err != nil {
			reasons = append(reasons, "sing-box JSON: "+err.Error())
		} else if len(obs) == 0 {
			reasons = append(reasons, "sing-box JSON: outbounds 为空")
		}
	}
	if proxiesLineRe.MatchString(body) || strings.Contains(body, "\nproxies:") || strings.HasPrefix(body, "proxies:") {
		obs, err := parse.ParseClashYAML(body)
		if err == nil && len(obs) > 0 {
			return obs, ""
		}
		if err != nil {
			reasons = append(reasons, "Clash YAML: "+err.Error())
		} else {
			reasons = append(reasons, "Clash YAML: proxies 为空")
		}
	}
	if links := parse.ParseLinks(body); len(links) > 0 {
		return links, ""
	}
	return nil, strings.Join(reasons, "; ")
}

// ParseSubscriptionBody 是 parseSubscriptionBody 的导出面。单独开一个导出名
// 而不是直接改私有函数的名字，是为了让包内既有调用点（fetchOne）与注释里的
// JS 对应关系保持原样；需要从包外调到这条完整链路的只有差分验收
// （difftest/任务 27），它必须喂到与 JS 版 parseSubscriptionBody 相同的生产
// 函数，而不是在测试架里重拼 ParseClashYAML + dropUnroutable。
func ParseSubscriptionBody(text string) []parse.Outbound {
	return parseSubscriptionBody(text)
}

// proxiesLineRe 对应 JS /^\s*proxies:\s*$/m：某一行只有 proxies: 键。
var proxiesLineRe = regexp.MustCompile(`(?m)^\s*proxies:\s*$`)

// dropUnroutable 丢掉 server 指向本机/本网络的节点（src/sub.js:193）。这些
// 节点会占探测 worker，每轮 rebuild 重新进池一次，永远不会 alive —— 是永久
// 居民。空地址（sing-box 订阅里的 selector/direct/block 出站没有 server）
// 不表态：剔除它们是 FilterByGroups 的 PROXY_TYPES 职责，判据里重复一遍只会
// 让两个职责纠缠。
func dropUnroutable(obs []parse.Outbound) []parse.Outbound {
	out := make([]parse.Outbound, 0, len(obs))
	for _, o := range obs {
		if !parse.IsUnroutableServer(o.Server) {
			out = append(out, o)
		}
	}
	return out
}

// errnoShortNames 把真实的套接字错误号映回稳定短码 —— src/sub.js:325-330 挖
// undici cause.code 的 Go 版等价物。undici 的网络错误把真因藏在 cause
// （ECONNREFUSED / ETIMEDOUT / 聚合错误），只取 message 的话日志里永远只有一句
// 无法定位的 "fetch failed"。Go 的错误链是 *net.OpError → *os.SyscallError →
// syscall.Errno，errors.As 一路 unwrap 就能拿到 Errno；但 Errno 的 Error()
// 文案随平台与系统语言漂移（Windows 是一整句英文），所以按错误号映回短码。
//
// 两段登记的原因：POSIX 侧用 syscall 包常量（如 Linux ECONNREFUSED=111）；
// Windows 的真实套接字错误落在 WSA 段（10061=WSAECONNREFUSED…），syscall 包里
// 的 POSIX 名是 APPLICATION_ERROR 起步的"发明值"，号对不上，必须按数字另行
// 登记。WSA 段的号在 POSIX 侧不存在（Linux errno 最大约 133），一张表不会
// 互相误配。
var errnoShortNames = map[syscall.Errno]string{
	syscall.ECONNREFUSED: "ECONNREFUSED",
	syscall.ECONNRESET:   "ECONNRESET",
	syscall.ETIMEDOUT:    "ETIMEDOUT",
	syscall.ECONNABORTED: "ECONNABORTED",
	syscall.EHOSTUNREACH: "EHOSTUNREACH",
	syscall.ENETUNREACH:  "ENETUNREACH",
	syscall.EADDRINUSE:   "EADDRINUSE",
	syscall.EACCES:       "EACCES",
	10061:                "ECONNREFUSED", // WSAECONNREFUSED
	10054:                "ECONNRESET",   // WSAECONNRESET
	10060:                "ETIMEDOUT",    // WSAETIMEDOUT
	10053:                "ECONNABORTED", // WSAECONNABORTED
	10065:                "EHOSTUNREACH", // WSAEHOSTUNREACH
	10051:                "ENETUNREACH",  // WSAENETUNREACH
	10048:                "EADDRINUSE",   // WSAEADDRINUSE
	10013:                "EACCES",       // WSAEACCES
}

// shortCode 沿 unwrap 链找 syscall.Errno 并映回短码；找不到返回空串，
// Detail.Error 就只剩错误自身的文案（超时类错误没有 errno，属正常）。
func shortCode(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errnoShortNames[errno]
	}
	return ""
}

// detailError 组装 Detail.Error：错误文案 + 短码后缀，截到 160 rune。
// JS 版 .slice(0, 160) 按 UTF-16 单元截断，这里按 rune 截，中文更省。
func detailError(err error) string {
	msg := err.Error()
	if code := shortCode(err); code != "" {
		msg += " (" + code + ")"
	}
	return truncate(msg, 160)
}

// truncate 按 rune 截断，超出部分直接丢（日志串，不是协议）。
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

// R28：这里曾有 LoadCache/SaveCache（data/subs_cache.json 的读写），生产零调用点。
// JS 的 loadCache/saveCache 是活的（index.js:413-422：拉取失败回落缓存、成功才
// 回写），Go 版把同一条语义交给了节点注册表 —— registry 每次 rebuild 都落盘、
// 带墓碑与连败记忆，rebuild.go 的失败回落读的就是它。留着第二份 last-good 缓存
// 只会让"离线启动用哪份名单"有两个答案，所以删掉而不是接线。
// 现网 data/subs_cache.json（432KB，JS 遗留）自此无人读写，由运维自行删除。
