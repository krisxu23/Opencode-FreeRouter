// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import (
	"regexp"
	"strconv"
	"strings"
)

// IdentityOf builds the string that decides "is this the same node as that one".
//
// 拼接顺序与拼写是**逐字节契约**，不是风格选择：data/node-registry.json 里
// JS 版 identityOf（src/parse-links.js:58）落盘的墓碑键是
//
//	vless|<server>:<port>|uuid=…|sni=…|tr=ws|path=/Ra-vl|host=…
//	hysteria2|<server>:443|password=…|sni=…|obfs=salamander:…
//	vless|<server>:<port>|uuid=…|sni=…|pbk=…|sid=|tr=grpc|svc=…
//
// 任何一条对不上（漏 `uuid=` 前缀、漏空的 `sid=`、obfs 少半段），现存墓碑
// 全部失配，被淘汰的节点会立刻复活、连败记忆被清零。
//
// 两个条目只要会拨同一个服务器、用同一套凭据，就是同一个代理，**不管它叫
// 什么名字** —— 订阅商随意改名，按名字做键会因为一次改名把节点连败记忆丢掉。
//
// 这里不做任何 lower/trim 之类的规范化：node-registry.json 已有只差大小写的
// 重复键，规范化会让墓碑永远匹配不上（总纲约束 9）。
func IdentityOf(o Outbound) string {
	// 前两段必须与旧版完全一致的写法与顺序：这是"旧键等价"的保证。
	parts := []string{o.Type, o.Server + ":" + portsOf(o)}
	if o.UUID != "" {
		parts = append(parts, "uuid="+o.UUID)
	}
	if o.Password != "" {
		parts = append(parts, "password="+o.Password)
	}
	if o.Method != "" {
		parts = append(parts, "method="+o.Method)
	}
	if o.Flow != "" {
		parts = append(parts, "flow="+o.Flow)
	}
	// JS 版是 `alter_id !== undefined` 就拼；本项目自己的解析器只在 aid>0 时
	// 写这个字段（与 JS parseVmess / clashProxyToOutbound 同规则），所以 != 0
	// 与 JS 的"存在即拼"在真实数据上等价。
	if o.AlterID != 0 {
		parts = append(parts, "aid="+strconv.Itoa(o.AlterID))
	}
	if o.TLS != nil {
		if o.TLS.ServerName != "" {
			parts = append(parts, "sni="+o.TLS.ServerName)
		}
		if o.TLS.Reality != nil {
			// pbk/sid 成对拼，sid 为空也要占位（真实墓碑里有 `pbk=…|sid=|tr=…`）。
			parts = append(parts, "pbk="+o.TLS.Reality.PublicKey, "sid="+o.TLS.Reality.ShortID)
		}
		if o.TLS.Insecure != nil && *o.TLS.Insecure {
			parts = append(parts, "insecure")
		}
	}
	if o.Transport != nil {
		// JS 版 transport 存在就拼 `tr=`（哪怕 type 为空），不能改成"非空才拼"。
		parts = append(parts, "tr="+o.Transport.Type)
		if o.Transport.Path != "" {
			parts = append(parts, "path="+o.Transport.Path)
		}
		if o.Transport.Service != "" {
			parts = append(parts, "svc="+o.Transport.Service)
		}
		if h := transportHost(o.Transport); h != "" {
			parts = append(parts, "host="+h)
		}
	}
	if o.Obfs != nil {
		parts = append(parts, "obfs="+o.Obfs.Type+":"+o.Obfs.Password)
	}
	// 顶层 path(R22):sing-box-JSON 与 Clash 两条路径把 http 类代理的 path 放在
	// **顶层**(clash.go 的 o.Path、SingBoxMap 也按顶层 path 发出),而过去只有
	// transport.path 进指纹 ⇒ 两个只有顶层 path 不同的代理塌缩成同一身份,轮内去重
	// 丢掉一个、Merge 把两台服务器当一个物理节点(连败与墓碑还会互相传染)。
	// 只在非空时才拼:没有顶层 path 的既有身份串一字不变 —— 现网墓碑键不至于全部失配。
	if o.Path != "" {
		parts = append(parts, "path="+o.Path)
	}
	return strings.Join(parts, "|")
}

func portsOf(o Outbound) string {
	// server_ports 是 "start:end" 区间串（hysteria2 mport 的唯一合法形状，
	// 裸单端口会 sing-box FATAL），join(',') 与 JS 版一致。
	if len(o.ServerPorts) > 0 {
		return strings.Join(o.ServerPorts, ",")
	}
	return strconv.Itoa(o.ServerPort)
}

// transportHost 取 host= 段的来源：transport.headers.Host 优先，缺席才回落
// transport.host（JS 的 ?? 语义 —— headers.Host 存在但为空串时**不**回落）。
// 数组形状（h2 的 host）join 成逗号串。
func transportHost(t *Transport) string {
	var host any
	if t.Headers != nil {
		if h, ok := t.Headers["Host"]; ok {
			host = h
		}
	}
	if host == nil {
		host = t.Host
	}
	return hostToString(host)
}

func hostToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []string:
		return strings.Join(t, ",")
	case []any:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = str(e)
		}
		return strings.Join(parts, ",")
	default:
		return str(v)
	}
}

// isUnroutable 的正则组逐字照抄 src/parse-links.js:33 isUnroutableServer。
// 这些模式是实测踩坑的结晶（127.0.0.1 假节点、::ffff: 映射地址绕过、
// 172.x 只有 16-31 段是私网），一个字符都不能"顺手优化"。
var (
	reLoopback127  = regexp.MustCompile(`^127(\.\d{1,3}){3}$`)
	reRfc1918Ten   = regexp.MustCompile(`^10(\.\d{1,3}){3}$`)
	reRfc1918_192  = regexp.MustCompile(`^192\.168(\.\d{1,3}){2}$`)
	reRfc1918_172  = regexp.MustCompile(`^172\.(1[6-9]|2\d|3[01])(\.\d{1,3}){2}$`)
	reLinkLocal169 = regexp.MustCompile(`^169\.254(\.\d{1,3}){2}$`)
	reIPv6ULA      = regexp.MustCompile(`^f[cd][0-9a-f]{2}:`)
)

// IsUnroutableServer reports whether a node advertises an address this machine
// cannot use as a remote proxy: loopback, RFC1918, link-local, or IPv6 ULA.
//
// 免费订阅里塞「广告节点」是常态：把 server 写成 127.0.0.1，用来告诉用户
// 「流量用完了，去官网充值」。对网关来说这些节点的后果比客户端更重：它占一个
// 出站槽、一份探测 worker，而且每轮 rebuild 重新进池一次 —— 永久居民，且永远
// 不会 alive；把流量送进 RFC1918 更是有后果的（可达内网服务）。
//
// 只判字面地址，不做 DNS 解析 —— 解析会把每个节点名变成一次网络请求（数千次），
// 而且 localhost.example.com 这类真实存在的域名会被误杀。::ffff:127.0.0.1 是
// IPv4-mapped 写法，必须显式列出，否则 IPv6 栈下绕过整条判据。
//
// 空地址返回 false（没有意见）而不是 true：sing-box JSON 订阅里的 direct /
// selector / block 出站压根没有 server 字段，判它们「指向本机」就是越权 ——
// 剔除它们是 FilterByGroups 的 PROXY_TYPES 职责，判据里重复一遍只会让两个
// 职责纠缠。
func IsUnroutableServer(server string) bool {
	h := strings.ToLower(strings.TrimSpace(server))
	h = strings.TrimPrefix(h, "[")
	h = strings.TrimSuffix(h, "]")
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return false
	}
	if h == "localhost" || h == "::1" || h == "::" || h == "0.0.0.0" {
		return true
	}
	if reLoopback127.MatchString(h) {
		return true
	}
	if strings.HasPrefix(h, "::ffff:127.") {
		return true
	}
	if reRfc1918Ten.MatchString(h) {
		return true
	}
	if reRfc1918_192.MatchString(h) {
		return true
	}
	if reRfc1918_172.MatchString(h) {
		return true
	}
	if reLinkLocal169.MatchString(h) {
		return true
	}
	// IPv6 ULA fc00::/7
	return reIPv6ULA.MatchString(h)
}
