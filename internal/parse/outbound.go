// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package parse turns subscription text into sing-box outbounds and gives each
// one a stable identity.
package parse

// Extra carries the sing-box fields this project does not model explicitly.
//
// Constraint 8 makes this load-bearing. sing-box understands more outbound
// fields than we parse (multiplex, domain_strategy, udp_over_tcp,
// network_strategy, detour, ...). A subscription that sets them works today
// because the JS build passed the whole parsed object through. Dropping them
// here would not produce an error -- sing-box would simply ignore an absent
// field, and the node would fail to connect with nothing in the log to explain
// why. So every field we do not name lands in Extra and is re-emitted
// verbatim.
//
// 注意：Extra 是"摊平"语义 —— 生成 sing-box 配置时（任务 7）必须把 Extra 的键
// 摊回出站对象的顶层，而不是嵌在 "extra" 键下面；sing-box 对出站选项做严格
// 解码，嵌套的未知键同样是 decode 期 FATAL。
type Extra map[string]any

// Outbound is one proxy node. It is a superset of what the panel shows and a
// subset of what sing-box accepts; the gap between the two is Extra.
type Outbound struct {
	// Tag 加 omitempty 与 JS 的「键不存在就不写」对位：sing-box JSON 订阅里的
	// selector/urltest 可以没有 tag，JS 原样透传（没有这个键），Go 的空串不能
	// 凭空落盘（差分 A5 抓到：sanitize 输出的出站本就无 tag）。
	Tag         string     `json:"tag,omitempty"`
	Type        string     `json:"type"`
	Server      string     `json:"server,omitempty"`
	ServerPort  int        `json:"server_port,omitempty"`
	ServerPorts []string   `json:"server_ports,omitempty"`
	UUID        string     `json:"uuid,omitempty"`
	Password    string     `json:"password,omitempty"`
	Method      string     `json:"method,omitempty"`
	Flow        string     `json:"flow,omitempty"`
	AlterID     int        `json:"alter_id,omitempty"`
	Path        string     `json:"path,omitempty"`
	Obfs        *Obfs      `json:"obfs,omitempty"`
	TLS         *TLS       `json:"tls,omitempty"`
	Transport   *Transport `json:"transport,omitempty"`
	Extra       Extra      `json:"extra,omitempty"`
}

// Obfs is the obfuscation wrapper.
//
// 两种形状共用一个结构：hysteria2 的 salamander 混淆是 {type,password}
// （parse-links.js parseHysteria2），真实墓碑键也按 `obfs=salamander:pw` 拼接，
// 所以 Password 必须在 —— 丢掉它混淆参数就静默失效，节点连不上且无日志。
type Obfs struct {
	Type     string `json:"type,omitempty"`
	Host     string `json:"host,omitempty"`
	Password string `json:"password,omitempty"`
}

// TLS carries the TLS layer including the REALITY block.
//
// Insecure 用 *bool 是数据形状的硬约束，不是风格：JS 版只在**链接/Clash 解析
// 器**构造的 tls 块里恒写 insecure（false 也写），而 sing-box JSON 订阅的 tls
// 块是原样透传 —— 有就是有、没有就是没有。bool 加 omitempty 会把 false 错误
// 地抹掉（差分 A1/A2 抓到），去掉 omitempty 又会给透传块凭空加字段（A3 抓到），
// 三态只有指针能表达。
type TLS struct {
	// Enabled 也走三态指针：JS sanitize 的判据是 `tls.enabled === false` 才删块
	// （src/singbox.js:242），**缺 enabled 视为开启并补写 enabled:true** —— Go 的
	// bool 零值把「缺」和「false」混成一种，曾把无 enabled 的 tls 块整个丢掉，
	// 与它自己文档注释里写的 "missing enabled means enabled" 相反（差分 A5 钉住）。
	Enabled    *bool    `json:"enabled,omitempty"`
	ServerName string   `json:"server_name,omitempty"`
	Insecure   *bool    `json:"insecure,omitempty"`
	ALPN       []string `json:"alpn,omitempty"`
	UTLS       *UTLS    `json:"utls,omitempty"`
	Reality    *Reality `json:"reality,omitempty"`
}

// BoolPtr 是 TLS.Insecure 三态的构造辅助：解析器恒写（false 也写），
// 透传路径只在键真实存在时写。
func BoolPtr(b bool) *bool { return &b }

// UTLS is the uTLS fingerprint.
type UTLS struct {
	Enabled     bool   `json:"enabled,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

// Reality is the REALITY handshake parameters.
type Reality struct {
	Enabled   bool   `json:"enabled,omitempty"`
	PublicKey string `json:"public_key,omitempty"`
	ShortID   string `json:"short_id,omitempty"`
}

// Transport is the transport layer (ws / grpc / http / httpupgrade / quic).
//
// Host 用 any 而不是 string：JS 版 h2 传输把 host 存成数组（sing-box 的 http
// transport 的 host 是数组），httpupgrade 存成字符串 —— 两种形状不能互相改写，
// 改写了配置就变形。
type Transport struct {
	Type    string         `json:"type,omitempty"`
	Path    string         `json:"path,omitempty"`
	Service string         `json:"service_name,omitempty"`
	Headers map[string]any `json:"headers,omitempty"`
	Host    any            `json:"host,omitempty"`
}
