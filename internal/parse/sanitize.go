// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import (
	"encoding/base64"
	"regexp"
	"strings"
)

// 出站整形：修正订阅方言里 sing-box 会直接拒绝（丢整份配置）或首拨才 panic
// 的形状。规则逐条对照 src/singbox.js:221 sanitizeOutbound 及其注释。

// ssMethods 是 sing-box v1.14 真正接受的 shadowsocks cipher 名单（17 个）。
//
// 逐个用 `bin/sing-box.exe check -c` 实测得出，不是抄文档：接受 aes-{128,192,
// 256}-{gcm,cfb,ctr}、chacha20-ietf-poly1305、xchacha20-ietf-poly1305、rc4-md5、
// chacha20-ietf、none，以及 2022-blake3-* 三兄弟；拒绝 plain、bf-cfb、
// camellia-*-cfb、cast5-cfb、des-cfb、idea-cfb、rc2-cfb、seed-cfb、salsa20、
// aes-128-ocb、aes-256-gcm-siv。原名单曾把 sing-box 其实接受的 aes-*-cfb/
// aes-*-ctr/rc4-md5 当成未知，同时收了一个并不存在的 plain。
var ssMethods = map[string]bool{
	"aes-128-gcm": true, "aes-192-gcm": true, "aes-256-gcm": true,
	"chacha20-ietf-poly1305": true, "xchacha20-ietf-poly1305": true,
	"aes-128-cfb": true, "aes-192-cfb": true, "aes-256-cfb": true,
	"aes-128-ctr": true, "aes-192-ctr": true, "aes-256-ctr": true,
	"rc4-md5": true, "chacha20-ietf": true, "none": true,
	"2022-blake3-aes-128-gcm": true, "2022-blake3-aes-256-gcm": true, "2022-blake3-chacha20-poly1305": true,
}

// ssMethodAliases：Clash / v2ray 方言 → sing-box 官方拼写。
//
// 这些名字在订阅里很常见（Clash 的 chacha20-poly1305 就是 IETF 变体），但
// sing-box 只认 *-ietf-*。不映射的代价不是"这个节点不可用"，而是**整份配置
// check 失败、所有节点一起不可用** —— 单节点错误被放大成全局故障。
var ssMethodAliases = map[string]string{
	"chacha20-poly1305":  "chacha20-ietf-poly1305",
	"chacha20":           "chacha20-ietf-poly1305",
	"xchacha20-poly1305": "xchacha20-ietf-poly1305",
	"xchacha20":          "xchacha20-ietf-poly1305",
	"aes-256-gcm-siv":    "aes-256-gcm",
}

// uuidRe：uuid 形状校验。免费订阅里偶见非法 uuid，sing-box 在 initialize 阶段
// FATAL 掉整份配置 —— 必须在源头剔除该节点。
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// utlsFingerprints：sing-box 认识的 uTLS 指纹（option/uTLSFingerprint 表）。
var utlsFingerprints = map[string]bool{
	"chrome": true, "firefox": true, "safari": true, "ios": true, "android": true,
	"edge": true, "360": true, "qq": true, "random": true, "randomized": true,
}

// NormalizeSSMethod 归一化 shadowsocks method：先查别名表，再判是否在 sing-box
// 的名单里。返回（归一结果, 是否认识）—— ok=false 等价 JS 的 null，调用方必须
// 剔除该节点（认不出的 cipher 会让整份配置 FATAL，fail-closed 是刻意的）。
// 不认识的 method 原样返回，调用方只看 ok。
func NormalizeSSMethod(m string) (string, bool) {
	raw := strings.ToLower(strings.TrimSpace(m))
	if raw == "" {
		return "", false
	}
	if aliased, hit := ssMethodAliases[raw]; hit {
		raw = aliased
	}
	if ssMethods[raw] {
		return raw, true
	}
	return m, false
}

// sanitizeCapableTypes：字段白名单适用的已识别协议（KNOWN0）。白名单之外
// 的类型（wireguard 等）字段布局未知，不做删改。
var sanitizeCapableTypes = map[string]bool{
	"vless": true, "vmess": true, "trojan": true, "hysteria2": true, "tuic": true,
	"anytls": true, "shadowsocks": true, "socks": true, "http": true, "ssh": true,
}

// tlsCapableTypes：这些协议才有 tls 字段 —— Clash 订阅会给 ss 标 tls:true，
// 而 sing-box 的 shadowsocks/socks/ssh 出站没有 tls 字段，带着会整份配置
// FATAL。
//
// http 在列(R24):sing-box v1.14 的 http 出站**有** tls
// (protocol/http/outbound.go:37 用 options.TLS 建 dialer),过去把它和 socks/ss
// 归成一类 ⇒ 一个 HTTPS 代理被静默降级成明文,凭据还是 clear text 发出去的。
var tlsCapableTypes = map[string]bool{
	"vless": true, "vmess": true, "trojan": true, "hysteria2": true, "tuic": true,
	"anytls": true, "http": true,
}

// knownTransportTypes：未知传输（xhttp 等 Xray 专属）会让 sing-box 在 decode
// 阶段 FATAL 掉整份配置 —— 只能丢弃该节点，不静默降级。
var knownTransportTypes = map[string]bool{
	"ws": true, "grpc": true, "http": true, "httpupgrade": true, "quic": true,
}

// SanitizeOutbound 修掉订阅方言里 sing-box 会直接拒绝的形状。ok=false 等价
// JS 版的 null，调用方丢弃该节点（A5 差分需要可比对的失败语义）。
//
//  1. tls 块无 enabled —— v1.14 的 vless/trojan 会构建 nil-config TLS dialer
//     并在首次连接时 panic；missing enabled 意味着 enabled。
//  2. reality 要求 uTLS —— 强制一个认识的指纹（缺省 chrome）。
//  3. 认不出的 uTLS 指纹（v2ray 的 unsafe）—— 丢 utls 块。
//  4. flow —— sing-box 只有 xtls-rprx-vision；none 与旧 -udp443 拼法归一/删除。
//  5. transport.type: tcp|raw —— v2ray 的「无传输层」写法，删字段即等价语义；
//     未知传输（xhttp…）丢弃节点而不是静默降级成 TCP。
//  6. xtls / detour —— legacy 字段与内部路由字段，进配置必炸。
//  7. shadowsocks method —— Clash 别名映射到 sing-box 拼写，
//     base64("method:password") 兜底解码，仍认不出则丢节点。
//     **fail-closed 是刻意的**：一个未知 cipher 会 FATAL 整份配置，留下一个
//     sing-box 建不出来的节点等于牺牲所有其它节点。
func SanitizeOutbound(o Outbound) (Outbound, bool) {
	type0 := o.Type
	if sanitizeCapableTypes[type0] {
		// uuid 形状校验只在三种协议上做（与 JS 的 typeof 判断同域）。
		// 空串按非法处理（fail-closed）：没有 uuid 的 vless/vmess/tuic 反正
		// 连不上，留着只会把错误推迟到 sing-box FATAL。
		if type0 == "vless" || type0 == "vmess" || type0 == "tuic" {
			if !uuidRe.MatchString(strings.TrimSpace(o.UUID)) {
				return Outbound{}, false
			}
		}
		if !tlsCapableTypes[type0] {
			o.TLS = nil
		}
		if type0 != "vless" {
			o.Flow = ""
		}
		if type0 != "vmess" {
			o.AlterID = 0
		}
		if type0 != "hysteria2" {
			o.ServerPorts = nil
			o.Obfs = nil
		}
	}

	if o.TLS != nil {
		// 同一类隔离(B15 的第二半):o 是值拷贝,但 TLS 是**指针**,就地补
		// enabled/utls 会写回调用方的原始块。浅拷一份就够 —— 本函数只改块自己的
		// 字段(Reality/UTLS 是整体替换而不是就地改),没有更深的层要隔离。
		detached := *o.TLS
		o.TLS = &detached
		// JS 的判据是 `tls.enabled === false` 才删块，**缺 enabled 视为开启**
		// 并补写 enabled:true（src/singbox.js:242-243）。指针三态才能区分
		// 「缺」与「false」—— bool 零值曾把无 enabled 的块整块丢掉，与上面
		// 文档注释第 1 条自相矛盾（差分 A5 钉住的实错）。
		if o.TLS.Enabled != nil && !*o.TLS.Enabled {
			o.TLS = nil
		} else {
			o.TLS.Enabled = BoolPtr(true)
			if o.TLS.Reality != nil && o.TLS.Reality.Enabled {
				// JS 的 tls.utls?.fingerprint ?? ''：utls 块缺席时按空串处理
				fp := ""
				if o.TLS.UTLS != nil {
					fp = strings.ToLower(o.TLS.UTLS.Fingerprint)
				}
				if !utlsFingerprints[fp] {
					fp = "chrome"
				}
				o.TLS.UTLS = &UTLS{Enabled: true, Fingerprint: fp}
			} else if o.TLS.UTLS != nil && o.TLS.UTLS.Fingerprint != "" && !utlsFingerprints[strings.ToLower(o.TLS.UTLS.Fingerprint)] {
				o.TLS.UTLS = nil
			}
		}
	}

	if o.Flow != "" {
		f := strings.ToLower(strings.TrimSpace(o.Flow))
		if f == "xtls-rprx-vision" || f == "xtls-rprx-vision-udp443" {
			o.Flow = "xtls-rprx-vision"
		} else {
			o.Flow = ""
		}
	}

	if o.Transport != nil {
		t := strings.ToLower(o.Transport.Type)
		if t == "" || t == "tcp" || t == "raw" {
			o.Transport = nil
		} else if !knownTransportTypes[t] {
			return Outbound{}, false
		} else {
			// 回写规范化后的 Type：旧实现只把 t 用于**判定**，`o.Transport.Type`
			// 仍留着原样的大小写（"WS" / "Http" / "GRPC" 全都原样进盘）。下游按
			// 精确匹配读它（identity 的指纹、按类型挑配置、面板渲染），于是
			// 「knownTransportTypes 里全小写」这个前提在数据层根本不成立 ——
			// 同一台服务器的指纹会随订阅源怎么写而变。
			o.Transport.Type = t
		}
	}

	if o.Extra != nil {
		// 先拷贝、再删键(B15)。Extra 是引用语义的 map:旧顺序把 delete 写在拷贝
		// 之前,于是「保护调用方」这条注释是假的 —— 调用方(sub.Fetch 返回的原始
		// 切片,生产三个调用点都是)的对象被就地改脏。JS 是在调用点先
		// structuredClone(index.js:438),Go 在这一个函数里补上同一层隔离。
		copied := make(Extra, len(o.Extra))
		for k, v := range o.Extra {
			copied[k] = v
		}
		delete(copied, "xtls")
		delete(copied, "detour")
		o.Extra = copied
	}

	// shadowsocks 的 method：正常归一 → 失败则试 base64("method:password")
	// 兜底 → 仍失败剔除节点。method 字段里塞 base64 的订阅历史上真出现过。
	if o.Type == "shadowsocks" {
		if normalized, ok := NormalizeSSMethod(o.Method); ok {
			o.Method = normalized
		} else {
			// JS 的 /%3D/gi → '='（大小写不敏感），RE2 无大小写内联标志不行
			decoded := b64decodeLenient(re3D.ReplaceAllString(o.Method, "="))
			if i := strings.Index(decoded, ":"); i > 0 {
				if fromB64, ok := NormalizeSSMethod(decoded[:i]); ok {
					o.Method = fromB64
					o.Password = decoded[i+1:]
				} else {
					return Outbound{}, false
				}
			} else {
				return Outbound{}, false
			}
		}
	}

	// 地址守卫（JS 版分散在 clashProxyToOutbound 的 !p.server 与
	// !(server_port > 0)，这里在 sanitize 收口一遍，兜住 sing-box JSON 订阅
	// 里没 server 的出站）。mport 节点 server_port 缺席但有端口段，两者互斥。
	if o.Server == "" {
		return Outbound{}, false
	}
	// 端口词汇(B16):链接路径早就用 isValidPort/isValidPortRange 执法,Clash 与
	// sing-box-JSON 两条路径过去只判「有没有」。于是 server_port=70000 一路通过
	// sanitize、到 sing-box 严格解码 uint16 才失败(option/outbound.go:185),而
	// app 的「剔除 N 个坏节点」只统计 sanitize 的拒收 —— 那种节点既不在池里、
	// 也不在任何计数里。server_ports=["1:"] 更糟:sing-quic 的 ParsePorts 把它
	// 当开区间展开成 65535 个端口,而条目数由订阅方控制(["1:","2:","3:"] ⇒ ~20 万)。
	if len(o.ServerPorts) > 0 {
		if !isValidPortRange(o.ServerPorts) {
			return Outbound{}, false
		}
		// server_port 与 server_ports 互斥(sing-box 的 hysteria2 二选一)。链接路径
		// parse.go 早就强制了,sanitize 过去从不 enforce:两个都发给 sing-box 时
		// 整条出站被 abort,只留一条 warn。以端口段为准。
		o.ServerPort = 0
	} else if o.ServerPort > 0 {
		if !isValidPort(o.ServerPort) {
			return Outbound{}, false
		}
	} else {
		return Outbound{}, false
	}
	// hysteria2 的 obfs:sing-box 以 "missing obfs password" 拒收带空密码的块
	// (protocol/hysteria2/outbound.go:64-77),同理在这里判掉 —— 上面已经把非
	// hysteria2 的 Obfs 清了,所以这条只会作用到真正带 obfs 的节点。
	if o.Obfs != nil {
		// 密码按**精确空串**判,与 sing-box 同形(整分支评审 NIT-8):它判的是
		// `options.Obfs.Password == ""`,纯空白在人家那边是合法密钥串;我们多一次
		// TrimSpace 就是替订阅方丢掉一个能用的节点,而且这种丢弃还会混进
		// 「剔除 N 个坏节点」的计数,运维看不出是自己把它判死的。
		// type 仍用 TrimSpace:那边是 enum 比较(option/hysteria2.go:85-91),带
		// 空白的 type 上游同样会拒 —— 严只能严在「上游也会拒」的地方。
		if o.Obfs.Password == "" || strings.TrimSpace(o.Obfs.Type) == "" {
			return Outbound{}, false
		}
	}
	return o, true
}

// re3D 对应 JS sanitizeOutbound 兜底解码里的 /%3D/gi —— method 字段里的
// base64 常把 '=' 转义成 %3D（或 %3d），不还原就解不出来。
var re3D = regexp.MustCompile(`(?i)%3D`)

// b64decodeLenient 是 sanitize 兜底解码用的 base64：Buffer.from 的宽容语义
// （非法字符跳过而不是报错）。注意这里**不做** URL-safe 字母表翻译 —— JS 版
// 的 fallback 只做 %3D→= 替换，两种行为保持一致。
func b64decodeLenient(input string) string {
	cleaned := make([]rune, 0, len(input))
	for _, r := range input {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=' {
			cleaned = append(cleaned, r)
		}
	}
	s := string(cleaned)
	if pad := len(s) % 4; pad != 0 {
		s += strings.Repeat("=", 4-pad)
	}
	dec, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return ""
	}
	return string(dec)
}
