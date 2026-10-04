// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// 节点分享链接 → sing-box 出站。字段映射逐条对照 src/parse-links.js 移植
// （后者又逐字段对照 freesub scripts/main_v2.py 的 parse_* 系列函数，含其
// 踩坑注释：hysteria2 的 mport 必须写成 "start:end" 区间、裸单端口会 FATAL；
// vmess 的 b64 是 URL-safe；ss 兼容 SIP002 与 legacy）。

// NAME_BLACKLIST：广告/信息节点的名字特征。名字命中的节点**不丢**，退回
// `协议://host:port` 回退 tag；回退 tag（host）命中的才整条丢弃。
var nameBlacklist = regexp.MustCompile(`(?i)(剩余流量|流量重置|expire|expired|官网|套餐|telegram\.me|t\.me/|获取订阅)`)

var (
	schemeRe  = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9+.\-]*):\/\/`)
	hostPortR = regexp.MustCompile(`^(\[[^\]]+\]|[^:/?#]+):(\d+)`)
	vlessRe   = regexp.MustCompile(`^vless://([^@#]+)@(\[[^\]]+\]|[^:@/]+):(\d+)(?:/?\?([^#]*))?(?:#(.*))?$`)
	trojanRe  = regexp.MustCompile(`^trojan://([^@#]+)@(\[[^\]]+\]|[^:@/]+):(\d+)(?:/?\?([^#]*))?(?:#(.*))?$`)
	hy2HostRe = regexp.MustCompile(`^(\[[^\]]+\]|[^:/?#]+):(\d+)(?:/?\?([^#]*))?$`)
	tuicRe    = regexp.MustCompile(`^tuic://([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):(\d+)(?:/?\?([^#]*))?$`)
	anytlsRe  = regexp.MustCompile(`^anytls://([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):(\d+)(?:/?\?([^#]*))?$`)
	sshRe     = regexp.MustCompile(`^ssh://([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):?(\d+)?`)
	socksRe   = regexp.MustCompile(`^(?:socks5h?|socks)://(?:([^@#/?]+)@)?(\[[^\]]+\]|[^:@/?]+):(\d+)`)
)

// PARSERS 的分发顺序逐字照抄 src/parse-links.js:467，**不要重排**：前缀匹配
// 先到先得，某些 scheme 互为前缀，顺序错一条就整类节点静默消失且无日志。
var parsers = []struct {
	prefix string
	fn     func(uri string) *Outbound
}{
	{"vless://", parseVless},
	{"vmess://", parseVmess},
	{"trojan://", parseTrojan},
	{"ss://", parseSs},
	{"hysteria2://", parseHysteria2},
	{"hy2://", parseHysteria2},
	{"tuic://", parseTuic},
	{"anytls://", parseAnytls},
	{"ssh://", parseSsh},
	{"socks5://", parseSocks},
	{"socks5h://", parseSocks},
	{"socks://", parseSocks},
}

// ParseNodeURI 解析一条节点链接 → 出站（tag 取 # 名，缺省/黑名单名退回
// `协议://host:port`）；不认识的 scheme、畸形、端口越界一律返回 error，不抛。
//
// scheme 大小写不敏感，**其余部分逐字保留**：`VLESS://…` 这类大写 scheme 曾
// 匹配上解析器却过不了自己的 /^vless:\/\//（前缀判的是 toLowerCase()，交给
// 解析器的却是原串），于是静默返回 null —— 那个 toLowerCase() 是死代码。修法
// 是只把 `scheme://` 这一段归一化：host、凭据、path、query 都是大小写敏感的，
// 整串小写会把密码改掉。
func ParseNodeURI(raw string) (out Outbound, err error) {
	// 入参是不可信数据（别人的订阅）。JS 版用 try/catch 兜住解析器的任何异常，
	// 这里用 recover 同样兜底 —— 一个畸形输入掀掉整轮订阅合并的代价不可接受。
	defer func() {
		if r := recover(); r != nil {
			out, err = Outbound{}, fmt.Errorf("parse: 解析节点链接时异常: %v", r)
		}
	}()

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Outbound{}, fmt.Errorf("parse: 空输入")
	}
	normalized := trimmed
	if m := schemeRe.FindStringSubmatch(trimmed); m != nil {
		normalized = strings.ToLower(m[1]) + trimmed[len(m[1]):]
	}
	var ob *Outbound
	for _, p := range parsers {
		if strings.HasPrefix(normalized, p.prefix) {
			ob = p.fn(normalized)
			break
		}
	}
	if ob == nil || ob.Server == "" {
		return Outbound{}, fmt.Errorf("parse: 无法识别的节点链接 %.60q", trimmed)
	}
	// 端口必须是 1..65535 的整数（`:99999`、`:65536` 曾原样进配置）；
	// 端口段每一端同理。两套校验互斥：有 server_port 就不再看 server_ports。
	if ob.ServerPort != 0 {
		if !isValidPort(ob.ServerPort) {
			return Outbound{}, fmt.Errorf("parse: 端口越界 %d", ob.ServerPort)
		}
	} else if !isValidPortRange(ob.ServerPorts) {
		return Outbound{}, fmt.Errorf("parse: 端口段非法 %v", ob.ServerPorts)
	}
	name := strings.TrimSpace(nodeName(trimmed))
	tag := name
	if name == "" || nameBlacklist.MatchString(name) {
		tag = ob.Type + ":" + ob.Server + ":" + portSpecOf(ob)
	}
	if nameBlacklist.MatchString(tag) {
		return Outbound{}, fmt.Errorf("parse: 回退 tag 命中黑名单 %q", tag)
	}
	ob.Tag = tag
	return *ob, nil
}

// ParseLinks 解析一段订阅文本里的全部节点链接（整段 base64 或明文行列表）。
//
// 逐条行为与 JS parseLinks（src/parse-links.js:525）**一一同构**：坏行跳过不
// 报错；名字命中黑名单的退回回退 tag（ParseNodeURI 内完成，回退 tag 再命中
// 黑名单才整条丢弃）；「指向本机的假节点」**在这里保留** —— 那道过滤住在
// 订阅层的 dropUnroutable（src/sub.js:193），解析器只回答「链接能不能解析」；
// 指纹去重也住在这里之外（sub.Fetch 的合并点）。差分验收（任务 27 A1）喂的
// 就是这个函数，多挂任何一层职责都会让同一份输入在两版产出不同的节点数。
func ParseLinks(text string) []Outbound {
	body := strings.TrimSpace(text)
	if !strings.Contains(body, "://") {
		// 整段 base64（v2rayN 形态）；解出来仍没有 :// 就按明文处理
		if decoded := b64decode(body); strings.Contains(decoded, "://") {
			body = decoded
		}
	}
	out := []Outbound{}
	for _, line := range strings.Split(body, "\n") {
		ob, err := ParseNodeURI(strings.TrimSuffix(line, "\r"))
		if err != nil {
			continue // 坏行跳过不报错，与 JS parseLinks 一致
		}
		out = append(out, ob)
	}
	return out
}

// ---- 各协议解析器（失败返回 nil，与 JS 的 null 等价）------------------------

func parseVless(uri string) *Outbound {
	m := vlessRe.FindStringSubmatch(uri)
	if m == nil {
		return nil
	}
	params := queryDict(m[4])
	ob := &Outbound{Type: "vless", Server: unbracket(m[2]), ServerPort: atoi(m[3]), UUID: m[1]}
	// flow 只保留 vision/xtls 系（freesub 同规则）
	if flow := params["flow"]; flow != "" && (strings.Contains(flow, "vision") || strings.Contains(flow, "xtls")) {
		ob.Flow = flow
	}
	ob.TLS = tlsParams(params, ob.Server)
	ob.Transport = transportParams(params)
	stashExtraQuery(ob, params)
	return ob
}

// tlsParams 是 vless 链接的 TLS 块构造，布尔链逐字照抄 src/parse-links.js:190
// （2026-09-30 重写版）：只有显式说「不加密」的写法才关 TLS；pbk 的**存在**
// （哪怕是空串）就走 reality 分支，而空 pbk 又被拒 —— 所以 `?pbk=` 与
// `?security=reality&pbk=` 必须一致地返回 nil。
func tlsParams(params map[string]string, host string) *TLS {
	security := strings.ToLower(params["security"])
	sni := firstNonEmpty(params["sni"], params["peer"], host)
	pbk, pbkPresent := params["pbk"]
	if security == "reality" || pbkPresent {
		if pbk == "" {
			return nil // reality 缺 pbk 无法测（freesub 同规则）：不给 tls 块，节点仍在
		}
		return &TLS{
			Enabled:    BoolPtr(true),
			ServerName: sni,
			UTLS:       &UTLS{Enabled: true, Fingerprint: firstNonEmpty(params["fp"], "chrome")},
			Reality:    &Reality{Enabled: true, PublicKey: pbk, ShortID: params["sid"]},
		}
	}
	// 优先级：显式 tls=0/false > 显式 security > 显式 tls=1/true > 缺省。
	// security 缺省只是规范的默认值，不该盖掉链接作者显式写下的 tls=1。
	tlsFlag := strings.ToLower(params["tls"])
	plain := tlsFlag == "0" || tlsFlag == "false"
	if !plain {
		if security != "" {
			plain = security == "none"
		} else {
			plain = tlsFlag != "1" && tlsFlag != "true"
		}
	}
	if plain {
		return nil
	}
	t := &TLS{
		Enabled:    BoolPtr(true),
		ServerName: sni,
		Insecure:   BoolPtr(params["insecure"] == "1" || params["allowInsecure"] == "1"),
	}
	if fp := params["fp"]; fp != "" {
		t.UTLS = &UTLS{Enabled: true, Fingerprint: fp}
	}
	if alpn := params["alpn"]; alpn != "" {
		t.ALPN = splitFilterEmpty(alpn, ",")
	}
	return t
}

// transportParams 把 type/path/host/serviceName 映射成传输层；未知 type
// （xhttp、tcp…）返回 nil —— 与 JS 一致，不带 transport 字段。
func transportParams(params map[string]string) *Transport {
	switch strings.ToLower(params["type"]) {
	case "ws":
		t := &Transport{Type: "ws"}
		if p := params["path"]; p != "" {
			t.Path = p
		}
		if h := params["host"]; h != "" {
			t.Headers = map[string]any{"Host": h}
		}
		return t
	case "grpc", "gun":
		t := &Transport{Type: "grpc"}
		// serviceName 是 grpc 的参数名，path 是部分订阅商的错拼别名
		if sn := firstNonEmpty(params["serviceName"], params["path"]); sn != "" {
			t.Service = sn
		}
		return t
	case "h2", "http":
		t := &Transport{Type: "http"}
		if p := params["path"]; p != "" {
			t.Path = p
		}
		if h := params["host"]; h != "" {
			t.Host = []string{h} // h2 的 host 是数组，ws 的在 headers —— 两种形状不能混
		}
		return t
	case "httpupgrade":
		t := &Transport{Type: "httpupgrade"}
		if p := params["path"]; p != "" {
			t.Path = p
		}
		if h := params["host"]; h != "" {
			t.Host = h
		}
		return t
	}
	return nil
}

func parseVmess(uri string) *Outbound {
	// fragment 不是载荷的一部分:vmess 的载荷是 base64,而 v2rayN/v2rayNG 导出的
	// 链接几乎恒带 `#名字`(B14)。b64decode 只丢掉 `#`、**留下**其后的字母数字,
	// 于是载荷变成「base64 + 垃圾」:带 padding 时 StdEncoding 在 '=' 位报
	// CorruptInputError,不带时多余字符污染流 —— 两条路都让 JSON 解析失败,
	// 整条 vmess 节点被静默丢弃,也就是订阅里**全部** vmess。Node 的 Buffer.from
	// 对尾部垃圾宽容,所以 JS 不坏:这是 Go 独有回归。其他协议解析器开头都做一次
	// SplitN("#"),只有 vmess 漏了(名字本身由 nodeName 从 fragment 取,不丢)。
	payload := strings.SplitN(uri[len("vmess://"):], "#", 2)[0]
	var data map[string]any
	// b64 是 URL-safe（含 - 与 _），JSON.parse 失败 → null（畸形载荷全丢）
	if err := json.Unmarshal([]byte(b64decode(payload)), &data); err != nil {
		return nil
	}
	server := strings.TrimSpace(str(data["add"]))
	port := numOrZero(data["port"])
	if server == "" || port <= 0 {
		return nil
	}
	ob := &Outbound{
		Type:       "vmess",
		Server:     server,
		ServerPort: port,
		UUID:       strings.TrimSpace(str(data["id"])),
	}
	// JS 解析器恒写 security:'auto'（src/parse-links.js:261，硬编码不从链接
	// 读）—— sing-box 的 vmess encryption 字段。Go 曾漏写，同一链接的 vmess
	// 出站在两版落盘/生成配置时形状不同（差分 A1 钉住的实错）。
	if ob.Extra == nil {
		ob.Extra = Extra{}
	}
	ob.Extra["security"] = "auto"
	if aid := numOrZero(data["aid"]); aid > 0 {
		ob.AlterID = aid // aid=0 不写字段，aid>0 才写
	}
	// JS 是严格比较 data.tls === 'tls' || === '1' || === true；
	// 数字 1 不算（typeof 不同），只有串 "1" 和布尔 true。
	switch t := data["tls"].(type) {
	case string:
		if t == "tls" || t == "1" {
			ob.TLS = &TLS{Enabled: BoolPtr(true), ServerName: strings.TrimSpace(firstNonEmpty(str(data["sni"]), str(data["host"]), server))}
		}
	case bool:
		if t {
			ob.TLS = &TLS{Enabled: BoolPtr(true), ServerName: strings.TrimSpace(firstNonEmpty(str(data["sni"]), str(data["host"]), server))}
		}
	}
	params := map[string]string{
		"type": strings.ToLower(str(data["net"])), // JS: String(data.net ?? 'tcp') —— 缺席即 'tcp'
	}
	if v, ok := data["path"]; ok && v != nil {
		params["path"] = str(v)
	}
	if v, ok := data["host"]; ok && v != nil {
		params["host"] = str(v)
	}
	ob.Transport = transportParams(params)
	stashExtraJSON(ob, data)
	return ob
}

func parseTrojan(uri string) *Outbound {
	m := trojanRe.FindStringSubmatch(uri)
	if m == nil {
		return nil
	}
	params := queryDict(m[4])
	server := unbracket(m[2])
	ob := &Outbound{
		Type:       "trojan",
		Server:     server,
		ServerPort: atoi(m[3]),
		Password:   decodeSafe(m[1]),
		TLS: &TLS{
			Enabled:    BoolPtr(true),
			ServerName: firstNonEmpty(params["sni"], server),
			Insecure:   BoolPtr(params["insecure"] == "1" || params["allowInsecure"] == "1"),
		},
	}
	if alpn := params["alpn"]; alpn != "" {
		ob.TLS.ALPN = splitFilterEmpty(alpn, ",")
	}
	stashExtraQuery(ob, params)
	return ob
}

func parseSs(uri string) *Outbound {
	body := strings.SplitN(uri[len("ss://"):], "#", 2)[0]
	// 空密码的取舍两条分支必须一致：**method 不能为空**（没有加密方式的
	// shadowsocks 出站 sing-box 直接 FATAL），**password 可以为空**（sing-box
	// 接受，且为一个空密码丢掉一个真实节点是更大的代价）。
	if strings.Contains(body, "@") {
		// SIP002: ss://base64(method:password)@host:port 或 ss://method:password@host:port
		at := strings.LastIndex(body, "@")
		userinfo := body[:at]
		// ?plugin=… 与 /path 都要被剥掉，否则 hostPort 解析不出来
		rest := strings.Split(body[at+1:], "/")[0]
		rest = strings.Split(rest, "?")[0]
		hp := hostPort(rest)
		if hp == nil {
			return nil
		}
		var method, password string
		if strings.Contains(userinfo, ":") {
			// 明文 userinfo 是百分号编码的:解一次。
			parts := strings.SplitN(userinfo, ":", 2)
			method, password = decodeSafe(parts[0]), decodeSafe(parts[1])
		} else {
			// base64 的 userinfo 解出来就是**原子值** —— 再解一次会把密码里恰好
			// 长成 `%XX` 的部分改掉(存进错误密码、连接失败、无诊断),而同一个逻辑
			// 输入走明文分支时只解一次(R26:两条分支必须一致)。
			dec := b64decode(userinfo)
			if !strings.Contains(dec, ":") {
				return nil
			}
			parts := strings.SplitN(dec, ":", 2)
			method, password = parts[0], parts[1]
		}
		if method == "" {
			return nil
		}
		return &Outbound{
			Type: "shadowsocks", Server: hp.server, ServerPort: hp.port,
			Method:   strings.ToLower(strings.TrimSpace(method)),
			Password: password,
		}
	}
	// legacy: ss://base64(method:password@host:port)
	dec := b64decode(body)
	at := strings.LastIndex(dec, "@")
	if at == -1 {
		return nil
	}
	userinfo, hostinfo := dec[:at], dec[at+1:]
	hp := hostPort(strings.TrimSpace(hostinfo))
	if hp == nil {
		return nil
	}
	i := strings.Index(userinfo, ":")
	if i == -1 {
		return nil
	}
	// 这一支恒是 base64 解出来的,同 R26:不做第二次百分号解码。
	method := userinfo[:i]
	if method == "" {
		return nil
	}
	return &Outbound{
		Type: "shadowsocks", Server: hp.server, ServerPort: hp.port,
		Method:   strings.ToLower(strings.TrimSpace(method)),
		Password: userinfo[i+1:],
	}
}

func parseHysteria2(uri string) *Outbound {
	prefix := "hysteria2://"
	if !strings.HasPrefix(uri, prefix) {
		prefix = "hy2://"
	}
	body := strings.SplitN(uri[len(prefix):], "#", 2)[0]
	at := strings.LastIndex(body, "@")
	if at <= 0 {
		return nil // 认证段为空（@ 打头）或根本没有 @ 都不成节点
	}
	auth := body[:at]
	m := hy2HostRe.FindStringSubmatch(body[at+1:])
	if m == nil {
		return nil
	}
	params := queryDict(m[3])
	server := unbracket(m[1])
	ob := &Outbound{
		Type: "hysteria2", Server: server, ServerPort: atoi(m[2]),
		Password: decodeSafe(auth),
		TLS: &TLS{
			Enabled:    BoolPtr(true),
			ServerName: firstNonEmpty(params["sni"], params["peer"], server),
			Insecure:   BoolPtr(params["insecure"] == "1" || params["allowInsecure"] == "1"),
		},
	}
	if alpn := params["alpn"]; alpn != "" {
		ob.TLS.ALPN = splitFilterEmpty(alpn, ",")
	}
	if obfs := params["obfs"]; obfs != "" && obfs != "none" {
		ob.Obfs = &Obfs{Type: obfs, Password: params["obfs-password"]}
	}
	if mport := firstNonEmpty(params["mport"], params["ports"]); mport != "" {
		// freesub 实测: server_ports 只接受 "start:end" 区间, 裸单端口会 FATAL，
		// 所以单端口也展开成 "p:p"；越界的区间不能只丢一端（`70000-80000`
		// 曾原样进配置），垃圾项逐条丢、合法项保留，全垃圾则回落到 server_port。
		ranges := []string{}
		for _, part := range strings.Split(mport, ",") {
			piece := strings.TrimSpace(part)
			if piece == "" {
				continue
			}
			if strings.Contains(piece, "-") {
				ab := strings.SplitN(piece, "-", 2)
				a, b := strings.TrimSpace(ab[0]), strings.TrimSpace(ab[1])
				if isAllDigits(a) && isAllDigits(b) && isValidPort(atoi(a)) && isValidPort(atoi(b)) {
					ranges = append(ranges, a+":"+b)
				}
			} else if isAllDigits(piece) && isValidPort(atoi(piece)) {
				ranges = append(ranges, piece+":"+piece)
			}
		}
		if len(ranges) > 0 {
			ob.ServerPorts = ranges
			ob.ServerPort = 0 // 与 JS 的 delete ob.server_port 等价：两者不能同时出现
		}
	}
	stashExtraQuery(ob, params)
	return ob
}

func parseTuic(uri string) *Outbound {
	m := tuicRe.FindStringSubmatch(strings.SplitN(uri, "#", 2)[0])
	if m == nil {
		return nil
	}
	if !strings.Contains(m[1], ":") {
		return nil // userinfo 没有冒号分不出 uuid/password
	}
	parts := strings.SplitN(m[1], ":", 2)
	params := queryDict(m[4])
	server := unbracket(m[2])
	ob := &Outbound{
		Type: "tuic", Server: server, ServerPort: atoi(m[3]),
		UUID:     decodeSafe(parts[0]),
		Password: decodeSafe(parts[1]),
		TLS: &TLS{
			Enabled:    BoolPtr(true),
			ServerName: firstNonEmpty(params["sni"], server),
			Insecure:   BoolPtr(params["allow_insecure"] == "1" || params["insecure"] == "1"),
			ALPN:       splitFilterEmpty(firstNonEmpty(params["alpn"], "h3"), ","),
		},
	}
	// JS 的 ?? 语义：键存在（哪怕空串）就用键值，缺席才落默认。
	if ob.Extra == nil {
		ob.Extra = Extra{}
	}
	if v, ok := params["congestion_control"]; ok {
		ob.Extra["congestion_control"] = v
	} else {
		ob.Extra["congestion_control"] = "bbr"
	}
	if v, ok := params["udp_relay_mode"]; ok {
		ob.Extra["udp_relay_mode"] = v
	} else {
		ob.Extra["udp_relay_mode"] = "native"
	}
	stashExtraQuery(ob, params)
	return ob
}

func parseAnytls(uri string) *Outbound {
	m := anytlsRe.FindStringSubmatch(strings.SplitN(uri, "#", 2)[0])
	if m == nil {
		return nil
	}
	params := queryDict(m[4])
	server := unbracket(m[2])
	ob := &Outbound{
		Type: "anytls", Server: server, ServerPort: atoi(m[3]),
		Password: decodeSafe(m[1]),
		TLS: &TLS{
			Enabled:    BoolPtr(true),
			ServerName: firstNonEmpty(params["sni"], server),
			// JS parseAnytls 读的是 allowInsecure（驼峰，src/parse-links.js:438），
			// 不是 tuic 那条车道的 allow_insecure —— 拼错一个字母,同一个链接在
			// 两版下一个跳过证书校验一个不跳（任务 27 A1 差分钉住的实错）。
			Insecure: BoolPtr(params["insecure"] == "1" || params["allowInsecure"] == "1"),
		},
	}
	if alpn := params["alpn"]; alpn != "" {
		ob.TLS.ALPN = splitFilterEmpty(alpn, ",")
	}
	stashExtraQuery(ob, params)
	return ob
}

func parseSsh(uri string) *Outbound {
	m := sshRe.FindStringSubmatch(strings.SplitN(uri, "#", 2)[0])
	if m == nil {
		return nil
	}
	port := 22
	if m[3] != "" {
		port = atoi(m[3])
	}
	ob := &Outbound{
		Type: "ssh", Server: unbracket(m[2]), ServerPort: port,
		Extra: Extra{"user": decodeSafe(strings.SplitN(m[1], ":", 2)[0])},
	}
	if strings.Contains(m[1], ":") {
		ob.Password = decodeSafe(strings.SplitN(m[1], ":", 2)[1])
	}
	return ob
}

func parseSocks(uri string) *Outbound {
	m := socksRe.FindStringSubmatch(strings.SplitN(uri, "#", 2)[0])
	if m == nil {
		return nil
	}
	ob := &Outbound{
		Type: "socks", Server: unbracket(m[2]), ServerPort: atoi(m[3]),
		Extra: Extra{"version": "5"},
	}
	if m[1] != "" {
		up := strings.SplitN(m[1], ":", 2)
		ob.Extra["username"] = decodeSafe(up[0])
		if len(up) == 2 {
			ob.Password = decodeSafe(up[1])
		}
	}
	return ob
}

// ---- 通用小工具 ---------------------------------------------------------------

// passthroughKeys：sing-box 认识、但本项目未建模成具名字段的字段（总纲约束 8
// 点名的四个）。必须收窄成白名单：v2ray 方言里到处是 sing-box 不认识的参数
// （encryption=none 最常见），照单全收会在任务 7 生成配置时被 sing-box 严格
// 解码 FATAL —— JS 版把它们原样丢弃，这里只放行这四个。
var passthroughKeys = []string{"multiplex", "domain_strategy", "udp_over_tcp", "network_strategy"}

func stashExtraQuery(ob *Outbound, params map[string]string) {
	for _, k := range passthroughKeys {
		if v, ok := params[k]; ok {
			if ob.Extra == nil {
				ob.Extra = Extra{}
			}
			ob.Extra[k] = v
		}
	}
}

func stashExtraJSON(ob *Outbound, data map[string]any) {
	for _, k := range passthroughKeys {
		if v, ok := data[k]; ok && v != nil {
			if ob.Extra == nil {
				ob.Extra = Extra{}
			}
			ob.Extra[k] = v // JSON 值可能是对象，原样保留不转字符串
		}
	}
}

// queryDict 与 JS 版逐字同构：裸 flag（无 =）记成自身真值；值做百分号解码，
// 解不动原样保留；键不解码；同键后者覆盖。
func queryDict(query string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(query, "&") {
		if pair == "" {
			continue
		}
		i := strings.Index(pair, "=")
		key, raw := pair, ""
		if i == -1 {
			raw = pair // JS: slice(i+1) 在 i=-1 时取整个 pair
		} else {
			key, raw = pair[:i], pair[i+1:]
		}
		out[key] = decodeSafe(raw)
	}
	return out
}

// decodeSafe 用 PathUnescape 而不是 QueryUnescape：后者会把 '+' 变成空格，
// 而节点名/密码里的 '+' 是字面字符（JS decodeURIComponent 不动 '+'）。
func decodeSafe(v string) string {
	dec, err := url.PathUnescape(v)
	if err != nil {
		return v // 非法转义（如 %E0%A4%A）原样保留，不抛
	}
	return dec
}

// nodeName 取 # 名并做百分号解码。
func nodeName(uri string) string {
	i := strings.Index(uri, "#")
	if i == -1 {
		return ""
	}
	return decodeSafe(uri[i+1:])
}

// b64decode 兼容 URL-safe 字母表并剥掉所有空白（订阅商爱在 base64 里插换行）。
// 解不出返回空串，不抛 —— Buffer.from 的宽容语义在这里只影响「decoded 里是否
// 含 ://」这一种判定，空串同样安全。
//
// O16c:过去是两趟(strings.Map 翻译 + allow-list 过滤),合成一趟:逐 rune 判一次,
// 该翻的翻、该丢的丢,行为逐项等价。
func b64decode(input string) string {
	cleaned := make([]rune, 0, len(input))
	for _, r := range input {
		switch {
		case r == '-':
			cleaned = append(cleaned, '+')
		case r == '_':
			cleaned = append(cleaned, '/')
		case unicode.IsSpace(r):
			// 丢弃:订阅文本里混进换行/空格是常态。
		case (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') ||
			r == '+' || r == '/' || r == '=':
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

func hostPort(hostinfo string) *struct {
	server string
	port   int
} {
	m := hostPortR.FindStringSubmatch(hostinfo)
	if m == nil {
		return nil
	}
	return &struct {
		server string
		port   int
	}{server: unbracket(m[1]), port: atoi(m[2])}
}

// portSpecOf 是 tag 回退里的端口描述：有 server_port 用它；mport 节点的
// server_port 已被删，用区间本身拼（`hysteria2:h.example:2000:3000`）——直接拼
// int 零值会得到 `…:0`，同一主机上端口区间不同的多个节点 tag 完全相同，
// registry 去重时互相覆盖，静默丢节点。
func portSpecOf(ob *Outbound) string {
	if ob.ServerPort != 0 {
		return strconv.Itoa(ob.ServerPort)
	}
	if len(ob.ServerPorts) > 0 {
		return strings.Join(ob.ServerPorts, ",")
	}
	return ""
}

/** 1..65535 的整数端口；其余（0、负数、越界）一律不合法。 */
func isValidPort(v int) bool {
	return v >= 1 && v <= 65535
}

// isValidPortRange：端口段里每一端都必须是合法端口；空数组视为不合法。
// "1:2:3" 的第三段照 JS 解构语义忽略（只看前两端）。
func isValidPortRange(ranges []string) bool {
	if len(ranges) == 0 {
		return false
	}
	for _, r := range ranges {
		parts := strings.Split(r, ":")
		if len(parts) < 2 {
			return false
		}
		for _, p := range parts[:2] {
			if !isAllDigits(p) || !isValidPort(atoi(p)) {
				return false
			}
		}
	}
	return true
}

// ---- 国家识别（src/sub.js 的四级识别，逐条移植）--------------------------------

// name2cc：freesub 括号英文名 → ISO-3166 alpha-2（权威级，订阅方自己标的）。
var name2cc = map[string]string{
	"Taiwan": "TW", "Hong Kong": "HK", "Japan": "JP", "Singapore": "SG",
	"United States": "US", "Netherlands": "NL", "Germany": "DE", "United Kingdom": "GB",
	"France": "FR", "Canada": "CA", "South Korea": "KR", "Korea": "KR",
	"Turkey": "TR", "Thailand": "TH", "Australia": "AU", "Russia": "RU",
	"Malaysia": "MY", "India": "IN", "Vietnam": "VN", "Philippines": "PH",
	"Brazil": "BR", "Argentina": "AR", "Chile": "CL", "Mexico": "MX",
	"Sweden": "SE", "Switzerland": "CH", "Austria": "AT", "Poland": "PL",
	"Spain": "ES", "Italy": "IT", "Ireland": "IE", "Finland": "FI", "Norway": "NO",
	"Ukraine": "UA", "Romania": "RO", "Bulgaria": "BG", "Greece": "GR", "Hungary": "HU",
	"Czechia": "CZ", "Denmark": "DK", "Belgium": "BE", "Portugal": "PT",
	"Israel": "IL", "UAE": "AE", "Bangladesh": "BD", "Pakistan": "PK",
	"Indonesia": "ID", "Cambodia": "KH", "Laos": "LA", "Myanmar": "MM",
	"New Zealand": "NZ", "South Africa": "ZA", "Kazakhstan": "KZ",
}

// ccKnown：ISO 码 token 的合法域（CC_KNOWN，src/sub.js:94）。
var ccKnown = map[string]bool{
	"US": true, "JP": true, "HK": true, "TW": true, "KR": true, "SG": true,
	"NL": true, "DE": true, "GB": true, "FR": true, "CA": true, "TR": true,
	"TH": true, "AU": true, "RU": true, "MY": true, "IN": true, "VN": true,
	"PH": true, "BR": true, "AR": true, "CL": true, "MX": true, "SE": true,
	"CH": true, "AT": true, "PL": true, "ES": true, "IT": true, "IE": true,
	"FI": true, "NO": true, "UA": true, "RO": true, "BG": true, "GR": true,
	"HU": true, "CZ": true, "DK": true, "BE": true, "PT": true, "IL": true,
	"AE": true, "BD": true, "PK": true, "ID": true, "KH": true, "LA": true,
	"MM": true, "NZ": true, "ZA": true, "KZ": true, "CN": true,
}

// euCCs：欧洲桶包含的国家码（src/sub.js:37）。
var euCCs = map[string]bool{
	"NL": true, "DE": true, "GB": true, "FR": true, "SE": true, "CH": true,
	"AT": true, "PL": true, "ES": true, "IT": true, "IE": true, "FI": true,
	"NO": true, "UA": true, "RO": true, "BG": true, "GR": true, "HU": true,
	"CZ": true, "DK": true, "BE": true, "PT": true,
}

// groups：出口地区固定分组。与 internal/check 的 RegionGroups 是同一份词表
// 的两份拷贝 —— parse 与 check 同为 L0，LAYERS.md 规定同层不互 import，
// 宁可复制 8 个字符串也不为此打破分层。
var groups = []string{"US", "JP", "HK", "TW", "KR", "SG", "EU", "OTHER"}

// proxyTypes：可入池的协议类型（PROXY_TYPES）。selector/urltest/direct/block
// 不进池 —— 这是类型职责，IsUnroutableServer 对没有 server 的出站没有意见。
var proxyTypes = map[string]bool{
	"vless": true, "vmess": true, "trojan": true, "shadowsocks": true, "ss": true,
	"hysteria2": true, "hy2": true, "tuic": true, "anytls": true, "ssh": true,
	"socks": true, "http": true,
}

// unknownKeepLimit：无名节点纳入探测的数量上限（避免超大订阅把入站撑爆）。
const unknownKeepLimit = 80

type kwRule struct {
	re      *regexp.Regexp
	cc      string
	exclude string // 命中该子串时整条规则让位（模拟 JS 的负向断言）
}

// keywords：中英文地名关键词（兜底层级，在 ISO 码 token 之后）。
// 「印度(?!尼)」RE2 不支持负向断言，用 exclude="印度尼" 等价实现。
var keywords = []kwRule{
	{mustRe(`香港|Hong ?Kong`), "HK", ""},
	{mustRe(`台湾|臺灣|Taiwan`), "TW", ""},
	{mustRe(`日本|Japan`), "JP", ""},
	{mustRe(`新加坡|獅城|狮城|Singapore`), "SG", ""},
	{mustRe(`韩国|韓國|Korea`), "KR", ""},
	{mustRe(`美国|美國|United States|Los ?Angeles|San ?Jose|Dallas|Seattle`), "US", ""},
	{mustRe(`英国|英國|United ?Kingdom|London`), "GB", ""},
	{mustRe(`德国|德國|Germany|Frankfurt`), "DE", ""},
	{mustRe(`法国|法國|France|Paris`), "FR", ""},
	{mustRe(`加拿大|Canada|Toronto`), "CA", ""},
	{mustRe(`土耳其|Turkey`), "TR", ""},
	{mustRe(`泰国|Thailand`), "TH", ""},
	{mustRe(`澳大利亚|澳洲|Australia`), "AU", ""},
	{mustRe(`俄罗斯|Russia`), "RU", ""},
	{mustRe(`马来西亚|Malaysia`), "MY", ""},
	{mustRe(`印度|India`), "IN", "印度尼"},
	{mustRe(`越南|Vietnam`), "VN", ""},
	{mustRe(`菲律宾|Philippines`), "PH", ""},
	{mustRe(`巴西|Brazil`), "BR", ""},
	{mustRe(`荷兰|Netherlands|Amsterdam`), "NL", ""},
	{mustRe(`印尼|Indonesia`), "ID", ""},
	{mustRe(`瑞典|Sweden`), "SE", ""},
	{mustRe(`瑞士|Switzerland`), "CH", ""},
	{mustRe(`波兰|Poland`), "PL", ""},
	{mustRe(`西班牙|Spain`), "ES", ""},
	{mustRe(`意大利|Italy`), "IT", ""},
	{mustRe(`爱尔兰|Ireland`), "IE", ""},
}

func mustRe(s string) *regexp.Regexp { return regexp.MustCompile(`(?i)` + s) }

// ISO 两字母段的识别说明：JS 用 /(?:^|[^A-Z])([A-Z]{2})(?=[^A-Z]|$)/g。RE2 不支持
// 前瞻，所以真正的匹配走 isoTokens 的手写扫描（语义逐条对齐：两字母组前后都不能
// 紧邻别的大写字母）。O16a：这里曾另有一个 `isoTokenShape` 正则变量只为"记录形状"，
// 全仓没有一次匹配用它 —— 注释留下，死变量删掉。

var flagRe = regexp.MustCompile(`[\x{1F1E6}-\x{1F1FF}]{2}`)
var parenRe = regexp.MustCompile(`\(([^()]+)\)`)

func flagToCC(flag string) string {
	rs := []rune(flag)
	if len(rs) != 2 {
		return ""
	}
	return string(rune(rs[0]-0x1F1E6+'A')) + string(rune(rs[1]-0x1F1E6+'A'))
}

func isUpper(r rune) bool { return r >= 'A' && r <= 'Z' }

func isoTokens(tag string) []string {
	rs := []rune(tag)
	out := []string{}
	for i := 0; i+1 < len(rs); {
		if isUpper(rs[i]) && isUpper(rs[i+1]) {
			before := i == 0 || !isUpper(rs[i-1])
			after := i+2 >= len(rs) || !isUpper(rs[i+2])
			if before && after {
				out = append(out, string(rs[i:i+2]))
				i += 2 // 与 /g 的 lastIndex 一致：匹配只消费这两个字符
				continue
			}
		}
		i++
	}
	return out
}

// isoCountry：按 " - " 分段取最后一段、段内取第一个码 ——
// "🇨🇳 Example-Node - TW-Wuri-…" 的标签名里带国家码、真实国家 TW 在后；
// "US-LA-02"（洛杉矶）的 LA 是老挝，国家码打头。两种实测形态都覆盖。
func isoCountry(text string) string {
	segment := text
	if i := strings.LastIndex(text, " - "); i >= 0 {
		segment = text[i+3:]
	}
	for _, tok := range isoTokens(segment) {
		if ccKnown[tok] {
			return tok
		}
	}
	return ""
}

// CountryOf 是 tag 的四级国家识别：
//  1. freesub 括号英文名（权威，订阅方自己标的）
//  2. ISO 码 token —— 取最后一个 " - " 段里的第一个独立两字母码
//  3. 中英文地名关键词
//  4. 旗帜 emoji（最不可靠，兜底）
//
// 没有名字的节点不会被丢弃 —— 返回空串后由 FilterByGroups 纳入"其他"桶参与
// 探测，再由探测的出口 IP 实测归桶。
func CountryOf(tag string) string {
	if m := parenRe.FindStringSubmatch(tag); m != nil {
		if cc, ok := name2cc[strings.TrimSpace(m[1])]; ok {
			return cc
		}
	}
	if iso := isoCountry(tag); iso != "" {
		return iso
	}
	for _, kw := range keywords {
		// exclude 为空表示无排除条件；strings.Contains(x, "") 恒为 true，
		// 直接写会把所有关键词规则全部短路成不命中。
		if kw.re.MatchString(tag) && (kw.exclude == "" || !strings.Contains(tag, kw.exclude)) {
			return kw.cc
		}
	}
	if flag := flagRe.FindString(tag); flag != "" {
		if cc := flagToCC(flag); ccKnown[cc] {
			return cc
		}
	}
	return ""
}

// BucketOf 国家码 → 出口地区分组（美日港台韩新/欧洲/其他）；空码归"其他"。
// bucketCache 预展开「2 字节 ASCII 国家码 → 分组」的查询表:BucketOf 在 Pick
// 的 rank 热路径上每节点调一次,[]rune + ToUpper + 线性扫 groups 的分配占了
// BenchmarkPick 3412 allocs/op 的大头(P3)。
var bucketCache = func() map[string]string {
	m := make(map[string]string, len(groups)+len(euCCs))
	for _, g := range groups {
		m[g] = g
	}
	for cc := range euCCs {
		m[cc] = "EU"
	}
	return m
}()

func BucketOf(cc string) string {
	// 快径:2 字节纯 ASCII 码(真实池里的绝对主流)。大小写不敏感与原实现
	// 一致;查不中(既非分组也非已知 EU 成员)按 OTHER,同样与原实现一致。
	if len(cc) == 2 && isASCIIAlpha(cc) {
		code := string([]byte{upperASCII(cc[0]), upperASCII(cc[1])})
		if b, ok := bucketCache[code]; ok {
			return b
		}
		return "OTHER"
	}
	r := []rune(strings.ToUpper(cc))
	if len(r) > 2 {
		r = r[:2]
	}
	code := string(r)
	for _, g := range groups {
		if g == code {
			return code
		}
	}
	if euCCs[code] {
		return "EU"
	}
	return "OTHER"
}

func isASCIIAlpha(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return false
		}
	}
	return true
}

func upperASCII(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - 32
	}
	return c
}

// FilterByGroups 按所选分组过滤出站。语义：
//   - 类型不在 PROXY_TYPES 的出站（selector/urltest/direct/block）不进池
//   - 同**配置指纹**去重(R21)
//   - tag 可识别国家的节点：所属分组被选中才保留
//   - 无名节点（tag 识别不出国家）：只要选了"其他"就保留（最多
//     unknownKeepLimit 个，探测后由出口 IP 实测归桶）
//
// 去重键过去是 Tag(JS sub.js:146-153 同款),实测 data/subs_cache.json 的 1246 个
// 出站里 7 个重复 tag 压掉了 84 个真不同的节点(disney_netflix_GB 76 个只留 1),
// 空 tag 更是把全部无命名出站塌缩成一个。管线其余去重点(registry.Merge、
// sub.Fetch 的轮内去重)都以 IdentityOf 为键,这里跟着改齐 —— 同一个物理节点换名
// 仍然折叠成一个,同名不同服务器不再互相顶掉。
func FilterByGroups(outs []Outbound, groups []string) []Outbound {
	want := map[string]bool{}
	for _, g := range groups {
		want[strings.ToUpper(g)] = true
	}
	seen := map[string]bool{}
	var matched, unknown []Outbound
	for _, o := range outs {
		if !proxyTypes[o.Type] {
			continue
		}
		key := IdentityOf(o)
		if seen[key] {
			continue
		}
		seen[key] = true
		cc := CountryOf(o.Tag)
		if cc == "" {
			unknown = append(unknown, o)
			continue
		}
		if want[BucketOf(cc)] {
			matched = append(matched, o)
		}
	}
	if want["OTHER"] {
		if len(unknown) > unknownKeepLimit {
			unknown = unknown[:unknownKeepLimit]
		}
		matched = append(matched, unknown...)
	}
	return matched
}

// ---- any → 标量的小转换（JSON/YAML 解出来的都是 any）---------------------------

func str(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return fmt.Sprint(v)
	}
}

// numOrZero 把 JSON/YAML 的数字变成 int。非整数（443.7）与 NaN 归 0 ——
// JS 的 Number.isInteger 校验会把它们判死，归 0 会走进同一条端口拒绝路径。
func numOrZero(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		if math.IsNaN(t) || t != math.Trunc(t) {
			return 0
		}
		return int(t)
	case bool:
		if t {
			return 1
		}
		return 0
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil || math.IsNaN(f) || f != math.Trunc(f) {
			return 0
		}
		return int(f)
	}
	return 0
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

// firstNonEmpty 对应 JS 的 `a || b`（空串让位），用于 sni||host||server。
//
// O16b:这里曾经另有一个 `firstNonEmptyStr` 纯别名(注释同款、实现是转调),
// 两个名字服务同一个语义 —— 调用点全部并到本函数,别名删掉。
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func unbracket(h string) string {
	// 与 JS host.replace(/^\[|\]$/g, '') 同构：只剥一对首尾方括号
	h = strings.TrimPrefix(h, "[")
	return strings.TrimSuffix(h, "]")
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func splitFilterEmpty(s, sep string) []string {
	out := []string{}
	for _, p := range strings.Split(s, sep) {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
