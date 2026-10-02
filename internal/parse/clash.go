// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Clash / sing-box JSON 两种订阅格式的入口，对照 src/sub.js:196
// clashProxyToOutbound 与 parseSubscriptionBodyRaw 的 JSON 分支。

// clashTypes：Clash type 名 → sing-box 协议名。白名单之外的类型
// （wireguard、ssr…）不支持，整条拒绝 —— 与 JS 的 CLASH_TYPE 查表同构。
var clashTypes = map[string]string{
	"ss": "shadowsocks", "vmess": "vmess", "vless": "vless", "trojan": "trojan",
	"hysteria2": "hysteria2", "hy2": "hysteria2", "tuic": "tuic", "anytls": "anytls",
	"socks5": "socks", "http": "http",
}

// ParseClashProxy 把一个 Clash proxy 节点映射成出站。tag 由调用方从 p["name"]
// 拆出来传（name 是必填项，缺了就拒绝，与 JS 的 !p.name 同条件）。
//
// 只取**存在**的键，ws-opts/grpc-opts/reality-opts 按需读 —— JS 版是新建对象
// 逐字段拷贝，Clash 里 sing-box 不认识的键（tfo、smux…）本来就会被丢掉，
// 这里保持一致（不进 Extra：收进来反而会在生成配置时炸掉严格解码）。
//
// loopback/内网地址的拒绝走 IsUnroutableServer 而不是硬编码 127.0.0.1 ——
// 与链接入口共用同一套地址判据，判据改一处两边同时生效。
func ParseClashProxy(tag string, p map[string]any) (Outbound, bool) {
	if p == nil || tag == "" {
		return Outbound{}, false
	}
	server := str(p["server"])
	outType, known := clashTypes[strings.ToLower(str(p["type"]))]
	if !known {
		return Outbound{}, false
	}
	o := Outbound{Type: outType, Tag: tag, Server: server, ServerPort: numOrZero(p["port"])}
	if o.Server == "" || o.ServerPort <= 0 {
		return Outbound{}, false // src/sub.js:196 的原条件
	}

	switch outType {
	case "shadowsocks":
		if v, ok := p["cipher"]; ok && v != nil {
			o.Method = strings.ToLower(str(v))
		}
		if v, ok := p["password"]; ok && v != nil {
			o.Password = str(v)
		}
	case "vmess":
		o.UUID = str(p["uuid"])
		if o.Extra == nil {
			o.Extra = Extra{}
		}
		// cipher 在 vmess 里叫 security（sing-box 的字段名），缺省 auto
		if v, ok := p["cipher"]; ok && v != nil {
			o.Extra["security"] = str(v)
		} else {
			o.Extra["security"] = "auto"
		}
		if aid := numOrZero(p["alterId"]); aid > 0 {
			o.AlterID = aid
		}
	case "vless":
		o.UUID = str(p["uuid"])
		if flow, ok := p["flow"]; ok && truthy(flow) {
			o.Flow = str(flow)
		}
	case "trojan", "hysteria2", "anytls":
		// ?? 语义：password 键存在（哪怕空串）就用它，缺席才看 auth
		if v, ok := p["password"]; ok && v != nil {
			o.Password = str(v)
		} else if v, ok := p["auth"]; ok && v != nil {
			o.Password = str(v)
		}
	case "tuic":
		o.UUID = str(p["uuid"])
		if v, ok := p["password"]; ok && v != nil {
			o.Password = str(v)
		}
		if o.Extra == nil {
			o.Extra = Extra{}
		}
		if v, ok := p["congestion-control"]; ok && v != nil {
			o.Extra["congestion_control"] = str(v)
		} else {
			o.Extra["congestion_control"] = "bbr"
		}
		if v, ok := p["udp-relay-mode"]; ok && v != nil {
			o.Extra["udp_relay_mode"] = str(v)
		} else {
			o.Extra["udp_relay_mode"] = "native"
		}
	case "socks":
		if o.Extra == nil {
			o.Extra = Extra{}
		}
		o.Extra["version"] = "5"
	}
	// username 对所有类型通用；password 的通用兜底只对上面没吃掉它的类型生效
	if u, ok := p["username"]; ok && truthy(u) {
		if o.Extra == nil {
			o.Extra = Extra{}
		}
		o.Extra["username"] = str(u)
	}
	if pw, ok := p["password"]; ok && truthy(pw) {
		if outType != "shadowsocks" && outType != "vless" && outType != "vmess" &&
			outType != "trojan" && outType != "hysteria2" && outType != "anytls" && outType != "tuic" {
			o.Password = str(pw)
		}
	}

	// trojan/hysteria2/tuic/anytls 天生 TLS；vless/vmess 要看 tls: true
	if tlsCapableTypes[outType] &&
		(p["tls"] == true || outType == "trojan" || outType == "hysteria2" || outType == "tuic" || outType == "anytls") {
		// JS clashProxyToOutbound 的 tls 块恒写 insecure（src/sub.js:263，
		// skip-cert-verify 为 false 时写 false）—— 指针三态保住这个形状。
		sv, _ := p["skip-cert-verify"].(bool)
		t := TLS{
			Enabled:  BoolPtr(true),
			ServerName: firstNonEmpty(str(p["servername"]), str(p["sni"]), o.Server),
			Insecure:   BoolPtr(sv),
		}
		if alpn, ok := p["alpn"].([]any); ok && len(alpn) > 0 {
			t.ALPN = strSlice(alpn)
		}
		if cf, ok := p["client-fingerprint"]; ok && truthy(cf) {
			t.UTLS = &UTLS{Enabled: true, Fingerprint: str(cf)}
		}
		if ro, ok := p["reality-opts"].(map[string]any); ok {
			if pk, ok := ro["public-key"]; ok && truthy(pk) {
				t.Reality = &Reality{Enabled: true, PublicKey: str(pk), ShortID: str(ro["short-id"])}
			}
		}
		o.TLS = &t
	}

	switch str(p["network"]) {
	case "ws":
		t := Transport{Type: "ws"}
		if wo, ok := p["ws-opts"].(map[string]any); ok {
			if pa, ok := wo["path"]; ok && truthy(pa) {
				t.Path = str(pa)
			}
			if hd, ok := wo["headers"].(map[string]any); ok {
				if h, ok := hd["Host"]; ok && truthy(h) {
					t.Headers = map[string]any{"Host": str(h)}
				}
			}
		}
		o.Transport = &t
	case "grpc":
		t := Transport{Type: "grpc"}
		if gopts, ok := p["grpc-opts"].(map[string]any); ok {
			if sn, ok := gopts["grpc-service-name"]; ok && truthy(sn) {
				t.Service = str(sn)
			}
		}
		o.Transport = &t
	case "h2":
		t := Transport{Type: "http"}
		if h2, ok := p["h2-opts"].(map[string]any); ok {
			if pa, ok := h2["path"]; ok && truthy(pa) {
				t.Path = str(pa)
			}
		}
		o.Transport = &t
	}

	if IsUnroutableServer(o.Server) {
		return Outbound{}, false
	}
	return o, true
}

// ParseClashYAML 解析 Clash 订阅的 proxies 列表（src/sub.js
// parseSubscriptionBodyRaw 的 Clash 分支）。YAML 损坏返回 error，由调用方
// 决定落到下一种格式；没有 proxies 键返回空切片不报错。
func ParseClashYAML(text string) ([]Outbound, error) {
	var doc struct {
		Proxies []map[string]any `yaml:"proxies"`
	}
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return nil, fmt.Errorf("parse: Clash YAML 解析失败: %w", err)
	}
	out := []Outbound{}
	for _, pr := range doc.Proxies {
		if pr == nil {
			continue
		}
		// 名字用宽容取值(R23):YAML 里不带引号的 `name: 123` 解出来是 int,旧的
		// 类型断言读到空串就被「tag 为空」判据拒掉,节点静默消失。JS 是
		// String(p.name)(src/sub.js:228-231),而同文件的 sing-box JSON 路径
		// 用的也是 str() —— 两条路径同一套取值法。
		tag := strings.TrimSpace(str(pr["name"]))
		if o, ok := ParseClashProxy(tag, pr); ok {
			out = append(out, o)
		}
	}
	return out, nil
}

// ParseSingBoxJSON 解析 {"outbounds":[...]} 形态的订阅，每项走与 Clash 同一套
// 字段映射。非对象整体（数组、标量、垃圾文本）与空数组返回空切片**不报错** ——
// 调用方（sub 包）要按顺序试多种格式，这一格式认输必须安静。
func ParseSingBoxJSON(text string) ([]Outbound, error) {
	var doc struct {
		Outbounds []json.RawMessage `json:"outbounds"`
	}
	if err := json.Unmarshal([]byte(text), &doc); err != nil {
		return []Outbound{}, nil
	}
	out := []Outbound{}
	for _, raw := range doc.Outbounds {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue // 数组里的非对象项：JS 原样放行、最终被 PROXY_TYPES 挡掉，这里提前跳过
		}
		if o, ok := outboundFromMap(m); ok {
			out = append(out, o)
		}
	}
	return out, nil
}

// outboundFromMap 把 sing-box JSON 出站的已知字段装进具名字段，其余全部进
// Extra 原样保留（约束 8）。tls/transport/obfs 若不是对象则丢弃而不是进
// Extra —— 形状已坏的嵌套块只会把 decode 期 FATAL 带进配置。
func outboundFromMap(m map[string]any) (Outbound, bool) {
	if m == nil {
		return Outbound{}, false
	}
	o := Outbound{
		Tag:        str(m["tag"]),
		Type:       str(m["type"]),
		Server:     str(m["server"]),
		ServerPort: numOrZero(m["server_port"]),
		UUID:       str(m["uuid"]),
		Password:   str(m["password"]),
		Method:     str(m["method"]),
		Flow:       str(m["flow"]),
		AlterID:    numOrZero(m["alter_id"]),
		Path:       str(m["path"]),
	}
	if sp, ok := m["server_ports"].([]any); ok {
		for _, v := range sp {
			o.ServerPorts = append(o.ServerPorts, str(v))
		}
	}
	if t, ok := m["tls"].(map[string]any); ok {
		tls := TLS{ServerName: str(t["server_name"])}
		if b, ok := t["enabled"].(bool); ok {
			tls.Enabled = BoolPtr(b) // 键存在才写 —— 透传块没有 enabled 时 JS 原样保留
		}
	if b, ok := t["insecure"].(bool); ok {
		tls.Insecure = BoolPtr(b) // 透传路径：键存在才写（JS 原样保留无 insecure 的块）
	}
		tls.ALPN = strSlice(t["alpn"])
		if u, ok := t["utls"].(map[string]any); ok {
			ut := UTLS{Fingerprint: str(u["fingerprint"])}
			if b, ok := u["enabled"].(bool); ok {
				ut.Enabled = b
			}
			tls.UTLS = &ut
		}
		if r, ok := t["reality"].(map[string]any); ok {
			rr := Reality{PublicKey: str(r["public_key"]), ShortID: str(r["short_id"])}
			if b, ok := r["enabled"].(bool); ok {
				rr.Enabled = b
			}
			tls.Reality = &rr
		}
		o.TLS = &tls
	}
	if tr, ok := m["transport"].(map[string]any); ok {
		t := Transport{Type: str(tr["type"]), Path: str(tr["path"]), Service: str(tr["service_name"])}
		if hd, ok := tr["headers"].(map[string]any); ok {
			t.Headers = hd
		}
		if h, ok := tr["host"]; ok {
			t.Host = h
		}
		o.Transport = &t
	}
	if ob, ok := m["obfs"].(map[string]any); ok {
		o.Obfs = &Obfs{Type: str(ob["type"]), Password: str(ob["password"])}
	}
	for k, v := range m {
		switch k {
		case "tag", "type", "server", "server_port", "server_ports", "uuid",
			"password", "method", "flow", "alter_id", "path", "tls", "transport", "obfs":
			continue
		}
		if o.Extra == nil {
			o.Extra = Extra{}
		}
		o.Extra[k] = v
	}
	return o, true
}

// truthy 是 JS 真值判断的近似：nil/false/空串/零值为假。Clash 字段的存在性
// 判断（p.flow、p['client-fingerprint']…）都靠它对齐。
func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	}
	return true
}

func strSlice(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		out = append(out, str(e))
	}
	return out
}
