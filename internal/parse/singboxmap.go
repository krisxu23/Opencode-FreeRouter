// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package parse

import "encoding/json"

// SingBoxMap renders the outbound in the shape sing-box reads, with every
// unmodelled field carried through verbatim from Extra.
//
// The explicit fields are mapped by hand; the rest of Extra is copied on top.
// Copying Extra last is deliberate: it lets a future sing-box field that we do
// not model survive, while the fields we do model always win.
//
// 调用契约：先过 SanitizeOutbound 再进这里。sanitize 保证 TLS（若在）enabled、
// transport 类型合法、flow/method 已归一——这里只做形状映射，不做语义修补。
func (o Outbound) SingBoxMap(tag string) map[string]any {
	m := map[string]any{"type": o.Type, "tag": tag}
	if o.Server != "" {
		m["server"] = o.Server
	}
	if o.ServerPort != 0 {
		m["server_port"] = o.ServerPort
	}
	if len(o.ServerPorts) > 0 {
		m["server_ports"] = o.ServerPorts
	}
	if o.UUID != "" {
		m["uuid"] = o.UUID
	}
	if o.Password != "" {
		m["password"] = o.Password
	}
	if o.Method != "" {
		// sanitize 已经把 shadowsocks 的 method 归一过；再走一遍归一是对
		// 非 ss 协议携带 method 字段的兜底（sing-box 只在 ss 上读它）。
		if n, ok := NormalizeSSMethod(o.Method); ok {
			m["method"] = n
		} else {
			m["method"] = o.Method
		}
	}
	if o.Flow != "" {
		m["flow"] = o.Flow
	}
	if o.AlterID != 0 {
		m["alter_id"] = o.AlterID
	}
	if o.Path != "" {
		m["path"] = o.Path
	}
	if o.Obfs != nil {
		obfs := map[string]any{}
		if o.Obfs.Type != "" {
			obfs["type"] = o.Obfs.Type
		}
		if o.Obfs.Password != "" {
			// hysteria2 的 salamander 混淆靠 password 生效；丢了它节点
			// 直连上游的混淆口，必然连不上且无日志。
			obfs["password"] = o.Obfs.Password
		}
		if o.Obfs.Host != "" {
			obfs["host"] = o.Obfs.Host
		}
		m["obfs"] = obfs
	}
	if o.TLS != nil {
		tls := map[string]any{"enabled": true}
		if o.TLS.ServerName != "" {
			tls["server_name"] = o.TLS.ServerName
		}
		if o.TLS.Insecure != nil && *o.TLS.Insecure {
			tls["insecure"] = true
		}
		if len(o.TLS.ALPN) > 0 {
			tls["alpn"] = o.TLS.ALPN
		}
		if o.TLS.UTLS != nil && o.TLS.UTLS.Enabled {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": o.TLS.UTLS.Fingerprint}
		}
		if o.TLS.Reality != nil && o.TLS.Reality.Enabled {
			tls["reality"] = map[string]any{
				"enabled":    true,
				"public_key": o.TLS.Reality.PublicKey,
				"short_id":   o.TLS.Reality.ShortID,
			}
		}
		m["tls"] = tls
	}
	if o.Transport != nil {
		tr := map[string]any{}
		if o.Transport.Type != "" {
			tr["type"] = o.Transport.Type
		}
		if o.Transport.Path != "" {
			tr["path"] = o.Transport.Path
		}
		if o.Transport.Service != "" {
			tr["service_name"] = o.Transport.Service
		}
		if len(o.Transport.Headers) > 0 {
			tr["headers"] = o.Transport.Headers
		}
		if o.Transport.Host != nil {
			tr["host"] = o.Transport.Host
		}
		m["transport"] = tr
	}
	for k, v := range o.Extra {
		if _, taken := m[k]; taken {
			continue
		}
		m[k] = materializeJSON(v)
	}
	return m
}

// materializeJSON 把 Extra 里"长得像 JSON 的字符串"还原成对象。
//
// 链接解析层把 multiplex 这类查询参数原样存成字符串（`{"enabled":true}`），
// 而 sing-box 的对应字段是结构体——字符串进去会在 decode 期被打回。JS 版在
// 解析层就 JSON.parse 过了，这里补上同一语义；不是 JSON 的字符串原样保留。
func materializeJSON(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if len(s) == 0 || (s[0] != '{' && s[0] != '[') {
		return v
	}
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return v
	}
	return parsed
}
