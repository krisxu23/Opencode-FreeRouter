// SPDX-License-Identifier: GPL-3.0-or-later
package parse

import "testing"

// 断言表来自计划任务 5 步骤 10，端口 src/sub.js:196 clashProxyToOutbound
// 与 parseSubscriptionBodyRaw 的 sing-box JSON 分支。

func TestParseClashProxyVlessWs(t *testing.T) {
	p := map[string]any{
		"type": "vless", "uuid": testUUID, "server": "h.example", "port": 443,
		"tls":     true,
		"network": "ws",
		"ws-opts": map[string]any{
			"path":    "/ws",
			"headers": map[string]any{"Host": "cdn.example"},
		},
		"servername": "s.example",
	}
	o, ok := ParseClashProxy("n1", p)
	if !ok {
		t.Fatal("ParseClashProxy rejected a valid vless")
	}
	if o.Type != "vless" || o.Tag != "n1" || o.UUID != testUUID {
		t.Fatalf("basic = %+v", o)
	}
	if o.Transport == nil || o.Transport.Type != "ws" || o.Transport.Path != "/ws" {
		t.Fatalf("transport = %+v", o.Transport)
	}
	if o.Transport.Headers["Host"] != "cdn.example" {
		t.Fatalf("headers = %+v", o.Transport.Headers)
	}
	if o.TLS == nil || o.TLS.Enabled == nil || !*o.TLS.Enabled || o.TLS.ServerName != "s.example" {
		t.Fatalf("tls = %+v", o.TLS)
	}
}

func TestParseClashProxyHysteria2(t *testing.T) {
	p := map[string]any{
		"type": "hysteria2", "password": "pw", "server": "h.example", "port": 443,
		"skip-cert-verify": true,
	}
	o, ok := ParseClashProxy("hy", p)
	if !ok {
		t.Fatal("ParseClashProxy rejected a valid hysteria2")
	}
	if o.Type != "hysteria2" || o.Password != "pw" {
		t.Fatalf("basic = %+v", o)
	}
	// hysteria2 是天生 TLS 的类型，不需要 tls: true 也得带 TLS 块
	if o.TLS == nil || o.TLS.Enabled == nil || !*o.TLS.Enabled || o.TLS.ServerName != "h.example" {
		t.Fatalf("tls = %+v", o.TLS)
	}
	if o.TLS.Insecure == nil || !*o.TLS.Insecure {
		t.Fatal("skip-cert-verify must map to insecure")
	}
	// reality-opts → reality 块。注意 vless 必须**显式** tls: true 才建 TLS 块
	// （JS 的 needsTls 对 vless 不像 trojan/hysteria2 那样天生开）—— 真实的
	// Clash reality 配置都带 tls: true。
	p2 := map[string]any{
		"type": "vless", "uuid": testUUID, "server": "h.example", "port": 443,
		"tls":          true,
		"reality-opts": map[string]any{"public-key": "PUBKEY", "short-id": "ab"},
	}
	o2, ok := ParseClashProxy("rl", p2)
	if !ok || o2.TLS == nil || o2.TLS.Reality == nil || o2.TLS.Reality.PublicKey != "PUBKEY" || o2.TLS.Reality.ShortID != "ab" {
		t.Fatalf("reality = %+v ok=%v", o2.TLS, ok)
	}
}

func TestParseClashProxyRejectsBadAddress(t *testing.T) {
	// src/sub.js:196 的原条件：!p.server 与 !(ob.server_port > 0)
	if _, ok := ParseClashProxy("n", map[string]any{"type": "vless", "uuid": testUUID, "port": 443}); ok {
		t.Error("empty server must be rejected")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "vless", "uuid": testUUID, "server": "h.example", "port": 0}); ok {
		t.Error("port 0 must be rejected")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "vless", "uuid": testUUID, "server": "h.example", "port": -1}); ok {
		t.Error("negative port must be rejected")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "vless", "uuid": testUUID, "server": "h.example", "port": "abc"}); ok {
		t.Error("non-numeric port must be rejected")
	}
	if _, ok := ParseClashProxy("", map[string]any{"type": "vless", "uuid": testUUID, "server": "h.example", "port": 443}); ok {
		t.Error("missing name must be rejected")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "wireguard", "server": "h.example", "port": 443}); ok {
		t.Error("unknown clash type must be rejected")
	}
}

func TestParseClashProxyRejectsLoopback(t *testing.T) {
	// loopback 拒绝走 IsUnroutableServer（与链接入口同一套判据收口），
	// 而不是在 ParseClashProxy 里硬编码 127.0.0.1。
	if _, ok := ParseClashProxy("n", map[string]any{"type": "trojan", "password": "p", "server": "127.0.0.1", "port": 443}); ok {
		t.Error("loopback must be rejected via IsUnroutableServer")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "trojan", "password": "p", "server": "[::1]", "port": 443}); ok {
		t.Error("ipv6 loopback must be rejected via IsUnroutableServer")
	}
	if _, ok := ParseClashProxy("n", map[string]any{"type": "trojan", "password": "p", "server": "10.0.0.1", "port": 443}); ok {
		t.Error("RFC1918 must be rejected via IsUnroutableServer")
	}
}

func TestParseSingBoxJSONReadsOutbounds(t *testing.T) {
	text := `{"outbounds":[
		{"type":"vless","tag":"a","server":"h.example","server_port":443,
		 "uuid":"` + testUUID + `","multiplex":{"enabled":true}},
		"not-an-object"
	]}`
	outs, err := ParseSingBoxJSON(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("outbounds = %d, want 1", len(outs))
	}
	o := outs[0]
	if o.Tag != "a" || o.Type != "vless" || o.Server != "h.example" || o.ServerPort != 443 || o.UUID != testUUID {
		t.Fatalf("outbound = %+v", o)
	}
	// 约束 8：sing-box 认识但未建模的字段进 Extra 原样保留
	if _, hit := o.Extra["multiplex"]; !hit {
		t.Fatalf("multiplex must land in Extra: %+v", o.Extra)
	}

	// 空数组 / 非对象 / 垃圾：空切片，不报错
	for _, junk := range []string{`{"outbounds":[]}`, `[]`, `"just a string"`, `42`, `not json at all`} {
		outs, err := ParseSingBoxJSON(junk)
		if err != nil {
			t.Errorf("ParseSingBoxJSON(%q) errored: %v", junk, err)
		}
		if len(outs) != 0 {
			t.Errorf("ParseSingBoxJSON(%q) = %d entries, want empty", junk, len(outs))
		}
	}
}

func TestParseClashYAMLReadsProxies(t *testing.T) {
	text := `proxies:
  - name: a
    type: ss
    server: 1.2.3.4
    port: 8388
    cipher: aes-256-gcm
    password: pw
  - name: b
    type: trojan
    server: 5.6.7.8
    port: 443
    password: tp
`
	outs, err := ParseClashYAML(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(outs) != 2 {
		t.Fatalf("proxies = %d, want 2", len(outs))
	}
	if outs[0].Type != "shadowsocks" || outs[0].Method != "aes-256-gcm" || outs[0].Password != "pw" || outs[0].Tag != "a" {
		t.Fatalf("ss proxy = %+v", outs[0])
	}
	if outs[1].Type != "trojan" || outs[1].Password != "tp" {
		t.Fatalf("trojan proxy = %+v", outs[1])
	}
	// 无 proxies 键 → 空切片不报错（JS 版落空后走其它格式，这里返回空由调用方接手）
	if outs, err := ParseClashYAML("hello: world"); err != nil || len(outs) != 0 {
		t.Fatalf("no proxies = %v, %v", outs, err)
	}
	if _, err := ParseClashYAML("proxies: [unclosed"); err == nil {
		t.Fatal("broken YAML must report an error")
	}
}

// TestClashNumericProxyNameKeepsTheNode 钉住 R23:YAML 里没加引号的 `name: 123`
// 被解成 int,而 Go 用类型断言 `pr["name"].(string)` 读到空串,于是整条代理被
// ParseClashProxy 的「tag 为空」判据拒掉,节点静默消失。JS 是 String(p.name)
// (src/sub.js:228-231),而 Go 在同一个文件的 sing-box JSON 路径用的是宽容的
// str() —— 同一份数据两条路径行为相反,取宽容的那条。
func TestClashNumericProxyNameKeepsTheNode(t *testing.T) {
	text := `proxies:
  - name: 123
    type: ss
    server: 1.2.3.4
    port: 8388
    cipher: aes-256-gcm
    password: pw
`
	outs, err := ParseClashYAML(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(outs) != 1 {
		t.Fatalf("proxies = %d, want 1(数字名不得丢节点)", len(outs))
	}
	if outs[0].Tag != "123" {
		t.Fatalf("tag = %q, want 123", outs[0].Tag)
	}
}

// TestParseClashYamlDuplicateKeyIsAnError 是 R25 的前提:重复键必须是**错误**
// 而不是静默回落 —— yaml.v3 的 uniqueKeys 判定是对的,错的是调用方把 err 丢了。
func TestParseClashYamlDuplicateKeyIsAnError(t *testing.T) {
	text := "proxies:\n  - name: a\n    type: ss\n    server: 1.2.3.4\n    port: 8388\nproxies:\n  - name: b\n    type: ss\n    server: 5.6.7.8\n    port: 8388\n"
	if _, err := ParseClashYAML(text); err == nil {
		t.Fatal("重复 proxies 键必须报错(错误信息由调用方决定是否上报)")
	}
}
