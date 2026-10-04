// SPDX-License-Identifier: GPL-3.0-or-later
package parse

import (
	"encoding/base64"
	"testing"
)

// 断言表来自计划任务 5 步骤 10，端口 src/singbox.js:126-154（SS_METHODS /
// SS_METHOD_ALIASES / normalizeSsMethod）与 :221（sanitizeOutbound）。

const testUUID = "00000000-0000-4000-8000-000000000001"

func TestNormalizeSSMethodAliases(t *testing.T) {
	// Clash/v2ray 方言 → sing-box 官方拼写。唯一的真炸点别名
	// chacha20-poly1305 曾原样进配置、整份配置 check 失败、2590 个节点全灭。
	cases := []struct{ in, want string }{
		{"chacha20-poly1305", "chacha20-ietf-poly1305"},
		{"chacha20", "chacha20-ietf-poly1305"},
		{"xchacha20-poly1305", "xchacha20-ietf-poly1305"},
		{"xchacha20", "xchacha20-ietf-poly1305"},
		{"aes-256-gcm-siv", "aes-256-gcm"},
		{"chacha20-ietf-poly1305", "chacha20-ietf-poly1305"}, // 本身就是官方拼写
		{"xchacha20-ietf-poly1305", "xchacha20-ietf-poly1305"},
		{"aes-128-ctr", "aes-128-ctr"},
		{"2022-blake3-aes-128-gcm", "2022-blake3-aes-128-gcm"},
	}
	for _, c := range cases {
		got, ok := NormalizeSSMethod(c.in)
		if !ok || got != c.want {
			t.Errorf("NormalizeSSMethod(%q) = (%q, %v), want (%q, true)", c.in, got, ok, c.want)
		}
	}
	// 大小写与首尾空白先归一
	if got, ok := NormalizeSSMethod("  AES-256-GCM "); !ok || got != "aes-256-gcm" {
		t.Errorf("case/space normalization = (%q, %v)", got, ok)
	}
	// 未知的原样返回（ok=false，调用方剔除该节点）
	if got, ok := NormalizeSSMethod("bogus-cipher"); ok || got != "bogus-cipher" {
		t.Errorf("unknown = (%q, %v), want as-is with false", got, ok)
	}
	// sing-box 拒绝的旧名字（实测名单里没有的）同样不行
	for _, bad := range []string{"plain", "bf-cfb", "salsa20", "rc2-cfb", ""} {
		if _, ok := NormalizeSSMethod(bad); ok {
			t.Errorf("NormalizeSSMethod(%q) should be rejected", bad)
		}
	}
}

func TestSanitizeDropsSingBoxInternalFields(t *testing.T) {
	in := Outbound{
		Type: "vless", Server: "h.example", ServerPort: 443, UUID: testUUID,
		Extra: Extra{
			"detour":    "hop-1",                         // sing-box 内部路由字段，指向的 tag 在我们生成的配置里不存在
			"xtls":      true,                            // legacy v2ray 字段，无 sing-box 对应物
			"multiplex": map[string]any{"enabled": true}, // sing-box 认识 —— 必须保留
		},
	}
	out, ok := SanitizeOutbound(in)
	if !ok {
		t.Fatal("sanitize dropped a healthy node")
	}
	if _, hit := out.Extra["detour"]; hit {
		t.Error("detour must be dropped")
	}
	if _, hit := out.Extra["xtls"]; hit {
		t.Error("xtls must be dropped")
	}
	if _, hit := out.Extra["multiplex"]; !hit {
		t.Error("multiplex must survive")
	}
}

func TestSanitizeKeepsUnknownFields(t *testing.T) {
	in := Outbound{
		Type: "trojan", Server: "h.example", ServerPort: 443, Password: "pw",
		Extra: Extra{"domain_strategy": "prefer_ipv4", "udp_over_tcp": true},
	}
	out, ok := SanitizeOutbound(in)
	if !ok {
		t.Fatal("sanitize dropped a healthy node")
	}
	if out.Extra["domain_strategy"] != "prefer_ipv4" || out.Extra["udp_over_tcp"] != true {
		t.Fatalf("unknown keys mutated: %+v", out.Extra)
	}
}

func TestSanitizeRequiresServerAndPort(t *testing.T) {
	// 无 server：调用方丢弃
	if _, ok := SanitizeOutbound(Outbound{Type: "vless", ServerPort: 443, UUID: testUUID}); ok {
		t.Error("missing server must be rejected")
	}
	// 无端口（port<=0 且没有端口段）：调用方丢弃
	if _, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h.example", UUID: testUUID}); ok {
		t.Error("missing port must be rejected")
	}
	if _, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h.example", ServerPort: -1, UUID: testUUID}); ok {
		t.Error("negative port must be rejected")
	}
	// hysteria2 mport 节点：server_port 缺席但有 "start:end" 区间 → 合法，
	// 不能用「port<=0 即拒」一刀切掉整类节点。
	out, ok := SanitizeOutbound(Outbound{
		Type: "hysteria2", Server: "h.example", ServerPorts: []string{"2000:3000"}, Password: "pw",
	})
	if !ok {
		t.Fatal("mport node with server_ports must pass")
	}
	if len(out.ServerPorts) != 1 || out.ServerPorts[0] != "2000:3000" {
		t.Fatalf("server_ports = %v", out.ServerPorts)
	}
	// 非法 uuid 的 vless：sing-box 在 initialize 阶段 FATAL 掉整份配置 —— 直接剔除
	if _, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: "not-a-uuid"}); ok {
		t.Error("malformed uuid must be rejected")
	}
}

// TestSanitizeNeverWritesThroughToTheCaller 钉住 B15(移植错误)。JS 是在调用点
// 先 structuredClone 再 sanitize(index.js:438),Go 漏了那层克隆,又把 Extra 的
// 防御性拷贝放在 delete **之后** —— 于是注释声称的「保护调用方」做不到:Extra 是
// map,值拷贝共享同一个底层桶。TLS 是指针,同一类问题:补 enabled/utls 会写回
// 调用方的原始出站。生产调用点(app.go、rebuild.go、sbx.go)传的都是 sub.Fetch
// 返回的原始切片。
func TestSanitizeNeverWritesThroughToTheCaller(t *testing.T) {
	extra := Extra{"xtls": map[string]any{"a": 1}, "detour": "x", "username": "u"}
	clean, ok := SanitizeOutbound(Outbound{Type: "socks", Server: "h", ServerPort: 1080, Extra: extra})
	if !ok {
		t.Fatal("socks must pass")
	}
	if _, has := clean.Extra["xtls"]; has {
		t.Fatal("返回的 Extra 里 xtls 必须被删掉")
	}
	if _, has := extra["xtls"]; !has {
		t.Fatal("调用方的 Extra 被就地改写了:拷贝必须发生在 delete 之前")
	}

	tls := &TLS{ServerName: "s.example"}
	out, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, TLS: tls})
	if !ok {
		t.Fatal("vless must pass")
	}
	if tls.Enabled != nil {
		t.Fatalf("调用方的 tls.enabled 被写成了 %v:sanitize 必须先在块内摘一份副本", *tls.Enabled)
	}
	if out.TLS == tls {
		t.Fatal("返回的 TLS 与调用方共享同一个指针")
	}
	if out.TLS.Enabled == nil || !*out.TLS.Enabled {
		t.Fatalf("副本上的 enabled 应当补齐: %+v", out.TLS)
	}
}

// TestSanitizeEnforcesThePortVocabulary 钉住 B16:端口/端口段的唯一执法点过去
// 只在链接路径(isValidPort/isValidPortRange),Clash 与 sing-box-JSON 两条路径
// 只判「有没有」。于是 server_port=70000 一路通过 sanitize、到 sing-box 严格解码
// uint16 才失败,而 app 的「剔除 N 个坏节点」只统计 sanitize 的拒收 —— 那种节点
// 既不在池里也不在任何计数里。server_ports=["1:"] 更糟:sing-quic 把它展开成
// 65535 个端口,条目数由订阅方控制(["1:","2:","3:"] ⇒ ~20 万)。
func TestSanitizeEnforcesThePortVocabulary(t *testing.T) {
	rejected := []struct {
		what string
		ob   Outbound
	}{
		{"port 0", Outbound{Type: "vless", Server: "h", ServerPort: 0, UUID: testUUID}},
		{"port 65536", Outbound{Type: "vless", Server: "h", ServerPort: 65536, UUID: testUUID}},
		{"port 70000", Outbound{Type: "vless", Server: "h", ServerPort: 70000, UUID: testUUID}},
		{"open range 1:", Outbound{Type: "hysteria2", Server: "h", ServerPorts: []string{"1:"}, Password: "p"}},
		{"bare port in range", Outbound{Type: "hysteria2", Server: "h", ServerPorts: []string{"443"}, Password: "p"}},
		{"range above uint16", Outbound{Type: "hysteria2", Server: "h", ServerPorts: []string{"70000:70001"}, Password: "p"}},
		{"obfs without password", Outbound{Type: "hysteria2", Server: "h", ServerPort: 443, Password: "p", Obfs: &Obfs{Type: "salamander"}}},
	}
	for _, tc := range rejected {
		if _, ok := SanitizeOutbound(tc.ob); ok {
			t.Errorf("%s 必须被拒(它会在 sing-box 侧失败或被放大)", tc.what)
		}
	}
	accepted := []struct {
		what string
		ob   Outbound
	}{
		{"plain port", Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID}},
		{"mport range", Outbound{Type: "hysteria2", Server: "h", ServerPorts: []string{"20000:20100"}, Password: "p"}},
		{"single-port range", Outbound{Type: "hysteria2", Server: "h", ServerPorts: []string{"443:443"}, Password: "p"}},
		{"obfs with password", Outbound{Type: "hysteria2", Server: "h", ServerPort: 443, Password: "p", Obfs: &Obfs{Type: "salamander", Password: "obfsp"}}},
	}
	for _, tc := range accepted {
		if _, ok := SanitizeOutbound(tc.ob); !ok {
			t.Errorf("%s 不该被拒", tc.what)
		}
	}
	// 两者同时出现时以端口段为准(sing-box 的 hysteria2 二选一;链接路径
	// parse.go:445 早就强制了,sanitize 过去从不 enforce)。
	both, ok := SanitizeOutbound(Outbound{Type: "hysteria2", Server: "h", ServerPort: 443,
		ServerPorts: []string{"20000:20100"}, Password: "p"})
	if !ok {
		t.Fatal("server_port 与 server_ports 并存不该丢节点,该归一")
	}
	if both.ServerPort != 0 {
		t.Fatalf("server_port = %d, want 0(端口段优先)", both.ServerPort)
	}
}

// TestSanitizeObfsPasswordMatchesSingBoxPredicate 钉住整分支评审的 NIT-8:
// 密码判据必须与 sing-box 同形 —— 它判的是 `options.Obfs.Password == ""`
// (protocol/hysteria2/outbound.go:65),纯空白在**它那边是一个合法密钥串**。
// 我们多一次 TrimSpace 就是替订阅方丢掉一个能用的节点,而且这条丢弃会混进
// 「剔除 N 个坏节点」的计数里,运维根本看不出是自己把它判死的。
//
// 判据要比上游严,只能严在「上游同样会拒」的地方:type 走 enum 比较
// (option/hysteria2.go:85-91,空白值也是 unknown obfs type),所以 type 这一侧
// 保留 TrimSpace。
func TestSanitizeObfsPasswordMatchesSingBoxPredicate(t *testing.T) {
	blank, ok := SanitizeOutbound(Outbound{Type: "hysteria2", Server: "h", ServerPort: 443,
		Password: "p", Obfs: &Obfs{Type: "salamander", Password: "  "}})
	if !ok {
		t.Fatal("空白 obfs 密码被我们判死了,而 sing-box 会照常接受这个节点")
	}
	if blank.Obfs.Password != "  " {
		t.Fatalf("sanitize 不该改写密码: %q", blank.Obfs.Password)
	}
	if _, ok := SanitizeOutbound(Outbound{Type: "hysteria2", Server: "h", ServerPort: 443,
		Password: "p", Obfs: &Obfs{Type: "  ", Password: "x"}}); ok {
		t.Fatal("空白 obfs type 应当被拒(sing-box 的 enum 比较同样会拒)")
	}
	// 真·空密码照旧拒绝:那才是 "missing obfs password"。
	if _, ok := SanitizeOutbound(Outbound{Type: "hysteria2", Server: "h", ServerPort: 443,
		Password: "p", Obfs: &Obfs{Type: "salamander"}}); ok {
		t.Fatal("空密码必须被拒")
	}
}

// TestSanitizeKeepsTlsOnHttpOutbound 钉住 R24:tlsCapableTypes 把 http 和
// socks/shadowsocks/ssh 归成一类,但 sing-box v1.14 的 http 出站**有** tls
// (protocol/http/outbound.go:37 用 options.TLS 建 dialer;socks/ss/ssh 确实没有)
// ⇒ 一个 HTTPS 代理被静默降级成明文,凭据还走 clear text。
func TestSanitizeKeepsTlsOnHttpOutbound(t *testing.T) {
	out, ok := SanitizeOutbound(Outbound{Type: "http", Server: "h", ServerPort: 8443,
		TLS: &TLS{ServerName: "h.example"}})
	if !ok {
		t.Fatal("http 出站应当通过")
	}
	if out.TLS == nil {
		t.Fatal("http 的 tls 被剥了:HTTPS 代理会静默降级成明文")
	}
	if out.TLS.Enabled == nil || !*out.TLS.Enabled {
		t.Fatalf("缺 enabled 应当补成 true: %+v", out.TLS)
	}
	// 对照组:socks 的 tls 仍然必须剥(sing-box 的 socks 出站没有该字段,带着
	// 会让整份配置 FATAL)。
	if s, _ := SanitizeOutbound(Outbound{Type: "socks", Server: "h", ServerPort: 1080, TLS: &TLS{Enabled: BoolPtr(true)}}); s.TLS != nil {
		t.Fatal("socks 仍不得带 tls")
	}
}

func TestSanitizeNormalizesFlowTransportAndTLS(t *testing.T) {
	// flow：只有 xtls-rprx-vision 存在；-udp443 拼法归一，其余删除
	f, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, Flow: "xtls-rprx-vision-udp443"})
	if !ok || f.Flow != "xtls-rprx-vision" {
		t.Fatalf("flow = %+v ok=%v", f.Flow, ok)
	}
	if f2, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, Flow: "none"}); !ok || f2.Flow != "" {
		t.Fatalf("flow none must be deleted, got %q ok=%v", f2.Flow, ok)
	}
	// transport：tcp/raw 等价于没有传输层；未知传输（xhttp）会让 sing-box
	// 在 decode 阶段 FATAL —— 只能丢弃该节点
	tr, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, Transport: &Transport{Type: "tcp"}})
	if !ok || tr.Transport != nil {
		t.Fatalf("tcp transport must be removed, ok=%v", ok)
	}
	if _, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, Transport: &Transport{Type: "xhttp"}}); ok {
		t.Fatal("unknown transport must drop the node")
	}
	// 已知传输的大小写要规范化回写：旧实现只把 ToLower 结果用于**判定**，
	// Type 原样进盘，于是同一台服务器的指纹随订阅源怎么写而变。
	if n, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, Transport: &Transport{Type: "WS"}}); !ok || n.Transport.Type != "ws" {
		t.Fatalf("transport type not normalized: %+v ok=%v", n.Transport, ok)
	}
	// tls 块 enabled:false 等价于没有 tls；非 vless 的协议带 tls 会整份配置 FATAL
	if s, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, TLS: &TLS{Enabled: BoolPtr(false)}}); !ok || s.TLS != nil {
		t.Fatalf("disabled tls must be removed, ok=%v", ok)
	}
	if s, ok := SanitizeOutbound(Outbound{Type: "shadowsocks", Server: "h", ServerPort: 443, Method: "aes-256-gcm", TLS: &TLS{Enabled: BoolPtr(true)}}); !ok || s.TLS != nil {
		t.Fatalf("ss must not carry tls, ok=%v", ok)
	}
	// reality 需要 uTLS：补一个 sing-box 认识的指纹（缺省 chrome）
	r, ok := SanitizeOutbound(Outbound{
		Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID,
		TLS: &TLS{Enabled: BoolPtr(true), Reality: &Reality{Enabled: true, PublicKey: "K"}},
	})
	if !ok || r.TLS.UTLS == nil || r.TLS.UTLS.Fingerprint != "chrome" {
		t.Fatalf("reality utls = %+v ok=%v", r.TLS.UTLS, ok)
	}
	// 未知 uTLS 指纹（v2ray 的 unsafe）— 丢 utls 块
	u, ok := SanitizeOutbound(Outbound{
		Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID,
		TLS: &TLS{Enabled: BoolPtr(true), UTLS: &UTLS{Enabled: true, Fingerprint: "unsafe"}},
	})
	if !ok || u.TLS.UTLS != nil {
		t.Fatalf("unknown fingerprint must drop utls, ok=%v", ok)
	}
	// 白名单外的协议字段删除：ss 带 flow / 非 hysteria2 带 server_ports
	if s, ok := SanitizeOutbound(Outbound{Type: "shadowsocks", Server: "h", ServerPort: 443, Method: "aes-256-gcm", Flow: "xtls-rprx-vision"}); !ok || s.Flow != "" {
		t.Fatalf("ss flow must be deleted, ok=%v", ok)
	}
	if s, ok := SanitizeOutbound(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: testUUID, ServerPorts: []string{"1:2"}}); !ok || s.ServerPorts != nil {
		t.Fatalf("vless server_ports must be deleted, ok=%v", ok)
	}
	// shadowsocks：未知 method 的 base64("method:password") 兜底解码
	b64 := base64.StdEncoding.EncodeToString([]byte("chacha20-ietf-poly1305:pw"))
	s, ok := SanitizeOutbound(Outbound{Type: "shadowsocks", Server: "h", ServerPort: 443, Method: b64})
	if !ok || s.Method != "chacha20-ietf-poly1305" || s.Password != "pw" {
		t.Fatalf("b64 method fallback = %+v ok=%v", s, ok)
	}
	// 彻底认不出的 cipher：剔除该节点，绝不让它炸掉整份配置
	if _, ok := SanitizeOutbound(Outbound{Type: "shadowsocks", Server: "h", ServerPort: 443, Method: "total-garbage"}); ok {
		t.Fatal("unknown cipher must drop the node")
	}
}
