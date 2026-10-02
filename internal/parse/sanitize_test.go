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
