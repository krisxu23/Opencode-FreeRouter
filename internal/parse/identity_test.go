// SPDX-License-Identifier: GPL-3.0-or-later
package parse

import "testing"

// tests/registry.test.js:8-11 的 ob() 只发 {tag,type,server,server_port}，
// 没有凭据字段。指纹在这些字段缺席时必须与 JS 版逐字节相同，否则注册表里
// 现存的墓碑全部失配，被淘汰的节点会立刻复活。
func TestIdentityDegeneratesToTypeAndServer(t *testing.T) {
	got := IdentityOf(Outbound{Type: "vless", Server: "n1.example", ServerPort: 443})
	if got != "vless|n1.example:443" {
		t.Fatalf("identity = %q, want %q", got, "vless|n1.example:443")
	}
}

func TestIdentityPrefersServerPortsList(t *testing.T) {
	// server_ports 的元素是 "start:end" 字符串（hysteria2 mport 的区间写法，
	// sing-box option 里也是 []string），不是整数 —— 整数装不下区间。
	got := IdentityOf(Outbound{Type: "shadowsocks", Server: "s", ServerPorts: []string{"80", "443"}})
	if got != "shadowsocks|s:80,443" {
		t.Fatalf("identity = %q", got)
	}
}

func TestIdentityIsCaseSensitive(t *testing.T) {
	// 约束 9：任何规范化都会破坏墓碑匹配。
	a := IdentityOf(Outbound{Type: "vless", Server: "h.example", TLS: &TLS{ServerName: "ov-Germany1.09vpn.com"}})
	b := IdentityOf(Outbound{Type: "vless", Server: "h.example", TLS: &TLS{ServerName: "ov-germany1.09vpn.com"}})
	if a == b {
		t.Fatalf("SNI case was normalised away: both = %q", a)
	}
}

func TestIdentityIncludesCredentialsAndTransport(t *testing.T) {
	// 换凭据必须算新身份：否则一个 server:port 上换 uuid 会继承旧节点的连败次数。
	a := IdentityOf(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: "u1"})
	b := IdentityOf(Outbound{Type: "vless", Server: "h", ServerPort: 443, UUID: "u2"})
	if a == b {
		t.Fatalf("different uuid produced the same identity %q", a)
	}
}

// TestIdentityIncludesTopLevelPath 钉住 R22:Clash / sing-box-JSON 路径把 http 类
// 代理的 path 放在**顶层**(clash.go 写入 o.Path、SingBoxMap 也按顶层 path 发出),
// 而 IdentityOf 只从 transport.path 拼 `path=` ⇒ 两个只有顶层 path 不同的代理塌缩
// 成同一身份:sub.Fetch 的轮内去重丢掉一个,registry.Merge 把它们当同一物理节点,
// 连败与墓碑还会跨服务器传染。JS 的 identityOf 同样只看 transport.path,所以这是
// 双方共有缺陷,但 Go 的合并与墓碑都以它为键,后果比 JS 重。
//
// 只在字段非空时才拼:没有顶层 path 的既有身份串(以及现网文件里的墓碑键)一字不变。
func TestIdentityIncludesTopLevelPath(t *testing.T) {
	a := Outbound{Type: "http", Server: "h.example", ServerPort: 8080, Path: "/one"}
	b := Outbound{Type: "http", Server: "h.example", ServerPort: 8080, Path: "/two"}
	c := Outbound{Type: "http", Server: "h.example", ServerPort: 8080, Path: "/one"}
	if IdentityOf(a) == IdentityOf(b) {
		t.Fatalf("只有顶层 path 不同的两个代理塌缩成同一身份: %q", IdentityOf(a))
	}
	if IdentityOf(a) != IdentityOf(c) {
		t.Fatalf("同一配置算出了两个身份: %q vs %q", IdentityOf(a), IdentityOf(c))
	}
	if got, want := IdentityOf(Outbound{Type: "http", Server: "h.example", ServerPort: 8080}), "http|h.example:8080"; got != want {
		t.Fatalf("无顶层 path 的身份串 = %q, want %q:键形状不能漂,否则现存墓碑全部失配", got, want)
	}
}

// JS 版落盘的墓碑键形状逐字节取样（形状，不是现网数据 —— 值全是合成样例）：
//
//	"vless|<ip>:<port>|uuid=…|sni=…|tr=ws|path=/Ra-vl|host=…"
//	"hysteria2|<ip>:443|password=…|sni=…|obfs=salamander:…"
//	"vless|<ip>:<port>|uuid=…|sni=…|pbk=…|sid=|tr=grpc|svc=…"
//
// 凭据字段带 `字段名=` 前缀；reality 的 pbk/sid **总是成对出现**（sid 为空也要
// 占位 `sid=`）；obfs 是 `type:password`；host 取自 transport.headers.Host。
// 任何一条对不上，现存墓碑全部失配。
func TestIdentityKeyShapeMatchesJS(t *testing.T) {
	got := IdentityOf(Outbound{
		Type: "vless", Server: "203.0.113.11", ServerPort: 59686,
		UUID: "00000000-0000-4000-8000-000000000001",
		TLS: &TLS{
			ServerName: "vless.example",
			UTLS:       &UTLS{Enabled: true, Fingerprint: "chrome"},
		},
		Transport: &Transport{
			Type:    "ws",
			Path:    "/Ra-vl",
			Headers: map[string]any{"Host": "vless.example"},
		},
	})
	want := "vless|203.0.113.11:59686|uuid=00000000-0000-4000-8000-000000000001|sni=vless.example|tr=ws|path=/Ra-vl|host=vless.example"
	if got != want {
		t.Fatalf("identity = %q, want %q", got, want)
	}

	// reality：pbk 与 sid 成对，sid 空也要占位（`pbk=…|sid=|tr=grpc|svc=…`）。
	reality := IdentityOf(Outbound{
		Type: "vless", Server: "203.0.113.13", ServerPort: 10453,
		UUID: "00000000-0000-4000-8000-000000000002",
		TLS: &TLS{
			ServerName: "reality.example",
			Reality:    &Reality{Enabled: true, PublicKey: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"},
		},
		Transport: &Transport{Type: "grpc", Service: "TunService"},
	})
	wantReality := "vless|203.0.113.13:10453|uuid=00000000-0000-4000-8000-000000000002|sni=reality.example|pbk=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA|sid=|tr=grpc|svc=TunService"
	if reality != wantReality {
		t.Fatalf("reality identity = %q, want %q", reality, wantReality)
	}

	// hysteria2 的 obfs 拼成 `type:password`（`obfs=salamander:…`）。
	hy2 := IdentityOf(Outbound{
		Type: "hysteria2", Server: "203.0.113.12", ServerPort: 443,
		Password: "synthetic-hy2-password",
		TLS:      &TLS{ServerName: "hy2.example"},
		Obfs:     &Obfs{Type: "salamander", Password: "synthetic-obfs-password"},
	})
	wantHy2 := "hysteria2|203.0.113.12:443|password=synthetic-hy2-password|sni=hy2.example|obfs=salamander:synthetic-obfs-password"
	if hy2 != wantHy2 {
		t.Fatalf("hysteria2 identity = %q, want %q", hy2, wantHy2)
	}
}

func TestUnroutableCatchesLoopbackAndPrivate(t *testing.T) {
	bad := []string{"127.0.0.1", "127.1.2.3", "::1", "::", "0.0.0.0", "[::1]",
		"::ffff:127.0.0.1", "10.0.0.1", "192.168.1.1", "172.16.0.1", "172.31.255.255",
		"169.254.1.1", "fc00::1", "fd12:3456::1", "localhost"}
	for _, s := range bad {
		if !IsUnroutableServer(s) {
			t.Errorf("%q should be unroutable", s)
		}
	}
}

func TestUnroutableCatchesMappedIPv6LinkLocalAndThisNet(t *testing.T) {
	// W15:过去只堵了 ::ffff:127. 一条 —— 映射写法的 RFC1918/云元数据、
	// IPv6 link-local(fe80::/10)与 0.0.0.0/8 全部穿透到 nodeprobe 的拨号器。
	bad := []string{
		"::ffff:10.0.0.5", "::ffff:192.168.1.10", "::ffff:172.16.0.9",
		"::ffff:169.254.169.254", "::ffff:0.1.2.3",
		"fe80::1", "febf::a", "feb0::", "feaa::5",
		"0.1.2.3",
	}
	for _, s := range bad {
		if !IsUnroutableServer(s) {
			t.Errorf("%q should be unroutable (mapped/link-local/this-net)", s)
		}
	}
}

func TestUnroutableSparesRealHosts(t *testing.T) {
	// 这一组是被误杀过的真实地址：128.x 是公网，172.15/172.32 在私网段外，
	// localhost.example.com 是真实域名。
	good := []string{"128.0.0.1", "27.0.0.1", "172.32.0.1", "172.15.0.1", "192.169.0.1",
		"11.0.0.1", "localhost.example.com", "[2001:db8::1]", "example.com:443", "",
		// fe8/fe9/fea/feb 是**合法主机名**的常见开头：旧实现用裸前缀判
		// link-local，把这些整条节点误杀。而 febf::1 这类真地址属于
		// fe80::/10 同一段，裸前缀反而漏判。
		"fe8-node.example.com", "fe9.example.org", "fea.example.net", "feb01.example.io"}
	for _, s := range good {
		if IsUnroutableServer(s) {
			t.Errorf("%q must not be treated as unroutable", s)
		}
	}
}

func TestParseNodeUriStripsIPv6BracketsWithoutFiltering(t *testing.T) {
	// tests/parse-links.test.js:533：解析层只负责剥方括号，
	// 过滤是 parseLinks / Clash 入口的事。
	o, err := ParseNodeURI("ssh://u@[::1]:22")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Server != "::1" {
		t.Fatalf("server = %q, want ::1", o.Server)
	}
	if o.Type != "ssh" || o.ServerPort != 22 {
		t.Fatalf("type/port = %q/%d, want ssh/22", o.Type, o.ServerPort)
	}
}
