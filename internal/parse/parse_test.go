// SPDX-License-Identifier: GPL-3.0-or-later
package parse

import (
	"encoding/base64"
	"strings"
	"testing"
)

// 断言表来自计划任务 5 步骤 7，每条对应 tests/parse-links.test.js 里一个
// 必须逐字复刻的行为。

func TestVlessWsTlsReality(t *testing.T) {
	o, err := ParseNodeURI("vless://u1@h.example:443?type=ws&security=reality&pbk=PUBKEY&sid=ab12&sni=s.example&fp=chrome&path=/x#Node")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Type != "vless" || o.UUID != "u1" || o.Server != "h.example" || o.ServerPort != 443 {
		t.Fatalf("basic = %+v", o)
	}
	if o.Tag != "Node" {
		t.Fatalf("tag = %q, want Node", o.Tag)
	}
	if o.TLS == nil || o.TLS.ServerName != "s.example" {
		t.Fatalf("tls = %+v", o.TLS)
	}
	if o.TLS.Reality == nil || o.TLS.Reality.PublicKey != "PUBKEY" || o.TLS.Reality.ShortID != "ab12" {
		t.Fatalf("reality = %+v", o.TLS.Reality)
	}
	if o.TLS.UTLS == nil || o.TLS.UTLS.Fingerprint != "chrome" {
		t.Fatalf("utls = %+v", o.TLS.UTLS)
	}
	if o.Transport == nil || o.Transport.Type != "ws" || o.Transport.Path != "/x" {
		t.Fatalf("transport = %+v", o.Transport)
	}
}

func TestVmessBase64(t *testing.T) {
	payload := `{"add":"v.example","port":"8443","id":"uuid-1","aid":4,"tls":"tls","sni":"s.example","net":"ws","path":"/ws","host":"cdn.example"}`
	o, err := ParseNodeURI("vmess://" + base64.StdEncoding.EncodeToString([]byte(payload)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Type != "vmess" || o.Server != "v.example" || o.ServerPort != 8443 || o.UUID != "uuid-1" {
		t.Fatalf("basic = %+v", o)
	}
	if o.AlterID != 4 {
		t.Fatalf("alter_id = %d, want 4", o.AlterID)
	}
	if o.TLS == nil || o.TLS.ServerName != "s.example" {
		t.Fatalf("tls = %+v", o.TLS)
	}
	if o.Transport == nil || o.Transport.Type != "ws" || o.Transport.Path != "/ws" {
		t.Fatalf("transport = %+v", o.Transport)
	}
}

// TestVmessWithFragmentStillParses 钉住 B14:v2rayN/v2rayNG 导出的 vmess 链接
// 几乎恒带 `#名字`,而载荷是 base64 —— fragment 不是载荷的一部分。旧代码把整段
// 余下内容喂给 b64decode:它丢掉 `#` 却留下其后的字母数字,于是载荷变成
// 「base64 + 垃圾」,StdEncoding 在 padding 位报 CorruptInputError → 空串 →
// JSON 失败 → 整条 vmess 静默丢弃,也就是订阅里**全部** vmess 节点。
// 其他协议解析器开头都做一次 SplitN("#"),只有 vmess 漏了;Node 的
// Buffer.from 对尾部垃圾宽容,所以 JS 不坏 —— 这是 Go 独有回归。
func TestVmessWithFragmentStillParses(t *testing.T) {
	payload := `{"add":"203.0.113.10","port":"443","id":"` + testUUID + `","net":"ws","path":"/vmpath","host":"vmh.example.com","tls":"tls","sni":"vmsni.example.com"}`
	for _, b64 := range []string{
		base64.StdEncoding.EncodeToString([]byte(payload)),
		// 去掉 padding 的变体:订阅商两种都发,而 fragment 接在 padding 后面
		// 是最容易踩的形状(StdEncoding 在 '=' 位直接报 CorruptInputError)。
		strings.TrimRight(base64.StdEncoding.EncodeToString([]byte(payload)), "="),
	} {
		for _, frag := range []string{"", "#US-01", "#🇺🇸 US-01", "#%F0%9F%87%BA%F0%9F%87%B8%20US-01", "#美国 01"} {
			o, err := ParseNodeURI("vmess://" + b64 + frag)
			if err != nil {
				t.Fatalf("fragment %q: %v", frag, err)
			}
			if o.Type != "vmess" || o.Server != "203.0.113.10" || o.ServerPort != 443 || o.UUID != testUUID {
				t.Fatalf("fragment %q: 载荷解错 %+v", frag, o)
			}
			if o.Transport == nil || o.Transport.Path != "/vmpath" {
				t.Fatalf("fragment %q: transport = %+v", frag, o.Transport)
			}
		}
	}
	// tag 仍然来自 fragment(与其他协议同一规则)
	o, err := ParseNodeURI("vmess://" + base64.StdEncoding.EncodeToString([]byte(payload)) + "#US-01")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Tag != "US-01" {
		t.Fatalf("tag = %q, want US-01", o.Tag)
	}
}

// TestSsPasswordsAreDecodedExactlyOnce 钉住 R26:SIP002 的 userinfo 有两种写法,
// 只有**明文**那种是百分号编码的 —— base64 解出来的 method/password 是原子的,
// 再解一次会把密码里恰好长成 `%XX` 的部分改掉(存进错误密码、连接失败、无诊断)。
// legacy 分支恒是 base64,同理。判据:同一逻辑输入的两条分支结果必须一致。
func TestSsPasswordsAreDecodedExactlyOnce(t *testing.T) {
	// 明文分支:百分号编码要解(这条不变)
	plain, err := ParseNodeURI("ss://aes-256-gcm:p%40ss@1.2.3.4:8388")
	if err != nil {
		t.Fatalf("plain: %v", err)
	}
	if plain.Password != "p@ss" {
		t.Fatalf("明文分支 password = %q, want p@ss", plain.Password)
	}
	// base64 的 userinfo:解出来的就是原子密码,不得再解一次
	b64ui := base64.StdEncoding.EncodeToString([]byte("aes-256-gcm:p%40ss"))
	enc, err := ParseNodeURI("ss://" + b64ui + "@1.2.3.4:8388")
	if err != nil {
		t.Fatalf("encoded userinfo: %v", err)
	}
	if enc.Password != "p%40ss" {
		t.Fatalf("base64 分支把密码二次百分号解码了: %q, want p%%40ss", enc.Password)
	}
	// legacy 分支(ss://base64(method:password@host:port))同样恒是 base64
	legacy := base64.StdEncoding.EncodeToString([]byte("aes-256-gcm:p%40ss@1.2.3.4:8388"))
	leg, err := ParseNodeURI("ss://" + legacy)
	if err != nil {
		t.Fatalf("legacy: %v", err)
	}
	if leg.Password != "p%40ss" || leg.Server != "1.2.3.4" || leg.ServerPort != 8388 {
		t.Fatalf("legacy 分支 = %+v, want password p%%40ss @1.2.3.4:8388", leg)
	}
}

func TestTrojanAndSs(t *testing.T) {
	tj, err := ParseNodeURI("trojan://pw@t.example:443#T")
	if err != nil {
		t.Fatalf("trojan: %v", err)
	}
	if tj.Type != "trojan" || tj.Password != "pw" || tj.Tag != "T" {
		t.Fatalf("trojan = %+v", tj)
	}
	if tj.TLS == nil || tj.TLS.Enabled == nil || !*tj.TLS.Enabled || tj.TLS.ServerName != "t.example" {
		t.Fatalf("trojan tls = %+v", tj.TLS)
	}
	if pw, _ := ParseNodeURI("trojan://p%40ss@t.example:443"); pw.Password != "p@ss" {
		t.Fatalf("trojan password decode = %q", pw.Password)
	}

	ss, err := ParseNodeURI("ss://aes-256-gcm:pw@1.2.3.4:8388#S")
	if err != nil {
		t.Fatalf("ss: %v", err)
	}
	if ss.Type != "shadowsocks" || ss.Method != "aes-256-gcm" || ss.Password != "pw" {
		t.Fatalf("ss = %+v", ss)
	}
	// SIP002 base64 userinfo 形态
	info := base64.StdEncoding.EncodeToString([]byte("aes-256-gcm:pw"))
	ss2, err := ParseNodeURI("ss://" + info + "@1.2.3.4:8388")
	if err != nil {
		t.Fatalf("ss b64 userinfo: %v", err)
	}
	if ss2.Method != "aes-256-gcm" || ss2.Password != "pw" {
		t.Fatalf("ss b64 userinfo = %+v", ss2)
	}
}

func TestHysteria2AndTuicAndAnytls(t *testing.T) {
	hy, err := ParseNodeURI("hysteria2://pw@h.example:443")
	if err != nil {
		t.Fatalf("hysteria2: %v", err)
	}
	if hy.Type != "hysteria2" || hy.Password != "pw" || hy.ServerPort != 443 {
		t.Fatalf("hysteria2 = %+v", hy)
	}
	if hy2, _ := ParseNodeURI("hy2://pw@h2.example:443"); hy2.Type != "hysteria2" {
		t.Fatalf("hy2 type = %q", hy2.Type)
	}
	// mport 必须展开成 "start:end" 区间，server_port 被删（两者不能同时出现）
	mp, err := ParseNodeURI("hysteria2://pw@h.example:443?mport=2000-3000")
	if err != nil {
		t.Fatalf("mport: %v", err)
	}
	if len(mp.ServerPorts) != 1 || mp.ServerPorts[0] != "2000:3000" || mp.ServerPort != 0 {
		t.Fatalf("mport = %+v", mp)
	}

	tu, err := ParseNodeURI("tuic://uuid-1:pw@t.example:443")
	if err != nil {
		t.Fatalf("tuic: %v", err)
	}
	if tu.Type != "tuic" || tu.UUID != "uuid-1" || tu.Password != "pw" {
		t.Fatalf("tuic = %+v", tu)
	}
	if tu.Extra["congestion_control"] != "bbr" || tu.Extra["udp_relay_mode"] != "native" {
		t.Fatalf("tuic extra = %+v", tu.Extra)
	}
	if tu.TLS == nil || len(tu.TLS.ALPN) != 1 || tu.TLS.ALPN[0] != "h3" {
		t.Fatalf("tuic tls = %+v", tu.TLS)
	}

	at, err := ParseNodeURI("anytls://pw@a.example:8443")
	if err != nil {
		t.Fatalf("anytls: %v", err)
	}
	if at.Type != "anytls" || at.Password != "pw" {
		t.Fatalf("anytls = %+v", at)
	}
}

func TestSshAndSocks(t *testing.T) {
	sh, err := ParseNodeURI("ssh://root@s.example")
	if err != nil {
		t.Fatalf("ssh: %v", err)
	}
	if sh.Type != "ssh" || sh.ServerPort != 22 || sh.Extra["user"] != "root" {
		t.Fatalf("ssh = %+v", sh)
	}
	if sh2, _ := ParseNodeURI("ssh://root:secret@s.example:2222"); sh2.Password != "secret" {
		t.Fatalf("ssh password = %q", sh2.Password)
	}

	sk, err := ParseNodeURI("socks://1.2.3.4:1080")
	if err != nil {
		t.Fatalf("socks: %v", err)
	}
	if sk.Type != "socks" || sk.ServerPort != 1080 || sk.Extra["version"] != "5" {
		t.Fatalf("socks = %+v", sk)
	}
	if sk2, _ := ParseNodeURI("socks5://u:p@1.2.3.4:1080"); sk2.Extra["username"] != "u" || sk2.Password != "p" {
		t.Fatalf("socks5 = %+v", sk2)
	}
}

func TestNameBlacklistDropsInfoNodes(t *testing.T) {
	// JS 语义（parse-links.js:519-520 与其测试 :457-466）：名字命中黑名单
	// **不丢节点**，退回 `协议://host:port` 回退 tag —— 这是 tag 拼法契约的一半；
	// 回退 tag（host）命中黑名单才整条丢弃。
	o, err := ParseNodeURI("vless://u@h.example:443#%E5%89%A9%E4%BD%99%E6%B5%81%E9%87%8F")
	if err != nil {
		t.Fatalf("blacklisted name must keep the node: %v", err)
	}
	if o.Tag != "vless:h.example:443" {
		t.Fatalf("tag = %q, want fallback", o.Tag)
	}
	// /i：大小写无关
	if e, _ := ParseNodeURI("vless://u@h.example:443#Expired%20Traffic"); e.Tag != "vless:h.example:443" {
		t.Fatalf("Expired Traffic tag = %q", e.Tag)
	}
	for _, name := range []string{"官网", "套餐", "获取订阅", "telegram.me", "t.me/x"} {
		if _, err := ParseNodeURI("vless://u@h.example:443#" + name); err != nil {
			t.Fatalf("name %q should fall back, got err %v", name, err)
		}
	}
	// 回退 tag 命中黑名单（host 带 expire）→ 整条被丢（tests :463）
	if _, err := ParseNodeURI("vless://u@expire.example:443"); err == nil {
		t.Fatal("host hitting the blacklist must drop the node")
	}
	links := ParseLinks("vless://u@expire.example:443\nvless://u@ok.example:443#ok")
	if len(links) != 1 || links[0].Tag != "ok" {
		t.Fatalf("ParseLinks = %+v", links)
	}
}

func TestTagDedupAndTruncation(t *testing.T) {
	// tests/parse-links.test.js:545-548 的 tag 拼法必须逐字不变
	cases := []struct{ uri, tag string }{
		{"vless://u@h.example:443", "vless:h.example:443"},
		{"hysteria2://pw@h.example:443", "hysteria2:h.example:443"},
		{"ssh://root@s.example", "ssh:s.example:22"},
		{"socks5://1.2.3.4:1080", "socks:1.2.3.4:1080"},
		// mport 节点用端口区间拼 tag，同主机不同区间不撞车（tests :334-340）
		{"hysteria2://pw@h.example:443?mport=2000-3000", "hysteria2:h.example:2000:3000"},
		{"hysteria2://pw@h.example:443?mport=2000-3000,4000", "hysteria2:h.example:2000:3000,4000:4000"},
	}
	for _, c := range cases {
		o, err := ParseNodeURI(c.uri)
		if err != nil {
			t.Fatalf("%s: %v", c.uri, err)
		}
		if o.Tag != c.tag {
			t.Fatalf("%s tag = %q, want %q", c.uri, o.Tag, c.tag)
		}
	}
	// 同指纹条目不去重：去重是 sub.Fetch 合并点的职责（src/sub.js:357 的
	// identityOf seen 集合），不是解析器的 —— JS parseLinks 对同一文本返回
	// 两条（差分 A1 钉住的契约）。
	dup := ParseLinks("vless://a@a.example:443\nvless://a@a.example:443")
	if len(dup) != 2 {
		t.Fatalf("parseLinks must not dedup, got %d entries", len(dup))
	}
	// unroutable 地址也不在解析器里剔：它住在订阅层 dropUnroutable
	// （src/sub.js:193），「链接能不能解析」和「该不该入池」是两件事。
	ur := ParseLinks("vless://u@127.0.0.1:443#local")
	if len(ur) != 1 {
		t.Fatalf("parseLinks must keep unroutable servers, got %d", len(ur))
	}
}

func TestUnknownQueryParamsLandInExtra(t *testing.T) {
	// 约束 8：sing-box 认识但本项目未建模的查询参数必须进 Extra 透传，
	// 丢弃它们节点会静默连不上且无日志线索。
	o, err := ParseNodeURI(`vless://u@h.example:443?security=tls&multiplex=%7B%22enabled%22%3Atrue%7D&domain_strategy=prefer_ipv4`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.Extra["multiplex"] != `{"enabled":true}` {
		t.Fatalf("multiplex = %v", o.Extra["multiplex"])
	}
	if o.Extra["domain_strategy"] != "prefer_ipv4" {
		t.Fatalf("domain_strategy = %v", o.Extra["domain_strategy"])
	}
	// sing-box 不认识的 v2ray 参数（encryption=none）照 JS 原样丢弃 ——
	// 收进配置会被严格解码 FATAL。
	if e, _ := ParseNodeURI(`vless://u@h.example:443?encryption=none&level=0`); len(e.Extra) != 0 {
		t.Fatalf("v2ray-only params must not leak into Extra: %+v", e.Extra)
	}
}

func TestBucketOfGroupsCountries(t *testing.T) {
	cases := []struct{ in, want string }{
		{"US", "US"}, {"JP", "JP"}, {"SG", "SG"},
		{"DE", "EU"}, {"FR", "EU"}, {"NL", "EU"}, {"GB", "EU"},
		{"ZZ", "OTHER"}, {"OTHER", "OTHER"}, {"", "OTHER"},
		{"de", "EU"}, // 大小写归一（JS toUpperCase）
	}
	for _, c := range cases {
		if got := BucketOf(c.in); got != c.want {
			t.Errorf("BucketOf(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestCountryOfReadsEmojiFlag(t *testing.T) {
	// 旗标是最后一级兜底（src/sub.js:113 的四级识别）
	if got := CountryOf("🇩🇪 德国 01"); got != "DE" {
		t.Fatalf("flag+keyword = %q, want DE", got)
	}
	if got := CountryOf("🇯🇵 node"); got != "JP" {
		t.Fatalf("flag-only = %q, want JP", got)
	}
	// 其余三级：括号英文名 → ISO 码 token → 中英文关键词
	if got := CountryOf("(Hong Kong) exit"); got != "HK" {
		t.Fatalf("paren name = %q", got)
	}
	if got := CountryOf("US-LA-02"); got != "US" {
		t.Fatalf("iso token = %q", got)
	}
	if got := CountryOf("🇨🇳 Example-Node - TW-Wuri"); got != "TW" {
		t.Fatalf("last segment after ' - ' = %q, want TW", got)
	}
	if got := CountryOf("香港 01"); got != "HK" {
		t.Fatalf("keyword = %q", got)
	}
	// 印度尼西亚不是印度（JS 负向断言 印度(?!尼)）
	if got := CountryOf("印度尼西亚雅加达"); got != "" {
		t.Fatalf("indonesia = %q, want empty", got)
	}
	if got := CountryOf("plain-node"); got != "" {
		t.Fatalf("no country = %q", got)
	}
	if got := BucketOf(CountryOf("plain-node")); got != "OTHER" {
		t.Fatalf("no country buckets to %q", got)
	}
}

func TestFilterByGroupsKeepsDirectOut(t *testing.T) {
	outs := []Outbound{
		{Tag: "🇺🇸 US-01", Type: "vless", Server: "a.example", ServerPort: 443},
		{Tag: "direct", Type: "direct"},
		{Tag: "unknown-node", Type: "vmess", Server: "b.example", ServerPort: 80},
	}
	// "KeepsDirectOut"：direct 被挡在池外 —— 这是 PROXY_TYPES 的类型职责，
	// 不是 IsUnroutableServer 的地址职责（direct 没有 server，地址判据对它
	// 没有意见）。
	got := FilterByGroups(outs, []string{"US"})
	if len(got) != 1 || got[0].Tag != "🇺🇸 US-01" {
		t.Fatalf("US filter = %+v", got)
	}
	// 无名节点只有选了 OTHER 才进（探测后由出口 IP 实测归桶）
	got2 := FilterByGroups(outs, []string{"OTHER"})
	if len(got2) != 1 || got2[0].Tag != "unknown-node" {
		t.Fatalf("OTHER filter = %+v", got2)
	}
	// 去重键是**配置指纹**而不是 tag(R21):实测 data/subs_cache.json 的 1246 个
	// 出站里有 7 个重复 tag 压掉了 84 个节点(disney_netflix_GB 76 个只留 1),
	// 而对同一批代理重算 IdentityOf 得到 1241 个不同身份 —— 它们是真不同的服务器。
	// JS 的 sub.js:146-153 同款按 tag 去重,所以这是双方共有的缺陷;管线其余去重点
	// (registry.Merge、sub.Fetch 的轮内去重)都用 IdentityOf,这里跟着改齐。
	dup := FilterByGroups([]Outbound{
		{Tag: "US", Type: "vless", Server: "a.example", ServerPort: 443},
		{Tag: "US", Type: "vless", Server: "b.example", ServerPort: 443},
	}, []string{"US"})
	if len(dup) != 2 {
		t.Fatalf("同名不同服务器的出站被压掉了: %+v", dup)
	}
	same := FilterByGroups([]Outbound{
		{Tag: "US", Type: "vless", Server: "a.example", ServerPort: 443},
		{Tag: "US-alias", Type: "vless", Server: "a.example", ServerPort: 443},
	}, []string{"US"})
	if len(same) != 1 {
		t.Fatalf("同一物理节点的别名没被折叠: %+v", same)
	}
}

// TestParseAnytlsAllowInsecureCamelCase 钉住 anytls 的跳过证书参数拼法:JS 版
// (src/parse-links.js:438)读 allowInsecure 驼峰 —— 与 tuic 的 allow_insecure
// 两个拼法并存。Go 版曾照抄 tuic 的下划线拼法,同一链接两版行为相反(任务 27
// A1 差分钉住的实错)。
func TestParseAnytlsAllowInsecureCamelCase(t *testing.T) {
	o, err := ParseNodeURI("anytls://pw@a.example.com:443?sni=s.example.com&allowInsecure=1#anytls")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.TLS == nil || o.TLS.Insecure == nil || !*o.TLS.Insecure {
		t.Fatalf("allowInsecure=1 must set tls.insecure, got %+v", o.TLS)
	}
	o2, _ := ParseNodeURI("anytls://pw@a.example.com:443?sni=s.example.com#anytls")
	if o2.TLS == nil || (o2.TLS.Insecure != nil && *o2.TLS.Insecure) {
		t.Fatalf("no allowInsecure must not set tls.insecure, got %+v", o2.TLS)
	}
}
