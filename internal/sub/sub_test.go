// SPDX-License-Identifier: GPL-3.0-or-later
package sub

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func link(n int) string {
	return "vless://00000000-0000-4000-8000-00000000000" + string(rune('0'+n%10)) +
		"@n" + string(rune('a'+n)) + ".example:443?type=ws&security=tls#node" + string(rune('a'+n))
}

// TestBrokenClashSubscriptionReportsTheRealReason 钉住 R25:ParseClashYAML 的
// err 过去被丢掉 —— 一份带重复键的 Clash 订阅(整个文件认输)落到 ParseLinks
// (没有 ://)返回 nil,运维只看到「没有识别出任何节点（格式不受支持？）」,
// 真因(哪个格式、为什么坏)完全不可见。按顺序回落是对的,但**为什么这一种格式
// 认输**必须跟着上来。
func TestBrokenClashSubscriptionReportsTheRealReason(t *testing.T) {
	body := "proxies:\n  - name: a\n    type: ss\n    server: 1.2.3.4\n    port: 8388\n" +
		"proxies:\n  - name: b\n    type: ss\n    server: 5.6.7.8\n    port: 8388\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()
	res, err := Fetch(context.Background(), []string{srv.URL}, nil)
	if err == nil {
		t.Fatal("一个节点都没认出来的订阅必须是错误")
	}
	if len(res.Details) != 1 || res.Details[0].OK {
		t.Fatalf("details = %+v, want 一条失败明细", res.Details)
	}
	msg := res.Details[0].Error
	if !strings.Contains(msg, "Clash") || !strings.Contains(msg, "already defined") {
		t.Fatalf("Detail.Error = %q, want 它带上「哪个格式坏、为什么」", msg)
	}
}

func TestFetchReturnsEverythingWhenAllSourcesWork(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(link(0) + "\n" + link(1)))
	}))
	defer srv.Close()
	res, err := Fetch(context.Background(), []string{srv.URL}, nil)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(res.Outbounds) != 2 {
		t.Fatalf("outbounds = %d, want 2", len(res.Outbounds))
	}
	if len(res.Details) != 1 || !res.Details[0].OK || res.Details[0].Nodes != 2 {
		t.Fatalf("details = %+v", res.Details)
	}
}

func TestPartialFailureIsNotAnError(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(link(0)))
	}))
	defer good.Close()
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	res, err := Fetch(context.Background(), []string{closedURL, good.URL}, nil)
	if err != nil {
		t.Fatalf("one good source must not be an error: %v", err)
	}
	if len(res.Outbounds) != 1 {
		t.Fatalf("outbounds = %d, want 1", len(res.Outbounds))
	}
	failed := 0
	for _, d := range res.Details {
		if !d.OK {
			failed++
			if d.Error == "" {
				t.Error("failed source must carry an error string")
			}
		}
	}
	if failed != 1 {
		t.Fatalf("failed count = %d, want 1", failed)
	}
	// 九审(2026-10-05 清场事故):「整体成功」与「全员到齐」是两件事。上层的
	// 差集删只敢在 SourcesOK == SourcesTotal 时执行 —— 这里钉住字段本身。
	if res.SourcesOK != 1 || res.SourcesTotal != 2 {
		t.Fatalf("sources = %d/%d, want 1/2(部分失败时上层必须能看到名单残缺)", res.SourcesOK, res.SourcesTotal)
	}
}

func TestSourcesCountFullSuccess(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(link(0)))
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(link(1)))
	}))
	defer b.Close()
	res, err := Fetch(context.Background(), []string{a.URL, b.URL}, nil)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if res.SourcesOK != 2 || res.SourcesTotal != 2 {
		t.Fatalf("sources = %d/%d, want 2/2(全成功才允许差集删)", res.SourcesOK, res.SourcesTotal)
	}
}

func TestAllSourcesFailingIsAnError(t *testing.T) {
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	u := closed.URL
	closed.Close()
	if _, err := Fetch(context.Background(), []string{u}, nil); err == nil {
		t.Fatal("all sources failing must return an error")
	}
}

func TestFetchRetriesFailedSourcesThroughExits(t *testing.T) {
	// 直连永远 502；只有经出口才成功 —— 模拟「源站只对某些出口可达」，
	// 这正是 2026-09-28 那次把 SUB_RETRY_EXITS 从 3 提到 12 的场景。
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Marker") == "" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(link(0)))
	}))
	defer srv.Close()

	var used int32
	res, err := Fetch(context.Background(), []string{srv.URL}, []Exit{{
		Name: "exit-a",
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&used, 1)
			d := &net.Dialer{}
			// 出口的"身份"用一个固定 marker 头表达：真实出口靠不同出口 IP 生效，
			// 这里只需要证明"第二轮确实走了 Dial 而不是直连"。
			conn, err := d.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return conn, nil
		},
		Header: map[string]string{"X-Marker": "1"},
	}})
	if err != nil {
		t.Fatalf("fetch through an exit must succeed: %v", err)
	}
	if len(res.Outbounds) != 1 {
		t.Fatalf("outbounds = %d, want 1", len(res.Outbounds))
	}
	if atomic.LoadInt32(&used) == 0 {
		t.Fatal("the exit dialer was never used")
	}
}

// TestParseSubscriptionBodyExportedMatchesPipeline 钉住导出面:差分验收
// (difftest)从包外喂进来的必须是与 fetchOne 同一条生产链——含
// dropUnroutable——而不是各格式解析器的裸输出。用一条 Clash YAML 带
// 127.0.0.1 假节点验证过滤确实在链上(src/sub.js parseSubscriptionBody 同款)。
// TestOversizedSubscriptionBodyIsRejected 是 R3 的钉子:订阅源由用户填,它
// 可以是任何一个被攻陷或坏掉的服务器。没有上限的 io.ReadAll 会把整个响应体
// 拉进内存 —— 一个恶意源就能把网关 OOM 掉。
//
// 断言的是"报错"而不是"截断":半份订阅文本会被解析成"格式不受支持",
// 把故障指向错误的方向。
func TestOversizedSubscriptionBodyIsRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain")
		// 上限 +1 字节:恰好等于上限是合法的,多一字节不是。
		_, _ = io.CopyN(w, zeroReader{}, maxSubBodyBytes+1)
	}))
	defer srv.Close()

	_, err := Fetch(context.Background(), []string{srv.URL}, nil)
	if err == nil {
		t.Fatal("超过上限的订阅体必须失败,不能默默吃下整份响应")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("错误文案要能归因到上限: %v", err)
	}
}

// zeroReader 产出无限个 'a'。用生成器而不是 strings.Repeat,避免测试自己先
// 分配一份 32MB 的字符串 —— 那样测的就不是被测代码的内存了。
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

func TestParseSubscriptionBodyExportedMatchesPipeline(t *testing.T) {
	yamlText := `proxies:
  - name: real
    type: ss
    server: 203.0.113.10
    port: 8388
    cipher: aes-256-gcm
    password: pw
  - name: ad
    type: trojan
    server: 127.0.0.1
    port: 443
    password: pw
`
	got := ParseSubscriptionBody(yamlText)
	if len(got) != 1 || got[0].Tag != "real" {
		t.Fatalf("ParseSubscriptionBody = %+v, want only the routable node", got)
	}
	if len(parseSubscriptionBody(yamlText)) != 1 {
		t.Fatalf("exported wrapper diverged from the package pipeline")
	}
}
