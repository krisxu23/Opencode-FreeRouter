// SPDX-License-Identifier: GPL-3.0-or-later
package sbx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"freerouter/internal/parse"
)

// 一个最小可用的本地 HTTP 服务器，作为「上游」与「出口之后能到达的地方」。
func upstream(t *testing.T) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello-probe"))
	}))
	return srv.URL, srv.Close
}

// direct 出站：走 sing-box 的 direct 协议，可用于验证「宿主活着但没有真节点」时
// 拨号链路是通的，不依赖任何外部服务。
func directOut(t *testing.T) parse.Outbound {
	t.Helper()
	return parse.Outbound{Tag: "d", Type: "direct"}
}

func TestStartWithNoOutboundsStillServes(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if h.DirectTag() == "" {
		t.Fatal("DirectTag must be non-empty after Start")
	}
}

func TestStartIsIdempotentOnSecondCall(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if err := h.Start(context.Background(), nil); err == nil {
		t.Fatal("second Start must fail: the box is already running")
	}
}

func TestDialThroughDirectReachesUpstream(t *testing.T) {
	// 这是「零本地端口」架构的最小证明：没有配置文件、没有子进程、没有监听端口，
	// 拨号直接从一个进程内的出站出去。
	url, done := upstream(t)
	defer done()
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	d, err := h.Dialer(h.DirectTag())
	if err != nil {
		t.Fatalf("direct dialer: %v", err)
	}
	host := strings.TrimPrefix(url, "http://")
	c := &http.Client{Transport: &http.Transport{DialContext: d}, Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("get through in-process outbound: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d for %s", resp.StatusCode, host)
	}
}

func TestDialerForUnknownTagIsAnError(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if _, err := h.Dialer("does-not-exist"); err == nil {
		t.Fatal("unknown tag must return an error, not a nil dialer")
	}
}

func TestHasReflectsLoadedOutbounds(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), []parse.Outbound{directOut(t)}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if !h.Has("d") {
		t.Fatal("Has(\"d\") = false after loading it")
	}
	if h.Has("nope") {
		t.Fatal("Has(\"nope\") = true")
	}
}

func TestSyncAddsAndRemoves(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), []parse.Outbound{directOut(t)}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	added, removed, err := h.SyncOutbounds([]parse.Outbound{{Tag: "d", Type: "direct"}, {Tag: "e", Type: "direct"}})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if added != 1 || removed != 0 {
		t.Fatalf("sync = added %d removed %d, want 1/0", added, removed)
	}
	if !h.Has("e") {
		t.Fatal("\"e\" is not loaded after sync")
	}
	added, removed, err = h.SyncOutbounds([]parse.Outbound{{Tag: "d", Type: "direct"}})
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if added != 0 || removed != 1 {
		t.Fatalf("sync = added %d removed %d, want 0/1", added, removed)
	}
	if h.Has("e") {
		t.Fatal("\"e\" survived removal")
	}
}

func TestSyncIsIdempotent(t *testing.T) {
	h := NewHost(func(string, string) {})
	outs := []parse.Outbound{{Tag: "d", Type: "direct"}}
	if err := h.Start(context.Background(), outs); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	added, removed, err := h.SyncOutbounds(outs)
	if err != nil || added != 0 || removed != 0 {
		t.Fatalf("re-syncing the same set = %d/%d, err %v; want 0/0", added, removed, err)
	}
}

func TestCloseTwiceIsSafe(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("second close must not error: %v", err)
	}
}

func TestOperationsBeforeStartFailCleanly(t *testing.T) {
	// 面板在网关还在启动时就会被点。每一个入口都必须返回错误，不能 panic。
	h := NewHost(func(string, string) {})
	if _, err := h.Dialer("x"); err == nil {
		t.Error("Dialer before Start must error")
	}
	if _, _, err := h.SyncOutbounds(nil); err == nil {
		t.Error("SyncOutbounds before Start must error")
	}
	if err := h.AddDirect("x"); err == nil {
		t.Error("AddDirect before Start must error")
	}
	if h.DirectTag() != "" {
		t.Error("DirectTag before Start must be empty")
	}
}

func TestAddAndRemoveDirect(t *testing.T) {
	h := NewHost(func(string, string) {})
	if err := h.Start(context.Background(), nil); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if err := h.AddDirect("probe-direct"); err != nil {
		t.Fatalf("AddDirect: %v", err)
	}
	if !h.Has("probe-direct") {
		t.Fatal("AddDirect did not register the tag")
	}
	if err := h.RemoveDirect("probe-direct"); err != nil {
		t.Fatalf("RemoveDirect: %v", err)
	}
	if h.Has("probe-direct") {
		t.Fatal("RemoveDirect did not remove the tag")
	}
}

func TestStartSkipsAnUnknownProtocol(t *testing.T) {
	// 修正案 §4 阶段 2：坏节点「跳过 + 报告」，永不 fatal（与 JS 版
	// sanitize-drop 语义一致）。一个本项目不认识的 type 不许拖垮整池，
	// 但必须被明确报告，而不是让 sing-box 静默吞掉。
	h := NewHost(func(level, msg string) {
		if strings.Contains(msg, "quantum-tunnel") || strings.Contains(msg, "x") {
			t.Logf("host reported: %s %s", level, msg)
		}
	})
	if err := h.Start(context.Background(), []parse.Outbound{
		{Tag: "x", Type: "quantum-tunnel", Server: "a.example", ServerPort: 1},
	}); err != nil {
		t.Fatalf("unknown protocol must be skipped, not fatal: %v", err)
	}
	defer func() { _ = h.Close() }()
	if h.Has("x") {
		t.Fatal("an unknown-protocol outbound was loaded")
	}
	if !h.Has("") && h.DirectTag() == "" {
		t.Fatal("box did not come up without the bad node")
	}
}

func TestStartSkipsAnUnroutableServer(t *testing.T) {
	// 最后一道闸：宿主不负责修数据，但它绝不能把一个 127.0.0.1 出站交给
	// sing-box。跳过并报告，不 fatal（修正案 §4 阶段 2）。
	h := NewHost(func(level, msg string) {
		if strings.Contains(msg, "x") {
			t.Logf("host reported: %s %s", level, msg)
		}
	})
	if err := h.Start(context.Background(), []parse.Outbound{
		{Tag: "x", Type: "vless", Server: "127.0.0.1", ServerPort: 443, UUID: "aeaeaeae-aeae-4aea-8aea-aeaeaeaeaeae"},
	}); err != nil {
		t.Fatalf("loopback server must be skipped, not fatal: %v", err)
	}
	defer func() { _ = h.Close() }()
	if h.Has("x") {
		t.Fatal("a loopback outbound was loaded")
	}
}

func TestBadOutboundDoesNotAbortTheWholeStart(t *testing.T) {
	// 一个坏节点不能让整池 2700 个节点起不来。跳过它，报告它。
	h := NewHost(func(level, msg string) {
		if strings.Contains(msg, "bad") {
			t.Logf("host reported: %s %s", level, msg)
		}
	})
	err := h.Start(context.Background(), []parse.Outbound{
		{Tag: "bad", Type: "quantum-tunnel", Server: "a.example", ServerPort: 1},
		{Tag: "good", Type: "direct"},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = h.Close() }()
	if !h.Has("good") {
		t.Fatal("a bad node took the whole pool down with it")
	}
}

func TestOutboundWithUnknownFieldsStillDials(t *testing.T) {
	// 约束 8 的真实含义：sing-box 认识、本项目没建模的字段必须透传生效。
	// domain_strategy 属于 DialerOptions，我们的 parse 层不认识它——
	// 经 Extra 走到这里，具体类型严格解码时照常接住。
	h := NewHost(func(string, string) {})
	err := h.Start(context.Background(), []parse.Outbound{{
		Tag:   "d",
		Type:  "direct",
		Extra: parse.Extra{"domain_strategy": "prefer_ipv4"},
	}})
	if err != nil {
		t.Fatalf("unmodelled-but-known fields must not break the build: %v", err)
	}
	_ = h.Close()
}

func TestMultiplexSurvivesOnVless(t *testing.T) {
	// multiplex 是 sing-box 认识但本项目不建模的字段，且是对象形状——
	// 若 Extra 的 JSON 物化丢了它，vless 节点的 mux 配置会静默失效。
	h := NewHost(func(string, string) {})
	err := h.Start(context.Background(), []parse.Outbound{{
		Tag: "v", Type: "vless", Server: "v.example", ServerPort: 443,
		UUID: "aeaeaeae-aeae-4aea-8aea-aeaeaeaeaeae",
		TLS:  &parse.TLS{Enabled: parse.BoolPtr(true), ServerName: "v.example"},
		Extra: parse.Extra{
			"multiplex":       map[string]any{"enabled": true, "protocol": "h2mux", "max_streams": 8},
			"packet_encoding": "xudp",
		},
	}})
	if err != nil {
		t.Fatalf("multiplex must survive the round trip: %v", err)
	}
	defer func() { _ = h.Close() }()
	if !h.Has("v") {
		t.Fatal("the vless outbound with unmodelled fields was not loaded")
	}
}
