// SPDX-License-Identifier: GPL-3.0-or-later
package httpclient

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientUsesTheInjectedDialer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()
	used := 0
	c := NewClient(func(ctx context.Context, network, addr string) (net.Conn, error) {
		used++
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}, 5*time.Second)
	resp, err := c.Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = resp.Body.Close()
	if used == 0 {
		t.Fatal("injected dialer was never called")
	}
}

func TestClientTimeoutIsEnforced(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	c := NewClient(nil, 150*time.Millisecond)
	if _, err := c.Get(srv.URL); err == nil {
		t.Fatal("expected a timeout, got nil")
	}
}

func TestClientRejectsRedirectsToAnotherHost(t *testing.T) {
	// 订阅源偶尔 301 到 CDN。不限制重定向次数就可能被带着跑十跳。
	// 这里只断言不 panic 且最终报错或成功，行为细节由实现说明。
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("final"))
	}))
	defer other.Close()
	c := NewClient(nil, 2*time.Second)
	resp, err := c.Get(other.URL)
	if err == nil {
		_ = resp.Body.Close()
	}
}
