// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"freerouter/internal/app"
)

// TestSingleInstanceSkipsAppLoad 钉住守卫的真正价值:第二个实例**不碰**网关
// —— 端口不抢、store 不写、订阅不拉。只断言 PanelUp 返回 true 证明不了
// app.Load 没被调用,所以 loadApp 要打桩:守卫生效时它一次都不能进。
func TestSingleInstanceSkipsAppLoad(t *testing.T) {
	root := t.TempDir()

	// 面板已在监听 = 已有实例在跑。用随机端口 + 对应的 settings.json,
	// 走真实的 PanelPort→PanelUp 链路,而不是桩掉守卫本身。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	settings := []byte(`{"panelPort":` + strconv.Itoa(port) + `}`)
	if err := os.WriteFile(filepath.Join(dataDir, "settings.json"), settings, 0o644); err != nil {
		t.Fatal(err)
	}

	loadCalls := 0
	origLoad := loadApp
	loadApp = func(string) (*app.Parts, error) {
		loadCalls++
		return nil, errors.New("app.Load must not run for a second launch")
	}
	defer func() { loadApp = origLoad }()

	openCalls := 0
	var openedURL string
	origOpen := openBrowser
	openBrowser = func(url string) {
		openCalls++
		openedURL = url
	}
	defer func() { openBrowser = origOpen }()

	if err := run(context.Background(), func() {}, root); err != nil {
		t.Fatalf("run: %v", err)
	}
	if loadCalls != 0 {
		t.Fatalf("loadApp 被调用了 %d 次: 守卫生效时必须是 0 次", loadCalls)
	}
	if openCalls != 1 {
		t.Fatalf("openBrowser 被调用了 %d 次: want 恰 1 次", openCalls)
	}
	if want := "http://127.0.0.1:" + strconv.Itoa(port); openedURL != want {
		t.Fatalf("打开的 URL = %q, want %q", openedURL, want)
	}
}

// fakeGateway 替身 *app.Parts:退出接线的三件事(取消 ctx、Shutdown 恰一次、
// 顺序先 cancel 后落盘)都能在无桌面环境断言,不必真的点托盘菜单。
type fakeGateway struct {
	shutdowns     int
	ctxDoneAtShut bool
	isDone        func() bool // Shutdown 执行瞬间查询外层 ctx 是否已 Done
}

func (f *fakeGateway) PanelURL() string { return "http://127.0.0.1:3458/" }

func (f *fakeGateway) Reload(context.Context) error { return nil }

func (f *fakeGateway) Shutdown(context.Context) error {
	f.shutdowns++
	if f.isDone != nil {
		f.ctxDoneAtShut = f.isDone()
	}
	return nil
}

func TestQuitCancelsContextThenShutsDown(t *testing.T) {
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	gw := &fakeGateway{isDone: func() bool { return ctx.Err() != nil }}
	opts := buildOptions(iconBytes, gw, ctx, stop)

	opts.Quit()

	if ctx.Err() == nil {
		t.Fatal("点退出之后 ctx 必须已是 Done(信号源停止、定时器随之退场)")
	}
	if gw.shutdowns != 1 {
		t.Fatalf("Shutdown 被调用 %d 次, want 1", gw.shutdowns)
	}
	if !gw.ctxDoneAtShut {
		t.Fatal("Shutdown 执行时 ctx 还没被取消:顺序必须是先 stop 再 Shutdown")
	}
}

func TestQuitShutsDownExactlyOnce(t *testing.T) {
	// 计划 25.2 的疑虑:退出会不会把 Shutdown 调两次?答案必须是否 ——
	// Quit 回调只调一次;之后的任何清理靠 Parts.Shutdown 自己的 sync.Once 幂等。
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	gw := &fakeGateway{}
	opts := buildOptions(iconBytes, gw, ctx, stop)

	opts.Quit()

	if gw.shutdowns != 1 {
		t.Fatalf("一次退出触发了 %d 次 Shutdown, want 恰 1 次", gw.shutdowns)
	}
}
