// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package tray

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestMenuLoopHandlesEveryItem 是菜单点击处理的钉。
//
// 2026-10-04 上一版这里把整个 for-select 直接写在 systray.Run 的 onReady
// 回调里，并加了 `defer close(done)` 做 goroutine 收口 —— onReady 回调建完菜单
// 项就**立刻返回**，于是 done 在开表几微秒后关闭，菜单 goroutine 当场退出：
// Open panel 打不开面板、Quit 不触发退出，用户只能去任务管理器强杀。没有桌面
// 会话就看不见这个形状，所以菜单处理必须是可单测的独立函数。
func TestMenuLoopHandlesEveryItem(t *testing.T) {
	done := make(chan struct{})
	openCh := make(chan struct{}, 1)
	reloadCh := make(chan struct{}, 1)
	quitCh := make(chan struct{}, 1)

	opened := make(chan struct{}, 1)
	reloaded := make(chan struct{}, 1)
	quitCalled := make(chan struct{}, 1)
	o := Options{
		OpenPanel: func() { opened <- struct{}{} },
		Reload:    func() { reloaded <- struct{}{} },
		Quit:      func() { quitCalled <- struct{}{} },
	}
	loopDone := make(chan struct{})
	go func() { menuLoop(done, openCh, reloadCh, quitCh, o, func() {}); close(loopDone) }()

	openCh <- struct{}{}
	select {
	case <-opened:
	case <-time.After(2 * time.Second):
		t.Fatal("点了 Open panel 没有调 OpenPanel")
	}
	reloadCh <- struct{}{}
	select {
	case <-reloaded:
	case <-time.After(2 * time.Second):
		t.Fatal("点了 Reload 没有调 Reload")
	}

	// done 关闭必须让循环退出（否则托盘退出后留一个永不退的 goroutine）。
	close(done)
	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("done 关闭后菜单循环没有退出")
	}
	// Quit 那条单独再验一次：它会返回循环，得起一条新的。
	done2 := make(chan struct{})
	quitCh2 := make(chan struct{}, 1)
	quitCalled2 := make(chan struct{}, 1)
	loopDone2 := make(chan struct{})
	go func() {
		menuLoop(done2, make(chan struct{}), make(chan struct{}), quitCh2,
			Options{Quit: func() { quitCalled2 <- struct{}{} }}, func() {})
		close(loopDone2)
	}()
	quitCh2 <- struct{}{}
	select {
	case <-quitCalled2:
	case <-time.After(2 * time.Second):
		t.Fatal("点了 Quit 没有调 Quit")
	}
	select {
	case <-loopDone2:
	case <-time.After(2 * time.Second):
		t.Fatal("Quit 之后菜单循环应返回")
	}
	close(done2)
}

// TestDoneIsOwnedByRunNotByTheOnReadyCallback 钉住上一版那个致命回归的**结构**
// 本身：done 的声明与 `defer close(done)` 必须在 systray.Run **外面**。
//
// 单元测试看不见「onReady 回调会立刻返回」这件事（无桌面会话进不去
// systray.Run），但这个回归的形状是可以静态判定的 —— 与
// internal/check 的探测摘要正则比对同款做法：本包的托盘路径全废、且没有任何
// 现有测试会红，所以只能把「done 属于 Run」这条归属钉成断言。
func TestDoneIsOwnedByRunNotByTheOnReadyCallback(t *testing.T) {
	raw, err := os.ReadFile("tray.go")
	if err != nil {
		t.Fatalf("read tray.go: %v", err)
	}
	src := string(raw)
	decl := strings.Index(src, "done := make(chan struct{})")
	run := strings.Index(src, "systray.Run(func()")
	if decl < 0 || run < 0 {
		t.Fatalf("结构变了：decl=%d run=%d（找不到 done 的声明或 systray.Run）", decl, run)
	}
	if decl > run {
		t.Fatal("done 声明在 systray.Run 之后 —— 它属于 onReady 回调，" +
			"`defer close(done)` 会在回调返回瞬间掐掉菜单 goroutine：" +
			"Open panel 打不开面板、Quit 不退出，只能任务管理器强杀")
	}
	// 光靠位置还不够：必须确认 defer close(done) 也在 Run 的作用域里。
	// 取 Run 函数体（从 `func Run(` 到下一个顶层 `func `）看它。
	start := strings.Index(src, "func Run(o Options) {")
	end := strings.Index(src[start+1:], "\nfunc ")
	if start < 0 || end < 0 {
		t.Fatal("找不到 Run 的函数体边界")
	}
	body := src[start : start+end]
	if !strings.Contains(body, "defer close(done)") {
		t.Fatal("Run 里没有 defer close(done)：托盘退出后菜单 goroutine 会泄漏")
	}
}

func TestPanelPortFallsBackTo3458(t *testing.T) {
	// 空目录连 settings.json 都没有:launcher/main.go:93-104 的口径是安静地
	// 用 3458,而不是报错 —— 托盘必须在设置文件缺失时也守得住单实例。
	if got := PanelPort(t.TempDir()); got != 3458 {
		t.Fatalf("PanelPort(空 root) = %d, want 3458", got)
	}
}

func TestPanelPortReadsSettings(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	// 键名 panelPort 与 launcher(及 app.Settings 的 json tag)逐字一致。
	if err := os.WriteFile(filepath.Join(root, "data", "settings.json"), []byte(`{"panelPort":3459}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := PanelPort(root); got != 3459 {
		t.Fatalf("PanelPort = %d, want 3459", got)
	}
}

func TestPanelPortIgnoresGarbageSettings(t *testing.T) {
	// 坏 JSON / 端口为 0 / 端口为负,三种「不可用值」都必须回到 3458,
	// 不 panic、不退出 —— launcher 的原始容错就是这一条 if 链。
	for name, content := range map[string]string{
		"not json":      "not json",
		"zero port":     `{"panelPort":0}`,
		"negative port": `{"panelPort":-5}`,
	} {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "data", "settings.json"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if got := PanelPort(root); got != 3458 {
			t.Errorf("%s: PanelPort = %d, want 3458", name, got)
		}
	}
}

func TestPanelUpIsFalseWhenNothingListens(t *testing.T) {
	// 先借 127.0.0.1:0 讨一个确定空闲的端口,再放手:比硬编码端口可靠。
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	start := time.Now()
	if PanelUp(port) {
		t.Fatalf("PanelUp(%d) = true, 该端口此刻无人监听", port)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("PanelUp 花了 %v: 必须在 300ms 拨号超时的量级内返回", elapsed)
	}
}

func TestPanelUpIsTrueAgainstALiveListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port
	if !PanelUp(port) {
		t.Fatalf("PanelUp(%d) = false, 监听中的端口必须判为在跑", port)
	}
}
