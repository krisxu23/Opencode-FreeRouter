// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Command freerouter is the gateway, the proxy core and the console in one
// process. sing-box is a library here, not a child process: there is no second
// binary to ship, no generated config file, and no per-node port to hand out.
// Compared with launcher/main.go this is a semantic inversion (plan task
// 25.1): the tray no longer spawns a node child, so the job object, the
// taskkill stop and launcherLog all disappear — Shutdown(ctx) plus the
// logger's own rotation do that work in-process.
package main

import (
	"context"
	_ "embed"
	"os"
	"os/signal"
	"syscall"

	"freerouter/internal/app"
	"freerouter/internal/tray"
)

//go:embed icon.ico
var iconBytes []byte

// loadApp and openBrowser are variables, not calls, so the single-instance
// test can replace them and assert that a second launch touches neither the
// data dir nor a browser. TestSingleInstanceSkipsAppLoad depends on it.
var (
	loadApp     = app.Load
	openBrowser = tray.OpenBrowser
)

func main() {
	// SIGTERM matters as much as SIGINT: the tray ends the process with an
	// exit code, and a hard exit skips every defer below — which is why every
	// store persists synchronously on its own writes rather than only on
	// shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root, err := app.RootDir()
	if err != nil {
		// The logger needs the data dir, and the failure may be that the data
		// dir is unwritable: one line on stderr is the only honest channel.
		os.Stderr.WriteString("freerouter: " + err.Error() + "\n")
		os.Exit(1)
	}
	if err := run(ctx, stop, root); err != nil {
		os.Stderr.WriteString("freerouter: " + err.Error() + "\n")
		os.Exit(1)
	}
}

// run is main's body once root is known. It returns when the tray quits or
// when the single-instance guard hands the launch over to the running panel;
// main owns every os.Exit so the testable path stays exit-free.
func run(ctx context.Context, stop context.CancelFunc, root string) error {
	// Single instance, decided before app.Load: that call binds the forward
	// port, so a second launch would either lose the port race or come up
	// half-built. A listening panel port means this program is already running,
	// so the right move is to show that panel and get out of the way
	// (launcher/main.go:144-151 逐字保留下来的唯一行为).
	if alreadyRunning(root) {
		return nil
	}

	parts, err := loadApp(root)
	if err != nil {
		return err
	}
	parts.StartTimers(ctx)

	// Run blocks until the tray quits. The callbacks are plain closures over
	// `parts` and `ctx` so that every one of them stays assertable in a test
	// without an interactive desktop (see internal/tray.Options).
	tray.Run(buildOptions(iconBytes, parts, ctx, stop))
	return nil
}

// alreadyRunning is the single-instance guard: the panel port listening means
// the gateway is up, and the only correct second-launch action is to open that
// panel and exit. It must read the same settings.json app.Load will read, or
// the port it probes could diverge from the port the panel actually binds
// (see tray.PanelPort for the path ruling).
func alreadyRunning(root string) bool {
	port := tray.PanelPort(root)
	if !tray.PanelUp(port) {
		return false
	}
	openBrowser(tray.PanelURL(port))
	return true
}

// gateway is the slice of *app.Parts the tray menu needs. Declared as an
// interface so buildOptions can be exercised with a counting fake — the real
// Parts would bind ports and start sing-box inside the test process.
type gateway interface {
	PanelURL() string
	Reload(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// buildOptions wires the tray's three menu actions to the running gateway.
//
// 「重启网关」的语义与 JS 版刻意不同（计划 25.3）：JS 是 stop()+start() 真重启
// 进程、端口全断再占；Go 版调 parts.Reload —— 重读设置、热插出站、刷新目录，
// 转发端口不断、在途连接不丢，sing-box 的热插已经能做到「换一批出口」。
// 「退出」先 stop() 取消 ctx 再 Shutdown：信号源与定时器先退场，然后监听、
// 落盘、关 sing-box 依次收尾；Shutdown 自身幂等，退出后进程自然结束。
func buildOptions(icon []byte, gw gateway, ctx context.Context, stop context.CancelFunc) tray.Options {
	return tray.Options{
		Icon:      icon,
		Title:     "FreeRouter",
		Tooltip:   "FreeRouter — 免费模型网关",
		PanelURL:  gw.PanelURL,
		OpenPanel: func() { openBrowser(gw.PanelURL()) },
		Reload:    func() { _ = gw.Reload(ctx) },
		Quit: func() {
			stop()
			_ = gw.Shutdown(context.Background())
		},
	}
}
