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
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"freerouter/internal/app"
	"freerouter/internal/logger"
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
	//
	// 启动参数(全部可选,缺省即现行行为):
	//   --data-dir  覆盖数据目录(等价 FREEROUTER_DATA,显式参数优先)。
	//   --version   打印 ldflags 注入的版本号即退,不启动网关(CI 烟测/排障用)。
	//   --pprof-port 本机回环诊断口(如 6060):只绑 127.0.0.1 的 pprof,而不是
	//               全网段暴露 —— 内存/CPU 剖析按需开,常态零监听。
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "data directory (overrides FREEROUTER_DATA)")
	showVersion := fs.Bool("version", false, "print version and exit")
	pprofPort := fs.Int("pprof-port", 0, "loopback pprof port (0 = disabled)")
	if err := fs.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Stderr.WriteString("freerouter: " + err.Error() + "\n")
		os.Exit(2)
	}
	if *showVersion {
		fmt.Println(app.Version)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := strings.TrimSpace(*dataDir)
	if root == "" {
		var err error
		root, err = app.RootDir()
		if err != nil {
			// The logger needs the data dir, and the failure may be that the data
			// dir is unwritable: one line on stderr is the only honest channel.
			os.Stderr.WriteString("freerouter: " + err.Error() + "\n")
			os.Exit(1)
		}
	} else {
		if abs, err := filepath.Abs(root); err != nil {
			os.Stderr.WriteString("freerouter: --data-dir 不是可用路径: " + err.Error() + "\n")
			os.Exit(1)
		} else {
			root = filepath.Clean(abs)
		}
		if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
			os.Stderr.WriteString("freerouter: --data-dir 不可写: " + err.Error() + "\n")
			os.Exit(1)
		}
	}
	if *pprofPort > 0 {
		startLoopbackPprof(*pprofPort)
	}
	if err := run(ctx, stop, root); err != nil {
		os.Stderr.WriteString("freerouter: " + err.Error() + "\n")
		os.Exit(1)
	}
}

// startLoopbackPprof 按需开 pprof:只绑 127.0.0.1,端口非法(<=0/>65535)直接
// 拒绝,不静默钳制 —— 排障开关的误配要响亮失败,而不是开在一个意外的口上。
func startLoopbackPprof(port int) {
	if port <= 0 || port > 65535 {
		os.Stderr.WriteString("freerouter: --pprof-port 非法: " + strconv.Itoa(port) + "\n")
		os.Exit(2)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		os.Stderr.WriteString("freerouter: pprof 监听失败: " + err.Error() + "\n")
		os.Exit(1)
	}
	go func() {
		// DefaultServeMux 已被 net/http/pprof 注册;这里只服务回环监听。
		_ = http.Serve(ln, nil)
	}()
	logger.Warn(fmt.Sprintf("[main] pprof 已在 127.0.0.1:%d 开启(仅回环)", port))
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

	// W11:信号桥。signal.NotifyContext 的 ctx 此前只被定时器与托盘回调消费
	// —— tray.Run 完全不看 ctx,Ctrl+C/SIGTERM 之后探测/重建/限额三个循环
	// 退场,但托盘、两个 HTTP server 与 sing-box 全部存活,Shutdown 永远不
	// 执行:进程变成「维护冻结」的僵尸(转发还通,池子却无人补充,关停路径
	// 的落盘与关 sing-box 一概不做)。桥回托盘的退出路径:先 Shutdown(与
	// 托盘「退出」同一收尾,自身幂等,与并发点击 Quit 亦安全),再 Quit 让
	// Run 返回、main 正常收场。
	go func() {
		<-ctx.Done()
		_ = parts.Shutdown(context.Background())
		// Quit 需要托盘已完成 systray.Register(tray.Quit 内部自会判),但
		// 「还没就绪」有两种终局:托盘马上起来(等到 Ready 再 Quit),或者
		// 信号来在 Load 完成之前、Run 根本还没被调到(等 30s 后自行退进程
		// —— Shutdown 已经跑完,没有可丢的东西)。没有这个兜底,开机头几秒
		// 的 Ctrl+C 会留下一个托盘永远起不来的进程。
		select {
		case <-tray.Ready():
			tray.Quit()
		case <-time.After(30 * time.Second):
			logger.Warn("[main] 托盘 30s 内未就绪,信号路径直接退出")
			os.Exit(0)
		}
	}()

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
		// O15:Reload 最坏 72s+(3 出口×20s+直连 12s,还有补偿重试链),同步
		// 跑在菜单回调 goroutine 里会把托盘菜单卡死一整段。后台跑 + 日志
		// 反馈;Rebuild 自带重建互斥,连点安全。ctx 取消(正在退出)时的
		// 失败静默 —— 那是退出,不是故障。
		Reload: func() {
			go func() {
				if err := gw.Reload(ctx); err != nil && ctx.Err() == nil {
					logger.Warn(fmt.Sprintf("[tray] 重启网关失败: %v", err))
				}
			}()
		},
		Quit: func() {
			stop()
			_ = gw.Shutdown(context.Background())
		},
	}
}
