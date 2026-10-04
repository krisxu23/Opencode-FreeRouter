// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package tray owns the Windows notification icon and the single-instance
// guard. It is the only package in the module allowed to import
// fyne.io/systray: every other layer reaches the tray through the callbacks
// in Options, so a headless environment can still exercise the wiring.
//
// 语义反转的边界在这里收口（计划任务 25.1）：launcher/main.go 的模型是
// 「托盘壳拉起 node 子进程，作业对象保证子进程跟自己死」。Go 版里 sing-box
// 是本进程内的库，没有子进程可挂接，所以作业对象、taskkill、nodePath、
// launcherLog 全部不搬运 —— 这个包只保留三件与进程模型无关的事：面板端口的
// 读取口径、单实例守卫的两个原语（PanelPort/PanelUp）、以及把三档菜单动作
// 表达成普通回调的 Run。
package tray

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"fyne.io/systray"
)

// ready 在 systray 完成 Register/初始化之后闭合。信号桥(main.go)必须等到
// 它之后才能调 Quit —— fyne.io/systray 的 Quit 在 Register 之前会解引用
// 尚未创建的窗口(nil 函数 panic);开机头几秒收到 Ctrl+C 恰好命中这个窗口。
var (
	readyOnce sync.Once
	ready     = make(chan struct{})
	quitOnce  sync.Once
)

// Ready 在托盘完成初始化后闭合。Quit 之前先等它(或超时),见 main.go。
func Ready() <-chan struct{} { return ready }

// Quit 退出托盘消息循环。等 Ready 之后才真正调 systray.Quit:未就绪时的
// 调用是 no-op(信号桥会超时兜底自行退出进程),就绪后幂等。
func Quit() {
	select {
	case <-ready:
	default:
		return // 托盘还没起来:本次不可能是「退出一个活着的托盘」,交给调用方超时兜底
	}
	quitOnce.Do(func() { systray.Quit() })
}

// defaultPanelPort 与 src/store.js SETTINGS_INITIAL 的 panelPort、
// app.defaultSettings 的 PanelPort 三处一致：设置文件缺席时，守卫探测的
// 端口必须仍是 app.Load 将要监听的那个。
const defaultPanelPort = 3458

// dialTimeout 是 PanelUp 的拨号超时。launcher/main.go:157-164 原本是无超时
// 的 net.Dial：对 127.0.0.1 的拒绝连接是瞬时的，但托盘启动路径上任何一次
// 意外挂起都会让第二个实例卡在黑窗口里，所以计划 25.1 裁定加 300ms 上限。
const dialTimeout = 300 * time.Millisecond

// PanelPort reads the panel port this root's gateway would listen on.
//
// 路径口径裁定（按 launcher 原文逐字对照）：launcher/main.go:93-104 读的是
// <exe 所在目录>/data/settings.json 的 panelPort —— 它的 exe 与 data/ 同级。
// Go 版没有 exeDir 概念：main 传进来的 root 是 app.RootDir() 的产物，而
// app.Load(root) 读的正是 <root>/data/settings.json（由
// TestLoadCreatesEveryStoreUnderRoot 钉死的布局）。守卫必须与 app.Load 看
// 同一个文件，否则两个读入口各拿一个端口，守卫说「没人跑」而面板其实换了
// 端口，双开抢端口的事故就回来了 —— 守卫存在的意义随之消失。因此这里是
// Join(root, "data", "settings.json")，launcher 的 exeDir 在 Go 版里由
// RootDir() 扮演。
//
// 容错照抄 launcher:93-104：读不到 / JSON 坏 / 端口 ≤ 0 ⇒ 3458，永不 panic、
// 永不退出。设置文件坏了不能拦住第二个实例做出「再开一个面板」的动作；
// 计划 25.1 也裁定「settings.json 坏 ⇒ 第二实例重新启动一次」与 JS 版完全
// 一致，不在此修。
func PanelPort(root string) int {
	port := defaultPanelPort
	if raw, err := os.ReadFile(filepath.Join(root, "data", "settings.json")); err == nil {
		var s struct {
			PanelPort int `json:"panelPort"`
		}
		if json.Unmarshal(raw, &s) == nil && s.PanelPort > 0 {
			port = s.PanelPort
		}
	}
	return port
}

// PanelURL renders the panel address. 照抄 launcher:106-108：不带尾斜杠，
// 浏览器自己会归一。
func PanelURL(port int) string {
	return "http://127.0.0.1:" + strconv.Itoa(port)
}

// PanelUp reports whether something already answers on the panel port.
// 照抄 launcher:157-164 的「能连上就算在跑」，仅按计划 25.1 加 300ms 超时。
func PanelUp(port int) bool {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), dialTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// OpenBrowser opens the panel in whatever the OS hands out.
// 照抄 launcher:166-168：rundll32 url.dll,FileProtocolHandler 是 Windows 上
// 最不挑环境的开浏览器方式 —— 不依赖默认浏览器注册表形状，也不引入新依赖。
// 错误被刻意吞掉：开不开得了浏览器都不该影响托盘自身的生命周期。
func OpenBrowser(url string) {
	// Start 之后必须 Wait（或 Release）：不回收的子进程句柄按 PID 计,
	// 进程活着就一直占一个 —— 每点一次「Open panel」漏一个,托盘常驻几周的
	// 实例会累积到进程句柄上限（默认 16 万量级,但是无界泄漏）。Wait 放在
	// 自己的 goroutine 里：rundll32 通常几十毫秒就退,但它也可能因为
	// 浏览器迟迟不接管而挂住,而这里不该为此阻塞调用方（菜单点击处理器）。
	if cmd := exec.Command("rundll32", "url.dll,FileProtocolHandler", url); cmd.Start() == nil {
		go func() { _ = cmd.Wait() }()
	}
}

// Options is everything Run needs from the outside world.
//
// 为什么放回调而不是 systray 的 *MenuItem（计划 25.2）：systray 的菜单句柄
// 只在 systray.Run 的 onReady 回调里创建，测试环境（无交互桌面）根本进不去。
// 把三档动作表达成三个普通 func()，Run 内部再把它们接到 systray.AddMenuItem
// 上 —— 于是「点退出先 cancel 再 Shutdown」这条语义可以直接在单元测试里
// 断言，不用真的点菜单。
type Options struct {
	Icon     []byte
	Title    string
	Tooltip  string
	PanelURL func() string
	// OpenPanel/Reload/Quit 都不许为 nil 的假设由调用方维持；Run 对 nil
	// 逐个兜底，托盘坏一个动作不能拖垮整个菜单循环。
	OpenPanel func()
	Reload    func()
	Quit      func()
}

// Run blocks until the tray quits.
//
// 菜单项文案用 ASCII（计划 25 步骤 7 的裁定）：systray 在 Windows 上走
// Shell_NotifyIcon + GDI 绘制，菜单项中文依赖系统里存在能画 CJK 的字体，
// 缺字就是方块；不引入新依赖去修，改成 Open panel/Reload/Quit 并在 README
// 说明。托盘标题与提示不受此限 —— SetTitle/SetTooltip 走 Unicode API
// （由调用方传中文），只有菜单项受影响。Quit 的就绪守卫见包首注释。

func Run(o Options) {
	// done 由 **Run** 持有，不由下面的 onReady 回调持有。
	//
	// onReady 回调一建完菜单项就立刻返回（systray.Run 自己继续阻塞整个托盘
	// 生命周期）。所以 `defer close(done)` 绝不能写在回调里：它会在开表几微秒
	// 后就掐掉菜单 goroutine，三个菜单项**全部失效** —— "Open panel" 不再打开
	// 面板、"Quit" 不再退出，用户只能去任务管理器强杀。这个错一旦犯就是整条
	// 托盘路径报废，且没有任何单元测试能在无桌面会话里看见它。
	//
	// 收口的正确时机是 systray.Run 返回（= 托盘结束）那一刻。
	done := make(chan struct{})
	defer close(done)

	systray.Run(func() {
		// Register 已经完成:放开信号桥的 Quit 许可。
		readyOnce.Do(func() { close(ready) })
		if len(o.Icon) > 0 {
			systray.SetIcon(o.Icon)
		}
		systray.SetTitle(o.Title)
		systray.SetTooltip(o.Tooltip)

		mOpen := systray.AddMenuItem("Open panel", "Open the gateway console in a browser")
		mReload := systray.AddMenuItem("Reload", "Reload settings and rebuild exits without dropping connections")
		systray.AddSeparator()
		mQuit := systray.AddMenuItem("Quit", "Stop the gateway and quit")

		// 菜单 goroutine 由 done 收口：只监听三个 ClickedCh 时,菜单点击处理器
		// 在 tray 退出后仍会留在 select 上（菜单项由 systray 持有,没人 Close
		// 它们）,进程里就多一个永不退的 goroutine。
		go menuLoop(done, mOpen.ClickedCh, mReload.ClickedCh, mQuit.ClickedCh, o, systray.Quit)
	}, nil)
}

// menuLoop 是托盘菜单的点击处理循环。done 关闭即返回（托盘已结束）。
//
// 独立成函数有三个理由：一是 Run 里只剩「谁拥有 done」这一处关键结构，读者一眼
// 能看出它属于 Run 而不是 onReady 回调（见 Run 的注释）；二是这里可以用假
// channel 测 —— 无交互桌面的测试环境进不去 systray.Run，但没有理由因此测不了
// 「点 Open panel 真的会调 OpenPanel / done 一关循环就退」；三是 quitTray 成了
// 注入点：systray.Quit() 在托盘没起来时会自锁，测试直接调它只会把测试二进制
// 挂死（这正是本包原来一个菜单项都测不了的原因）。
func menuLoop(done <-chan struct{}, openCh, reloadCh, quitCh <-chan struct{}, o Options, quitTray func()) {
	for {
		select {
		case <-done:
			return
		case <-openCh:
			if o.OpenPanel != nil {
				o.OpenPanel()
			} else if o.PanelURL != nil {
				// 兜底：调用方没给动作时，用包内自己的 OpenBrowser 开
				// PanelURL —— 菜单档位不许空转。
				OpenBrowser(o.PanelURL())
			}
		case <-reloadCh:
			if o.Reload != nil {
				o.Reload()
			}
		case <-quitCh:
			// 先 o.Quit()（它负责 stop()+Shutdown）再 systray.Quit()：
			// 反过来会让消息循环先死、善后回调可能不执行
			// （launcher:194-197 的顺序同款）。
			if o.Quit != nil {
				o.Quit()
			}
			quitOnce.Do(func() { quitTray() })
			return
		}
	}
}
