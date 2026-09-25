// lite-gateway 托盘壳：拉起 node 网关子进程，托盘菜单（打开面板/重启网关/退出）。
// sing-box 由网关进程自己管理生命周期，托盘只认 node 一棵进程树（/T 连带杀孙进程）。
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"fyne.io/systray"
)

//go:embed icon.ico
var iconBytes []byte

type gateway struct {
	cmd *exec.Cmd
}

func exeDir() string {
	p, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(p)
}

func fileExists(p string) bool {
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// nodePath: 优先随包分发的 runtime\node.exe，否则用 PATH 里的 node。
func nodePath() string {
	if p := filepath.Join(exeDir(), "runtime", "node.exe"); fileExists(p) {
		return p
	}
	return "node"
}

func panelURL() string {
	port := 3458
	if raw, err := os.ReadFile(filepath.Join(exeDir(), "data", "settings.json")); err == nil {
		var s struct {
			PanelPort int `json:"panelPort"`
		}
		if json.Unmarshal(raw, &s) == nil && s.PanelPort > 0 {
			port = s.PanelPort
		}
	}
	return fmt.Sprintf("http://127.0.0.1:%d", port)
}

func (g *gateway) start() error {
	cmd := exec.Command(nodePath(), filepath.Join(exeDir(), "src", "index.js"))
	cmd.Dir = exeDir()
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	logFile, err := os.OpenFile(filepath.Join(exeDir(), "data", "gateway.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	g.cmd = cmd
	return nil
}

// stop 连带杀掉 node 的整棵子树（含 sing-box）。
func (g *gateway) stop() {
	if g.cmd == nil || g.cmd.Process == nil {
		return
	}
	_ = exec.Command("taskkill", "/T", "/F", "/PID", fmt.Sprint(g.cmd.Process.Pid)).Run()
	g.cmd = nil
}

func main() {
	systray.Run(onReady, nil)
}

func onReady() {
	systray.SetIcon(iconBytes)
	systray.SetTitle("Opencode-FreeRouter")
	systray.SetTooltip("Opencode-FreeRouter — opencode.ai 免费车道网关")

	mOpen := systray.AddMenuItem("打开面板", "打开网关设置与状态面板")
	mRestart := systray.AddMenuItem("重启网关", "重启 node 网关（连带 sing-box）")
	systray.AddSeparator()
	mQuit := systray.AddMenuItem("退出", "停止网关并退出托盘")

	g := &gateway{}
	if err := g.start(); err != nil {
		mRestart.SetTitle("网关启动失败: " + err.Error())
	}

	go func() {
		for {
			select {
			case <-mOpen.ClickedCh:
				_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", panelURL()).Start()
			case <-mRestart.ClickedCh:
				g.stop()
				if err := g.start(); err != nil {
					mRestart.SetTitle("网关启动失败: " + err.Error())
				}
			case <-mQuit.ClickedCh:
				g.stop()
				systray.Quit()
				return
			}
		}
	}()
}
