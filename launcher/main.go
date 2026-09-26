// lite-gateway 托盘壳：拉起 node 网关子进程，托盘菜单（打开面板/重启网关/退出）。
// sing-box 由网关进程自己管理生命周期，托盘只认 node 一棵进程树（/T 连带杀孙进程）。
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"unsafe"

	"fyne.io/systray"
	"golang.org/x/sys/windows"
)

//go:embed icon.ico
var iconBytes []byte

var jobHandle windows.Handle

type gateway struct {
	cmd *exec.Cmd
}

// createKillOnCloseJob 作业对象：句柄关闭（本程序退出/崩溃/被杀）时，
// 内所有成员（node 及其子进程 sing-box）一并终止 —— 孤儿从源头杜绝。
func createKillOnCloseJob() windows.Handle {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil || job == 0 {
		return 0
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return 0
	}
	return job
}

func assignToJob(job windows.Handle, pid int) {
	if job == 0 {
		return
	}
	h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		return
	}
	defer windows.CloseHandle(h)
	_ = windows.AssignProcessToJobObject(job, h)
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

func panelPort() int {
	port := 3458
	if raw, err := os.ReadFile(filepath.Join(exeDir(), "data", "settings.json")); err == nil {
		var s struct {
			PanelPort int `json:"panelPort"`
		}
		if json.Unmarshal(raw, &s) == nil && s.PanelPort > 0 {
			port = s.PanelPort
		}
	}
	return port
}

func panelURL() string {
	return "http://127.0.0.1:" + strconv.Itoa(panelPort())
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
	// node 连同它拉起的 sing-box 一起纳入作业对象：本程序死 => 全家死
	assignToJob(jobHandle, cmd.Process.Pid)
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
	// 单实例守卫：面板端口已在监听 = 程序已在运行，直接打开那个面板并退出，
	// 避免双开导致端口抢占、第二个实例停在空闲态。
	if panelUp(panelPort()) {
		openBrowser(panelURL())
		os.Exit(0)
	}
	jobHandle = createKillOnCloseJob()
	systray.Run(onReady, nil)
}

func panelUp(port int) bool {
	conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func openBrowser(url string) {
	_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
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
				openBrowser(panelURL())
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
