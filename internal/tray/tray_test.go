// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package tray

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
