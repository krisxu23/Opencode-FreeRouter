// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// armLifecycle 给测试自己拼出来的 Parts 装上生命周期根 —— 生产路径上这是
// Load 的职责,夹具绕过了 Load,所以得自己来。
func armLifecycle(p *Parts) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	p.lifeCtx = ctx
	p.cancel = cancel
	return cancel
}

// TestShutdownDisarmsPendingTimers 钉住 B9 的一半:关停必须停掉还没触发的
// afterFunc。从前 Shutdown 既不 Stop 定时器也不 join timersWG,于是托盘
// 「退出」之后一秒的重跑定时器照样触发,以 context.Background() 重入探测,
// 继续写 node-registry.json / node-health.json 和网关日志。
func TestShutdownDisarmsPendingTimers(t *testing.T) {
	p := newProbeParts(t, 1)
	armLifecycle(p)

	var hits int32
	p.afterFunc(50*time.Millisecond, func() { atomic.AddInt32(&hits, 1) })

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	time.Sleep(250 * time.Millisecond)
	if got := atomic.LoadInt32(&hits); got != 0 {
		t.Fatalf("关停后仍有 %d 个定时器回调跑了, want 0", got)
	}
}

// TestShutdownWaitsForRunningTimers 钉住 B9 的另一半:已经在跑的回调属于
// timersWG,关停要等它跑完。测法是把一个回调卡在 300ms 的睡眠里,再断言
// Shutdown 的耗时覆盖了它 —— 修前 Shutdown 立刻返回,落盘/Host.Close 与
// 那个还在写注册表的回调叠在一起。
func TestShutdownWaitsForRunningTimers(t *testing.T) {
	p := newProbeParts(t, 1)
	armLifecycle(p)

	started := make(chan struct{})
	p.afterFunc(10*time.Millisecond, func() {
		close(started)
		time.Sleep(300 * time.Millisecond)
	})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("定时器回调没有跑起来")
	}

	begin := time.Now()
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < 200*time.Millisecond {
		t.Fatalf("Shutdown 只用了 %v:没有等在跑的回调", elapsed)
	}
}

// TestShutdownCapsTheTimerJoin 钉住 join 的兜底:调用方给了带超时的 ctx 时,
// 一个卡在慢网络里的在途轮次不能把退出永久挂住。
func TestShutdownCapsTheTimerJoin(t *testing.T) {
	p := newProbeParts(t, 1)
	armLifecycle(p)

	release := make(chan struct{})
	started := make(chan struct{})
	p.afterFunc(10*time.Millisecond, func() {
		close(started)
		<-release
	})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("定时器回调没有跑起来")
	}
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 3*time.Second {
		t.Fatalf("Shutdown 被卡住的回调拖了 %v", elapsed)
	}
}

// TestRebuildReportsSubscriptionFailure 钉住 B11 的用户可见面:订阅全挂时
// Rebuild 必须回错误,面板「刷新」才不会再对着一次全军覆没 toast 成功。
func TestRebuildReportsSubscriptionFailure(t *testing.T) {
	p := newProbeParts(t, 2)
	swallowTimers(p)
	p.Settings.SubURLs = []string{subAndCatalogServer(t, "boom", "boom", 500)}

	err := p.Rebuild(context.Background())
	if err == nil {
		t.Fatal("订阅全挂时 Rebuild 必须返回错误")
	}
	if p.lastRebuildOK {
		t.Fatal("订阅全挂却把本轮重建记为成功")
	}
	if p.lastRebuildErr == "" {
		t.Fatal("失败原因没有记进 lastRebuildErr")
	}
	if p.Registry.Len() != 2 {
		t.Fatalf("注册表 = %d, want 2(降级照旧:失败不清池子)", p.Registry.Len())
	}
}

// TestJoinBootWaitsForTheGoroutine 钉住 R12 的一半:等待是真的等待。
// Load 失败路径从前只 cancel,不 join —— 那个开场订阅协程仍可能在 Load 返回
// 错误之后继续 reg.Merge/Flush/Host.SyncOutbounds,调用方却以为一切已经收场。
func TestJoinBootWaitsForTheGoroutine(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	released := make(chan struct{})
	go func() {
		defer wg.Done()
		<-released
	}()

	begin := time.Now()
	done := make(chan struct{})
	go func() {
		joinBoot(&wg, 5*time.Second)
		close(done)
	}()
	// 200ms 内 joinBoot 不能返回:协程还卡着。
	select {
	case <-done:
		t.Fatal("joinBoot 在协程仍在跑时就返回了")
	case <-time.After(200 * time.Millisecond):
	}
	close(released)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("协程退出后 joinBoot 仍未返回(等了 %v)", time.Since(begin))
	}
}

// TestJoinBootCapsTheWait 钉住另一半:上限必须生效。订阅源若无视 ctx,没有上限
// 的等待会让 Load 永不返回 —— 调用方拿不到「启动失败」这个错误,只能看着进程
// 挂在那里。
func TestJoinBootCapsTheWait(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // 永不 Done:模拟一个卡死的订阅协程。

	begin := time.Now()
	joinBoot(&wg, 50*time.Millisecond)
	elapsed := time.Since(begin)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("joinBoot 只等了 %v, want >= 50ms(不能提前放行)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("joinBoot 等了 %v:上限没有生效", elapsed)
	}
}

// TestVersionMatchesPackageJSONWhenInjected 钉住 B12:控制台显示的版本号只有
// 一个来源 —— package.json。构建脚本把它经 -X 注入 app.Version,所以带 LDFLAGS
// 跑测试时两者必须一致。裸 `go test`(没注入)拿的是开发默认值,跳过。
func TestVersionMatchesPackageJSONWhenInjected(t *testing.T) {
	if Version == "0.0.0-dev" {
		t.Skip("未经 -X 注入(裸 go test),版本一致性由构建脚本的 LDFLAGS 保证")
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "package.json"))
	if err != nil {
		t.Fatalf("读 package.json: %v", err)
	}
	var pkg struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		t.Fatalf("解析 package.json: %v", err)
	}
	if pkg.Version != Version {
		t.Fatalf("app.Version = %q, package.json version = %q", Version, pkg.Version)
	}
}
