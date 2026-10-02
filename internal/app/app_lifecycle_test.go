// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

// TestProbeRerunDoesNotOutliveShutdown 钉住 B9 最刺眼的那条复活路径:
// finishProbeRound 安排的一秒重跑从前写死 context.Background(),是四个复活点
// 里唯一连 ctx.Err() 都不查的 —— 关停之后它真的会跑完一整轮探测。
func TestProbeRerunDoesNotOutliveShutdown(t *testing.T) {
	p := newProbeParts(t, 3)
	armLifecycle(p)
	prober := p.Prober.(*fakeProber)

	p.markRerun(true)
	p.finishProbeRound() // 排一秒后的重跑

	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	time.Sleep(1300 * time.Millisecond)

	prober.mu.Lock()
	rounds := prober.allCnt
	prober.mu.Unlock()
	if rounds != 0 {
		t.Fatalf("关停后仍跑了 %d 轮探测, want 0", rounds)
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
