// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"sync"
	"testing"
	"time"
)

// TestAfterFuncFiredBeforeAttachIsNotRetained 是整分支评审的 RISK-5:afterFunc
// 过去是「先排程、后登记」,而且回调读的是主线程里那个尚未被同步赋值的 `self`。
// 只要回调在登记之前跑完(接缝、或任何调度次序),forgetTimer 拿到的就是 nil、
// 直接早退,随后 rememberTimer 把一个**已经触发过**的定时器追加进 p.timers ——
// 它再也没有摘除点,登记表随探测/重建轮次单调增长,正是 B9 要防的那类
// 「长命状态不回收」。
//
// 测法用接缝把回调安排在「返回之前」跑完:这是确定性次序,不赌时钟粒度。
func TestAfterFuncFiredBeforeAttachIsNotRetained(t *testing.T) {
	p := &Parts{}
	p.afterFuncFn = func(d time.Duration, fn func()) *time.Timer {
		fn() // 回调先于登记跑完
		return &time.Timer{}
	}
	p.afterFunc(time.Millisecond, func() {})

	p.timersMu.Lock()
	n := len(p.timers)
	p.timersMu.Unlock()
	if n != 0 {
		t.Fatalf("p.timers 留着 %d 个已经触发过的定时器:登记表只长不消", n)
	}
	// 计数也必须配平:回调跑过了 → wrapped 自己 Done → 关停不该再等它。
	done := make(chan struct{})
	go func() { p.timersWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timersWG 没有配平,Shutdown 会一直等到超时")
	}
}

// TestAfterFuncNilSeamDoesNotRetain 是同一条纪律的另一半:接缝返回 nil
// (swallowTimers 的用法)时,既没有可 Stop 的定时器、也没有回调来摘它 —— 这一格
// 同样不许留在表里。旧实现靠「rememberTimer(nil) 不登记」做到,新实现必须保住。
func TestAfterFuncNilSeamDoesNotRetain(t *testing.T) {
	p := &Parts{}
	swallowTimers(p)
	p.afterFunc(time.Second, func() {})
	p.timersMu.Lock()
	n := len(p.timers)
	p.timersMu.Unlock()
	if n != 0 {
		t.Fatalf("nil timer 被登记了 %d 格, want 0(没人能 Stop 它,也没人摘它)", n)
	}
}

// TestStopPendingTimersClosesTheLedger 钉住关停路径下的三条不变量:
//   - 未触发的定时器被 Stop、计数归还(wrapped 不会再跑);
//   - 表被清空;
//   - **关停之后再排程**不得留下永远没人停的格子 —— 否则 B9 的「自我复活」换个
//     形式回来:回调照样会在几十分钟之后写盘。
func TestStopPendingTimersClosesTheLedger(t *testing.T) {
	p := &Parts{}
	var mu sync.Mutex
	ran := 0
	// 0 延时:回调立刻跑或被 Stop 拦下都是合法结局,所以只断言不变量。
	p.afterFunc(0, func() { mu.Lock(); ran++; mu.Unlock() })
	p.stopPendingTimers()

	p.timersMu.Lock()
	n := len(p.timers)
	p.timersMu.Unlock()
	if n != 0 {
		t.Fatalf("stopPendingTimers 之后表里还剩 %d 格", n)
	}
	done := make(chan struct{})
	go func() { p.timersWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("关停后的 timersWG 没有配平")
	}

	p.afterFunc(time.Hour, func() {})
	p.timersMu.Lock()
	n = len(p.timers)
	p.timersMu.Unlock()
	if n != 0 {
		t.Fatalf("已关停的登记表又收了 %d 格:关停后排程必须当场拦掉", n)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if ran != 0 {
		t.Fatal("关停之后排的定时器照样跑了(B9 的自我复活换了个形式)")
	}
}
