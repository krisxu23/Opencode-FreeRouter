// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package gate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// 对照修正案(2026-10-01-plan-corrections.md §3 gate 行)重写的间隔闸门测试。
// 语义以 src/gate.js 为准:token 间隔闸门(探测泵错峰用),不是每分钟窗口。
// 耗时断言的纪律:下限卡语义(timer 只会晚不会早,下限因此不 flaky),
// 上限放宽 4-5 倍给慢 CI 留抖动余量。

func TestFirstWaitIsImmediate(t *testing.T) {
	g := New(50)
	t0 := time.Now()
	if err := g.Wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if d := time.Since(t0); d > 20*time.Millisecond {
		t.Fatalf("first wait took %v, want immediate(首次时隙 = max(0, now),必须立即放行)", d)
	}
	na := g.NextAt()
	if na <= 0 {
		t.Fatal("NextAt() = 0 after first wait, want a scheduled slot")
	}
	// 下一个时隙 ≈ 放行时刻 + gap:比「现在」晚约一个 gap,且绝不是过去。
	if d := na - time.Now().UnixMilli(); d <= 0 || d > 50 {
		t.Fatalf("NextAt()-now = %dms, want in (0,50]", d)
	}
}

func TestWaitsAreSpacedByGap(t *testing.T) {
	g := New(80)
	ctx := context.Background()
	t0 := time.Now()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t1 := time.Now()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t2 := time.Now()
	if err := g.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	t3 := time.Now()
	if gap := t2.Sub(t1); gap < 80*time.Millisecond {
		t.Fatalf("gap1 = %v, want >= 80ms(串行调用者睡到自己那格,放行只会晚于时隙不会早于)", gap)
	}
	if gap := t3.Sub(t2); gap < 80*time.Millisecond {
		t.Fatalf("gap2 = %v, want >= 80ms", gap)
	}
	// 总时长下限是无测量偏差的硬不变量:t0 <= r1 <= r3-2*gap <= t3。
	if total := t3.Sub(t0); total < 160*time.Millisecond {
		t.Fatalf("3 waits took %v, want >= 160ms(2 个完整间隔)", total)
	}
	if total := t3.Sub(t0); total >= 800*time.Millisecond {
		t.Fatalf("3 waits took %v, want < 800ms(gap 80 的三连发放行不该慢成这样)", total)
	}
}

func TestConcurrentWaitsSerialize(t *testing.T) {
	g := New(100)
	ctx := context.Background()
	const n = 10
	start := make(chan struct{})
	ready := make(chan struct{}, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			errs[i] = g.Wait(ctx)
		}(i)
	}
	for i := 0; i < n; i++ {
		<-ready
	}
	t0 := time.Now()
	close(start)
	wg.Wait()
	d := time.Since(t0)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	// 并发调用各自领到独立时隙:第 k 个的时隙 >= 首个 + (k-1)*gap,
	// 所以全体完成不可能早于 (n-1)*gap —— 这是闸门的核心承诺。
	if d < 900*time.Millisecond {
		t.Fatalf("10 concurrent waits finished in %v, want >= 900ms(最后一个时隙 >= 9*100ms)", d)
	}
	if d > 1300*time.Millisecond {
		t.Fatalf("10 concurrent waits finished in %v, want <= 1300ms(9*100ms + 400ms 抖动余量)", d)
	}
}

func TestWaitRespectsContextCancellation(t *testing.T) {
	// 正在排队的请求必须能被取消,否则等待会在拥塞时把调用方堆死。
	g := New(300)
	if err := g.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := g.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded(只透传 ctx.Err(),不造新错误类型)", err)
	}
}

func TestZeroGapAdmitsImmediately(t *testing.T) {
	g := New(0)
	for i := 0; i < 5; i++ {
		t0 := time.Now()
		if err := g.Wait(context.Background()); err != nil {
			t.Fatalf("wait %d: %v", i, err)
		}
		if d := time.Since(t0); d > 20*time.Millisecond {
			t.Fatalf("wait %d took %v, want immediate(gap<=0 视为 0,无间隔)", i, d)
		}
	}
}

func TestSlotAnchoredToActualRelease(t *testing.T) {
	g := New(50)
	if err := g.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 睡过一整个 gap:原排的时隙已经落在过去(模拟事件循环/调度拥塞)。
	time.Sleep(120 * time.Millisecond)
	t0 := time.Now()
	if err := g.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 30*time.Millisecond {
		t.Fatalf("wait after idle took %v, want immediate(slot = max(nextAt, now),过期时隙不许拖累新调用)", d)
	}
	// 时隙必须锚到本次真实放行时刻 + gap(≈ now+50),而不是停在过去的旧值 ——
	// 锚定保证的是「两次放行之间」的间隔,否则拥塞后的两次请求会贴在一起。
	na := g.NextAt()
	now := time.Now().UnixMilli()
	if na < now {
		t.Fatalf("NextAt() = %d < now = %d, want anchored to release+gap", na, now)
	}
	if na > now+100 {
		t.Fatalf("NextAt() = %d, now = %d, want ≈ now+50", na, now)
	}
}
