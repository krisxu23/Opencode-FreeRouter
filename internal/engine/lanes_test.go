package engine

// lanes 闸门(ExitConcurrency)的测试。夹具沿用 engine_test.go:真实
// health、8 节点池、独立出口 IP、按脚本回 SSE 的伪造上游。lanes 自身的
// 单元语义(FIFO/ctx 取消/幂等 release)在 magpie 原件上已验,这里钉的是
// 「引擎把它接对了」:排队不换出口、记账全归还、面板视图对得上。

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"freerouter/internal/nodeprobe"
)

// setExitConcurrency 是面板保存 ExitConcurrency 的测试替身。
func (f *fixture) setExitConcurrency(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settings.ExitConcurrency = n
}

// gateAll 让**每个**到达上游的请求都阻塞到 close(gate):测试要的窗口是
// 「多个请求同时都在上游读响应」,而不是只有第一个。release 后全部放行。
func gateAll(t *testing.T) (block func(n int) (int, string, string), release func()) {
	t.Helper()
	gate := make(chan struct{})
	block = func(n int) (int, string, string) {
		<-gate
		return 200, "text/event-stream", sseChat("ok")
	}
	release = func() { close(gate) }
	return block, release
}

// gateOpen 在第 seen 次请求到达时由上游脚本调用:阻塞到 close(gate) 才
// 返回,让测试精确控制「槽位被占住」的窗口。
func gateOpen(t *testing.T) (block func(n int) (int, string, string), release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	block = func(n int) (int, string, string) {
		if n == 0 {
			<-gate
		}
		return 200, "text/event-stream", sseChat("ok")
	}
	release = func() { once.Do(func() { close(gate) }) }
	return block, release
}

func TestLanesZeroIsUnlimited(t *testing.T) {
	// 零值(默认设置)绝不排队:8 个节点并发打满也不等。
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	seedAliveIPs(f.h)
	for i := 0; i < 16; i++ {
		if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", fmt.Sprintf("u%d", i)), nil); err != nil {
			t.Fatalf("不限流下第 %d 个请求不应失败: %v", i, err)
		}
	}
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("全部完成后 lanes 应为空(无泄漏): %v", lanes)
	}
}

func TestLanesQueueWaitsForTheSlot(t *testing.T) {
	// ExitConcurrency=1:第二个并发请求必须在队列里等第一个读完 —— 等
	// 待不是失败,不换出口、不报错。
	block, release := gateOpen(t)
	f := newFixture(t, block)
	seedAliveIPs(f.h)
	f.setExitConcurrency(1)

	done := make(chan error, 2)
	go func() {
		_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	// 等第一个请求真正抵达上游(占住唯一的槽),再放第二个。
	deadline := time.After(5 * time.Second)
	for f.up.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("第一个请求没到上游")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// 同会话(同 user)的第二个请求:sticky 已把 u1 钉到 node-0,同 user
	// 的 u2 也会 Pick 到同一个出口 —— 这才是同 IP 槽位竞争的生产形状。
	go func() {
		_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	// 短暂等待后确认第二个还在排队:没换出口、没失败。
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("第二个请求不该在槽位释放前完成: %v", err)
	default:
	}
	lanes := f.eng.Lanes()
	ln, ok := lanes["10.0.0.1"]
	if !ok {
		t.Fatalf("10.0.0.1 车道应在视图里: %v", lanes)
	}
	if ln.Busy != 1 || ln.Waiting != 1 || ln.Limit != 1 {
		t.Fatalf("车道视图 busy=1 waiting=1 limit=1, got %+v", ln)
	}

	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("排队请求最终应成功: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("放行后请求没有完成")
		}
	}
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("全部完成后 lanes 应为空: %v", lanes)
	}
}

func TestLanesCtxCancelLeavesTheQueue(t *testing.T) {
	// 排队时客户端离开:这个请求被取消,永不发给上游;先到的那对继续。
	block, release := gateOpen(t)
	f := newFixture(t, block)
	seedAliveIPs(f.h)
	f.setExitConcurrency(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() {
		_, err := f.eng.Complete(ctx, simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	deadline := time.After(5 * time.Second)
	for f.up.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("第一个请求没到上游")
		case <-time.After(5 * time.Millisecond):
		}
	}
	go func() {
		_, err := f.eng.Complete(ctx, simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond) // 让第二个进队列

	cancel() // 客户端离开
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("被取消的排队请求应返回 ctx.Err()")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("取消后没有返回")
	}

	// 被取消的那位不该偷跑:上游仍只收到第一个请求。
	release()
	if n := f.up.count(); n != 1 {
		t.Fatalf("取消的请求不该发给上游, 上游收到 %d 个", n)
	}
	<-done // 第一个正常收尾
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("全部完成后 lanes 应为空: %v", lanes)
	}
}

func TestLanesReleaseOnRotationPath(t *testing.T) {
	// 换出口(continue 路径)也必须还车道槽:node-0 先 429,node-1 成功。
	// 不归还的话第二个出口的槽会被第一次尝试永久占用,池子越走越窄。
	script := func(n int) (int, string, string) {
		if n == 0 {
			return 429, "application/json", quotaBody
		}
		return 200, "text/event-stream", sseChat("ok")
	}
	f := newFixture(t, script)
	seedAliveIPs(f.h)
	f.setExitConcurrency(1)
	out, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
	if err != nil {
		t.Fatalf("换出口后应成功: %v", err)
	}
	if out.Text != "ok" {
		t.Fatalf("text %q", out.Text)
	}
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("换出口路径泄漏了车道槽: %v", lanes)
	}
}

func TestLanesLimitRaiseUnblocksWaiters(t *testing.T) {
	// 限额被解除(limit<=0):排着队的人立刻走。magpie 的 grant 语义 ——
	// lane.limit 只在 acquire/releaser 触碰时更新,解除限额由下一位
	// acquire 者执行 grant() 完成,所以排队者被随后任一 acquire 放走。
	block, release := gateOpen(t)
	f := newFixture(t, block)
	seedAliveIPs(f.h)
	f.setExitConcurrency(1)

	done := make(chan error, 3)
	go func() {
		_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	deadline := time.After(5 * time.Second)
	for f.up.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("第一个请求没到上游")
		case <-time.After(5 * time.Millisecond):
		}
	}
	for i := 2; i <= 3; i++ {
		// 两个排队者都用同 user:同 session 才会被 sticky 钉到 node-0 的
		// 同一条车道上(不同会话会分流到 node-1,不构成竞争)。
		go func(u string) {
			_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", u), nil)
			done <- err
		}("u1")
	}
	time.Sleep(100 * time.Millisecond)

	// 面板把闸门关掉(0=不限):排队者由随后任一 acquire 的 grant 放行。
	// 这里从包内直接发一次 acquire 触发(等价于运行中设置变更后的下一
	// 个请求),拿到即还,不占槽。
	releaseLane, ok := f.eng.exitLanes.acquire(context.Background(), "10.0.0.1", 0)
	if !ok {
		t.Fatal("不限流 acquire 应立即成功")
	}
	releaseLane()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("限额解除后排队者应立刻成功: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("限额解除后排队者没有放行")
	}

	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("剩余请求应成功: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("放行后请求没有完成")
		}
	}
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("全部完成后 lanes 应为空: %v", lanes)
	}
}

func TestLanesDifferentIPsDoNotBlockEachOther(t *testing.T) {
	// 闸门按出口 IP 分道:node-0 的槽被占住,node-1 的请求照走。两个**不同
	// 会话**(不同 user)sticky 各自独立 —— u2 由排序挑 node-1,不被
	// node-0 的队列拖住。gateAll 让两个请求都停在上游读响应,车道视图
	// 才有时间窗可见。
	block, release := gateAll(t)
	f := newFixture(t, block)
	seedAliveIPs(f.h)
	f.setExitConcurrency(1)

	done := make(chan error, 2)
	go func() {
		_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil)
		done <- err
	}()
	deadline := time.After(5 * time.Second)
	for f.up.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("第一个请求没到上游")
		case <-time.After(5 * time.Millisecond):
		}
	}
	go func() {
		_, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u2"), nil)
		done <- err
	}()
	// 轮询等第二条车道出现(u2 到上游才有 busy,不能用 sleep 死等)。
	deadline = time.After(5 * time.Second)
	for {
		lanes := f.eng.Lanes()
		if ln, ok := lanes["10.0.0.2"]; ok && ln.Busy == 1 {
			// 两条车道同时 busy:node-0 的排队没拖住 node-1。
			if lanes["10.0.0.1"].Busy != 1 {
				t.Fatalf("10.0.0.1 车道应 busy=1: %v", lanes)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("10.0.0.2 车道没出现(不同 IP 该并行): %v", f.eng.Lanes())
		case err := <-done:
			t.Fatalf("请求提前结束: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	release()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("应成功: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("请求没有完成")
		}
	}
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("完成后 lanes 应为空: %v", lanes)
	}
}

func TestLanesSnapshotMatchesEngineLanes(t *testing.T) {
	// status.go 走 Engine.Lanes():确认方法确实暴露了 lanes 内部视图。
	f := newFixture(t, func(n int) (int, string, string) {
		return 200, "text/event-stream", sseChat("ok")
	})
	seedAliveIPs(f.h)
	f.setExitConcurrency(4)
	if _, err := f.eng.Complete(context.Background(), simpleReq("big-pickle", "u1"), nil); err != nil {
		t.Fatalf("应成功: %v", err)
	}
	// 完成后为空;并发在途形状已由 TestLanesQueueWaitsForTheSlot 钉死。
	if lanes := f.eng.Lanes(); len(lanes) != 0 {
		t.Fatalf("完成后应为空: %v", lanes)
	}
	_ = nodeprobe.StateAlive // 保持与夹具同源(实测探针位),防止 import 漂移
}
