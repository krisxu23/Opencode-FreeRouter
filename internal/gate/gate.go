// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package gate 是令牌间隔闸门:把并发调用按固定最小间隔错开放行
// (语义照抄 src/gate.js 的 createRateGate)。
//
// 用途:一轮探测里有几百个节点要打真实会话(消耗配额),同时到达会被上游
// 限流,把整轮自己压扁。错开它们能让一轮跑完,而不是半路被削。
//
// 抽成独立包的原因:这段逻辑原先内联在 index.js 的探测泵里(tierGate),
// 是一个用绝对时间戳手写的计数闸门,读的时候要先在脑子里推演一遍才敢改;
// 同类需求(第二处、第三处限流点)会重复这个推演。
package gate

import (
	"context"
	"sync"
	"time"
)

// Gate 保证两次放行之间至少隔 gapMS 毫秒。并发调用各自领到不同时隙,
// 因此天然串成间隔序列。
type Gate struct {
	mu     sync.Mutex
	gapMS  int64
	nextAt int64 // 下一个待分配时隙(UnixMilli;0 = 还没排过任何时隙)
}

// New 返回一个最小放行间隔为 gapMS 毫秒的闸门。gapMS <= 0 视为 0:
// 无间隔、立即放行(JS 版对负数抛错,Go 侧按修正案放宽为夹紧到 0)。
func New(gapMS int) *Gate {
	g := int64(gapMS)
	if g < 0 {
		g = 0
	}
	return &Gate{gapMS: g}
}

// Wait 领取下一个时隙并等到它;ctx 结束时返回 ctx.Err()。
//
// 只返回 ctx.Err(),不造新错误类型:调用方收到失败要做的只有放弃这一次
// 排期,没必要再分辨原因。取消的请求不回收时隙 —— 回收要和并发分配竞争,
// 多留一格只影响错峰密度,不影响正确性。
func (g *Gate) Wait(ctx context.Context) error {
	// 取消检查必须覆盖**直通路径**(R4):delay <= 0 时下面的 select 根本不会
	// 执行,所以只在 select 里看 ctx 是不够的 —— 常态(无间隔或时隙已过)下
	// Wait 会拿着一个已死的 ctx 返回 nil,调用方以为排期成功,继续跑完一整轮
	// 探测。这里先查一次,让「ctx 结束」在所有路径上都是同一个结果。
	if err := ctx.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	now := time.Now().UnixMilli()
	slot := g.nextAt
	if now > slot {
		slot = now
	}
	// 不能把时隙排到过去。原实现直接用 nextAt,而它从 0 起步 —— 于是前
	// N 次(N = 满额并发数)拿到的时隙全都早于当前时间,等于一次都没错峰,
	// 只有第 N+1 次之后才开始生效;也就是说原实现在最需要错峰的开局阶段
	// 是失效的(src/gate.js:27-30 的实测注释)。
	g.nextAt = slot + g.gapMS
	delay := slot - now
	g.mu.Unlock()

	if delay > 0 {
		t := time.NewTimer(time.Duration(delay) * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}

	// 醒来时可能已经晚了(调度拥塞、timer 只保证不早于)。这时下一个调用者
	// 算出的 slot 已经在过去,它会立即放行 —— 两次请求贴在一起,正是这个
	// 闸门要防的形状。把时隙锚定到真正的放行时刻,保证的是「两次放行之间」
	// 的间隔,而不只是「两次排期之间」的间隔(src/gate.js:38-43)。
	released := time.Now().UnixMilli()
	if released > slot {
		g.mu.Lock()
		if na := released + g.gapMS; na > g.nextAt {
			g.nextAt = na
		}
		g.mu.Unlock()
	}
	return nil
}

// NextAt 返回下一个待分配的时隙(UnixMilli;从未排过为 0)。测试与面板用。
func (g *Gate) NextAt() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.nextAt
}
