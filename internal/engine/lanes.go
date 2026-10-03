// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package engine

import (
	"context"
	"sync"
)

// lanes 是出口车道闸门:同一出口 IP 上最多 ExitConcurrency 个在途请求,
// 多出来的按先来后进排队等槽 —— 等待不是失败:排队的请求不换出口、不
// 记失败,只在队列里等前一个回合读完或其客户端离开。移植自 magpie 的
// internal/gateway/concurrency.go(那边按 key/account 限并发,这里按出
// 口 IP —— FreeRouter 的稀缺资源是出口 IP,magpie 的是供应商账号)。
//
// limit<=0 视为不限流:acquire 直接放行,releaser 是 no-op。
type lanes struct {
	mu sync.Mutex
	m  map[string]*lane
}

// lane 是一个出口 IP 的车道:在途多少、谁在等(队列严格 FIFO)。
type lane struct {
	limit int
	busy  int
	queue []chan struct{}
}

// acquire 等一个 who 名下的槽位,轮到了才返回。返回的 release 必须
// 恰好调用一次(回合结束时);ctx 先结束则拿不到槽,返回 false。
// limit<=0 不占槽(不限流),也顺带把还在等的人全放走 —— 运行中把
// ExitConcurrency 调回 0 时,排着队的请求立刻全部放行。
func (l *lanes) acquire(ctx context.Context, who string, limit int) (release func(), ok bool) {
	l.mu.Lock()
	if l.m == nil {
		l.m = map[string]*lane{}
	}
	ln := l.m[who]
	if limit <= 0 {
		if ln != nil {
			// 限额被解除:排着队的人现在就能走
			ln.limit = 0
			ln.grant()
		}
		l.mu.Unlock()
		return func() {}, true
	}
	if ln == nil {
		ln = &lane{}
		l.m[who] = ln
	}
	ln.limit = limit
	ln.grant() // 限额调大会让排着队的人先走
	if ln.busy < ln.limit && len(ln.queue) == 0 {
		ln.busy++
		l.mu.Unlock()
		return l.releaser(who, ln), true
	}
	ch := make(chan struct{})
	ln.queue = append(ln.queue, ch)
	l.mu.Unlock()
	select {
	case <-ch:
		return l.releaser(who, ln), true
	case <-ctx.Done():
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, c := range ln.queue {
		if c == ch {
			// ctx 先结束了:从队列摘掉自己,这个请求永远不会发出
			ln.queue = append(ln.queue[:i], ln.queue[i+1:]...)
			l.drop(who, ln)
			return nil, false
		}
	}
	// 摘不到说明刚被 grant 过(close 过):把槽位还给下一个人
	ln.busy--
	ln.grant()
	l.drop(who, ln)
	return nil, false
}

// releaser 还槽,无论被调多少次只还一次(release 幂等,panic 兜底也不
// 会把同一槽还两遍)。
func (l *lanes) releaser(who string, ln *lane) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			ln.busy--
			ln.grant()
			l.drop(who, ln)
			l.mu.Unlock()
		})
	}
}

// grant 按队列顺序放行:有位子(或限额解除)就让队首走,逐个放。
func (ln *lane) grant() {
	for len(ln.queue) > 0 && (ln.limit <= 0 || ln.busy < ln.limit) {
		ln.busy++
		close(ln.queue[0])
		ln.queue = ln.queue[1:]
	}
}

// drop 忘掉一条没人占也没人等的车道,防 map 泄漏。
func (l *lanes) drop(who string, ln *lane) {
	if ln.busy <= 0 && len(ln.queue) == 0 && l.m[who] == ln {
		delete(l.m, who)
	}
}

// Lane 是一条车道的实时状态:在途、排队、当前限额(给面板看)。
type Lane struct {
	Busy    int `json:"busy"`
	Waiting int `json:"waiting"`
	Limit   int `json:"limit"`
}

// lanesSnapshot 是全部车道的视图(面板 /api/status 的 lanes 键)。
// 只读锁下拍快照,谁也不阻塞。
func (l *lanes) snapshot() map[string]Lane {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]Lane, len(l.m))
	for who, ln := range l.m {
		out[who] = Lane{Busy: ln.busy, Waiting: len(ln.queue), Limit: ln.limit}
	}
	return out
}
