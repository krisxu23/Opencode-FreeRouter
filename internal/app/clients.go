// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"net/http"
	"sync"

	"freerouter/internal/health"
)

// exitClientLimit 是一个代内可以同时存活的出口 client 数(O3)。免费档实测一轮
// 池子里被真实请求命中的出口远少于此;上限只在异常路径(一个订阅把几千个出口
// 都探了一遍)才生效,那时挤掉的也只是最久没用过的空闲池。
const exitClientLimit = 32

// exitClientCache 是按「出站代数 + 出口 tag」复用的流式 client 缓存。
//
// 为什么必须有:engine 的 Dialer 回调过去每次尝试都新建一个 *http.Client。
// 两个后果 —— 其一,出口的连接池每轮从零开始(TLS 握手按请求数累加,而
// http.Transport 的每主机空闲上限本就只有默认值 2);其二,被丢掉的 client 没有
// 人调用 CloseIdleConnections,换过一轮出口就在进程里攒下一批半开 socket,直到
// GC 才回收。
//
// 为什么必须按**代数**而不是只按 tag:一次 client 里绑的是 `host.Dialer(tag)` 当
// 时给出的拨号闭包。rebuild 的 SyncOutbounds 换掉出站之后继续复用旧 client,
// 等于把请求拨到一条已经被撤下的出站上 —— 这正是 NewClient 的注释里「a stale
// pooled connection to a deleted node is not cheap」那条不能犯的错误。所以每次
// SyncOutbounds 成功都换代,旧代的条目在下次访问时清掉并关闭空闲连接。
type exitClientCache struct {
	mu      sync.Mutex
	limit   int
	gen     uint64
	order   []string // 当前代内的 LRU,最久未用在前
	clients map[string]*http.Client
}

func newExitClientCache(limit int) *exitClientCache {
	if limit <= 0 {
		limit = exitClientLimit
	}
	return &exitClientCache{limit: limit, clients: map[string]*http.Client{}}
}

// get 返回 tag 在当前代数下的 client,不存在时用 build 造一个。build 只在真正
// 需要新建时被调用一次,调用方在里面去要 host.Dialer(tag)。
func (c *exitClientCache) get(gen uint64, tag string, build func() (*http.Client, error)) (*http.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen != c.gen {
		// 换代:旧代的所有 client 连同其空闲连接一起丢掉。这里的 Close 是整条
		// 路径上唯一一处会主动释放出口 socket 的地方,不能省着。
		for key, cl := range c.clients {
			cl.CloseIdleConnections()
			delete(c.clients, key)
		}
		c.order = nil
		c.gen = gen
	}
	key := tag
	if cl, ok := c.clients[key]; ok {
		c.touchLocked(key)
		return cl, nil
	}
	cl, err := build()
	if err != nil {
		return nil, err
	}
	c.clients[key] = cl
	c.order = append(c.order, key)
	for len(c.order) > c.limit {
		evict := c.order[0]
		c.order = c.order[1:]
		if old, ok := c.clients[evict]; ok {
			old.CloseIdleConnections()
			delete(c.clients, evict)
		}
	}
	return cl, nil
}

// touchLocked 把 key 移到 LRU 尾部(调用方已持锁)。用「移除 + 追加」而不是
// 原地交换:上限只有几十个,换来的是不会写错的实现。
func (c *exitClientCache) touchLocked(key string) {
	for i, k := range c.order {
		if k == key {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}
	c.order = append(c.order, key)
}

// len 是当前代内的 client 数(测试与诊断用)。
func (c *exitClientCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.clients)
}

// poolBox 缓存「注册表 → engine 候选池」这份派生视图(O10)。
//
// 过去 engine 每个请求都重算一次:reg.All() 的全量浅拷贝(池上限 8000)加每 tag
// 一次 parse.CountryOf —— 那是「括号名 → ISO 双字母 → 28 条 (?i) 关键词 → 国旗
// 正则」的四级扫描。两者都是纯函数,只在成员关系变化时才变,所以按 registry 的
// 代数缓存(JS 那边 pool 常驻、country 直接长在节点对象上,本来就没有这笔账)。
//
// 返回的切片是共享的:调用方只能读,不能原地排序。今天 engine 只做两件事 —— 按
// 排除集拷一份候选、把候选交给 Pick,而 Pick 排的是它自己新建的 ranked 切片。这条
// 约束写在这里,是为了下一个改 engine 的人不至于顺手去 sort req.Pool。
type poolBox struct {
	mu     sync.Mutex
	gen    uint64
	cached []health.PoolNode
}

// get 在代数没变时返回缓存,否则用 build 重建。
//
// 代数**倒退**(build 期间发生了 Merge,拿着旧代数来取)时同样重建:这里不试图
// 记住"见过的最大代数",因为那会让一次迟到的旧读把新视图永久钉住 —— 重建只是
// 多算一次,钉住才是错误。
func (b *poolBox) get(gen uint64, build func() []health.PoolNode) []health.PoolNode {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cached != nil && b.gen == gen {
		return b.cached
	}
	out := build()
	b.cached = out
	b.gen = gen
	return out
}
