// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"fmt"
	"net/http"
	"sync"

	"freerouter/internal/health"
)

// exitClientLimit 是一个代内可以同时存活的出口 client 数(O3)。免费档实测一轮
// 池子里被真实请求命中的出口远少于此;上限只在异常路径(一个订阅把几千个出口
// 都探了一遍)才生效,那时挤掉的也只是最久没用过的空闲池。
const exitClientLimit = 32

// genChurnLimit 是「构建期间一直撞上换代」的重试上限。热插风暴下与其无限重试,
// 不如把这一次取用判成失败 —— engine 对 Dialer 失败的处理是换下一个出口,而不是
// 把一个可能绑着旧出站的 client 塞进缓存。
const genChurnLimit = 4

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

// get 返回 tag 在当前代数下的 client,不存在时用 build 造一个。
//
// build 一律在**锁外**跑(整分支评审 RISK-4):build 里是 host.Dialer(tag),
// 它要取 sing-box 的读锁,而 SyncOutbounds 在整个热插期间独占那把锁(实测每轮
// churn 1700-1900 个 tag)。锁内跑 build 的写法会让「一次换代后的第一个 miss」
// 持着全局缓存锁阻塞在 sbx 写锁上,把其它所有出口的取用排在后面 —— 与本批提交
// 自己在 registry(R15)/health/stats 三处确立的「锁内只做内存活」纪律相反。
//
// 代数用**取值函数**而不是传值:传一个固定值时,一次持续换代能让调用永远落不到
// 地;取函数则每轮重试都看见最新代数,并在「本次构建前后代数不一致」时丢弃这一
// 份(它可能绑着刚被撤下的出站),重试上限 genChurnLimit 之后判失败而不是硬塞。
func (c *exitClientCache) get(gen func() uint64, tag string, build func() (*http.Client, error)) (*http.Client, error) {
	for attempt := 0; ; attempt++ {
		want := gen()

		c.mu.Lock()
		if want != c.gen {
			// 换代:旧代的所有 client 连同其空闲连接一起丢掉。这里是整条路径上
			// 唯一主动释放出口 socket 的地方,不能省着。
			for key, cl := range c.clients {
				cl.CloseIdleConnections()
				delete(c.clients, key)
			}
			c.order = nil
			c.gen = want
		}
		if cl, ok := c.clients[tag]; ok {
			c.touchLocked(tag)
			c.mu.Unlock()
			return cl, nil
		}
		c.mu.Unlock()

		cl, err := build()
		if err != nil {
			return nil, err
		}

		after := gen()

		c.mu.Lock()
		if after != want || c.gen != want {
			// 构建期间世界动了:这一份可能是旧出站,收掉它的空闲连接再来一轮。
			c.mu.Unlock()
			cl.CloseIdleConnections()
			if attempt >= genChurnLimit {
				return nil, fmt.Errorf("app: 出口 %q 的 client 构建连续 %d 次撞上热插换代", tag, attempt+1)
			}
			continue
		}
		if winner, ok := c.clients[tag]; ok {
			// 另一个请求已经建好了同一出口的 client:输的这一份丢掉,不然它会
			// 在进程里留一组没人再用的空闲连接。
			c.touchLocked(tag)
			c.mu.Unlock()
			cl.CloseIdleConnections()
			return winner, nil
		}
		c.clients[tag] = cl
		c.order = append(c.order, tag)
		for len(c.order) > c.limit {
			evict := c.order[0]
			c.order = c.order[1:]
			if old, ok := c.clients[evict]; ok {
				old.CloseIdleConnections()
				delete(c.clients, evict)
			}
		}
		c.mu.Unlock()
		return cl, nil
	}
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
//
// build 同样在锁外跑(整分支评审 RISK-4):reg.All() 取的是注册表那把锁的读侧,
// 而一轮 rebuild 的 Merge/RetainOnly 持着它的写侧几千条 —— 持着 poolBox.mu 排队
// 在那把锁上,等于把所有并发请求串到一次重建后面。
type poolBox struct {
	mu     sync.Mutex
	gen    uint64
	cached []health.PoolNode
}

// get 在代数没变时返回缓存,否则按当前代数重建。
//
// 代数**倒退**或构建期间被改动时重建(见 exitClientCache.get 的同一条纪律):
// 内容与其代数标注必须成对,否则一次迟到的旧读能把新视图永久钉住。
func (b *poolBox) get(gen func() uint64, build func() []health.PoolNode) []health.PoolNode {
	for attempt := 0; ; attempt++ {
		want := gen()

		b.mu.Lock()
		if b.cached != nil && b.gen == want {
			out := b.cached
			b.mu.Unlock()
			return out
		}
		b.mu.Unlock()

		out := build()
		after := gen()

		b.mu.Lock()
		if after != want {
			if attempt >= genChurnLimit {
				// 一直在变:把这一份直接交出去但不落缓存,下一发重新对齐。
				b.mu.Unlock()
				return out
			}
			b.mu.Unlock()
			continue
		}
		if existing := b.cached; existing != nil && b.gen == want {
			b.mu.Unlock()
			return existing
		}
		b.cached = out
		b.gen = want
		b.mu.Unlock()
		return out
	}
}
