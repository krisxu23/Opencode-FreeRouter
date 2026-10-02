// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package health's second file holds the selector. It is separate from the
// state because pickExit is the one function that must stay a pure function
// of its arguments plus the table: it returns its own candidate snapshot
// instead of writing a module-level "last order", so two concurrent turns
// cannot mix each other's candidate lists into their route records.
package health

import (
	"math"
	"sort"
	"strings"
	"time"

	"freerouter/internal/parse"
	"freerouter/internal/tracelog"
)

// orderSentinelCut 与 JS withOrder 的 1e15 阈值一致:延迟超过它就是「没有延迟
// 数据」的哨兵(Number.MAX_SAFE_INTEGER)在乘法里放大的结果,落盘时写 0
// (JS 写 null),不把 9.2e18 级别的魔数写进面板。
const orderSentinelCut = int64(1000000000000000) // 1e15

// PoolNode is one candidate exit from the pool the engine rebuilt this
// round. A tag absent from the pool is not selectable no matter how healthy
// it looks in the table.
type PoolNode struct {
	Tag     string `json:"tag"`
	Country string `json:"country,omitempty"`
}

// PickRequest asks for the next exit.
type PickRequest struct {
	// Restricted forces B-tier-only candidates. 审计 O15 这里曾有 `Model`
	// (文档说"used only for IsRestrictedModel"),但生产从不读它 —— 判受限的一方
	// (engine)自己算好了再传 Restricted,health 侧再看一次 model 只会让同一个判定
	// 有两处真相(JS 的 pickExit 也带一个从未使用的 model 参数,同源缺陷)。
	Restricted bool
	// Countries are the user's groups in fallback order: US, JP, HK, TW, KR,
	// SG, EU, OTHER. These are groups, not ISO codes, which is why nodes are
	// bucketed through BucketOf before matching.
	Countries []string
	Pool      []PoolNode
	// StickyNode is the session's pinned tag, tried before the pool. Empty
	// when the session has no usable sticky exit.
	StickyNode string
}

// Picked is the chosen exit plus the snapshot that chose it. Order is the
// explanation: "why is it still using that slow egress" is answerable from
// this record alone.
type Picked struct {
	NodeKey string
	Country string
	// ExitIP is empty when the node has no fresh measurement. Callers must
	// treat it as "unknown", not as "a private IP": it is the value the
	// quota and stickiness accounting needs to know about.
	//
	// 审计 O15 说它生产零读取 —— 对,而且报告自己也把「engine 用 ExitIPOf 重新
	// 推导两次」判为**不是缺陷**(JS 同款,第 20 条)。这里保留:它是 Pick 那一刻
	// 解析出的 IP,而重新推导发生在尝试**之后**,那时节点可能已被下一轮探针改判;
	// 生产要的是后者,测试要观察的是前者。删掉字段只会把这条观察换成一次更弱的断言。
	ExitIP string
	// Order holds at most 8 rows and is the candidate snapshot, not the
	// attempt list; the engine turns attempts into TryRow separately.
	Order []tracelog.OrderRow
}

// ranked 是 rank 的原始行:latency/cost 保留哨兵原值参与排序,落盘形状在
// orderRowOf 里压。
type ranked struct {
	node      PoolNode
	bucket    int
	cost      int64
	country   string
	ip        string
	load      int
	latency   int64
	throttled bool
	geo       bool
}

// Pick returns nil when nothing is usable. Callers must not fall back to
// direct egress: a wrong-country answer still proves the exit works, a direct
// one proves nothing.
//
// 候选按 bucket、再按 (load+1)×latency 排序。三个独立的 +1 惩罚只重排、永不
// 排除:被别的会话钉着的出口 IP、正超过软上限的出口 IP、刚撞过配额墙的出口。
// (src/health.js:905-924)
func (h *Health) Pick(req PickRequest) *Picked {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	// 长期占用:每个出口 IP 被多少个活会话钉着(不含调用方自己那个)
	busy := h.busyExitIpsLocked(req.StickyNode)
	// 配额扩散:还在记号有效期内的出口 IP 集合
	quotaIps := h.quotaMarkedExitIpsLocked(now)
	var lastOrder []*ranked
	withOrder := func(hit *ranked) *Picked {
		if hit == nil {
			return nil
		}
		p := &Picked{NodeKey: hit.node.Tag, Country: hit.country, ExitIP: hit.ip}
		for _, r := range lastOrder {
			p.Order = append(p.Order, orderRowOf(r, req.StickyNode))
		}
		return p
	}

	// sticky 优先(src/health.js:1007-1019):命中时 Order 只有它一行 —— 粘性
	// 压过排序是这个模块最容易被误判的行为(「为什么还在用那个慢出口」只能从
	// 这行回答)。rank 为 nil(节点不可用/受限模型遇到非 B)则照常落回池内选路;
	// StickyNode 不在池里同样跳过。
	if req.StickyNode != "" {
		for _, node := range req.Pool {
			if node.Tag != req.StickyNode {
				continue
			}
			if r := h.rankLocked(node, req, busy, quotaIps, now); r != nil {
				lastOrder = []*ranked{r}
				return withOrder(r)
			}
			break
		}
	}

	// countries 是固定分组(US/JP/HK/TW/KR/SG/EU/OTHER),节点的 tag 推断国与
	// 出口 IP 实测国都归到分组后再匹配;byGroup 必须按分组 key,不能按原始国家码
	// (否则 EU/OTHER 分组永远查不到,如 NL→EU、CA→OTHER,src/health.js:1026-1029)。
	// want 保序去重:Set 的插入序就是回退序,不能排字典序。
	want := make([]string, 0, len(req.Countries))
	seen := map[string]bool{}
	for _, g := range req.Countries {
		group := strings.ToUpper(g)
		if group == "" || seen[group] {
			continue
		}
		seen[group] = true
		want = append(want, group)
	}
	byGroup := map[string][]*ranked{}
	for _, node := range req.Pool {
		r := h.rankLocked(node, req, busy, quotaIps, now)
		if r == nil {
			continue
		}
		group := parse.BucketOf(r.country)
		if !seen[group] {
			continue
		}
		byGroup[group] = append(byGroup[group], r)
	}
	// (bucket, cost, tag) 三元组。第三项是 Go 版新增:JS 的 Array.prototype.sort
	// 稳定性由引擎保证,sort.SliceStable 之下两个同 bucket 同 cost 的候选还得有
	// 一个确定次序,否则「为什么选了它」在重放时无法复现。
	sortRanked := func(list []*ranked) {
		sort.SliceStable(list, func(i, j int) bool {
			if list[i].bucket != list[j].bucket {
				return list[i].bucket < list[j].bucket
			}
			if list[i].cost != list[j].cost {
				return list[i].cost < list[j].cost
			}
			return list[i].node.Tag < list[j].node.Tag
		})
	}
	for _, group := range want {
		list := byGroup[group]
		if len(list) == 0 {
			continue
		}
		sortRanked(list)
		// 只留前 8:整池几千个候选全写下来,一条记录就能顶掉一天的量,而这个
		// 顺序的前 8 名已经能解释「为什么选了它」(src/health.js:1037-1040)。
		lastOrder = list
		if len(lastOrder) > 8 {
			lastOrder = lastOrder[:8]
		}
		return withOrder(list[0])
	}
	// 选定分组全部落空:仍然优先给一个可用池内节点而不是直接失败 —— 错国家的
	// 好答案胜过没有答案。直连在这里永远不是候选;受限模型已在 rank 里滤掉非 B。
	// (src/health.js:1044-1048)
	any := make([]*ranked, 0, len(req.Pool))
	for _, node := range req.Pool {
		if r := h.rankLocked(node, req, busy, quotaIps, now); r != nil {
			any = append(any, r)
		}
	}
	if len(any) > 0 {
		sortRanked(any)
		lastOrder = any
		if len(lastOrder) > 8 {
			lastOrder = lastOrder[:8]
		}
		return withOrder(any[0])
	}
	return withOrder(nil)
}

// rankLocked 是 pick 的核心公式(src/health.js:965-999)。返回 nil 的两种情形:
// 节点不可用(dead/冷却中)、受限模型遇到非 B 出口。其余任何节点都参与排序,
// 只重排、不排除。
func (h *Health) rankLocked(node PoolNode, req PickRequest, busy map[string]int, quotaIps map[string]bool, now int64) *ranked {
	r, hasRow := h.nodes[node.Tag]
	if !h.nodeUsableLocked(node.Tag) {
		return nil
	}
	gated := hasRow && r.Tier == TierB
	// 受限模型只认 B 类(唯一真实对话验证过的出口);普通模型 A/B 并用,
	// B 类更快优先(bucket -1)。(src/health.js:969-971)
	if req.Restricted && !gated {
		return nil
	}
	// 出口 IP 走信任窗口(exitIpOf):没有可用量测时按「独立 IP」处理,既不
	// 参与归组也不参与配额扩散 —— 未知不是「和谁共享」。(src/health.js:972-974)
	ip := h.exitIpOfLocked(node.Tag, now)
	// 这个 IP 的负载 = 钉在它上面的会话数 + 正在它上面跑的请求数。两者都算:
	// 前者是长期占用,后者是瞬时压力,配额和上游并发上限按 IP 计。
	// (src/health.js:976-977)
	load := 0
	if ip != "" {
		load += busy[ip]
	}
	load += h.exitBusyCountLocked(ip, now)
	// 被别的会话占着 → 降一级(不是禁用,只是排在空闲 IP 后面)。
	shared := 0
	if load > 0 {
		shared = 1
	}
	// 超过软上限 → 再降一级,整批排到最后。满载是「健康但排队」—— 不排除、
	// 不记失败。(src/health.js:980-982)
	saturated := 0
	if load > exitSoftCap {
		saturated = 1
	}
	// 刚撞过配额墙的降一级,同样只是排后面。按节点和按出口 IP 两种读法都算
	// (理由见 quotaMarkedExitIps)。(src/health.js:983-985)
	throttled := 0
	if h.quotaMarkedLocked(node.Tag, now) || (ip != "" && quotaIps[ip]) {
		throttled = 1
	}
	// 订阅标签标的国家和实测出口国家对不上 → 降一级。同样是「排在对得上的
	// 后面」,不是排除:标签本身就可能错,一个比特的怀疑换不掉一个出口。
	// (src/health.js:986-988)
	geo := 0
	if hasRow && r.GeoMismatch {
		geo = 1
	}
	bucket := shared + saturated + throttled + geo
	if hasRow && r.State == StateAlive {
		// alive 从 0 起步;dead 已被 nodeUsable 过滤,这里只剩 unknown +1
	} else {
		bucket += 1
	}
	if gated {
		bucket -= 1
	}
	// 排序延迟:真实 TTFT 优先(≥3 个新鲜样本),否则退回探测延迟。两者都只做
	// 同 bucket 内的相对比较,绝对值不进任何门槛。无任何数据时是哨兵值 ——
	// 排最后,落盘时写 0。(src/health.js:990-994)
	latency := int64(math.MaxInt64)
	if tt, ok := h.ttftLatencyLocked(node.Tag, now); ok {
		latency = tt
	} else if hasRow {
		// JS 的 `??` 只在缺失时回退,-1(dead 的 latencyMin)原样参战;Go 侧
		// 0 是「没填」(markProbe/noteQuota 不会产出 0),按缺失回退。
		if r.LatencyMin != 0 {
			latency = r.LatencyMin
		} else if r.LatencyMS != 0 {
			latency = r.LatencyMS
		}
	}
	// 同 bucket 内再按 (load+1)×latency 排:负载每多一条就把等效延迟放大一档,
	// 于是「快而挤」会输给「略慢而空」。用乘法而不是先比负载,是为了不让一个
	// 30ms 的热节点输给一个 900ms 的冷节点。(src/health.js:995-998)
	var cost int64
	if latency > orderSentinelCut {
		cost = math.MaxInt64 // 哨兵:排序压底;先判后乘,避免溢出
	} else {
		cost = latency * int64(load+1)
	}
	// effectiveCountry(src/health.js:961-964):实测出口国(恰好 2 位)优先,
	// 否则退回引擎给的池内国家上截 2 位。
	country := ""
	if hasRow && len(r.ExitCountry) == 2 {
		country = r.ExitCountry
	}
	if country == "" {
		country = strings.ToUpper(node.Country)
		if len(country) > 2 {
			country = country[:2]
		}
	}
	return &ranked{
		node:      node,
		bucket:    bucket,
		cost:      cost,
		country:   country,
		ip:        ip,
		load:      load,
		latency:   latency,
		throttled: throttled == 1,
		geo:       geo == 1,
	}
}

// orderRowOf 把 ranked 压成落盘的 OrderRow:哨兵值在这里变 0 —— JS 写 null,
// Go 的 int 写不了 null,0 读起来就是「没量到」,9223372036854775807 在面板上
// 是个看不出含义的魔数(计划修正:不得写 MaxInt64)。
func orderRowOf(r *ranked, stickyNode string) tracelog.OrderRow {
	out := tracelog.OrderRow{
		Tag:       r.node.Tag,
		Country:   r.country,
		IP:        r.ip,
		Bucket:    r.bucket,
		Load:      r.load,
		Throttled: r.throttled,
		Geo:       r.geo,
		Sticky:    r.node.Tag == stickyNode,
	}
	if r.latency > orderSentinelCut {
		out.Latency = 0
		out.Cost = 0
	} else {
		out.Latency = r.latency
		out.Cost = int(r.cost)
	}
	return out
}

// busyExitIpsLocked 当前被活会话钉住的出口 IP,以及各被钉了几个会话。
// (src/health.js:873-903)
// 为什么按 IP 计数:实测 191 个存活节点报出的 exitIp 塌缩成 96 个不同 IP,其中
// 19 个被 2-7 个节点共用;纯按 bucket+latency 排会把每个新会话都灌进同几个快
// IP —— 共享配额正是最先在那里跑光。计数而不是布尔:2 个会话钉着比 1 个更该
// 让路。与 exitBusy 分开记,因为「这个会话钉在这里」是长期事实,「现在正压着
// 一条请求」是瞬时事实,两者都算进负载但不能互相覆盖。
func (h *Health) busyExitIpsLocked(ownSticky string) map[string]int {
	out := map[string]int{}
	now := time.Now().UnixMilli()
	for session, hit := range h.sticky {
		if now-hit.At > ttlOf(hit) {
			// R20:过期行就地删掉。这张表按客户端可控的会话标识建键,而行过去只在
			// 「同一个会话又被读到」时才作废 —— 于是每次 Pick 都要在独占锁下扫一遍
			// 历史会话数,而表长只增不消。这次遍历本来就已经付了,顺手回收是免费的。
			delete(h.sticky, session)
			delete(h.stickyFail, session) // 同一把键的另一张表,一起放手
			continue
		}
		// 调用方自己钉着的那个 tag 永不给自己降级(src/health.js:896)
		if ownSticky != "" && hit.NodeKey == ownSticky {
			continue
		}
		if !h.nodeUsableLocked(hit.NodeKey) {
			continue
		}
		ip := hit.ExitIP
		if ip == "" {
			ip = h.exitIpOfLocked(hit.NodeKey, now)
		}
		if ip != "" {
			out[ip]++
		}
	}
	return out
}
