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
	"sync"
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

// rankedPool 复用 Pick 整池 rank 的行对象:8000 节点 × 每 attempt 一次 Pick,
// 每次 `&ranked{...}` 堆分配是 GC 压力大头。值切片 + 索引传递,排序只排索引,
// 行对象不出借(调用方拿到的是 Picked 拷贝,无别名风险)。
var rankedPool = sync.Pool{New: func() any { return make([]ranked, 0, 256) }}

// Pick returns nil when nothing is usable. Callers must not fall back to
// direct egress: a wrong-country answer still proves the exit works, a direct
// one proves nothing.
//
// 候选按 bucket、再按 (load+1)×latency 排序。三个独立的 +1 惩罚只重排、永不
// 排除:被别的会话钉着的出口 IP、正超过软上限的出口 IP、刚撞过配额墙的出口。
// (src/health.js:905-924)
// pickRanked 把排好序的候选压成 Picked(带前 8 行的决策快照)。O2 之后排序
// 发生在锁外,这个小助手取代了旧闭包对 lastOrder 的就地赋值。
func pickRanked(hit *ranked, sorted []*ranked, stickyNode string) *Picked {
	if hit == nil {
		return nil
	}
	p := &Picked{NodeKey: hit.node.Tag, Country: hit.country, ExitIP: hit.ip}
	for _, r := range sorted {
		p.Order = append(p.Order, orderRowOf(r, stickyNode))
	}
	return p
}

func (h *Health) Pick(req PickRequest) *Picked {
	now := time.Now().UnixMilli()

	// 快照阶段(RLock):只拷贝 rank 需要的输入,不做任何删除。写操作(过期
	// cool/busy/sticky/quota 的懒删)全部延后到选中后的短写锁里提交 ——
	// 过去整池 rank 握着写锁做 CPU 活,8000 节点 × 每 attempt 一次 Pick,
	// 并发 Pick 全串行。快照里的行是值拷贝(ttft/busy/sticky 的指针只读),
	// NoteTtft/NoteExitBusy 整行替换旧指针,锁外读到的旧值仍自洽。
	snap := h.pickSnapshot(now, req.StickyNode, req.Pool)

	// 锁外 rank:纯计算,零共享写。ranked 行从池里复用,排序只排索引。
	// 归还必须还**真正长成了的那一条**:buf 起步 cap 256,现场池 1700-8000,
	// append 必然换 backing 数组 —— 旧写法无条件 Put(buf[:0]) 归还的是原始
	// 小数组,长大的一条每次进 GC,"池复用"名不副实(第六轮审计 F3)。
	buf := rankedPool.Get().([]ranked)
	rankedAll := buf[:0]
	defer func() {
		if cap(rankedAll) > cap(buf) {
			buf = rankedAll
		}
		rankedPool.Put(buf[:0])
	}()
	// 两段式的第一段:pickSnapshot 带 sticky 时只拷了那一个节点(若有)。
	// 对它 rank 成功 → 整池拷贝当场省掉(见 pickSnapshot 注释);失败或
	// 无 sticky → fill 补齐其余池节点。
	stickyHit := (*ranked)(nil)
	if req.StickyNode != "" && len(snap.nodes) > 0 && snap.nodes[0].tag == req.StickyNode {
		if r, ok2 := rankNode(snap.nodes[0], req, snap); ok2 {
			rankedAll = append(rankedAll, r)
			stickyHit = &rankedAll[0]
		}
	}
	var want []string
	var byGroup map[string][]int
	var allIdx []int
	if stickyHit == nil {
		// 两段式的第二段:带 sticky 但首段没定案时**整体重取**一份同刻快照
		// (表与行必须来自同一时刻,见 fillPoolSnapshot 注释)。无 sticky 时
		// pickSnapshot 已经给了全量同刻快照,这里什么都不补 —— 过去无条件调
		// fill 会让无 sticky 的请求白拷两份(表一趟、行一趟),且两趟不同刻。
		if req.StickyNode != "" {
			h.fillPoolSnapshot(snap, now, req.Pool, req.StickyNode)
		}
		want = make([]string, 0, len(req.Countries))
		seen := map[string]bool{}
		for _, g := range req.Countries {
			group := strings.ToUpper(g)
			if group == "" || seen[group] {
				continue
			}
			seen[group] = true
			want = append(want, group)
		}
		byGroup = map[string][]int{}
		allIdx = make([]int, 0, len(snap.nodes))
		for i := range snap.nodes {
			// sticky 已命中则不会走到这里;未命中时 sticky 节点仍参与池排。
			r, ok := rankNode(snap.nodes[i], req, snap)
			if !ok {
				continue
			}
			rankedAll = append(rankedAll, r)
			idx := len(rankedAll) - 1
			allIdx = append(allIdx, idx)
			group := parse.BucketOf(rankedAll[idx].country)
			if !seen[group] {
				continue
			}
			byGroup[group] = append(byGroup[group], idx)
		}
	}

	// 锁外排序:三元组(bucket, cost, tag),与旧 sortRanked 同序。
	sortIdx := func(list []int) {
		sort.SliceStable(list, func(a, b int) bool {
			ra, rb := &rankedAll[list[a]], &rankedAll[list[b]]
			if ra.bucket != rb.bucket {
				return ra.bucket < rb.bucket
			}
			if ra.cost != rb.cost {
				return ra.cost < rb.cost
			}
			return ra.node.Tag < rb.node.Tag
		})
	}

	var hitIdx int = -1
	var orderIdx []int
	switch {
	case stickyHit != nil:
		// sticky 命中:Order 只有它一行(粘性压过排序,见旧注释)。
		hitIdx = 0
		orderIdx = []int{0}
	default:
		for _, group := range want {
			list := byGroup[group]
			if len(list) == 0 {
				continue
			}
			sortIdx(list)
			hitIdx = list[0]
			if len(list) > 8 {
				list = list[:8]
			}
			orderIdx = list
			break
		}
		if hitIdx < 0 && len(allIdx) > 0 {
			// 选定分组全部落空:给一个可用池内节点而不是直接失败。
			sortIdx(allIdx)
			hitIdx = allIdx[0]
			if len(allIdx) > 8 {
				allIdx = allIdx[:8]
			}
			orderIdx = allIdx
		}
	}

	// 选中后短写锁提交:把快照期发现的过期键一次删掉(懒删的合法性:这些键
	// 在快照时刻已过期,删除只影响内存回收,不改变 rank 结论)。
	// ranked 的归还全部交给上面的 defer —— 这里**绝不能再 Put 一次**:
	// 双 Put 会让池里出现两个共享同一 backing 的切片,两个后来的 Pick 各
	// 自 Get 到同一底层数组 → 互相覆盖(第六轮改造中途引入过这一处,删)。
	if hitIdx < 0 {
		h.commitPickSweep(snap)
		return nil
	}
	out := pickRanked(&rankedAll[hitIdx], rankedSlice(rankedAll, orderIdx), req.StickyNode)
	h.commitPickSweep(snap)
	return out
}

// rankedSlice 把索引切片翻译成 pickRanked 要的行指针切片(行仍活在调用方的
// rankedAll 里, pickRanked 只读不存,无别名外泄)。
func rankedSlice(all []ranked, idx []int) []*ranked {
	out := make([]*ranked, 0, len(idx))
	for _, i := range idx {
		out = append(out, &all[i])
	}
	return out
}

// pickSnap 是 Pick 一次选路所需的全部只读输入,RLock 下一次拷完。
// 行是值拷贝;ttft/busy/sticky 的指针只读(写侧整行替换,旧值自洽)。
// sweep* 是快照期发现的已过期键,提交阶段短写锁删掉。
type pickSnap struct {
	nodes       []snapNode
	busy        map[string]int
	quotaIps    map[string]bool
	busyTbl     map[string]int
	sweepCool   []string
	sweepBusy   []string
	sweepSticky []string
	sweepQuota  []string
}

type snapNode struct {
	tag       string
	country   string
	row       row
	hasRow    bool
	coolOk    bool // true = 可用(cool 缺席或已过期);false = 冷却中
	ttft      int64
	hasTtft   bool
	exitIP    string
	quotaSelf bool // 自身配额记号新鲜
}

// pickSnapshot 在 RLock 下拷贝 rank 的全部输入,不做任何删除。
//
// **两段式**(第六轮审计 F2,第七轮复审 A1 修正同刻性):带 sticky 的请求
// (稳态下的主流形状)先只拷那一个节点的 snapNode 进 nodes[0];它 rank 成功
// 就到此为止,整池 8000 行的拷贝当场省掉。它 rank 失败时由 Pick 调
// fillPoolSnapshot **整体重取**一份快照(第二次 RLock,罕见路径付两趟可接受)。
//
// 无 sticky 的请求在这里一次取全 —— 这既是省一趟拷贝,更是同刻性要求:
// 若也走「第一段只取表、第二段补行」,聚合表(busy/busyTbl/quotaIps)与逐行
// 数据来自两个时刻,而间隙可能被写者持整表锁拉长;间隙里节点重探换了
// exitIP、或旧 IP 被并发 NoteExitBusy/NoteQuota 推过阈值,rankNode 就会拿
// 旧表算新行,把「正忙、已被限流」的节点算成零负载而赢下分组 —— 那不是
// 「稍旧」,是错误选路(且硬不变量没破,查不出来)。表与行必须同刻。
func (h *Health) pickSnapshot(now int64, ownSticky string, pool []PoolNode) *pickSnap {
	h.mu.RLock()
	defer h.mu.RUnlock()
	snap := h.pickAggLocked(now, ownSticky)
	if ownSticky != "" {
		for _, node := range pool {
			if node.Tag == ownSticky {
				snap.nodes = append(snap.nodes, h.snapNodeLocked(now, node, snap))
				break
			}
		}
		return snap
	}
	h.appendPoolLocked(snap, now, pool)
	return snap
}

// appendPoolLocked 在当前 RLock 下把池里全部节点拷进 snap(与聚合表同刻)。
//
// 一次性给足容量:满池 8000 节点若沿用 pickAggLocked 的 256 起步会连realloc
// 好几次(每次都是一段 ~200B×n 的拷贝,六审 F3 控的正是这个)。
func (h *Health) appendPoolLocked(snap *pickSnap, now int64, pool []PoolNode) {
	if cap(snap.nodes)-len(snap.nodes) < len(pool) {
		grown := make([]snapNode, 0, len(snap.nodes)+len(pool))
		grown = append(grown, snap.nodes...)
		snap.nodes = grown
	}
	for _, node := range pool {
		snap.nodes = append(snap.nodes, h.snapNodeLocked(now, node, snap))
	}
}

// fillPoolSnapshot 在 sticky 没能定案时**整体重取**一份完整快照:聚合表与
// 逐行数据在同一个 RLock 内产出,并整体替换第一段的结果(第一段那个 sticky
// 节点也丢弃重拷,池排里自然只有它一份)。
//
// 两个被这条整体重取一起消掉的形状(第七轮复审 A):
//   - 表/行不同刻(见 pickSnapshot 注释)会错误选路;
//   - 旧写法靠 `node.Tag == skip` 跳过第一段已拷的节点,而 ownSticky 不在池里
//     时 skip 是零值 "",于是池中 tag 为空的节点被一起静默跳过、从候选里消失
//     (tag 由 app.go 直接透传 Registry,未校验非空)。
//
// ownSticky 必须原样透传给 pickAggLocked(八审 L5):busy 的语义是「别的会话
// 占着这个 IP」,第一段已把 own hold 排除在外;fill 若传 "",own sticky 的
// hold 会被算成外部负载 —— 同一请求走没走 fill,busy 口径漂移一档,own
// sticky 与同 IP 邻居的排序跟着位移。
//
// sweep 列表也必须整体替换,不能只覆盖 busy 表:它们与三张表同刻同源,
// 混着两份会导致 commitPickSweep 漏删或多删(漏删由下一轮补,多删会误清
// 新鲜的 cool/busy 记号)。
func (h *Health) fillPoolSnapshot(snap *pickSnap, now int64, pool []PoolNode, ownSticky string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	fresh := h.pickAggLocked(now, ownSticky)
	fresh.nodes = snap.nodes[:0]
	h.appendPoolLocked(fresh, now, pool)
	*snap = *fresh
}

// pickAggLocked 拷 rank 公式需要的聚合表与 sweep 列表。调用方必须持 RLock。
func (h *Health) pickAggLocked(now int64, ownSticky string) *pickSnap {
	snap := &pickSnap{
		nodes:    make([]snapNode, 0, 256),
		busy:     map[string]int{},
		quotaIps: map[string]bool{},
		busyTbl:  map[string]int{},
	}
	// busy 表快照:只拷窗内计数,过窗只记不删(提交阶段删)。
	for ip, b := range h.busy {
		if b == nil {
			continue
		}
		if now-b.At > exitBusyStale {
			snap.sweepBusy = append(snap.sweepBusy, ip)
			continue
		}
		snap.busyTbl[ip] = b.Count
	}
	// sticky 全扫(读侧),过期只记不删。
	for session, hit := range h.sticky {
		if hit == nil || now-hit.At > ttlOf(hit) {
			snap.sweepSticky = append(snap.sweepSticky, session)
			continue
		}
		if ownSticky != "" && hit.NodeKey == ownSticky {
			continue
		}
		// 可用性用快照内的行判定(不调带删的 nodeUsableLocked)。
		usable := true
		if r, ok := h.nodes[hit.NodeKey]; ok && r.State == StateDead {
			usable = false
		} else if c, ok := h.cool[hit.NodeKey]; ok && c.Until > now {
			usable = false
		}
		if !usable {
			continue
		}
		ip := hit.ExitIP
		if ip == "" {
			if r, ok := h.nodes[hit.NodeKey]; ok && r.ExitIP != "" {
				at := r.ExitIPAt
				if at == 0 {
					at = r.LastProbeAt
				}
				if now-at <= exitIPTTL {
					ip = r.ExitIP
				}
			}
		}
		if ip != "" {
			snap.busy[ip]++
		}
	}
	// quota:只走索引,过期只记不删。
	for key := range h.quotaTags {
		r, ok := h.nodes[key]
		if !ok || !(r.LastQuotaAt > 0 && now-r.LastQuotaAt < quotaMark) {
			snap.sweepQuota = append(snap.sweepQuota, key)
			continue
		}
		if r.ExitIP != "" {
			at := r.ExitIPAt
			if at == 0 {
				at = r.LastProbeAt
			}
			if now-at <= exitIPTTL {
				snap.quotaIps[r.ExitIP] = true
			}
		}
	}
	return snap
}

// snapNodeLocked 拷单个池节点的 rank 输入(含 sweepCool 记账)。调用方持 RLock。
func (h *Health) snapNodeLocked(now int64, node PoolNode, snap *pickSnap) snapNode {
	r, hasRow := h.nodes[node.Tag]
	sn := snapNode{tag: node.Tag, country: node.Country, row: r, hasRow: hasRow}
	// cool:冷却中不可用;过期只记不删。
	if c, ok := h.cool[node.Tag]; ok {
		if c.Until > now {
			sn.coolOk = false
		} else {
			sn.coolOk = true
			snap.sweepCool = append(snap.sweepCool, node.Tag)
		}
	} else {
		sn.coolOk = true
	}
	// ttft:只读中位数(写侧已算好,见 ttftRow 注释)。
	if t, ok := h.ttft[node.Tag]; ok && t.hasMedian && now-t.At <= ttftFresh {
		sn.ttft, sn.hasTtft = t.median, true
	}
	// exitIP:信任窗口内才有效(与 exitIpOfLocked 同口径,只读不续命)。
	if hasRow && r.ExitIP != "" {
		at := r.ExitIPAt
		if at == 0 {
			at = r.LastProbeAt
		}
		if now-at <= exitIPTTL {
			sn.exitIP = r.ExitIP
		}
	}
	// 自身配额记号。
	sn.quotaSelf = hasRow && r.LastQuotaAt > 0 && now-r.LastQuotaAt < quotaMark
	return sn
}

// rankNode 是 rankLocked 的无锁版:同一公式,只读快照。ok=false 的两种情形
// 与旧函数一致:节点不可用(dead/冷却中)、受限模型遇到非 B。
func rankNode(sn snapNode, req PickRequest, snap *pickSnap) (ranked, bool) {
	var zero ranked
	if sn.hasRow && sn.row.State == StateDead {
		return zero, false
	}
	if !sn.coolOk {
		return zero, false
	}
	gated := sn.hasRow && sn.row.Tier == TierB
	if req.Restricted && !gated {
		return zero, false
	}
	ip := sn.exitIP
	load := 0
	if ip != "" {
		load += snap.busy[ip]
	}
	load += snap.busyTbl[ip]
	shared := 0
	if load > 0 {
		shared = 1
	}
	saturated := 0
	if load > exitSoftCap {
		saturated = 1
	}
	throttled := 0
	if sn.quotaSelf || (ip != "" && snap.quotaIps[ip]) {
		throttled = 1
	}
	geo := 0
	if sn.hasRow && sn.row.GeoMismatch {
		geo = 1
	}
	bucket := shared + saturated + throttled + geo
	if sn.hasRow && sn.row.State == StateAlive {
	} else {
		bucket += 1
	}
	if gated {
		bucket -= 1
	}
	latency := int64(math.MaxInt64)
	if sn.hasTtft {
		latency = sn.ttft
	} else if sn.hasRow {
		if sn.row.LatencyMin != 0 {
			latency = sn.row.LatencyMin
		} else if sn.row.LatencyMS != 0 {
			latency = sn.row.LatencyMS
		}
		if latency <= 0 {
			latency = orderSentinelCut + 1
		}
	}
	var cost int64
	if latency > orderSentinelCut {
		cost = math.MaxInt64
	} else {
		cost = latency * int64(load+1)
	}
	country := ""
	if sn.hasRow && len(sn.row.ExitCountry) == 2 {
		country = sn.row.ExitCountry
	}
	if country == "" {
		country = strings.ToUpper(sn.country)
		if len(country) > 2 {
			country = country[:2]
		}
	}
	return ranked{
		node:      PoolNode{Tag: sn.tag, Country: sn.country},
		bucket:    bucket,
		cost:      cost,
		country:   country,
		ip:        ip,
		load:      load,
		latency:   latency,
		throttled: throttled == 1,
		geo:       geo == 1,
	}, true
}

// commitPickSweep 短写锁提交快照期的懒删。
func (h *Health) commitPickSweep(snap *pickSnap) {
	if len(snap.sweepCool)+len(snap.sweepBusy)+len(snap.sweepSticky)+len(snap.sweepQuota) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, k := range snap.sweepSticky {
		if hit, ok := h.sticky[k]; ok && now-hit.At > ttlOf(hit) {
			delete(h.sticky, k)
			delete(h.stickyFail, k)
		}
	}
	for _, k := range snap.sweepQuota {
		if r, ok := h.nodes[k]; !ok || !(r.LastQuotaAt > 0 && now-r.LastQuotaAt < quotaMark) {
			delete(h.quotaTags, k)
		}
	}
	for _, k := range snap.sweepCool {
		if c, ok := h.cool[k]; ok && c.Until <= now {
			delete(h.cool, k)
		}
	}
	for _, ip := range snap.sweepBusy {
		if r, ok := h.busy[ip]; ok && now-r.At > exitBusyStale {
			delete(h.busy, ip)
		}
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
