// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package health owns two orthogonal per-node facts and the scheduling that
// consumes them: a coarse alive/dead/unknown gate, and a B/A/untiered verdict
// on whether the exit can serve the region-gated models. One row per node, not
// a model x node matrix: the pool churns 1700-1900 tags per rebuild, and a
// matrix keyed by model lost every verdict before it could be reused, so
// region-gated routing silently degraded to "any node" and every turn hit
// 403 RegionError.
package health

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"freerouter/internal/nodeprobe"
	"freerouter/internal/parse"
	"freerouter/internal/persistence"
)

// NodeState is the coarse gate. Unknown is usable on purpose: unprobed must
// not mean unavailable.
type NodeState string

const (
	StateUnknown NodeState = "unknown"
	StateAlive   NodeState = "alive"
	StateDead    NodeState = "dead"
)

// Tier is the refined gate. A means the coarse gate passed, so any
// non-gated model works. B means A plus a real minimal conversation with the
// region-gated probe model succeeded.
type Tier string

const (
	TierNone Tier = ""
	TierA    Tier = "A"
	TierB    Tier = "B"
)

const (
	// RegionProbeModel is the single gated probe model. Both muse-spark rows
	// are gated identically and their verdicts measured 189 vs 198 nodes, so a
	// second probe would double the cost for nothing.
	RegionProbeModel = "muse-spark-1.3-contributor-free"

	// stickyTTLBase is the fallback TTL. It is not a constant of nature: the
	// real value is decided by the last measured saving, per stickyTTLOf.
	stickyTTLBase    = 30 * 60 * 1000
	cacheWorth       = 1024
	cacheCold        = 5 * 60 * 1000
	cacheStickKeep   = 2 * 60 * 60 * 1000
	cacheFresh       = 10 * 60 * 1000
	exitIPTTL        = 4 * 60 * 60 * 1000
	exitSoftCap      = 2
	exitBusyStale    = 10 * 60 * 1000
	quotaMark        = 90 * 1000
	cooldownBase     = 60 * 1000
	cooldownMaxShift = 3
	ttftSamples      = 8
	ttftMinSamples   = 3
	ttftFresh        = 6 * 60 * 60 * 1000
	// stickyFailCap 是单会话粘性连败计数的上限:熔断阈值 2,正常语义下计数
	// 到 2 即换出口,不再增长;上限纯防 session 可控键的溢出。
	stickyFailCap = 1 << 20
)

// row is one node's health, and the exact on-disk shape of
// data/node-health.json, so a legacy file loads without translation. The
// persisted set is deliberately just these ten fields: cooling is recomputed
// every round and must never survive a restart.
//
// omitempty 的取舍照 JS 写盘的形状逐字段核对过（差分 A9 钉住）：markProbe
// 构造的行**恒带** exitIp/exitCountry/geoMismatch（false/空串也写，JS 的对象
// 字面量没有省略这回事），所以这三个字段不能加 omitempty；exitIpAt/tier/
// lastQuotaAt 在 JS 里是 undefined（JSON.stringify 丢键），omitempty 正确。
type row struct {
	State       NodeState `json:"state"`
	LatencyMS   int64     `json:"latencyMs"`
	LatencyMin  int64     `json:"latencyMin"`
	ExitIP      string    `json:"exitIp"`
	ExitIPAt    int64     `json:"exitIpAt,omitempty"`
	ExitCountry string    `json:"exitCountry"`
	GeoMismatch bool      `json:"geoMismatch"`
	LastProbeAt int64     `json:"lastProbeAt,omitempty"`
	// Streak 是连续失败的 pass 轮数(1.3.0 双档状态机):热区连续 2 轮失败降
	// 冷区,冷区连续 3 轮失败触发「彻底删除」;任何一次通过清零。持久化是为了
	// 重启后删除计数不归零。
	Streak      int   `json:"streak,omitempty"`
	Tier        Tier  `json:"tier,omitempty"`
	LastQuotaAt int64 `json:"lastQuotaAt,omitempty"`
	// NeverAlive 标记「从未活过」:首探判死的行置 true,第一次 alive 清掉。
	// 烂水池里这类行占 90%+,冷区删除对它们用更短的门槛(2 轮),别让从没证明
	// 过价值的节点占着池位和探测预算。落盘(重启后仍记得谁没活过)。
	NeverAlive bool `json:"neverAlive,omitempty"`
}

// NodeView is row plus the cooling overlay the panel shows. It is a separate
// type so persisting a snapshot cannot accidentally write the overlay out.
type NodeView struct {
	row
	CoolingUntil    int64 `json:"coolingUntil,omitempty"`
	CoolingFailures int   `json:"coolingFailures,omitempty"`
}

// MergeInto 把这个观测行逐键写进调用方的 map(O2)。
//
// /api/status 过去对**每个节点**做一次 json.Marshal 加一次 json.Unmarshal,只为
// 把行合并进已经带着 tag/country 的 map —— 每 5 秒一轮、池上限 8000 个节点。
// 手写这份 map 的前提是逐键复刻 encoding/json 的输出:row 上面那段注释记过哪
// 三个字段恒出现(即使为零值,JS 的对象字面量没有"省略"这回事)、哪三个是
// omitempty(JS 侧是 undefined,序列化时整个键丢掉),cooling 两项同理。
// 两者的相等性由 TestNodeViewMergeIntoMatchesJSONShape 钉住,不是「看起来一样」。
func (v NodeView) MergeInto(row map[string]any) {
	row["state"] = string(v.State)
	row["latencyMs"] = v.LatencyMS
	row["latencyMin"] = v.LatencyMin
	row["exitIp"] = v.ExitIP
	if v.ExitIPAt != 0 {
		row["exitIpAt"] = v.ExitIPAt
	}
	row["exitCountry"] = v.ExitCountry
	row["geoMismatch"] = v.GeoMismatch
	if v.LastProbeAt != 0 {
		row["lastProbeAt"] = v.LastProbeAt
	}
	if v.Tier != TierNone {
		row["tier"] = string(v.Tier)
	}
	if v.LastQuotaAt != 0 {
		row["lastQuotaAt"] = v.LastQuotaAt
	}
	if v.NeverAlive {
		row["neverAlive"] = true
	}
	if v.CoolingUntil != 0 {
		row["coolingUntil"] = v.CoolingUntil
	}
	if v.CoolingFailures != 0 {
		row["coolingFailures"] = v.CoolingFailures
	}
}

// sticky is a session pinned to an egress IP, not to a node: the prompt cache
// is accounted per egress (0% hit rate under random rotation vs 99.8% under a
// fixed one), and the pool is heavily clustered, so "same IP, different node"
// is one interchangeable resource.
type sticky struct {
	NodeKey      string
	ExitIP       string
	At           int64
	TTLMS        int64
	CacheRead    float64
	CacheAt      int64
	PromptTokens float64
}

type busy struct {
	Count int
	At    int64
}

type cooling struct {
	Failures int
	Until    int64
}

type ttftRow struct {
	Samples []int64
	At      int64
	// median/hasMedian 是写入时算好的中位数(O12)。rankLocked 对**每个候选**都要
	// 读一次 TTFT,而池上限 8000 —— 过去那里每次都拷一遍 ≤8 个样本再排序,一次
	// Pick 就是上千次小排序。排序结果只取决于样本集合,而样本集合只在本函数改写
	// 时变,所以在写的一侧算一次就够。样本不足最低数量时 hasMedian 为 false,
	// 读侧的「样本数」与「新鲜度」两道判据原样保留。
	median    int64
	hasMedian bool
}

// Health is the whole scheduling state. Every exported method takes h.mu
// briefly: Pick reads a snapshot under RLock, ranks it **outside** the lock,
// and commits lazy deletions under a short write lock (六审 F7 同步:旧注释说
// "Pick holds it for its whole body" 是 1.2.x 的写锁模型,读锁化后已不成立).
// No method here ever blocks on IO: Persist snapshots under the lock and
// writes outside it.
//
// 持久化裁决(对计划类型块的有意偏离):计划写的是 sync.Mutex +
// *persistence.Store,但 Store 的值类型是 map[string]any,与 row 不匹配,逐方法
// JSON 往返不可接受。改按 internal/registry 的做法直接持 RWMutex + 五个 map,
// file 只存路径,Load/Persist 各做一次带类型的读写;cooling/sticky/busy/ttft
// 一律不落盘,row 的十个字段就是落盘全集。
type Health struct {
	mu    sync.RWMutex
	file  string
	nodes map[string]row
	// quotaTags 是 LastQuotaAt>0 的行索引:quotaMarkedExitIpsLocked 从「每次
	// Pick 扫全池」变「扫少数中招者」(P3);行删除后的残留由该函数懒清理。
	quotaTags map[string]struct{}
	sticky    map[string]*sticky
	busy      map[string]*busy
	cool      map[string]*cooling
	ttft      map[string]*ttftRow
	// stickyFail counts consecutive pre-content failures on the session's
	// sticky exit. A node can be alive on the probe and still fail every real
	// turn on transport; without this fuse one session re-hits the same dead
	// exit for the full TTL, paying one wasted failure per request.
	stickyFail map[string]int
	// flushMu 把所有写盘(只有 Persist 一处)串行化。快照在 mu 内拷贝、IO 在
	// mu 外,但两个并发 Persist 仍会并发 WriteJSONFile 同一个文件(各自的 temp
	// 文件不同,坏的是 rename 的先后顺序 → 旧快照覆盖新快照)。
	flushMu sync.Mutex
}

// NewHealth opens (or creates) the health file. An empty file path disables
// persistence entirely, which is how the tests get isolation for free.
func NewHealth(file string) *Health {
	h := &Health{
		file:       file,
		nodes:      map[string]row{},
		quotaTags:  map[string]struct{}{},
		sticky:     map[string]*sticky{},
		busy:       map[string]*busy{},
		cool:       map[string]*cooling{},
		ttft:       map[string]*ttftRow{},
		stickyFail: map[string]int{},
	}
	if file != "" {
		// JS setHealthFile 打开 store 后立刻 loadHealth(src/health.js:211-214)。
		// 坏文件按空表起步:JS 的 JsonStore 对损坏文件是「另存 + 重置」,健康表
		// 全是探测可重建的结论,带着坏文件拒绝启动反而更糟。
		_ = h.Load()
	}
	return h
}

// diskFile 是 data/node-health.json 的落盘形状 {"nodes":{tag:row}},与 JS 版
// 逐字段一致(loadHealth 只认 v.state === 'alive' | 'dead')。
type diskFile struct {
	Nodes map[string]row `json:"nodes"`
}

// Load re-reads the health file, replacing the node table. A missing file is
// the first run (empty table); a corrupt file is an error for the explicit
// caller so it can quarantine instead of silently rewriting.
//
// 只载入 alive/dead 两类行(src/health.js:473):unknown 行(旧版落盘的半成品、
// noteQuota 造的临时行)不进内存表 —— 它们没有可达性结论,留在表里只会让
// 「上次没探完」冒充「这次不用探」。
func (h *Health) Load() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.file == "" {
		return nil // JS store === null 的等价:没有文件就没有可载入的东西
	}
	var df diskFile
	if err := persistence.ReadJSONFile(h.file, &df); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			h.nodes = map[string]row{}
			return nil
		}
		return fmt.Errorf("health: 载入 %s: %w", h.file, err)
	}
	nodes := make(map[string]row, len(df.Nodes))
	for key, r := range df.Nodes {
		if r.State != StateAlive && r.State != StateDead {
			continue // src/health.js:473
		}
		nodes[key] = r
	}
	h.nodes = nodes
	// P3b:按盘上数据重建配额记号索引(重启后 Pick 的配额降级不丢)。
	h.quotaTags = map[string]struct{}{}
	for key, r := range h.nodes {
		if r.LastQuotaAt > 0 {
			h.quotaTags[key] = struct{}{}
		}
	}
	return nil
}

// Persist writes the whole node table to disk synchronously. 托盘退出是
// taskkill /F 硬杀、exit 钩子不执行(src/health.js:480-482),所以这里同步落盘
// 而不是等去抖;调用点是每轮 rebuild/probe 一次。快照在锁内拷贝、写盘在锁外
// —— 锁内不做 IO。cooling/sticky/busy/ttft 一律不落盘。
func (h *Health) Persist() error {
	if h.file == "" {
		return nil
	}
	h.flushMu.Lock()
	defer h.flushMu.Unlock()
	h.mu.Lock()
	// R9:顺带回收过窗的在途计数条目。这张表按出口 IP 建键,订阅轮换会让
	// IP 不断换代,不回收就是无界增长。选这里是因为 Persist 每轮 rebuild/
	// probe 各一次 —— 频率远低于每次请求,却足以跟上 IP 换代速度。
	h.pruneBusyLocked(time.Now().UnixMilli())
	snap := make(map[string]row, len(h.nodes))
	for key, r := range h.nodes {
		snap[key] = r
	}
	h.mu.Unlock()
	if err := persistence.WriteJSONFile(h.file, diskFile{Nodes: snap}, true); err != nil {
		return fmt.Errorf("health: %w", err)
	}
	return nil
}

// ---- ttft(真实首字延迟) -----------------------------------------------------

// NoteTtft 记一次真实请求的首字延迟(成功出内容的那一次才记 —— 半截流不能代表
// 这个出口的常态速度)。(src/health.js:165-179)
//
// 为什么不用探测延迟代替:探测量的是「TCP/TLS/Cloudflare 那一段」,用户等的是
// 「厂商开始吐第一个 token」,中间隔着排队、模型冷启、上游限速 —— 代理到厂商
// 30ms 与 3000ms 的节点在 liveness 探测下毫无区别,在真实请求下差两个数量级。
// 只做同 bucket 内的次序修正,不改门槛也不改分桶。
// 不落盘:TTFT 是当前质量信号,跨重启的旧值比没有更糟(src/health.js:154-155)。
//
// 判据是 <0 而不是 <=0(第六轮审计):0ms 是**合法量测**(本机回环/极快出口
// 上 time.Since 真的会取到 0),旧写法把它连同负数一起丢掉,而 stats 侧早已
// 裁决「0 也算一次真实量测」—— 两侧口径必须一致,否则快出口的 TTFT 样本
// 系统性偏大(只有慢的进得了窗口)。
func (h *Health) NoteTtft(nodeKey string, ms int64) {
	if nodeKey == "" || ms < 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var samples []int64
	if r, ok := h.ttft[nodeKey]; ok {
		samples = append(samples, r.Samples...)
	}
	samples = append(samples, ms)
	// 窗口满了丢最老的:样本只有 8 个,push+shift 的直白写法比环形缓冲划算
	// (src/health.js:175-177)。
	if len(samples) > ttftSamples {
		samples = samples[len(samples)-ttftSamples:]
	}
	row := &ttftRow{Samples: samples, At: time.Now().UnixMilli()}
	if len(samples) >= ttftMinSamples {
		row.median, row.hasMedian = medianOf(samples)
	}
	h.ttft[nodeKey] = row
}

// medianOf 排序取中位数:奇数个取中间,偶数个取中间两个的均值并 Round —— 与
// TtftSnapshot 面板显示的是同一个数(同一个值算两遍不该差 0.0001)。排序在**副本**
// 上做,调用方的样本切片要保持插入序(那是「最近 8 次」的窗口语义)。
func medianOf(samples []int64) (int64, bool) {
	if len(samples) < ttftMinSamples {
		return 0, false
	}
	sorted := append([]int64(nil), samples...)
	slices.Sort(sorted)
	mid := len(sorted) >> 1
	if len(sorted)%2 == 1 {
		return sorted[mid], true
	}
	return int64(math.Round(float64(sorted[mid-1]+sorted[mid]) / 2)), true
}

// ttftLatencyLocked 这个节点当前该用于排序的延迟(ms);false = 没有够新的
// TTFT 样本。(src/health.js:181-196)
// 样本数与新鲜度都在这里把关,rank 不需要知道中位数窗口的存在。样本 <3 只用
// 探测延迟 —— 1 个样本的延迟不是延迟,只是噪声。
//
// 中位数取 NoteTtft 写入时算好的那份(O12):这里被 rankLocked 对**每个候选**
// 调一次,过去每次都要「拷 ≤8 个样本 + 排序」,一次 Pick 就是上千次小排序。
// 数值与原来逐字相同(medianOf 就是当年内联的那四行)。
func (h *Health) ttftLatencyLocked(nodeKey string, now int64) (int64, bool) {
	r, ok := h.ttft[nodeKey]
	if !ok || !r.hasMedian {
		return 0, false
	}
	if now-r.At > ttftFresh {
		return 0, false
	}
	return r.median, true
}

// TtftView 是 ttftSnapshot 的行形状(面板/测试观测用)。(src/health.js:198-206)
type TtftView struct {
	NodeKey string  `json:"nodeKey"`
	Samples []int64 `json:"samples"`
	N       int     `json:"n"`
	At      int64   `json:"at"`
	Fresh   bool    `json:"fresh"`
}

// TtftSnapshot 当前采到的 TTFT 观测。按 NodeKey 排序:Go map 无序,面板与
// 差分测试都需要稳定输出(JS 靠 Map 插入序,这里没有)。
func (h *Health) TtftSnapshot() []TtftView {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now().UnixMilli()
	keys := make([]string, 0, len(h.ttft))
	for key := range h.ttft {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	out := make([]TtftView, 0, len(keys))
	for _, key := range keys {
		r := h.ttft[key]
		samples := append([]int64(nil), r.Samples...)
		out = append(out, TtftView{NodeKey: key, Samples: samples, N: len(samples), At: r.At, Fresh: now-r.At <= ttftFresh})
	}
	return out
}

// ---- 节点行的增删 -------------------------------------------------------------

// Forget 删掉一个离开池子的节点(探测失败淘汰后调用)。(src/health.js:227-234)
// TTFT 观测跟着节点走:节点名可能明天被另一个订阅条目复用(去重按配置指纹,
// 换了服务器/凭据就是另一条线路),把旧出口的速度记在新出口头上会让排序错一个
// 量级。冷却同理(R17):JS 靠单线程让两张表天然同步,Go 必须在这里一起删。
func (h *Health) Forget(nodeKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.forgetLocked(nodeKey)
}

// forgetLocked 是按节点建键的表的统一回收点:判决行、TTFT 观测、冷却、
// 配额索引。quotaTags 漏删会让索引只增不减(静默期无 Pick 搭车时泄漏)。
func (h *Health) forgetLocked(nodeKey string) {
	delete(h.nodes, nodeKey)
	delete(h.ttft, nodeKey)
	delete(h.cool, nodeKey)
	delete(h.quotaTags, nodeKey)
}

// ClearQuotaMark 测试钩子:忘掉一个节点的配额记号,不动行的其余部分。
// (src/health.js:236-240)
func (h *Health) ClearQuotaMark(nodeKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.nodes[nodeKey]; ok {
		r.LastQuotaAt = 0
		h.nodes[nodeKey] = r
	}
	delete(h.quotaTags, nodeKey)
}

// ---- 粗探 -------------------------------------------------------------------

// MarkProbe 应用一轮粗探(零配额 HTTP 闸门)的判决,通关即 tier A。
// (src/health.js:244-307)
// MarkPassSuccess 记一轮 pass 通过(或一次数据面成功):状态归 alive、连败
// 清零、LastProbeAt 刷新。行不存在(冷区节点被数据面信号捞到?理论不可达;
// 健康表损坏?)时建一行 —— 保守地按 alive 处理,交给下一轮 pass 复核。
func (h *Health) MarkPassSuccess(nodeKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	r, ok := h.nodes[nodeKey]
	if !ok {
		r = row{State: StateAlive, LatencyMS: -1}
	}
	r.State = StateAlive
	r.Streak = 0
	r.LastProbeAt = now
	r.NeverAlive = false // 数据面成功同样是「活过」的证据
	h.nodes[nodeKey] = r
}

// MarkPassFail 记一轮 pass 失败(或一次数据面连通性失败),推进双档状态机:
//   - 热区(alive)连续 2 轮失败 → 降冷区(State=dead,streak 归零重数);
//   - 冷区(dead)连续 3 轮失败 → 返回 "delete",由调用方执行彻底删除
//     (注册表条目 + 健康行,零记录)。
//
// allowDelete=false 的两类调用者(数据面 transport/timeout 失败、冻结期的
// 冷区 pass)对**冷区行连计数都不推进**:删除判决必须数满 3 次「闸门开着、
// 亲眼看到」的冷区失败 —— 通道坏了期间照常 ++streak 的话,解锁的那一刻,
//
//	outage 期间积满的连败会立刻把一批节点删掉,冻结只剩延迟执行、没有挡住
//
// 任何判决(「通道抖三下 = 白删一池子」从后门回来)。数据面失败打在冷区行
// 上同样不该计数:它的降档责任在热区侧已经付过,或留给下一轮 sweep 重数。
// alive 行的计数与降档不受本位影响。行不存在返回 ""。
func (h *Health) MarkPassFail(nodeKey string, allowDelete bool) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	r, ok := h.nodes[nodeKey]
	if !ok {
		return ""
	}
	switch r.State {
	case StateDead:
		if !allowDelete {
			// 冻结期间/数据面信号:不计数、不删除,行原样(连 LastProbeAt
			// 也不刷 —— 这一轮对冷区行没有可记的结论)。
			return ""
		}
		r.Streak++
		// 从未活过的行早删(2 轮):烂水池里它们占 90%+,3 轮是给「曾经活过、
		// 可能只是暂时抖动」的宽限,从没证明过价值的不配拿满。
		threshold := coldDeleteAfter
		if r.NeverAlive {
			threshold = coldDeleteNeverAliveAfter
		}
		if r.Streak >= threshold {
			delete(h.nodes, nodeKey)
			return "delete"
		}
		r.LastProbeAt = now
		h.nodes[nodeKey] = r
		return ""
	case StateAlive: // 热区
		r.Streak++
		if r.Streak >= hotDemoteAfter {
			r.State = StateDead
			r.Streak = 0 // 降档重数:冷区的 3 次从进冷区起算
			r.LastProbeAt = now
			h.nodes[nodeKey] = r
			return "demote"
		}
		r.LastProbeAt = now
		h.nodes[nodeKey] = r
		return ""
	case StateUnknown:
		// unknown 行(NoteQuota 造的临时行、还没探完的行)从没拿到可达性结论,
		// 不能当成「热区连败器」计数:2 次 pass 失败就会把它按 hotDemoteAfter
		// 写成 dead,而它可能只是这一轮没轮到。与冷区同理 —— 不计数、不改状态,
		// 只记下「这一轮没给出判决」。
		r.LastProbeAt = now
		h.nodes[nodeKey] = r
		return ""
	}
	return "" // 未知 state 字符串(旧版本落盘的脏值):当没发生过
}

// hotDemoteAfter / coldDeleteAfter 是双档状态机的两个门槛(1.3.0 定稿:
// 热区连续 2 轮失败降冷;冷区连续 3 轮失败删除)。降档时 streak 归零,
// 两个门槛各数各的。
const (
	hotDemoteAfter  = 2
	coldDeleteAfter = 3
	// coldDeleteNeverAliveAfter 是「从未活过」行的冷区删除门槛:2 轮。
	coldDeleteNeverAliveAfter = 2
)

func (h *Health) MarkProbe(nodeKey string, res nodeprobe.ProbeResult) {
	// state == "unknown" 是本轮不完整(probeAll 的超时兜底),不是判决:整行
	// 原样保留,既不刷新 lastProbeAt 也不改写 state。把它当 dead 会凭一次调度
	// 侧的超时给一个可能健康的节点记连败,比漏探一轮严重得多(src/health.js:258-260)。
	if res.State != nodeprobe.StateAlive && res.State != nodeprobe.StateDead {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	alive := res.State == nodeprobe.StateAlive
	prev, hasPrev := h.nodes[nodeKey]

	// 出口 IP 的更新是三分支的,不是无条件覆盖(这是本文件曾经的真实缺陷,
	// src/health.js:247-253):
	//   1. 探到 IP → 覆盖 IP 与国家,并把 exitIpAt 推到现在。
	//   2. 存活但 echo 全败(没探到 IP)→ 保留上一次的 IP 与国家,exitIpAt 原样
	//      不动。echo 源是四个第三方站点,它们一起抖动一小时是常事;把「没量到」
	//      当成「换 IP 了」会让同 IP 归组、粘性锚点、配额扩散三件事同时在抖动
	//      窗口里失效。
	//   3. 变为 dead → 清掉 IP 与国家(连同 exitIpAt)。不存在的出口谈不上出口 IP。
	measuredIp := strings.TrimSpace(res.ExitIP)
	// 国家码按 rune 上截 2 位:字节截会把 CJK 等非 ASCII 撕成非法串。
	measuredCc := strings.ToUpper(res.ExitCountry)
	if rs := []rune(measuredCc); len(rs) > 2 {
		measuredCc = string(rs[:2])
	}
	if measuredCc != "" && !isASCIILetters(measuredCc) {
		measuredCc = ""
	}
	var exitIp, exitCountry string
	if alive {
		if measuredIp != "" {
			exitIp = measuredIp
		} else if hasPrev {
			exitIp = prev.ExitIP
		}
		if measuredCc != "" {
			exitCountry = measuredCc
		} else if hasPrev {
			exitCountry = prev.ExitCountry
		}
	}
	var exitIpAt int64
	switch {
	case exitIp == "":
		// 分支 3:dead 清空,时间戳一并清零
	case measuredIp != "":
		exitIpAt = now // 只有真量到 IP 的那一轮才刷新它
	default:
		// 分支 2 的保留必须留着旧时间戳,否则「保留」会把一个陈旧 IP 永久续命,
		// 正好抵消掉信任窗口的意义(src/health.js:292-293)。
		exitIpAt = prev.ExitIPAt
	}

	// 订阅标签推断的国家 vs 实测出口国家。两边都非空且不一致才置位:单边缺失
	// 时无从判断,宁可不管。这条标记只用于降一级排序,从不排除节点 —— 整份
	// 订阅标签都错时所有节点一起降级 = 相对次序不变,所以没有集体误伤
	// (src/health.js:271-276)。
	taggedCc := ""
	if alive {
		taggedCc = parse.CountryOf(nodeKey)
	}
	geoMismatch := alive && exitCountry != "" && taggedCc != "" && taggedCc != exitCountry

	latMS, latMin := int64(-1), int64(-1)
	if alive {
		// Go 的零值没有「缺失」语义:nodeprobe 对 alive 恒填正数毫秒,0 只可能
		// 是「调用方没填」,按 JS 的 `?? -1`(src/health.js:284/:290)落到 -1;
		// latencyMin 缺失先退 latencyMs,再退 -1(网络毫秒不可能真为 0)。
		latMS = res.LatencyMS
		latMin = res.LatencyMin
		if latMin <= 0 {
			latMin = res.LatencyMS
		}
		if latMin <= 0 {
			latMin = -1
		}
		if latMS <= 0 {
			latMS = -1
		}
	}

	// The coarse gate re-ran, so it is the authority on A-vs-none. A B node
	// that still passes stays B (its region verdict is not invalidated by a
	// liveness re-check); everything else drops back to untiered.
	// (src/health.js:302-305)
	tier := TierNone
	if alive {
		if hasPrev && prev.Tier == TierB {
			tier = TierB
		} else {
			tier = TierA
		}
		// **出口 IP 变了就作废 B 档凭证**:B 的含义是「从这个出口出去,门控
		// 模型可用」,IP 一变这句话就不再成立。过去 B 被无条件保留、且已证 B
		// 永不重测(runTierPipeline 跳过),一个动态 IP 节点漂到墙外国家后会
		// 永远挂着 B,被 gated 流量**优选**(rankLocked 给 B 桶位 −1),每轮
		// 真实对话先付一次 REGION 失败再轮换——坏凭证永不自愈。判据:本轮
		// **量到**的 IP(measuredIp,不是沿用下来的 exitIp)与上一行不同 →
		// 降回 A,下一轮 runTierPipeline 就会重验它;本轮没量到则不动 ——
		// 没有 IP 变化的证据就不撤销凭证(与分支 2 的保守保留同一口径)。
		if tier == TierB && hasPrev && measuredIp != "" && prev.ExitIP != "" && prev.ExitIP != measuredIp {
			tier = TierA
		}
	}

	// 配额记号跟着节点走,探测轮不改写它(src/health.js:299-300):粗探打的是
	// 零配额的 /zen/v1/models 闸门,它过了也说明不了免费通道的额度恢复了
	// (那是上游的计费决定)。
	var lastQuotaAt int64
	if hasPrev {
		lastQuotaAt = prev.LastQuotaAt
	}

	// Streak 同样跟着节点走:整行重写若把它清零,MarkPassFail 攒下的
	// 「热区连败 N 轮」会被下一轮全量粗探无声归零,双档状态机的降冷/删除
	// 判决(以及冻结期解除后的补数)永远数不满。实测:粗探与 pass 闸门并行
	// 跑,每轮粗探都插在两次 pass 之间,不保留 = hotDemoteAfter/coldDeleteAfter
	// 两个门槛形同虚设。
	streak := 0
	if hasPrev {
		streak = prev.Streak
	}

	// NeverAlive:无行首探判死 → true(从没证明过价值);alive → 清掉;
	// 其余(已有行的 dead 重判)保持原值 —— 曾经活过的不应被打回。
	// unknown 行的判死同样置 true:unknown 只能来自 NoteQuota 临时行或
	// 半程行(state==alive/dead 的行不会变回 unknown),它从未拿到过可达性
	// 结论,"prev.NeverAlive=false" 只是没被标过、不是"曾活过"的证据;
	// 漏标会让这类首探判死的节点白拿 3 轮宽限(承诺是 2 轮)。
	neverAlive := false
	if hasPrev {
		neverAlive = prev.NeverAlive
	}
	if alive {
		neverAlive = false
	} else if !hasPrev || prev.State == StateUnknown {
		neverAlive = true
	}

	h.nodes[nodeKey] = row{
		State:       NodeState(res.State),
		LatencyMS:   latMS,
		LatencyMin:  latMin,
		ExitIP:      exitIp,
		ExitIPAt:    exitIpAt,
		ExitCountry: exitCountry,
		GeoMismatch: geoMismatch,
		LastProbeAt: now,
		Tier:        tier,
		LastQuotaAt: lastQuotaAt,
		Streak:      streak,
		NeverAlive:  neverAlive,
	}
}

// isASCIILetters 报告 s 是否全由 ASCII 字母组成:国家码只可能是两位字母,
// 非 ASCII 串(截断撕裂的 CJK、emoji)一律按缺失处理,不进分桶与 geo 判定。
func isASCIILetters(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z') {
			return false
		}
	}
	return true
}

// ExitIPOf 这个节点当前可信的出口 IP,空串表示「没有可用量测」。
// (src/health.js:309-324)
// 唯一的出口 IP 读取口径:任何直接读 nodes.get(k).exitIp 的地方都绕过了信任
// 窗口,会拿一个两周前的 IP 去做同 IP 归组/粘性决策。窗口内的旧 IP 照常返回
// (markProbe 分支 2 的保留正是为了这个),过期则视为没有。exitIpAt 缺失的行
// 来自旧版本落盘的 data/node-health.json:退回 lastProbeAt 判定,老数据因此
// 自然到期而不是被当成「永久新鲜」。
func (h *Health) ExitIPOf(nodeKey string, now int64) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.exitIpOfLocked(nodeKey, now)
}

func (h *Health) exitIpOfLocked(nodeKey string, now int64) string {
	r, ok := h.nodes[nodeKey]
	if !ok || r.ExitIP == "" {
		return ""
	}
	at := r.ExitIPAt
	if at == 0 {
		at = r.LastProbeAt
	}
	if now-at > exitIPTTL {
		return ""
	}
	return r.ExitIP
}

// ProbedWithin 这个节点是不是刚刚才拿到判决(结果缓存 TTL)。(src/health.js:326-341)
// 只有 alive/dead 两种判决算数:unknown(本轮不完整)与 unknown 状态的配额行
// 都没给出可达性结论,跳过它们等于把一个没测过的节点当成「刚测过」,会让它
// 永远得不到探测。lastProbeAt 缺失或为 0 的行不算新鲜。
// now 由调用方给，与 ExitIPOf 同口径：包内所有「新鲜度」判定都读**同一个**
// 时刻，测试才能把边界钉在确定的时间点上（过去这里自己调 time.Now()，窗口
// 边界只能靠 sleep 逼近，既不可测也让同一轮探测里各行的「现在」不是同一个）。
func (h *Health) ProbedWithin(nodeKey string, windowMS int64, now int64) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.nodes[nodeKey]
	if !ok {
		return false
	}
	if r.State != StateAlive && r.State != StateDead {
		return false
	}
	at := r.LastProbeAt
	return at > 0 && now-at <= windowMS
}

// MarkTierProbe 用 region-gated 探针模型的判决精化一个粗探通关的节点。
// region-blocked 降到 A(可达,但进不了 gated 模型);available 升到 B;其它
// 判决(throttled/transport/unknown)一律不动 —— 不确定的探针不得降级一个
// 很可能没问题的节点。(src/health.js:343-355)
func (h *Health) MarkTierProbe(nodeKey, verdict string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.nodes[nodeKey]
	if !ok {
		return
	}
	switch verdict {
	case "available":
		r.Tier = TierB
	case "region-blocked":
		r.Tier = TierA
	}
	h.nodes[nodeKey] = r
}

// HealthOf 缺行 = unknown。(src/health.js:357-359)
func (h *Health) HealthOf(nodeKey string) NodeState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.healthOfLocked(nodeKey)
}

func (h *Health) healthOfLocked(nodeKey string) NodeState {
	if r, ok := h.nodes[nodeKey]; ok {
		return r.State
	}
	return StateUnknown
}

// ---- 配额记号 ----------------------------------------------------------------

// NoteQuota 记一次配额墙(429)。(src/health.js:375-400)
// 不冷却、不淘汰:429 说的是上游的计费决定,不是这个出口坏了 —— 它下一轮粗探
// 照样能过,判死它等于因为上游的账单去砍自己的池子。所以这里只留一个时间戳,
// 由 Pick 把它降一级,排在「最近没撞墙」的出口后面,永远不排除候选。
func (h *Health) NoteQuota(nodeKey string) {
	if nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.nodes[nodeKey]
	if !ok {
		// 还没有健康行的节点(首次探测轮之前就被选中了):建一行形状与面板兜底
		// {state:'unknown', latencyMs:-1} 一致的条目,否则配额记号会被静默丢掉
		// —— 而「记了等于没记」正是这里要修的病(src/health.js:390-397)。
		// state 仍是 unknown:NodeUsable 的「未知即可用」语义不变。
		r = row{State: StateUnknown, LatencyMS: -1}
	}
	r.LastQuotaAt = time.Now().UnixMilli()
	h.nodes[nodeKey] = r
	if h.quotaTags == nil {
		h.quotaTags = map[string]struct{}{}
	}
	h.quotaTags[nodeKey] = struct{}{}
}

// quotaMarkedLocked 这个节点自己的配额记号还新鲜吗?(src/health.js:402-406)
func (h *Health) quotaMarkedLocked(nodeKey string, now int64) bool {
	r, ok := h.nodes[nodeKey]
	if !ok {
		return false
	}
	return r.LastQuotaAt > 0 && now-r.LastQuotaAt < quotaMark
}

// quotaMarkedExitIpsLocked 还在配额记号有效期内的出口 IP 集合。
// (src/health.js:408-426)
// 为什么按 IP 扩散而不是只标那一个节点:配额的归属至今没有定论(按会话计还是
// 按出口 IP 计,两处旧注释互斥)。两种读法下这个写法都不会错:
//   - 若按出口 IP 计:同一 IP 的兄弟节点额度一起耗光,只标中招的那个等于没修。
//   - 若按会话计:多降级几个同 IP 节点是轻微过度降级,但只降级不排除,90s 后自愈。
//
// 反过来(只标节点)在第一种读法下是静默失效,所以不对称的代价偏向这边。
func (h *Health) quotaMarkedExitIpsLocked(now int64) map[string]bool {
	// P3:过去每次 Pick 扫全池(h.nodes,上限 8000)只为找 LastQuotaAt!=0 的
	// 少数中招行 —— 现在只走索引;行已删/记号已过期的残留在这里懒清理
	// (调用方持写锁,删除安全)。
	out := map[string]bool{}
	for key := range h.quotaTags {
		if !h.quotaMarkedLocked(key, now) {
			delete(h.quotaTags, key)
			continue
		}
		if ip := h.exitIpOfLocked(key, now); ip != "" {
			out[ip] = true
		}
	}
	return out
}

// ---- 可用性与观测 -------------------------------------------------------------

// NodeUsable:unknown 可用;只有实测 dead 排除。(src/health.js:428-436)
func (h *Health) NodeUsable(nodeKey string) bool {
	// 写锁:过期的冷却条目在这里顺手删除。
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodeUsableLocked(nodeKey, time.Now().UnixMilli())
}

func (h *Health) nodeUsableLocked(nodeKey string, now int64) bool {
	if h.healthOfLocked(nodeKey) == StateDead {
		return false
	}
	c, ok := h.cool[nodeKey]
	if !ok {
		return true
	}
	// 过期顺手删除,避免冷却表随节点标签流转无限增长(src/health.js:433-434)。
	// now 由调用方传入:rank 热路径上每节点自取一次 time.Now() 在 Windows 上
	// 不是免费的(2100 节点 = 2100 次系统调用级取时),调用方手里就有同一时刻。
	if c.Until <= now {
		delete(h.cool, nodeKey)
		return true
	}
	return false
}

// TierOf:B(gated-capable) | A(粗探可达) | ""(无判决)。(src/health.js:438-441)
func (h *Health) TierOf(nodeKey string) Tier {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if r, ok := h.nodes[nodeKey]; ok {
		return r.Tier
	}
	return TierNone
}

// NodeGatedCapable:这个出口能服务 region-gated 模型吗?B 是唯一证明。
// (src/health.js:443-446)
func (h *Health) NodeGatedCapable(nodeKey string) bool {
	return h.TierOf(nodeKey) == TierB
}

// NodeSnapshot 是面板 JSON 编码器的输入:行拷贝 + cooling 叠加层。
// (src/health.js:448-454) 返回新 map,调用方改不动内存表(深拷贝纪律)。
func (h *Health) NodeSnapshot() map[string]NodeView {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now().UnixMilli()
	out := make(map[string]NodeView, len(h.nodes))
	for key, r := range h.nodes {
		v := NodeView{row: r}
		if c, ok := h.cool[key]; ok && c.Until > now {
			v.CoolingUntil = c.Until
			v.CoolingFailures = c.Failures
		}
		out[key] = v
	}
	return out
}

// TierCount 是 tierCounts 的返回形状 {alive, a, b}。(src/health.js:456-465)
type TierCount struct {
	Alive int `json:"alive"`
	A     int `json:"a"`
	B     int `json:"b"`
}

// EvictRank 给注册表淘汰用:分越小越先被挤掉。0=池外残留(既无行也不在
// 池里的历史判决),1=从未活过的死节点,2=活过但现在死的,3=unknown/冷却中,
// 4=alive,5=B 档。**调用方负责给「本轮新入池、还没有行」的 tag 显式中
// 间档**（rebuild/boot 传 3）：map 零值恰是 0=最先挤掉,新节点若按零值
// 参与排序,池满时每轮新 tag 恒被首驱、永远得不到首探——「健康感知淘汰」
// 退化成「新人永不进」。调用方(registry.EnforceCapRanked)在锁外按快照算好传入。
func (h *Health) EvictRank() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make(map[string]int, len(h.nodes))
	now := time.Now().UnixMilli()
	for tag, r := range h.nodes {
		switch {
		case r.State == StateAlive && r.Tier == TierB:
			out[tag] = 5
		case r.State == StateAlive:
			out[tag] = 4
		case r.State == StateDead && r.NeverAlive:
			out[tag] = 1
		case r.State == StateDead:
			out[tag] = 2
		default:
			out[tag] = 3
		}
		if c, ok := h.cool[tag]; ok && c.Until > now {
			// 冷却中降一档:别把正在退避的活节点当健康挤掉别人。
			if out[tag] > 0 {
				out[tag]--
			}
		}
	}
	return out
}

// TierCounts 只数 alive 的行。
func (h *Health) TierCounts() TierCount {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var c TierCount
	for _, r := range h.nodes {
		if r.State != StateAlive {
			continue
		}
		c.Alive++
		switch r.Tier {
		case TierB:
			c.B++
		case TierA:
			c.A++
		}
	}
	return c
}

// PruneStale 删掉离开池子的节点的行。(src/health.js:485-497)
// 池子每轮 rebuild churn 1700-1900 个 tag —— 早期版本这里还会顺带清空 region
// 矩阵,判决在复用之前就被删光,region-gated 路由静默退化成「任意节点」,每一
// 轮都撞 403 RegionError。tier 现在长在节点行上,活着的判决不受影响
// (回归:TestForgetAndPruneStaleKeepLiveVerdicts)。
//
// 三张按节点建键的表要一起删(R17):JS 单线程里 pruneStale 只删判决行也没事,
// 因为它的 ttft/cooling 同样只在 pruneStale 里被跳过 —— Go 这边每轮 churn 都是
// 真实并发,只删一张表等于让另外两张按历史 tag 单调增长。EnforceCap 挤出去的
// tag 不走 Forget,所以这里的「不在 active 就删」也正是那条路径的回收点。
func (h *Health) PruneStale(activeKeys []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	on := make(map[string]bool, len(activeKeys))
	for _, key := range activeKeys {
		on[key] = true
	}
	for key := range h.nodes {
		if !on[key] {
			delete(h.nodes, key)
		}
	}
	for key := range h.ttft {
		if !on[key] {
			delete(h.ttft, key)
		}
	}
	for key := range h.cool {
		if !on[key] {
			delete(h.cool, key)
		}
	}
	for key := range h.quotaTags {
		if !on[key] {
			delete(h.quotaTags, key)
		}
	}
}

// GatedUsable:存在任意一个 alive 的 B 吗?没有的话 gated 模型没有已验证出口。
// (src/health.js:499-503)
func (h *Health) GatedUsable() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, r := range h.nodes {
		if r.State == StateAlive && r.Tier == TierB {
			return true
		}
	}
	return false
}

// NoteRegionError 真实流量的观测:gated 模型的一轮真实对话被拒。(src/health.js:507-512)
func (h *Health) NoteRegionError(model, nodeKey string) {
	if model == "" || nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.nodes[nodeKey]; ok {
		r.Tier = TierA
		h.nodes[nodeKey] = r
	}
}

// NoteRegionOK 真实流量的观测:gated 模型的一轮真实对话成功。(src/health.js:514-518)
func (h *Health) NoteRegionOK(model, nodeKey string) {
	if model == "" || nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if r, ok := h.nodes[nodeKey]; ok {
		r.Tier = TierB
		h.nodes[nodeKey] = r
	}
}

// RegionSnapshot:/api/status 的「哪些模型是 region-gated」名单。
// (src/health.js:520-522)
//
// JS 的形状里还有 `tiered: {tag: tier}`,Go 曾一并构造它(审计 O6):唯一读者
// status.go 只取 `.Models`,面板的每节点档位走 NodeSnapshot 的 row.Tier —— 一个
// 构造 + 序列化后没人读的观测面,每 5 秒随节点数扫一遍全表。删掉;真要它的时候
// 按 row 加回来,而不是留一份没人看的副本。
type RegionSnapshot struct {
	Models []string `json:"models"`
}

func (h *Health) RegionSnapshot() RegionSnapshot {
	return RegionSnapshot{Models: []string{RegionProbeModel}}
}

// SeedRestrictedModels 不移植:JS 里它只是为 index.js 的调用保形的空壳
// (src/health.js:524-529),gated 集合已是编译期常量 RegionProbeModel,Go 侧
// 留一个空函数只会让人以为还有动态逻辑。
//
// IsRestrictedModel:这是 region-gated 模型吗?探针模型集合,按目录行。
// (src/health.js:531-534)
func IsRestrictedModel(model string) bool {
	return strings.HasPrefix(model, "muse-spark")
}

// RegionProbeCandidates 不移植(审计 O6):JS 的 regionProbeCandidates
// (src/health.js:536-546)在 Go 里零调用 —— 第二段 B 探针的名单由 app 的
// tier 流水线从探测结果自己组,不需要这张表。留一个没人调的名单,只会让人以为
// B 档证明还有第二条入口。
//
// UnavailableEverywhere:这个模型是不是因为没有任何 alive 的 B 出口而不可服务?
// (src/health.js:548-558) 只有 gated 模型可能是:非受限模型(big-pickle)每个
// A 出口都能服务,忘了传 model 就会把整个目录藏起来 —— 非受限模型必须返回
// false。
//
// 状态要说清楚(O13 之后):/v1/models 过去逐行调它,而它对受限模型的每一次调用
// 都要扫一遍 h.nodes ⇒ 目录行数 × 池子大小。那个循环现在自己把 GatedUsable()
// 提到循环外,本函数因此**只剩单模型问答的语义**(pick_test 钉着这三档行为)。
// 它不是面板在读的那格(那走 NodeSnapshot 的 tier),留着是因为「这个模型现在
// 可服务吗」是调用方最可能问 health 的问题 —— 如果 T16 评审认为它就是该删,
// 删法是把 pick_test 的三条断言改成直接问 GatedUsable。
func (h *Health) UnavailableEverywhere(model string) bool {
	if !IsRestrictedModel(model) {
		return false
	}
	return !h.GatedUsable()
}

// ---- 粘性会话 ----------------------------------------------------------------

// ttlOf 行内生效 TTL:JS `hit.ttlMs ?? STICKY_TTL_MS` 的等价(TTLMS 恒由
// NoteSticky/NoteCacheRead 写入,<=0 只可能来自手工构造,按基线处理)。
func ttlOf(s *sticky) int64 {
	if s.TTLMS <= 0 {
		return stickyTTLBase
	}
	return s.TTLMS
}

// finiteNonNegative 对应 JS 的 Number.isFinite(x) ? Math.max(0, x) : 0:缺失/
// NaN/Inf 的含义是「厂商没报,等于没命中」,负数同理截到 0。
func finiteNonNegative(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	return v
}

// NoteCacheRead 上一次请求从厂商缓存读到了多少 token。(src/health.js:568-590)
// 这是「这个出口到底值不值得粘」的唯一直接证据。只喂读到量,不喂 prompt 总长:
// 命中量本身就是「有多少是省下来的」,而 prompt 规模是会话属性不是出口属性,
// 混进选路信号里会让长 prompt 会话无条件粘住。没有 sticky 行就不凭空造一行
// —— 没有粘性就没有收益可言。返回落定后的 TTL 与是否生效(JS 的 hit.ttlMs)。
func (h *Health) NoteCacheRead(session string, cacheReadTokens float64) (int64, bool) {
	if session == "" {
		return 0, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	hit, ok := h.sticky[session]
	if !ok {
		return 0, false
	}
	hit.CacheRead = finiteNonNegative(cacheReadTokens)
	hit.CacheAt = time.Now().UnixMilli()
	hit.TTLMS = h.stickyTTLOfLocked(hit, hit.CacheAt)
	return hit.TTLMS, true
}

// stickyTTLOfLocked 由上一次真实收益算出的 TTL(ms)。(src/health.js:592-614)
// 三档:
//   - cacheRead ≥ cacheWorth(1024):厂商确实在这个出口上命中了缓存,继续粘着
//     是在省钱 → 放大到 2h。
//   - cacheRead ≈ 0 且 prompt 大到「本该命中」(≥4096):缓存没命中,说明换出口
//     的代价可能是零 → 缩到 5min,把出口让给别的会话。
//   - 其余(第一次请求还没有 usage、prompt 很小):没有任何信息,必须保守 ——
//     保持 30min 基线。拿「第一次请求必然 0 命中」当「粘性没用」会把新会话的
//     第一轮就踢掉出口,而那正是建缓存的那一轮。
//
// cacheRead 观测超过 10min(cacheFresh)已不能代表「现在值不值得粘」→ 回基线。
func (h *Health) stickyTTLOfLocked(s *sticky, now int64) int64 {
	if now-s.CacheAt > cacheFresh {
		return stickyTTLBase
	}
	if s.CacheRead >= cacheWorth {
		return cacheStickKeep
	}
	if s.PromptTokens >= cacheWorth*4 {
		return cacheCold
	}
	return stickyTTLBase
}

// StickyUsage 是一次成功请求交回粘性表的 token 记账(src/health.js:626 的
// usage 对象形状)。零值 = 上游没报,按「没命中/没信息」处理。
//
// JS 的形状还有 promptTokens(OpenAI 拼写,含缓存),src/health.js:634-636 为此
// 有一条「inputTokens 缺席就用 promptTokens 减缓存」的兜底;Go 的 usage 只有一个
// 来源 —— stream.Usage 的 In 在 ScanUsage 里**已经**按 disjoint-count 减过缓存了
// (B4/O15),所以那条兜底在这里是死支,字段一并删掉:留着等于宣称还有第二条
// 入口,而它永远不会被填。
type StickyUsage struct {
	CacheReadTokens float64
	InputTokens     float64
}

// NoteStickyUsage 把一次请求的缓存收益交回粘性表(engine 成功路径调用)。
// (src/health.js:616-639) 收 usage 对象而不是裸数字,是因为定档需要两个量:
// 命中量(cacheReadTokens)和「本次本该命中多少」(inputTokens,已是不含缓存的
// 净输入)。拿不到任何 usage 就什么都不做:那次请求对「值不值得粘」没有发言权,
// 不能让它覆盖上一次已经量到的收益。
func (h *Health) NoteStickyUsage(session string, usage StickyUsage) {
	if session == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	hit, ok := h.sticky[session]
	if !ok {
		return
	}
	cacheRead := finiteNonNegative(usage.CacheReadTokens)
	input := finiteNonNegative(usage.InputTokens)
	hit.PromptTokens = input
	hit.CacheRead = cacheRead
	hit.CacheAt = time.Now().UnixMilli()
	hit.TTLMS = h.stickyTTLOfLocked(hit, hit.CacheAt)
}

// stickyCap 是粘性表的上限(R20)。键来自客户端可控的 user/conversation,而行只在
// 同会话再次被读到时才作废 —— 没有上限,一个每请求换会话名的客户端就能让这张表
// 以及每次 Pick 在独占锁下对它的全表扫描无界增长。1024 远大于真实并发会话数
// (这是个跑在个人机器上的网关),同时把扫描成本钉在一个常数上。
const stickyCap = 1024

// evictStickyLocked 按最久未用(at 最旧)挤掉 n 行(R20 的上限)。挤掉一个会话
// 的代价只是它下一次请求重新选路 —— 丢一次缓存亲和,换表长与扫描成本的常数上界。
// 挤完顺手把该会话的连败计数一起删掉:那是同一把键的另一张表。
//
// 只在**越过上限**时才付这次 O(n log n),所以平时零成本。
func (h *Health) evictStickyLocked(n int, keep string) {
	if n <= 0 {
		return
	}
	type row struct {
		session string
		at      int64
	}
	all := make([]row, 0, len(h.sticky))
	for session, hit := range h.sticky {
		if session == keep {
			continue // 本轮刚钉下的这一行不参与自己引发的淘汰
		}
		all = append(all, row{session, hit.At})
	}
	slices.SortFunc(all, func(a, b row) int {
		if a.at != b.at {
			if a.at < b.at {
				return -1
			}
			return 1
		}
		return strings.Compare(a.session, b.session) // 同毫秒:按会话字典序,可复现
	})
	for _, x := range all {
		if n <= 0 {
			break
		}
		delete(h.sticky, x.session)
		delete(h.stickyFail, x.session)
		n--
	}
}

// NoteSticky 把会话钉到一个出口。(src/health.js:641-685)
// 锚点是出口 IP,不是节点:提示词缓存按出口 IP 计账(随机轮换实测 0% 命中,
// 固定出口 99.8%),而池子里 96 个 IP 后面挂着 191 个节点 —— 「同一个 IP 换一个
// 节点」对缓存是无损的,「换一个 IP」才是丢缓存。这里同时记下当时那个 IP,
// 节点只回答「当时是谁在服务这个 IP」。
// TTL 不在钉定时重算:一次请求的 cacheRead 只说明那次请求的收益,而 at 是
// sticky 落定的时刻。重算会让「上一轮命中很多」把本轮粘性无限延长,给一个可能
// 早就失效的出口续命。
func (h *Health) NoteSticky(session, nodeKey string, withinTurn bool) {
	if session == "" || nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	if prev, ok := h.sticky[session]; ok && prev.NodeKey == nodeKey {
		// 同一出口上继续服务 → 只刷 at,保留缓存收益的记账(src/health.js:664-675)。
		prev.At = now
		// 轮内不许被缩到基线以下。这条从本轮的第二次请求起生效(调用方先
		// ExitForSession 再 NoteSticky,读写顺序决定),而那正是工具结果开始
		// 堆积、缓存最值钱的地方;本轮第一次请求的 prompt 最小,判据也最弱。
		if withinTurn && ttlOf(prev) < stickyTTLBase {
			prev.TTLMS = stickyTTLBase
		}
		return
	}
	// 换 IP(或首次)→ 全新行:旧出口的 cacheRead 不继承过来,那会变成给新
	// 出口的无依据续命(src/health.js:664-665)。轮内落定的出口一律从基线 TTL
	// 起算 —— 这一段前缀最长、缓存最值钱,noteCacheRead 之后会再调档。
	h.sticky[session] = &sticky{
		NodeKey: nodeKey,
		ExitIP:  h.exitIpOfLocked(nodeKey, now),
		At:      now,
		TTLMS:   stickyTTLBase,
	}
	// R20 的硬上界:过期行的就地回收只在「扫到」时发生,而新增行的速率由客户端控
	// (会话名是它给的)。超上限就按最久未用挤掉,把表长钉成常数。
	if over := len(h.sticky) - stickyCap; over > 0 {
		h.evictStickyLocked(over, session)
	}
}

// StickyTTL 这个会话当前判定的粘性 TTL(ms)。(src/health.js:687-697,JS 名
// noteStickyTtl) 只给观测与测试用 —— 读路径一律走 ExitForSession/busyExitIps,
// 它们自己按行里的 ttlMs 计时。第二个返回值 false = 没有 sticky 行(JS 的
// null),区别于「有行且等于基线」。
//
// 审计 O6 把它列为「仅测试使用」的死符号,这里**保留**:粘性 TTL 的三档定档是
// L1 的内部状态,而钉住它的用例在 L4 的 engine 包(轮内不许改档那条)—— 跨包没有
// 别的观察窗口,删掉它等于删掉那条行为的测试。同一条理由保住了 gate.NextAt。
func (h *Health) StickyTTL(session string) (int64, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	hit, ok := h.sticky[session]
	if !ok {
		return 0, false
	}
	return ttlOf(hit), true
}

// NoteStickyFailure 记一次从粘性出口出发、未出内容就失败的尝试。
// (src/health.js:699-703)
//
// 无 sticky 行的会话不建键:session 客户端可控,无行也建等于任由调用方
// 把表撑大;且没有粘性可熔断时记数毫无意义。计数值封顶(熔断阈值 2,
// 封顶 1<<20 纯防溢出,正常语义 2 即换出口、计数不再增长)。
func (h *Health) NoteStickyFailure(session string) {
	if session == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.sticky[session]; !ok {
		return
	}
	if h.stickyFail[session] < stickyFailCap {
		h.stickyFail[session]++
	}
}

// ClearStickyFailures 一轮出了内容即清零连败。(src/health.js:705-709)
func (h *Health) ClearStickyFailures(session string) {
	if session == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.stickyFail, session)
}

// StickyBurned 连跪两次的会话必须轮换出口。(src/health.js:711-714)
// 没有这根保险丝,探针活着的坏出口会让同一个会话在整整 30min TTL 里每条请求
// 都白付一次失败+重试。
func (h *Health) StickyBurned(session string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.stickyFail[session] >= 2
}

// sameExitNodeLocked 在同一个出口 IP 内挑一个还能用的节点。(src/health.js:716-741)
// 池子重度聚簇(96 个 IP / 191 个节点,19 个 IP 被 2-7 个节点共用),原节点坏掉
// 时通常有替代品,而换节点不换 IP 对提示词缓存无损。排序与 pick 一致:先存活、
// 再按 latencyMin(-1/0 视为无数据、排最后),并列按 tag 字典序(JS 靠 Map 插入
// 序,Go map 无序,确定性优先)。
//
// 实现是一次遍历选最优,不是「排序全表再取第一」:调用点在锁内,而全表排序
// (O(n log n) 加一次 n 大小的分配)在 2000 节点池上是每次同 IP 轮换都要付的
// 税;逐点比较用同样的判据(状态档 → 延迟 → tag 字典序)取 min,结果与排序后
// 取第一**逐位相同**,与 map 迭代顺序无关。
func (h *Health) sameExitNodeLocked(exitIP, excludeKey string) string {
	if exitIP == "" {
		return ""
	}
	now := time.Now().UnixMilli()
	bestState := 2 // 没有候选时的终值;任何能进循环体的候选都 < 2
	bestLat := int64(math.MaxInt64)
	bestKey := ""
	for key := range h.nodes {
		if key == excludeKey {
			continue
		}
		if h.exitIpOfLocked(key, now) != exitIP {
			continue
		}
		if !h.nodeUsableLocked(key, now) {
			continue
		}
		state := 1
		if h.nodes[key].State == StateAlive {
			state = 0
		}
		lat := h.nodes[key].LatencyMin
		if lat <= 0 {
			lat = math.MaxInt64
		}
		if state < bestState ||
			(state == bestState && lat < bestLat) ||
			(state == bestState && lat == bestLat && key < bestKey) {
			bestState, bestLat, bestKey = state, lat, key
		}
	}
	return bestKey
}

// ExitForSession 会话当前应该走哪个出口;"" = 没有可用粘性(JS 的 null)。
// (src/health.js:743-774) 原节点还可用 → 原样返回;不可用 → 在同一出口 IP 内
// 轮换到另一个节点;连替代品都没有(或本来就没量到出口 IP)→ 作废并返回 "",
// 走正常选路。
// at 在同出口轮换时不刷新:否则一个坏 IP 被反复轮换就能把会话无限续命。TTL 由
// NoteCacheRead 在收益落定那一刻定档,这里只按它计时 —— 不在每次读时重算。
func (h *Health) ExitForSession(session string) string {
	if session == "" {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	hit, ok := h.sticky[session]
	if !ok {
		return ""
	}
	now := time.Now().UnixMilli()
	if now-hit.At > ttlOf(hit) || h.stickyFail[session] >= 2 {
		// 过期或烧掉 → 作废
		delete(h.sticky, session)
		delete(h.stickyFail, session)
		return ""
	}
	if h.nodeUsableLocked(hit.NodeKey, now) {
		return hit.NodeKey
	}
	ip := hit.ExitIP
	if ip == "" {
		ip = h.exitIpOfLocked(hit.NodeKey, now)
	}
	alt := h.sameExitNodeLocked(ip, hit.NodeKey)
	if alt != "" {
		// 同出口轮换:连同 at 与 TTL 一起搬过去(同一个锚点,同一份收益证据),
		// 不刷新 at(src/health.js:751-753/:766-768)。
		rotated := *hit
		rotated.NodeKey = alt
		h.sticky[session] = &rotated
		return alt
	}
	delete(h.sticky, session)
	delete(h.stickyFail, session)
	return ""
}

// ---- 节点冷却 ----------------------------------------------------------------

// NoteCooldown 记一次节点失败并进入指数冷却。(src/health.js:796-808)
// 与 stickyFail 的分工:stickyFail 按会话记分(这个会话在这个出口上连跪两次就
// 换),这里按节点记分(这个出口本身坏了,对所有会话都先别用)。
// 冷却只应由「节点自身可归因」的失败触发(engine 的 COOLDOWN_ON 只有
// transport/timeout,src/engine.js:47):region/quota/empty 是全局性问题,给
// 它们冷却会把整个池子冻住,网关对所有请求不可用 —— 那比偶尔重试一个坏节点
// 严重得多。
// 不持久化:进程重启后重新探测即可,冷却状态没有跨重启的价值。
func (h *Health) NoteCooldown(nodeKey string, retryAfterMS int64) {
	if nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	failures := 1
	if c, ok := h.cool[nodeKey]; ok {
		failures = c.Failures + 1
	}
	// 2^(failures-1) 的指数上限:最多 8 倍基础时长(src/health.js:794)。
	shift := uint(failures - 1)
	if shift > cooldownMaxShift {
		shift = cooldownMaxShift
	}
	backoff := int64(cooldownBase) * (int64(1) << shift)
	// 上游给的 Retry-After 比退避更长时以它为准(那是权威的恢复时间);由调用
	// 方决定是否传 —— 只有节点可归因的失败才该传(src/health.js:798-800)。
	var hinted int64
	if retryAfterMS > 0 {
		hinted = retryAfterMS
	}
	h.cool[nodeKey] = &cooling{Failures: failures, Until: time.Now().UnixMilli() + max(hinted, backoff)}
}

// ClearCooldown 节点恢复正常(一次成功即清零连败计数)。(src/health.js:810-813)
func (h *Health) ClearCooldown(nodeKey string) {
	if nodeKey == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.cool, nodeKey)
}

// CooldownState 是冷却中节点的面板/诊断行。(src/health.js:815-821)
type CooldownState struct {
	Failures    int   `json:"failures"`
	Until       int64 `json:"until"`
	RemainingMS int64 `json:"remainingMs"`
}

// CooldownSnapshot 当前处于冷却中的节点快照(面板与诊断用)。
func (h *Health) CooldownSnapshot() map[string]CooldownState {
	h.mu.RLock()
	defer h.mu.RUnlock()
	now := time.Now().UnixMilli()
	out := map[string]CooldownState{}
	for key, c := range h.cool {
		if c.Until <= now {
			continue
		}
		out[key] = CooldownState{Failures: c.Failures, Until: c.Until, RemainingMS: c.Until - now}
	}
	return out
}

// ---- 出口在途计数 -------------------------------------------------------------

// NoteExitBusy 记一条「这个出口 IP 上开始了在途请求」,返回归账用的 IP
// (空串 = 不用还)。(src/health.js:825-846)
// 为什么要有归还:只看粘性只数得出「有几个会话钉在这个 IP 上」,数不出「这个
// IP 现在正压着几条请求」。一个会话连发 5 个并发请求在粘性表里仍是 1 条,而
// 配额和上游并发上限按 IP 计 —— 那 5 条会一起撞墙。
// 陈旧回收:上游单次超时是 300s,窗口取 10 分钟远大于它。超过窗口的旧计数只
// 可能来自漏调的 ReleaseExitBusy(进程异常路径),直接归零再计,否则一个漏掉
// 的计数会把那个 IP 永久标成繁忙。
func (h *Health) NoteExitBusy(exitIP string) string {
	if exitIP == "" {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now().UnixMilli()
	count := 0
	if r, ok := h.busy[exitIP]; ok && now-r.At <= exitBusyStale {
		count = r.Count
	}
	h.busy[exitIP] = &busy{Count: count + 1, At: now}
	return exitIP // 原样返回,方便调用方写成 acct := NoteExitBusy(ip)
}

// ExitBusyCount 这个出口 IP 当前记账的在途请求数(只读观测)。NoteExitBusy /
// ReleaseExitBusy 是一对私有账本,轮换引擎的任务 18 测试表要求断言「每条路径
// 都把令牌还了回来」,没有只读窗口就只能在同包测试里摸 h.busy;面板的饱和视图
// 将来读的也是同一个数。过窗的陈旧条目按 0 读出(与 exitBusyCountLocked 同一
// 口径),不在这里做删除回收。
func (h *Health) ExitBusyCount(exitIP string) int {
	if exitIP == "" {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.busy[exitIP]
	if !ok {
		return 0
	}
	if time.Now().UnixMilli()-r.At > exitBusyStale {
		return 0
	}
	return r.Count
}

// ReleaseExitBusy 归还一条在途计数。(src/health.js:848-858)
// 零值条目保留(Resin Dec 的注释:删掉再建会把并发 Inc 的那一次丢掉,那个 IP
// 从此永远少算一条在途请求),改由 exitBusyCountLocked 按窗口回收。
func (h *Health) ReleaseExitBusy(exitIP string) {
	if exitIP == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r, ok := h.busy[exitIP]
	if !ok {
		return
	}
	if r.Count > 0 {
		r.Count--
	}
	// At 只在 NoteExitBusy 递增时刷新:每次归还都刷新会让「陈旧计数 10 分钟清零」
	// 的窗口被任何一次 Release 反复续命 —— 一个漏 Release 的计数只要与同 IP 的
	// 正常 release 交错就长期存活,exitBusyStale 形同虚设。这里是递减,不是
	// 新一轮在途的起点。
}

// exitBusyCountLocked 这个出口 IP 当前的在途请求数(已过窗的陈旧计数按 0 处理,
// 顺手回收)。(src/health.js:860-871)
func (h *Health) exitBusyCountLocked(exitIP string, now int64) int {
	if exitIP == "" {
		return 0
	}
	r, ok := h.busy[exitIP]
	if !ok {
		return 0
	}
	if now-r.At > exitBusyStale {
		// R9:过窗条目直接删掉,不再原地归零。原地归零会保留条目,而这张表
		// 按出口 IP 建键 —— 订阅轮换会让 IP 不断换代,条目数只增不减。
		delete(h.busy, exitIP)
		return 0
	}
	return r.Count
}

// pruneBusyLocked 批量回收所有已过窗的在途计数条目(R9)。调用方必须已持写锁。
//
// 只删过窗的,不动窗内的零值条目:ReleaseExitBusy 的注释说明过,删掉刚归零的
// 条目再重建会把并发 Inc 的那一次丢掉。窗口(10 分钟)远大于上游单次超时
// (300s),所以过窗条目只可能来自漏调的 Release,不可能还有在途请求挂着。
func (h *Health) pruneBusyLocked(now int64) {
	for ip, r := range h.busy {
		if now-r.At > exitBusyStale {
			delete(h.busy, ip)
		}
	}
}
