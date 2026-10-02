// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package registry 是网关里唯一的节点池(移植自 src/registry.js,该文件是
// 行为的最终事实)。
//
// 所有节点增删改都发生在这一个池子里,没有第二份名单:
//
//   - 订阅拉取只是「增量输入」:新节点去重后加入,已有节点永不因某次拉取
//     失败而消失(源抖动不会引起池子横跳)。
//   - 连续 MaxFails 轮检测失败才彻底删除;中途任何一轮通关即清零。淘汰时按
//     身份立墓碑;TTL 之内,同一个物理节点(哪怕换了个名字)从订阅里重新
//     出现会继承连败计数 —— 不是干净复活。墓碑也不是永久黑名单,超 TTL 放
//     它干净复活,这一点是刻意的。
//
// 早期版本用「单轮判定」,那是一次过度纠正:探针三源并发,任何一环抖一下,
// 整池节点会在同一轮里被判死,爆炸半径远超一次探针故障。实测事故:
// `2590 → 97(淘汰 2554,98.6%)`、`63/2368 alive`、`0/63 alive` —— 池子在
// 一轮内被清零。现在爆的是连败计数,抖动容忍不再只压在探测单点上
// (src/registry.js:13-18)。
//
// 持久化到 data/node-registry.json(原子替换),重启不丢节点、不重测全量;
// 墓碑也跨重启活着 —— 「重启一次就把全部淘汰判决忘掉」正是墓碑要防的事,
// churn 与重启是两个独立事件(src/registry.js:107-108)。
//
// 约束 9 在这里是主战场:墓碑键是 parse.IdentityOf 的逐字输出,现网文件里
// 已有只差大小写的重复键,它们各自对应一个真实的、名字不同的节点 —— 任何
// 大小写/空白归一化都会让墓碑错配到错误的节点上,把别的节点提前淘汰。
package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"sync"
	"time"

	"freerouter/internal/parse"
	"freerouter/internal/persistence"
)

const (
	// MaxFails 连续多少轮检测失败才彻底删除(src/registry.js:36)。不是 1
	// (单轮判定能一轮清零整池,见包注释的事故),也不宜太大 —— 免费节点的
	// 寿命本来就短,长期留着死节点只会让每轮探测白烧配额与时间。3 轮 × 默认
	// 30 分钟探测间隔 ≈ 90 分钟的容忍窗口。
	MaxFails = 3

	// TombstoneTTL 墓碑存活时长(src/registry.js:52 的 6 小时)。TTL 之内
	// 复活继承连败,治的是订阅 churn 造出的软淘汰回路:实测刚被判死的节点在
	// 同一天里原样回来,每一轮探测都在重测同一批死节点。超期后允许真正复活
	// —— 定 6 小时是因为 churn 以订阅刷新为周期发生(实测 1 小时内就有一次),
	// 更长会把「换了个出口 IP 的真新节点」也一起压掉(src/registry.js:38-51)。
	TombstoneTTL = 6 * time.Hour

	// PoolCap 池子上限(src/registry.js:27)。订阅偶尔会给出上万条,放不下时
	// 按加入时间淘汰最旧的。JS 用默认参数表达,Go 没有默认参数,导出给调用方
	// 传给 EnforceCap。
	PoolCap = 8000
)

// tombstoneCap 墓碑数量上限(src/registry.js:54):churn 一天能产出几千条,
// 超过就按最后失败时间淘汰最旧的。只在内部 prune 时使用,不导出。
const tombstoneCap = 20000

// entry 池内条目。LastFailAt 零值 = 从未失败(JS 用数字 0 表达同一语义)。
type entry struct {
	Outbound   parse.Outbound
	AddedAt    time.Time
	LastSeenAt time.Time
	Fails      int
	LastFailAt time.Time
}

// tombstone 淘汰判决的持久记忆。**按身份而不是 tag 存**:订阅里同一个物理
// 节点换个名字(`US-01` → `🇺🇸 US-01`)是常态,按 tag 存等于换个马甲就洗白
// (src/registry.js:57-61)。
type tombstone struct {
	Tag        string
	Fails      int
	LastFailAt time.Time
}

// Registry 是节点池本体。entries 按 **tag** 键(与 JS 版及 data/node-
// registry.json 实测形状一致),tombstones 按身份键。不维护「身份 → tag」
// 索引:JS 每轮 merge 现算身份集合(src/registry.js:149),常驻索引要在
// Remove/Load/update 三条路径上手工保鲜,漏一处就是静默错配 —— O(n) 现算
// 对 8000 上限的池子毫秒级,不值得换。
type Registry struct {
	mu         sync.RWMutex
	file       string
	entries    map[string]*entry
	tombstones map[string]*tombstone
}

// NewRegistry 返回空池,**不读盘**;显式 Load()(与 JS initRegistry 在进程
// 启动时读一次不同,Go 把「何时载入」交给调用方,便于测试与热替换)。
func NewRegistry(file string) *Registry {
	return &Registry{
		file:       file,
		entries:    map[string]*entry{},
		tombstones: map[string]*tombstone{},
	}
}

// Load 从盘上载入注册表。文件不存在不是错误(首次运行);文件损坏是错误 ——
// 静默回退空池的话,下一次 Flush 就会把用户攒下的整个池子抹掉。载入守卫
// 照抄 src/registry.js:90-115:条目键必须与 outbound.tag 一致(:97);fails
// 只认正整数(:102);过期墓碑不载入,等于放它复活(:112)。
func (r *Registry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var df struct {
		Entries    map[string]diskEntryIn   `json:"entries"`
		Tombstones map[string]diskTombstone `json:"tombstones"`
	}
	if err := persistence.ReadJSONFile(r.file, &df); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			r.entries = map[string]*entry{}
			r.tombstones = map[string]*tombstone{}
			return nil
		}
		return fmt.Errorf("registry: 载入 %s: %w", r.file, err)
	}
	now := time.Now()
	entries := make(map[string]*entry, len(df.Entries))
	for tag, de := range df.Entries {
		if len(de.Outbound) == 0 {
			continue
		}
		o, err := absorbOutbound(de.Outbound)
		if err != nil {
			return fmt.Errorf("registry: 载入 %s: 条目 %q: %w", r.file, tag, err)
		}
		if o.Tag != tag {
			continue // JS :97:键与 outbound.tag 不一致的条目不可信,丢弃
		}
		e := &entry{Outbound: o, AddedAt: now, LastSeenAt: now}
		if de.AddedAt != 0 {
			e.AddedAt = time.UnixMilli(de.AddedAt)
		}
		if de.LastSeenAt != 0 {
			e.LastSeenAt = time.UnixMilli(de.LastSeenAt)
		}
		if de.Fails != nil && *de.Fails > 0 {
			e.Fails = *de.Fails
		}
		if de.LastFailAt != 0 {
			e.LastFailAt = time.UnixMilli(de.LastFailAt)
		}
		entries[tag] = e
	}
	tombs := make(map[string]*tombstone, len(df.Tombstones))
	for key, dt := range df.Tombstones {
		// lastFailAt 缺失/为 0 的墓碑在 JS 里过不了 Number.isFinite 这关(:111);
		// 过期的直接不载入 —— 也就等于放它复活(:112)。
		if dt.LastFailAt <= 0 || now.Sub(time.UnixMilli(dt.LastFailAt)) > TombstoneTTL {
			continue
		}
		tombs[key] = &tombstone{Tag: dt.Tag, Fails: dt.Fails, LastFailAt: time.UnixMilli(dt.LastFailAt)}
	}
	r.entries = entries
	r.tombstones = tombs
	return nil
}

// Merge 增量合并一批订阅出站,返回池子净新增的 tag 数(JS mergeNodes 的
// added + revived:复活也是池子净增;Go 签名只回一个 int,把两个数合并;
// duplicate 与 updated 不计)。JS 对应 src/registry.js:140-189。
func (r *Registry) Merge(outs []parse.Outbound) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	// 去重键 = 身份,**不含 tag**(src/registry.js:145-148):订阅里同一个
	// 物理节点换了个名字("US-01" → "🇺🇸 US-01")仍是同一个出口,占两个池位
	// 只会让端口段翻倍消耗、并让同一出口拿到两个判决。
	seen := make(map[string]bool, len(r.entries))
	for _, e := range r.entries {
		seen[parse.IdentityOf(e.Outbound)] = true
	}
	added := 0
	for _, o := range outs {
		if o.Tag == "" {
			continue // JS :152:没有名字的条目进不了池子
		}
		key := parse.IdentityOf(o)
		existing, has := r.entries[o.Tag]
		if seen[key] {
			// 身份已存在于池中(JS :155-161):tag 相同则只是重复订阅条目,
			// 只摸一下 lastSeenAt;tag 不同则是同一出口的别名,同样不重复入池。
			if has {
				existing.LastSeenAt = now
			}
			continue
		}
		if !has {
			// 新 tag:若身份还在墓碑里,继承连败计数(TOMBSTONE_TTL 之内)。
			// 继承来的计数**不额外 +1** —— 它这一轮还没被探测过,淘汰的判定权
			// 留给探测轮,merge 只负责不让它假装自己从没失败过(JS :162-171)。
			inherited, ok := r.inheritedFailsLocked(key, now)
			e := &entry{Outbound: o, AddedAt: now, LastSeenAt: now, Fails: inherited}
			if ok {
				e.LastFailAt = now // JS :167:复活条目的 lastFailAt 锚到复活时刻
			}
			r.entries[o.Tag] = e
			seen[key] = true
			added++
			continue
		}
		// 同 tag 但身份变了(机场轮换凭据/改 sni):原位更新,**不重置连败** ——
		// tag 没变就还是「那个位置的节点」,JS :173-175 同款。
		existing.LastSeenAt = now
		existing.Outbound = o
		seen[key] = true
	}
	return added
}

// inheritedFailsLocked 墓碑里该身份还新鲜吗?新鲜则返回它记录的连败计数;
// 过期的当场删除(JS :86 的懒清理)—— 也就等于放它干净复活。
func (r *Registry) inheritedFailsLocked(key string, now time.Time) (int, bool) {
	t, ok := r.tombstones[key]
	if !ok {
		return 0, false
	}
	if now.Sub(t.LastFailAt) > TombstoneTTL {
		delete(r.tombstones, key)
		return 0, false
	}
	return t.Fails, true
}

// NoteFail 给节点记一轮连败。JS 里这件事发生在 retainOnly 的死节点分支
// (src/registry.js:233-234);Go 把「计败」与「淘汰判决」拆成两个调用:
// 探测层每确认一轮失败记一次,health 在组 alive 名单时执行连败门槛(见
// RetainOnly)。不在池子里的 tag 没有可记的对象,忽略。
func (r *Registry) NoteFail(tag string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[tag]
	if !ok {
		return
	}
	e.Fails++
	e.LastFailAt = now
}

// RetainOpts 是 RetainOnly 的判据。
//
// ProbedTags:本轮**真的被探测过**的 tag 集合。没被探测的节点(端口段满、
// 没分到端口的那些)必须原样保留 —— 否则「端口不够用」会被静默转写成「节点
// 被判死并删除」,真正的故障原因被替换成一个无关的原因(JS :215-220)。
// nil 集合等价于 JS 传 null(:218):全部视为已探测。
//
// Protected:本轮测过、失败,但连败还没到门槛的观察期节点。它们必须**既不淘汰、
// 也不清零连败**。这一项过去不存在,观察期节点被塞进 alive 来「不淘汰」,而 alive
// 同时意味着「通关 → 连败清零」,于是计数每轮 1→0,门槛永远够不到 —— 现场表现是
// 日志里 `淘汰 0 · 观察期 1601` 长期钉死,整池死节点一个都不处理。
//
// MaxFails:-1 表示 Infinity(事故轮,只记账不淘汰)。正值的连败门槛由调用方
// (health)用 alive/Protected 两个名单表达 —— Go 把 JS retainOnly 里耦在一起的
// 「计败」(e.fails += 1,JS :233)与「判决」(fails >= maxFails,JS :236)拆成了
// NoteFail + 两个名单,本方法只认 -1 哨兵;常规轮请传 registry.MaxFails。
type RetainOpts struct {
	ProbedTags map[string]bool
	Protected  map[string]bool
	MaxFails   int
}

// RetainOnly 执行淘汰判决,返回被淘汰的 tag(JS retainOnly 的 removedTags,
// src/registry.js:213-256;调用方据此清理端口与健康状态)。alive 是**本轮通关**
// 的名单;观察期(测过、失败、连败未到门槛)的节点走 opts.Protected。
//
//   - 淘汰判据 = 本轮测过 ∧ 不在 alive ∧ 不在 Protected ∧ MaxFails != -1;
//   - 淘汰必先立墓碑再删条目:删除会带走条目本身,而连败计数必须活下来,
//     这是整条机制唯一的持久记忆(JS :237-239);
//   - 通关的节点把连败清零并顺手清掉同身份墓碑(JS :226-229);观察期的节点
//     计数不动 —— 清零就等于把它退回起点,门槛永远够不到。
func (r *Registry) RetainOnly(alive []string, opts RetainOpts) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	aliveSet := make(map[string]bool, len(alive))
	for _, tag := range alive {
		aliveSet[tag] = true
	}
	dropped := []string{}
	for tag, e := range r.entries {
		if aliveSet[tag] {
			if e.Fails != 0 {
				e.Fails = 0 // 通关即清零(JS :226)
			}
			// 活过来了就把墓碑清掉:留着它只会在下次 churn 时把一次已失效的
			// 旧判决重新贴到这个节点身上(JS :228-229)。
			delete(r.tombstones, parse.IdentityOf(e.Outbound))
			continue
		}
		if opts.Protected[tag] {
			// 观察期:条目与连败计数都原样留着,下一轮继续往上累加。
			continue
		}
		if opts.ProbedTags != nil && !opts.ProbedTags[tag] {
			continue // 本轮没测到它,保持原判(JS :232)
		}
		if opts.MaxFails == -1 {
			continue // 事故轮:Infinity —— 本轮只记账(NoteFail 已计),不淘汰
		}
		// 墓碑**在删除之前**立(JS :239)。计数用条目当前值:含复活继承的旧账
		// 与之后 NoteFail 的新账,同一身份多次淘汰自然累计。
		r.tombstones[parse.IdentityOf(e.Outbound)] = &tombstone{Tag: tag, Fails: e.Fails, LastFailAt: now}
		delete(r.entries, tag)
		dropped = append(dropped, tag)
	}
	r.pruneTombstonesLocked() // JS :246-253
	sort.Strings(dropped)     // Go map 无序;JS 的 removedTags 按池内插入序,这里退化成字典序(调用方只按 tag 清理,顺序无语义)
	return dropped
}

// pruneTombstonesLocked 把墓碑数量压到 tombstoneCap 以内,按 lastFailAt 升序
// 删最旧的(JS :246-253)。churn 一天能产出几千条墓碑,不设上限文件会无限
// 膨胀。
func (r *Registry) pruneTombstonesLocked() {
	if len(r.tombstones) <= tombstoneCap {
		return
	}
	type kv struct {
		key string
		t   *tombstone
	}
	all := make([]kv, 0, len(r.tombstones))
	for k, t := range r.tombstones {
		all = append(all, kv{k, t})
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].t.LastFailAt.Equal(all[j].t.LastFailAt) {
			return all[i].t.LastFailAt.Before(all[j].t.LastFailAt)
		}
		return all[i].key < all[j].key // 同毫秒:JS 靠 Map 插入序,Go map 无序,退化成键字典序
	})
	for _, x := range all {
		if len(r.tombstones) <= tombstoneCap {
			break
		}
		delete(r.tombstones, x.key)
	}
}

// EnforceCap 池子超上限时按加入时间淘汰最旧的,返回驱逐数(JS :271-282)。
// 容量不是节点的罪,驱逐**不立墓碑** —— 这些只是挤不下的,不是被判死的。
func (r *Registry) EnforceCap(cap int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) <= cap {
		return 0
	}
	type kv struct {
		tag string
		e   *entry
	}
	all := make([]kv, 0, len(r.entries))
	for tag, e := range r.entries {
		all = append(all, kv{tag, e})
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].e.AddedAt.Equal(all[j].e.AddedAt) {
			return all[i].e.AddedAt.Before(all[j].e.AddedAt)
		}
		// 同毫秒(JS 一次 merge 共用一个 now,同批必同毫秒):JS 靠 Map 插入序,
		// Go map 无序,退化成 tag 字典序 —— 只影响同批内谁先出局,不影响总量。
		return all[i].tag < all[j].tag
	})
	removed := 0
	for _, x := range all {
		if len(r.entries) <= cap {
			break
		}
		delete(r.entries, x.tag)
		removed++
	}
	return removed
}

// Remove 彻底删除一个节点(check 自愈剔除用),**不立墓碑**(JS :200-204):
// 自愈剔除是主动修复,不是探测判决,不该给下次合并记仇。
func (r *Registry) Remove(tag string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[tag]; !ok {
		return false
	}
	delete(r.entries, tag)
	return true
}

// All 当前池子里的全部出站(JS :192-194)。切片是新分配的,元素是浅拷贝 ——
// 与 JS 一样共享内层对象,调用方不得改写 TLS/Transport 等指针字段。
func (r *Registry) All() []parse.Outbound {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]parse.Outbound, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.Outbound)
	}
	return out
}

// Has 池子里有没有这个 tag。
func (r *Registry) Has(tag string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.entries[tag]
	return ok
}

// FailCount 某个节点的连败计数,0 = 健康或从未失败(JS :266-268)。
func (r *Registry) FailCount(tag string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.entries[tag]; ok {
		return e.Fails
	}
	return 0
}

// Len 池子大小(JS size,:196-198)。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}

// Flush 把当前快照落盘(JS 的 persist :125-133 与 flush :284-286)。
//
// 与 JS 的差异要写清楚:JS 在每个改动点(merge/retain/enforce)当场 persist,
// 因为托盘退出走 `taskkill /T /F`(硬杀,退出钩子不会执行),注册表必须在
// 改动点就落盘;而改动频率是每轮 rebuild/probe 一次,同步写几百 KB 可忽略。
// Go 版把落盘做成显式 Flush,由 app 在每轮 rebuild/probe 之后调用(任务 20
// 接线)。契约是「改完就 Flush」—— 调用方忘掉它会丢掉自上次 Flush 以来的
// 全部增删改,这个窗口的性质与 JS 注释里那个 8 分 20 秒的窗口相同。
func (r *Registry) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := persistence.WriteJSONFile(r.file, r.snapshotLocked(), true); err != nil {
		return fmt.Errorf("registry: %w", err)
	}
	return nil
}

// diskFile 是 data/node-registry.json 的落盘形状:
//
//	{"entries":{tag:{outbound,addedAt,lastSeenAt,fails,lastFailAt}},
//	 "tombstones":{identity:{tag,fails,lastFailAt}}}
//
// 时间字段是 epoch 毫秒数(总纲 §5 与现网文件实测;计划正文写的 RFC3339
// 是错的,照它做 JS 版就读不了现网文件)。
type diskFile struct {
	Entries    map[string]diskEntry     `json:"entries"`
	Tombstones map[string]diskTombstone `json:"tombstones"`
}

type diskEntry struct {
	// Outbound 用扁平 map:Extra 的键摊到顶层,与 JS 落盘形状逐键一致
	// (见 flattenOutbound)。
	Outbound   map[string]any `json:"outbound"`
	AddedAt    int64          `json:"addedAt"`
	LastSeenAt int64          `json:"lastSeenAt"`
	Fails      int            `json:"fails"`
	LastFailAt int64          `json:"lastFailAt"`
}

type diskTombstone struct {
	Tag        string `json:"tag"`
	Fails      int    `json:"fails"`
	LastFailAt int64  `json:"lastFailAt"`
}

// diskEntryIn 是载入侧的条目形状:outbound 留原始字节交给 absorbOutbound,
// fails 用指针区分「缺失」与 0(JS :102 对非正整数一律归 0,两者结果相同,
// 指针只是让判断直白)。
type diskEntryIn struct {
	Outbound   json.RawMessage `json:"outbound"`
	AddedAt    int64           `json:"addedAt"`
	LastSeenAt int64           `json:"lastSeenAt"`
	Fails      *int            `json:"fails"`
	LastFailAt int64           `json:"lastFailAt"`
}

// snapshotLocked 生成内存 → 落盘的快照。
func (r *Registry) snapshotLocked() diskFile {
	df := diskFile{
		Entries:    make(map[string]diskEntry, len(r.entries)),
		Tombstones: make(map[string]diskTombstone, len(r.tombstones)),
	}
	for tag, e := range r.entries {
		ob, err := flattenOutbound(e.Outbound)
		if err != nil {
			// json 链路对纯数据结构不会失败;真来了就跳过这一条,不让单个坏
			// 条目把整份注册表锁死(与 Load 的逐条守卫对称)。
			continue
		}
		df.Entries[tag] = diskEntry{
			Outbound:   ob,
			AddedAt:    msOf(e.AddedAt),
			LastSeenAt: msOf(e.LastSeenAt),
			Fails:      e.Fails,
			LastFailAt: msOf(e.LastFailAt),
		}
	}
	for key, t := range r.tombstones {
		df.Tombstones[key] = diskTombstone{Tag: t.Tag, Fails: t.Fails, LastFailAt: msOf(t.LastFailAt)}
	}
	return df
}

// msOf 时间 → epoch 毫秒。零值 time 映射成 0 —— JS 用数字 0 表示「从未失败」
// (src/registry.js:103/:167),time.Time 零值直接 UnixMilli 会得到一个公元
// 前的负数,JS 读回 Number.isFinite 为 true,语义就坏了。
func msOf(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// modeledKeys 是 parse.Outbound 的 JSON 键集合(internal/parse/outbound.go)。
// 落盘文件顶层不在这个集合里的键,全部收进 Extra —— 与解析层把不建模的查询
// 参数收进 Extra 是同一条约束 8 纪律。给 parse.Outbound 加字段时记得同步这份
// 清单:漏了不会坏数据(该字段会落进 Extra,并被 SingBoxMap 原样带出到
// sing-box 配置),只是身份指纹看不到它。
var modeledKeys = map[string]bool{
	"tag": true, "type": true, "server": true, "server_port": true,
	"server_ports": true, "uuid": true, "password": true, "method": true,
	"flow": true, "alter_id": true, "path": true, "obfs": true,
	"tls": true, "transport": true, "extra": true,
}

// flattenOutbound 把 parse.Outbound 序列化成 JS 版落盘的**扁平**形状。
//
// parse.Outbound 的 Extra 在 struct 标签下会序列化成嵌套的 "extra" 键,而
// JS 版(data/node-registry.json 实测)把这些字段直接放在 outbound 顶层
// (socks 的 username 等)。嵌着写,JS 版读回时 outbound.username 就是
// undefined,凭据直接丢。生成 sing-box 配置时的摊平逻辑在
// parse.Outbound.SingBoxMap(任务 7),这里是落盘版的同一规则。
func flattenOutbound(o parse.Outbound) (map[string]any, error) {
	b, err := json.Marshal(o)
	if err != nil {
		return nil, fmt.Errorf("registry: outbound 序列化: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("registry: outbound 序列化: %w", err)
	}
	if ex, ok := m["extra"].(map[string]any); ok {
		delete(m, "extra")
		for k, v := range ex {
			if _, taken := m[k]; !taken {
				m[k] = materialize(v) // 建模键永远赢,Extra 只补空位
			}
		}
	}
	return m, nil
}

// absorbOutbound 把落盘的 outbound 字节解码回 parse.Outbound:建模字段走
// 标准解码;顶层未建模字段(如 socks 的 username)收进 Extra —— 否则每次
// Load→Flush 循环都会把 JS 写下的凭据静默抹掉,socks 节点直接失联。
func absorbOutbound(raw json.RawMessage) (parse.Outbound, error) {
	var o parse.Outbound
	if err := json.Unmarshal(raw, &o); err != nil {
		return parse.Outbound{}, fmt.Errorf("registry: outbound 解码: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return parse.Outbound{}, fmt.Errorf("registry: outbound 解码: %w", err)
	}
	for k, v := range m {
		if modeledKeys[k] {
			continue // "extra" 也在内:嵌套旧形状已由标准解码收进 Extra
		}
		if o.Extra == nil {
			o.Extra = parse.Extra{}
		}
		if _, exists := o.Extra[k]; !exists {
			o.Extra[k] = v
		}
	}
	return o, nil
}

// materialize 把「长得像 JSON 的字符串」还原成对象。链接解析层把 multiplex
// 这类查询参数存成 JSON 字符串,JS 版在解析时就 JSON.parse 过了;落盘若是
// 字符串,JS 版读回再喂 sing-box 就会在 decode 期炸。与 parse.materializeJSON
// 同规则(那边不导出):规则就一句「像 JSON 就还原」,复制一份比为此在 L0
// 加导出面划算,漂移风险极低。
func materialize(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if len(s) == 0 || (s[0] != '{' && s[0] != '[') {
		return v
	}
	var parsed any
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return v
	}
	return parsed
}
