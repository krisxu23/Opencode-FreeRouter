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
//   - 淘汰不在注册表里判:1.3.0 起连败状态机搬到 health(热区 2 轮降冷、
//     冷区 3 轮删除),注册表只管「订阅里有什么」。两条删除路径 —— 冷区
//     3 击、订阅刷新后消失 —— 都由 app 执行 Registry.Remove + Health.Forget,
//     **零记录**:免费池里失败记忆毫无价值,节点重新拉到就当全新节点重走
//     首探(用户裁定,墓碑机制随之整体退役)。
//
// 早期版本曾用「连败 3 轮 + 身份墓碑继承」,那套机械在真实数据上暴露了两个
// 问题:抖动池里观察期占全池 97%(淘汰被永久压制)、墓碑把「上一世」的失败
// 记忆贴到「这一世」的节点身上。新模型里抖动容忍由 health 的状态机承担
// (单轮失败不降档、事故滑窗冻结),注册表回归纯粹的成员账本。
//
// 持久化到 data/node-registry.json(原子替换),重启不丢节点、不重测全量。
//
// 约束 9 在这里依然成立:身份键(merge 去重)是 parse.IdentityOf 的逐字输出,
// 现网文件里已有只差大小写的重复键,任何归一化都会错配合并。
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

	// PoolCap 池子上限(src/registry.js:27)。订阅偶尔会给出上万条,放不下时
	// 按加入时间淘汰最旧的。JS 用默认参数表达,Go 没有默认参数,导出给调用方
	// 传给 EnforceCap。
	PoolCap = 8000
)

// entry 池内条目。
type entry struct {
	Outbound   parse.Outbound
	AddedAt    time.Time
	LastSeenAt time.Time
	// flat 是 O24 的落盘形态缓存:snapshotLocked 每次 Flush 都把全池重新
	// flatten(1.1MB 级文件 = 全池 marshal 一遍),而 Outbound 的赋值点只有
	// Merge 的两处 —— 缓存随写失效,Fails 等账目字段不进 flat。调用方
	// 必须持 r.mu。
	flat     map[string]any
	flatHave bool
}

// flatForm 返回(必要时构建)这一条的落盘形态;构建失败的条目恒返回 false
// (与 snapshotLocked 的跳过语义一致)。调用方必须持 r.mu。
func (e *entry) flatForm() (map[string]any, bool) {
	if !e.flatHave {
		ob, err := flattenOutbound(e.Outbound)
		if err != nil {
			return nil, false
		}
		e.flat = ob
		e.flatHave = true
	}
	return e.flat, true
}

// Registry 是节点池本体。entries 按 **tag** 键(与 JS 版及 data/node-
// registry.json 实测形状一致)。1.3.0 起不再维护墓碑表:淘汰=彻底删除,
// 没有「继承失败记忆」的需求。
type Registry struct {
	mu      sync.RWMutex
	file    string
	entries map[string]*entry
	// writeFile 是落盘接缝,生产路径就是 persistence.WriteJSONFile。它存在的唯一
	// 理由是把 R15 的纪律(锁内只取快照、写盘在锁外)变成可断言的事实:测试把写盘
	// 卡住,再去看池子读取能不能照常返回。没有这个接缝就只能拿计时采样赌磁盘。
	// flushMu 只串「写盘」这一段(不碰 r.mu,所以 R15 的收益原样保留:读路径不会
	// 被写盘挡住)。配套的 flushSeq/writtenSeq 补上 R15 留下的洞:把写挪到锁外
	// 之后,「先取快照」与「先落盘」不再同序 —— 晚到的旧快照会把新快照盖掉,而
	// 这一份文件带着淘汰判决,被盖一次就是被淘汰的节点在下次启动复活
	// (整分支评审 RISK-6;可达窗口:开场订阅的 Flush 不在 rebuildMu 内)。
	// 序号在取快照时发号(持 r.mu),写盘前比对:低于已落盘序号就直接丢弃。
	flushMu    sync.Mutex
	flushSeq   uint64
	writtenSeq uint64
	writeFile  func(file string, v any, indent bool) error
	// generation 每次**成员关系**变化 +1(Load/Merge/EnforceCap/Remove)。
	// 它存在的唯一理由是让调用方能安全缓存「池子的派生视图」(O10:候选池 + 每 tag
	// 一次 CountryOf 的四级正则扫描,过去每请求重算一次,池上限 8000)。
	generation uint64
}

// Generation 是当前成员关系代数。缓存方按它判新旧:代数没变就可以直接用缓存的
// 视图,不必知道池子具体动了哪一条。调用方必须**先读代数、再取内容**(见 app 的
// pool 缓存),否则可能在两次读之间被 Merge 抢过去,把新代的内容配到旧代上。
func (r *Registry) Generation() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.generation
}

// NewRegistry 返回空池,**不读盘**;显式 Load()(与 JS initRegistry 在进程
// 启动时读一次不同,Go 把「何时载入」交给调用方,便于测试与热替换)。
func NewRegistry(file string) *Registry {
	return &Registry{
		file:      file,
		entries:   map[string]*entry{},
		writeFile: persistence.WriteJSONFile,
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
		Entries map[string]diskEntryIn `json:"entries"`
		// 旧文件里的 tombstones 键在 1.3.0 墓碑退役后按未知字段忽略。
	}
	if err := persistence.ReadJSONFile(r.file, &df); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			r.entries = map[string]*entry{}
			r.generation++
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
		// fails/lastFailAt 有意**不载入**:1.3.0 起连败状态机整体搬到 health
		// （见包注释的「零记录」裁定），注册表回归纯粹的成员账本。盘上这两个
		// 键由旧版本写成、现在恒为 0，按未知字段忽略（registry_test.go 的
		// 「1.3.0:fails/墓碑随机制退役」一条钉的就是这个）。
		entries[tag] = e
	}
	r.entries = entries
	r.generation++
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
			// 新 tag 直接干净入池:1.3.0 起没有墓碑,失败记忆不再跨「池外」
			// 生命周期 —— 重新拉到的节点就是全新节点(用户裁定)。
			e := &entry{Outbound: o, AddedAt: now, LastSeenAt: now}
			r.entries[o.Tag] = e
			seen[key] = true
			added++
			continue
		}
		// 同 tag 但身份变了(机场轮换凭据/改 sni):原位更新,**不重置连败** ——
		// tag 没变就还是「那个位置的节点」,JS :173-175 同款。身份真的变了
		// 必须涨 generation:派生视图(候选池按 tag 国旗段的 CountryOf、
		// 出口拨号闭包按新凭据)跟着身份走,缓存方按代数判新旧(O10)。
		// 过去这个分支不涨代数,而 Generation() 的注释宣称它是派生视图的
		// 失效信号 —— 接线缓存的瞬间这就是一个错账。
		existing.LastSeenAt = now
		if parse.IdentityOf(existing.Outbound) != key {
			r.generation++
		}
		existing.Outbound = o
		existing.flat = nil
		existing.flatHave = false
		seen[key] = true
	}
	if added > 0 {
		r.generation++ // 只有真的进了新 tag 才算换了成员关系(O10)
	}
	return added
}

// EnforceCap 池子超上限时按加入时间淘汰最旧的,返回**被驱逐的 tag 列表**
// (JS :271-282)。返回名单而不是个数:调用方必须把这批 tag 交给
// Health.Forget —— 只删注册表不删健康行,被淘汰节点的行会一直留在
// node-health.json 里(生命周期审计 D-C1/D-A2)。PruneStale 只在**探测轮**
// 里跑,池子静默(订阅刷新淘汰而探测没跑)时没有任何回收点。
// 容量不是节点的罪,驱逐**不立墓碑** —— 这些只是挤不下的,不是被判死的。
func (r *Registry) EnforceCap(cap int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) <= cap {
		return nil
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
	var evicted []string
	for _, x := range all {
		if len(r.entries) <= cap {
			break
		}
		delete(r.entries, x.tag)
		evicted = append(evicted, x.tag)
	}
	if len(evicted) > 0 {
		r.generation++
	}
	return evicted
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
	r.generation++
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
//
// 锁的边界(R15):锁内只生成快照,序列化与原子写都在锁外 —— 实测
// node-registry.json 有 1.1MB,每请求的候选池读取走 All()(同一把锁的 RLock),
// 持锁跨过写盘就等于每轮 rebuild/probe 给所有在途请求加几十毫秒的排队。
// health.Persist 从一开始就是这个写法,这里是同一条纪律。
//
// 代价与它的补法:落盘的那份快照可能在写出去之前被后面的改动超越。超越本身无害
// (下一次 Flush 会补上),**乱序落盘**才有害 —— 旧快照后写就会把新状态盖掉,所以
// 每次取快照发一个单调序号,写盘在 flushMu 内比对序号,落后的一路直接丢弃。
func (r *Registry) Flush() error {
	r.mu.Lock()
	snap := r.snapshotLocked()
	r.flushSeq++
	seq := r.flushSeq
	r.mu.Unlock()
	return r.writeSnapshot(seq, snap)
}

// writeSnapshot 按序号落一份快照:序号不领先于已落盘的那一份就直接丢弃。
//
// 这一层单独出来是因为它才是 RISK-6 的实质:「锁内取快照、锁外写盘」之后,谁先取
// 快照与谁先拿到写权不再同序,晚到的旧快照会把新状态盖掉,而这文件里躺着连败计数
// 与墓碑 —— 被盖一次就等于被淘汰的节点在下次启动复活。写段由 flushMu 串行(读
// 路径不碰它,R15 的收益不变),序号比较负责丢弃过期快照。
//
// 丢弃**不是**失败:盘上现在是更新的状态,所以返回 nil。
func (r *Registry) writeSnapshot(seq uint64, snap diskFile) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	if seq <= r.writtenSeq {
		return nil
	}
	if err := r.writeFile(r.file, snap, true); err != nil {
		return fmt.Errorf("registry: %w", err)
	}
	r.writtenSeq = seq
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
	Entries map[string]diskEntry `json:"entries"`
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

// diskEntryIn 是载入侧的条目形状:outbound 留原始字节交给 absorbOutbound。
//
// 曾经这里还有 Fails *int 与 LastFailAt int64(指针用来区分「缺失」与 0),
// 两个字段从 1.3.0 起**零生产调用点** —— 连败状态机整体退役、迁到 health,
// 盘上的旧键按未知字段忽略即可。留着它们会让读者以为「载入会恢复失败记忆」,
// 而它实际什么都不做。
type diskEntryIn struct {
	Outbound   json.RawMessage `json:"outbound"`
	AddedAt    int64           `json:"addedAt"`
	LastSeenAt int64           `json:"lastSeenAt"`
}

// snapshotLocked 生成内存 → 落盘的快照。
func (r *Registry) snapshotLocked() diskFile {
	df := diskFile{
		Entries: make(map[string]diskEntry, len(r.entries)),
	}
	for tag, e := range r.entries {
		ob, ok := e.flatForm()
		if !ok {
			// json 链路对纯数据结构不会失败;真来了就跳过这一条,不让单个坏
			// 条目把整份注册表锁死(与 Load 的逐条守卫对称)。
			continue
		}
		// Fails/LastFailAt 恒写 0:1.3.0 起连败机制整体退役（包注释），这两个键
		// 是为了**字节兼容**现网文件与 JS 版才留在形状里，写出来的 0 与
		// 「键缺失」语义相同（载入侧按未知字段忽略，见 Load）。
		df.Entries[tag] = diskEntry{
			Outbound:   ob,
			AddedAt:    msOf(e.AddedAt),
			LastSeenAt: msOf(e.LastSeenAt),
		}
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
