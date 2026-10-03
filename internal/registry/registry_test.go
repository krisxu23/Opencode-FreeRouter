// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"freerouter/internal/parse"
)

func ob(tag string) parse.Outbound {
	return parse.Outbound{Tag: tag, Type: "vless", Server: tag, ServerPort: 443}
}

// TestGenerationTracksMembershipOnly 钉住 O10 缓存的正确性前提:代数必须在**成员
// 关系**变化的每一条路径上涨,在不改变成员的路径上不涨。涨漏一条 = 调用方的派生
// 视图里永久留着一个已淘汰的节点(engine 会一直把它当候选);白涨一次只是多算一遍。
func TestGenerationTracksMembershipOnly(t *testing.T) {
	r := newReg(t)
	base := r.Generation()

	r.Merge([]parse.Outbound{ob("a"), ob("b")})
	if g := r.Generation(); g == base {
		t.Fatal("新增 tag 必须涨代数")
	}
	base = r.Generation()
	r.Merge([]parse.Outbound{ob("a")}) // 重复身份:成员没变
	if g := r.Generation(); g != base {
		t.Fatalf("重复合并也涨了代数: %d -> %d", base, g)
	}
	r.NoteFail("a", time.Now()) // 连败不改变成员
	if g := r.Generation(); g != base {
		t.Fatalf("NoteFail 不该涨代数: %d -> %d", base, g)
	}
	if dropped := r.RetainOnly([]string{"a"}, RetainOpts{MaxFails: 1}); len(dropped) != 1 {
		t.Fatalf("RetainOnly dropped = %v, want 一条", dropped)
	}
	if g := r.Generation(); g == base {
		t.Fatal("淘汰必须涨代数:被淘汰的节点不能留在缓存的候选池里")
	}
	base = r.Generation()
	r.Merge([]parse.Outbound{ob("c"), ob("d"), ob("e")})
	if g := r.Generation(); g == base {
		t.Fatal("Merge 新节点后应涨代数")
	}
	base = r.Generation()
	if r.EnforceCap(2) == 0 {
		t.Fatal("EnforceCap 应当有驱逐")
	}
	if g := r.Generation(); g == base {
		t.Fatal("驱逐必须涨代数(O10 的缓存正是靠这一条失效)")
	}
	base = r.Generation()
	// EnforceCap 已经按加入时间挤掉过最旧的几条,这里从现存池子里取一条来验
	// Remove(前面的步骤会随淘汰变化,写死 tag 名会把测试写成对淘汰顺序的断言)。
	left := r.All()
	if len(left) == 0 {
		t.Fatal("池子被前面的步骤清空了,无法验证 Remove")
	}
	if !r.Remove(left[0].Tag) {
		t.Fatalf("Remove(%q) = false", left[0].Tag)
	}
	if g := r.Generation(); g == base {
		t.Fatal("Remove 必须涨代数")
	}
	base = r.Generation()
	if err := r.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if g := r.Generation(); g == base {
		t.Fatal("Load 换掉了整张表,必须涨代数")
	}
}

// TestFlushGuardDropsAnOlderSnapshot 是整分支评审的 RISK-6:R15 把「序列化 + 原子
// 写」挪到 r.mu 之外之后,谁先取快照与谁先拿到写权不再同序。晚到的旧快照会把新
// 状态盖掉,而这文件里躺着连败计数与墓碑 —— 被盖一次就等于被淘汰的节点在下次
// 启动复活(阶段 2 约束 9)。可达:开场订阅协程的 reg.Flush() 不在 rebuildMu 内,
// 托盘 Reload / 面板 Refresh 的 Rebuild 能从那个窗口并发进来。
//
// 乱序本身是调度竞态,没法用挂起钩子确定性制造(第一版尝试这么做,结果与新加的
// flushMu 互相 deadlock —— 旧写持着写锁,主线程的新 Flush 永久等待,而这正是
// flushMu 串行化生效的证明)。所以这里按不变量测:把两份快照按**有害的顺序**直接
// 交给写段,旧的那一份必须被丢弃。
func TestFlushGuardDropsAnOlderSnapshot(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-registry.json")
	r := NewRegistry(file)

	// alloc 复刻 Flush 的前半段:持 r.mu 取快照并领序号。
	alloc := func() (uint64, diskFile) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.flushSeq++
		return r.flushSeq, r.snapshotLocked()
	}

	r.Merge([]parse.Outbound{ob("a")})
	seqOld, snapOld := alloc()
	r.Merge([]parse.Outbound{ob("a"), ob("b")})
	seqNew, snapNew := alloc()
	if seqOld >= seqNew {
		t.Fatalf("序号必须单调:old=%d new=%d", seqOld, seqNew)
	}

	// 有害顺序:新快照先落盘,旧快照后到。
	if err := r.writeSnapshot(seqNew, snapNew); err != nil {
		t.Fatalf("newer write: %v", err)
	}
	if err := r.writeSnapshot(seqOld, snapOld); err != nil {
		t.Fatalf("older write must be dropped, not fail: %v", err)
	}
	if got := entriesOnDisk(t, file); len(got) != 2 {
		t.Fatalf("盘上剩 %d 条(%v):晚到的旧快照把新状态盖掉了", len(got), keysOf(got))
	}

	// 正序仍然照常落盘(守卫不是「只写一次」)。
	r.Merge([]parse.Outbound{ob("a"), ob("b"), ob("c")})
	seqThird, snapThird := alloc()
	if err := r.writeSnapshot(seqThird, snapThird); err != nil {
		t.Fatalf("third write: %v", err)
	}
	if got := entriesOnDisk(t, file); len(got) != 3 {
		t.Fatalf("正序落盘失效: %d 条", len(got))
	}
	// 走生产入口也必须写出去(序号在 Flush 内部领)。
	if err := r.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := entriesOnDisk(t, file); len(got) != 3 {
		t.Fatalf("Flush 之后盘上 %d 条, want 3", len(got))
	}
}

func entriesOnDisk(t *testing.T, file string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("解析落盘文件: %v (%s)", err, raw)
	}
	entries, _ := doc["entries"].(map[string]any)
	return entries
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestFlushWritesTheFileOutsideTheLock 钉住 R15:锁内只生成快照,序列化与原子写
// 都在锁外。实测 node-registry.json 有 1.1MB,而每请求的候选池读取走的是同一把
// 锁的 RLock —— 持锁跨写盘等于每轮 rebuild/probe 给所有在途请求加几十毫秒排队。
// 判据不能靠计时(慢盘是环境属性),所以用落盘接缝把写盘卡在「已经开始」的那一刻:
// 旧实现在这里是真的死锁,而不是「读得慢」。
func TestFlushWritesTheFileOutsideTheLock(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-registry.json")
	r := NewRegistry(file)
	r.Merge([]parse.Outbound{ob("a"), ob("b")})

	entered := make(chan struct{})
	release := make(chan struct{})
	orig := r.writeFile
	r.writeFile = func(path string, v any, indent bool) error {
		close(entered)
		<-release
		return orig(path, v, indent)
	}

	flushed := make(chan error, 1)
	go func() { flushed <- r.Flush() }()
	<-entered

	all := make(chan int, 1)
	go func() { all <- len(r.All()) }()
	select {
	case n := <-all:
		if n != 2 {
			t.Fatalf("写盘窗口里的池子读取 = %d, want 2", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("R15:Flush 在写盘期间仍持着独占锁,候选池读取被卡死")
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatalf("flush: %v", err)
	}
	again := NewRegistry(file)
	if err := again.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if again.Len() != 2 {
		t.Fatalf("len = %d, want 2(写盘挪到锁外之后落盘内容不能少)", again.Len())
	}
}

func newReg(t *testing.T) *Registry {
	t.Helper()
	return NewRegistry(filepath.Join(t.TempDir(), "node-registry.json"))
}

func TestMergeAddsNewTags(t *testing.T) {
	r := newReg(t)
	if n := r.Merge([]parse.Outbound{ob("a"), ob("b")}); n != 2 {
		t.Fatalf("added = %d, want 2", n)
	}
	if r.Len() != 2 {
		t.Fatalf("len = %d, want 2", r.Len())
	}
}

func TestMergeIsIncremental(t *testing.T) {
	// 池 = 历史并集。订阅某个源失败时,不能因为这次没拿到它就把它的节点删掉。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	r.Merge([]parse.Outbound{ob("b")})
	if r.Len() != 2 {
		t.Fatalf("len = %d, want 2 (merge must not replace)", r.Len())
	}
}

func TestMergeRenamedNodeKeepsFailCountByIdentity(t *testing.T) {
	// 换名字但凭据相同 = 同一个物理节点:JS 把它记为 duplicate,**不新开池位**
	// (src/registry.js:145-160 —— 同一出口占两个池位会让端口段翻倍消耗、并让
	// 同一出口拿到两个判决)。连败记忆留在原有条目身上;新 tag 没有自己的条目,
	// 按新 tag 查 FailCount 就是 0。计划原文断言 FailCount("new-name")==2 与
	// JS 相反,按修正案原则(JS 源码是最终事实)改写为断言 duplicate 语义。
	// 换凭据 = 新身份,不继承(src/registry.js:162-171 只继承墓碑里的记忆)。
	r := newReg(t)
	base := parse.Outbound{Tag: "old-name", Type: "vless", Server: "h", ServerPort: 443, UUID: "u1"}
	r.Merge([]parse.Outbound{base})
	r.NoteFail("old-name", time.Now())
	r.NoteFail("old-name", time.Now())
	if got := r.FailCount("old-name"); got != 2 {
		t.Fatalf("fails = %d, want 2", got)
	}
	renamed := base
	renamed.Tag = "new-name"
	if n := r.Merge([]parse.Outbound{renamed}); n != 0 {
		t.Fatalf("renamed alias added %d entries, want 0 (duplicate by identity)", n)
	}
	if r.Len() != 1 {
		t.Fatalf("len = %d, want 1 (alias must not take a second slot)", r.Len())
	}
	if got := r.FailCount("old-name"); got != 2 {
		t.Fatalf("fail memory lost on the original entry: %d, want 2", got)
	}
	rotated := renamed
	rotated.UUID = "u2"
	if n := r.Merge([]parse.Outbound{rotated}); n != 1 {
		t.Fatalf("rotated credentials added %d, want 1", n)
	}
	if got := r.FailCount("new-name"); got != 0 {
		t.Fatalf("rotated credentials inherited %d fails, want 0", got)
	}
}

func TestTombstoneKeyIsCaseSensitive(t *testing.T) {
	// 约束 9:data/node-registry.json 里就有只差大小写的墓碑键。
	// 归一化会让它们互相错配,把别的节点提前淘汰。
	r := newReg(t)
	lower := parse.Outbound{Tag: "a", Type: "vless", Server: "h", ServerPort: 443, TLS: &parse.TLS{ServerName: "ov-germany1.09vpn.com"}}
	upper := lower
	upper.TLS = &parse.TLS{ServerName: "OV-Germany1.09vpn.com"}
	upper.Tag = "b"
	if parse.IdentityOf(lower) == parse.IdentityOf(upper) {
		t.Fatalf("test is wrong: identities collided at %q", parse.IdentityOf(lower))
	}
	r.Merge([]parse.Outbound{lower})
	r.NoteFail("a", time.Now())
	if r.FailCount("a") == 0 {
		t.Fatal("seed a failure")
	}
	// upper 是不同身份:不该继承 a 的连败
	r.Merge([]parse.Outbound{upper})
	if got := r.FailCount("b"); got != 0 {
		t.Fatalf("case-different node inherited %d fails", got)
	}
}

func TestRetainOnlyKeepsAliveTags(t *testing.T) {
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a"), ob("b"), ob("c")})
	dropped := r.RetainOnly([]string{"a"}, RetainOpts{
		ProbedTags: map[string]bool{"a": true, "b": true, "c": true}, MaxFails: 3,
	})
	if len(dropped) != 2 {
		t.Fatalf("dropped = %v, want b and c", dropped)
	}
	if r.Len() != 1 || !r.Has("a") {
		t.Fatalf("len = %d, want only a left", r.Len())
	}
}

func TestRetainOnlyKeepsUnprobedTags(t *testing.T) {
	// 探测超时(unknown)的节点没进 ProbedTags:没有证据说它坏了,
	// 不能因为这轮没测到就淘汰 —— 否则「端口不够用」会被静默转写成
	// 「节点被判死并删除」(src/registry.js:215-220 的事故注释)。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a"), ob("b")})
	r.RetainOnly([]string{"a"}, RetainOpts{
		ProbedTags: map[string]bool{"a": true}, MaxFails: 3,
	})
	if !r.Has("b") {
		t.Fatal("an unprobed node was evicted")
	}
}

func TestRetainOnlyProtectsObservationPeriodFails(t *testing.T) {
	// 观察期节点(测过、失败、连败未到门槛)必须既不被淘汰、也不被清零连败。
	// 它过去靠「塞进 alive 名单」来免淘汰,而 alive 名单同时会清零计数,于是
	// 门槛永远够不到 —— 现场就是 `淘汰 0 · 观察期 1601` 长期钉死。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a"), ob("b")})
	r.NoteFail("a", time.Now()) // 观察期:1 < MaxFails
	r.NoteFail("b", time.Now())
	r.NoteFail("b", time.Now())
	r.NoteFail("b", time.Now()) // 到门槛:b 本轮该被淘汰
	// 本轮没有任何节点通关,所以 alive 是空的;a 靠 Protected 免淘汰。
	dropped := r.RetainOnly([]string{}, RetainOpts{
		ProbedTags: map[string]bool{"a": true, "b": true},
		Protected:  map[string]bool{"a": true},
		MaxFails:   3,
	})
	if len(dropped) != 1 || dropped[0] != "b" {
		t.Fatalf("dropped = %v, want [b]", dropped)
	}
	if !r.Has("a") {
		t.Fatal("观察期节点被淘汰了")
	}
	if got := r.FailCount("a"); got != 1 {
		t.Fatalf("观察期连败被清零:%d, want 1", got)
	}
}

func TestRetainOnlyWithMaxFailsInfinityKeepsFailedNodes(t *testing.T) {
	// 事故轮(alive 比例异常低)走这条路:本轮只记账、不淘汰 —— 单轮判定曾在
	// 一轮内清零整个池子(实测 2590 → 97),事故轮必须停掉二次伤害
	// (src/registry.js:213 的 maxFails: Infinity 用法)。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	r.NoteFail("a", time.Now())
	r.NoteFail("a", time.Now())
	r.NoteFail("a", time.Now())
	r.RetainOnly([]string{}, RetainOpts{
		ProbedTags: map[string]bool{"a": true}, MaxFails: -1,
	})
	if !r.Has("a") {
		t.Fatal("a node with 3 fails was evicted during an accident round")
	}
}

func TestRevivedNodeInheritsTheTombstoneFails(t *testing.T) {
	// 墓碑不是永久黑名单,但 TTL 之内复活**不是干净复活**:订阅 churn 会把刚
	// 被判死的节点原样端回来,干净复活会让每一轮探测都重测同一批死节点
	// (src/registry.js:38-51 的事故记录)。复活条目继承墓碑里的连败数
	// (src/registry.js:166-169 `fails: inherited ?? 0`),且**不额外 +1**
	// (:164-165 —— 它这轮还没被探测过,判定权留给探测轮)。
	// 计划原测试断言复活后 FailCount==0,与 JS 相反,按修正案 #2 改写。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	r.NoteFail("a", time.Now())
	r.RetainOnly([]string{}, RetainOpts{ProbedTags: map[string]bool{"a": true}, MaxFails: 3})
	if r.Has("a") {
		t.Fatal("a should have been evicted")
	}
	r.Merge([]parse.Outbound{ob("a")})
	if got := r.FailCount("a"); got != 1 {
		t.Fatalf("a revived node came back with %d fails, want the tombstone's 1", got)
	}
}

func TestTombstonePastTTLRevivesClean(t *testing.T) {
	// 墓碑的另一半语义:超 TTL 的墓碑**不**继承,当场懒删除、干净复活 ——
	// 墓碑不是永久黑名单,这一点是刻意的(src/registry.js:86、:50)。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	r.NoteFail("a", time.Now())
	r.RetainOnly([]string{}, RetainOpts{ProbedTags: map[string]bool{"a": true}, MaxFails: 3})
	id := parse.IdentityOf(ob("a"))
	if _, ok := r.tombstones[id]; !ok {
		t.Fatal("eviction did not leave a tombstone")
	}
	// 直接把墓碑拨旧到 TTL 之外(同包测试,允许碰内部状态)
	r.tombstones[id] = &tombstone{Tag: "a", Fails: 1, LastFailAt: time.Now().Add(-TombstoneTTL - time.Minute)}
	if n := r.Merge([]parse.Outbound{ob("a")}); n != 1 {
		t.Fatalf("revival added %d, want 1", n)
	}
	if got := r.FailCount("a"); got != 0 {
		t.Fatalf("expired tombstone leaked %d fails, want a clean revival", got)
	}
	if len(r.tombstones) != 0 {
		t.Fatal("expired tombstone was not lazily deleted on encounter (JS :86)")
	}
}

func TestRetainOnlyClearsFailsAndDropsTheTombstoneForAlive(t *testing.T) {
	// 通关即清零(JS :226),且活过来就把墓碑清掉 —— 留着它只会在下次 churn
	// 时把一次已失效的旧判决重新贴到这个节点身上(JS :228-229)。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	r.NoteFail("a", time.Now())
	r.tombstones[parse.IdentityOf(ob("a"))] = &tombstone{Tag: "a", Fails: 3, LastFailAt: time.Now()}
	r.RetainOnly([]string{"a"}, RetainOpts{ProbedTags: map[string]bool{"a": true}, MaxFails: 3})
	if got := r.FailCount("a"); got != 0 {
		t.Fatalf("fails = %d, want 0 after an alive round", got)
	}
	if len(r.tombstones) != 0 {
		t.Fatal("tombstone survived an alive round; it must be dropped")
	}
}

func TestRepeatedEvictionsAccumulateFails(t *testing.T) {
	// 同一身份在 TTL 内多次淘汰,计数必须累计:墓碑记的是条目当前连败数
	// (含复活继承 + 之后 NoteFail 的新账,src/registry.js:239),不是每次
	// 从零重记。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	for i := 0; i < 3; i++ {
		r.NoteFail("a", time.Now())
	}
	r.RetainOnly([]string{}, RetainOpts{ProbedTags: map[string]bool{"a": true}, MaxFails: 3})
	r.Merge([]parse.Outbound{ob("a")}) // 复活,继承 3
	if got := r.FailCount("a"); got != 3 {
		t.Fatalf("revival inherited %d fails, want 3", got)
	}
	r.NoteFail("a", time.Now())
	r.RetainOnly([]string{}, RetainOpts{ProbedTags: map[string]bool{"a": true}, MaxFails: 3})
	got := r.tombstones[parse.IdentityOf(ob("a"))].Fails
	if got != 4 {
		t.Fatalf("second eviction carried %d fails, want accumulated 4", got)
	}
}

func TestRemoveDropsTheEntryWithoutATombstone(t *testing.T) {
	// Remove(check 自愈剔除)只删条目,**不立墓碑**(src/registry.js:200-204):
	// 自愈剔除是主动修复,不是探测判决,不该给下次合并记仇。
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	if !r.Remove("a") {
		t.Fatal("remove reported a miss")
	}
	if r.Remove("a") {
		t.Fatal("second remove reported a hit")
	}
	if len(r.tombstones) != 0 {
		t.Fatal("remove wrote a tombstone")
	}
	if n := r.Merge([]parse.Outbound{ob("a")}); n != 1 {
		t.Fatalf("re-merge after remove added %d, want 1", n)
	}
	if got := r.FailCount("a"); got != 0 {
		t.Fatalf("re-added node came back with %d fails, want a clean slate", got)
	}
}

func TestEnforceCapEvictsTheOldest(t *testing.T) {
	r := newReg(t)
	for i := 0; i < 5; i++ {
		r.Merge([]parse.Outbound{ob(string(rune('a' + i)))})
	}
	if n := r.EnforceCap(3); n != 2 {
		t.Fatalf("evicted = %d, want 2", n)
	}
	if r.Len() != 3 {
		t.Fatalf("len = %d, want 3", r.Len())
	}
}

func TestFlushRoundTrips(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-registry.json")
	r := NewRegistry(file)
	r.Merge([]parse.Outbound{ob("a")})
	r.NoteFail("a", time.Now())
	if err := r.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	again := NewRegistry(file)
	if err := again.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if again.Len() != 1 || !again.Has("a") {
		t.Fatalf("roundtrip lost the entry: len %d", again.Len())
	}
	if got := again.FailCount("a"); got != 1 {
		t.Fatalf("roundtrip lost the fail count: %d, want 1", got)
	}
}

func TestFlushWritesExtraFlatLikeJS(t *testing.T) {
	// 约束 8 的落盘面:Extra 存的是「本项目不显式建模、但 sing-box/JS 版要读」
	// 的字段(如 socks 的 username)。JS 版落盘时它们在 outbound 顶层
	// (data/node-registry.json 实测);嵌在 "extra" 键下,JS 版读回时
	// outbound.username 就是 undefined,凭据直接丢。
	file := filepath.Join(t.TempDir(), "node-registry.json")
	r := NewRegistry(file)
	o := ob("a")
	o.Extra = parse.Extra{"username": "u1"}
	r.Merge([]parse.Outbound{o})
	if err := r.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var disk struct {
		Entries map[string]struct {
			Outbound map[string]any `json:"outbound"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(b, &disk); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, ok := disk.Entries["a"].Outbound["username"]
	if !ok || got != "u1" {
		t.Fatalf("username not at outbound top level: %v", disk.Entries["a"].Outbound)
	}
	if _, nested := disk.Entries["a"].Outbound["extra"]; nested {
		t.Fatal("extra must be flattened into the outbound, not nested")
	}
}

func TestLoadMissingFileIsNotAnError(t *testing.T) {
	// 首次运行没有注册表文件,不是错误(JS openStore 的默认空池)。
	r := newReg(t)
	if err := r.Load(); err != nil {
		t.Fatalf("missing file: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("len = %d, want 0", r.Len())
	}
}

func TestLoadCorruptFileIsAnError(t *testing.T) {
	// 损坏文件必须是错误:静默回退空池的话,下一次 Flush 就会把用户攒下的
	// 整个池子抹掉。
	file := filepath.Join(t.TempDir(), "node-registry.json")
	if err := os.WriteFile(file, []byte("{not json"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	r := NewRegistry(file)
	if err := r.Load(); err == nil {
		t.Fatal("a corrupt file must be an error")
	}
}

func TestLoadAppliesTheJSGuards(t *testing.T) {
	// 载入守卫照抄 src/registry.js:90-115:
	//   - 条目键必须与 outbound.tag 一致,不一致丢弃(:97);
	//   - fails 只认正整数,否则归 0(:102);
	//   - 过期墓碑不载入,等于放它复活(:112);
	//   - outbound 顶层未建模字段(此处的 username)收进 Extra,凭据不丢。
	file := filepath.Join(t.TempDir(), "node-registry.json")
	fresh := time.Now().UnixMilli()
	expired := time.Now().Add(-TombstoneTTL - time.Hour).UnixMilli()
	doc := map[string]any{
		"entries": map[string]any{
			"good": map[string]any{
				"outbound": map[string]any{"tag": "good", "type": "vless", "server": "h", "server_port": 443, "username": "u1"},
				"addedAt":  fresh, "lastSeenAt": fresh, "fails": 2, "lastFailAt": fresh,
			},
			"mismatch": map[string]any{
				"outbound": map[string]any{"tag": "other", "type": "vless", "server": "h", "server_port": 443},
				"addedAt":  fresh, "lastSeenAt": fresh, "fails": 9, "lastFailAt": 0,
			},
			"zerofails": map[string]any{
				"outbound": map[string]any{"tag": "zerofails", "type": "vless", "server": "h", "server_port": 443},
				"addedAt":  fresh, "lastSeenAt": fresh, "fails": 0, "lastFailAt": 0,
			},
		},
		"tombstones": map[string]any{
			"stale|identity": map[string]any{"tag": "t1", "fails": 3, "lastFailAt": expired},
			"live|identity":  map[string]any{"tag": "t2", "fails": 1, "lastFailAt": fresh},
		},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(file, b, 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	r := NewRegistry(file)
	if err := r.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	if !r.Has("good") || r.Has("mismatch") {
		t.Fatalf("tag guard failed: good=%v mismatch=%v", r.Has("good"), r.Has("mismatch"))
	}
	if !r.Has("zerofails") {
		t.Fatal("zerofails entry was dropped")
	}
	if got := r.FailCount("good"); got != 2 {
		t.Fatalf("fails = %d, want 2", got)
	}
	if got := r.FailCount("zerofails"); got != 0 {
		t.Fatalf("zero fails must read 0, got %d", got)
	}
	if len(r.tombstones) != 1 {
		t.Fatalf("tombstones = %d, want 1 (the expired one must not load)", len(r.tombstones))
	}
	if _, ok := r.tombstones["live|identity"]; !ok {
		t.Fatal("fresh tombstone was lost")
	}
	for _, o := range r.All() {
		if o.Tag == "good" && o.Extra["username"] != "u1" {
			t.Fatalf("username was not absorbed into Extra: %v", o.Extra)
		}
	}
}
