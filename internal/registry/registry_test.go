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

func TestEnforceCapEvictsTheOldest(t *testing.T) {
	r := newReg(t)
	for i := 0; i < 5; i++ {
		r.Merge([]parse.Outbound{ob(string(rune('a' + i)))})
	}
	if n := len(r.EnforceCap(3)); n != 2 {
		t.Fatalf("evicted = %d, want 2", n)
	}
	// 返回值必须是**名单**:调用方(app/rebuild/probe)靠它给被淘汰的 tag
	// 清健康行(D-C1)。只验证个数的话,把返回值改回 int 也能过这条测试。
	r2 := newReg(t)
	for i := 0; i < 5; i++ {
		r2.Merge([]parse.Outbound{ob(string(rune('a' + i)))})
	}
	got := r2.EnforceCap(3)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("evicted tags = %v, want [a b](最旧的两个)", got)
	}
	if r.Len() != 3 {
		t.Fatalf("len = %d, want 3", r.Len())
	}
}

func TestFlushRoundTrips(t *testing.T) {
	file := filepath.Join(t.TempDir(), "node-registry.json")
	r := NewRegistry(file)
	r.Merge([]parse.Outbound{ob("a")})
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
	//   - outbound 顶层未建模字段(此处的 username)收进 Extra,凭据不丢。
	//   - 1.3.0:fails/墓碑随机制退役,盘上旧键按未知字段忽略。
	file := filepath.Join(t.TempDir(), "node-registry.json")
	fresh := time.Now().UnixMilli()
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
	for _, o := range r.All() {
		if o.Tag == "good" && o.Extra["username"] != "u1" {
			t.Fatalf("username was not absorbed into Extra: %v", o.Extra)
		}
	}
}

// TestGenerationTracksMembershipChanges 钉住 O10 缓存的正确性前提:代数在**成员
// 关系**变化的每一条路径上涨,在不改变成员的路径上不涨。
func TestGenerationTracksMembershipChanges(t *testing.T) {
	r := newReg(t)
	base := r.Generation()
	r.Merge([]parse.Outbound{ob("a")})
	if r.Generation() != base+1 {
		t.Fatalf("merge 新增必须涨代数: %d -> %d", base, r.Generation())
	}
	if n := r.Merge([]parse.Outbound{ob("a")}); n != 0 {
		t.Fatalf("duplicate added %d", n)
	}
	if r.Generation() != base+1 {
		t.Fatal("重复 merge 不该涨代数")
	}
	rotated := ob("a")
	rotated.UUID = "u2"
	if n := r.Merge([]parse.Outbound{rotated}); n != 0 {
		t.Fatalf("同 tag 换身份 added = %d, want 0(原位更新)", n)
	}
	if r.Generation() != base+2 {
		t.Fatalf("同 tag 换身份必须涨代数: %d", r.Generation())
	}
	if !r.Remove("a") {
		t.Fatal("remove reported a miss")
	}
	if r.Generation() != base+3 {
		t.Fatalf("remove 必须涨代数: %d", r.Generation())
	}
}

// TestMergeAliasDoesNotTakeASecondSlot:换名字但凭据相同 = 同一个物理节点,
// 不新开池位(src/registry.js:145-160)。
func TestMergeAliasDoesNotTakeASecondSlot(t *testing.T) {
	r := newReg(t)
	base := parse.Outbound{Tag: "old-name", Type: "vless", Server: "h", ServerPort: 443, UUID: "u1"}
	r.Merge([]parse.Outbound{base})
	renamed := base
	renamed.Tag = "new-name"
	if n := r.Merge([]parse.Outbound{renamed}); n != 0 {
		t.Fatalf("renamed alias added %d entries, want 0 (duplicate by identity)", n)
	}
	if r.Len() != 1 {
		t.Fatalf("len = %d, want 1 (alias must not take a second slot)", r.Len())
	}
	rotated := renamed
	rotated.UUID = "u2"
	if n := r.Merge([]parse.Outbound{rotated}); n != 1 {
		t.Fatalf("rotated credentials added %d, want 1", n)
	}
}

// TestMergeDedupIsCaseSensitive:约束 9 —— 身份键大小写敏感(现网文件里就有
// 只差大小写的键)。归一化会把两个真实节点错配成一个。
func TestMergeDedupIsCaseSensitive(t *testing.T) {
	r := newReg(t)
	lower := parse.Outbound{Tag: "a", Type: "vless", Server: "h", ServerPort: 443, TLS: &parse.TLS{ServerName: "ov-germany1.09vpn.com"}}
	upper := lower
	upper.TLS = &parse.TLS{ServerName: "OV-Germany1.09vpn.com"}
	upper.Tag = "b"
	if parse.IdentityOf(lower) == parse.IdentityOf(upper) {
		t.Fatalf("test is wrong: identities collided at %q", parse.IdentityOf(lower))
	}
	if n := r.Merge([]parse.Outbound{lower, upper}); n != 2 {
		t.Fatalf("大小写不同的 sni 是两个身份, added = %d, want 2", n)
	}
}

// TestRemoveDropsTheEntryCleanly:删除路径(冷区 3 击/订阅消失)是**零记录**:
// 条目删掉,重拉到就是全新节点。
func TestRemoveDropsTheEntryCleanly(t *testing.T) {
	r := newReg(t)
	r.Merge([]parse.Outbound{ob("a")})
	if !r.Remove("a") {
		t.Fatal("remove reported a miss")
	}
	if r.Remove("a") {
		t.Fatal("second remove reported a hit")
	}
	if n := r.Merge([]parse.Outbound{ob("a")}); n != 1 {
		t.Fatalf("re-merge after remove added %d, want 1(全新节点)", n)
	}
}
