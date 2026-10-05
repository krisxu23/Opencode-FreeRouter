// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package stats is the local usage board: per-day and per-model token totals
// plus a dual-capped sample of recent turns used for TTFT percentiles. Every
// number stays on this machine — nothing here touches the network.
//
// It lives at layer 1 and imports only persistence. The JS version keeps this
// state in a module-level singleton inside src/index.js, which is exactly why
// the phase-5 differential acceptance has to treat A11 as a process-level
// comparison; keeping the logic in its own package at least lets the Go side
// be exercised on its own.
package stats

import (
	"errors"
	"io/fs"
	"sort"
	"sync"
	"time"

	"freerouter/internal/persistence"
)

const (

	// sampleWindowMS is how old a sample may be and still survive a trim.
	sampleWindowMS = 24 * 3600 * 1000
	// defaultSampleMax caps how many samples are retained.
	defaultSampleMax = 2000
	// msPerDay is used by History to walk backwards. It is deliberately plain
	// millisecond arithmetic and not a calendar walk: the bucket keys are UTC
	// strings, so stepping by calendar days would drift out of step with the
	// keys recordUsage writes (src/index.js:1032 does the same).
	msPerDay = 86400000
)

// exitsMax 是按出口聚合桶的数量上限(stats.Record 的 P5 淘汰)。var 而不是
// const 只为让测试能调小,理由与 sampleMax 相同。
var exitsMax = 512

// sampleMax is the count cap. It is a var rather than a const only so a test
// can lower it: the time window (src/index.js:196-199) runs *only* when the
// count cap is exceeded, so with the real 2000 the window branch is
// unreachable from a unit test.
var sampleMax = defaultSampleMax

// flushDelay is how long Record waits before landing a write, coalescing every
// turn inside the window into one pass (O1). 300ms is not invented here: the
// JS side gives every JsonStore the same debounce (src/store.js:63-72), so a
// burst of requests produces one file write there too. It is a var only so a
// test can stretch or shrink the window.
var flushDelay = 300 * time.Millisecond

// Record is one finished turn, in the shape the engine can hand over without
// knowing anything about this package.
type Record struct {
	At    int64
	Model string
	// Exit 是这次调用实际走的出口 tag。按出口分列是 README 承诺过的面板能力
	// (「记用量(模型 × 出口的调用与失败)」)—— 过去 stats 只有 model 维度,
	// 「哪个出口在失败」在面板上无处可看。空串(测试/未知)不建行。
	Exit   string
	OK     bool
	Input  int64
	Output int64
	TTFTMS int64
}

// Bucket is one row of the per-day or per-model totals. The JSON names are
// load-bearing: the JS board and this file must be interchangeable on disk.
type Bucket struct {
	Req int64 `json:"req"`
	In  int64 `json:"in"`
	Out int64 `json:"out"`
}

// Sample is one retained turn. TTFTMS is a pointer because the JS writer
// stores `null` when a turn never produced a first token (src/index.js:192),
// and 264 of the 1908 rows in the live data/stats.json are exactly that. A
// plain int64 would serialize as 0, which would read as "instant" and quietly
// poison every TTFT percentile.
type Sample struct {
	T      int64  `json:"t"`
	Model  string `json:"model"`
	Exit   string `json:"exit,omitempty"`
	OK     bool   `json:"ok"`
	TTFTMS *int64 `json:"ttftMs"`
	Out    int64  `json:"out"`
}

// Snapshot is the whole board. Field order is the on-disk key order the JS
// version writes, so a diff of the two files stays readable.
type Snapshot struct {
	Requests int64             `json:"requests"`
	Days     map[string]Bucket `json:"days"`
	Models   map[string]Bucket `json:"models"`
	// Exits 是按出口聚合的同一批调用(面板用量页的出口表)。旧文件没有这个
	// 键:normalize 会补空表,JSON 里 omitempty 让空表在落盘时省略 —— 与旧版
	// 数据文件保持字节兼容。
	Exits   map[string]Bucket `json:"exits,omitempty"`
	Samples []Sample          `json:"samples"`
}

// HistoryRow is one day of History: a Bucket plus the UTC date it belongs to.
// The date is not part of Bucket because a Bucket also appears as a map value,
// where the key already carries it.
type HistoryRow struct {
	Date string `json:"date"`
	Req  int64  `json:"req"`
	In   int64  `json:"in"`
	Out  int64  `json:"out"`
}

// Stats is the board plus the file it lands in. A zero file means
// memory-only, which is what the tests use to keep 2500 writes cheap.
type Stats struct {
	mu      sync.RWMutex
	file    string
	snap    Snapshot
	lastErr error
	// lastPruneMS 是上次在 Record 侧做 Days 定期修剪的时刻(UnixMilli)。
	// pruneDays 只管落盘副本的话内存 Days 会随运行天数无限涨(约 365 键/年);
	// 但每条 Record 都全量扫一遍 Days 又是浪费,所以按天节流:最多每天剪一次。
	lastPruneMS int64
	// timer 是去抖写(O1)的挂起定时器。nil 表示当前没有待写的改动。
	// dirty 表示内存比磁盘新,到期或 Flush 时才需要真正写一次。
	timer *time.Timer
	dirty bool
	// flushMu 把写盘串行化,并且**快照的克隆也必须在它里面**。去抖只是缩小了
	// 并发窗口,没有封死它:flushPending 走到写盘时,Record 可能正在置
	// dirty=true 并武装新 timer,而一次显式 Flush 也可能同时进来。若只给
	// WriteJSONFile 串行化、快照在锁外克隆,两路的 rename 顺序仍可能让**旧**
	// 快照后写、覆盖新快照(丢数据)。把「克隆 + 置 dirty=false + 写盘」整体
	// 放进 flushMu,盘上任何时刻只有一个写者,且它写的一定是最新克隆。
	flushMu sync.Mutex
}

// New returns an empty board writing to file.
//
// It does not read the file: Load is a separate call so the caller can decide
// what a missing or damaged file means. (The JS JsonStore loads in its
// constructor; there the read is swallowed either way, so the split loses
// nothing and keeps "first run" distinguishable from "corrupt".)
func New(file string) *Stats {
	return &Stats{file: file, snap: emptySnapshot()}
}

// emptySnapshot returns a Snapshot whose containers are empty but non-nil.
// They must not be nil: nil maps and a nil slice serialize as null, and the
// console renders the first frame straight from this object — a null there
// blanks the page, while `{}` and `[]` render as an empty board.
func emptySnapshot() Snapshot {
	return Snapshot{
		Days:    map[string]Bucket{},
		Models:  map[string]Bucket{},
		Exits:   map[string]Bucket{},
		Samples: []Sample{},
	}
}

// Load replaces the in-memory board with the file's contents.
//
// A missing file is not an error — that is simply the first run, and the JS
// side treats it the same way. Any other failure (unreadable, malformed) is
// returned with the previous state left untouched, so a damaged file cannot
// silently zero the board; the caller logs it and carries on.
func (s *Stats) Load() error {
	if s.file == "" {
		// Memory-only board: there is no file to read, and ReadJSONFile("")
		// would report a confusing path error rather than "nothing to do".
		// persist and Flush already short-circuit on the same condition.
		return nil
	}
	var loaded Snapshot
	err := persistence.ReadJSONFile(s.file, &loaded)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if errors.Is(err, fs.ErrNotExist) {
		s.snap = emptySnapshot()
		return nil
	}
	s.snap = normalize(loaded)
	return nil
}

// normalize repairs a decoded snapshot so the invariants Record relies on
// hold. Unmarshalling `{"requests":7}` leaves all three containers nil, and
// the next Record would then panic on assignment to a nil map — a partial
// file must degrade to "no rows yet", not to a dead gateway.
func normalize(snap Snapshot) Snapshot {
	if snap.Days == nil {
		snap.Days = map[string]Bucket{}
	}
	if snap.Models == nil {
		snap.Models = map[string]Bucket{}
	}
	if snap.Exits == nil {
		snap.Exits = map[string]Bucket{}
	}
	if snap.Samples == nil {
		snap.Samples = []Sample{}
	}
	return snap
}

// Record folds one finished turn into the board. The write is debounced.
//
// It never panics and never returns an error: src/index.js:172 and :202 both
// wrap the whole accounting in try/catch for one reason — a failed write must
// not turn an answer that already succeeded into a 500. Failures are kept in
// LastError instead, where /api/status can surface them.
//
// The in-memory update happens under the lock and the write is scheduled, not
// performed (O1). The JS version debounces every store write by 300ms
// (src/store.js:63-72) and only calls statsStore.edit() per turn
// (src/index.js:179-202); the Go port had dropped the debounce, so every
// single request cloned the whole board and rewrote all 268,949 bytes of
// data/stats.json. Coalescing restores the JS behaviour and removes the
// window in which two request goroutines race on the same temp file (B5).
func (s *Stats) Record(r Record) {
	if r.At == 0 {
		// JS: `record.at ?? Date.now()`.
		r.At = time.Now().UnixMilli()
	}

	s.mu.Lock()
	day := dayKey(r.At)
	d := s.snap.Days[day]
	d.Req++
	d.In += r.Input
	d.Out += r.Output
	s.snap.Days[day] = d

	m := s.snap.Models[r.Model]
	m.Req++
	m.In += r.Input
	m.Out += r.Output
	s.snap.Models[r.Model] = m

	if r.Exit != "" {
		x := s.snap.Exits[r.Exit]
		x.Req++
		x.In += r.Input
		x.Out += r.Output
		s.snap.Exits[r.Exit] = x
		// Exits 是全仓唯一的无界增长面:出口 tag 随订阅 churn 无限换代,而
		// Days(90 天)/Samples(24h+2000) 都有上限。按 req 保 top exitsMax ——
		// 活跃出口请求多不会被挤掉,历史出口随新出口进入自然沉底。淘汰在
		// Record 的锁内做:只 prune 落盘副本的话,内存 map 仍会无限涨。
		if len(s.snap.Exits) > exitsMax {
			type exitReq struct {
				k   string
				req int64
			}
			all := make([]exitReq, 0, len(s.snap.Exits))
			for k, b := range s.snap.Exits {
				all = append(all, exitReq{k, b.Req})
			}
			sort.Slice(all, func(i, j int) bool {
				if all[i].req != all[j].req {
					return all[i].req > all[j].req
				}
				return all[i].k < all[j].k
			})
			fresh := make(map[string]Bucket, exitsMax)
			for _, e := range all[:exitsMax] {
				fresh[e.k] = s.snap.Exits[e.k]
			}
			s.snap.Exits = fresh
		}
	}

	s.snap.Requests++

	var ttft *int64
	if r.TTFTMS >= 0 {
		// 0 也算一次真实量测。旧的 `!= 0` 把「本轮 TTFT 恰为 0 毫秒」当成
		// 「没量到」，样本的 ttftMs 键直接不写 —— 本机回环上的快速模型真的
		// 会量出 0ms，那一行在面板上就成了「无数据」。负数才是「没量到」。
		v := r.TTFTMS
		ttft = &v
	}
	s.snap.Samples = append(s.snap.Samples, Sample{
		T:      r.At,
		Model:  r.Model,
		Exit:   r.Exit,
		OK:     r.OK,
		TTFTMS: ttft,
		Out:    r.Output,
	})

	// Retention is capped twice, and the order matters: the time window runs
	// first, then the count. Keeping only the newest N turns would let one
	// fast request from three days ago hold a slot forever and drag the TTFT
	// distribution up; keeping only a time window would let a burst grow the
	// file without bound. Note the cutoff uses the wall clock rather than the
	// record's own At, exactly like the JS version. Trimming stays inside the
	// lock so Snapshot never observes an over-cap board.
	if len(s.snap.Samples) > sampleMax {
		cutoff := time.Now().UnixMilli() - sampleWindowMS
		kept := make([]Sample, 0, len(s.snap.Samples))
		for _, sm := range s.snap.Samples {
			if sm.T >= cutoff {
				kept = append(kept, sm)
			}
		}
		if len(kept) > sampleMax {
			kept = kept[len(kept)-sampleMax:]
		}
		s.snap.Samples = kept
	}

	s.dirty = true
	// 内存 Days 定期修剪(每天最多一次):只修落盘副本的话内存 map 无限涨。
	if now := time.Now().UnixMilli(); now-s.lastPruneMS > msPerDay {
		s.lastPruneMS = now
		pruneDays(s.snap, now)
	}
	if s.file != "" && s.timer == nil {
		s.timer = time.AfterFunc(flushDelay, s.flushPending)
	}
	s.mu.Unlock()
}

// flushPending is the debounce timer's callback: it lands whatever Record has
// accumulated since the window opened.
//
// flushMu 在 s.mu **之外**先抢：抢到之后再进 s.mu 复检 dirty（期间可能有另一路
// 写者已经把这批改动落盘），复检不过就什么都不做。反过来先锁 s.mu 再抢
// flushMu 会与 Record 形成 ABBA 的锁序风险（Record 只碰 s.mu，不碰 flushMu，
// 实际不会死锁，但把「持有一把锁去等另一把」写进常规路径不划算）。
func (s *Stats) flushPending() {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	s.timer = nil
	if !s.dirty || s.file == "" {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	snap := clone(s.snap)
	pruneDays(snap, time.Now().UnixMilli())
	s.mu.Unlock()

	s.persist(snap)
}

// persist writes snap and remembers the failure. It is a no-op for a
// memory-only board.
func (s *Stats) persist(snap Snapshot) {
	if s.file == "" {
		return
	}
	if err := persistence.WriteJSONFile(s.file, snap, true); err != nil {
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
	}
}

// Snapshot returns a deep copy of the board.
//
// The copy is the point: the caller (the console, the phase-5 fixture dump)
// may keep or mutate what it gets, and the JS version's `get()` hands out the
// live object, which is precisely how the two versions drift apart.
func (s *Stats) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return clone(s.snap)
}

// clone deep-copies a snapshot, including all maps and the sample slice.
// TTFTMS 是**指针**,copy 只复制指针本身 —— 调用方解引用改写会直接污染看板
// 内存值并写回下一个 flush,这里对非 nil 的样本复制一份值(I19 的「号称深拷贝
// 其实浅拷贝」)。
func clone(snap Snapshot) Snapshot {
	out := Snapshot{
		Requests: snap.Requests,
		Days:     make(map[string]Bucket, len(snap.Days)),
		Models:   make(map[string]Bucket, len(snap.Models)),
		Exits:    make(map[string]Bucket, len(snap.Exits)),
		Samples:  make([]Sample, len(snap.Samples)),
	}
	for k, v := range snap.Days {
		out.Days[k] = v
	}
	for k, v := range snap.Models {
		out.Models[k] = v
	}
	for k, v := range snap.Exits {
		out.Exits[k] = v
	}
	copy(out.Samples, snap.Samples)
	for i := range out.Samples {
		if out.Samples[i].TTFTMS != nil {
			v := *out.Samples[i].TTFTMS
			out.Samples[i].TTFTMS = &v
		}
	}
	return out
}

// daysRetention 是按天桶的保留期:Days 的键每天增一、永不删(约 365 键/年,
// 各 24 字节),长期运行的实例会无限累积。90 天远超面板历史(7 天)与任何
// 实际查询窗口,淘汰只发生在落盘前,不改变内存账目的正确性。
const daysRetention = 90

// pruneDays 删掉保留期之外的按天桶。日期键是 UTC 的 "2006-01-02"(dayKey);
// 解析不了的键(不该存在)原样保留 —— 宁可多留一行也不能误删数据。
func pruneDays(snap Snapshot, now int64) {
	if len(snap.Days) == 0 {
		return
	}
	cutoff := time.UnixMilli(now).UTC().AddDate(0, 0, -daysRetention)
	for k := range snap.Days {
		d, err := time.Parse("2006-01-02", k)
		if err != nil {
			continue // 解析不了的键保留:宁可多留一行,也不误删数据(与注释承诺一致)
		}
		if d.Before(cutoff) {
			delete(snap.Days, k)
		}
	}
}

// History returns the last days days, oldest first, one row per day with
// missing days zero-filled.
//
// The walk is plain millisecond arithmetic (src/index.js:1030-1035) rather
// than a calendar walk: the keys are UTC strings, and stepping by calendar
// days would eventually disagree with the keys recordUsage writes.
func (s *Stats) History(days int, now int64) []HistoryRow {
	if days <= 0 {
		return []HistoryRow{}
	}
	out := make([]HistoryRow, 0, days)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := days - 1; i >= 0; i-- {
		date := dayKey(now - int64(i)*msPerDay)
		b := s.snap.Days[date]
		out = append(out, HistoryRow{Date: date, Req: b.Req, In: b.In, Out: b.Out})
	}
	return out
}

// Flush lands the current board on disk and reports the error.
//
// Record swallows write failures by design; this is the path the shutdown
// sequence and the tests use when the caller does want to know. Any pending
// debounce is cancelled first: otherwise a timer armed before shutdown would
// fire after the process had already decided it was done writing.
func (s *Stats) Flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.dirty = false
	snap := clone(s.snap)
	pruneDays(snap, time.Now().UnixMilli())
	s.mu.Unlock()

	if s.file == "" {
		return nil
	}
	if err := persistence.WriteJSONFile(s.file, snap, true); err != nil {
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
		return err
	}
	return nil
}

// LastError is the most recent write failure, or "" when there has been none.
// It exists for the console's diagnostics: the JS version drops this
// information entirely, so a board that silently stopped updating looked
// identical to one with nothing to report.
func (s *Stats) LastError() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lastErr == nil {
		return ""
	}
	return s.lastErr.Error()
}

// dayKey is the bucket key for an instant: the UTC date, not the local one.
//
// UTC is not a stylistic choice. src/index.js:182 uses
// `new Date(at).toISOString().slice(0, 10)`, so in UTC+8 every request before
// 08:00 lands on the previous day's row; switching to time.Local would move a
// slice of every morning's traffic and misalign the board across month
// boundaries. This is pinned by TestDayKeyIsUTCNotLocal.
func dayKey(at int64) string {
	return time.UnixMilli(at).UTC().Format("2006-01-02")
}
