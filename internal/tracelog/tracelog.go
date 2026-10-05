// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package tracelog records one structured line per routing decision: the
// candidate ordering that produced the pick, every attempt that followed, and
// the outcome.
//
// Why a separate store from the text log: a routing decision is only
// explainable if the candidate list and the per-attempt results are queryable
// together. Flattening them into prose lines loses the ordering that produced
// the pick, which is the exact question asked when an exit misbehaves.
//
// Every failure mode here is silent by design. A logging bug must never be the
// reason a request fails, so Record swallows its errors and the package
// degrades to the in-memory ring.
//
// 日期口径与 src/tracelog.js 的 dayKey 一致：**UTC** 的 YYYY-MM-DD（
// toISOString().slice(0,10)），不是本地时区——本地时区会让东八区 08:00 前
// 的记录落到前一天的文件里，跨天排障时文件对不上。
//
// 掉电裁决(六审 F11,有意为之):每行 open-append-close 不含 FlushFileBuffers,
// 进程崩溃/被杀一行不丢,掉电或 BSOD 丢最后 ~1s 窗口的行。路由轨迹是排障
// 素材,同 logger 的取舍 —— 不为每行一次 fsync 的代价买单;账本类文件走
// persistence.WriteJSONFile(数据 + 目录 fsync)。
package tracelog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	historyDays    = 30
	maxBytesPerDay = 32 << 20
	// maxTotalBytes 是 route 目录的总量上限:32MB/天 × 30 天 ≈ 960MB 无人管。
	// prune 按总量从老删,总量超 200MB 时删到 150MB 水位线。
	maxTotalBytes   = 200 << 20
	maxTotalLowMark = 150 << 20
	ringMax         = 500
	orderRowsInFile = 8
	// maxConsecFails 是连续失败阈值:过去 OpenFile/Write 任一失败即整日停写,
	// 一次瞬时抖动(AV 锁、磁盘抖)丢一整天路由证据。连续 N 次才停写。
	maxConsecFails = 5
)

// OrderRow is one candidate in the pick, with the reason it ranked where it
// did. Bucket is signed: a gated class-B node carries a negative bucket.
type OrderRow struct {
	Tag       string `json:"tag"`
	Country   string `json:"country"`
	IP        string `json:"ip"`
	Bucket    int    `json:"bucket"`
	Cost      int    `json:"cost"`
	Load      int    `json:"load"`
	Latency   int64  `json:"latency"`
	Throttled bool   `json:"throttled"`
	Geo       bool   `json:"geo"`
	Sticky    bool   `json:"sticky"`
}

// TryRow is one attempt at reaching the upstream through a node.
type TryRow struct {
	Tag     string `json:"tag"`
	Country string `json:"country"`
	IP      string `json:"ip"`
	Code    string `json:"code"`
	MS      int64  `json:"ms"`
	Served  bool   `json:"served"`
}

// Route is one complete routing decision.
type Route struct {
	Kind       string     `json:"kind"`
	Model      string     `json:"model"`
	Text       string     `json:"text"`
	Result     string     `json:"result"`
	WithinTurn bool       `json:"withinTurn"`
	Order      []OrderRow `json:"order"`
	Tries      []TryRow   `json:"tries"`
	MS         int64      `json:"ms"`
	At         int64      `json:"at"`
}

var (
	mu           sync.RWMutex
	dir          string
	ring         []Route
	writeOff     bool
	consecFails  int
	lastPruneDay string
	dayBytes     int64
	// writeMu 把磁盘 I/O 挪出 mu:Record 曾全程持 mu 做 open/write/close,
	// Recent(面板轮询)与所有 Record 在同一把锁上排队 —— 轮询高峰时路由
	// 记录要等 I/O。mu 只保护内存状态与配额账,写按到达序在 writeMu 下
	// 落盘;锁序恒为 writeMu → mu,无环。
	writeMu sync.Mutex
)

// Init points the tracer at a directory, creating it when possible. A directory
// that cannot be created leaves the ring as the only store.
func Init(d string) {
	mu.Lock()
	defer mu.Unlock()
	// Per-directory accounting: reusing yesterday's numbers for a new
	// directory would skip today's prune and let yesterday's files pile up.
	lastPruneDay = ""
	dayBytes = 0
	writeOff = false
	ring = nil
	if d == "" {
		dir = ""
		return
	}
	if err := os.MkdirAll(d, 0o755); err != nil {
		dir = ""
		return
	}
	dir = d
	// dayBytes 只计本进程写入 —— 重启后同一天的文件还能再写满 32MB,
	// 跨重启合计超配额。按当天文件的实际大小初始化计数。
	today := time.Now().UTC().Format("2006-01-02")
	if fi, err := os.Stat(filepath.Join(d, today+".jsonl")); err == nil {
		dayBytes = fi.Size()
	}
}

// Record appends one decision. It has no error return on purpose.
func Record(r Route) {
	mu.Lock()
	// JS：at = record.at ?? Date.now()。At 缺省（0）时取当前时刻。
	if r.At == 0 {
		r.At = time.Now().UnixMilli()
	}
	if len(r.Order) > orderRowsInFile {
		// 必须在进 ring 之前截：struct 拷贝共享 slice 底层数组，
		// 先 append 再截只改了本地头，ring 里仍是全长。
		r.Order = r.Order[:orderRowsInFile]
	}
	ring = append(ring, r)
	if len(ring) > ringMax {
		ring = ring[len(ring)-ringMax:]
	}
	// B6：跨天的复位必须发生在 writeOff 的早退**之前**。writeOff 从前只有
	// Init 会复位，于是一天触顶（或日文件被杀软/编辑器占用一次）之后，
	// data/route/<day>.jsonl 在整个进程余下的生命周期里都不再增长：面板照常、
	// 历史为空、任何地方都不报错。而 JS 的配额账是 dayBytes: Map<day, bytes>，
	// 跨天天然不受影响——「一天触顶 = 永久停写」是 Go 独有回归。
	//
	// 现在 writeOff 与 dayBytes 一样是**按天**的状态：新的一天重新给一次机会。
	// 最坏情况是每天失败一次后再次停写，既不会静默永久停摆，也不会退化成
	// 每条记录一次的错误风暴。
	day := time.UnixMilli(r.At).UTC().Format("2006-01-02")
	// 只在**日期前进**时轮转并重置每日账(第七轮复审 B):旧判据是
	// `day != lastPruneDay`,于是一条 At 早于当前轮转日的记录(时钟回拨、
	// 上游回放、调用方显式给历史时刻)会把 lastPruneDay 拉回旧日期,并把
	// dayBytes/writeOff/consecFails 一起清零 —— 反复在两天之间交替的 At 能
	// 把 32MB/天的配额账无限重置(写的是不同日名的文件,所以连不上限都不撞)。
	// 生产不可达(唯一调用点的 At ≡ nowMS()),但账本不该有一条「历史时刻
	// 即清零今日配额」的路。历史日的记录照旧写它自己那天的文件,只是不再
	// 参与轮转判定,字节仍记在当前轮转日账上(保守口径,见下方 F7)。
	if dir != "" && (lastPruneDay == "" || day > lastPruneDay) {
		pruneLocked(day)
		lastPruneDay = day
		// 每日字节配额与保险丝按天重置。不重置配额的话,昨天攒下的计数会把
		// 今天提前顶到 32MB 上限,整个新的一天都静默停写。
		//
		// consecFails 必须一起清:跨天块过去只清 writeOff/dayBytes 漏清它,
		// 于是某个坏天熔满 5 次后计数永远挂着,此后**每天** 1 次瞬时抖动
		// (AV 扫一下盘)就立刻重新熔掉一整天 —— 「一次抖动不丢一天」只对
		// 首个坏天成立的回归(第六轮审计 F6)。
		dayBytes = 0
		writeOff = false
		consecFails = 0
	}
	d := dir
	off := writeOff
	mu.Unlock()
	if d == "" || off {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	b = append(b, '\n')

	// I21:配额预检在 mu 的短临界区里完成,写与最终扣账在 writeMu 下按到达序
	// 进行(见 writeMu 注释;扣账落盘后补,见下方 F7 注释)。
	writeMu.Lock()
	defer writeMu.Unlock()
	mu.Lock()
	// 跨天护栏:两次进 mu 之间若别的 Record 换了目录,本条会写到与 d 不同的
	// 目录去 —— 直接丢弃(本包 fail-silent by design)。
	//
	// 注意**不再**因为 lastPruneDay != day 丢这一条:那个条件让任何 At 早于
	// 当前轮转日的记录(时钟回拨、上游回放、调用方显式给历史时刻)被**永久**
	// 静默丢弃 —— 旧日文件明明还在,这一行却再也进不去。字节账记在当前
	// 轮转日(lastPruneDay)的额度上:回拨行的字节不能把新一天的 32MB 额度
	// 撑爆,而旧一天的额度无法核实(那天早已 prune),所以记当前天是唯一
	// 保守的选择。
	if writeOff || dir != d {
		mu.Unlock()
		return
	}
	if dayBytes+int64(len(b)) > maxBytesPerDay {
		// Stop writing for the rest of the day rather than truncating: the
		// existing lines are the evidence for whatever went wrong.
		writeOff = true
		mu.Unlock()
		return
	}
	// 扣账与保险丝复位都以**真落盘**为准(第六轮审计 F7):旧写法在 OpenFile
	// 成功处就清零 consecFails 并扣账,而「开得出、写不进」的形状(盘满:
	// O_APPEND 开既有文件不需要空间,f.Write 才报 ENOSPC)每行都把计数清零
	// 再 +1 —— 保险丝永不熔断,幻影配额还照涨。writeMu 串行化了整段
	// open→write→记账,所以补扣(按实际落盘字节 n)与重验之间没有其他写者。
	f, err := os.OpenFile(filepath.Join(d, day+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		noteFailLocked()
		mu.Unlock()
		return
	}
	mu.Unlock()
	n, werr := f.Write(b)
	_ = f.Close()
	mu.Lock()
	dayBytes += int64(n) // 按实际落盘的字节入账:账不高于盘
	if werr != nil {
		// 写失败(盘满最现实:O_APPEND 开既有文件不需要空间,f.Write 才
		// ENOSPC)不再像旧实现那样被上一行的 open 成功清零 —— 熔断只在
		// **连续写不进**时熔,「开得出、写不进」的形状烧满 5 行即整日停写。
		noteFailLocked()
	} else {
		consecFails = 0
	}
	mu.Unlock()
}

// noteFailLocked 记一次写失败:连续 maxConsecFails 次才整日停写。瞬时抖动
// (AV 锁、磁盘抖)只丢几行,不丢一整天。调用方已持 mu 写锁。
func noteFailLocked() {
	consecFails++
	if consecFails >= maxConsecFails {
		writeOff = true
	}
}

// pruneLocked deletes jsonl files older than the retention window. Only
// *.jsonl is considered, so a README left in the directory is never a casualty.
func pruneLocked(today string) {
	cutoff, err := time.Parse("2006-01-02", today)
	if err != nil {
		return
	}
	cutoff = cutoff.AddDate(0, 0, -(historyDays - 1))
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".jsonl" {
			continue
		}
		d, err := time.Parse("2006-01-02", name[:min(10, len(name))])
		if err != nil {
			continue
		}
		if d.Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	// 总量上限:32MB/天 × 30 天 ≈ 960MB 无人管。超 200MB 时按 mtime 从老删
	// 到 150MB 水位线 —— 天数保留策略管「老」,总量管「大」。
	pruneTotalLocked()
}

// pruneTotalLocked 按总量删老文件。调用方已持 mu 写锁(pruneLocked 内)。
func pruneTotalLocked() {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type finfo struct {
		name  string
		mtime int64
		size  int64
	}
	var total int64
	var files []finfo
	for _, e := range ents {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		total += fi.Size()
		files = append(files, finfo{name: e.Name(), mtime: fi.ModTime().Unix(), size: fi.Size()})
	}
	if total <= maxTotalBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mtime < files[j].mtime })
	for _, f := range files {
		if total <= maxTotalLowMark {
			break
		}
		if err := os.Remove(filepath.Join(dir, f.name)); err == nil {
			total -= f.size
		}
	}
}

// Recent returns up to limit decisions in chronological order (oldest of the
// window first), matching the ring semantics the panel expects.
func Recent(limit int) []Route {
	mu.RLock()
	defer mu.RUnlock()
	if limit <= 0 || len(ring) == 0 {
		return nil
	}
	n := limit
	if n > len(ring) {
		n = len(ring)
	}
	out := make([]Route, n)
	copy(out, ring[len(ring)-n:])
	return out
}
