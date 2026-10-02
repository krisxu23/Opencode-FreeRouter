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
package tracelog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	historyDays     = 30
	maxBytesPerDay  = 32 << 20
	ringMax         = 500
	orderRowsInFile = 8
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
	lastPruneDay string
	dayBytes     int64
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
}

// Record appends one decision. It has no error return on purpose.
func Record(r Route) {
	mu.Lock()
	defer mu.Unlock()
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
	if dir != "" && day != lastPruneDay {
		pruneLocked(day)
		lastPruneDay = day
		// 每日字节配额按天重置。不重置的话，昨天攒下的计数会把今天
		// 提前顶到 32MB 上限，整个新的一天都静默停写。
		dayBytes = 0
		writeOff = false
	}
	if dir == "" || writeOff {
		return
	}
	b, err := json.Marshal(r)
	if err != nil {
		return
	}
	b = append(b, '\n')
	path := filepath.Join(dir, day+".jsonl")
	if dayBytes+int64(len(b)) > maxBytesPerDay {
		// Stop writing for the rest of the day rather than truncating: the
		// existing lines are the evidence for whatever went wrong.
		writeOff = true
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		writeOff = true
		return
	}
	n, err := f.Write(b)
	_ = f.Close()
	if err != nil {
		writeOff = true
		return
	}
	dayBytes += int64(n)
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
