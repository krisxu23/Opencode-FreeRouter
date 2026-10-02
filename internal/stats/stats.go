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

// sampleMax is the count cap. It is a var rather than a const only so a test
// can lower it: the time window (src/index.js:196-199) runs *only* when the
// count cap is exceeded, so with the real 2000 the window branch is
// unreachable from a unit test.
var sampleMax = defaultSampleMax

// Record is one finished turn, in the shape the engine can hand over without
// knowing anything about this package.
type Record struct {
	At     int64
	Model  string
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
	Samples  []Sample          `json:"samples"`
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
	mu      sync.Mutex
	file    string
	snap    Snapshot
	lastErr error
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
	if snap.Samples == nil {
		snap.Samples = []Sample{}
	}
	return snap
}

// Record folds one finished turn into the board and lands it on disk.
//
// It never panics and never returns an error: src/index.js:172 and :202 both
// wrap the whole accounting in try/catch for one reason — a failed write must
// not turn an answer that already succeeded into a 500. Failures are kept in
// LastError instead, where /api/status can surface them.
//
// The in-memory update happens under the lock; the atomic write happens
// outside it. An atomic write on Windows costs tens of milliseconds, and
// holding the lock across it would make every request's accounting a global
// serialization point.
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

	s.snap.Requests++

	var ttft *int64
	if r.TTFTMS != 0 {
		v := r.TTFTMS
		ttft = &v
	}
	s.snap.Samples = append(s.snap.Samples, Sample{
		T:      r.At,
		Model:  r.Model,
		OK:     r.OK,
		TTFTMS: ttft,
		Out:    r.Output,
	})

	// Retention is capped twice, and the order matters: the time window runs
	// first, then the count. Keeping only the newest N turns would let one
	// fast request from three days ago hold a slot forever and drag the TTFT
	// distribution up; keeping only a time window would let a burst grow the
	// file without bound. Note the cutoff uses the wall clock rather than the
	// record's own At, exactly like the JS version.
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

	snap := clone(s.snap)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	return clone(s.snap)
}

// clone deep-copies a snapshot, including both maps and the sample slice.
func clone(snap Snapshot) Snapshot {
	out := Snapshot{
		Requests: snap.Requests,
		Days:     make(map[string]Bucket, len(snap.Days)),
		Models:   make(map[string]Bucket, len(snap.Models)),
		Samples:  make([]Sample, len(snap.Samples)),
	}
	for k, v := range snap.Days {
		out.Days[k] = v
	}
	for k, v := range snap.Models {
		out.Models[k] = v
	}
	copy(out.Samples, snap.Samples)
	return out
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

	s.mu.Lock()
	defer s.mu.Unlock()
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
// sequence and the tests use when the caller does want to know.
func (s *Stats) Flush() error {
	s.mu.Lock()
	snap := clone(s.snap)
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
