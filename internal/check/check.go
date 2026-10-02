// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package check holds the harness-neutral vocabulary: failure codes, region
// group names, and the numeric constants whose values were measured rather than
// guessed.
//
// It is a leaf package on purpose. Failure codes are needed by the transport
// layer (L1) and the rotation engine (L4); region groups by the pool (L2) and
// the panel (L5). Without it those two would import each other just to share a
// string, which the layer rule forbids.
package check

import "regexp"

// Failure taxonomy shared by the transport layer and the rotation engine.
// The literal values are contract, not cosmetics: the engine switches on them
// and the panel displays them.
const (
	CodeRegion     = "REGION_BLOCKED"
	CodeQuota      = "RATE_LIMIT"
	CodeCredential = "INVALID_CREDENTIAL"
	CodeTransport  = "TRANSPORT"
	CodeTimeout    = "TIMEOUT"
	CodeServer     = "SERVER"
	CodeEmpty      = "EMPTY_RESPONSE"
	CodeAborted    = "ABORTED"
)

// RegionGroups is the panel's country multi-select; the slice order IS the
// fallback order, so it must never be sorted or deduplicated.
var RegionGroups = []string{"US", "JP", "HK", "TW", "KR", "SG", "EU", "OTHER"}

// SubRetryExits is how many live exits a source that failed the direct fetch is
// retried through. Raised from 3 to 12 on 2026-09-28 after measuring that the
// first two exits answered ECONNRESET and only the third worked.
const SubRetryExits = 12

// EphemeralPortFloor is where Windows starts handing ports to outbound
// connections. The Go build has no per-node local ports, but the panel still
// renders the old port-range setting and its help text quotes this number.
const EphemeralPortFloor = 49152

// ProbeSummaryPattern is the shape of the one log line per probe round that the
// console scrapes with probeFromLogs (web/app.js:192). It lives here, next to
// the other contracts, so the Go side that emits the line and the Go side that
// checks it share one literal instead of drifting apart.
//
// The separators are deliberately loose: the line carries Chinese labels
// between the numbers and the console must keep parsing it if those labels are
// reworded.
const ProbeSummaryPattern = `probe round:\s*(\d+)\/(\d+)\s+alive\s*\(A\s*(\d+)[^0-9]*B\s*(\d+)[^)]*\)\s*in\s*([\d.]+)s`

// ProbeSummaryRe is ProbeSummaryPattern compiled once. Groups: 1 alive,
// 2 scanned, 3 tier A, 4 tier B, 5 seconds.
var ProbeSummaryRe = regexp.MustCompile(ProbeSummaryPattern)
