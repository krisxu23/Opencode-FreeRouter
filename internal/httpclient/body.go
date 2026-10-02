// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package httpclient

import (
	"fmt"
	"io"
)

// ReadCapped reads at most limit bytes and fails loudly if the source has more.
//
// The unbounded io.ReadAll is a real OOM path here: every caller of this helper
// reads the body of a host we do not control (a subscription provider, a probe
// target). Truncating at the cap would be worse than failing — a half-read
// model list parses as "the response is not JSON" and sends the next reader
// after the wrong fault — so over-limit is an error.
//
// The read is limit+1 so the check is exact: a body of exactly limit bytes is
// fine, one byte more is not.
func ReadCapped(r io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("httpclient: response body exceeds %d bytes", limit)
	}
	return body, nil
}
