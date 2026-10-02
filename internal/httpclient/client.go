// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package httpclient builds the two HTTP clients the program needs: requests
// that go straight out, and requests that go through a specific proxy node.
package httpclient

import (
	"context"
	"net"
	"net/http"
	"time"
)

// Dialer opens a connection to addr. It is a type alias on purpose: sbx.Dialer
// and the default dialer both match this shape, so either can be passed
// without a conversion at every call site.
type Dialer = func(ctx context.Context, network, addr string) (net.Conn, error)

// NewClient returns an HTTP client whose connections are opened by d. A nil d
// means the host's own network.
//
// The transport is built by hand rather than by cloning http.DefaultTransport
// for one reason: a node must keep working while the pool is being reloaded,
// and http.DefaultTransport pools connections across the change. A per-node
// client is cheap; a stale pooled connection to a deleted node is not.
func NewClient(d Dialer, timeout time.Duration) *http.Client {
	tr := &http.Transport{
		DialContext:           d,
		MaxIdleConns:          8,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// node's pipeline: pipelining 0 in the JS build became this flag. Keeping
		// it explicit matters because the default is 1 and a pipelined request
		// that fails mid-flight is attributed to the wrong node.
		ForceAttemptHTTP2: false,
	}
	if d == nil {
		tr.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	}
	return &http.Client{Transport: tr, Timeout: timeout}
}
