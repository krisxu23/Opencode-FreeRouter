// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package stream reads Server-Sent Events and extracts usage.
//
// It reads incrementally and calls back per event. That is not a style choice:
// time-to-first-token is the signal the sticky/exit-load feedback runs on, and
// a reader that buffers until [DONE] reports the whole duration as TTFT, which
// makes every exit look equally slow.
package stream

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
	"time"
)

// Event is one SSE event. Data keeps the raw text, including the newlines of a
// multi-line data block.
type Event struct {
	Event string
	Data  string
}

// ReadSSE calls fn for every event in r, in order, and returns the first error
// fn returns. Comments (lines starting with ':') and blank lines are skipped.
func ReadSSE(r io.Reader, fn func(Event) error) error {
	sc := bufio.NewScanner(r)
	// An event's data can exceed 64KB when a tool call arrives in one chunk.
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var cur Event
	var data []string
	flush := func() error {
		if len(data) == 0 {
			cur = Event{}
			return nil
		}
		cur.Data = strings.Join(data, "\n")
		data = data[:0]
		err := fn(cur)
		cur = Event{}
		return err
	}
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			cur.Event = value
		case "data":
			data = append(data, value)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return flush()
}

// Usage is the accumulated token accounting of one streamed response.
//
// In is the **uncached** input count, not the upstream's prompt total: the
// harness follows a disjoint-count rule (js stream.js:11-13), so the OpenAI
// prompt_tokens figure has its cache hits subtracted back out into CacheRead.
// Callers that need the gross number add the two back together
// (engine.OpenAIUsage, health.NoteStickyUsage).
type Usage struct {
	In        int64
	Out       int64
	CacheRead int64
	TTFTMS    int64
	HasUsage  bool
}

// ScanUsage folds one chunk into acc.
//
// firstContentSeen is a pointer so the caller owns the flag across chunks and
// can also pass it to the rotation engine's trace rows. TTFT is frozen on the
// first content delta: the first SSE event is usually a role skeleton, and
// timing that instead of the first visible token would understate TTFT by the
// whole time-to-first-byte of the protocol handshake.
func ScanUsage(chunk []byte, acc *Usage, firstContentSeen *bool, t0 time.Time) (delta int64, isContent bool) {
	var env struct {
		Type  string `json:"type"`
		Delta struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage *struct {
			InputTokens         int64 `json:"input_tokens"`
			OutputTokens        int64 `json:"output_tokens"`
			PromptTokens        int64 `json:"prompt_tokens"`
			CompletionTokens    int64 `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			InputTokensDetails *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(chunk, &env); err != nil {
		return 0, false
	}
	if env.Usage != nil {
		acc.HasUsage = true
		// 上游的输入总数是**毛值**(prompt_tokens/input_tokens 已含缓存命中),
		// 而 harness 的 disjoint-count 规则要求 inputTokens 是**未缓存输入**:
		// js stream.js:11-13 把这条规则写在模块注释里,mapUsage:122 用
		// Math.max(0, prompt - cached) 落地。CacheRead 单独一个量。
		gross := env.Usage.InputTokens + env.Usage.PromptTokens
		acc.Out = env.Usage.OutputTokens + env.Usage.CompletionTokens
		acc.CacheRead = 0
		if d := env.Usage.PromptTokensDetails; d != nil {
			acc.CacheRead = d.CachedTokens
		}
		if d := env.Usage.InputTokensDetails; d != nil {
			acc.CacheRead = d.CachedTokens
		}
		acc.In = gross - acc.CacheRead
		if acc.In < 0 {
			acc.In = 0
		}
	}
	switch {
	case env.Type == "content_block_delta" && env.Delta.Type == "text_delta":
		isContent = true
	case env.Type == "content_block_start" && len(env.Content) > 0 && env.Content[0].Type == "text":
		isContent = true
	}
	if isContent && !*firstContentSeen {
		*firstContentSeen = true
		acc.TTFTMS = time.Since(t0).Milliseconds()
	}
	if env.Delta.Text != "" {
		return int64(len(env.Delta.Text)), isContent
	}
	return 0, isContent
}
