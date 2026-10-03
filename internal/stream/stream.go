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
	"errors"
	"io"
	"strings"
	"time"
)

// maxEventBytes caps one event's accumulated data (R11).
//
// sc.Buffer's 8MB is a **per-line** cap. SSE lets an event carry any number of
// `data:` lines, and flush() only runs on a blank line or EOF — so an upstream
// that streams `data:` lines forever without ever emitting a blank line grows
// this slice without bound, and the gateway is the one holding the memory.
// The cap matches the per-line cap: an event that needs more than 8MB of data
// is not a chat frame.
const maxEventBytes = 8 << 20

// ErrEventTooLarge is returned when one event's data exceeds maxEventBytes.
// Truncating instead would hand the caller a half-parsed JSON frame, which is
// worse than failing the turn.
var ErrEventTooLarge = errors.New("stream: SSE event data exceeds the cap")

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
	// size 是 data 里已累积的字节数(含 Join 会插入的换行)。data 切片会被
	// flush 复用(len 归零但容量还在),所以字节数必须单独记,不能靠 len(data)。
	size := 0
	flush := func() error {
		if len(data) == 0 {
			cur = Event{}
			return nil
		}
		cur.Data = strings.Join(data, "\n")
		data = data[:0]
		size = 0
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
			// 每行之间 strings.Join 会插一个换行,只有非首行才计入,否则
			// 单行事件会被多算一个字节、在正好等于上限时被误拒。
			if len(data) > 0 {
				size++
			}
			size += len(value)
			if size > maxEventBytes {
				return ErrEventTooLarge
			}
			data = append(data, value)
		}
	}
	if err := sc.Err(); err != nil {
		// bufio 的行长上限与 maxEventBytes 是同一个 8MB,"data: " 前缀让
		// **单行**超限先在这里撞响,而不是在上面的累加器(协议审计 L4)。
		// 翻译成 ErrEventTooLarge 是让它与多行拼出来的超限走**同一个哨兵**:
		// 两条路径的语义完全相同(一个事件超过了上限),不该因为测量点不同
		// 而叫两个名字。诚实记录:engine 的 classifyAttemptError 现在把这两
		// 者都归 SERVER(它只认 errors.Failure),所以这条翻译改变的是**本包
		// 的返回契约一致性**,不是下游的分类/冷却;真要区分冷却,得在 engine
		// 那一侧动分类,而那张矩阵是被 JS 逐条钉死的,不在这里擅动。
		if errors.Is(err, bufio.ErrTooLong) {
			return ErrEventTooLarge
		}
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

// ScanUsage folds one chunk into acc and reports whether the chunk carried
// content. (旧签名还返回文本增量的字节数 —— 生产调用方从来不读它,删掉。)
//
// firstContentSeen is a pointer so the caller owns the flag across chunks and
// can also pass it to the rotation engine's trace rows. TTFT is frozen on the
// first content delta: the first SSE event is usually a role skeleton, and
// timing that instead of the first visible token would understate TTFT by the
// whole time-to-first-byte of the protocol handshake.
func ScanUsage(chunk []byte, acc *Usage, firstContentSeen *bool, t0 time.Time) (isContent bool) {
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
		// 四个计数都用指针 + float64:指针是因为 js 的 `??` 判的是 undefined,
		// Go 的零值分不出「字段缺失」与「上游显式发了 0」,没有这层区分 B4 的
		// 两种拼写就没法按 `prompt_tokens ?? input_tokens` 择一;float64 是因为
		// 上游偶发以浮点形状发 token 数(`1234.0`、`1e3`)—— 旧 *int64 遇到会在
		// Unmarshal 里报 saveError,整个结构体解码失败,**连同内容检测一起**把
		// 这帧丢掉,chat 线的整轮 usage 就此蒸发。token 计数在 2^53 内,float64
		// 无损。B4/R14 依赖的「缺席 vs 显式 0」区分由指针保留。
		Usage *struct {
			InputTokens         *float64 `json:"input_tokens"`
			OutputTokens        *float64 `json:"output_tokens"`
			PromptTokens        *float64 `json:"prompt_tokens"`
			CompletionTokens    *float64 `json:"completion_tokens"`
			PromptTokensDetails *struct {
				CachedTokens *float64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			InputTokensDetails *struct {
				CachedTokens *float64 `json:"cached_tokens"`
			} `json:"input_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(chunk, &env); err != nil {
		return false
	}
	if env.Usage != nil {
		// B4:prompt_tokens/input_tokens 与 completion_tokens/output_tokens 是
		// **同一个量的两种拼写**(js stream.js:115-116 的 `??`),不是两个量。
		// 相加会让输入与输出一起翻倍(报告实测 gross=200 / Out=10,正确 100 / 5)。
		prompt := env.Usage.PromptTokens
		if prompt == nil {
			prompt = env.Usage.InputTokens
		}
		completion := env.Usage.CompletionTokens
		if completion == nil {
			completion = env.Usage.OutputTokens
		}
		// js stream.js:120:两侧都不提的 usage 帧**不产出 usage**(`{"usage":{}}`
		// 这种心跳帧过去会把整个累加器清零并置 HasUsage)。
		if prompt != nil || completion != nil {
			acc.HasUsage = true
			// 缓存细节也是 `??` 而不是覆盖:prompt_tokens_details 里没有
			// cached_tokens 才回退 input_tokens_details(js stream.js:117)。
			var cached int64
			switch {
			case env.Usage.PromptTokensDetails != nil && env.Usage.PromptTokensDetails.CachedTokens != nil:
				cached = int64(*env.Usage.PromptTokensDetails.CachedTokens)
			case env.Usage.InputTokensDetails != nil && env.Usage.InputTokensDetails.CachedTokens != nil:
				cached = int64(*env.Usage.InputTokensDetails.CachedTokens)
			}
			if prompt == nil {
				// R14:只带输出侧的帧**并入**既有值,而不是整份覆盖
				// (js stream.js:290-291 的三元)。Claude 线的 message_delta 就是
				// 这个形状,整份覆盖会把 message_start 已给出的输入侧清零,
				// 连带污染 sticky TTL(cacheRead/In 比例)与面板用量。
				if completion != nil {
					acc.Out = int64(*completion)
				}
			} else {
				// 上游的输入总数是**毛值**(prompt_tokens/input_tokens 已含缓存命中),
				// 而 harness 的 disjoint-count 规则要求 inputTokens 是**未缓存输入**:
				// js stream.js:11-13 把这条规则写在模块注释里,mapUsage:122 用
				// Math.max(0, prompt - cached) 落地。CacheRead 单独一个量。
				acc.CacheRead = cached
				acc.In = int64(*prompt) - cached
				if acc.In < 0 {
					acc.In = 0
				}
				if completion != nil {
					acc.Out = int64(*completion)
				} else {
					acc.Out = 0
				}
			}
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
	return isContent
}
