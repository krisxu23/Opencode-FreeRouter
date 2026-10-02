// SPDX-License-Identifier: GPL-3.0-or-later
package stream

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// 断言 12：逐行回调。缓冲到 [DONE] 再一次性吐出，会让 TTFT 变成整段耗时，
// 粘性与轮换的反馈全部失效。
//
// 确定性写法：writer 写完第一个事件后**阻塞等回调信号**才继续写/关流——
// 计划原稿让 writer 先给 buffered chan 发 "first" 再睡 300ms，回调与
// "first" 的到达顺序取决于调度，是竞态。
func TestReadSSECallsBackBeforeTheStreamEnds(t *testing.T) {
	pr, pw := io.Pipe()
	seen := make(chan string)
	go func() {
		_, _ = pw.Write([]byte("data: {\"a\":1}\n\n"))
		<-seen // 等回调先到：流还没结束，第一个事件必须已经送达调用方
		_, _ = pw.Write([]byte("data: [DONE]\n\n"))
		_ = pw.Close()
	}()
	err := ReadSSE(pr, func(e Event) error {
		if e.Data == `{"a":1}` {
			seen <- "callback"
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestReadSSESplitsMultilineData(t *testing.T) {
	// SSE 规范：多个 data: 行属于同一个事件，用空行分隔。
	in := "event: message_start\ndata: {\"x\":\ndata: 1}\n\ndata: [DONE]\n\n"
	var got []Event
	if err := ReadSSE(strings.NewReader(in), func(e Event) error { got = append(got, e); return nil }); err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("events = %d, want 2: %+v", len(got), got)
	}
	if got[0].Event != "message_start" || got[0].Data != "{\"x\":\n1}" {
		t.Fatalf("event 0 = %+v", got[0])
	}
	if got[1].Data != "[DONE]" {
		t.Fatalf("event 1 = %+v", got[1])
	}
}

func TestReadSSEIgnoresCommentsAndBlankLines(t *testing.T) {
	in := ": keep-alive\n\n\ndata: [DONE]\n\n"
	n := 0
	if err := ReadSSE(strings.NewReader(in), func(Event) error { n++; return nil }); err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != 1 {
		t.Fatalf("callbacks = %d, want 1", n)
	}
}

func TestCallbackErrorStopsTheRead(t *testing.T) {
	sentinel := errors.New("stop")
	n := 0
	err := ReadSSE(strings.NewReader("data: 1\n\ndata: 2\n\ndata: 3\n\n"), func(Event) error {
		n++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the callback's error", err)
	}
	if n != 1 {
		t.Fatalf("callbacks = %d, want 1", n)
	}
}

func TestScanUsageReadsBothCacheTokenLocations(t *testing.T) {
	// 两种上游形状都要认：Claude Code 走 prompt_tokens_details，
	// OpenAI 兼容层走 input_tokens_details。
	for _, body := range []string{
		`{"type":"message_delta","usage":{"input_tokens":10,"output_tokens":5,"input_tokens_details":{"cached_tokens":7}}}`,
		`{"type":"message_delta","usage":{"input_tokens":10,"output_tokens":5,"prompt_tokens_details":{"cached_tokens":7}}}`,
	} {
		var acc Usage
		t0 := time.Now()
		if _, isContent := ScanUsage([]byte(body), &acc, new(bool), t0); isContent {
			t.Fatalf("usage-only chunk must not count as content: %s", body)
		}
		if !acc.HasUsage {
			t.Fatalf("HasUsage = false for %s", body)
		}
		// 缓存命中必须从输入里减掉:harness 的 inputTokens 是「未缓存输入」
		// (js stream.js:11-13 的 disjoint-count 规则、mapUsage:122 的
		// Math.max(0, prompt - cached)),CacheRead 单独一个量。
		if acc.In != 3 || acc.Out != 5 || acc.CacheRead != 7 {
			t.Fatalf("usage = %+v, want in=3(10-7) out=5 cacheRead=7", acc)
		}
	}
}

func TestTTFTFreezesOnFirstContentNotFirstChunk(t *testing.T) {
	var acc Usage
	first := new(bool)
	t0 := time.Now().Add(-2 * time.Second) // 假装已经过了 2s
	// 第一个 chunk 只是 role 骨架。
	if _, isContent := ScanUsage([]byte(`{"type":"message_start"}`), &acc, first, t0); isContent {
		t.Fatal("message_start is not content")
	}
	if acc.TTFTMS != 0 {
		t.Fatalf("TTFT set by a non-content chunk: %d", acc.TTFTMS)
	}
	// 第二个才是内容。
	if _, isContent := ScanUsage([]byte(`{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`), &acc, first, t0); !isContent {
		t.Fatal("text_delta is content")
	}
	if acc.TTFTMS < 1500 {
		t.Fatalf("TTFT = %d, want ~2000 (measured from the request start)", acc.TTFTMS)
	}
}
