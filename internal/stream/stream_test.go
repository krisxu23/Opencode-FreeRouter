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
		if isContent := ScanUsage([]byte(body), &acc, new(bool), t0); isContent {
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

// B4:两种拼写是同一个量的**别名**,不是两个量。相加会把输入与输出一起翻倍
// (报告实测:gross=200 / Out=10,正确值是 100 / 5)。js stream.js:115-116 逐字
// 用 `prompt_tokens ?? input_tokens`、`completion_tokens ?? output_tokens`。
func TestScanUsagePicksOneSpellingInsteadOfAddingThem(t *testing.T) {
	var acc Usage
	body := `{"usage":{"prompt_tokens":100,"input_tokens":100,"output_tokens":5,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":40}}}`
	if isContent := ScanUsage([]byte(body), &acc, new(bool), time.Now()); isContent {
		t.Fatal("a usage-only frame is not content")
	}
	if acc.In != 60 || acc.Out != 5 || acc.CacheRead != 40 {
		t.Fatalf("usage = %+v, want in=60(100-40) out=5 cacheRead=40", acc)
	}
}

// B4 的另外两面:只给一种拼写时取值照旧;缓存细节是
// `prompt_tokens_details?.cached_tokens ?? input_tokens_details?.cached_tokens`
// (js stream.js:117),前者缺字段才回退后者,而不是后者无条件覆盖前者。
func TestScanUsagePrefersTheFirstSpellingAndTheFirstCacheDetail(t *testing.T) {
	cases := []struct {
		name               string
		body               string
		in, out, cacheRead int64
	}{
		{"prompt-only", `{"usage":{"prompt_tokens":10,"completion_tokens":2}}`, 10, 2, 0},
		{"input-only", `{"usage":{"input_tokens":10,"output_tokens":2}}`, 10, 2, 0},
		{"prompt-detail-wins",
			`{"usage":{"prompt_tokens":10,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":3},"input_tokens_details":{"cached_tokens":7}}}`,
			7, 2, 3},
		{"fallback-to-input-detail",
			`{"usage":{"prompt_tokens":10,"completion_tokens":2,"prompt_tokens_details":{},"input_tokens_details":{"cached_tokens":7}}}`,
			3, 2, 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var acc Usage
			ScanUsage([]byte(tc.body), &acc, new(bool), time.Now())
			if !acc.HasUsage || acc.In != tc.in || acc.Out != tc.out || acc.CacheRead != tc.cacheRead {
				t.Fatalf("usage = %+v, want in=%d out=%d cacheRead=%d", acc, tc.in, tc.out, tc.cacheRead)
			}
		})
	}
}

// R14:只带输出侧的 usage 帧必须**并入**既有值,而不是整份覆盖
// (js stream.js:284-293 的三元表达式)。整份覆盖会把 In/CacheRead 清零,
// 于是 sticky TTL(cacheRead/In 比例)与面板用量一起失真。
func TestScanUsageMergesAnOutputOnlyFrame(t *testing.T) {
	start := Usage{In: 11, CacheRead: 3, Out: 5, HasUsage: true}
	for _, body := range []string{
		`{"usage":{"output_tokens":7}}`,
		`{"usage":{"completion_tokens":7}}`,
		`{"type":"message_delta","usage":{"output_tokens":7}}`,
	} {
		acc := start
		ScanUsage([]byte(body), &acc, new(bool), time.Now())
		if acc.In != 11 || acc.CacheRead != 3 || acc.Out != 7 || !acc.HasUsage {
			t.Fatalf("%s -> %+v, want in=11 cacheRead=3 out=7 (the input side is carried)", body, acc)
		}
	}
}

// R14 的另一半:输入侧与输出侧都不提的 usage 帧**不产出 usage**
// (js stream.js:120 的 `if (prompt === undefined && completion === undefined)
// return undefined`),acc 与 HasUsage 都不动。
func TestScanUsageIgnoresAFrameThatNamesNeitherSide(t *testing.T) {
	acc := Usage{In: 11, CacheRead: 3, Out: 5, HasUsage: true}
	for _, body := range []string{
		`{"usage":{}}`,
		`{"usage":{"prompt_tokens_details":{"cached_tokens":7}}}`,
		`{"usage":null}`,
	} {
		got := acc
		ScanUsage([]byte(body), &got, new(bool), time.Now())
		if got != acc {
			t.Fatalf("%s -> %+v, want the accumulator untouched (%+v)", body, got, acc)
		}
	}
	var fresh Usage
	ScanUsage([]byte(`{"usage":{}}`), &fresh, new(bool), time.Now())
	if fresh.HasUsage {
		t.Fatalf("an empty usage object must not report usage: %+v", fresh)
	}
}

func TestTTFTFreezesOnFirstContentNotFirstChunk(t *testing.T) {
	var acc Usage
	first := new(bool)
	t0 := time.Now().Add(-2 * time.Second) // 假装已经过了 2s
	// 第一个 chunk 只是 role 骨架。
	if isContent := ScanUsage([]byte(`{"type":"message_start"}`), &acc, first, t0); isContent {
		t.Fatal("message_start is not content")
	}
	if acc.TTFTMS != 0 {
		t.Fatalf("TTFT set by a non-content chunk: %d", acc.TTFTMS)
	}
	// 第二个才是内容。
	if isContent := ScanUsage([]byte(`{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`), &acc, first, t0); !isContent {
		t.Fatal("text_delta is content")
	}
	if acc.TTFTMS < 1500 {
		t.Fatalf("TTFT = %d, want ~2000 (measured from the request start)", acc.TTFTMS)
	}
}

// TestEventDataIsBounded 是 R11 的钉子。sc.Buffer 的 8MB 是**每行**上限,
// 而 flush 只在空行/EOF 触发 —— 对端只要一直发 `data:` 行、从不发空行,
// 这个切片就无界增长,而内存记在网关头上。
//
// 用 io.Pipe 逐行喂,保证是"很多行、没有空行"的形状;上限之内的正常事件
// 必须照常回调,所以同一个测试先钉合法面再钉越界面。
func TestEventDataIsBounded(t *testing.T) {
	// 先确认上限之内正常:8MB 以内的一条多行事件必须回调。
	small := strings.Repeat("data: x\n", 3) + "\n"
	var got []Event
	if err := ReadSSE(strings.NewReader(small), func(ev Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("上限之内的事件不该失败: %v", err)
	}
	if len(got) != 1 || got[0].Data != "x\nx\nx" {
		t.Fatalf("events = %+v, want one three-line event", got)
	}

	// 再确认越界会失败。用生成器而不是构造一份 8MB 字符串:测试自己先分配
	// 的话,测的就不是被测代码的内存了。
	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		line := []byte("data: " + strings.Repeat("a", 64*1024) + "\n")
		for sent := 0; sent <= maxEventBytes; sent += len(line) {
			if _, err := pw.Write(line); err != nil {
				return // 读侧已返回,pipe 关闭
			}
		}
	}()
	err := ReadSSE(pr, func(Event) error { return nil })
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("err = %v, want ErrEventTooLarge", err)
	}
}

// TestOversizedSingleLineSurfacesAsEventTooLarge 是协议审计 L4 的钉:一条
// 超过 8MB 的**单行** data 先撞的是 bufio.Scanner 的行长上限(累加器要等
// 这行扫完才看得到它),过去漏出的是 bufio.ErrTooLong —— 与多行拼出来的
// ErrEventTooLarge 是两个不同的哨兵,同一个语义却叫两个名字。现在必须收敛
// 成同一个 ErrEventTooLarge。
//
// 用 io.Pipe 流式喂(与上面 :230 的越界测试同一手法):测试自己先分配一个
// 8MB+ 的字符串,测的就不是被测代码的内存了。
func TestOversizedSingleLineSurfacesAsEventTooLarge(t *testing.T) {
	pr, pw := io.Pipe()
	go func() {
		defer func() { _ = pw.Close() }()
		if _, err := pw.Write([]byte("data: ")); err != nil {
			return
		}
		chunk := []byte(strings.Repeat("a", 64*1024))
		for sent := 0; sent <= maxEventBytes; sent += len(chunk) {
			if _, err := pw.Write(chunk); err != nil {
				return // 读侧已返回,pipe 关闭
			}
		}
	}()
	err := ReadSSE(pr, func(Event) error { return nil })
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("err = %v, want ErrEventTooLarge(L4:单行超限必须与多行超限同一哨兵,不得漏出 bufio.ErrTooLong)", err)
	}
}

// TestEventCapDoesNotRejectAnEventExactlyAtTheLimit 是 R11 的差一字节钉子。
// 多行事件在 Join 时会插入换行,只有非首行才该计入;否则一个正好等于上限的
// 事件会被多算字节而误拒。
//
// 事件由 8 行拼成(首行少 7 字节),拼出的 data 长度恰好等于 maxEventBytes:
// 8×1MB − 7 + 7 个换行 = 8MB。单行做不到这一点 —— bufio.Scanner 的每行上限
// 也是 8MB,而 "data: " 前缀会让单行先撞上 Scanner 的 ErrTooLong。
func TestEventCapDoesNotRejectAnEventExactlyAtTheLimit(t *testing.T) {
	const lineSize = 1 << 20
	firstSize := lineSize - 7
	parts := make([]io.Reader, 0, 8)
	parts = append(parts, strings.NewReader("data: "), io.LimitReader(byteFiller{}, int64(firstSize)), strings.NewReader("\n"))
	for i := 0; i < 7; i++ {
		parts = append(parts, strings.NewReader("data: "), io.LimitReader(byteFiller{}, lineSize), strings.NewReader("\n"))
	}
	parts = append(parts, strings.NewReader("\n"))

	var got int
	if err := ReadSSE(io.MultiReader(parts...), func(ev Event) error {
		got = len(ev.Data)
		return nil
	}); err != nil {
		t.Fatalf("恰好等于上限的事件必须通过: %v", err)
	}
	if got != maxEventBytes {
		t.Fatalf("data = %d 字节, want %d", got, maxEventBytes)
	}
}

// byteFiller 产出无限个 'a'。
type byteFiller struct{}

func (byteFiller) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	return len(p), nil
}

func TestScanUsageAcceptsFloatLiterals(t *testing.T) {
	// W8:上游偶发以浮点形状发 token 数;旧 *int64 遇到 1234.0 会在解码时
	// 整帧失败,连同内容检测一起丢 —— chat 线的整轮 usage 就此蒸发。
	// 注意:choices 形状的内容判定不归 ScanUsage(adapter 的 carriesDelta 管,
	// ScanUsage 只认 claude 线的 content_block 形状),所以这里只断言 usage。
	var acc Usage
	ScanUsage([]byte(`{"choices":[{"delta":{"content":"hi"}}],"usage":{"prompt_tokens":1234.0,"completion_tokens":5e0,"prompt_tokens_details":{"cached_tokens":34.0}}}`), &acc, new(bool), time.Now())
	if !acc.HasUsage {
		t.Fatal("浮点形状的 usage 帧必须被识别")
	}
	if acc.In != 1200 || acc.Out != 5 || acc.CacheRead != 34 {
		t.Fatalf("In=%d Out=%d CacheRead=%d, want 1200/5/34(disjoint-count 减缓存)", acc.In, acc.Out, acc.CacheRead)
	}
}
