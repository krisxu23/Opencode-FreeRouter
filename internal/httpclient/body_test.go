// SPDX-License-Identifier: GPL-3.0-or-later
package httpclient

import (
	"errors"
	"strings"
	"testing"
)

// R3:上限必须是**精确**的。limit 字节是合法响应(很多真实订阅恰好压着上限),
// 多一字节才是越界——差一字节的判定会让一个正常源被判失败。
func TestReadCappedAcceptsExactlyTheLimit(t *testing.T) {
	const limit = 1024
	body, err := ReadCapped(strings.NewReader(strings.Repeat("a", limit)), limit)
	if err != nil {
		t.Fatalf("恰好等于上限必须成功: %v", err)
	}
	if len(body) != limit {
		t.Fatalf("读了 %d 字节, want %d", len(body), limit)
	}
}

func TestReadCappedRejectsOneByteOver(t *testing.T) {
	const limit = 1024
	_, err := ReadCapped(strings.NewReader(strings.Repeat("a", limit+1)), limit)
	if err == nil {
		t.Fatal("超过上限必须报错,而不是静默截断成半份响应")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("错误文案要能归因: %v", err)
	}
}

// 读侧错误必须原样冒出来:调用方靠它区分「对端断了」和「响应体太大」。
func TestReadCappedPropagatesReaderError(t *testing.T) {
	boom := errors.New("boom")
	_, err := ReadCapped(errReader{boom}, 1024)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
