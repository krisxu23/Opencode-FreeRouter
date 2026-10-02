// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package errors

import (
	stderrors "errors"
	"testing"

	"freerouter/internal/check"
)

// 对照 tests/errors.test.js 与阶段 3 计划任务 12 步骤 1 的 8 条测试表。
// 分类顺序在这里就是契约本身:403 + "usage limit" 文案必须落进配额而不是
// 凭证 —— 顺序一换,引擎会把「换个出口就能解决」的问题当成「全池凭证失效」
// 去慢扫(src/errors.js:47-61 的顺序事故)。

func TestClassifyRegion(t *testing.T) {
	f := Classify(451, []byte(`{"error":{"type":"RegionError"}}`), 0)
	if f.Code != check.CodeRegion {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeRegion)
	}
}

func TestClassifyQuotaOn429(t *testing.T) {
	f := Classify(429, []byte(`{"error":{"type":"FreeUsageLimitError","message":"usage limit"}}`), 12000)
	if f.Code != check.CodeQuota {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeQuota)
	}
	if f.RetryAfterMS != 12000 {
		t.Fatalf("RetryAfterMS = %d, want 12000", f.RetryAfterMS)
	}
}

func TestClassifyQuotaOn403WithLimitWording(t *testing.T) {
	// 顺序敏感的那一条:这个 403 是配额拒绝,不是凭证失败。
	f := Classify(403, []byte(`{"error":{"message":"usage limit reached"}}`), 0)
	if f.Code != check.CodeQuota {
		t.Fatalf("403 + usage-limit wording classified as %q, want %q (配额分支必须在凭证分支之前)", f.Code, check.CodeQuota)
	}
}

func TestClassifyFreeTier403IsQuota(t *testing.T) {
	f := Classify(403, []byte(`{"error":{"type":"FreeTierError","message":"nope"}}`), 0)
	if f.Code != check.CodeQuota {
		t.Fatalf("Code = %q, want %q (FreeTierError 文案不匹配时按 type 命中)", f.Code, check.CodeQuota)
	}
}

func TestClassify401IsCredential(t *testing.T) {
	// 401 是配置错误:标成可重试会让一个错配置变成扫全池的慢失败。
	f := Classify(401, []byte(`{"error":{"type":"AuthenticationError","message":"bad key"}}`), 0)
	if f.Code != check.CodeCredential {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeCredential)
	}
	if f.Retryable {
		t.Fatal("401 must not be retryable")
	}
}

func TestClassifyModelUnavailableIsServerAndFlagsUnavailable(t *testing.T) {
	f := Classify(400, []byte(`{"error":{"type":"ModelError","message":"model is unavailable"}}`), 0)
	if f.Code != check.CodeServer {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeServer)
	}
	if !f.Unavailable {
		t.Fatal("ModelError must flag Unavailable (换出口救不了,不该轮换)")
	}
}

func TestClassifyEmptyBodyFallsBackToStatusMessage(t *testing.T) {
	f := Classify(500, nil, 0)
	if f.Message != "upstream HTTP 500" {
		t.Fatalf("Message = %q, want %q", f.Message, "upstream HTTP 500")
	}
}

func TestRetryAfterParsesSecondsOnly(t *testing.T) {
	cases := []struct {
		header string
		want   int64
	}{
		{"12", 12000},
		{"abc", 0},
		{"0", 0},
		{"-5", 0},
		{"", 0},
	}
	for _, c := range cases {
		if got := RetryAfter(c.header); got != c.want {
			t.Errorf("RetryAfter(%q) = %d, want %d", c.header, got, c.want)
		}
	}
}

func TestCodeOfReturnsCodeOrEmpty(t *testing.T) {
	if got := CodeOf(Failure{Code: check.CodeQuota}); got != check.CodeQuota {
		t.Fatalf("CodeOf(Failure) = %q, want %q", got, check.CodeQuota)
	}
	// 普通 error(取消、拨号失败)必须返回 "" 而不是 panic —— 每个决定是否
	// 轮换的层都会调它。
	if got := CodeOf(stderrors.New("context canceled")); got != "" {
		t.Fatalf("CodeOf(plain error) = %q, want \"\"", got)
	}
}

// TestClassifyBareErrorObjectIsThePayloadToo 钉住 `payload?.error ?? payload`
// 的后半句(src/errors.js:40):上游偶发的裸错误对象(type/message 直接在顶层)
// 也必须走完整分类链 —— 少这一层兜底时,同样的响应体会掉进 default 的 server
// 分类,地区块模型会错误地原地重试而不是换出口(任务 27 差分前夜抓到的实错)。
func TestClassifyBareErrorObjectIsThePayloadToo(t *testing.T) {
	f := Classify(451, []byte(`{"type":"RegionError","message":"not available in your country"}`), 0)
	if f.Code != check.CodeRegion {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeRegion)
	}
	f = Classify(403, []byte(`{"type":"FreeTierError"}`), 0)
	if f.Code != check.CodeQuota {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeQuota)
	}
	// 信封形状优先:有 error 键就整体用它,顶层字段不再掺入。
	f = Classify(401, []byte(`{"error":{"type":"api_error"},"type":"RegionError","message":"x"}`), 0)
	if f.Code != check.CodeCredential {
		t.Fatalf("Code = %q, want credential (envelope wins wholesale)", f.Code)
	}
}
