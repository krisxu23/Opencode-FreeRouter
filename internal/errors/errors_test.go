// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package errors

import (
	stderrors "errors"
	"fmt"
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

// R8:Retry-After 提示不能只有配额分支带。5xx + Retry-After 是供应商真实会发
// 的组合(网关过载时),而 engine 只在 cooldownOn={transport,timeout} 里消费它。
// 从前 default 分支把 retryAfterMS 丢掉,于是这条提示永远到不了 NoteCooldown,
// health 里 `if retryAfterMS > 0` 成了死分支。
func TestClassifyKeepsRetryAfterOnServerErrors(t *testing.T) {
	f := Classify(503, []byte(`{"error":{"message":"upstream overloaded"}}`), 12000)
	if f.Code != check.CodeServer {
		t.Fatalf("Code = %q, want %q", f.Code, check.CodeServer)
	}
	if !f.Retryable {
		t.Fatal("503 must stay retryable")
	}
	if f.RetryAfterMS != 12000 {
		t.Fatalf("RetryAfterMS = %d, want 12000(5xx 的 Retry-After 必须带到 engine)", f.RetryAfterMS)
	}
	// 不可重试的分支不该开始携带提示:401 是配置错误,退避语义没有意义。
	if got := Classify(401, []byte(`{"error":{"message":"bad key"}}`), 12000); got.RetryAfterMS != 0 {
		t.Fatalf("401 RetryAfterMS = %d, want 0(凭证失败不是退避)", got.RetryAfterMS)
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
		{"120", 120000},  // 封顶线以下(2 分钟)原样放行
		{"700", 600000},  // 700s > 10 分钟:钳到封顶值
		{"1e18", 600000}, // 浮点天文数字同样封顶(旧实现会算出 1e21ms ≈ 3 万年)
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

// TestClassifyRegionTextRequiresFourXX 钉住上游层审计的 REGION 投毒链的咽喉:
// 旧 regionRe 里有一个裸词 `region`,匹配任何文案含 region/regional/regions
// 的响应**且不分状态码** —— 一句 500 的 "regional datacenter issue" 就被判成
// REGION。REGION 在 engine 里除了换出口还会调 NoteRegionError,把该出口的
// B 档凭证无条件打回 A;B 是全池唯一的「门控模型可用」证据,误判一次就让
// muse-spark 从 /v1/models 消失、gated 请求全报「无健康出口」,只能等下一轮
// 粗探自愈。现在:显式 type==RegionError 任何状态都认;文案判据只留完整短语
// 且必须配 4xx。
func TestClassifyRegionTextRequiresFourXX(t *testing.T) {
	// 真地区拒绝的两种文案:4xx 上必须仍判 REGION(收紧不能把真判决也收紧掉)。
	for _, body := range []string{
		`{"error":{"message":"The model is not available in your country"}}`,
		`{"error":{"message":"this model is not available in this region"}}`,
	} {
		for _, status := range []int{403, 451} {
			if f := Classify(status, []byte(body), 0); f.Code != check.CodeRegion {
				t.Fatalf("status %d body %s: code = %q, want REGION", status, body, f.Code)
			}
		}
	}
	// 结构化类型:任何状态都认,包括 5xx(type 是权威信号,不受文案收紧影响)。
	if f := Classify(503, []byte(`{"error":{"type":"RegionError","message":"whatever"}}`), 0); f.Code != check.CodeRegion {
		t.Fatalf("type=RegionError on 503: code = %q, want REGION (类型判据不分状态码)", f.Code)
	}
	// 投毒面:5xx 文案里出现 region/regional/regions,不得再判 REGION。
	for _, body := range []string{
		`{"error":{"message":"a regional datacenter issue occurred"}}`,
		`{"error":{"message":"we are migrating to new regions"}}`,
		`{"error":{"message":"RegionError is a red herring"}}`, // 文案提到这个词,但 type 不是
	} {
		f := Classify(500, []byte(body), 0)
		if f.Code == check.CodeRegion {
			t.Fatalf("500 %s 被判成 REGION —— 投毒链复发(误打 B 档凭证)", body)
		}
		if f.Code != check.CodeServer {
			t.Fatalf("500 %s: code = %q, want SERVER(落回默认分支)", body, f.Code)
		}
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

func TestRegionVariantsAreRecognizedOn4xx(t *testing.T) {
	// v1.2.3 收紧 regionRe 消灭了 5xx 投毒链,但把 4xx 的真实变体也漏掉了:
	// "not available in your region"/"unsupported region" 过去会落进 403
	// 凭证分支变成不可重试的硬失败。
	for _, body := range []string{
		`{"error":{"message":"not available in your region","type":"RegionError"}}`,
		`{"error":{"message":"unsupported region"}}`,
	} {
		f := Classify(403, []byte(body), 0)
		if f.Code != check.CodeRegion || !f.Retryable {
			t.Fatalf("body %s → %v, want REGION 可重试", body, f)
		}
	}
}

func TestInsufficientNarrowsToBillingPhrases(t *testing.T) {
	// 裸 "insufficient" 子串过宽:基础设施文案(如 insufficient disk space)
	// 会从 SERVER 偏移到 QUOTA。收窄到计费短语后,5xx 基础设施失败回到原判。
	f := Classify(500, []byte(`{"error":{"message":"insufficient disk space on worker"}}`), 0)
	if f.Code == check.CodeQuota {
		t.Fatalf("disk space 不该算配额: %v", f)
	}
	f = Classify(402, []byte(`{"error":{"message":"Insufficient account funds"}}`), 0)
	if f.Code != check.CodeQuota || !f.Retryable {
		t.Fatalf("402 计费拒绝应归 quota 可重试: %v", f)
	}
}

// TestModelReIsNarrowedToModelDimension 钉住 modelRe 的收窄。旧实现里的裸词
// `not supported` 让任何提到「不支持」的文案都命中模型支,而这一支打的是
// Unavailable 位 —— engine 见到它就直接失败、不再扫池(engine.go:537)。
// 一句基础设施 5xx 里的 "region not supported" 就足以让可用模型第一次尝试
// 就报死。
func TestModelReIsNarrowedToModelDimension(t *testing.T) {
	// 真判决:模型维度的拒绝仍必须打 Unavailable。
	for _, body := range []string{
		`{"error":{"type":"ModelError","message":"model is unavailable"}}`,
		`{"error":{"message":"This model is not supported"}}`,
		`{"error":{"message":"model not found"}}`,
		`{"error":{"message":"unknown model: gpt-x"}}`,
		`{"error":{"message":"unsupported model"}}`,
	} {
		f := Classify(400, []byte(body), 0)
		if !f.Unavailable {
			t.Errorf("%s 应判 Unavailable(换出口救不了),得到 %+v", body, f)
		}
	}
	// 非模型维度的「不支持」必须落回默认分支:5xx 仍 Retryable,4xx 不带
	// Unavailable 位(否则就是上面那条误杀)。
	for _, body := range []string{
		`{"error":{"message":"this endpoint is not supported"}}`,
		`{"error":{"message":"streaming is not supported for this key"}}`,
		`{"error":{"message":"region not supported"}}`,
		`{"error":{"message":"unsupported region"}}`,
	} {
		f := Classify(500, []byte(body), 0)
		if f.Unavailable {
			t.Errorf("%s 被误判成模型不可用 —— 会让可用模型首尝试就失败: %+v", body, f)
		}
		if f.Code != check.CodeServer || !f.Retryable {
			t.Errorf("%s 应落回默认 SERVER 可重试,得到 %+v", body, f)
		}
	}
}

// TestCodeOfSeesWrappedFailure:链路里被 fmt.Errorf("%w") 包过的 Failure 也必须
// 拿到失败码 —— 裸类型断言在那种路径上静默返回 ""。
func TestCodeOfSeesWrappedFailure(t *testing.T) {
	inner := Failure{Code: check.CodeCredential}
	if got := CodeOf(fmt.Errorf("拨号重试: %w", inner)); got != check.CodeCredential {
		t.Fatalf("CodeOf(wrapped Failure) = %q, want %q", got, check.CodeCredential)
	}
	if got := CodeOf(fmt.Errorf("外层: %w", stderrors.New("plain"))); got != "" {
		t.Fatalf("CodeOf(wrapped plain) = %q, want \"\"", got)
	}
}

// 八审 L1:"Retry-After: NaN" 旧行为会把出口冷却成 int64 最小负数 ——
// ParseFloat("NaN") 不报错,NaN 过掉 <=0 和 >=600 两个比较,int64(NaN*1000)
// 在 amd64 上是负最小值。NaN 与 ±Inf 的分界:NaN 一律当「没有提示」返回 0;
// +Inf 数值上确已越过封顶,照 1e18 同款处理给 retryAfterMaxMS。
func TestRetryAfterRejectsNaN(t *testing.T) {
	for _, h := range []string{"NaN", "nan", "-NaN", "NAN"} {
		if got := RetryAfter(h); got != 0 {
			t.Fatalf("RetryAfter(%q) = %d, want 0(NaN 不是提示,更不是负退避)", h, got)
		}
	}
	if got := RetryAfter("+Inf"); got != retryAfterMaxMS {
		t.Fatalf("RetryAfter(+Inf) = %d, want %d(越顶照封顶)", got, retryAfterMaxMS)
	}
}
