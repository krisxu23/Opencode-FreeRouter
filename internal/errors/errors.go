// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package errors 把一次上游失败归类进网关的共享失败词汇表(移植自
// src/errors.js,该文件是从上游 src/http.js 逐字抽出的,分类顺序是最终事实)。
//
// 三个专用类型的语义:
//   - RegionError —— 模型还在,但当前出口国家被排除。换出口立刻恢复,
//     所以它喂地区矩阵,而不是原地重试。
//   - FreeUsageLimitError / 429 —— 每会话配额耗尽;换新会话重试只会更糟。
//   - ModelError / "Model is unavailable" —— 池内账号整体不再路由该模型。
//
// 分类顺序是承重墙(实测事故):429/配额分支必须排在 403-FreeTier 分支之前,
// 也必须排在 401/403 凭证分支之前。顺序一换,带 "usage limit" 文案的 403 会
// 从「换出口重试」退化成「扫全池的凭证慢失败」。
package errors

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"freerouter/internal/check"
)

// Failure 是一次归类完成的上游拒绝。Unavailable 与 Retryable 是两个独立的
// 位:供应商不再服务的模型是服务端事实,轮换不能把它当成「换个出口」的理由。
type Failure struct {
	Code         string
	Status       int
	Type         string
	Message      string
	RetryAfterMS int64
	Unavailable  bool
	Retryable    bool
}

// Error 让 Failure 能以 error 值的身份在调用链上传播。
func (f Failure) Error() string { return f.Code + ": " + f.Message }

// CodeOf 返回 err 携带的失败码;err 不是 Failure 时返回 ""。每个决定是否
// 轮换的层都会调它,所以必须容忍普通 error(取消、拨号失败)而不是 panic。
func CodeOf(err error) string {
	if f, ok := err.(Failure); ok {
		return f.Code
	}
	return ""
}

var (
	// regionRe 只认**明确**的地区拒绝文案。这是对 JS 的一处有意收紧:
	// 原来这里还有一个裸词 `region`,它匹配任何文案里出现 region / regional /
	// regions 的响应 —— 一句 500 的 "regional datacenter issue" 就被判成
	// REGION;而 REGION 在 engine 里除了轮换还会调 NoteRegionError,把该出口的
	// B 档凭证**无条件打回 A**。B 是全池唯一的「门控模型可用」证据
	// (GatedUsable),一次误判就让 muse-spark 从 /v1/models 上消失、gated
	// 请求全部报「无健康出口」,且只能等下一轮粗探(≥5 分钟)自愈。
	// 结构化的 type == "RegionError" 仍是首选判据(任何状态码都认),文案判据
	// 只保留完整短语,并在 Classify 里额外要求 4xx。
	regionRe = regexp.MustCompile(`(?i)not available in your country|not available in (?:this|your) region|unsupported region`)
	quotaRe  = regexp.MustCompile(`(?i)usage limit|rate limit`)
	freeRe   = regexp.MustCompile(`(?i)freetier|free.?tier`)
	modelRe  = regexp.MustCompile(`(?i)model is unavailable|not supported`)
)

// Classify 把 HTTP 状态码和响应体映射成一个 Failure。
//
// 信封的两种形状都要认(src/errors.js:40 的 `payload?.error ?? payload`):
// OpenAI 形状套在 error 键里,而上游偶发的裸错误对象({type,message} 直接在
// 顶层)走 `?? payload` 兜底 —— Go 版少这一层兜底时,同样的响应体会从
// REGION/quota 掉进 default 的 server 分类,轮换决策随之漂移(任务 27 差分
// 前夜对照 JS 源码抓到)。
func Classify(status int, body []byte, retryAfterMS int64) Failure {
	var env struct {
		Error *struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	// 非 JSON 响应体(边缘代理吐的 HTML 页)很常见,不能因此改变分类。
	_ = json.Unmarshal(body, &env)

	typ, msg := "", ""
	if env.Error != nil {
		typ, msg = env.Error.Type, env.Error.Message
	} else {
		typ, msg = env.Type, env.Message
	}
	if msg == "" {
		msg = "upstream HTTP " + strconv.Itoa(status)
	}
	flat := strings.ToLower(msg)

	switch {
	// 显式的 type == "RegionError" 是结构信号,任何状态码都认;文案判据必须
	// 配 4xx —— 5xx 文案里提到地区是供应商自身的基础设施问题,与本出口的
	// 国家无关,把它算成地区拒绝会误伤 B 档凭证(见 regionRe 注释)。
	case typ == "RegionError" || (status >= 400 && status < 500 && regionRe.MatchString(flat)):
		return Failure{Code: check.CodeRegion, Status: status, Type: typ, Message: msg, Retryable: true}

	// 配额分支必须在凭证分支之前:文案是 "usage limit" 的 403 是配额拒绝,
	// 换出口恰恰是正确的应对(src/errors.js:47)。
	case status == 429 || typ == "FreeUsageLimitError" || typ == "GoUsageLimitError" ||
		status == 402 || quotaRe.MatchString(flat) ||
		strings.Contains(flat, "insufficient funds") || strings.Contains(flat, "insufficient quota") ||
		strings.Contains(flat, "insufficient credits"):
		// GoUsageLimitError 是配额分支的第二个类型名(dsh harness 仓库 2026-10
		// 实测:它以 403 且文案不含 "usage limit" 的形状出现,漏认会掉进下面的
		// 凭证分支变成不可重试的慢失败;自带 retry-after ≈600s)。402 与
		// "insufficient funds" 是上游账户侧的计费拒绝(同仓库实测信封),对
		// 网关语义同样是「这个出口的配额没了」—— 换出口重试,不冷却节点。
		return Failure{Code: check.CodeQuota, Status: status, Type: typ, Message: msg, RetryAfterMS: retryAfterMS, Retryable: true}

	// 故意限定 status == 403(src/errors.js:50-57 的两个限定都不可选):
	//   - 401 是明确的凭证问题,把它降级成「可重试」会让一个配置错误变成
	//     扫全池的慢失败。
	//   - 上面的配额分支已经拦下了带 "usage limit"/"rate limit" 文案的 403,
	//     这里补的是 type=FreeTierError 而文案不匹配的情况(upstream.js 记录
	//     过这条车道的 FreeTierError 实测)。
	case status == 403 && freeRe.MatchString(typ+" "+msg):
		return Failure{Code: check.CodeQuota, Status: status, Type: typ, Message: msg, RetryAfterMS: retryAfterMS, Retryable: true}

	case status == 401 || status == 403:
		return Failure{Code: check.CodeCredential, Status: status, Type: typ, Message: msg}

	case typ == "ModelError" || modelRe.MatchString(flat):
		return Failure{Code: check.CodeServer, Status: status, Type: typ, Message: msg, Unavailable: true}

	// R8: Retry-After 提示必须在这一支也带上。从前只有配额两支设 RetryAfterMS,
	// 而唯一的消费者是 engine 的 `if cooldownOn[code] { NoteCooldown(…, RetryAfterMS) }`,
	// 其中 cooldownOn = {transport, timeout} —— 于是「5xx + Retry-After」这条真实
	// 存在的组合永远传不到 NoteCooldown,health 里的 `if retryAfterMS > 0` 成了死分支。
	// 现在 5xx(以及其它落到 default 的可重试码)能把供应商的退避提示带到 engine。
	default:
		return Failure{Code: check.CodeServer, Status: status, Type: typ, Message: msg, RetryAfterMS: retryAfterMS, Retryable: status >= 500}
	}
}

// RetryAfter 把 Retry-After 头解析成毫秒。只认数字形式 —— 这家供应商从不发
// HTTP-date 形式。非正数返回 0(不是 -1),所有调用方统一用 "> 0" 判断。
//
// 提示值封顶在 retryAfterMaxMS:NoteCooldown 对 hinted 退避不做二次截断,一个
// 行为异常(或被劫持)的出口回 `Retry-After: 1e18` 就能把该出口冷却到
// 「实质永久」且永不自愈。10 分钟与一次探测窗口同量级,足够让上游的退避意图
// 生效,又保证出口总有机会被重新实测。
const (
	retryAfterMaxSecs = 600
	retryAfterMaxMS   = retryAfterMaxSecs * 1000
)

func RetryAfter(h string) int64 {
	secs, err := strconv.ParseFloat(h, 64)
	if err != nil || secs <= 0 {
		return 0
	}
	if secs >= retryAfterMaxSecs {
		return retryAfterMaxMS
	}
	return int64(secs * 1000)
}
