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
	regionRe = regexp.MustCompile(`(?i)not available in your country|region`)
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
	case typ == "RegionError" || regionRe.MatchString(flat):
		return Failure{Code: check.CodeRegion, Status: status, Type: typ, Message: msg, Retryable: true}

	// 配额分支必须在凭证分支之前:文案是 "usage limit" 的 403 是配额拒绝,
	// 换出口恰恰是正确的应对(src/errors.js:47)。
	case status == 429 || typ == "FreeUsageLimitError" || quotaRe.MatchString(flat):
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

	default:
		return Failure{Code: check.CodeServer, Status: status, Type: typ, Message: msg, Retryable: status >= 500}
	}
}

// RetryAfter 把 Retry-After 头解析成毫秒。只认数字形式 —— 这家供应商从不发
// HTTP-date 形式。非正数返回 0(不是 -1),所有调用方统一用 "> 0" 判断。
func RetryAfter(h string) int64 {
	secs, err := strconv.ParseFloat(h, 64)
	if err != nil || secs <= 0 {
		return 0
	}
	return int64(secs * 1000)
}
