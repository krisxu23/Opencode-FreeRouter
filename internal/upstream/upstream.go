// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package upstream is the client fingerprint and the endpoint table.
//
// Every constant here was measured by live A/B against the free lane, not
// guessed. The reasoning behind each is quoted from src/upstream.js, which
// records the alternatives that were tried and refused. The facts encoded
// here were verified against the live gateway by direct request: the public
// pooled credential, the client fingerprint headers, the per-model endpoint
// split, the free-tier tool-fingerprint gate (403 FreeTierError without it)
// and the regional gate (403 RegionError). Re-verified by a live A/B matrix
// on 2026-09-30 across both wires and five real exits; two claims from the
// 2026-09-24 pass — the required tool set and the header family — did not
// survive that run, and the notes that carried them have been corrected
// rather than deleted so the next reader can see what changed and why.
package upstream

import (
	"os"
	"regexp"
	"strings"
)

// UpstreamBase is the provider root. src/upstream.js:24 reads
// OUR_FREE_MODEL_BASE so the selftest can point the adapter at a dead port
// and exercise the transport-failure path without burning the real free
// lane's quota; Go 常量读不了环境变量,该覆盖由组装层(adapter 的 Deps.Base)
// 注入,本包只提供默认值。
const UpstreamBase = "https://opencode.ai"

// BaseFromEnv 是 OUR_FREE_MODEL_BASE 的**唯一**读取口径:默认值 UpstreamBase,
// 环境变量非空则覆盖。上游层审计指出过一个真陷阱 —— 这个开关的存在理由是
// 「自测时把**全部**流量指到假上游,一行真配额都不烧」,但过去只有 catalog
// 刷新与 B 档探针经它拼 URL,adapter 的 Deps.Base 在装配时写死了常量:设了
// OUR_FREE_MODEL_BASE 的自测里,真对话回合照样打到 opencode.ai 烧车道配额,
// 开关恰好在它声称要解决的问题上失效。修复不是再加一处读取,而是让**所有**
// 读取共用这一个函数(adapter 装配、Parts.base)。
//
// 环境变量按进程生命周期看待(Load 读一次即可):runtime 改 os.Setenv 不是
// 本产品支持的操作面,而数据面每请求重读一个 getenv 是白付的开销。
func BaseFromEnv() string {
	if v := os.Getenv("OUR_FREE_MODEL_BASE"); v != "" {
		return v
	}
	return UpstreamBase
}

// ClientVersion / ClientKind 是 1.x 时代的桌面端指纹。src/upstream.js:26-52
// 记录了五个在现场被试过又被回滚的"更好"候选:平台三元组 UA(opencode2api
// 的形状)、四段式 opencode/latest/2.0.16/cli UA(取自真 v2 CLI 二进制)、
// 两段式 UA(取自 session/llm/request.ts)、v2 ses_<64hex> 会话形状、稳定的
// per-session x-opencode-request —— 每一个都在所有出口上得到
// "Console: free tier can only be used from within OpenCode",而朴素的
// opencode/1.18.31 + 1.x 会话 id 通过。其中两条值得脚注(2026-09-30 一轮
// 证明它们从来不是"值错了"):平台三元组 UA **能**过闸门,是冗余不是被拒,
// 回滚只因为它什么都买不到;v2 会话形状是真被拒。这条 lane 校验 1.x 会话
// 形状 + 一个宽松的 UA token;models.dev 与 v2 二进制描述的是一个该端点
// 不接受的客户端。
const (
	ClientVersion = "1.18.31"
	ClientKind    = "desktop"
)

// AnthropicAPIVersion is what the /zen/v1/messages wire requires.
const AnthropicAPIVersion = "2023-06-01"

// maxSessionLength / MaxToolNameLen:两个超了就会被上游直接拒绝的长度上限
// (src/upstream.js:56-57)。
const (
	maxSessionLength = 256
	MaxToolNameLen   = 128
)

// UserAgent 返回指纹 UA,尊重 OUR_FREE_MODEL_UA 覆盖(src/upstream.js:55)。
// 闸门找的是 user-agent 里任意位置的、空白分隔的
// `opencode/<2-3 段点分数字>` token(大小写不敏感);裸 opencode/1.18.31
// 是能通过的最小形式,也是本网关发送的值。
func UserAgent() string {
	if v := os.Getenv("OUR_FREE_MODEL_UA"); v != "" {
		return v
	}
	return "opencode/" + ClientVersion
}

// Wire 是一条上游线路的请求/响应形状;驱动请求编码与响应解析两条路径。
type Wire string

const (
	WireChat      Wire = "chat"
	WireResponses Wire = "responses"
	WireMessages  Wire = "messages"
)

// RESPONSES_MODELS / MESSAGES_MODELS:各端点的白名单(src/upstream.js:128、:130)。
var responsesModels = map[string]bool{
	"muse-spark-1.2-contributor-free": true,
	"muse-spark-1.3-contributor-free": true,
}

var messagesModels = map[string]bool{"union-alpha": true}

// museSparkRe 对路径最后一段做 muse-spark 家族判定。(?i) 是必须的 ——
// 家族匹配大小写不敏感;这与 SessionRe 的"小写 hex 不许加 (?i)"正好相反,
// 两处正则的要求各自独立,不要"统一"。
var museSparkRe = regexp.MustCompile(`(?i)^muse[-_]?spark(?:$|[-_:.\s])`)

// thinkingSuffixRe:剥尾部 "(level)" 思考前缀,让查找命中基础 id
// (src/upstream.js:213-215)。它只匹配**简单**括号组:嵌套串 "a(b(c))"
// 的尾部不是简单组,整体不匹配、原样返回;多个尾组 "a(b)(c)" 只剥最后
// 一层(2026-10-01 用 node 对 src/upstream.js 实测确认)。
var thinkingSuffixRe = regexp.MustCompile(`\([^()]+\)\s*$`)

// BaseModelID strips a trailing "(level)" thinking suffix so lookups hit the
// base id. 替换在原始串上做、trim 在替换后做 —— 顺序与 JS 一致
// (js :214 的 replace(...).trim()),"  muse (high)  " 才能得到 "muse"。
func BaseModelID(model string) string {
	return strings.TrimSpace(thinkingSuffixRe.ReplaceAllString(model, ""))
}

// isMuseSpark:家族判定只看 "/" 后最后一段(js :217-221)。
func isMuseSpark(modelID string) bool {
	clean := BaseModelID(modelID)
	base := clean
	if i := strings.LastIndex(clean, "/"); i >= 0 {
		base = clean[i+1:]
	}
	return museSparkRe.MatchString(base)
}

func isResponsesModel(modelID string) bool {
	return responsesModels[BaseModelID(modelID)] || isMuseSpark(modelID)
}

func isMessagesModel(modelID string) bool {
	return messagesModels[BaseModelID(modelID)]
}

// EndpointFor returns which upstream path serves this model
// (src/upstream.js:232-236):muse-spark 家族走 /zen/v1/responses,
// union-alpha 走 /zen/v1/messages,其余一律 /zen/v1/chat/completions。
func EndpointFor(modelID string) string {
	if isResponsesModel(modelID) {
		return "/zen/v1/responses"
	}
	if isMessagesModel(modelID) {
		return "/zen/v1/messages"
	}
	return "/zen/v1/chat/completions"
}

// WireFor returns the wire shape one endpoint speaks; drives request encoding
// and response parsing (src/upstream.js:239-244)。路径→线路的映射以端点为
// 唯一事实源,判定逻辑不重复写第二份。
func WireFor(modelID string) Wire {
	switch EndpointFor(modelID) {
	case "/zen/v1/responses":
		return WireResponses
	case "/zen/v1/messages":
		return WireMessages
	default:
		return WireChat
	}
}
