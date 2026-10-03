// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package catalog 回答「这个网关能报出哪些模型,它们各能做什么」(移植自
// src/catalog.js,JS 源是行为的最终事实)。三个来源分层叠放,任何单个来源
// 失效都拖不垮网关:
//
//  1. 上游列表本身(/zen/v1/models)—— 网关当前会报出的 id 权威集合;
//  2. 一张本地审核过的能力表(上下文窗口/视觉/推理),因为上游列表只
//     披露一个 id,其余一概不说;
//  3. models.dev 的限额 overlay(src/limits.js)—— 由调用方套用,不在
//     本包,这样 catalog 才能离线可用。
//
// 实测事故:「模型列表清空」—— 上游一次抖动曾让整个目录清空,用户以为
// 没额度。所以 Refresh 失败/空列表一律回落 Static() 且 err 为 nil。
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"unicode"

	"freerouter/internal/upstream"
)

// alwaysFree 是不带 -free 后缀也属于免费车道的固定 id(用户实测确认)。
var alwaysFree = map[string]bool{
	"union-alpha":      true,
	"space-bunny-free": true,
	"big-pickle":       true,
}

// Capabilities 是本地基线表的一行。数值跟踪的是 models.dev 的 *免费车道*
// 行,不是 canonical 行:免费车道行携带的是车道配额(mimo-v2.6-flash-free
// 是 200000/32000,canonical id 是 1048576/131072)。照抄 canonical 数字是
// 一次 5 倍的静默超卖,只会在上游截断时才现形。
//
// Match 是本行的锚定正则,也是表的索引;表按 specific-first 排序,
// generic 行排在后面,否则会提前遮蔽 specific 行。
type Capabilities struct {
	Vision             bool
	Reasoning          bool
	ContextWindow      int64
	MaxOutput          int64
	CanDisableThinking bool
	Match              *regexp.Regexp
}

// capabilitiesTable 是 17 行基线表(src/catalog.js:37-55)。JS 用
// undefined/false 二态表达 canDisableThinking/reasoning 的「未声明」与
// 「声明为否」;Go 的 bool 没有三态,加载时把 undefined 归一成 true,
// 与 buildCatalog 的 `!== false` 行为完全一致。
var capabilitiesTable = []Capabilities{
	// longcat:2026-10-02 dsh-review 实测 longcat-2.5-preview-free 带图请求
	// 返回 500 —— 本地表过去写 Vision:true,models.dev overlay 对见过的行
	// 不覆盖 vision 位,错值永不自愈,按实测改掉(整个 ^longcat 前缀收口)。
	{Match: regexp.MustCompile(`^longcat`), Vision: false, Reasoning: true, ContextWindow: 1000000, MaxOutput: 131072, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^space.?bunny`), Vision: true, Reasoning: true, ContextWindow: 1048576, MaxOutput: 524288, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^mimo.*v2\.6`), Vision: true, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: false},
	{Match: regexp.MustCompile(`^mimo.*v2\.5`), Vision: true, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: false},
	{Match: regexp.MustCompile(`^mimo`), Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 65536, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^muse.?spark`), Vision: true, Reasoning: true, ContextWindow: 1048576, MaxOutput: 131072, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^nemotron.*3\.5.*lightning`), Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 262144, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^nemotron.*ultra`), Vision: false, Reasoning: true, ContextWindow: 1000000, MaxOutput: 128000, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^nemotron`), Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 128000, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^ling`), Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 32768, CanDisableThinking: true},
	// big-pickle:2026-10-02 dsh-review 实测带图请求返回 200 —— models.dev
	// 漏报、本地表过去写 Vision:false;实测优先(overlay 永远不会修正这一位)。
	{Match: regexp.MustCompile(`^big.?pickle`), Vision: true, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^union`), Vision: true, Reasoning: false, ContextWindow: 262144, MaxOutput: 131072, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^deepseek`), Vision: false, Reasoning: true, ContextWindow: 200000, MaxOutput: 128000, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^kimi`), Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 262144, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^qwen`), Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 65536, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^glm`), Vision: false, Reasoning: true, ContextWindow: 204800, MaxOutput: 131072, CanDisableThinking: true},
	{Match: regexp.MustCompile(`^jev`), Vision: false, Reasoning: false, ContextWindow: 32768, MaxOutput: 4096, CanDisableThinking: true},
}

// displayNames 是人工校对过的展示名,原始上游 id 不该直接出现在选择器里
// (src/catalog.js:58-76)。
var displayNames = map[string]string{
	"mimo-v2.6-flash-free":            "MiMo V2.6 Flash",
	"mimo-v2.5-free":                  "MiMo V2.5",
	"mimo-v2-pro-free":                "MiMo V2 Pro",
	"muse-spark-1.3-contributor-free": "Muse Spark 1.3",
	"muse-spark-1.2-contributor-free": "Muse Spark 1.2",
	"nemotron-3-ultra-free":           "Nemotron 3 Ultra",
	"nemotron-3.5-lightning-free":     "Nemotron 3.5 Lightning",
	"ling-3.0-flash-fin-free":         "Ling 3.0 Flash Fin",
	"space-bunny-free":                "Space Bunny",
	"union-alpha":                     "Union Alpha",
	"deepseek-v4-flash-free":          "DeepSeek V4 Flash",
	"longcat-2.5-preview-free":        "LongCat 2.5 Preview",
	"longcat-2.0-free":                "LongCat 2.0",
	"kimi-k2.5-free":                  "Kimi K2.5",
	"qwen3.6-plus-free":               "Qwen 3.6 Plus",
	"glm-5-free":                      "GLM 5",
	"jev-1.13-free":                   "Jev 1.13",
}

// regionSensitiveRe 是已知地区依赖出口的 id 家族。
var regionSensitiveRe = regexp.MustCompile(`^muse.?spark`)

// freeLaneRe 判定一个 base id 是否落在免密车道。网关的列表混着付费与免费
// id,只有这些不用 per-user key 就能答。
var freeLaneRe = regexp.MustCompile(`(?:^|[-_])free(?:$|[-_.])`)

// measuredDead 是「实测不可达 denylist」:正则管**纳新**(上游新增的 -free id
// 自动进目录),这张表管**摘除** —— 被实测判死的 id 不再发给用户。staticIDs
// 里的条目若进了这张表同样被跳过(目录统一在 Build 入口过滤)。摘除要带
// 日期与证据来源;上游恢复了就把行删掉,让它经正则/静态表自然回来。
var measuredDead = map[string]string{
	// 2026-10-02 实测(dsh harness 仓库,双通道):/v1/messages 对匿名与付费
	// key 都 500、chat 端点 404 —— 上游已把这条车道整体摘除。
	"ling-3.0-flash-fin-free": "2026-10-02 实测双通道皆死(messages 500 / chat 404)",
}

// IsMeasuredDead 报告一个 base id 是否在实测 denylist 里。
func IsMeasuredDead(id string) bool {
	_, ok := measuredDead[id]
	return ok
}

// IsFreeLane 报告这个 id 是否免密车道:ALWAYS_FREE 固定免费,或 base id
// 带 free 词元(前后必须是边界,「freemodel」不算)。
func IsFreeLane(modelID string) bool {
	base := upstream.BaseModelID(modelID)
	if alwaysFree[base] {
		return true
	}
	return freeLaneRe.MatchString(base)
}

// IsRegionSensitive 报告这个 id 的地区可用性是否依赖出口。
func IsRegionSensitive(modelID string) bool {
	return regionSensitiveRe.MatchString(upstream.BaseModelID(modelID))
}

// CapabilitiesFor 查一个 base id 的基线能力;缺行回落到「未知模型」的
// 通用值(src/catalog.js:95)。
func CapabilitiesFor(modelID string) Capabilities {
	base := upstream.BaseModelID(modelID)
	for _, row := range capabilitiesTable {
		if row.Match.MatchString(base) {
			return row
		}
	}
	return Capabilities{Vision: false, Reasoning: true, ContextWindow: 131072, MaxOutput: 32768, CanDisableThinking: true}
}

var (
	sepRunsRe    = regexp.MustCompile(`[-_.]+`)
	digitSpaceRe = regexp.MustCompile(`(\d)\s+`)
)

// DisplayModelName 把裸上游 id 变成选择器能展示的名字:已知名直接命中,
// 未知 id 走 title-case,数字开头的词保持原形(JS /^\d/ 分支)。
func DisplayModelName(modelID string) string {
	base := upstream.BaseModelID(modelID)
	if known, ok := displayNames[base]; ok {
		return known
	}
	flat := digitSpaceRe.ReplaceAllString(sepRunsRe.ReplaceAllString(base, " "), "$1 ")
	flat = strings.TrimSpace(flat)
	if flat == "" {
		return ""
	}
	words := strings.Split(flat, " ")
	for i, w := range words {
		r := []rune(w)
		if len(r) == 0 {
			continue
		}
		// JS 的 \d 是 ASCII 数字:数字开头的词原样保留,不做首字母大写。
		if r[0] >= '0' && r[0] <= '9' {
			continue
		}
		r[0] = unicode.ToUpper(r[0])
		words[i] = string(r)
	}
	return strings.Join(words, " ")
}

// staticIDs 是离线/首启动回落名单。上游一次抖动不能让目录清空(实测事故:
// 目录清空 → 用户以为没额度),这份名单保证 Static() 永远有货。
var staticIDs = []string{
	"union-alpha", "space-bunny-free", "big-pickle", // ALWAYS_FREE 三件套先列
	"mimo-v2.6-flash-free", "mimo-v2.5-free", "mimo-v2-pro-free",
	"muse-spark-1.3-contributor-free", "muse-spark-1.2-contributor-free",
	"nemotron-3-ultra-free", "nemotron-3.5-lightning-free",
	"ling-3.0-flash-fin-free", "deepseek-v4-flash-free",
	"longcat-2.5-preview-free", "longcat-2.0-free",
	"kimi-k2.5-free", "qwen3.6-plus-free", "glm-5-free", "jev-1.13-free",
}

// Model 是一条目录项,按 listing 顺序排列。
type Model struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Wire 取自适配器路由用的同一个函数,所以目录项不可能声称一条请求
	// 实际不会走的线路。union-alpha 说的是 Anthropic messages 形状,而裸的
	// isResponsesModel() 测出的是 "chat" —— 从路由侧推导才是全部意义所在。
	Wire               upstream.Wire `json:"wire"`
	Vision             bool          `json:"vision"`
	Reasoning          bool          `json:"reasoning"`
	ContextWindow      int64         `json:"context_window"`
	MaxOutput          int64         `json:"max_output"`
	CanDisableThinking bool          `json:"can_disable_thinking"`
	RegionSensitive    bool          `json:"region_sensitive"`
}

// Static 返回离线回落目录:上游没答上号时,网关依然有模型可报。
func Static() []Model {
	return Build(staticIDs)
}

// Build 把上游 id 列表与本地能力表合并成目录项,保持 listing 顺序。
// 非免费 id 跳过;同一 base id 的多个免费变体(裸 id、"(high)" 思考后缀)
// 只留一个 —— 重复列出会让面板出现两行同模型。
func Build(ids []string) []Model {
	seen := make(map[string]bool, len(ids))
	entries := make([]Model, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" || !IsFreeLane(id) || IsMeasuredDead(id) {
			continue
		}
		base := upstream.BaseModelID(id)
		if seen[base] {
			continue
		}
		seen[base] = true
		caps := CapabilitiesFor(base)
		// number(caps.contextWindow) ?? 131072 的同款守卫:表里不会出现
		// 非正值,但守卫留在原地,防未来某行手滑写 0 时静默超卖。
		cw := caps.ContextWindow
		if cw <= 0 {
			cw = 131072
		}
		mo := caps.MaxOutput
		if mo <= 0 {
			mo = 32768
		}
		entries = append(entries, Model{
			ID:                 base,
			Name:               DisplayModelName(base),
			Wire:               upstream.WireFor(base),
			Vision:             caps.Vision,
			Reasoning:          caps.Reasoning,
			ContextWindow:      cw,
			MaxOutput:          mo,
			CanDisableThinking: caps.CanDisableThinking,
			RegionSensitive:    IsRegionSensitive(base),
		})
	}
	return entries
}

// ParseListing 解析网关的模型列表,依次尝试 {"data":[…]}、{"models":[…]}、
// 顶层数组三种形状(src/catalog.js:159-160)。行可以是字符串或 {"id":…}
// 对象;非字符串 id、空 id 一律过滤。坏 JSON 返回 error;形状都不匹配
// 返回空列表(与 JS 的行为一致,不是错误)。
func ParseListing(raw []byte) ([]string, error) {
	var payload any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("catalog: 解析模型列表: %w", err)
	}
	var rows []any
	switch v := payload.(type) {
	case map[string]any:
		if d, ok := v["data"].([]any); ok {
			rows = d
		} else if m, ok := v["models"].([]any); ok {
			rows = m
		}
	case []any:
		rows = v
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		switch v := row.(type) {
		case string:
			if v != "" {
				out = append(out, v)
			}
		case map[string]any:
			if id, ok := v["id"].(string); ok && id != "" {
				out = append(out, id)
			}
		}
	}
	return out, nil
}

// Refresh 打一次 GET {base}/models,ParseListing 后 Build。失败或空列表
// 返回 Static() 且 err 为 nil —— 上游一次抖动不该让整个目录清空(当年就是
// 这样退化成「模型列表为空 → 用户以为没额度」),回落不是错误,是承诺。
// client 为 nil 时用 http.DefaultClient;走哪个出口由调用方的 client 决定。
func Refresh(ctx context.Context, client *http.Client, base string) ([]Model, error) {
	if client == nil {
		client = http.DefaultClient
	}
	url := strings.TrimRight(base, "/") + "/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Static(), nil
	}
	req.Header.Set("accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return Static(), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Static(), nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return Static(), nil
	}
	ids, err := ParseListing(body)
	if err != nil || len(ids) == 0 {
		return Static(), nil
	}
	return Build(ids), nil
}
