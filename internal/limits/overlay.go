// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package limits

import (
	"encoding/json"
	"math"
	"os"
	"time"

	"freerouter/internal/persistence"
)

// models.dev 限额覆盖层 —— src/limits.js 的逐字移植。
//
// 证据链(照抄原注释,已实测):/zen/v1/models 只披露 id;OpenChamber 的 Zen
// 页渲染的额度数字(「100万 上下文 · 13.1万 输出」)来自 models.dev 的
// opencode 供应商行(https://models.dev/providers/opencode),也就是
// GET https://models.dev/api.json 里的那张表。
//
// 这里的函数全是纯数据操作:抓取由组装层(app)做 —— JS 的 fetchImpl 注入
// 同一意图,Go 侧用「调用方传 payload 字节」表达。全程 fail-soft:抓取失败
// 回退陈旧磁盘缓存,连缓存都没有就返回空覆盖层,本地能力表继续服务。

// OverlayURL 是 models.dev 快照地址(src/limits.js LIMITS_URL)。
const OverlayURL = "https://models.dev/api.json"

// OverlayTTL 是覆盖层的新鲜窗口(src/limits.js LIMITS_TTL_MS)。
const OverlayTTL = 24 * time.Hour

// OverlayRow 是一个模型的覆盖层容量。指针布尔区分「上游没说」与「说了 false」:
// reasoning 跟随覆盖层,vision 只对本地表从没见过的回退行生效。
type OverlayRow struct {
	ContextWindow int64 `json:"contextWindow"`
	MaxOutput     int64 `json:"maxOutput"`
	Reasoning     *bool `json:"reasoning,omitempty"`
	Vision        *bool `json:"vision,omitempty"`
}

// overlayCache 是 data/modelsdev.json 的落盘形状(JS saveLimitsCache 的 data)。
type overlayCache struct {
	FetchedAt int64                 `json:"fetchedAt"`
	ByID      map[string]OverlayRow `json:"byId"`
	Source    string                `json:"source"`
}

// fallbackContext / fallbackOutput 是 catalog 通用回退值 —— 它们的组合是
// 「本地表从没见过这个模型」的标记,vision 覆盖只对这种行生效
// (src/limits.js FALLBACK_CONTEXT/FALLBACK_OUTPUT)。
const fallbackContext = int64(131072)
const fallbackOutput = int64(32768)

// positiveInt 移植 src/limits.js:34 的同名函数:有限且 > 0 的数就收,**小数截断**
// (Math.trunc)而不是拒绝 —— models.dev 的额度全是整数,但「拒绝 131072.5」与 JS
// 的「收下并截成 131072」会让同一行覆盖层在一侧生效、另一侧静默丢弃(任务 27
// A7 差分钉住的实错)。+Inf 超出 int64 且 JS 侧 Number.isFinite 也不收,一并排除。
func positiveInt(v float64) (int64, bool) {
	// 上界 1<<62:int64(v) 对超过 MaxInt64 的有限大数(1e30)是
	// implementation-defined 的转换,结果会进 contextWindow/maxOutput 的
	// 预算算术。NaN/±Inf 已挡,这里补最后一个角。
	if v > 0 && v < 1<<62 && !math.IsInf(v, 1) {
		return int64(v), true
	}
	return 0, false
}

// normalizeRow 移植 src/limits.js normalizeRow:limit.context/limit.output
// 都是正整数才收;reasoning/attachment 的布尔可选。
func normalizeRow(row map[string]any) (OverlayRow, bool) {
	var out OverlayRow
	limit, _ := row["limit"].(map[string]any)
	if limit == nil {
		return out, false
	}
	ctxF, _ := limit["context"].(float64)
	outF, _ := limit["output"].(float64)
	ctx, ok1 := positiveInt(ctxF)
	outN, ok2 := positiveInt(outF)
	if !ok1 || !ok2 {
		return out, false
	}
	out.ContextWindow = ctx
	out.MaxOutput = outN
	if r, ok := row["reasoning"].(bool); ok {
		out.Reasoning = &r
	}
	if a, ok := row["attachment"].(bool); ok {
		out.Vision = &a
	}
	return out, true
}

// ExtractOpencodeOverlay 从 api.json 载荷里抽 opencode 供应商的逐模型覆盖行
// (src/limits.js extractOpencodeOverlay)。形状不对返回空表,不报错。
func ExtractOpencodeOverlay(payload []byte) map[string]OverlayRow {
	var top struct {
		Opencode struct {
			Models map[string]map[string]any `json:"models"`
		} `json:"opencode"`
	}
	if err := json.Unmarshal(payload, &top); err != nil {
		return map[string]OverlayRow{}
	}
	byID := make(map[string]OverlayRow, len(top.Opencode.Models))
	for id, row := range top.Opencode.Models {
		if norm, ok := normalizeRow(row); ok {
			byID[id] = norm
		}
	}
	return byID
}

// LoadOverlayCache 读磁盘缓存(data/modelsdev.json);缺文件或坏形状返回
// (nil, 0, false) —— JS loadLimitsCache 的 null 同义。
func LoadOverlayCache(file string) (map[string]OverlayRow, int64, bool) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, 0, false
	}
	var c overlayCache
	if err := json.Unmarshal(raw, &c); err != nil || c.FetchedAt <= 0 || c.ByID == nil {
		return nil, 0, false
	}
	return c.ByID, c.FetchedAt, true
}

// SaveOverlayCache 走统一原子写入口(persistence,与 JS writeJsonFile 同款
// 紧凑格式);失败返回 error,由调用方记日志(fail-soft:缓存写不下去只影响
// 下次的 freshness,不影响本轮)。
func SaveOverlayCache(file string, byID map[string]OverlayRow, fetchedAt int64) error {
	return persistence.WriteJSONFile(file, overlayCache{FetchedAt: fetchedAt, ByID: byID, Source: "models.dev"}, false)
}

// OverlayStale 报告缓存是否已过新鲜窗口。fetchedAt==0(没有缓存)恒为陈旧。
func OverlayStale(fetchedAt int64, now time.Time) bool {
	if fetchedAt <= 0 {
		return true
	}
	// 未来时间戳（时钟回拨、另一台机器写的缓存）恒为「不陈旧」：now.Sub
	// 得到负数，永远 < OverlayTTL，那份缓存就此**永久不刷新** —— 上游改了
	// 上限也读不到。负向年龄按「年龄未知」处理，当陈旧。
	age := now.Sub(time.UnixMilli(fetchedAt))
	return age < 0 || age >= OverlayTTL
}

// fallbackMarked 报告一个目录行是否是「本地表没见过」的回退行
// (vision 覆盖只对它生效,见包注释)。
func FallbackMarked(contextWindow, maxOutput int64, vision bool) bool {
	return contextWindow == fallbackContext && maxOutput == fallbackOutput && !vision
}
