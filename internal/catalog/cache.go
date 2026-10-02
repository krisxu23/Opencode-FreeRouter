// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// 模型目录的磁盘缓存。总纲 §5 的文件清单漏了这一份,差分验收 B5 才暴露:
// Go 冷启动只播 Static(),而 JS 播的是上一轮存下的真实列表 —— 重启后的
// /v1/models 一边 34 条、一边 18 条。JS 的对应实现是 src/index.js:219
// (路径)、:234-246(读)、:249(写)。
package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"freerouter/internal/logger"
	"freerouter/internal/persistence"
)

// cacheTTL 之后缓存不再"新鲜",但仍然照用。
//
// 过期不丢弃是实测结论而非宽容:这台机器直连拉不通上游(Caddy/防火墙挡了
// opencode.ai),丢掉过期缓存就等于丢掉整个模型列表,而 CAPABILITIES 兜底表
// 只能补元数据、补不出 id —— 兜底表列不出上游才有模型的名字。
const cacheTTL = 7 * 24 * time.Hour

// cacheFile 的键名与 JS 写的逐字一致:同一份文件两版都要读,用户也会手改。
type cacheFile struct {
	FetchedAt int64    `json:"fetchedAt"`
	IDs       []string `json:"ids"`
}

// LoadCache 返回缓存里的 id 列表;没有可用缓存(缺文件、坏 JSON、ids 不是
// 数组或为空)返回 nil,由调用方回落 Static()。
func LoadCache(file string) []string {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	// ids 里混进非字符串时 JS 是逐元素 filter(src/index.js:238),所以这里
	// 不能直接用 []string —— 一个数字就会让 json.Unmarshal 整体失败,把一份
	// 本来可用的列表判成没有缓存。
	var shell struct {
		FetchedAt int64             `json:"fetchedAt"`
		IDs       []json.RawMessage `json:"ids"`
	}
	if err := json.Unmarshal(raw, &shell); err != nil || shell.IDs == nil {
		return nil
	}
	ids := make([]string, 0, len(shell.IDs))
	for _, r := range shell.IDs {
		var s string
		if json.Unmarshal(r, &s) == nil && s != "" {
			ids = append(ids, s)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if age := time.Since(time.UnixMilli(shell.FetchedAt)); shell.FetchedAt > 0 && age > cacheTTL {
		logger.Warn(fmt.Sprintf("目录缓存已过期（%d 小时前写入），先按缓存展示，等本轮上游刷新覆盖", int(age.Hours())))
	}
	return ids
}

// SaveCache 写回上游刚报回来的 id 列表,格式紧凑(与 JS 的 writeJsonFile 同款)。
func SaveCache(file string, ids []string, fetchedAt int64) error {
	return persistence.WriteJSONFile(file, cacheFile{FetchedAt: fetchedAt, IDs: ids}, false)
}
