// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// DisambiguateTags 给重名 tag 消歧（2026-10-06）。
//
// 免费聚合订阅里成千上万个不同节点共用同一个名字（实测两个上游源：
// "EPODONIOS" x7207，对应 3179 个不同服务器）。Registry 以 tag 为主键，
// 同 tag 不同身份在 Merge 里会被原位覆盖 —— 整批只剩最后一个节点，
// 1.5 万节点进池只剩 214（98.6% 静默丢失）。这是"几万变几百"的主因，
// 比 unknownKeepLimit=80 和 PoolCap=8000 加起来还狠。
//
// 消歧是纯函数，不依赖池状态，跨轮稳定：
//   - 同一批里一个 tag 只对应一个身份 → 原样保留（改名/轮换凭据走 Merge 原逻辑）
//   - 一个 tag 对应多个不同身份 → 身份排序后最小的保留原 tag，
//     其余改写为 tag#<身份哈希前6位>
//
// 排序保证与订阅顺序无关：同一批节点每次算出同一个 tag 分配，健康历史不断。
// 调用点：fetchSubscriptions 在 sub.Fetch 之后、present 名单构建之前。
func DisambiguateTags(outs []Outbound) []Outbound {
	// 预扫描：tag → 该批中出现的不同身份集合
	tagIdentities := make(map[string]map[string]struct{})
	for _, o := range outs {
		if o.Tag == "" {
			continue // 无名条目 Merge 会跳过，这里原样保留
		}
		m := tagIdentities[o.Tag]
		if m == nil {
			m = make(map[string]struct{})
			tagIdentities[o.Tag] = m
		}
		m[IdentityOf(o)] = struct{}{}
	}

	out := make([]Outbound, 0, len(outs))
	for _, o := range outs {
		if o.Tag == "" {
			out = append(out, o)
			continue
		}
		ids := tagIdentities[o.Tag]
		if len(ids) <= 1 {
			out = append(out, o)
			continue
		}
		// 碰撞：身份排序，最小的保留原 tag，其余加稳定后缀
		sorted := make([]string, 0, len(ids))
		for id := range ids {
			sorted = append(sorted, id)
		}
		sort.Strings(sorted)
		if IdentityOf(o) == sorted[0] {
			out = append(out, o)
			continue
		}
		o.Tag = o.Tag + "#" + shortIdentityHash(IdentityOf(o))
		out = append(out, o)
	}
	return out
}

// shortIdentityHash 身份字符串的 6 位十六进制指纹，用于消歧后缀。
// FNV-1a 足够：输入是 IdentityOf 的长串，6 位（1677 万空间）下
// 同一批内二次碰撞概率可忽略；即使撞上也只是两个节点共享 tag，
// 退化为修复前的单点覆盖，不引入新故障模式。
func shortIdentityHash(identity string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(identity))
	return fmt.Sprintf("%06x", h.Sum32()&0xffffff)
}
