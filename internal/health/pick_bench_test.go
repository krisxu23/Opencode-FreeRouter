// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package health

import (
	"fmt"
	"testing"

	"freerouter/internal/nodeprobe"
)

// BenchmarkPickOverFullPool 是 O11/O12 的量尺。池子取现场量级(实测一轮候选
// 1700-1900 个),因为这两项的成本只在池子大了才显形:
//   - O11:选定分组全落空时,Pick 过去要把整池 rank **两遍**;
//   - O12:rankLocked 对每个候选读一次 TTFT,而那里过去每次都「拷 ≤8 个样本 +
//     排序」。
//
// Countries 故意选一个不存在的分组,逼出的正是回退路径 —— 那才是会 rank 两遍的
// 那条分支。-benchmem 的 allocs/op 是这两项最直接的证据。
func BenchmarkPickOverFullPool(b *testing.B) {
	const size = 1700
	h := NewHealth("")
	pool := make([]PoolNode, 0, size)
	for i := 0; i < size; i++ {
		tag := fmt.Sprintf("n%04d", i)
		lat := int64(20 + i%400)
		h.MarkProbe(tag, nodeprobe.ProbeResult{State: nodeprobe.StateAlive, LatencyMS: lat, LatencyMin: lat,
			ExitIP: fmt.Sprintf("10.%d.%d.%d", (i/65536)%256, (i/256)%256, i%256+1), ExitCountry: "US"})
		for s := 0; s < 5; s++ {
			h.NoteTtft(tag, int64(30+s*7+i%11))
		}
		pool = append(pool, PoolNode{Tag: tag, Country: "US"})
	}
	req := PickRequest{Countries: []string{"ZZ"}, Pool: pool}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if h.Pick(req) == nil {
			b.Fatal("Pick = nil")
		}
	}
}
