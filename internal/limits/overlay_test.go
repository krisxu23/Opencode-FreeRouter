// SPDX-License-Identifier: GPL-3.0-or-later
package limits

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sampleAPI = `{"opencode":{"models":{
	"mimo-v2.6-flash-free":{"limit":{"context":1000000,"output":131072},"reasoning":true,"attachment":true},
	"glm-5-free":{"limit":{"context":204800,"output":131072}},
	"paid-canonical":{"limit":{"context":1,"output":1}}
}}}`

func TestExtractOpencodeOverlayKeepsOnlyCompleteRows(t *testing.T) {
	byID := ExtractOpencodeOverlay([]byte(sampleAPI))
	if len(byID) != 3 {
		t.Fatalf("rows = %d, want 3", len(byID))
	}
	m := byID["mimo-v2.6-flash-free"]
	if m.ContextWindow != 1000000 || m.MaxOutput != 131072 {
		t.Fatalf("mimo = %+v", m)
	}
	if m.Reasoning == nil || !*m.Reasoning || m.Vision == nil || !*m.Vision {
		t.Fatalf("mimo bools = %+v", m)
	}
	// limit 缺一个维度的行必须被丢(fail-closed 的另一面:半截数据比没有危险)。
	byID = ExtractOpencodeOverlay([]byte(`{"opencode":{"models":{"half":{"limit":{"context":10}}}}}`))
	if len(byID) != 0 {
		t.Fatalf("half row kept: %+v", byID)
	}
}

func TestOverlayCacheRoundTripAndTTL(t *testing.T) {
	file := filepath.Join(t.TempDir(), "modelsdev.json")
	if _, _, ok := LoadOverlayCache(file); ok {
		t.Fatal("missing cache must not load")
	}
	now := time.Now()
	no := false
	byID := map[string]OverlayRow{"a": {ContextWindow: 10, MaxOutput: 5, Reasoning: &no}}
	if err := SaveOverlayCache(file, byID, now.UnixMilli()); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, fetchedAt, ok := LoadOverlayCache(file)
	if !ok || got["a"].ContextWindow != 10 || *got["a"].Reasoning {
		t.Fatalf("roundtrip = %+v ok %v", got, ok)
	}
	if OverlayStale(fetchedAt, now.Add(time.Hour)) {
		t.Fatal("1h old cache must be fresh (TTL 24h)")
	}
	if !OverlayStale(fetchedAt, now.Add(25*time.Hour)) {
		t.Fatal("25h old cache must be stale")
	}
	if !OverlayStale(0, now) {
		t.Fatal("no cache must count as stale")
	}
	// 坏形状:缺 fetchedAt 视为无缓存。
	_ = os.WriteFile(file, []byte(`{"byId":{}}`), 0o644)
	if _, _, ok := LoadOverlayCache(file); ok {
		t.Fatal("corrupt cache must not load")
	}
}

// TestExtractTruncatesFractionalLimits 钉住 positiveInt 的 JS 语义:有限且 > 0
// 的小数**截断收下**(src/limits.js:34 的 Math.trunc),不是拒绝 —— models.dev 的
// 额度本该全是整数,但两版在这一条上分道扬镳时,同一行覆盖层会一侧生效一侧
// 静默丢弃(任务 27 A7 差分钉住的实错)。
func TestExtractTruncatesFractionalLimits(t *testing.T) {
	byID := ExtractOpencodeOverlay([]byte(`{"opencode":{"models":{"f":{"limit":{"context":131072.5,"output":32768.9}}}}}`))
	row, ok := byID["f"]
	if !ok {
		t.Fatalf("fractional limits must be kept (truncated), got %v", byID)
	}
	if row.ContextWindow != 131072 || row.MaxOutput != 32768 {
		t.Fatalf("row = %+v, want 131072/32768", row)
	}
	byID = ExtractOpencodeOverlay([]byte(`{"opencode":{"models":{"z":{"limit":{"context":0,"output":-3}}}}}`))
	if len(byID) != 0 {
		t.Fatalf("non-positive limits must be dropped, got %v", byID)
	}
}
