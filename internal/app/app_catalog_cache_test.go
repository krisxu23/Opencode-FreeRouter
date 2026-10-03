// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"freerouter/internal/catalog"
	"freerouter/internal/logger"
)

// 目录基线（data/catalog-ids.json）。2026-10-03 之前它是冷启动的目录来源;
// 之后降级为「上次运行见过的 id」:启动时装进 baselineIDs 仅供换代对比,
// 目录本身恒播静态表(目录 id 的唯一来源是上游实时列表)。

// seedCatalogBaseline 写一份基线并返回其 fetchedAt。
func seedCatalogBaseline(t *testing.T, root string, ids []string) int64 {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatalf("建数据目录: %v", err)
	}
	seededAt := time.Now().Add(-48 * time.Hour).UnixMilli()
	body, err := json.Marshal(struct {
		FetchedAt int64    `json:"fetchedAt"`
		IDs       []string `json:"ids"`
	}{FetchedAt: seededAt, IDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "data", "catalog-ids.json"), body, 0o644); err != nil {
		t.Fatalf("写基线: %v", err)
	}
	return seededAt
}

// readCatalogBaseline 读回基线的 fetchedAt 与 ids(形状仍与 JS 同名同形)。
func readCatalogBaseline(t *testing.T, root string) (int64, []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "data", "catalog-ids.json"))
	if err != nil {
		t.Fatalf("读目录基线: %v", err)
	}
	var j struct {
		FetchedAt float64  `json:"fetchedAt"`
		IDs       []string `json:"ids"`
	}
	if err := json.Unmarshal(raw, &j); err != nil {
		t.Fatalf("基线不是 JSON: %v (%s)", err, raw)
	}
	return int64(j.FetchedAt), j.IDs
}

// TestBootIgnoresBaselineForCatalog:被毒化的缓存(models.dev 快照曾写进
// 上游已下架的 id)不能污染启动目录 —— 冷启动恒播静态表,基线只进对比账本。
func TestBootIgnoresBaselineForCatalog(t *testing.T) {
	logger.Init("")
	root := t.TempDir()
	seededAt := seedCatalogBaseline(t, root, []string{"cached-model-free", "ling-2.6-flash-free"})

	// 端口走一次性分配(见 app_test.go 的 loadWithPorts):这条验的是目录装载,
	// 让它去抢默认的 3457/3458 只会平白沾上端口重绑的 flake。
	parts, err := loadWithPorts(t, root, nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer func() { _ = parts.Shutdown(context.Background()) }()

	list := parts.catalog.get()
	static := catalog.Static()
	if len(list) != len(static) {
		t.Fatalf("冷启动目录 %d 行, want 静态表 %d 行(基线不得作目录来源)", len(list), len(static))
	}
	for i := range list {
		if list[i].ID != static[i].ID {
			t.Fatalf("冷启动目录第 %d 行 = %s, want 静态表 %s", i, list[i].ID, static[i].ID)
		}
	}

	// 基线进了对比账本,且启动不回写(只有换代才落盘,fetchedAt 保持旧值)。
	if len(parts.baselineIDs) != 2 || parts.baselineIDs[0] != "cached-model-free" {
		t.Fatalf("baselineIDs = %v, want 载入的 2 个 id", parts.baselineIDs)
	}
	if got, ids := readCatalogBaseline(t, root); got != seededAt || len(ids) != 2 {
		t.Fatalf("启动不应重写基线: fetchedAt=%v ids=%v, want %v", got, ids, seededAt)
	}
}

// TestApplyIDsPersistsBaseline:任何一次换代都把 id 列表落盘,下次启动才有
// diff 基准。形状与 JS 的 writeJsonFile 逐字一致(紧凑、fetchedAt 在前)。
func TestApplyIDsPersistsBaseline(t *testing.T) {
	p := newProbeParts(t, 0)
	p.applyIDs([]string{"written-model", "second-model"}, "test")

	got, ids := readCatalogBaseline(t, p.Root)
	if got <= 0 {
		t.Fatalf("fetchedAt = %v, want 正数毫秒时间戳", got)
	}
	if len(ids) != 2 || ids[0] != "written-model" || ids[1] != "second-model" {
		t.Fatalf("ids = %v, want [written-model second-model]", ids)
	}
	raw, err := os.ReadFile(filepath.Join(p.Root, "data", "catalog-ids.json"))
	if err != nil {
		t.Fatalf("读基线: %v", err)
	}
	// JS 的 writeJsonFile 默认 indent=false → 无空格、键序按字面量。
	head := `{"fetchedAt":`
	if len(raw) < len(head) || string(raw[:len(head)]) != head {
		t.Fatalf("基线开头 = %q, want 紧凑 JSON 以 %q 起", string(raw[:len(head)]), head)
	}
	for _, b := range raw {
		if b == '\n' {
			t.Fatalf("基线不该是缩进 JSON: %s", raw)
		}
	}
}

// TestApplyIDsLogsAdditionsAndRemovals:换代时对着上一份基线打增删对比日志。
func TestApplyIDsLogsAdditionsAndRemovals(t *testing.T) {
	p := newProbeParts(t, 0)
	p.applyIDs([]string{"keep-free", "old-free"}, "test") // 建立基线
	p.baselineMu.Lock()
	prev := append([]string(nil), p.baselineIDs...)
	p.baselineMu.Unlock()
	if len(prev) != 2 {
		t.Fatalf("换代后 baselineIDs = %v, want 2 个", prev)
	}
	// 第二代:摘掉 old-free,新增 new-free;keep-free 不动。
	p.applyIDs([]string{"keep-free", "new-free"}, "test")
	added, removed := diffIDs([]string{"keep-free", "old-free"}, []string{"keep-free", "new-free"})
	if len(added) != 1 || added[0] != "new-free" || len(removed) != 1 || removed[0] != "old-free" {
		t.Fatalf("diffIDs = +%v -%v, want +[new-free] -[old-free]", added, removed)
	}
}

// TestDiffIDsOrderAndDedup:diff 结果保持输入首现顺序、列表内重复按一次计。
func TestDiffIDsOrderAndDedup(t *testing.T) {
	added, removed := diffIDs(
		[]string{"a-free", "b-free", "a-free", "c-free"},
		[]string{"c-free", "d-free", "c-free", "b-free"},
	)
	wantAdded := []string{"d-free"}
	wantRemoved := []string{"a-free"}
	if fmt.Sprint(added) != fmt.Sprint(wantAdded) {
		t.Fatalf("added = %v, want %v", added, wantAdded)
	}
	if fmt.Sprint(removed) != fmt.Sprint(wantRemoved) {
		t.Fatalf("removed = %v, want %v", removed, wantRemoved)
	}
	if added, removed = diffIDs(nil, nil); added != nil || removed != nil {
		t.Fatalf("空对空应得 nil, got +%v -%v", added, removed)
	}
}

// TestSummarizeIDsCapsTheList:diff 列表超过 8 个时压缩为前 8 个加计数。
func TestSummarizeIDsCapsTheList(t *testing.T) {
	long := []string{"m1-free", "m2-free", "m3-free", "m4-free", "m5-free", "m6-free", "m7-free", "m8-free", "m9-free"}
	got := summarizeIDs(long)
	want := "m1-free, m2-free, m3-free, m4-free, m5-free, m6-free, m7-free, m8-free …等共 9 个"
	if got != want {
		t.Fatalf("summarizeIDs = %q, want %q", got, want)
	}
	if short := summarizeIDs(long[:2]); short != "m1-free, m2-free" {
		t.Fatalf("短列表应原样列出, got %q", short)
	}
}
