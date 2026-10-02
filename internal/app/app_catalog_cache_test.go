// SPDX-License-Identifier: GPL-3.0-or-later
package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"freerouter/internal/logger"
)

// 目录落盘缓存（data/catalog-ids.json）。JS 侧的动机写在 src/index.js:253-254：
// 这台机器上直连被封而节点可用，冷启动若只能等上游，面板 opening 就是一张
// 空模型表 —— 缓存让第一帧就有上一轮的列表。

func readCatalogCache(t *testing.T, root string) (float64, []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "data", "catalog-ids.json"))
	if err != nil {
		t.Fatalf("读目录缓存: %v", err)
	}
	var j struct {
		FetchedAt float64  `json:"fetchedAt"`
		IDs       []string `json:"ids"`
	}
	if err := json.Unmarshal(raw, &j); err != nil {
		t.Fatalf("缓存不是 JSON: %v (%s)", err, raw)
	}
	return j.FetchedAt, j.IDs
}

// TestBootSeedsCatalogFromDiskCache：缓存存在时冷启动用它，而不是静态回退表。
// 同时钉住「启动不重写缓存」—— JS 的 boot 分支直接 buildCatalog(_bootIds)，
// 只有 applyIds 才 saveCatalogCache(src/index.js:263-264)，所以 fetchedAt 必须
// 还是上一轮写入的那个值；若在 boot 也存一次，缓存的年龄就永远是 0，过期告警
// 会失去意义。
func TestBootSeedsCatalogFromDiskCache(t *testing.T) {
	logger.Init("")
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatalf("建数据目录: %v", err)
	}
	seededAt := time.Now().Add(-48 * time.Hour).UnixMilli()
	// id 必须长得像免费线路:catalog.Build 与 JS 的 buildCatalog 同样会丢掉
	// 非 -free 的 id，否则这条测试断言的是过滤规则而不是缓存装载。
	body := fmt.Sprintf(`{"fetchedAt":%d,"ids":["cached-model-free"]}`, seededAt)
	if err := os.WriteFile(filepath.Join(root, "data", "catalog-ids.json"), []byte(body), 0o644); err != nil {
		t.Fatalf("写缓存: %v", err)
	}

	parts, err := Load(root)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer func() { _ = parts.Shutdown(context.Background()) }()

	list := parts.catalog.get()
	if len(list) != 1 || list[0].ID != "cached-model-free" {
		t.Fatalf("冷启动目录 = %+v, want 单行 cached-model-free", list)
	}
	if got, ids := readCatalogCache(t, root); int64(got) != seededAt || len(ids) != 1 {
		t.Fatalf("启动不应重写缓存: fetchedAt=%v ids=%v, want %v", got, ids, seededAt)
	}
}

// TestCatalogTurnoverPersistsCache：任何一次换代都把 id 列表落盘,
// 下一台冷启动才有东西可读。形状与 JS 的 writeJsonFile 逐字一致（紧凑、
// fetchedAt 在前、ids 在后）。
func TestCatalogTurnoverPersistsCache(t *testing.T) {
	p := newProbeParts(t, 0)
	p.applyIDs([]string{"written-model", "second-model"}, "test")

	got, ids := readCatalogCache(t, p.Root)
	if got <= 0 {
		t.Fatalf("fetchedAt = %v, want 正数毫秒时间戳", got)
	}
	if len(ids) != 2 || ids[0] != "written-model" || ids[1] != "second-model" {
		t.Fatalf("ids = %v, want [written-model second-model]", ids)
	}
	raw, err := os.ReadFile(filepath.Join(p.Root, "data", "catalog-ids.json"))
	if err != nil {
		t.Fatalf("读缓存: %v", err)
	}
	// JS 的 writeJsonFile 默认 indent=false → 无空格、键序按字面量。
	head := `{"fetchedAt":`
	if len(raw) < len(head) || string(raw[:len(head)]) != head {
		t.Fatalf("缓存开头 = %q, want 紧凑 JSON 以 %q 起", string(raw[:len(head)]), head)
	}
	if bytes.Contains(raw, []byte("\n")) {
		t.Fatalf("缓存不该是缩进 JSON: %s", raw)
	}
}
