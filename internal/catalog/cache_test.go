// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package catalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 基线文件的存在理由(2026-10-03 起语义变更):它不再是冷启动的目录来源
// (目录 id 的唯一来源是上游实时列表,冷启动恒播静态表),只是「上次运行见过
// 的 id」的落盘,供换代时打增删对比日志。src/index.js:219/234-250 是 JS 侧
// 的同名同形状文件。

func TestLoadCacheMissingFileIsEmpty(t *testing.T) {
	if got := LoadCache(filepath.Join(t.TempDir(), "nope.json")); got != nil {
		t.Fatalf("missing cache returned %v, want nil", got)
	}
}

func TestLoadCacheRejectsCorruptAndEmpty(t *testing.T) {
	cases := map[string]string{
		"不是 JSON":   `{{{`,
		"没有 ids 键":  `{"fetchedAt":1}`,
		"ids 非数组":   `{"fetchedAt":1,"ids":"x"}`,
		"ids 空数组":   `{"fetchedAt":1,"ids":[]}`,
		"ids 全非字符串": `{"fetchedAt":1,"ids":[42,null,true]}`,
	}
	for name, raw := range cases {
		file := filepath.Join(t.TempDir(), "c.json")
		if err := os.WriteFile(file, []byte(raw), 0o644); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		if got := LoadCache(file); got != nil {
			t.Fatalf("%s: got %v, want nil", name, got)
		}
	}
}

func TestLoadCacheKeepsStringsAndDropsTheRest(t *testing.T) {
	file := filepath.Join(t.TempDir(), "c.json")
	raw := `{"fetchedAt":1790890819197,"ids":["a-free",7,"b-free",null]}`
	if err := os.WriteFile(file, []byte(raw), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got := LoadCache(file)
	if len(got) != 2 || got[0] != "a-free" || got[1] != "b-free" {
		t.Fatalf("got %v, want [a-free b-free] in file order", got)
	}
}

// 过期不丢弃:实测本机直连拉不通,丢掉过期缓存就等于丢掉整个模型列表。
// 过期只该降级成一条日志(JS src/index.js:241-245 同语义)。
func TestLoadCacheServesAnExpiredListAnyway(t *testing.T) {
	file := filepath.Join(t.TempDir(), "c.json")
	tenYearsAgo := "1"
	raw := `{"fetchedAt":` + tenYearsAgo + `,"ids":["old-free"]}`
	if err := os.WriteFile(file, []byte(raw), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if got := LoadCache(file); len(got) != 1 || got[0] != "old-free" {
		t.Fatalf("expired cache was dropped: %v", got)
	}
}

func TestSaveCacheRoundTripsJSFileShape(t *testing.T) {
	file := filepath.Join(t.TempDir(), "sub", "catalog-ids.json")
	if err := SaveCache(file, []string{"x-free", "y-free"}, 1790890819197); err != nil {
		t.Fatalf("save: %v", err)
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("SaveCache must create the parent dir: %v", err)
	}
	// 键名逐字兼容 JS 写的文件(src/index.js:249) —— 差分与手改都靠它。
	s := string(raw)
	if !strings.Contains(s, `"fetchedAt":1790890819197`) || !strings.Contains(s, `"ids":["x-free","y-free"]`) {
		t.Fatalf("on-disk shape is not the JS one: %s", s)
	}
	if got := LoadCache(file); len(got) != 2 || got[1] != "y-free" {
		t.Fatalf("round trip lost ids: %v", got)
	}
}
