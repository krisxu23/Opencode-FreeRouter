// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package effort

import (
	"math"
	"reflect"
	"testing"
)

// effort 是**真实发出的 max_tokens**,不是提示词形容词,所以每个数字都要对着
// wire 断言。测试夹具与 tests/effort.test.js 的三个模型形状一一对应:
//   - ALWAYS_THINKS:mimo-v2.6-flash-free,思考不可关闭 → JS 侧每一档翻倍
//     (ALWAYS_THINKING_FACTOR)。Go 的 Entry 没有携带 canDisableThinking(计划
//     类型块是契约,字段面不许加),翻倍无法在 effort 包内表达 —— 这三条 JS
//     断言没有对应的 Go 条目,是**已知偏离**,引擎层若要补偿须在构造 Entry 前
//     处理。其余条目全部按 JS 实测行为对照。
//   - TUNABLE:deepseek-v4-flash-free,普通可推理 → 档位就是档位。
//   - NO_MENU:union-alpha,没有 effort 菜单 → 整窗属于答案,任何档位都不得
//     压缩它。
var (
	ALWAYS_THINKS = Entry{ID: "mimo-v2.6-flash-free", ContextWindow: 200000, MaxOutput: 32000, SupportsReasoning: true}
	TUNABLE       = Entry{ID: "deepseek-v4-flash-free", ContextWindow: 200000, MaxOutput: 131072, SupportsReasoning: true}
	NO_MENU       = Entry{ID: "union-alpha", ContextWindow: 262144, MaxOutput: 131072, SupportsReasoning: false}
)

func ptr(v int) *int { return &v }

// TestBudgetForNilSettingsFallsBackToMaxOutput —— 计划四条之一(nil 回落)。
// data/settings.json 里 defaultMaxTokens 就是 null:回落目标必须是模型自己的
// 上限,而不是 0 —— 上游把 max_tokens:0 当「不要输出任何内容」,0 会变成
// 每一轮都空回复的静默故障。
func TestBudgetForNilSettingsFallsBackToMaxOutput(t *testing.T) {
	if got := BudgetFor(LevelExtra, ALWAYS_THINKS, 0, nil); got != 32000 {
		t.Fatalf("BudgetFor(extra, requested=0, nil) = %d, want 32000 (JS budgetFor('deep', ALWAYS_THINKS, undefined, undefined))", got)
	}
	if got := BudgetFor(LevelNone, TUNABLE, 0, nil); got != 131072 {
		t.Fatalf("BudgetFor(none, requested=0, nil) = %d, want 131072", got)
	}
}

// TestBudgetForNeverReturnsANegativeBudget 是 int 宽度的钉。capacity 是
// int64(E.MaxOutput)，旧实现直接 int(capacity) 返回：32 位平台上超过 2^31-1
// 的模型上限溢成负数，而调用方按「非正 = 未设置」处理，那个预算被静默丢弃、
// 请求不带 max_tokens。返回值必须恒为正。
func TestBudgetForNeverReturnsANegativeBudget(t *testing.T) {
	huge := Entry{SupportsReasoning: true, MaxOutput: math.MaxInt64}
	got := BudgetFor(LevelNone, huge, 0, nil)
	if got <= 0 {
		t.Fatalf("BudgetFor(MaxOutput=MaxInt64) = %d, 必须恒为正（溢出成负数会被当「未设置」丢弃）", got)
	}
	if int64(got) != math.MaxInt64 {
		t.Fatalf("BudgetFor(MaxOutput=MaxInt64) = %d, want %d（钳在 math.MaxInt）", got, math.MaxInt64)
	}
}

// TestBudgetForZeroIsNotACeiling —— 计划四条之一(0 不回落)。
//
// JS 语义:requested == 0 与「未设置」**同一条路**。usableTokens 对非正数返回
// Infinity(tests/effort.test.js:64-69 实测:requested 0/”/-1/NaN 都不构成
// 上限),于是显式 0 不产生 max_tokens:0,而是继续回落 settingsDefault、再回落
// 模型上限。Go 的 int 没有 NaN/空串两种形态,0 与负数合并成同一条「未指定」路。
func TestBudgetForZeroIsNotACeiling(t *testing.T) {
	// JS budgetFor('deep', ALWAYS_THINKS, 0, 0) === 32000:两边都是 0 也一样
	if got := BudgetFor(LevelExtra, ALWAYS_THINKS, 0, ptr(0)); got != 32000 {
		t.Fatalf("BudgetFor(extra, 0, &0) = %d, want 32000", got)
	}
	if got := BudgetFor(LevelExtra, ALWAYS_THINKS, -1, nil); got != 32000 {
		t.Fatalf("BudgetFor(extra, -1, nil) = %d, want 32000", got)
	}
	// 显式 0 不得压过模型上限变成 0:settings 页清空输入曾把所有模型钳到 MIN_BUDGET
	if got := BudgetFor(LevelExtra, TUNABLE, 0, ptr(32768)); got != 32768 {
		t.Fatalf("BudgetFor(extra, 0, &32768) = %d, want 32768 (min(131072,32768))", got)
	}
}

// TestBudgetForExplicitValuesWin —— 计划四条之一(显式值优先)。
// 会话自己设的上限比档位更低时,由会话决定;档位上限只能压低容量,永远不能
// 抬高它(tests/effort.test.js:49-56)。
func TestBudgetForExplicitValuesWin(t *testing.T) {
	cases := []struct {
		name           string
		level          Level
		entry          Entry
		requested      int
		settings       *int
		want           int
		jsCounterparts []string
	}{
		{"requested beats everything", LevelExtra, TUNABLE, 4096, ptr(32768), 4096, []string{"budgetFor('deep', TUNABLE, 4096, 32768) === 4096"}},
		{"settings beat capacity", LevelLow, TUNABLE, 0, ptr(1024), 1024, []string{"budgetFor('light', TUNABLE, undefined, 1024) === 1024"}},
		{"level ceiling caps capacity", LevelLow, TUNABLE, 0, ptr(32768), 2048, []string{"budgetFor('light', TUNABLE, undefined, 32768) === 2048"}},
		{"balanced ceiling", LevelHigh, TUNABLE, 0, ptr(32768), 8192, []string{"budgetFor('balanced', TUNABLE, undefined, 32768) === 8192"}},
		{"deep inherits capacity", LevelExtra, TUNABLE, 0, ptr(32768), 32768, []string{"budgetFor('deep', TUNABLE, undefined, 32768) === 32768"}},
		{"no menu ignores any level", LevelLow, NO_MENU, 0, ptr(32768), 32768, []string{"budgetFor('light', NO_MENU, undefined, 32768) === 32768"}},
		{"no menu honours requested", LevelNone, NO_MENU, 4096, ptr(32768), 4096, []string{"budgetFor(undefined, NO_MENU, 4096, 32768) === 4096"}},
		{"floor: answer must land", LevelLow, Entry{ID: "x", MaxOutput: 100, SupportsReasoning: true}, 0, nil, 100, []string{"budgetFor('light', {reasoning:true, maxOutput:100}, undefined, undefined) === 100(下限不超模型上限,旧 512 会超上游上限→400)"}},
		{"floor: tiny capacity", LevelExtra, Entry{ID: "x", MaxOutput: 1, SupportsReasoning: true}, 0, ptr(1), 1, []string{"budgetFor('deep', {reasoning:true, maxOutput:1}, undefined, 1) === 1(同上)"}},
		{"zero entry means default capacity", LevelExtra, Entry{}, 0, nil, 32768, []string{"budgetFor('deep', undefined, undefined, undefined) === 32768"}},
	}
	for _, tc := range cases {
		if got := BudgetFor(tc.level, tc.entry, tc.requested, tc.settings); got != tc.want {
			t.Fatalf("%s: BudgetFor(%s, requested=%d) = %d, want %d (JS: %v)", tc.name, tc.level, tc.requested, got, tc.want, tc.jsCounterparts)
		}
	}
}

// TestEffortsForNonReasoningHasOnlyNone —— 计划四条之一。
// 没有 effort 菜单的模型不该被任何档位压缩;菜单模型按升序给全四档。
func TestEffortsForNonReasoningHasOnlyNone(t *testing.T) {
	if got := EffortsFor(NO_MENU); !reflect.DeepEqual(got, []Level{LevelNone}) {
		t.Fatalf("EffortsFor(NO_MENU) = %v, want [none]", got)
	}
	ascending := EffortsFor(TUNABLE)
	want := []Level{LevelNone, LevelLow, LevelHigh, LevelExtra}
	if !reflect.DeepEqual(ascending, want) {
		t.Fatalf("EffortsFor(TUNABLE) = %v, want %v", ascending, want)
	}
}

// TestResolveLevelFoldsRequestSettingsModel —— ResolveLevel 的三路折叠:请求的
// reasoning_effort → 设置档 → 模型支持。未知档位降级默认而不报错。
func TestResolveLevelFoldsRequestSettingsModel(t *testing.T) {
	cases := []struct {
		name      string
		requested string
		settings  string
		entry     Entry
		want      Level
		jsNote    string
	}{
		{"requested wins", "low", "high", TUNABLE, LevelLow, ""},
		{"settings fill silence", "", "extra", TUNABLE, LevelExtra, ""},
		{"default is the virtual balanced", "", "", TUNABLE, LevelHigh,
			"resolveLevel(undefined, TUNABLE).id === 'balanced';balanced 解析成模型的中档 high(8192),与 JS balanced 同一预算"},
		{"unknown degrades to default", "turbo", "high", TUNABLE, LevelHigh,
			"resolveLevel('turbo', TUNABLE).id === 'balanced'"},
		{"deep alias resolves to extra", "deep", "", TUNABLE, LevelExtra, "resolveLevel('deep', TUNABLE).id === 'deep'"},
		{"no menu has no rung", "light", "high", NO_MENU, LevelNone,
			"resolveLevel('light', NO_MENU) === undefined;无菜单模型整窗属答案"},
		{"none is an honourable level", "none", "", TUNABLE, LevelNone, ""},
	}
	for _, tc := range cases {
		if got := ResolveLevel(tc.requested, tc.settings, tc.entry); got != tc.want {
			t.Fatalf("%s: ResolveLevel(%q, %q) = %q, want %q (JS: %s)", tc.name, tc.requested, tc.settings, got, tc.want, tc.jsNote)
		}
	}
}

// TestBudgetForHonoursTheJSLadder —— JS tests/effort.test.js 的对照条目:档位
// 与预算的对应关系(light 2048 / balanced 8192 / deep 模型上限)在 Go 词汇下
// 必须逐一成立,否则选择器报的数字与线上发出的数字会对不上。
func TestBudgetForHonoursTheJSLadder(t *testing.T) {
	if LevelLow != "low" || LevelHigh != "high" || LevelExtra != "extra" || LevelNone != "none" {
		t.Fatalf("level spellings are wire contract: %q %q %q %q", LevelNone, LevelLow, LevelHigh, LevelExtra)
	}
	if DefaultLevel != "balanced" {
		t.Fatalf("DefaultLevel = %q, want balanced (data/settings.json 的 effortLevel 值)", DefaultLevel)
	}
	// settings.effortLevel 是 "balanced"(虚拟档):对可推理模型解析成中档,
	// 预算与 JS balanced 的 8192 一致
	if got := BudgetFor(ResolveLevel("", "balanced", TUNABLE), TUNABLE, 0, ptr(32768)); got != 8192 {
		t.Fatalf("resolved balanced budget = %d, want 8192", got)
	}
	// 无菜单模型:解析成 none,预算是整窗 —— JS resolveLevel('light', NO_MENU)
	// 返回 undefined 后 budgetFor 走同一条无档位路
	if got := BudgetFor(ResolveLevel("light", "high", NO_MENU), NO_MENU, 0, ptr(32768)); got != 32768 {
		t.Fatalf("resolved no-menu budget = %d, want 32768", got)
	}
}
