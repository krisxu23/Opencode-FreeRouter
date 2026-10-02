// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package catalog

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"freerouter/internal/upstream"
)

func idsOf(rows []Model) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.ID)
	}
	return out
}

func contains(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

// 对照 tests/catalog.test.js 第 1 条 + 补齐边界:后缀、前缀、中缀、ALWAYS_FREE
// 固定免费、以及付费相似形都必须判定正确。
func TestFreeLaneDetection(t *testing.T) {
	cases := []struct {
		id   string
		want bool
	}{
		{"mimo-v2.6-flash-free", true}, // -free 后缀
		{"space-bunny-free", true},
		{"big-pickle", true},  // ALWAYS_FREE,无后缀(用户实测确认,2026-09-26)
		{"union-alpha", true}, // ALWAYS_FREE
		{"free-tier-x", true}, // 前缀命中
		{"x-free-y", true},    // 中缀命中
		{"mimo_free-x", true}, // 下划线也算边界
		{"freemodel", false},  // free 后无边界
		{"xfreex", false},     // free 前无边界
		{"gpt-5", false},      // 无后缀付费模型必须排除
		{"claude-opus-5", false},
		{"muse-spark-1.3", false}, // 免费的是 -contributor-free 变体
		{"qwen3.8-max", false},
	}
	for _, c := range cases {
		if got := IsFreeLane(c.id); got != c.want {
			t.Errorf("IsFreeLane(%q) = %v, want %v", c.id, got, c.want)
		}
	}
}

// 对照 tests/catalog.test.js 第 2 条。
func TestBuildIncludesBigPickleWithNameAndCaps(t *testing.T) {
	rows := Build([]string{"big-pickle", "mimo-v2.6-flash-free", "gpt-5"})
	ids := idsOf(rows)
	if !contains(ids, "big-pickle") || !contains(ids, "mimo-v2.6-flash-free") {
		t.Fatalf("ids = %v, want big-pickle + mimo-v2.6-flash-free", ids)
	}
	if contains(ids, "gpt-5") {
		t.Fatalf("paid id gpt-5 must not enter the free catalog: %v", ids)
	}
	var pickle Model
	for _, r := range rows {
		if r.ID == "big-pickle" {
			pickle = r
		}
	}
	if pickle.Name != "Big Pickle" {
		t.Fatalf("name = %q, want Big Pickle(不在 DISPLAY_NAMES,走 title-case)", pickle.Name)
	}
	if pickle.Wire != upstream.WireChat {
		t.Fatalf("wire = %q, want chat", pickle.Wire)
	}
	if pickle.ContextWindow != 200000 {
		t.Fatalf("contextWindow = %d, want 200000", pickle.ContextWindow)
	}
}

// 对照 tests/catalog.test.js 第 3 条:目录里的 wire 必须与适配器实际路由一致。
// 曾经 wire 由 isResponsesModel() 三元算出,永远报不出 messages,而 union-alpha
// 走 /zen/v1/messages —— 三条 wire 都要能报出来,且同一来源(upstream.WireFor)。
func TestBuildWireMatchesTheAdapterRouter(t *testing.T) {
	rows := Build([]string{"muse-spark-1.3-contributor-free", "union-alpha", "mimo-v2.6-flash-free"})
	wireOf := func(id string) upstream.Wire {
		for _, r := range rows {
			if r.ID == id {
				return r.Wire
			}
		}
		return ""
	}
	if w := wireOf("muse-spark-1.3-contributor-free"); w != upstream.WireResponses {
		t.Fatalf("muse-spark wire = %q, want responses", w)
	}
	if w := wireOf("union-alpha"); w != upstream.WireMessages {
		t.Fatalf("union-alpha wire = %q, want messages", w)
	}
	if w := wireOf("mimo-v2.6-flash-free"); w != upstream.WireChat {
		t.Fatalf("mimo wire = %q, want chat", w)
	}
	// muse-spark 系是已知的出口敏感模型,regionSensitive 必须立起来,
	// 补探据此发现特殊节点。
	for _, r := range rows {
		want := r.ID == "muse-spark-1.3-contributor-free"
		if r.RegionSensitive != want {
			t.Fatalf("%s RegionSensitive = %v, want %v", r.ID, r.RegionSensitive, want)
		}
	}
}

func TestStaticCatalogContainsTheAlwaysFreeTrio(t *testing.T) {
	static := Static()
	if len(static) == 0 {
		t.Fatal("Static() is empty(离线回落目录不许为空)")
	}
	ids := idsOf(static)
	for _, want := range []string{"union-alpha", "space-bunny-free", "big-pickle"} {
		if !contains(ids, want) {
			t.Fatalf("Static() missing ALWAYS_FREE %q: %v", want, ids)
		}
	}
	for _, r := range static {
		if r.Name == "" {
			t.Fatalf("Static() row %q has empty Name", r.ID)
		}
	}
}

// 17 行能力表每行一个正则命中样本,数值逐字段核对 —— 这些数字跟踪的是
// models.dev 的 Zen(免费车道)行,不是 canonical 行,抄错就是 5 倍超卖。
func TestCapabilitiesTableHitsAllSeventeenRows(t *testing.T) {
	probes := []struct {
		id      string
		want    Capabilities
		inTable bool
	}{
		{"longcat-2.5-preview-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 1000000, MaxOutput: 131072, CanDisableThinking: true}, true},
		{"space-bunny-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 1048576, MaxOutput: 524288, CanDisableThinking: true}, true},
		{"mimo-v2.6-flash-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: false}, true},
		{"mimo-v2.5-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: false}, true},
		{"mimo-mini-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 65536, CanDisableThinking: true}, true},
		{"muse-spark-1.3-contributor-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 1048576, MaxOutput: 131072, CanDisableThinking: true}, true},
		{"nemotron-3.5-lightning-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 262144, CanDisableThinking: true}, true},
		{"nemotron-3-ultra-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 1000000, MaxOutput: 128000, CanDisableThinking: true}, true},
		{"nemotron-8-turbo-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 128000, CanDisableThinking: true}, true},
		{"ling-3.0-flash-fin-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 262144, MaxOutput: 32768, CanDisableThinking: true}, true},
		{"big-pickle", Capabilities{Vision: false, Reasoning: true, ContextWindow: 200000, MaxOutput: 32000, CanDisableThinking: true}, true},
		{"union-alpha", Capabilities{Vision: true, Reasoning: false, ContextWindow: 262144, MaxOutput: 131072, CanDisableThinking: true}, true},
		{"deepseek-v4-flash-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 200000, MaxOutput: 128000, CanDisableThinking: true}, true},
		{"kimi-k2.5-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 262144, CanDisableThinking: true}, true},
		{"qwen3.6-plus-free", Capabilities{Vision: true, Reasoning: true, ContextWindow: 262144, MaxOutput: 65536, CanDisableThinking: true}, true},
		{"glm-5-free", Capabilities{Vision: false, Reasoning: true, ContextWindow: 204800, MaxOutput: 131072, CanDisableThinking: true}, true},
		{"jev-1.13-free", Capabilities{Vision: false, Reasoning: false, ContextWindow: 32768, MaxOutput: 4096, CanDisableThinking: true}, true},
	}
	if len(capabilitiesTable) != 17 {
		t.Fatalf("capabilitiesTable has %d rows, want 17", len(capabilitiesTable))
	}
	for _, p := range probes {
		got := CapabilitiesFor(p.id)
		if got.Vision != p.want.Vision || got.Reasoning != p.want.Reasoning ||
			got.ContextWindow != p.want.ContextWindow || got.MaxOutput != p.want.MaxOutput ||
			got.CanDisableThinking != p.want.CanDisableThinking {
			t.Errorf("CapabilitiesFor(%q) = %+v, want %+v", p.id, got, p.want)
		}
		if p.inTable && got.Match == nil {
			t.Errorf("CapabilitiesFor(%q) hit no table row", p.id)
		}
	}
}

// 对照 tests/limits.test.js 的「specific-first」条:specific 行必须排在
// generic 行之前,否则 generic 正则会提前遮蔽 specific 行。
func TestCapabilitiesRowsAreOrderedSpecificFirst(t *testing.T) {
	idx := func(re string) int {
		for i, row := range capabilitiesTable {
			if row.Match.String() == re {
				return i
			}
		}
		return -1
	}
	if idx(`^nemotron.*3\.5.*lightning`) > idx(`^nemotron`) {
		t.Fatal("lightning row must precede generic nemotron")
	}
	if idx(`^nemotron.*ultra`) > idx(`^nemotron`) {
		t.Fatal("ultra row must precede generic nemotron")
	}
	if idx(`^mimo.*v2\.6`) > idx(`^mimo`) {
		t.Fatal("mimo v2.6 row must precede generic mimo")
	}
}

func TestCapabilitiesFallbackForUnknownModel(t *testing.T) {
	caps := CapabilitiesFor("brand-new-thing-free")
	if caps.Vision || !caps.Reasoning || caps.ContextWindow != 131072 || caps.MaxOutput != 32768 {
		t.Fatalf("fallback = %+v, want {vision:false, reasoning:true, 131072, 32768}", caps)
	}
}

func TestBuildDedupesByBaseID(t *testing.T) {
	// 同一 base 的两个免费变体(裸 id + "(high)" 思考后缀)只留一个;
	// 重复列出会让面板出现两行同模型。
	rows := Build([]string{"mimo-v2.6-flash-free", "mimo-v2.6-flash-free (high)", "mimo-v2.6-flash-free"})
	if len(rows) != 1 {
		t.Fatalf("rows = %v, want 1 entry(按 base id 去重)", idsOf(rows))
	}
	if rows[0].ID != "mimo-v2.6-flash-free" {
		t.Fatalf("id = %q, want the bare base id", rows[0].ID)
	}
}

// 对照 tests/limits.test.js 的「buildCatalog carries the Zen-lane numbers」:
// 本地表数值要端到端流进 Build 的产物。
func TestBuildCarriesZenLaneNumbers(t *testing.T) {
	zen := []struct {
		id  string
		ctx int64
		out int64
	}{
		{"mimo-v2.6-flash-free", 200000, 32000},
		{"muse-spark-1.3-contributor-free", 1048576, 131072},
		{"nemotron-3.5-lightning-free", 262144, 262144},
		{"nemotron-3-ultra-free", 1000000, 128000},
		{"ling-3.0-flash-fin-free", 262144, 32768},
		{"space-bunny-free", 1048576, 524288},
		{"big-pickle", 200000, 32000},
		{"longcat-2.5-preview-free", 1000000, 131072},
	}
	ids := make([]string, 0, len(zen))
	for _, z := range zen {
		ids = append(ids, z.id)
	}
	rows := Build(ids)
	if len(rows) != len(zen) {
		t.Fatalf("rows = %d, want %d", len(rows), len(zen))
	}
	for _, z := range zen {
		var row Model
		for _, r := range rows {
			if r.ID == z.id {
				row = r
			}
		}
		if row.ContextWindow != z.ctx || row.MaxOutput != z.out {
			t.Errorf("%s = %d/%d, want %d/%d", z.id, row.ContextWindow, row.MaxOutput, z.ctx, z.out)
		}
	}
}

func TestParseListingShapes(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    []string
		wantErr bool
	}{
		{"data objects", `{"data":[{"id":"a-free"},{"id":"b-free"}]}`, []string{"a-free", "b-free"}, false},
		{"models strings", `{"models":["c-free","d-free"]}`, []string{"c-free", "d-free"}, false},
		{"top-level array", `["e-free","f-free"]`, []string{"e-free", "f-free"}, false},
		{"mixed rows filter", `{"data":[{"id":""},{"id":42},{"id":"g-free"},"h-free"]}`, []string{"g-free", "h-free"}, false},
		{"empty data", `{"data":[]}`, []string{}, false},
		{"no arrays", `{"foo":1}`, []string{}, false},
		{"broken json", `not-json`, nil, true},
	}
	for _, c := range cases {
		got, err := ParseListing([]byte(c.body))
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: want error, got %v", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// stubRT 是不出网的 RoundTripper:Refresh 的回落路径不允许真打网络。
type stubRT struct {
	status  int
	body    string
	err     error
	lastURL string
}

func (s *stubRT) RoundTrip(r *http.Request) (*http.Response, error) {
	s.lastURL = r.URL.String()
	if s.err != nil {
		return nil, s.err
	}
	return &http.Response{
		StatusCode: s.status,
		Body:       io.NopCloser(strings.NewReader(s.body)),
		Header:     http.Header{},
	}, nil
}

func TestRefreshFallsBackToStaticWithoutAnError(t *testing.T) {
	// 上游一次抖动不该让整个目录清空 —— 当年拉取失败直接清目录,退化成
	// 「模型列表为空 → 用户以为没额度」。所以失败/空列表都回落 Static,
	// 且 err 为 nil(回落不是错误,是承诺)。
	cases := []stubRT{
		{err: errors.New("dial fail")},
		{status: 500, body: "boom"},
		{status: 200, body: `{"data":[]}`},
	}
	for i := range cases {
		rt := &cases[i]
		got, err := Refresh(context.Background(), &http.Client{Transport: rt}, "https://upstream.test/zen/v1")
		if err != nil {
			t.Fatalf("case %d: err = %v, want nil", i, err)
		}
		if !reflect.DeepEqual(idsOf(got), idsOf(Static())) {
			t.Fatalf("case %d: got %v, want Static()", i, idsOf(got))
		}
	}
}

func TestRefreshBuildsFromTheListing(t *testing.T) {
	rt := &stubRT{status: 200, body: `{"data":[{"id":"big-pickle"},{"id":"gpt-5"},{"id":"mimo-v2.6-flash-free"}]}`}
	got, err := Refresh(context.Background(), &http.Client{Transport: rt}, "https://upstream.test/zen/v1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"big-pickle", "mimo-v2.6-flash-free"}
	if !reflect.DeepEqual(idsOf(got), want) {
		t.Fatalf("got %v, want %v(按 listing 顺序,付费 id 剔除)", idsOf(got), want)
	}
	if rt.lastURL != "https://upstream.test/zen/v1/models" {
		t.Fatalf("GET %q, want {base}/models", rt.lastURL)
	}
}

func TestDisplayModelName(t *testing.T) {
	cases := []struct {
		id   string
		want string
	}{
		{"mimo-v2.6-flash-free", "MiMo V2.6 Flash"},          // 已知名直接命中
		{"big-pickle", "Big Pickle"},                         // 未知 → title-case
		{"unheard-thing-2.0-free", "Unheard Thing 2 0 Free"}, // 数字开头的词保持原形
	}
	for _, c := range cases {
		if got := DisplayModelName(c.id); got != c.want {
			t.Errorf("DisplayModelName(%q) = %q, want %q", c.id, got, c.want)
		}
	}
}
