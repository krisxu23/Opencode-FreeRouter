// SPDX-License-Identifier: GPL-3.0-or-later
package check

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegionGroupsOrderIsTheFallbackOrder(t *testing.T) {
	want := []string{"US", "JP", "HK", "TW", "KR", "SG", "EU", "OTHER"}
	if len(RegionGroups) != len(want) {
		t.Fatalf("group count = %d, want %d", len(RegionGroups), len(want))
	}
	for i := range want {
		if RegionGroups[i] != want[i] {
			t.Fatalf("group[%d] = %q, want %q", i, RegionGroups[i], want[i])
		}
	}
}

func TestSubRetryExitsKeepsTheMeasuredRaise(t *testing.T) {
	// 2026-09-28 实测：前两个出口 ECONNRESET，第三个才成功，预算从 3 提到 12。
	// 这不是随手取的整数，是那次事故的修复值。
	if SubRetryExits != 12 {
		t.Fatalf("SubRetryExits = %d, want 12", SubRetryExits)
	}
}

// TestEphemeralPortFloorIsTheWindowsBoundary 随 EphemeralPortFloor 一起删了
// (审计 O6):每节点本地端口在零端口架构下已不存在,那个数字只剩面板帮助文案里
// 的前端字面量(web/app.js 冻结),后端没有任何判据读它。

func TestFailureCodesAreDistinctAndNonEmpty(t *testing.T) {
	codes := map[string]string{
		"region": CodeRegion, "quota": CodeQuota, "credential": CodeCredential,
		"transport": CodeTransport, "timeout": CodeTimeout, "server": CodeServer,
		"empty": CodeEmpty, "aborted": CodeAborted,
	}
	seen := map[string]string{}
	for label, v := range codes {
		if v == "" {
			t.Fatalf("code %q is empty", label)
		}
		if prev, dup := seen[v]; dup {
			t.Fatalf("code %q duplicates %q (both %q)", label, prev, v)
		}
		seen[v] = label
	}
}

// TestProbeSummaryPatternMatchesTheConsoleCopy 钉住「同一份正则只写一遍」。
//
// 这条正则有两份手抄：check.ProbeSummaryPattern（Go 侧）与 web/app.js 的
// probeFromLogs（前端）。两边都在解析同一行日志，一次改动只落到一边的话，
// 面板上那格探测摘要要么永远空着、要么数字停在旧口径上 —— 而两边都能正常
// 编译通过，没有测试会红。这里读出 JS 那一行逐字比对。
func TestProbeSummaryPatternMatchesTheConsoleCopy(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "web", "app.js"))
	if err != nil {
		t.Fatalf("read web/app.js: %v", err)
	}
	// 正则字面量自身含 `\/` 转义，所以要按「未被反斜杠转义的 /」找结尾，
	// 不能拿 strings.Index 撞上第一个斜杠。
	const marker = "const re = /"
	i := strings.Index(string(raw), marker)
	if i < 0 {
		t.Fatal("web/app.js 里找不到 probeFromLogs 的正则字面量（前端改成别的方式解析日志了？）")
	}
	rest := string(raw)[i+len(marker):]
	end := -1
	for k := 0; k < len(rest); k++ {
		if rest[k] != '/' {
			continue
		}
		if k > 0 && rest[k-1] == '\\' {
			continue // `\/`：字面量里的斜杠
		}
		end = k
		break
	}
	if end < 0 {
		t.Fatal("web/app.js 的正则字面量未闭合")
	}
	got := rest[:end]
	if got != ProbeSummaryPattern {
		t.Fatalf("探测摘要正则与前端漂移了:\n  check.ProbeSummaryPattern = %q\n  web/app.js probeFromLogs = %q", ProbeSummaryPattern, got)
	}
	// 还要真的能解析一行真实的日志文本（前端那份改了分隔符时这里是第二道闸）。
	if m := ProbeSummaryRe.FindStringSubmatch("probe round: 3/12 alive (A 5 · B 1 · 本轮新验 B 0 · 降冷 1) in 8.4s"); m == nil {
		t.Fatal("真实的探测摘要行解析不出来")
	} else if m[1] != "3" || m[2] != "12" || m[3] != "5" || m[4] != "1" || m[5] != "8.4" {
		t.Fatalf("分组解析错误: %q", m)
	}
}

func TestCodeQuotaLiteralIsRateLimit(t *testing.T) {
	// 引擎的轮换分支 switch 在这些字面量上，测试里也只能用常量。
	// 这个字面量在 JS 侧写错过一次，所以单独立一条钉住。
	if CodeQuota != "RATE_LIMIT" {
		t.Fatalf("CodeQuota = %q, want RATE_LIMIT", CodeQuota)
	}
}
