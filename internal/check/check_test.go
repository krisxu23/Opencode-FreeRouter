// SPDX-License-Identifier: GPL-3.0-or-later
package check

import "testing"

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

func TestEphemeralPortFloorIsTheWindowsBoundary(t *testing.T) {
	// Windows 从 49152 起把端口动态分配给出站连接。Go 版已不用每节点本地端口，
	// 但面板仍显示旧端口区间设置，帮助文案里的数字必须一致。
	if EphemeralPortFloor != 49152 {
		t.Fatalf("EphemeralPortFloor = %d, want 49152", EphemeralPortFloor)
	}
}

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

func TestCodeQuotaLiteralIsRateLimit(t *testing.T) {
	// 引擎的轮换分支 switch 在这些字面量上，测试里也只能用常量。
	// 这个字面量在 JS 侧写错过一次，所以单独立一条钉住。
	if CodeQuota != "RATE_LIMIT" {
		t.Fatalf("CodeQuota = %q, want RATE_LIMIT", CodeQuota)
	}
}
