// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
package parse

import "testing"

// 重名 tag 消歧：同名不同身份的节点必须全部进池，不能互相覆盖。
func TestDisambiguateTags_CollidingTags(t *testing.T) {
	mk := func(tag, server, uuid string) Outbound {
		return Outbound{Tag: tag, Type: "vless", Server: server, ServerPort: 443, UUID: uuid}
	}
	outs := []Outbound{
		mk("EPODONIOS", "1.1.1.1", "aaaaaaaa-1111-1111-1111-111111111111"),
		mk("EPODONIOS", "2.2.2.2", "bbbbbbbb-2222-2222-2222-222222222222"),
		mk("EPODONIOS", "3.3.3.3", "cccccccc-3333-3333-3333-333333333333"),
		mk("US-01", "4.4.4.4", "dddddddd-4444-4444-4444-444444444444"),
	}
	got := DisambiguateTags(outs)
	if len(got) != 4 {
		t.Fatalf("条目数应不变，got %d", len(got))
	}
	seen := map[string]bool{}
	for _, o := range got {
		if seen[o.Tag] {
			t.Fatalf("消歧后仍有重名 tag: %q", o.Tag)
		}
		seen[o.Tag] = true
	}
	// 不碰撞的 tag 原样保留
	found := false
	for _, o := range got {
		if o.Tag == "US-01" {
			found = true
		}
	}
	if !found {
		t.Fatal("未碰撞的 US-01 应保留原 tag")
	}
	// 跨轮稳定：同一批输入两次调用分配一致
	got2 := DisambiguateTags(outs)
	for i := range got {
		if got[i].Tag != got2[i].Tag {
			t.Fatalf("消歧不稳定: %q vs %q", got[i].Tag, got2[i].Tag)
		}
	}
	// 顺序无关：打乱输入顺序，tag 分配不变
	rev := []Outbound{outs[3], outs[2], outs[1], outs[0]}
	got3 := DisambiguateTags(rev)
	tags1, tags3 := map[string]bool{}, map[string]bool{}
	for _, o := range got {
		tags1[o.Tag] = true
	}
	for _, o := range got3 {
		tags3[o.Tag] = true
	}
	for tag := range tags1 {
		if !tags3[tag] {
			t.Fatalf("顺序打乱后 tag 分配变化，缺 %q", tag)
		}
	}
}

// 空 tag 原样保留（Merge 会跳过，不在此处处理）
func TestDisambiguateTags_EmptyTagUntouched(t *testing.T) {
	outs := []Outbound{{Tag: "", Type: "vless", Server: "1.1.1.1", ServerPort: 443}}
	got := DisambiguateTags(outs)
	if len(got) != 1 || got[0].Tag != "" {
		t.Fatal("空 tag 应原样保留")
	}
}
