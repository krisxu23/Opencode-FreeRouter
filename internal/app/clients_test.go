// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package app

import (
	"net/http"
	"testing"

	"freerouter/internal/health"
)

// TestExitClientCacheIsGenerationScoped 钉住 O3 的复用与失效两半:同代同出口必须
// 复用同一个 client(否则连接池每轮从零开始);换代必须重建(旧 client 里绑的是
// 上一代的拨号闭包,继续复用等于把请求拨到一条已被撤下的出站上)。
func TestExitClientCacheIsGenerationScoped(t *testing.T) {
	c := newExitClientCache(4)
	builds := 0
	build := func() (*http.Client, error) { builds++; return &http.Client{}, nil }

	a, err := c.get(1, "n1", build)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if b, _ := c.get(1, "n1", build); b != a || builds != 1 {
		t.Fatalf("同代同出口没复用 client: builds=%d a=%p b=%p", builds, a, b)
	}
	if d, _ := c.get(1, "n2", build); d == a || builds != 2 {
		t.Fatalf("不同出口共用了一个 client(空闲连接会跨出口串用): builds=%d", builds)
	}
	e, _ := c.get(2, "n1", build)
	if e == a {
		t.Fatal("换代后仍复用旧代 client:拨号闭包指向已被替换的出站")
	}
	if n := c.len(); n != 1 {
		t.Fatalf("旧代条目应随换代回收,剩 %d, want 1", n)
	}
}

// TestExitClientCacheEvictsLeastRecentlyUsed 是上限那一半:超出上限时挤掉最久没
// 被取用的出口,而不是把内存与空闲连接无界攒下去。
func TestExitClientCacheEvictsLeastRecentlyUsed(t *testing.T) {
	c := newExitClientCache(2)
	seen := map[string]*http.Client{}
	for _, tag := range []string{"a", "b"} {
		cl, err := c.get(1, tag, func() (*http.Client, error) { return &http.Client{}, nil })
		if err != nil {
			t.Fatalf("get %s: %v", tag, err)
		}
		seen[tag] = cl
	}
	// a 最近被用过,b 变旧 —— 再插入 c 时必须挤掉 b。
	if cl, _ := c.get(1, "a", func() (*http.Client, error) { return &http.Client{}, nil }); cl != seen["a"] {
		t.Fatal("取用不应重建 client")
	}
	c1, err := c.get(1, "c", func() (*http.Client, error) { return &http.Client{}, nil })
	if err != nil {
		t.Fatalf("get c: %v", err)
	}
	if c1 == seen["b"] {
		t.Fatal("复用了本该被挤掉的 client")
	}
	if n := c.len(); n != 2 {
		t.Fatalf("client 数 = %d, want 2(上限)", n)
	}
	if again, _ := c.get(1, "a", func() (*http.Client, error) { return &http.Client{}, nil }); again != seen["a"] {
		t.Fatal("LRU 挤错了对象:最近用过的 a 不该被丢")
	}
}

// TestExitClientCachePropagatesBuildError:拨不出来的出口不该在缓存里留下一个 nil
// client —— 后续每次取用都要重新真去拨一次,而不是拿到一个空壳。
// TestPoolBoxRebuildsOnlyOnGeneration 钉住 O10 的两半:同代数复用(省掉每请求一次
// 全量 All() + CountryOf 四级正则),代数一变必须重建 —— 否则被淘汰的节点会永久
// 留在候选池里。
func TestPoolBoxRebuildsOnlyOnGeneration(t *testing.T) {
	var b poolBox
	builds := 0
	build := func() []health.PoolNode {
		builds++
		return []health.PoolNode{{Tag: "n1"}}
	}
	first := b.get(1, build)
	second := b.get(1, build)
	if builds != 1 {
		t.Fatalf("同代重建了 %d 次, want 1", builds)
	}
	if &first[0] != &second[0] {
		t.Fatal("同代没有复用同一份切片")
	}
	third := b.get(2, build)
	if builds != 2 {
		t.Fatalf("换代后重建次数 = %d, want 2", builds)
	}
	if &third[0] == &first[0] {
		t.Fatal("换代仍返回旧视图:已淘汰的节点会继续被当成候选")
	}
	// 代数**倒退**的那一侧也要重建:拿着旧代数来取,不能读到新代缓存的视图 ——
	// 那种情形下 build 出来的内容与其代数标注必须一致,否则缓存就永久钉死了。
	fourth := b.get(1, build)
	if builds != 3 {
		t.Fatalf("代数倒退后重建次数 = %d, want 3", builds)
	}
	if &fourth[0] == &third[0] {
		t.Fatal("旧代数读到了新代的视图:缓存从此不再失效")
	}
}

func TestExitClientCachePropagatesBuildError(t *testing.T) {
	c := newExitClientCache(2)
	calls := 0
	boom := http.ErrAbortHandler
	if _, err := c.get(1, "n1", func() (*http.Client, error) { calls++; return nil, boom }); err != boom {
		t.Fatalf("err = %v, want 原样回传", err)
	}
	if n := c.len(); n != 0 {
		t.Fatalf("失败也被缓存了: %d", n)
	}
	if _, err := c.get(1, "n1", func() (*http.Client, error) { calls++; return &http.Client{}, nil }); err != nil {
		t.Fatalf("第二次取用应重新尝试: %v", err)
	}
	if calls != 2 {
		t.Fatalf("build 调用 %d 次, want 2(失败不占缓存)", calls)
	}
}
