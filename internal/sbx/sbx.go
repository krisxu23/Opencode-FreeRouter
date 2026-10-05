// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package sbx hosts sing-box inside this process.
//
// The JS build ran sing-box as a child process and gave every node its own
// local mixed-inbound port: 1146 ports, a 780KB config file, a full `sing-box
// check` and a process restart per rebuild, and the gateway reached each node
// through an HTTP proxy at 127.0.0.1:<port>. Linked in, a node is just an
// adapter.Outbound in this process and "go out through node X" is one
// DialContext call. Nothing listens, nothing is written to disk, and the
// per-node cost drops from a port plus a process to a struct in a map.
package sbx

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"sync"

	boxpkg "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"

	"freerouter/internal/parse"
	sjson "github.com/sagernet/sing/common/json"
	M "github.com/sagernet/sing/common/metadata"
)

// outboundItem 是一次 Start/Sync 候选：已转换成 sing-box 选项的出站连同它的
// tag。Start 的自愈循环要按错误串里的下标剥掉对应项，所以 tag 必须随行。
type outboundItem struct {
	tag string
	ob  option.Outbound
}

// singUnmarshal 用 sing 的上下文解码器把 SingBoxMap 的 JSON 装进具体选项类型。
func singUnmarshal(raw []byte, v any) error {
	return sjson.UnmarshalContext(context.Background(), raw, v)
}

// Dialer opens a connection through one named outbound. It is a type alias on
// purpose: httpclient.Dialer is an alias of the same unnamed function type, so
// the two are the same type and every call site passes one where the other is
// expected without a conversion. nodeprobe therefore never needs to import
// this package — the alias breaks the only same-layer dependency.
type Dialer = func(ctx context.Context, network, addr string) (net.Conn, error)

// Host is one running sing-box instance plus the tag bookkeeping around it.
// Every exported method takes h.mu: sync adds/removes race with dialers being
// handed out, and a dialer that outlives its outbound is fine (the interface
// value is copied out), but a torn tag map is not.
type Host struct {
	mu  sync.RWMutex
	box *boxpkg.Box
	// ctx 是 Start 时装饰过的 context：box.New 会在它身上注册 log factory、
	// network manager 等服务，事后 Manager.Create 构建新出站时必须拿到同一个
	// 实例——裸 context.Background() 会让 dialer.NewWithOptions 解引用空指针。
	ctx       context.Context
	tags      map[string]struct{}
	directTag string
	logf      func(level, msg string)
}

// NewHost returns a host. logf receives sing-box-facing warnings (skipped
// outbounds, self-heal drops) as (level, message); nil disables reporting.
func NewHost(logf func(level, msg string)) *Host {
	return &Host{
		tags: map[string]struct{}{},
		logf: logf,
	}
}

// 出站注册表只构建一次。include.OutboundRegistry() 每次调用都会把全部协议
// 重新注册进一张新表——makeOutbound 每节点都调它的话，重建一轮 2700 个节点
// 就要白付 2700 次注册成本。
var (
	regOnce sync.Once
	reg     *outbound.Registry
)

func outboundRegistry() *outbound.Registry {
	regOnce.Do(func() { reg = include.OutboundRegistry() })
	return reg
}

// Start brings up the box with the given outbounds plus a direct outbound.
//
// A node that fails to convert is skipped and reported, never fatal: the pool
// holds thousands of nodes and one unsupported protocol must not take the
// gateway down. Start returns an error only when the box itself cannot run.
//
// 转换成功 ≠ initialize 成功：sanitize 覆盖的是已知致命类（未知 cipher、非法
// uuid、未知传输），sing-box 在构建出站时仍可能因 Extra 里的罕见字段拒绝。
// JS 版靠 `sing-box check` 自愈循环兜这一层；进程内等价物是解析
// `initialize outbound[i]` 的错误串，把下标对应的出站剥掉重试（上限 5 次，
// 与 JS 的自愈预算同量级）。
func (h *Host) Start(ctx context.Context, outs []parse.Outbound) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.box != nil {
		return fmt.Errorf("sbx: 已在运行")
	}
	h.directTag = "direct"
	h.tags = map[string]struct{}{}

	items := make([]outboundItem, 0, len(outs)+1)
	if ob, err := makeOutbound("direct", parse.Outbound{Type: "direct"}); err == nil {
		items = append(items, outboundItem{tag: "direct", ob: ob})
	} else if h.logf != nil {
		h.logf("warn", "sbx: direct 出站构建失败: "+err.Error())
	}
	for _, o := range outs {
		if o.Tag == "" || o.Tag == h.directTag {
			// 内置 direct 已占这个 tag：订阅真给它一个同名节点时只能让位，
			// 重复 tag 会让 box.New 直接失败。
			continue
		}
		ob, err := makeOutbound(o.Tag, o)
		if err != nil {
			if h.logf != nil {
				h.logf("warn", fmt.Sprintf("sbx: 跳过无法装载的节点 %s: %v", o.Tag, err))
			}
			continue
		}
		items = append(items, outboundItem{tag: o.Tag, ob: ob})
	}

	boxCtx := include.Context(ctx)
	for attempt := 0; ; attempt++ {
		opts := h.buildOptions(items)
		b, err := boxpkg.New(boxpkg.Options{Context: boxCtx, Options: opts})
		if err == nil {
			if err := b.Start(); err != nil {
				// New 已在 ctx 上注册 log factory、network manager 等服务,
				// Start 半途失败会把已启动的那部分服务留在进程里 —— Close
				// 收掉,别让测试/嵌入场景泄漏 goroutine 与 fd。
				_ = b.Close()
				return fmt.Errorf("sbx: 启动: %w", err)
			}
			h.box = b
			h.ctx = boxCtx
			for _, it := range items {
				if it.tag != h.directTag {
					h.tags[it.tag] = struct{}{}
				}
			}
			return nil
		}
		idx, dropTag := initFailureIndex(err.Error(), items)
		if idx < 0 || attempt >= 5 {
			return fmt.Errorf("sbx: 建 box: %w", err)
		}
		if h.logf != nil {
			h.logf("warn", fmt.Sprintf("sbx: sing-box 拒绝出站 %s，已剔除重试: %v", dropTag, err))
		}
		items = append(items[:idx], items[idx+1:]...)
	}
}

// buildOptions 组装 box 的 options：只有 outbounds + 一条兜底 route。
//
//   - 没有 inbounds：入站是网关自己的 HTTP server（forward），不是 sing-box 的。
//   - 路由只指定兜底出站，不写 rules：我们不按域名分流，只按调用方指定的
//     tag 走，Dialer(tag) 已经做完了选择，rules 在这里是重复表达。
//   - Log 关掉：sing-box 的日志走它自己的 factory，禁用后错误仍以 error
//     返回值的形式出来，不会被静默。
func (h *Host) buildOptions(items []outboundItem) option.Options {
	obs := make([]option.Outbound, 0, len(items))
	for _, it := range items {
		obs = append(obs, it.ob)
	}
	return option.Options{
		Log:       &option.LogOptions{Disabled: true},
		Outbounds: obs,
		Route: &option.RouteOptions{
			Final: h.directTag,
			Rules: []option.Rule{},
		},
	}
}

// initFailureIndex 在 box.New 的错误串里找 `initialize outbound[i]`，返回 i
// 与对应 tag；找不到返回 -1。错误形态见 src/singbox.js parseCheckError 的
// 注释——decode 形态（outbounds[N]）在进程内配置下不会出现，因为具体
// options 对象是我们手工构建的，没有 JSON decode 一步。
var initFailureRe = regexp.MustCompile(`initialize outbound\[(\d+)\]`)

func initFailureIndex(msg string, items []outboundItem) (int, string) {
	m := initFailureRe.FindStringSubmatch(msg)
	if m == nil {
		return -1, ""
	}
	i, err := strconv.Atoi(m[1])
	if err != nil || i < 0 || i >= len(items) {
		return -1, ""
	}
	return i, items[i].tag
}

// makeOutbound converts one of ours into sing-box's option wrapper.
//
// Two things are load-bearing here:
//
//   - The concrete options type comes from the registry, not from a type
//     switch. The JS build had a hand-written mapping per protocol and every
//     protocol sing-box added since was silently dropped. registry.CreateOptions
//     returns *option.VLESSEndboundOptions etc., so "what fields does vless
//     have" is answered by the linked library rather than by this file.
//   - box.New 直接把 option.Outbound.Options（any）传给出站构造函数，构造函数
//     对它做具体类型断言——传 option.Outbound 包装类型会 panic：
//     `interface conversion: interface {} is option.Outbound, not *option.VLESSEndboundOptions`。
//
// 整形顺序是刻意的：先 sanitize（fail-closed，修掉 sing-box 必拒的方言形状），
// 再生成 map。direct 没有 server、不走 sanitize 白名单，单独放行。
func makeOutbound(tag string, o parse.Outbound) (option.Outbound, error) {
	if o.Type != "direct" {
		if parse.IsUnroutableServer(o.Server) {
			// 最后一道闸：宿主不负责修数据，但它绝不能把一个 127.0.0.1
			// 出站交给 sing-box——那会把网关自己的出站变成环。
			return option.Outbound{}, fmt.Errorf("地址不可路由: %s", o.Server)
		}
		sanitized, ok := parse.SanitizeOutbound(o)
		if !ok {
			return option.Outbound{}, fmt.Errorf("无法整形为 sing-box 配置")
		}
		o = sanitized
	}
	// SingBoxMap 产出的是 JS 配置形状（含顶层 type/tag）；具体选项类型上没有
	// 这两个字段（它们属于 option.Outbound 包装层），且具体类型的解码是
	// DisallowUnknownFields 的——不剥掉必然报 `json: unknown field "tag"`。
	m := o.SingBoxMap(tag)
	delete(m, "type")
	delete(m, "tag")
	raw, err := json.Marshal(m)
	if err != nil {
		return option.Outbound{}, err
	}
	optsAny, ok := outboundRegistry().CreateOptions(o.Type)
	if !ok {
		return option.Outbound{}, fmt.Errorf("未知协议 %q", o.Type)
	}
	// 用 sing 的上下文解码器而不是 encoding/json：sing-box 的选项类型大量
	// 实现 UnmarshalJSONContext（Durationish 等），标准解码器认不出。
	// 解码本身是严格模式（sing-box 的解码契约）：sing-box 认识但我们没建模的
	// 字段（multiplex、domain_strategy…）由具体类型照常接住——这正是 Extra
	// 透传的真实含义；连 sing-box 都不认识的字段会在这里报错、节点被剔除，
	// 与 JS 版 `sing-box check` 自愈循环的结局一致，只是更早、更便宜。
	if err := singUnmarshal(raw, optsAny); err != nil {
		return option.Outbound{}, fmt.Errorf("装载 %s: %w", tag, err)
	}
	return option.Outbound{Type: o.Type, Tag: tag, Options: optsAny}, nil
}

// Dialer returns a dialer that exits through the named node.
//
// Note what is not here: no port, no proxy URL, no socks handshake. The
// returned func hands the address to sing-box's own dialer, which speaks the
// node's protocol. This is why the gateway needs no local listening ports.
func (h *Host) Dialer(tag string) (Dialer, error) {
	h.mu.RLock()
	b := h.box
	var d adapter.Outbound
	if b != nil {
		if ob, ok := b.Outbound().Outbound(tag); ok {
			d = ob
		}
	}
	h.mu.RUnlock()
	if b == nil {
		return nil, fmt.Errorf("sbx: 尚未启动")
	}
	if d == nil {
		return nil, fmt.Errorf("sbx: 没有出站 %q", tag)
	}
	// 闭包持有的是出站接口值的拷贝：注册表在 Remove 时会被改写，持引用会让
	// 一次失败的 SyncOutbounds 把正在服务的请求一起带走。
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialContext(ctx, network, M.ParseSocksaddr(addr))
	}, nil
}

// Has reports whether tag is a loaded (user-supplied or AddDirect) outbound.
// 内置 direct 不进这张表：它不属于池子，Has 直接看 tags 的语义最不容易误用。
func (h *Host) Has(tag string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	_, ok := h.tags[tag]
	return ok
}

// DirectTag returns the tag of the built-in direct outbound, empty before
// Start or after Close.
func (h *Host) DirectTag() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.directTag
}

// SyncOutbounds makes the running box match outs, returning how many were
// added and removed. Outbounds whose tag already exists are left alone:
// re-adding a node mid-flight would drop the connections using it.
//
// 单个出站构建失败只跳过并记日志（与 Start 同一语义）；Remove 失败说明
// box 内部状态已经与账本不一致，这必须升级为 error。
func (h *Host) SyncOutbounds(outs []parse.Outbound) (added, removed int, err error) {
	// O3:出站构建(JSON marshal + sing 上下文解码,数千节点是纯 CPU)在
	// **锁外**做。过去整段坐在 h.mu 写锁里,热插期间所有 Dialer(tag) 的
	// RLock 都被挡住、在途请求的拨号全部排队。锁内只做 Remove/Create 与
	// 账目变更 —— 两段式快照 + 落锁复核,保证与并发的 Start/Close/其他
	// Sync 不重复记账。
	h.mu.RLock()
	box := h.box
	directTag := h.directTag
	existing := make(map[string]struct{}, len(h.tags))
	for tag := range h.tags {
		existing[tag] = struct{}{}
	}
	h.mu.RUnlock()
	if box == nil {
		return 0, 0, fmt.Errorf("sbx: 尚未启动")
	}

	want := map[string]parse.Outbound{}
	for _, o := range outs {
		if o.Tag == "" || o.Tag == directTag {
			continue
		}
		want[o.Tag] = o
	}
	var toRemove []string
	for tag := range existing {
		if _, keep := want[tag]; keep {
			delete(want, tag)
			continue
		}
		toRemove = append(toRemove, tag)
	}
	sort.Strings(toRemove) // map 无序:固定顺序让 Remove 的日志可复现
	type pendingAdd struct {
		tag string
		ob  option.Outbound
	}
	var toAdd []pendingAdd
	for tag, o := range want {
		ob, err := makeOutbound(tag, o)
		if err != nil {
			if h.logf != nil {
				h.logf("warn", fmt.Sprintf("sbx: 跳过无法装载的节点 %s: %v", tag, err))
			}
			continue
		}
		toAdd = append(toAdd, pendingAdd{tag: tag, ob: ob})
	}
	sort.Slice(toAdd, func(i, j int) bool { return toAdd[i].tag < toAdd[j].tag })

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.box == nil {
		return added, removed, fmt.Errorf("sbx: 尚未启动")
	}
	b := h.box
	// 分批落锁:大 churn(1700+ tag)时持写锁逐个 Remove/Create 会让全部
	// Dialer 的 RLock 排队,在途拨号+探测集体 stall。每批后解一小会儿锁让
	// 读侧插空,复核 box 代数(Close 并发时 box 置 nil,剩下的不做了)。
	const syncBatch = 200
	flushBatch := func() bool {
		if h.box == nil || h.box != b {
			return false
		}
		h.mu.Unlock()
		runtime.Gosched()
		h.mu.Lock()
		if h.box == nil || h.box != b {
			return false
		}
		return true
	}
	for i, tag := range toRemove {
		if i > 0 && i%syncBatch == 0 && !flushBatch() {
			return added, removed, fmt.Errorf("sbx: 热插中被关闭")
		}
		if _, still := h.tags[tag]; !still {
			continue // 快照之后已被并发路径摘掉:不重复记账
		}
		if err := b.Outbound().Remove(tag); err != nil {
			return added, removed, fmt.Errorf("sbx: 移除 %s: %w", tag, err)
		}
		delete(h.tags, tag)
		removed++
	}
	for i, it := range toAdd {
		if i > 0 && i%syncBatch == 0 && !flushBatch() {
			return added, removed, fmt.Errorf("sbx: 热插中被关闭")
		}
		if _, exists := h.tags[it.tag]; exists {
			continue // 快照之后已被并发路径装上:不重复 Create
		}
		if err := h.create(b, it.ob); err != nil {
			if h.logf != nil {
				h.logf("warn", fmt.Sprintf("sbx: 新增出站 %s 失败: %v", it.tag, err))
			}
			continue
		}
		h.tags[it.tag] = struct{}{}
		added++
	}
	return added, removed, nil
}

// AddDirect registers one more direct-typed outbound (the probe round uses one
// to reach the liveness sources without burning a node's exit).
func (h *Host) AddDirect(tag string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.box == nil {
		return fmt.Errorf("sbx: 尚未启动")
	}
	if tag == h.directTag {
		return fmt.Errorf("sbx: %q 是内置 direct 的保留 tag", tag)
	}
	if _, exists := h.tags[tag]; exists {
		return nil
	}
	ob, err := makeOutbound(tag, parse.Outbound{Type: "direct"})
	if err != nil {
		return err
	}
	if err := h.create(h.box, ob); err != nil {
		return fmt.Errorf("sbx: 新增 %s: %w", tag, err)
	}
	h.tags[tag] = struct{}{}
	return nil
}

// RemoveDirect removes an AddDirect-registered tag. The built-in direct is
// protected: the fallback dial path depends on it.
func (h *Host) RemoveDirect(tag string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.box == nil {
		return fmt.Errorf("sbx: 尚未启动")
	}
	if tag == h.directTag {
		return fmt.Errorf("sbx: %q 是内置 direct 的保留 tag", tag)
	}
	if err := h.box.Outbound().Remove(tag); err != nil {
		return fmt.Errorf("sbx: 移除 %s: %w", tag, err)
	}
	delete(h.tags, tag)
	return nil
}

func (h *Host) create(b *boxpkg.Box, ob option.Outbound) error {
	// 与 box.New 内部同款：ctx、router、logger 都取自运行中的 box/Start 现场。
	return b.Outbound().Create(h.ctx, b.Router(),
		b.LogFactory().NewLogger("outbound/"+ob.Type+"["+ob.Tag+"]"),
		ob.Tag, ob.Type, ob.Options)
}

// Close shuts the box down. It is safe to call twice: the launcher closes on
// exit and the panel closes on restart, and a panic in that path would hide the
// original problem.
func (h *Host) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.box == nil {
		return nil
	}
	err := h.box.Close()
	h.box = nil
	h.ctx = nil
	h.tags = map[string]struct{}{}
	h.directTag = ""
	return err
}
