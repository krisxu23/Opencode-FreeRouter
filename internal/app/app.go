// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package app is the assembly point: it is the only package allowed to know
// about every layer at once. Its job is wiring and lifetime, not logic —
// anything a test could reasonably assert belongs in the layer that owns it,
// and whatever only this file knows is exactly what wiring is.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"freerouter/internal/adapter"
	"freerouter/internal/catalog"
	"freerouter/internal/effort"
	"freerouter/internal/engine"
	"freerouter/internal/forward"
	"freerouter/internal/gate"
	"freerouter/internal/health"
	"freerouter/internal/httpclient"
	"freerouter/internal/limits"
	"freerouter/internal/logger"
	"freerouter/internal/nodeprobe"
	"freerouter/internal/panel"
	"freerouter/internal/parse"
	"freerouter/internal/persistence"
	"freerouter/internal/registry"
	"freerouter/internal/sbx"
	"freerouter/internal/stats"
	"freerouter/internal/stream"
	"freerouter/internal/sub"
	"freerouter/internal/tracelog"
	"freerouter/internal/upstream"
)

// streamIdleTimeout 是流式回合的空闲截止（B1）。JS 权威
// archive/node/src/http.js:103/:185 默认 300000ms，且 :235 每收到一块就续期。
// 它不是整请求死线：一次正常的回复可以吐 40 秒以上，整请求死线会在中途把它
// 腰斩，而客户端看到的是一个被截断的回复加一个 502。
const streamIdleTimeout = 300 * time.Second

// Settings is the slice of data/settings.json this phase reads.
//
// The JSON names are kept identical to the JS version's so a legacy settings
// file loads with no translation table at all. app reads the file once and
// hands narrow copies to each part; no part re-reads it.
type Settings struct {
	ForwardPort      int      `json:"forwardPort"`
	PanelPort        int      `json:"panelPort"`
	SubURLs          []string `json:"subUrls"`
	Countries        []string `json:"countries"`
	Enabled          bool     `json:"enabled"`
	ProbeEnabled     bool     `json:"probeEnabled"`
	ProbeWorkers     int      `json:"probeWorkers"`
	ProbeIntervalMin int      `json:"probeIntervalMin"`
	EffortLevel      string   `json:"effortLevel"`
	DefaultMaxTokens any      `json:"defaultMaxTokens"`
	MaxAttempts      int      `json:"maxAttempts"`
	MaxWallClockMS   int64    `json:"maxWallClockMs"`
	ForwardKey       string   `json:"forwardKey"`
}

// Parts is the running gateway.
//
// Load returns non-nil only after the forward port can already serve a
// request, so no caller ever has to ask "is it up yet".
type Parts struct {
	Root string
	// Settings 是当前生效的设置。它只允许经 settingsMu 访问 —— 面板 PUT
	// (ApplySettings)与托盘 Reload 写它,而 5 秒一次的 /api/status 轮询、
	// 探测轮、Rebuild 都在读它(B7:无锁读写整结构会让读者看到撕裂值,
	// 例如半个 probeIntervalMin、旧端口配新 key)。
	//
	// 保持指针字段而不是 atomic.Pointer[Settings],是因为测试夹具直接写
	// p.Settings.X(约 25 处);读侧一律走 settingsSnapshot(),写侧一律走
	// setSettings()。
	Settings   *Settings
	settingsMu sync.RWMutex
	Host       *sbx.Host
	Registry   *registry.Registry
	Health     *health.Health
	// Prober 是探测轮次的实现接口(probe.go 定义):生产装 *nodeprobe.Prober,
	// 测试装假实现 —— 真实探测要打外网,而缓存/事故/淘汰逻辑必须能离线断言。
	Prober  Prober
	Engine  *engine.Engine
	Forward *forward.Server
	Panel   *panel.Server
	// StatsStore 是用量记账(任务 21);RecordUsage 回调把引擎的 harness 形状
	// usage 换算成 stats.Record —— model 由引擎一并给到。
	StatsStore *stats.Stats

	// firstFetch is closed once the opening subscription fetch settles, so the
	// placeholder probe ticker can start on the fallback pool immediately
	// instead of waiting out a timeout against an empty world.
	firstFetch chan struct{}

	settingsStore *persistence.Store
	panelLn       net.Listener
	forwardLn     net.Listener
	cancel        context.CancelFunc
	shutdownOnce  sync.Once
	shutdownErr   error

	// ---- 生命周期(B9) ----
	//
	// lifeCtx 是所有后台定时器与重跑回调的根 context。从前 afterFunc 的回调里
	// 写死 context.Background(),于是 Shutdown 取消 ctx 之后这些定时器照样触发、
	// 以全新 context 重入 Rebuild/ProbeNow,继续写注册表、健康表和日志;而
	// Shutdown 也从不 join timersWG,回调可以在关停流程跑完后仍在运行。
	//
	// lifeCtx 由 Load 建立(它的 cancel 就是 p.cancel);StartTimers 把调用方的
	// ctx(生产里是 main 的信号 ctx)桥接进来。
	lifeCtx context.Context

	// timersMu 保护 timers。afterFunc 登记,回调与 Shutdown 摘除 ——
	// 回调跑完必须自己摘掉,否则长时间运行会无限攒定时器指针。
	timersMu sync.Mutex
	timers   []*time.Timer

	// ---- 探测轮次状态(probe.go) ----
	probing         atomic.Bool
	probeRerunMu    sync.Mutex
	probeRerun      bool
	probeRerunForce bool
	tierGate        *gate.Gate
	tierBuckets     tierChains

	// ---- 订阅重建状态(rebuild.go) ----
	rebuildMu       sync.Mutex
	rebuilding      bool
	rebuildQueued   bool
	subFetchRetries int

	rebuildStateMu sync.Mutex
	lastAdded      int
	lastRemoved    int
	lastDropped    int
	lastRebuildAt  int64
	lastRebuildOK  bool
	lastRebuildErr string

	// ---- 目录与 models.dev 覆盖层(rebuild.go / status.go) ----
	catalog         *catalogBox
	upstreamMu      sync.Mutex
	lastUpstreamIDs []string
	overlayMu       sync.Mutex
	overlayByID     map[string]limits.OverlayRow
	limitsMu        sync.Mutex
	limitsRows      int
	limitsFetchedAt int64
	limitsStale     bool

	// ---- 测试接缝(status.go):非 nil 时替换真实时钟与定时器 ----
	clockFn     func() time.Time
	afterFuncFn func(time.Duration, func()) *time.Timer
	waitFn      func(time.Duration) <-chan time.Time
	timersWG    sync.WaitGroup
}

// defaultSettings mirrors src/store.js:99-149 SETTINGS_INITIAL.
//
// Enabled is the one entry that is not literally in the JS table: the JS build
// has no "enabled" key in data/settings.json at all and hard-codes
// enabled:true when it builds the forward config (src/index.js:1004). A Go
// zero value would be false and turn every route of a fresh install into a
// 503, so the default map carries true — and because persistence.Store.Load
// keeps defaults for keys the file is missing, a legacy settings.json that
// never had the key still comes out enabled.
func defaultSettings() Settings {
	return Settings{
		ForwardPort:      3457,
		PanelPort:        3458,
		SubURLs:          []string{},
		Countries:        []string{"US", "JP", "HK", "TW", "KR", "SG"},
		Enabled:          true,
		ProbeEnabled:     true,
		ProbeWorkers:     48,
		ProbeIntervalMin: 30,
		EffortLevel:      effort.DefaultLevel,
		DefaultMaxTokens: nil,
		MaxAttempts:      20,
		MaxWallClockMS:   0,
		ForwardKey:       "",
	}
}

// settingsMap renders Settings through JSON so the store's map keys are
// exactly the struct's json tags. persistence.NewStore only accepts a
// map[string]any initial value; anything else is silently ignored.
func settingsMap(s Settings) map[string]any {
	raw, err := json.Marshal(s)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func settingsFromStore(store *persistence.Store) (Settings, error) {
	m, _ := store.Get().(map[string]any)
	return settingsFromMap(m)
}

// settingsFromMap 把一份设置 map 解成 Settings。B10 的候选校验用它:先确认
// 「补丁 merge 进当前快照」的结果能解出来,再决定要不要落盘。
func settingsFromMap(m map[string]any) (Settings, error) {
	var out Settings
	raw, err := json.Marshal(m)
	if err != nil {
		return out, fmt.Errorf("app: 设置无法序列化: %w", err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("app: 设置格式不对: %w", err)
	}
	return out, nil
}

// Load resolves every part and starts the forward listener. It returns before
// serving; the caller owns Shutdown. On error nothing is left listening: each
// listener opened so far is closed before returning.
//
// The order below is not stylistic. The registry is loaded before the first
// subscription pull because the fail counts inside it are the starting point
// of that pull; the listener is opened last because the moment the port is
// open it must be able to answer for real.
func Load(root string) (*Parts, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("app: 建数据目录 %s: %w", dataDir, err)
	}
	// logger.Init and tracelog.Init both swallow a failing MkdirAll and go
	// quietly file-less. Degradation rule 4 says an unwritable data directory
	// must fail startup instead, so the check has to live here.
	if err := probeWritable(dataDir); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	var closers []func() error
	fail := func(err error) (*Parts, error) {
		for i := len(closers) - 1; i >= 0; i-- {
			_ = closers[i]()
		}
		cancel()
		return nil, err
	}

	logger.Init(filepath.Join(dataDir, "gateway.log"))
	tracelog.Init(filepath.Join(dataDir, "route"))

	// logf adapts the (level, msg) shape that sbx and nodeprobe ask for onto
	// the package-level logger.
	logf := func(level, msg string) {
		switch level {
		case "warn":
			logger.Warn(msg)
		case "error":
			logger.Error(msg)
		default:
			logger.Info(msg)
		}
	}

	// 1. settings
	store := persistence.NewStore("settings", filepath.Join(dataDir, "settings.json"), settingsMap(defaultSettings()))
	if err := store.Load(); err != nil {
		return fail(fmt.Errorf("app: 读设置: %w", err))
	}
	settings, err := settingsFromStore(store)
	if err != nil {
		return fail(err)
	}
	if settings.ForwardKey == "" {
		// 首启就生成密钥并立刻落盘：JS 的 openStore + flush 也是这个时机
		// （src/index.js:56-59），密钥只有在重启之间保持稳定才有意义。
		settings.ForwardKey = forward.GenerateKey()
		store.Update(map[string]any{"forwardKey": settings.ForwardKey})
	}
	if err := store.Flush(); err != nil {
		return fail(fmt.Errorf("app: 写设置: %w", err))
	}

	// 2. sing-box, with an empty pool first: it takes seconds to come up, so
	// it runs in parallel with the subscription pull instead of behind it.
	host := sbx.NewHost(logf)
	if err := host.Start(ctx, nil); err != nil {
		// Degradation rule 3: if sing-box cannot start the process must exit.
		// Falling back to a direct dial would send every request out of the
		// user's real address.
		return fail(fmt.Errorf("app: 起 sing-box: %w", err))
	}
	closers = append(closers, host.Close)

	// 3. registry, then hand its outbounds to the host before pulling anything.
	reg := registry.NewRegistry(filepath.Join(dataDir, "node-registry.json"))
	if err := reg.Load(); err != nil {
		return fail(fmt.Errorf("app: 读注册表: %w", err))
	}
	closers = append(closers, reg.Flush)
	// Materialize the file on a first run. JS's openStore flushes as soon as
	// it is constructed (src/store.js:69), so a fresh data directory always
	// contains every store even before the first node arrives; Go's Load only
	// reads, and an operator (or the acceptance script) looking for the file
	// would otherwise find nothing until the opening subscription settles.
	if err := reg.Flush(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 注册表首次落盘失败（继续启动）: %v", err))
	}
	if _, _, err := host.SyncOutbounds(reg.All()); err != nil {
		return fail(fmt.Errorf("app: 装载注册表出口: %w", err))
	}

	// 4. the opening subscription pull, off the critical path. Degradation
	// rule 1: a total failure keeps the outbounds from step 3 and still opens
	// the listener. The pull itself is launched at step 6.5 — right after
	// `parts` exists — because it has to record its outcome into lastCheck.
	firstFetch := make(chan struct{})

	// 5. health
	h := health.NewHealth(filepath.Join(dataDir, "node-health.json"))
	closers = append(closers, h.Persist)
	// Same reason as the registry above: NewHealth only loads, so write the
	// empty table now and the file exists from the first second of the first
	// run instead of only after the first probe lands.
	if err := h.Persist(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 健康表首次落盘失败（继续启动）: %v", err))
	}

	// 5.5 用量记账。RecordUsage 永不 panic(stats 自己保证),它挂了不能拖累
	// 一次已经成功的回答(src/index.js:172/:202 的两条注释)。
	statStore := stats.New(filepath.Join(dataDir, "stats.json"))
	if err := statStore.Load(); err != nil {
		logger.Warn(fmt.Sprintf("[app] 用量统计读取失败（继续启动）: %v", err))
	}
	closers = append(closers, statStore.Flush)

	// 6. engine, then limits. The plan's wiring table puts limits before the
	// engine, but limits.New takes the stats provider as an argument, so the
	// engine has to exist first. Nothing about limits can block startup:
	// a missing snapshot file is simply an empty one.
	//
	// 目录挂在 catalogBox 上:rebuild.go 的 applyIDs 是唯一换代入口,engine 的
	// State 回调每轮路由读一次 —— 换代对在途请求不可见,它们用旧的一代跑完。
	// 冷启动优先读上一轮落盘的上游 id 列表(data/catalog-ids.json,JS
	// src/index.js:219/235 同名同语义):本机实测直连被封而节点可用,若只能等
	// 上游,面板 opening 就是一张空模型表。缓存里没有才播静态回退表 —— 静态表
	// 18 行,上游实测 34 行,少了 16 个模型也是「用户以为没额度」。
	cachedIDs := catalog.LoadCache(filepath.Join(dataDir, "catalog-ids.json"))
	catList := catalog.Static()
	if len(cachedIDs) > 0 {
		catList = catalog.Build(cachedIDs)
	}
	catBox := &catalogBox{list: catList}
	// parts 先声明后装配:下面几个闭包要读活设置(B7),而 parts 的字段又依赖
	// 它们构造出来的 eng/statStore。闭包只可能在 Load 返回之后被调用,
	// 那时 parts 一定已赋值。
	var parts *Parts
	eng := engine.NewEngine(engine.Deps{
		State: func() engine.State {
			return engine.State{Catalog: catBox.get(), Health: h}
		},
		Pool: func() []health.PoolNode {
			outs := reg.All()
			pool := make([]health.PoolNode, 0, len(outs))
			for _, o := range outs {
				pool = append(pool, health.PoolNode{Tag: o.Tag, Country: parse.CountryOf(o.Tag)})
			}
			return pool
		},
		Settings: func() engine.Settings {
			// 必须读活值:面板上保存的 effortLevel / defaultMaxTokens 会写进
			// 同一份 settings,热路径每请求取一次(B2/B3)。过去 EffortLevel
			// 写死常量、MaxTokens 根本没接线,保存了也不生效;而这里原先读的是
			// Load 的局部副本,面板保存后同样读不到(B7 顺手一起修)。
			live := parts.settingsSnapshot()
			maxTokens, _ := jsonNumberOrNil(live.DefaultMaxTokens)
			return engine.Settings{
				Countries:        live.Countries,
				EffortLevel:      live.EffortLevel,
				DefaultMaxTokens: maxTokens,
				MaxAttempts:      live.MaxAttempts,
				MaxWallClockMS:   live.MaxWallClockMS,
			}
		},
		// RecordUsage:base id 由引擎给到(stream.Usage 是 harness 形状,
		// In 已是未缓存净输入 —— 任务 21 的 a471bcf 裁决)。
		RecordUsage: func(model string, u stream.Usage) {
			statStore.Record(stats.Record{
				At:     time.Now().UnixMilli(),
				Model:  model,
				OK:     true,
				Input:  u.In,
				Output: u.Out,
				TTFTMS: u.TTFTMS,
			})
		},
		Dialer: func(tag string) (*http.Client, error) {
			d, err := host.Dialer(tag)
			if err != nil {
				return nil, err
			}
			// One connection pool per exit: sharing http.DefaultTransport's
			// idle connections would let one exit's socket be reused for
			// another exit's request.
			//
			// 流式回合用的是空闲死线而不是整请求死线（B1）：一次正常的回复
			// 可以吐 40 秒以上，20s 的整请求死线会在第 20 秒把它腰斩。JS 权威
			// 是 300s 空闲（archive/node/src/http.js:103/:185，每块续期）。
			return httpclient.NewStreamClient(d, streamIdleTimeout), nil
		},
		AdapterDeps: adapter.Deps{
			Base: upstream.UpstreamBase,
			// Effort/Entry/Model/Wire/SessionID/NodeKey/Tools/Client are all
			// overwritten per attempt by engine; only the assembly-level
			// template lives here.
			Effort: effort.DefaultLevel,
		},
		RecordTrace: tracelog.Record,
		Log:         func(msg string) { logger.Info(fmt.Sprintf("engine: %s", msg)) },
	})

	// 探测轮次的两个闸门:全局错峰(B 档 60ms 一个时隙)与按出口 IP 的串行链。
	// tierGate 必须是 Parts 上的字段而不是每轮新建 —— 每轮新建会让上一轮的
	// 排队成果全部丢失,开局重新失去错峰(src/index.js:756 的原注释)。
	// 覆盖层磁盘缓存先于一切目录动作装载(JS 启动即 loadLimitsCache,
	// src/index.js:223):boot 目录因此能带上上一份的额度值。
	bootOverlay, bootFetchedAt, _ := limits.LoadOverlayCache(filepath.Join(dataDir, "modelsdev.json"))
	if bootOverlay == nil {
		bootOverlay = map[string]limits.OverlayRow{}
	}
	parts = &Parts{
		Root:        root,
		Settings:    &settings,
		Host:        host,
		Registry:    reg,
		Health:      h,
		Prober:      nodeprobe.NewProber(logf),
		Engine:      eng,
		StatsStore:  statStore,
		firstFetch:  firstFetch,
		tierGate:    gate.New(tierGapMS),
		catalog:     catBox,
		overlayByID: bootOverlay,
		// B8:这两个字段与 limitsMu 保护,必须在结构体字面量里装配好 ——
		// 从前是字面量之后的两行无锁写,而 refreshLimitsOverlay 已经在
		// limitsMu 下读写它们。
		limitsFetchedAt: bootFetchedAt,
		limitsRows:      len(bootOverlay),
		settingsStore:   store,
		cancel:          cancel,
		// B9:生命周期根 context。它的 cancel 就是 p.cancel,所以
		// Shutdown 取消它 = 所有后台定时器与重跑回调同时失去根。
		lifeCtx: ctx,
	}

	if len(bootOverlay) > 0 {
		catBox.set(parts.applyOverlay(catBox.get()))
	}

	// 6.5 开场订阅拉取。仍然是后台跑(监听端口不该等网络),但必须等 parts 建好
	// 再启动:它要把结果记进 lastCheck/lastRebuild。此前这一步在 parts 之前启动,
	// 于是冷启动路径上没有任何人调用 setRebuildResult —— lastRebuildAt 恒为 0、
	// lastCheck.ok 恒为 null,前端 checkBadge/checkAlert 对 ok==null 直接返回空串,
	// 面板顶部的「上次检查」在每次开机后都是永久空白,看起来就像网关什么都没做。
	go func() {
		defer close(firstFetch)
		// 订阅地址与放行国家取**活值**:面板上保存的设置要能影响这一轮,而不是
		// 开机那一瞬间的副本(B7)。监听端口不在此列 —— 端口已经绑定,改端口
		// 必须重启,那是设计而不是遗漏。
		cur := parts.settingsSnapshot()
		subURLs := cur.SubURLs
		if len(subURLs) == 0 {
			logger.Info(fmt.Sprintf("[app] 未配置订阅或全部拉取失败 — 使用注册表历史节点（%d 个）；注册表为空则以纯直连兜底模式启动", reg.Len()))
			parts.setRebuildResult(0, 0, 0, nil)
			return
		}
		exits := make([]sub.Exit, 0, reg.Len())
		for _, o := range reg.All() {
			d, derr := host.Dialer(o.Tag)
			if derr != nil {
				continue
			}
			exits = append(exits, sub.Exit{Name: o.Tag, Dial: d})
		}
		res, ferr := sub.Fetch(ctx, subURLs, exits)
		if ferr != nil {
			logger.Warn(fmt.Sprintf("[app] 订阅失败，沿用 %d 个已知出口: %v", reg.Len(), ferr))
			parts.setRebuildResult(0, 0, 0, ferr)
			return
		}
		picked := parse.FilterByGroups(res.Outbounds, cur.Countries)
		clean := make([]parse.Outbound, 0, len(picked))
		for _, o := range picked {
			if ok, keep := parse.SanitizeOutbound(o); keep {
				clean = append(clean, ok)
			}
		}
		// 订阅里被 sing-box 拒收的节点数(非法 uuid / 不认的 cipher / 未知传输)。
		// 前端拿它显示「剔除 N 个坏节点」,不传过去那条告警就永远不出现。
		dropped := len(picked) - len(clean)
		merged := reg.Merge(clean)
		evicted := reg.EnforceCap(registry.PoolCap)
		if err := reg.Flush(); err != nil {
			logger.Warn(fmt.Sprintf("[app] 注册表落盘失败: %v", err))
		}
		syncAdded, syncRemoved, serr := host.SyncOutbounds(reg.All())
		if dropped > 0 {
			logger.Warn(fmt.Sprintf("[app] 订阅里有 %d 个节点 sing-box 无法使用（非法 uuid / 不认的 cipher / 未知传输），未入池", dropped))
		}
		if serr != nil {
			logger.Warn(fmt.Sprintf("[app] 热插出站失败: %v", serr))
			parts.setRebuildResult(syncAdded, syncRemoved, dropped, serr)
			return
		}
		parts.setRebuildResult(syncAdded, syncRemoved, dropped, nil)
		logger.Info(fmt.Sprintf("[app] 订阅：合并 %d 个出口（新增 %d，淘汰 %d），池内现有 %d 个（热插 %d，撤下 %d）",
			len(clean), merged, evicted, reg.Len(), syncAdded, syncRemoved))
	}()

	// 7. the forward listener, last: the port may only open once it can serve.
	srv := forward.New(forward.Config{
		// B7:两个闭包每请求取一次活快照。它们原先捕获的是 Load 的局部副本,
		// 面板/托盘改完之后仍按开机时的旧值鉴权。
		Enabled:    func() bool { return parts.settingsSnapshot().Enabled },
		ForwardKey: func() string { return parts.settingsSnapshot().ForwardKey },
		Complete: func(cctx context.Context, req engine.Request, onChunk func(engine.Chunk) error) (engine.Outcome, error) {
			// Degradation rule 2. The engine answers an empty pool with a 503
			// -flavoured failure, but forward maps every error from Complete
			// onto 502 (forward.go:330, pinned by task 19's 31 tests) and the
			// JS build never handed a 503 to forward either. The thing the
			// user has to be able to read is "there is no exit", so say it
			// here, before the engine has a chance to describe it as a
			// model-level failure.
			if reg.Len() == 0 {
				return engine.Outcome{}, fmt.Errorf("no usable exit: 注册表里一个出口都没有（订阅未配置或全部失败）")
			}
			return eng.Complete(cctx, req, onChunk)
		},
		ModelRows: func() []engine.Row { return eng.ModelRows() },
		Log:       func(msg string) { logger.Warn(fmt.Sprintf("forward: %s", msg)) },
	})

	fwdLn, err := net.Listen("tcp", listenAddr(settings.ForwardPort))
	if err != nil {
		return fail(fmt.Errorf("app: 监听转发端口 %d: %w", settings.ForwardPort, err))
	}
	closers = append(closers, fwdLn.Close)
	go func() {
		if err := srv.Serve(fwdLn); err != nil {
			logger.Error(fmt.Sprintf("[app] 转发监听退出: %v", err))
		}
	}()
	closers = append(closers, srv.Close)
	parts.Forward = srv
	parts.forwardLn = fwdLn

	// 8. the panel port — the real console now, not the phase-3 stand-in.
	// PanelDeps 全是函数值,panel 不认识 engine/health/registry 的任何类型
	// (它自己也是 L5,同层不 import app,方向永远 app → panel)。
	panelLn, err := net.Listen("tcp", listenAddr(settings.PanelPort))
	if err != nil {
		return fail(fmt.Errorf("app: 监听面板端口 %d: %w", settings.PanelPort, err))
	}
	closers = append(closers, panelLn.Close)
	console := panel.New(panel.PanelDeps{
		Status:      parts.Status,
		GetSettings: func() any { return parts.SettingsView() },
		ApplySettings: func(patch map[string]any) (any, error) {
			// B10:错误必须透传到面板(→ 400),不能吞成一行 warn ——
			// 吞掉之后前端照样 toast「已保存并应用」,而改动从未生效。
			if _, err := parts.ApplySettings(patch); err != nil {
				return nil, err
			}
			return parts.SettingsView(), nil
		},
		Actions: panel.PanelActions{
			ProbeNow: func(ctx context.Context, force bool) error {
				_, err := parts.ProbeNow(ctx, force)
				return err
			},
			// Refresh/RefreshLimits 的签名没有 ctx:重建自带 180s 总预算,
			// 限额刷新自带单次超时,都不需要外层取消。但根必须是 lifeCtx
			// 而不是 context.Background()(B9):面板上的手动刷新一旦发生在
			// 关停之后,不能再以全新 context 重入重建、继续写注册表。
			Refresh:       func() error { return parts.Rebuild(parts.ctx()) },
			RefreshLimits: func() error { return parts.refreshLimitsOverlay(parts.ctx()) },
		},
		Logs:        logger.Recent,
		RouteRecent: tracelog.Recent,
		Version:     Version,
		Log:         func(msg string) { logger.Info(fmt.Sprintf("panel: %s", msg)) },
		Limits:      parts.LimitsView,
	})
	go func() {
		if err := console.Serve(panelLn); err != nil && err != http.ErrServerClosed {
			logger.Error(fmt.Sprintf("[app] 面板监听退出: %v", err))
		}
	}()
	closers = append(closers, console.Close)

	parts.Panel = console
	parts.panelLn = panelLn

	logger.Info(fmt.Sprintf("[app] 注册表 %d 个出口（本进程 0 个本地端口）", reg.Len()))
	logger.Info(fmt.Sprintf("[app] 转发端口 %d 已监听", portOf(fwdLn)))
	logger.Info(fmt.Sprintf("[app] 面板端口 %d 已监听（控制台资产已内置于 exe）", portOf(panelLn)))
	return parts, nil
}

// Shutdown stops the listener, flushes every store and closes the sing-box
// host. Safe to call twice, and safe on the zero Parts after a failed Load.
//
// B9:它同时要**收回后台定时器**。从前这里丢掉传入的 ctx,也从不 join
// timersWG,于是关停之后排着的 afterFunc 仍会触发,并且用写死的
// context.Background() 重入 Rebuild/ProbeNow —— 托盘「退出」之后还在写
// node-registry.json、node-health.json 与网关日志。现在顺序是:取消 lifeCtx
// (所有回调的根)→ 停掉待触发定时器 → join 定时器与循环 → 才关监听/落盘/
// 关 host。join 用调用方的 ctx 兜底,免得一个卡在慢网络里的在途轮次把退出
// 永久挂住。
func (p *Parts) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.shutdownOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		p.stopPendingTimers()
		if ctx != nil {
			done := make(chan struct{})
			go func() {
				p.timersWG.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
			}
		} else {
			p.timersWG.Wait()
		}
		if p.Forward != nil {
			_ = p.Forward.Close()
		}
		if p.forwardLn != nil {
			_ = p.forwardLn.Close()
		}
		if p.Panel != nil {
			_ = p.Panel.Close()
		}
		if p.panelLn != nil {
			_ = p.panelLn.Close()
		}
		// Flush before closing the host: an exit that is still in the registry
		// must survive the restart even though its socket just went away.
		flushes := []struct {
			name string
			fn   func() error
		}{
			{"health", func() error {
				if p.Health == nil {
					return nil
				}
				return p.Health.Persist()
			}},
			{"registry", func() error {
				if p.Registry == nil {
					return nil
				}
				return p.Registry.Flush()
			}},
			{"settings", func() error {
				if p.settingsStore == nil {
					return nil
				}
				return p.settingsStore.Flush()
			}},
			{"stats", func() error {
				if p.StatsStore == nil {
					return nil
				}
				return p.StatsStore.Flush()
			}},
		}
		for _, f := range flushes {
			if err := f.fn(); err != nil && p.shutdownErr == nil {
				p.shutdownErr = fmt.Errorf("app: 关停时落盘 %s: %w", f.name, err)
			}
		}
		if p.Host != nil {
			if err := p.Host.Close(); err != nil && p.shutdownErr == nil {
				p.shutdownErr = fmt.Errorf("app: 关停 sing-box: %w", err)
			}
		}
	})
	return p.shutdownErr
}

// RootDir resolves where data/ lives. FREEROUTER_DATA wins so a test or an
// operator can point the gateway at an empty directory; otherwise data/ sits
// next to the executable.
func RootDir() (string, error) {
	if v := strings.TrimSpace(os.Getenv("FREEROUTER_DATA")); v != "" {
		if filepath.IsAbs(v) {
			return filepath.Clean(v), nil
		}
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", fmt.Errorf("app: FREEROUTER_DATA=%q 不是可用路径: %w", v, err)
		}
		return abs, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("app: 找不到可执行文件: %w", err)
	}
	// A symlinked or deleted executable must not stop the gateway: keep the
	// path os.Executable handed us and resolve it only when possible.
	//
	// 返回值是「包含 data/ 的那一层」,不是 data/ 本身 —— Load(root) 会再拼
	// 一层 data。这里若返回 <exeDir>/data,生产双击就会把数据落进
	// <exeDir>/data/data(JS 版的布局是 exe 旁一份 data)。任务 25 验收时
	// 实测发现,修于此处。
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}
	return filepath.Dir(exe), nil
}

// probeWritable proves the data directory can actually be written to. Both
// logger.Init and tracelog.Init report a failed MkdirAll by silently going
// file-less, so without this check a read-only data directory would start a
// gateway that logs nowhere and remembers nothing.
func probeWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".freerouter-write-probe-*")
	if err != nil {
		return fmt.Errorf("app: 数据目录 %s 不可写: %w", dir, err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

func listenAddr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

func portOf(ln net.Listener) int {
	if tcp, ok := ln.Addr().(*net.TCPAddr); ok {
		return tcp.Port
	}
	return 0
}
