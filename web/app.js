/**
 * Browser-side console app, served as a static asset at GET /app.js.
 *
 * Kept as a real .js file rather than an inlined template literal on purpose:
 * the previous single-file version had to be written entirely in string
 * concatenation, because any `${` or `\n` inside `PAGE` got evaluated by the
 * template literal before reaching the browser — which once produced a
 * SyntaxError that silently blanked the whole console. Here the file is read
 * verbatim and sent as-is, so what you write is what the browser parses.
 *
 * Data flow:
 *   window.__BOOT__  ← server-injected snapshot (status + settings + logs),
 *                      so the first paint already has real numbers
 *   /api/status      ← polled every 5s
 *   /api/logs        ← polled every 15s
 *
 * @module src/panel-client.js
 */
(function () {
  'use strict'

  /* ═══════════════════════════════════════════════════════════════════
     1. 常量
     ═══════════════════════════════════════════════════════════════════ */
  const CC_NAME = {
    US:'美国', JP:'日本', HK:'中国香港', TW:'中国台湾', KR:'韩国', SG:'新加坡',
    CN:'中国', DE:'德国', NL:'荷兰', SE:'瑞典', FR:'法国', FI:'芬兰', CH:'瑞士',
    DK:'丹麦', HU:'匈牙利', IN:'印度', MX:'墨西哥', VN:'越南', RU:'俄罗斯',
    CA:'加拿大', PL:'波兰', NO:'挪威', CZ:'捷克', LV:'拉脱维亚', LT:'立陶宛',
    EE:'爱沙尼亚', IT:'意大利', ES:'西班牙', GB:'英国', AT:'奥地利', RO:'罗马尼亚',
    TH:'泰国', ZA:'南非', BR:'巴西', PE:'秘鲁', IE:'爱尔兰', TR:'土耳其',
    UA:'乌克兰', AE:'阿联酋', AU:'澳大利亚', NZ:'新西兰', MY:'马来西亚',
    ID:'印度尼西亚', PH:'菲律宾', PK:'巴基斯坦', BD:'孟加拉', IL:'以色列',
    SA:'沙特', AR:'阿根廷', CL:'智利', CO:'哥伦比亚', PT:'葡萄牙', GR:'希腊',
    BE:'比利时', LU:'卢森堡', SK:'斯洛伐克', SI:'斯洛文尼亚', HR:'克罗地亚',
    BG:'保加利亚', RS:'塞尔维亚', MD:'摩尔多瓦', KZ:'哈萨克斯坦', AM:'亚美尼亚',
    GE:'格鲁吉亚', AZ:'阿塞拜疆', MN:'蒙古', NP:'尼泊尔', LK:'斯里兰卡',
    KH:'柬埔寨', LA:'老挝', MM:'缅甸', BN:'文莱', MO:'中国澳门', CY:'塞浦路斯',
    MT:'马耳他', IS:'冰岛'
  }
  /* 必须与 src/sub.js 的 EU_CCS / GROUPS 保持一致，否则筛选出来的数字对不上后端分桶 */
  const EU_CCS = ['NL','DE','GB','FR','SE','CH','AT','PL','ES','IT','IE','FI','NO','UA','RO','BG','GR','HU','CZ','DK','BE','PT']
  const BUCKETS = [
    { id:'US', name:'美国' }, { id:'JP', name:'日本' },
    { id:'HK', name:'中国香港' }, { id:'TW', name:'中国台湾' },
    { id:'KR', name:'韩国' }, { id:'SG', name:'新加坡' },
    { id:'EU', name:'欧洲' }, { id:'OTHER', name:'其他' }
  ]
  const TITLES = { overview:'概览', nodes:'出口节点', models:'免费模型', usage:'用量', logs:'日志', settings:'设置' }
  const LAT_CAP = 10000

  /* 已知无害的日志模式：代码自己标注「属正常噪音」的，以及探测死节点时的远端断连。
     折叠它们，但绝不折叠真正的错误。 */
  const NOISE_PATTERNS = [
    /拨号失败 \d+ 次（多为探测死节点，属正常噪音）/,
    /connection upload closed: write tcp .* wsasend: An existing connection was forcibly closed/
  ]

  /* ═══════════════════════════════════════════════════════════════════
     2. 工具
     ═══════════════════════════════════════════════════════════════════ */
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"'`]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;','`':'&#96;' }[c]))
  }
  function bucketOf(cc) {
    const c = String(cc || '').toUpperCase()
    if (BUCKETS.some(b => b.id === c)) return c
    if (EU_CCS.indexOf(c) >= 0) return 'EU'
    return 'OTHER'
  }
  /* Windows 的 Segoe UI Emoji 不含国旗字形，🇩🇪 会退化成「DE」两个字母。
     国家信息另有「地区」列承载，所以统一改用国家码芯片。 */
  function ccChip(cc) {
    const c = String(cc || '').toUpperCase()
    if (!/^[A-Z]{2}$/.test(c)) return '<span class="cc zero">??</span>'
    return '<span class="cc">' + c + '</span>'
  }
  /* 节点 tag 自带国旗 emoji（订阅给的），在 Windows 上是字母噪音。
     剥掉纯 emoji，保留 CN-SG 这类有意义的文字。 */
  function stripRI(s) {
    return String(s == null ? '' : s)
      .replace(/[\u{1F1E6}-\u{1F1FF}]/gu, '')
      .replace(/\s{2,}/g, ' ')
      .replace(/\s+([-_|])/g, '$1')
      .trim()
  }
  function fmtTok(n) {
    if (!isFinite(n) || n <= 0) return '0'
    if (n >= 1e6) return (n / 1e6).toFixed(n % 1e6 === 0 ? 0 : 2) + 'M'
    if (n >= 1e3) {
      /* 999500 起就切 M：否则 999999 会显示成「1000K」—— K 的下一个刻度越过了 M */
      if (n / 1e3 >= 999.5) return (n / 1e6).toFixed(2) + 'M'
      return Math.round(n / 1e3) + 'K'
    }
    return String(n)
  }
  function fmtLat(ms) {
    if (!isFinite(ms) || ms < 0) return '—'
    return ms < 1000 ? ms + 'ms' : (ms / 1000).toFixed(1) + 's'
  }
  function fmtTime(t) { return t ? new Date(t).toLocaleTimeString('zh-CN', { hour12: false }) : '—' }
  function fmtDT(t) { return t ? new Date(t).toLocaleString('zh-CN', { hour12: false }) : '—' }
  function latClass(ms) { return ms < 1500 ? 'f-fast' : ms > 5000 ? 'f-slow' : '' }
  /* 延迟跨度实测 0.35s–21s（60 倍）。线性刻度让快节点全部贴地不可见，
     纯对数又把快端挤在一起。√ 压缩 + 封顶 10s 效果最好：
       0.35s→19%  0.7s→27%  1.2s→35%  2.5s→50%  5s→71%  10s+→100% */
  function latW(ms) {
    if (ms < 0) return 0
    return Math.max(6, Math.min(100, Math.sqrt(Math.min(ms, LAT_CAP) / LAT_CAP) * 100))
  }
  function isNoise(msg) {
    for (let i = 0; i < NOISE_PATTERNS.length; i++) if (NOISE_PATTERNS[i].test(msg)) return true
    return false
  }
  /* lanes 是出口车道视图 {ip: {busy, waiting, limit}}（ExitConcurrency 闸门）。
     渲染成表行；没有任何活动时返回空串（实时负载卡整体隐藏）。 */
  function laneRows(lanes) {
    const keys = Object.keys(lanes || {}).sort()
    if (!keys.length) return ''
    return keys.map(function (ip) {
      const l = lanes[ip]
      return '<tr><td class="mono">' + esc(ip) + '</td><td>' + l.busy + '</td>'
        + '<td>' + (l.waiting || 0) + '</td><td>' + (l.limit > 0 ? l.limit : '∞') + '</td></tr>'
    }).join('')
  }
  const $ = id => document.getElementById(id)

  /* ═══════════════════════════════════════════════════════════════════
     3. 状态
     ═══════════════════════════════════════════════════════════════════ */
  const DATA = Object.assign(
    { singbox:{}, forward:{}, lanes:{}, models:[], modelCaps:{}, limits:null, regionModels:[],
      nodes:[], usage:{ today:{ req:0, in:0, out:0 }, requests:0, byModel:{} },
      diagnostics:{}, settings:{}, logs:[] },
    window.__BOOT__ || {}
  )

  const S = {
    view: 'overview',
    nodeQ: '', nodeState: 'all', nodeBucket: 'all', nodeSort: 'latency', nodeSortDir: 1,
    logLevel: 'all', logQ: '', logDedupe: true, logFoldNoise: true, logAuto: true,
    order: [], form: {}, baseline: null,
    probing: false, busy: false,
    routes: []  /* 请求轨迹(/api/routes),日志页展示 —— README 承诺的「每次请求走了哪个出口」 */
  }

  function readSettings() {
    const s = DATA.settings || {}
    S.order = (s.countries || []).slice()
    S.form = {
      subUrls: (s.subUrls || []).join('\n'),
      probeEnabled: s.probeEnabled !== false,
      effortLevel: s.effortLevel || 'balanced',
      defaultMaxTokens: s.defaultMaxTokens == null ? '' : String(s.defaultMaxTokens),
      probeWorkers: String(s.probeWorkers == null ? 24 : s.probeWorkers),
      /* 1.3.0:probeIntervalMin 退役,被三档间隔取代(热区复检/冷区扫描/
         订阅刷新)。旧键既不进表单也不再回写 —— 盘上的遗留值由后端忽略。 */
      refreshIntervalMin: String(s.refreshIntervalMin == null ? 30 : s.refreshIntervalMin),
      hotIntervalSec: String(s.hotIntervalSec == null ? 60 : s.hotIntervalSec),
      coldIntervalSec: String(s.coldIntervalSec == null ? 300 : s.coldIntervalSec),
      maxWallClockMs: String(s.maxWallClockMs == null ? 0 : s.maxWallClockMs),
      exitConcurrency: String(s.exitConcurrency == null ? 0 : s.exitConcurrency),
    }
    S.baseline = JSON.parse(JSON.stringify({ order: S.order, form: S.form }))
  }

  /* ═══════════════════════════════════════════════════════════════════
     4. 派生数据
     ═══════════════════════════════════════════════════════════════════ */
  let D = null
  function derive() {
    const nodes = DATA.nodes || []
    const alive = nodes.filter(n => n.state === 'alive')
    // 2026-10-06:5秒轮询不带 nodes（?include_nodes=false），概览计数用 lastCheck。
    // nodes 数组只在初始 bootstrap 和节点页手动刷新时有。
    const lc = (DATA.singbox && DATA.singbox.lastCheck) || {}
    const totalCount = lc.nodes != null ? lc.nodes : nodes.length
    const aliveCount = lc.alive != null ? lc.alive : alive.length
    const lats = alive.map(n => n.latencyMs).filter(x => x >= 0).sort((a, b) => a - b)
    const bucketCount = {}
    for (const n of nodes) {
      const b = bucketOf(n.country)
      if (!bucketCount[b]) bucketCount[b] = { total:0, alive:0, b:0 }
      bucketCount[b].total++
      if (n.state === 'alive') {
        bucketCount[b].alive++
        if (n.tier === 'B') bucketCount[b].b++
      }
    }
    const models = (DATA.models || []).map(id => Object.assign(
      { id: id }, DATA.modelCaps && DATA.modelCaps[id] ? DATA.modelCaps[id] : {},
      { gated: (DATA.regionModels || []).indexOf(id) >= 0 }
    ))
    const usage = DATA.usage || { today:{ req:0, in:0, out:0 }, requests:0, byModel:{}, byExit:{} }
    const rows = Object.keys(usage.byModel || {})
      .map(m => Object.assign({ model: m }, usage.byModel[m]))
      .sort((a, b) => b.req - a.req)
    const history = Array.isArray(usage.history) ? usage.history : []
    const todayKey = new Date().toISOString().slice(0, 10)
    D = {
      nodes: nodes, alive: alive,
      // 2026-10-06:概览计数用 lastCheck（轮询不带 nodes 时保持更新）
      nodeTotal: totalCount, nodeAlive: aliveCount,
      // 车道视图直传：渲染层读 D.lanes（实时负载卡）。DATA.lanes 由
      // absorbStatus 随 /api/status 更新；缺省空对象让 laneRows 返回空串。
      lanes: DATA.lanes || {},
      tierB: alive.filter(n => n.tier === 'B').length,
      tierA: alive.filter(n => n.tier === 'A').length,
      bucketCount: bucketCount, models: models, usage: usage, rows: rows,
      history: history, historyMax: Math.max(1, history.map(h => h.req).concat([1]).reduce((a, b) => Math.max(a, b), 1)),
      todayKey: todayKey,
      maxIn: Math.max(1, rows.map(r => r.in + r.out).concat([1]).reduce((a, b) => Math.max(a, b), 1)),
      latMin: lats.length ? lats[0] : -1,
      latMed: lats.length ? lats[lats.length >> 1] : -1,
      latMax: lats.length ? lats[lats.length - 1] : -1,
      fast: alive.filter(n => n.latencyMs >= 0 && n.latencyMs < 1500).length,
      mid: alive.filter(n => n.latencyMs >= 1500 && n.latencyMs <= 5000).length,
      slow: alive.filter(n => n.latencyMs > 5000).length,
      probe: probeFromLogs()
    }
  }
  /* 探测摘要直接解析日志，保证面板与日志里的数字永远同源 */
  function probeFromLogs() {
    const re = /probe round:\s*(\d+)\/(\d+)\s+alive\s*\(A\s*(\d+)[^0-9]*B\s*(\d+)[^)]*\)\s*in\s*([\d.]+)s/
    const logs = DATA.logs || []
    for (let i = logs.length - 1; i >= 0; i--) {
      const m = re.exec(logs[i].msg || '')
      if (m) return { alive:+m[1], scanned:+m[2], a:+m[3], b:+m[4], secs:+m[5], at:logs[i].t }
    }
    return null
  }

  /* ═══════════════════════════════════════════════════════════════════
     5. 图标
     ═══════════════════════════════════════════════════════════════════ */
  const I = {
    gauge:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 14a2 2 0 1 0 0-4 2 2 0 0 0 0 4Z"/><path d="M13.4 10.6 19 5"/><path d="M20.5 16a9 9 0 1 0-17 0"/></svg>',
    nodes:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="5" r="2.2"/><circle cx="5" cy="19" r="2.2"/><circle cx="19" cy="19" r="2.2"/><path d="M12 7.2v4.3M12 11.5 6.6 17M12 11.5 17.4 17"/></svg>',
    cube:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2.8 20 7v10l-8 4.2L4 17V7l8-4.2Z"/><path d="M4 7l8 4.2L20 7M12 11.2v10"/></svg>',
    chart:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg>',
    scroll:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 4h11a2 2 0 0 1 2 2v12a2 2 0 0 0 2 2H7a2 2 0 0 1-2-2V4Z"/><path d="M8 8h7M8 12h7M8 16h4"/></svg>',
    gear:'<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-2.7 1.1v.2a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-1-1.5 1.6 1.6 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0-1.1-2.7H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.5-1 1.6 1.6 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 1.8.3H9a1.6 1.6 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 2.7 1.1l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0-.3 1.8V9a1.6 1.6 0 0 0 1.5 1h.2a2 2 0 1 1 0 4h-.1a1.6 1.6 0 0 0-1.5 1Z"/></svg>',
    search:'<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>',
    copy:'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>',
    check:'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m4 12.5 5 5L20 6.5"/></svg>',
    bolt:'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 4.5 13.5H11l-1 8.5 8.5-11.5H12l1-8.5Z"/></svg>',
    refresh:'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.6-6.4"/><path d="M21 3v6h-6"/></svg>',
    grip:'<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><circle cx="9" cy="6" r="1.6"/><circle cx="15" cy="6" r="1.6"/><circle cx="9" cy="12" r="1.6"/><circle cx="15" cy="12" r="1.6"/><circle cx="9" cy="18" r="1.6"/><circle cx="15" cy="18" r="1.6"/></svg>',
    warn:'<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M10.3 3.9 1.9 18a2 2 0 0 0 1.7 3h16.8a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0Z"/><path d="M12 9v4M12 17h.01"/></svg>',
    info:'<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01"/></svg>',
    star:'<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="m12 2.5 2.9 6 6.6.9-4.8 4.6 1.2 6.5L12 17.4 6.1 20.5l1.2-6.5L2.5 9.4l6.6-.9 2.9-6Z"/></svg>',
    up:'<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 15 6-6 6 6"/></svg>',
    down:'<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>',
    x:'<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M6 6l12 12M18 6 6 18"/></svg>',
    empty:'<svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 10h18M9 4v16"/></svg>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     6. Toast / 剪贴板
     ═══════════════════════════════════════════════════════════════════ */
  function toast(msg, kind) {
    const el = document.createElement('div')
    el.className = 'toast ' + (kind || '')
    el.innerHTML = (kind === 'ok' ? I.check : kind === 'bad' ? I.warn : I.info) + '<span>' + esc(msg) + '</span>'
    $('toasts').appendChild(el)
    setTimeout(function () {
      el.className += ' out'
      setTimeout(function () { el.remove() }, 260)
    }, 2400)
  }
  function copy(text, label, btn) {
    const done = function () {
      toast('已复制' + (label ? '：' + label : ''), 'ok')
      if (btn) {
        const old = btn.innerHTML
        btn.innerHTML = I.check + '已复制'
        setTimeout(function () { btn.innerHTML = old }, 1400)
      }
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, function () { fallback() })
    } else fallback()
    function fallback() {
      const ta = document.createElement('textarea')
      ta.value = text
      ta.style.position = 'fixed'
      ta.style.opacity = '0'
      document.body.appendChild(ta)
      ta.select()
      let ok = false
      try { ok = document.execCommand('copy') } catch (e) { /* 忽略 */ }
      ta.remove()
      /* execCommand 失败返回 false 而不是抛异常——旧实现吞掉返回值,失败也弹
         「已复制」,而复制失败恰恰发生在复制 forwardKey 这些核心操作上 */
      if (ok) done()
      else toast('复制失败，请手动复制', 'bad')
    }
  }

  /* ═══════════════════════════════════════════════════════════════════
     7. 导航 / 顶栏
     ═══════════════════════════════════════════════════════════════════ */
  const NAV_MONITOR = [
    { id:'overview', label:'概览', icon:I.gauge },
    { id:'nodes', label:'出口节点', icon:I.nodes, badge:function () { return String(D.nodeAlive) } },
    { id:'models', label:'免费模型', icon:I.cube, badge:function () { return String(D.models.length) } },
    { id:'usage', label:'用量', icon:I.chart },
    { id:'logs', label:'日志', icon:I.scroll }
  ]
  const NAV_CONFIG = [{ id:'settings', label:'设置', icon:I.gear }]

  function renderNav() {
    function mk(list) {
      return list.map(function (n) {
        const b = n.badge ? n.badge() : ''
        return '<button class="nav-item' + (S.view === n.id ? ' active' : '') + '" data-nav="' + n.id + '">'
          + n.icon + '<span>' + n.label + '</span>'
          + (b ? '<span class="nav-badge">' + b + '</span>' : '') + '</button>'
      }).join('')
    }
    $('nav1').innerHTML = mk(NAV_MONITOR)
    $('nav2').innerHTML = mk(NAV_CONFIG)
  }

  /**
   * 配置校验状态的一行摘要（状态栏用）；一切正常时返回 ''。
   *
   * 这些信息以前只写进 gateway.log：用户在面板上只看到「网关空闲」，
   * 而真正的原因（配置里有节点加载不了 / 整份配置没启用）需要翻日志才知道。
   */
  function checkBadge(sb) {
    const lc = sb.lastCheck
    if (!lc || lc.ok == null) return ''
    if (lc.ok === false) return '配置未启用'
    if (lc.mode === 'direct') return '直连兜底'
    const parts = []
    if (lc.filtered > 0) parts.push('过滤丢弃 ' + lc.filtered + ' 个')
    if (lc.dropped > 0) parts.push('剔除 ' + lc.dropped + ' 个坏节点')
    if (parts.length) return parts.join('，')
    return ''
  }

  /** 配置校验状态的完整说明（概览页用）；一切正常时返回 ''。 */
  function checkAlert(sb) {
    const lc = sb.lastCheck
    if (!lc || lc.ok == null) return ''
    const when = lc.at ? new Date(lc.at).toLocaleTimeString() : ''
    const at = when ? '（' + when + ' 检测）' : ''
    if (lc.ok === false) return '配置没有启用：' + esc(lc.error || '未知原因') + '。当前实例继续沿用上一份配置' + at + '。'
    if (lc.mode === 'direct') return '所有节点都不可用，已进入纯直连兜底模式' + (lc.dropped ? '（本轮剔除 ' + lc.dropped + ' 个无法加载的节点）' : '') + at + '。'
    const notes = []
    if (lc.filtered > 0) notes.push('本轮 ' + lc.filtered + ' 个节点因地区未选中/类型不支持被过滤，未入池')
    if (lc.dropped > 0) notes.push('本轮 ' + lc.dropped + ' 个节点未通过 sing-box 配置校验、已被剔除')
    if (notes.length) return notes.join('；') + '；' + lc.nodes + ' 个节点正常启用' + at + '。'
    return ''
  }

  function renderStatusbar() {
    const sb = DATA.singbox || {}
    const fw = DATA.forward || {}
    const up = sb.running === true
    const badge = checkBadge(sb)
    /* R7+O9 的另一半：diagnostics.stats.lastError 由后端备好，这里消费 ——
       写盘持续失败时面板不能再「看起来一切正常」 */
    const dg = DATA.diagnostics || {}
    const diskErr = (dg.stats && dg.stats.lastError) || ''
    $('statusbar').innerHTML =
      '<span class="sb-item"><span class="dot ' + (up ? 'ok live' : 'bad') + '"></span>'
        + (up ? 'sing-box 运行中' : 'sing-box 未运行') + '</span>'
      + (diskErr ? '<span class="sb-item" style="color:var(--warn)" title="用量统计写盘失败：' + esc(diskErr) + '">写盘失败</span>' : '')
      + (badge ? '<span class="sb-item" style="color:var(--warn)">' + esc(badge) + '</span>' : '')
      + '<span class="sb-item">出口 ' + D.nodeAlive + '/' + D.nodeTotal + '</span>'
      + '<span class="sb-item">今日 ' + D.usage.today.req + ' 次</span>'
      + '<span class="sb-item">:' + (fw.port == null ? 3457 : fw.port) + '</span>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     8. 视图：概览
     ═══════════════════════════════════════════════════════════════════ */
  function viewOverview() {
    const sb = DATA.singbox || {}
    const fw = DATA.forward || {}
    const up = sb.running === true
    const gated = D.models.filter(function (m) { return m.gated })
    const today = D.usage.today

    return ''
    + '<div class="view">'
    + (checkAlert(sb)
        ? '  <div class="card sec" style="border-color:var(--warn)"><div class="card-body" style="color:var(--warn);font-size:13px">'
          + '<b>sing-box 配置：</b>' + checkAlert(sb) + '</div></div>'
        : '')
    + '  <div class="grid-kpi sec">'
    + '    <div class="kpi">'
    + '      <div class="kpi-label"><span class="dot ' + (up ? 'ok live' : 'bad') + '"></span>网关状态</div>'
    + '      <div class="kpi-value">' + (up ? '运行中' : '未运行') + '</div>'
    + '      <div class="kpi-sub"><span class="mono">sing-box pid ' + (sb.pid == null ? '—' : sb.pid) + '</span></div>'
    + '    </div>'
    + '    <div class="kpi">'
    + '      <div class="kpi-label">可用出口</div>'
    + '      <div class="kpi-value">' + D.nodeAlive + '<span class="unit">/ ' + D.nodeTotal + ' 节点</span></div>'
    + '      <div class="kpi-sub"><span class="badge badge-tier badge-ok">B ' + D.tierB + '</span>'
    + '        <span class="badge badge-tier badge-neutral">A ' + D.tierA + '</span>'
    + '        <span class="dim">延迟中位 ' + fmtLat(D.latMed) + '</span></div>'
    + '    </div>'
    + '    <div class="kpi">'
    + '      <div class="kpi-label">今日请求</div>'
    + '      <div class="kpi-value">' + today.req + '<span class="unit">次</span></div>'
    + '      <div class="kpi-sub"><span class="dim">累计 ' + D.usage.requests + ' 次</span></div>'
    + '    </div>'
    + '    <div class="kpi">'
    + '      <div class="kpi-label">今日 Token</div>'
    + '      <div class="kpi-value">' + fmtTok(today.in + today.out) + '</div>'
    + '      <div class="kpi-sub"><span class="mono">输入 ' + fmtTok(today.in) + '</span><span class="dim">·</span>'
    + '        <span class="mono">输出 ' + fmtTok(today.out) + '</span></div>'
    + '    </div>'
    + '  </div>'

    + ((fw.busy && fw.busy.requests > 0) || laneRows(D.lanes)
        ? '  <div class="card sec"><div class="card-head"><h2>实时负载</h2>'
          + '<span class="sub">正在做工作的请求与出口车道</span></div>'
          + '<div class="card-body">'
          + (fw.busy && fw.busy.requests > 0
              ? '<div class="cred-row"><span class="k">在途请求</span>'
                + '<b>' + fw.busy.requests + '</b><span class="dim">个正在跑（agent 工具链间隙也会保持计数，直到回合完成）</span></div>'
              : '')
          + (laneRows(D.lanes)
              ? '<table class="tbl" style="margin-top:6px"><thead><tr><th>出口 IP</th><th>在途</th><th>排队</th><th>限额</th></tr></thead><tbody>'
                + laneRows(D.lanes) + '</tbody></table>'
              : '')
          + '</div></div>'
        : '')

    + '  <div class="sec"><div class="cred">'
    + '    <div class="cred-head">' + I.bolt + '<h2>接入信息</h2>'
    + '      <span class="sub">粘进 opencode 或任何 OpenAI 兼容客户端</span></div>'
    + '    <div class="cred-row"><span class="k">API 地址</span>'
    + '      <code id="c-base">http://127.0.0.1:' + (fw.port == null ? 3457 : fw.port) + '/v1</code>'
    + '      <button class="btn btn-sm" data-copy="c-base" data-label="API 地址">' + I.copy + '复制</button></div>'
    + '    <div class="cred-row"><span class="k">API Key</span>'
    + '      <code id="c-key">' + esc(fw.key || '') + '</code>'
    + '      <button class="btn btn-sm" data-copy="c-key" data-label="API Key">' + I.copy + '复制</button></div>'
    + '    <div class="cred-row"><span class="k">一键复制</span>'
    + '      <span class="dim" style="font-size:12.5px">按客户端格式生成，省去手动拼装</span>'
    + '      <span class="spacer" style="flex:1"></span>'
    + '      <button class="btn btn-sm" data-copy-tpl="env">.env</button>'
    + '      <button class="btn btn-sm" data-copy-tpl="curl">curl 示例</button>'
    + '      <button class="btn btn-sm" data-copy-tpl="json">JSON 配置</button></div>'
    + '    <div class="steps">'
    + '      <span class="step"><span class="step-n">1</span>复制地址与 Key</span>'
    + '      <span class="step"><span class="step-n">2</span>选好出口地区</span>'
    + '      <span class="step"><span class="step-n">3</span>保存并等首轮探测</span></div>'
    + '  </div></div>'

    + (gated.length
      ? '  <div class="sec"><div class="note note-warn">' + I.warn
        + '<div><b>有 ' + gated.length + ' 个受限模型需要 B 级出口</b>：'
        + gated.map(function (m) { return '<code>' + esc(m.id) + '</code>' }).join(' ')
        + '<br>这类模型只在被上游标记为可用（B 级）的节点上才能出站。当前 B 级出口 <b>' + D.tierB + '</b> 个'
        + (D.tierB > 0 ? '，可直接使用' : '，暂不可用——请刷新订阅或稍后重试') + '。</div></div></div>'
      : '')

    + '  <div class="grid-2 sec">'
    + '    <div class="card"><div class="card-head"><h2>最近一次探测</h2>'
    + '      <span class="spacer" style="flex:1"></span>'
    + '      <button class="btn btn-sm" id="btnProbe"' + (S.probing ? ' disabled' : '') + '>'
    + (S.probing ? I.refresh + '探测中…' : I.bolt + '立即探测') + '</button></div>'
    + '      <div class="card-body">'
    + (D.probe
      ? '        <div class="kv-list">'
        + '<div class="r"><span class="k">本轮结果</span><span class="v">' + D.probe.alive + ' / ' + D.probe.scanned + ' 存活</span></div>'
        + '<div class="r"><span class="k">层级</span><span class="v">A ' + D.probe.a + ' · B ' + D.probe.b + '</span></div>'
        + '<div class="r"><span class="k">探测耗时</span><span class="v">' + D.probe.secs + 's</span></div>'
        + '<div class="r"><span class="k">完成于</span><span class="v">' + fmtTime(D.probe.at) + '</span></div>'
        + '<div class="r"><span class="k">下次自动</span><span class="v">' + esc(S.form.hotIntervalSec) + ' 秒后（热区复检）</span></div>'
        + '</div>'
      : '        <div class="empty">还没有探测记录</div>')
    /* 真实接口是阻塞的，拿不到进度百分比——用不定态进度条，不编造数字 */
    + (S.probing ? '<div class="progress indeterminate"><i></i></div>'
        + '<div class="dim mono" style="font-size:12px;margin-top:6px">正在探测全部节点，完成后自动刷新…</div>' : '')
    + '      </div></div>'

    + '    <div class="card"><div class="card-head"><h2>出口延迟分布</h2>'
    + '      <span class="sub">' + D.alive.length + ' 个存活节点</span></div>'
    + '      <div class="card-body">'
    + [
      { label:'快 < 1.5s', n:D.fast, color:'var(--ok)' },
      { label:'中 1.5–5s', n:D.mid, color:'var(--fg-3)' },
      { label:'慢 > 5s', n:D.slow, color:'var(--warn)' }
    ].map(function (r) {
      return '<div class="bar-row" style="margin-bottom:9px">'
        + '<span class="name" style="font-family:var(--ui);font-size:12.5px">' + r.label + '</span>'
        + '<span class="bar-track"><span class="bar-in" style="width:'
        + (r.n / Math.max(1, D.alive.length) * 100).toFixed(1) + '%;background:' + r.color + '"></span></span>'
        + '<span class="val">' + r.n + ' 个</span></div>'
    }).join('')
    + '        <div class="dim" style="font-size:12px;margin-top:10px">中位 ' + fmtLat(D.latMed)
    + ' · 最快 ' + fmtLat(D.latMin) + ' · 最慢 ' + fmtLat(D.latMax) + '</div>'
    + '      </div></div>'
    + '  </div>'

    + '  <div class="card sec"><div class="card-head"><h2>快捷操作</h2></div>'
    + '    <div class="card-body inline">'
    + '      <button class="btn" id="btnProbe2"' + (S.probing ? ' disabled' : '') + '>' + I.bolt + '立即探测</button>'
    + '      <button class="btn" id="btnRefresh">' + I.refresh + '刷新订阅并重建</button>'
    + '      <button class="btn" id="btnLimits">' + I.refresh + '刷新限额表</button>'
    + '      <span class="dim" style="font-size:12px">限额表 ' + ((DATA.limits && DATA.limits.rows) || 0) + ' 行 · '
    + (DATA.limits && DATA.limits.stale ? '使用缓存' : '已是最新') + ' · '
    + fmtDT(DATA.limits && DATA.limits.fetchedAt) + '</span>'
    + '    </div></div>'
    + '</div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     9. 视图：出口节点
     ═══════════════════════════════════════════════════════════════════ */
  function filteredNodes() {
    let list = D.nodes.slice()
    if (S.nodeState === 'alive') list = list.filter(function (n) { return n.state === 'alive' })
    else if (S.nodeState === 'dead') list = list.filter(function (n) { return n.state !== 'alive' })
    else if (S.nodeState === 'B') list = list.filter(function (n) { return n.tier === 'B' })
    else if (S.nodeState === 'A') list = list.filter(function (n) { return n.tier === 'A' })
    if (S.nodeBucket !== 'all') list = list.filter(function (n) { return bucketOf(n.country) === S.nodeBucket })
    const q = S.nodeQ.trim().toLowerCase()
    if (q) {
      list = list.filter(function (n) {
        return String(n.tag).toLowerCase().indexOf(q) >= 0
          || String(n.exitIp || '').toLowerCase().indexOf(q) >= 0
          || (CC_NAME[n.country] || '').indexOf(q) >= 0
      })
    }
    const dir = S.nodeSortDir, key = S.nodeSort
    list.sort(function (a, b) {
      if (key === 'latency') {
        /* I23：三分支比较。旧实现两个未知值(Infinity)相减得 NaN，comparator
           返回 NaN 属实现定义行为；且 dir 反转时未知节点会排到最前面。
           语义：未知永远沉底，已知之间按方向排。 */
        const aKnown = a.latencyMs >= 0, bKnown = b.latencyMs >= 0
        if (aKnown !== bKnown) return aKnown ? -1 : 1
        if (!aKnown) return 0
        return (a.latencyMs - b.latencyMs) * dir
      }
      if (key === 'country') return String(a.country).localeCompare(String(b.country)) * dir
      return String(a.tag).localeCompare(String(b.tag), 'zh') * dir
    })
    return list
  }

  function viewNodes() {
    const buckets = BUCKETS.map(function (b) {
      const c = D.bucketCount[b.id] || { total:0, alive:0, b:0 }
      return { id:b.id, name:b.name, total:c.total, alive:c.alive, b:c.b }
    })
    const list = filteredNodes()

    function th(id, label) {
      return '<th class="sortable' + (S.nodeSort === id ? ' sorted' : '') + '" data-sort="' + id + '">'
        + label + ' <span class="arrow">' + (S.nodeSort === id ? (S.nodeSortDir > 0 ? '▲' : '▼') : '▲▼') + '</span></th>'
    }

    return ''
    + '<div class="view">'
    + '  <div class="card">'
    + '    <div class="toolbar">'
    + '      <span class="search">' + I.search
    + '        <input type="search" id="nodeQ" placeholder="搜索节点名 / 出口 IP" value="' + esc(S.nodeQ) + '"></span>'
    + '      <span class="seg" id="stateSeg">'
    + '        <button data-st="all" class="' + (S.nodeState === 'all' ? 'on acc' : '') + '">全部 ' + D.nodes.length + '</button>'
    + '        <button data-st="alive" class="' + (S.nodeState === 'alive' ? 'on acc' : '') + '">存活 ' + D.alive.length + '</button>'
    + '        <button data-st="B" class="' + (S.nodeState === 'B' ? 'on acc' : '') + '">B 级 ' + D.tierB + '</button>'
    + '        <button data-st="A" class="' + (S.nodeState === 'A' ? 'on acc' : '') + '">A 级 ' + D.tierA + '</button>'
    + '      </span>'
    + '      <span class="spacer" style="flex:1"></span>'
    + '      <span class="dim" style="font-size:12px">共 ' + list.length + ' 行</span>'
    + '    </div>'

    + '    <div class="region-strip">'
    + '      <span class="rcard ' + (S.nodeBucket === 'all' ? 'on' : '') + '" data-bucket="all">'
    + '        <span>全部地区</span><span class="cnt">' + D.nodes.length + '</span></span>'
    + buckets.map(function (b) {
      return '<span class="rcard ' + (S.nodeBucket === b.id ? 'on' : '') + '" data-bucket="' + b.id + '"'
        + ' title="' + (b.total ? b.alive + ' 个存活 / 共 ' + b.total + ' 个' : '该地区当前无节点') + '">'
        + '<span class="cc' + (b.alive ? '' : ' zero') + '">' + b.id + '</span>'
        + '<span class="' + (b.alive ? '' : 'zero') + '">' + b.name + '</span>'
        + '<span class="cnt ' + (b.alive ? '' : 'zero') + '">' + b.alive + '</span>'
        + (b.b ? '<span class="badge badge-tier badge-ok" style="height:17px;font-size:10.5px">B' + b.b + '</span>' : '')
        + '</span>'
    }).join('')
    + '    </div>'

    + '    <div class="tbl-wrap"><table><thead><tr>'
    + th('tag', '节点') + th('country', '地区')
    + '<th>层级</th><th>健康</th>' + th('latency', '延迟')
    + '<th>出口 IP</th><th>最后探测</th>'
    + '</tr></thead><tbody>'
    + (list.length
      ? list.map(function (n) {
        const lat = n.latencyMs
        return '<tr>'
          + '<td class="node-name" title="' + esc(n.tag) + '">' + esc(stripRI(n.tag)) + '</td>'
          + '<td>' + ccChip(n.country) + ' ' + esc(CC_NAME[n.country] || n.country || '—') + '</td>'
          + '<td>' + (n.tier === 'B' ? '<span class="badge badge-tier badge-ok">B</span>'
              : n.tier === 'A' ? '<span class="badge badge-tier badge-neutral">A</span>' : '<span class="dim">—</span>') + '</td>'
          + '<td>' + (n.state === 'alive' ? '<span class="badge badge-ok">存活</span>'
              : '<span class="badge badge-bad">不可用</span>') + '</td>'
          + '<td><span class="lat-cell" title="' + (lat >= 0 ? fmtLat(lat) + '（刻度 √ 压缩，封顶 10s）' : '未测得') + '">'
          + '<span class="lat-track"><span class="lat-fill ' + latClass(lat) + '" style="width:' + latW(lat).toFixed(1) + '%"></span></span>'
          + '<span class="num">' + fmtLat(lat) + '</span></span></td>'
          + '<td class="num">' + (n.exitIp ? esc(n.exitIp) : '<span class="dim">未探测</span>') + '</td>'
          + '<td class="num dim">' + fmtTime(n.lastProbeAt) + '</td>'
          + '</tr>'
      }).join('')
      : '<tr><td colspan="7"><div class="empty">' + I.empty + '<div>没有匹配的节点</div>'
        + '<div style="font-size:12px;margin-top:6px">试试清空搜索词，或切换地区筛选</div></div></td></tr>')
    + '</tbody></table></div>'
    + '  </div>'
    + '  <div class="note note-info" style="margin-top:14px">' + I.info
    + '    <div><b>层级说明</b>：<b>B 级</b> = 已验证可通过上游区域校验，可服务全部模型（含受限模型）；'
    + '<b>A 级</b> = 仅粗粒度可达，受限模型会绕开它。热区每 ' + esc(S.form.hotIntervalSec) + ' 秒复检一轮，冷区每 ' + esc(Math.round(Number(S.form.coldIntervalSec) / 60)) + ' 分钟扫一轮复活。'
    + '<br><b>延迟条</b>用平方根压缩刻度（封顶 10s）——节点延迟跨度达 60 倍，线性刻度会让快节点全部贴地看不出差别。'
    + '绿色 &lt;1.5s、琥珀色 &gt;5s。</div></div>'
    + '</div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     10. 视图：免费模型
     ═══════════════════════════════════════════════════════════════════ */
  function viewModels() {
    const lim = DATA.limits || {}
    const anyGated = D.models.some(function (m) { return m.gated })
    return ''
    + '<div class="view">'
    + '  <div class="card sec"><div class="card-head"><h2>限额表来源</h2>'
    + '    <span class="sub">models.dev · opencode 行</span>'
    + '    <span class="spacer" style="flex:1"></span>'
    + '    <button class="btn btn-sm" id="btnLimits2">' + I.refresh + '刷新限额表</button></div>'
    + '    <div class="card-body inline" style="font-size:12.5px">'
    + '      <span class="badge ' + (lim.stale ? 'badge-warn' : 'badge-ok') + '">'
    + (lim.stale ? '使用缓存' : '最新') + '</span>'
    + '      <span class="dim">' + (lim.rows || 0) + ' 行 · 拉取于 ' + fmtDT(lim.fetchedAt) + ' · 24 小时自动刷新</span>'
    + '    </div></div>'

    + (anyGated
      ? '<div class="note note-warn sec">' + I.warn
        + '<div><b>受限模型</b>（带 ★ 标记）只能经 B 级节点出站。当前 B 级出口 ' + D.tierB + ' 个。</div></div>'
      : '')

    + '  <div class="model-grid">'
    + D.models.map(function (m) {
      return '<div class="mcard' + (m.gated ? ' gated' : '') + '">'
        + '<div class="mcard-top"><span class="mcard-id">' + esc(m.id) + '</span>'
        + (m.gated ? '<span class="badge badge-warn" title="需要 B 级出口">' + I.star + '受限</span>' : '')
        + '</div>'
        + '<div class="mcard-caps">'
        + '<span>上下文 <b>' + fmtTok(m.contextWindow) + '</b></span>'
        + '<span>输出上限 <b>' + fmtTok(m.maxOutput) + '</b></span>'
        + '</div>'
        + '<div class="mcard-foot"><button class="btn btn-sm" data-copy-text="' + esc(m.id) + '" data-label="模型 id">'
        + I.copy + '复制 id</button></div>'
        + '</div>'
    }).join('')
    + '  </div>'

    + '  <div class="card sec" style="margin-top:16px"><div class="card-body inline">'
    + '    <button class="btn" id="btnCopyAll">' + I.copy + '复制全部模型 id（换行分隔）</button>'
    + '    <span class="dim" style="font-size:12px">共 ' + D.models.length + ' 个免费模型，付费模型不进目录</span>'
    + '  </div></div>'
    + '</div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     11. 视图：用量
     ═══════════════════════════════════════════════════════════════════ */
  function viewUsage() {
    const t = D.usage.today
    return ''
    + '<div class="view">'
    + '  <div class="grid-kpi sec">'
    + '    <div class="kpi"><div class="kpi-label">今日请求</div><div class="kpi-value">' + t.req + '<span class="unit">次</span></div></div>'
    + '    <div class="kpi"><div class="kpi-label">输入 Token</div><div class="kpi-value">' + fmtTok(t.in) + '</div></div>'
    + '    <div class="kpi"><div class="kpi-label">输出 Token</div><div class="kpi-value">' + fmtTok(t.out) + '</div></div>'
    + '    <div class="kpi"><div class="kpi-label">累计请求</div><div class="kpi-value">' + D.usage.requests + '<span class="unit">次</span></div></div>'
    + '  </div>'

    + '  <div class="card sec"><div class="card-head"><h2>近 7 日趋势</h2>'
    + '    <span class="sub">按请求数</span></div>'
    + '    <div class="card-body">'
    + (D.history.length
      ? '<div class="chart">' + D.history.map(function (h) {
        const isToday = h.date === D.todayKey
        const px = h.req === 0 ? 3 : Math.max(3, Math.round(h.req / D.historyMax * 120))
        const cls = 'chart-col' + (isToday ? ' today' : h.req === 0 ? ' dim' : '')
        return '<div class="' + cls + '" title="' + esc(h.date) + '：' + h.req + ' 次 · 输入 ' + fmtTok(h.in) + ' · 输出 ' + fmtTok(h.out) + '">'
          + '<span class="chart-v">' + (h.req || '–') + '</span>'
          + '<span class="chart-barwrap"><span class="chart-bar" style="height:' + px + 'px"></span></span>'
          + '<span class="chart-x">' + esc(h.date.slice(5)) + '</span>'
          + '</div>'
      }).join('') + '</div>'
      : '<div class="empty">' + I.empty + '<div>还没有跨日数据</div></div>')
    + '    </div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>今日模型分布</h2><span class="sub">按请求数排序</span></div>'
    + '    <div class="card-body">'
    + '      <div class="legend"><span><i style="background:var(--accent)"></i>输入 Token</span>'
    + '        <span><i style="background:var(--fg-3)"></i>输出 Token</span></div>'
    + (D.rows.length
      ? '<div class="bars">' + D.rows.map(function (r) {
        const total = r.in + r.out
        const pct = total / D.maxIn * 100
        const inShare = total ? r.in / total * 100 : 0
        return '<div class="bar-row">'
          + '<span class="name" title="' + esc(r.model) + '">' + esc(r.model) + '</span>'
          + '<span class="bar-track">'
          + '<span class="bar-in" style="width:' + (pct * inShare / 100).toFixed(2) + '%"></span>'
          + '<span class="bar-out" style="width:' + (pct * (100 - inShare) / 100).toFixed(2) + '%"></span>'
          + '</span>'
          + '<span class="val">' + r.req + ' 次 · ' + fmtTok(total) + '</span></div>'
      }).join('') + '</div>'
      : '<div class="empty">' + I.empty + '<div>今天还没有请求</div></div>')
    + '    </div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>明细</h2></div>'
    + '    <div class="tbl-wrap" style="max-height:none"><table>'
    + '      <thead><tr><th>模型</th><th>请求</th><th>输入 Token</th><th>输出 Token</th><th>合计</th></tr></thead>'
    + '      <tbody>'
    + (D.rows.length
      ? D.rows.map(function (r) {
        return '<tr><td class="node-name" style="max-width:340px">' + esc(r.model) + '</td>'
          + '<td class="num">' + r.req + '</td>'
          + '<td class="num">' + r.in.toLocaleString() + '</td>'
          + '<td class="num">' + r.out.toLocaleString() + '</td>'
          + '<td class="num">' + (r.in + r.out).toLocaleString() + '</td></tr>'
      }).join('')
      : '<tr><td colspan="5"><div class="empty">暂无记录</div></td></tr>')
    + '      </tbody></table></div></div>'

    + exitRows()

    + '  <div class="note note-info">' + I.info
    + '    <div>用量全部留在本机（<code>data/stats.json</code>）。趋势按 UTC 日切分；'
    + '请求级采样保留最近 24 小时、最多 2000 条，用于后续的分位统计。</div></div>'
    + '</div>'
  }

  /* 按出口分列的用量(README 承诺的「模型 × 出口」维度;后端 stats.Exits 聚合) */
  function shortTag(tag) {
    return stripRI(tag).slice(0, 28)
  }
  function exitRows() {
    const byExit = (D.usage && D.usage.byExit) || {}
    const rows = Object.keys(byExit)
      .map(function (e) { return Object.assign({ exit: e }, byExit[e]) })
      .sort(function (a, b) { return b.req - a.req })
      .slice(0, 20)
    return ''
    + '  <div class="card sec"><div class="card-head"><h2>按出口</h2>'
    + '    <span class="sub">请求实际走的出口 · 前 20 行</span></div>'
    + '    <div class="tbl-wrap" style="max-height:none"><table>'
    + '      <thead><tr><th>出口</th><th>请求</th><th>输入 Token</th><th>输出 Token</th><th>合计</th></tr></thead>'
    + '      <tbody>'
    + (rows.length
      ? rows.map(function (r) {
        return '<tr><td class="node-name" style="max-width:340px" title="' + esc(r.exit) + '">' + esc(shortTag(r.exit)) + '</td>'
          + '<td class="num">' + r.req + '</td>'
          + '<td class="num">' + (r.in || 0).toLocaleString() + '</td>'
          + '<td class="num">' + (r.out || 0).toLocaleString() + '</td>'
          + '<td class="num">' + ((r.in || 0) + (r.out || 0)).toLocaleString() + '</td></tr>'
      }).join('')
      : '<tr><td colspan="5"><div class="empty">暂无出口数据</div></td></tr>')
    + '      </tbody></table></div></div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     12. 视图：日志
     ═══════════════════════════════════════════════════════════════════ */
  function buildLogRows() {
    let lines = (DATA.logs || []).slice()
    if (S.logLevel !== 'all') lines = lines.filter(function (l) { return l.level === S.logLevel })
    const q = S.logQ.trim().toLowerCase()
    if (q) lines = lines.filter(function (l) { return String(l.msg).toLowerCase().indexOf(q) >= 0 })
    const out = []
    let noiseCount = 0
    for (let i = 0; i < lines.length; i++) {
      const l = lines[i]
      if (S.logFoldNoise && isNoise(l.msg)) { noiseCount++; continue }
      const last = out[out.length - 1]
      if (S.logDedupe && last && last.msg === l.msg && last.level === l.level) {
        last.n++
        last.t = l.t
        continue
      }
      out.push({ t: l.t, level: l.level, msg: l.msg, n: 1 })
    }
    return { rows: out, noiseCount: noiseCount }
  }

  function viewLogs() {
    const r = buildLogRows()
    const total = (DATA.logs || []).length
    return ''
    + '<div class="view">'
    + '  <div class="card">'
    + '    <div class="toolbar">'
    + '      <span class="search">' + I.search
    + '        <input type="search" id="logQ" placeholder="搜索日志内容" value="' + esc(S.logQ) + '"></span>'
    + '      <span class="seg" id="levelSeg">'
    + ['all','info','warn','error'].map(function (lv) {
      return '<button data-lv="' + lv + '" class="' + (S.logLevel === lv ? 'on acc' : '') + '">' + lv + '</button>'
    }).join('')
    + '      </span>'
    + '      <span class="spacer" style="flex:1"></span>'
    + '      <label class="inline" style="font-size:12.5px;gap:6px"><input type="checkbox" id="cbDedupe"' + (S.logDedupe ? ' checked' : '') + '> 合并重复</label>'
    + '      <label class="inline" style="font-size:12.5px;gap:6px"><input type="checkbox" id="cbNoise"' + (S.logFoldNoise ? ' checked' : '') + '> 折叠噪音</label>'
    + '      <label class="inline" style="font-size:12.5px;gap:6px"><input type="checkbox" id="cbAuto"' + (S.logAuto ? ' checked' : '') + '> 自动滚到底</label>'
    + '      <button class="btn btn-sm" id="btnLogCopy">' + I.copy + '复制</button>'
    + '    </div>'
    + (S.logFoldNoise && r.noiseCount
      ? '<div class="noise-sum">' + I.info + '<span>已折叠 <b>' + r.noiseCount + '</b> 条已知噪音'
        + '（探测期拨号失败、连接被远端重置）—— 这些不代表故障</span></div>'
      : '')
    + '    <div class="logbox" id="logbox">'
    + (r.rows.length
      ? r.rows.map(function (l) {
        return '<div class="logline">'
          + '<span class="t">' + fmtTime(l.t) + '</span>'
          + '<span class="lv lv-' + esc(l.level) + '">' + esc(String(l.level).toUpperCase()) + '</span>'
          + '<span class="m">' + esc(l.msg) + (l.n > 1 ? '<span class="dupcnt">×' + l.n + '</span>' : '') + '</span></div>'
      }).join('')
      : '<div class="empty">' + I.empty + '<div>没有匹配的日志</div></div>')
    + '    </div>'
    + '  </div>'
    + routesCard()
    + '  <div class="note note-info" style="margin-top:14px">' + I.info
    + '    <div>显示 ' + r.rows.length + ' 行（原始 ' + total + ' 行）。完整日志在 <code>data/gateway.log</code>，'
    + '超 5MB 自动轮转为 <code>gateway.old.log</code>。请求轨迹在 <code>data/route/</code>（保留 30 天）。</div></div>'
    + '</div>'
  }

  /* 请求轨迹：每次路由决策的候选顺序与逐次尝试结果(/api/routes，README
     承诺的「每次请求走了哪个出口，面板可查」) */
  function routesCard() {
    const rows = S.routes || []
    return ''
    + '  <div class="card sec" style="margin-top:14px"><div class="card-head"><h2>请求轨迹</h2>'
    + '    <span class="sub">最近 ' + rows.length + ' 次路由决策</span></div>'
    + '    <div class="tbl-wrap" style="max-height:320px"><table>'
    + '      <thead><tr><th>时间</th><th>模型</th><th>结果</th><th>尝试链（出口 · 码 · 耗时）</th><th>总耗时</th></tr></thead>'
    + '      <tbody>'
    + (rows.length
      ? rows.map(function (r) {
        const tries = (r.tries || []).map(function (t) {
          return esc(shortTag(t.tag)) + ' <span class="dim">(' + esc(t.code || 'ok') + ' ' + t.ms + 'ms)</span>'
        }).join(' → ')
        return '<tr>'
          + '<td class="num dim">' + fmtTime(r.at) + '</td>'
          + '<td class="node-name" style="max-width:220px">' + esc(r.model) + '</td>'
          + '<td>' + esc(r.result || '') + '</td>'
          + '<td style="font-size:12px">' + (tries || '<span class="dim">—</span>') + '</td>'
          + '<td class="num">' + r.ms + 'ms</td>'
          + '</tr>'
      }).join('')
      : '<tr><td colspan="5"><div class="empty">还没有路由记录</div></td></tr>')
    + '      </tbody></table></div></div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     13. 视图：设置
     ═══════════════════════════════════════════════════════════════════ */
  function viewSettings() {
    const bc = D.bucketCount
    const chosen = {}
    S.order.forEach(function (id) { chosen[id] = true })
    function cnt(id) { return bc[id] ? bc[id].alive : 0 }
    const chain = S.order.length
      ? S.order.map(function (id) {
        const b = BUCKETS.filter(function (x) { return x.id === id })[0]
        return b ? b.name : id
      }).join(' → ')
      : '（尚未选择任何地区）'
    const dead = S.order.filter(function (id) { return cnt(id) === 0 })
    const subCount = S.form.subUrls.split('\n').filter(function (x) { return x.trim() }).length

    return ''
    + '<div class="view">'
    + '  <div class="card sec"><div class="card-head"><h2>接入与订阅</h2></div>'
    + '    <div class="card-body"><div class="field">'
    + '      <label class="lbl" for="f-subUrls">订阅链接</label>'
    + '      <textarea id="f-subUrls" rows="5" spellcheck="false">' + esc(S.form.subUrls) + '</textarea>'
    + '      <div class="help">每行一条，按顺序多源回退。留空则仅以直连兜底模式运行。当前 ' + subCount + ' 条。</div>'
    + '    </div></div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>出口地区与回退顺序</h2>'
    + '    <span class="sub">顺序即回退链，靠前的优先</span></div>'
    + '    <div class="card-body">'
    + '      <div class="order-grid">'
    + '        <div class="order-panel"><div class="ph">可选地区'
    + '          <span class="spacer" style="flex:1"></span><span class="dim" style="font-weight:400">点击加入 →</span></div>'
    + '          <div class="order-list">'
    + BUCKETS.map(function (b) {
      return '<div class="pick' + (chosen[b.id] ? ' off' : '') + '" data-add="' + b.id + '">'
        + '<span class="cc">' + b.id + '</span><span class="nm">' + b.name + '</span>'
        + '<span class="cnt">' + cnt(b.id) + ' 个可用</span>'
        + (chosen[b.id] ? '<span class="dim" style="font-size:11.5px">已选</span>' : '') + '</div>'
    }).join('')
    + '          </div></div>'
    + '        <div class="order-panel"><div class="ph">回退顺序'
    + '          <span class="spacer" style="flex:1"></span><span class="dim" style="font-weight:400">拖动或 ↑↓ 调整</span></div>'
    + '          <div class="order-list" id="orderList">'
    + (S.order.length
      ? S.order.map(function (id, i) {
        const b = BUCKETS.filter(function (x) { return x.id === id })[0] || { name: id }
        const c = cnt(id)
        /* esc(id)：countries 数组元素会回流到 data-id 与展示文本里 —— 后端已
           白名单 8 个分组 id，这里是防御纵深（写盘的手工编辑不受后端管） */
        return '<div class="order-item" draggable="true" data-idx="' + i + '" data-id="' + esc(id) + '">'
          + '<span class="idx">' + (i + 1) + '</span><span class="grip">' + I.grip + '</span>'
          + '<span class="cc">' + esc(id) + '</span><span class="nm">' + esc(b.name) + '</span>'
          + '<span class="cnt">' + (c === 0 ? '<span style="color:var(--warn)">0 个</span>' : c + ' 个') + '</span>'
          + '<button class="btn btn-sm btn-ghost" data-move="' + i + '" data-dir="-1" title="上移"' + (i === 0 ? ' disabled' : '') + '>' + I.up + '</button>'
          + '<button class="btn btn-sm btn-ghost" data-move="' + i + '" data-dir="1" title="下移"' + (i === S.order.length - 1 ? ' disabled' : '') + '>' + I.down + '</button>'
          + '<button class="btn btn-sm btn-ghost" data-del="' + i + '" title="移出">' + I.x + '</button>'
          + '</div>'
      }).join('')
      : '<div class="order-empty">从左侧点选地区，按你希望的优先级依次加入</div>')
    + '          </div></div>'
    + '      </div>'
    + '      <div class="chain">回退链：' + esc(chain) + '</div>'
    + (dead.length
      ? '<div class="note note-warn" style="margin-top:10px">' + I.warn
        + '<div>回退链里有 <b>' + dead.length + '</b> 个地区当前没有可用节点（'
        + dead.map(function (id) {
          const b = BUCKETS.filter(function (x) { return x.id === id })[0]
          /* esc(id)：与上面回退链同一防御纵深 —— id 能从手改的 settings.json
             回流,BUCKETS 命中时取静态名,兜底展示的原始 id 必须过 esc */
          return b ? b.name : esc(id)
        }).join('、')
        + '）——命中它们会直接跳到下一个地区。可以保留，节点是动态的，下一轮探测可能就有。</div></div>'
      : '')
    + '      <div class="help" style="margin-top:8px">"其他"包含无法识别国名的节点（上限 80 个），'
    + '探测后按出口 IP 实测归桶。</div>'
    + '    </div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>探测</h2></div>'
    + '    <div class="card-body">'
    + '      <div class="inline" style="gap:20px">'
    + '        <label class="inline"><input type="checkbox" id="f-probeEnabled"' + (S.form.probeEnabled ? ' checked' : '') + '> 自动探测</label>'
    + '        <label class="inline">探测并发 <input type="number" id="f-probeWorkers" value="' + esc(S.form.probeWorkers) + '" style="width:76px" min="1" max="128"></label>'
    + '      </div>'
    + '      <div class="inline" style="gap:20px;margin-top:8px">'
    + '        <label class="inline">热区复检 <input type="number" id="f-hotIntervalSec" value="' + esc(S.form.hotIntervalSec) + '" style="width:76px" min="15" max="3600"> 秒</label>'
    + '        <label class="inline">冷区扫描 <input type="number" id="f-coldIntervalSec" value="' + esc(S.form.coldIntervalSec) + '" style="width:86px" min="60" max="86400"> 秒</label>'
    + '        <label class="inline">订阅刷新 <input type="number" id="f-refreshIntervalMin" value="' + esc(S.form.refreshIntervalMin) + '" style="width:76px" min="5" max="10080"> 分钟</label>'
    + '      </div>'
    + '      <div class="help">并发越高越快，但更容易触发上游限流。当前 ' + D.nodes.length + ' 个节点，'
    + '一轮约需 ' + Math.max(10, Math.round(D.nodes.length / Math.max(1, Number(S.form.probeWorkers) || 24) * 15)) + ' 秒。'
    + '间隔从上一轮**完整结束**后起算，一轮跑多久都不会堆积。</div>'
    + '    </div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>模型与输出</h2></div>'
    + '    <div class="card-body">'
    + '      <div class="field"><label class="lbl" for="f-effortLevel">默认思考强度</label>'
    + '        <select id="f-effortLevel">'
    + '          <option value="light"' + (S.form.effortLevel === 'light' ? ' selected' : '') + '>Light 精简（约 2K）</option>'
    + '          <option value="balanced"' + (S.form.effortLevel === 'balanced' ? ' selected' : '') + '>Balanced 均衡（约 24K）</option>'
    + '          <option value="deep"' + (S.form.effortLevel === 'deep' ? ' selected' : '') + '>Deep 深思（模型上限）</option>'
    + '        </select>'
    + '        <div class="help">客户端未显式指定时的默认值。</div></div>'
    + '      <div class="field"><label class="lbl" for="f-defaultMaxTokens">默认输出上限</label>'
    + '        <input type="number" id="f-defaultMaxTokens" value="' + esc(S.form.defaultMaxTokens) + '" placeholder="留空 = 用各模型自己的上限" style="max-width:320px">'
    + '        <div class="help">留空表示不额外设限，每个模型用自己车道的上限；填数字则在此之上再压一道刹车。</div></div>'
    + '      <div class="field"><label class="lbl" for="f-maxWallClockMs">单轮墙钟上限（毫秒）</label>'
    + '        <input type="number" id="f-maxWallClockMs" value="' + esc(S.form.maxWallClockMs) + '" placeholder="0" style="max-width:320px" min="0">'
    + '        <div class="help">一轮请求的总耗时上限，出厂默认 180000（3 分钟）：连续命中慢超时节点时'
    + '兜住总耗时（20 次尝试 × 超时）。填 0 = 不限，交给最坏小时的长尾。只掐会前重试的长扫池，已开'
    + '始吐字的流式回合不受它腰斩。</div></div>'
    + '      <div class="field"><label class="lbl" for="f-exitConcurrency">单出口并发上限</label>'
    + '        <input type="number" id="f-exitConcurrency" value="' + esc(S.form.exitConcurrency) + '" placeholder="0" style="max-width:320px" min="0" max="128">'
    + '        <div class="help">同一个出口 IP 上最多的在途请求数，0 = 不限（默认）。超出的请求在网关内'
    + '排队等槽（不算失败、不换出口），请求发出前不占「在途」计数。并发敏感的代理出口设 3～5 可以少挨上游限流。</div></div>'
    + '    </div></div>'

    + '  <div class="card sec"><div class="card-head"><h2>危险操作</h2></div>'
    + '    <div class="card-body inline">'
    + '      <button class="btn btn-danger" id="btnProbeForce">立即探测一轮</button>'
    + '      <span class="dim" style="font-size:12px">与概览/节点页的「立即探测」是同一个动作（触发一轮热区复检 + 冷区扫描）。'
    + '      探测没有结果缓存，每一轮都是现场重扫；只认节点当前的存活与层级记录，不清空它们——没有「重置」语义。</span>'
    + '    </div></div>'
    + '</div>'
  }

  /* ═══════════════════════════════════════════════════════════════════
     14. 渲染
     ═══════════════════════════════════════════════════════════════════ */
  const VIEWS = {
    overview: viewOverview, nodes: viewNodes, models: viewModels,
    usage: viewUsage, logs: viewLogs, settings: viewSettings
  }

  function render() {
    derive()
    lastRenderSig = statusSig()
    renderNav()
    renderStatusbar()
    $('pageTitle').textContent = TITLES[S.view] || ''
    const c = $('content')
    c.innerHTML = VIEWS[S.view]()
    if (S.view === 'logs' && S.logAuto) {
      const lb = $('logbox')
      if (lb) lb.scrollTop = lb.scrollHeight
    }
    checkDirty()
  }

  /* 局部刷新，保留滚动位置与输入焦点 */
  function rerenderSoft() {
    const c = $('content')
    const top = c.scrollTop
    const activeId = document.activeElement && document.activeElement.id
    const selStart = document.activeElement && document.activeElement.selectionStart
    derive()
    lastRenderSig = statusSig()
    renderNav()
    c.innerHTML = VIEWS[S.view]()
    c.scrollTop = top
    if (activeId) {
      const el = $(activeId)
      if (el && el.focus) {
        el.focus()
        if (selStart != null && el.setSelectionRange) {
          try { el.setSelectionRange(selStart, selStart) } catch (e) { /* number input 不支持 */ }
        }
      }
    }
    if (S.view === 'logs' && S.logAuto) {
      const lb = $('logbox')
      if (lb) lb.scrollTop = lb.scrollHeight
    }
    checkDirty()
  }

  /* 轮询时如果用户正在输入，不要重建内容区——否则光标会被顶走 */
  function isEditing() {
    const a = document.activeElement
    if (!a) return false
    const tag = a.tagName
    return (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') && $('content').contains(a)
  }

  function go(view, replace) {
    if (!VIEWS[view]) view = 'overview'
    S.view = view
    render()
    $('content').scrollTop = 0
    const want = '#' + view
    try {
      if (replace) history.replaceState(null, '', want)
      else if (location.hash !== want) location.hash = view
    } catch (e) { /* file:// 下可能受限 */ }
  }

  /* ── 未保存检测 ─────────────────────────────────────────────────── */
  const FIELD_LABEL = {
    subUrls:'订阅链接', probeEnabled:'自动探测', effortLevel:'思考强度',
    defaultMaxTokens:'输出上限', probeWorkers:'探测并发',
    hotIntervalSec:'热区复检间隔', coldIntervalSec:'冷区扫描间隔', refreshIntervalMin:'订阅刷新间隔',
    maxWallClockMs:'墙钟上限', exitConcurrency:'单出口并发上限',
  }
  function dirtyKeys() {
    const keys = []
    if (JSON.stringify(S.order) !== JSON.stringify(S.baseline.order)) keys.push('出口地区')
    for (const k in FIELD_LABEL) {
      if (String(S.form[k]) !== String(S.baseline.form[k])) keys.push(FIELD_LABEL[k])
    }
    return keys
  }
  function checkDirty() {
    const n = dirtyKeys().length
    $('dirtyCount').textContent = n
    $('dirtybar').className = 'dirtybar' + (n > 0 ? ' show' : '')
  }

  /* ═══════════════════════════════════════════════════════════════════
     15. 与网关通信
     ═══════════════════════════════════════════════════════════════════ */
  /* W22：阻塞型动作请求必须有超时 —— fetch 不设超时的话，一个半死的连接会
     把动作按钮永久卡在 disabled，只能整页刷新。超时中止在这里被翻译成
     明确的中文文案：TimeoutError 的原始 message 是浏览器内部串（"signal timed
     out"/"The operation was aborted"），直接 toast 出去用户看不懂。
     注：这份文件里曾同时存在两个 function j 声明 —— 后者覆盖前者，于是
     「!r.ok 时抛响应体文本」那一半只在读者眼里存在，实际从未运行；
     现在只留这一份，两半行为都在。 */
  function j(url, opt) {
    return fetch(url, opt).catch(function (e) {
      if (e && (e.name === 'TimeoutError' || e.name === 'AbortError')) {
        throw new Error('请求超时，请稍后重试')
      }
      throw e
    }).then(function (r) {
      if (!r.ok) return r.text().then(function (t) { throw new Error(t) })
      return r.json()
    })
  }
  function post(url, timeoutMs) {
    const opt = { method: 'POST' }
    if (typeof AbortSignal !== 'undefined' && AbortSignal.timeout) {
      opt.signal = AbortSignal.timeout(timeoutMs || 180000)
    }
    return j(url, opt)
  }
  function absorbStatus(s) {
    if (!s) return
    if (s.singbox) DATA.singbox = s.singbox
    if (s.forward) DATA.forward = s.forward
    if (s.models) DATA.models = s.models
    if (s.modelCaps) DATA.modelCaps = s.modelCaps
    if ('limits' in s) DATA.limits = s.limits
    if (s.regionModels) DATA.regionModels = s.regionModels
    if (s.nodes) DATA.nodes = s.nodes
    if (s.usage) DATA.usage = s.usage
    if (s.diagnostics) DATA.diagnostics = s.diagnostics
    /* 车道视图与 busy 一起随 5s 轮询更新；没有闸门活动时是空对象。 */
    if (s.lanes) DATA.lanes = s.lanes
    /* F5：probing 位以服务端为准 —— 异步受理后，前端本地的乐观值只活到
       下一次 /api/status 轮询（5s）；探测完成/失败也由这里复位按钮。 */
    if (typeof s.probing === 'boolean') {
      /* 探测完成的沿:异步受理后完成信号只有服务端 probing 位这一处 */
      if (S.probing && !s.probing) {
        toast('探测完成', 'ok')
        refreshLogs()
      }
      S.probing = s.probing
    }
  }
  /* W18：轮询的时序保护。慢的旧响应回来时，新一轮可能已经落地 —— 不做保护
     会把 5 秒前的旧快照覆盖上去(显示回跳)。单调 seq：响应落地前比对发起时
     的序号，不是最新就丢弃；statusBusy 让上一轮没回来时跳过本轮 tick，避免
     请求堆积。 */
  let statusSeq = 0, statusBusy = false
  let lastRenderSig = ''
  /* O18：内容没变就跳过整页重建。节点表是最大头(池上限 8000，每 5s 全量
     innerHTML 重建开销可观)。sig 原来是 JSON.stringify 整个 nodes/usage 数组
     —— 每 5s 对几千节点做一次全量序列化，恰好是 O18 想省的那类开销。改成
     对 **derive() 的聚合结果** 签名：refreshStatus 在比较前本来就跑完了
     derive()，而聚合值(各级计数/延迟分位/用量行)正是渲染实际读的全部输入
     —— 节点条目是原地更新的，只有聚合级签名才既便宜又不漏变更。 */
  function statusSig() {
    const d = D
    const bc = Object.keys(d.bucketCount || {})
      .sort().map(k => { const b = d.bucketCount[k]; return k + ':' + b.total + '/' + b.alive + '/' + b.b }).join(',')
    /* 节点行的可变字段只有 state/latencyMs（探测改写）；tag/country/tier 只随
       整池重建变，那必然连长度都变。逐项拼这俩字段 —— 不做全对象序列化。 */
    const nodesSig = d.nodes.map(n => (n.state && n.state[0]) + n.latencyMs).join('')
    return [
      d.nodes.length, nodesSig, d.alive.length, d.tierA, d.tierB, bc,
      d.models.length, d.models.map(m => m.id + (m.gated ? '!' : '')).join(','),
      JSON.stringify(d.usage.today || {}), d.usage.requests || 0,
      d.rows.map(r => r.model + ':' + r.req + ':' + (r.in + r.out)).join(','),
      JSON.stringify(DATA.singbox), JSON.stringify(DATA.limits),
      JSON.stringify(DATA.diagnostics), S.probing ? 1 : 0,
      JSON.stringify(DATA.lanes || {}),
      (DATA.forward && DATA.forward.busy ? DATA.forward.busy.requests : 0),
      S.routes.length + ':' + (S.routes.length ? JSON.stringify(S.routes[S.routes.length - 1]) : '')
    ].join('|')
  }
  function refreshStatus() {
    if (statusBusy) return Promise.resolve()
    statusBusy = true
    const seq = ++statusSeq
    // 2026-10-06:全量节点太重，5秒轮询不带 nodes（走 ?include_nodes=false），
    // 节点列表页按需从 /api/nodes 分页加载。
    return j('/api/status?include_nodes=false').then(function (s) {
      statusBusy = false
      if (seq !== statusSeq) return
      absorbStatus(s)
      derive()
      renderStatusbar()
      /* 设置页是表单：自动刷新只更新顶栏，不重建内容区 */
      if (S.view === 'settings' || isEditing()) return
      const sig = statusSig()
      if (sig === lastRenderSig) return  /* O18：什么都没变，不重建 DOM */
      lastRenderSig = sig
      rerenderSoft()
    }).catch(function () { statusBusy = false /* 轮询失败静默，下一轮再试 */ })
  }
  function refreshLogs() {
    return j('/api/logs').then(function (d) {
      const lines = d.lines || []
      /* F7：日志页内层滚动器(#logbox)每 15s 被软重建重置 —— O18 的 sig 守卫
         只盖住了 refreshStatus，logs/routes 轮询是无条件 rerenderSoft。内容
         真没变就不重建；变了也先记下内层滚动位置，重建后恢复。
         「没变」逐行比全量而不是看 length+首尾两条：环形缓冲滚动一格时
         长度不变，而中间那一行的内容已经换了 —— 那次滚动正好是用户最想
         看到的更新，旧启发式会把它静默吃掉。行数有上限，O(行长) 可接受。 */
      const unchanged = lines.length > 0 && lines.length === (DATA.logs || []).length &&
        lines.every(function (ln, i) { return ln.msg === (DATA.logs[i] || {}).msg })
      DATA.logs = lines
      if (S.view !== 'logs' || isEditing()) return
      if (unchanged) return
      const lb = $('logbox')
      const keep = lb ? lb.scrollTop : null
      rerenderSoft()
      if (lb) { /* rerenderSoft 已重建 DOM，重新取 */
        const nb = $('logbox')
        if (nb && keep != null && !S.logAuto) nb.scrollTop = keep
      }
    }).catch(function () { /* 同上 */ })
  }
  function refreshRoutes() {
    return j('/api/routes?limit=20').then(function (d) {
      const rows = d.rows || []
      const unchanged = rows.length === S.routes.length && rows.length > 0 &&
        JSON.stringify(rows[rows.length - 1]) === JSON.stringify(S.routes[rows.length - 1])
      S.routes = rows
      if (S.view !== 'logs' || isEditing()) return
      if (unchanged) return
      rerenderSoft()
    }).catch(function () { /* 静默：轨迹是增强信息 */ })
  }

  function runProbe() {
    if (S.probing) return
    S.probing = true
    if (S.view === 'overview') rerenderSoft()
    /* F5：探测改异步受理 —— POST 立即返回（已在跑则回 409/错误），进度与
       完成经 /api/status 的 probing 位呈现。旧阻塞式给 10 分钟超时，大池上
       fetch 一掐、C2 取消守卫把整轮丢掉，白等且零结论。本地 S.probing 只是
       乐观值：下一次 refreshStatus 会用服务端位纠正。 */
    post('/api/probe', 15000)
      .then(function () { return refreshStatus() })
      .then(function () {
        toast('探测已受理，正在后台进行；按钮状态随 /api/status 的 probing 更新')
      })
      .catch(function (e) {
        S.probing = false
        if (S.view === 'overview') rerenderSoft()
        toast('探测失败：' + (e && e.message ? e.message : e), 'bad')
      })
  }

  function saveSettings() {
    const body = {
      subUrls: S.form.subUrls.split('\n').map(function (x) { return x.trim() }).filter(Boolean),
      countries: S.order.slice(),
      probeEnabled: S.form.probeEnabled,
      /* 后端硬上限 128，HTML 的 max 只是提示。前端 clamp 而不是放行 400 */
      probeWorkers: Math.min(128, Math.max(1, Number(S.form.probeWorkers) || 24)),
      /* 1.3.0 三档间隔:clamp 与后端读取处/PUT 校验同口径(热 15..3600 秒、
         冷 60..86400 秒、订阅刷新 5..10080 分钟)。同一纪律:前端 clamp,
         不放行 400。旧 probeIntervalMin 退役,不再回写。 */
      hotIntervalSec: Math.min(3600, Math.max(15, Math.round(Number(S.form.hotIntervalSec) || 60))),
      coldIntervalSec: Math.min(86400, Math.max(60, Math.round(Number(S.form.coldIntervalSec) || 300))),
      refreshIntervalMin: Math.min(10080, Math.max(5, Math.round(Number(S.form.refreshIntervalMin) || 30))),
      /* 0 = 不限(默认)；负数按 0 归一 */
      maxWallClockMs: Math.max(0, Math.round(Number(S.form.maxWallClockMs) || 0)),
      /* 0 = 不限(默认)；同一出口 IP 的最大在途数 */
      exitConcurrency: Math.min(128, Math.max(0, Math.round(Number(S.form.exitConcurrency) || 0))),
      effortLevel: S.form.effortLevel || 'balanced',
      /* 空值 = 不额外设限（每个模型用自己车道的上限）。必须发 null 而不是 undefined
         —— JSON.stringify 会丢掉 undefined，旧值就会活过整个往返，字段永远清不掉。 */
      defaultMaxTokens: (function () {
        const v = Number(S.form.defaultMaxTokens)
        return isFinite(v) && v > 0 ? v : null
      })(),
    }
    return j('/api/settings', {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body)
    }).then(function (applied) {
      DATA.settings = applied
      readSettings()
      toast('已保存并应用', 'ok')
      render()
      return refreshStatus()
    }).catch(function (e) {
      toast('保存失败：' + (e && e.message ? e.message : e), 'bad')
    })
  }

  /* ═══════════════════════════════════════════════════════════════════
     16. 事件
     ═══════════════════════════════════════════════════════════════════ */
  document.addEventListener('click', function (e) {
    const t = e.target

    const nav = t.closest('[data-nav]')
    if (nav) { go(nav.getAttribute('data-nav')); return }

    const cp = t.closest('[data-copy]')
    if (cp) { copy($(cp.getAttribute('data-copy')).textContent, cp.getAttribute('data-label'), cp); return }

    const cpt = t.closest('[data-copy-tpl]')
    if (cpt) {
      const fw = DATA.forward || {}
      const base = 'http://127.0.0.1:' + (fw.port == null ? 3457 : fw.port) + '/v1'
      const key = fw.key || ''
      const sample = D.models.length ? D.models[0].id : 'big-pickle'
      const map = {
        env: 'OPENAI_BASE_URL=' + base + '\nOPENAI_API_KEY=' + key,
        curl: 'curl ' + base + '/chat/completions \\\n  -H "Authorization: Bearer ' + key + '" \\\n'
          + '  -H "Content-Type: application/json" \\\n'
          + '  -d \'{"model":"' + sample + '","messages":[{"role":"user","content":"hi"}]}\'',
        json: JSON.stringify({ baseURL: base, apiKey: key }, null, 2)
      }
      copy(map[cpt.getAttribute('data-copy-tpl')], cpt.textContent.trim(), cpt)
      return
    }

    const ct = t.closest('[data-copy-text]')
    if (ct) { copy(ct.getAttribute('data-copy-text'), ct.getAttribute('data-label'), ct); return }

    const st = t.closest('#stateSeg button')
    if (st) { S.nodeState = st.getAttribute('data-st'); rerenderSoft(); return }

    const bk = t.closest('[data-bucket]')
    if (bk) {
      const v = bk.getAttribute('data-bucket')
      S.nodeBucket = S.nodeBucket === v ? 'all' : v
      rerenderSoft()
      return
    }

    const th = t.closest('th[data-sort]')
    if (th) {
      const k = th.getAttribute('data-sort')
      if (S.nodeSort === k) S.nodeSortDir *= -1
      else { S.nodeSort = k; S.nodeSortDir = 1 }
      rerenderSoft()
      return
    }

    const lv = t.closest('#levelSeg button')
    if (lv) { S.logLevel = lv.getAttribute('data-lv'); rerenderSoft(); return }

    const add = t.closest('[data-add]')
    if (add) {
      const id = add.getAttribute('data-add')
      if (S.order.indexOf(id) < 0) S.order.push(id)
      rerenderSoft()
      return
    }
    const del = t.closest('[data-del]')
    if (del) { S.order.splice(Number(del.getAttribute('data-del')), 1); rerenderSoft(); return }
    const mv = t.closest('[data-move]')
    if (mv) {
      const i = Number(mv.getAttribute('data-move'))
      const d = Number(mv.getAttribute('data-dir'))
      const jj = i + d
      if (jj >= 0 && jj < S.order.length) {
        const x = S.order.splice(i, 1)[0]
        S.order.splice(jj, 0, x)
        rerenderSoft()
      }
      return
    }

    if (t.closest('#themeBtn')) {
      const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark'
      setTheme(next)
      try { localStorage.setItem('ofr-theme', next) } catch (err) { /* 隐私模式 */ }
      return
    }

    if (t.closest('#btnCopyAll')) {
      copy(D.models.map(function (m) { return m.id }).join('\n'), '全部模型 id', t.closest('button'))
      return
    }
    if (t.closest('#btnLogCopy')) {
      const rows = buildLogRows().rows
      copy(rows.map(function (l) {
        return fmtTime(l.t) + ' [' + l.level + '] ' + l.msg + (l.n > 1 ? ' ×' + l.n : '')
      }).join('\n'), '日志', t.closest('button'))
      return
    }

    if (t.closest('#btnProbe') || t.closest('#btnProbe2') || t.closest('#btnProbeForce')) { runProbe(); return }
    if (t.closest('#btnRefresh')) {
      /* I28：防重入 —— 连点会并发多个 POST /api/refresh，后端按 rebuildMu
         排队但前端 toast 会乱套 */
      if (S.busy) return
      S.busy = true
      toast('已触发订阅刷新，正在重建节点池…')
      post('/api/refresh', 300000).then(function () { return refreshStatus() })
        .then(function () { S.busy = false; toast('订阅已刷新，节点池已重建', 'ok') })
        .catch(function (e) { S.busy = false; toast('刷新失败：' + (e && e.message ? e.message : e), 'bad') })
      return
    }
    if (t.closest('#btnLimits') || t.closest('#btnLimits2')) {
      if (S.busy) return
      S.busy = true
      toast('正在强制刷新限额表…')
      post('/api/limits', 120000).then(function (r) {
        S.busy = false
        toast('限额表已更新：' + (r && r.rows != null ? r.rows : '?') + ' 行', 'ok')
        return refreshStatus()
      }).catch(function (e) { S.busy = false; toast('刷新失败：' + (e && e.message ? e.message : e), 'bad') })
      return
    }
    if (t.closest('#btnDiscard')) {
      S.order = JSON.parse(JSON.stringify(S.baseline.order))
      S.form = JSON.parse(JSON.stringify(S.baseline.form))
      render()
      toast('已放弃更改')
      return
    }
    if (t.closest('#btnSave')) { saveSettings(); return }
  })

  document.addEventListener('input', function (e) {
    const t = e.target
    if (t.id === 'nodeQ') { S.nodeQ = t.value; rerenderSoft(); return }
    if (t.id === 'logQ') { S.logQ = t.value; rerenderSoft(); return }
    const fmap = {
      'f-subUrls':'subUrls', 'f-probeWorkers':'probeWorkers', 'f-hotIntervalSec':'hotIntervalSec',
      'f-coldIntervalSec':'coldIntervalSec', 'f-refreshIntervalMin':'refreshIntervalMin',
      'f-effortLevel':'effortLevel', 'f-defaultMaxTokens':'defaultMaxTokens', 'f-maxWallClockMs':'maxWallClockMs',
      'f-exitConcurrency':'exitConcurrency',
    }
    if (fmap[t.id]) { S.form[fmap[t.id]] = t.value; checkDirty() }
  })

  document.addEventListener('change', function (e) {
    const t = e.target
    if (t.id === 'f-probeEnabled') { S.form.probeEnabled = t.checked; checkDirty(); return }
    if (t.id === 'cbDedupe') { S.logDedupe = t.checked; rerenderSoft(); return }
    if (t.id === 'cbNoise') { S.logFoldNoise = t.checked; rerenderSoft(); return }
    if (t.id === 'cbAuto') { S.logAuto = t.checked; return }
  })

  /* ── 拖拽排序 ───────────────────────────────────────────────────── */
  let dragIdx = null
  document.addEventListener('dragstart', function (e) {
    const it = e.target.closest && e.target.closest('.order-item')
    if (!it) return
    dragIdx = Number(it.getAttribute('data-idx'))
    it.className += ' dragging'
    e.dataTransfer.effectAllowed = 'move'
    try { e.dataTransfer.setData('text/plain', String(dragIdx)) } catch (err) { /* 忽略 */ }
  })
  document.addEventListener('dragover', function (e) {
    const it = e.target.closest && e.target.closest('.order-item')
    if (!it || dragIdx === null) return
    e.preventDefault()
    document.querySelectorAll('.order-item.drop-target').forEach(function (x) {
      x.className = x.className.replace(' drop-target', '')
    })
    it.className += ' drop-target'
  })
  document.addEventListener('drop', function (e) {
    const it = e.target.closest && e.target.closest('.order-item')
    if (!it || dragIdx === null) return
    e.preventDefault()
    const to = Number(it.getAttribute('data-idx'))
    if (to !== dragIdx) {
      const x = S.order.splice(dragIdx, 1)[0]
      S.order.splice(to, 0, x)
    }
    dragIdx = null
    rerenderSoft()
  })
  document.addEventListener('dragend', function () {
    dragIdx = null
    document.querySelectorAll('.order-item').forEach(function (x) {
      x.className = x.className.replace(' dragging', '').replace(' drop-target', '')
    })
  })

  window.addEventListener('hashchange', function () {
    const v = location.hash.slice(1)
    if (VIEWS[v] && v !== S.view) { S.view = v; render(); $('content').scrollTop = 0 }
  })

  /* ── 主题 ───────────────────────────────────────────────────────── */
  function setTheme(t) {
    document.documentElement.setAttribute('data-theme', t)
    const btn = $('themeBtn')
    if (btn) btn.textContent = t === 'dark' ? '◐ 浅色' : '◐ 深色'
  }

  /* ═══════════════════════════════════════════════════════════════════
     17. 启动
     ═══════════════════════════════════════════════════════════════════ */
  try {
    const forced = new URLSearchParams(location.search).get('theme')
    const saved = localStorage.getItem('ofr-theme')
    if (forced === 'dark' || forced === 'light') setTheme(forced)
    else if (saved === 'dark' || saved === 'light') setTheme(saved)
    else setTheme('light')
  } catch (e) { setTheme('light') }

  readSettings()
  /* L1:boot 现在带 probing/lanes 了,首帧也要吸收一次 —— 开机后 warmUp
     探测立刻在跑,浏览器这时打开面板,不吸收就把「立即探测」按钮画成可点,
     点下去只收到一句 already running。守卫与 absorbStatus 逐字相同。 */
  if (typeof DATA.probing === 'boolean') S.probing = DATA.probing
  const v = $('verTxt')
  if (v && window.__BOOT__ && window.__BOOT__.version) v.textContent = 'v' + window.__BOOT__.version

  go(location.hash.slice(1) || 'overview', true)
  setInterval(refreshStatus, 5000)
  setInterval(function () { refreshLogs(); refreshRoutes() }, 15000)
})()
