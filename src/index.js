/**
 * Boot orchestration and the rebuild/probe lifecycle.
 *
 * Boot order solves the chicken-and-egg of "settings UI needs the gateway up,
 * sing-box needs the subscription": panel + forward listeners start first (idle
 * state — the panel is reachable to enter a subscription), then the first
 * rebuild brings sing-box up when a subscription (or its disk cache) arrives.
 *
 * Lifecycle contracts (from Free-Router's battle scars):
 *   - failKeepOld: a failed rebuild never touches the serving instance;
 *     a zero-node parse (mid-refresh) keeps the current instance.
 *   - readiness: the new process must survive 2s before the old one is killed;
 *     a config error exits immediately, which the settle window catches.
 *   - watchdog restarts a dead sing-box; probe rounds are single-flight with a
 *     rerun flag so a refresh landing mid-round is not lost.
 *   - the first probe round is deferred 12s so the panel answers first.
 *
 * @module src/index.js
 */

import path from 'node:path'
import fs from 'node:fs'
import { fileURLToPath } from 'node:url'
import { execFile } from 'node:child_process'
import { generateKey, startForwardServer } from './forward.js'
import { createEngine } from './engine.js'
import { startPanel } from './panel.js'
import { JsonStore, SETTINGS_INITIAL } from './store.js'
import { fetchSub, filterByGroups, loadCache, saveCache, bucketOf, countryOf, GROUPS } from './sub.js'
import { assignPorts, sanitizeOutbound, buildConfig, writeConfig, startSingbox, watchSingbox, waitPort, waitPortFree, pidHoldingPort, pidImageName, killPid } from './singbox.js'
import { fetchUpstreamIds, probeModel } from './probe.js'
import { pruneDispatchers } from './http.js'
import { probeAll } from './nodeprobe.js'
import { buildCatalog } from './catalog.js'
import { refreshLimits, applyLimitsOverlay, loadLimitsCache } from './limits.js'
import { initLogger, logger } from './logger.js'
import * as registry from './registry.js'
import * as health from './health.js'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const DATA = path.join(ROOT, 'data')
initLogger(path.join(DATA, 'gateway.log'))
registry.initRegistry(path.join(DATA, 'node-registry.json'))

const log = (...parts) => logger.info(...parts)

// ---- settings ---------------------------------------------------------------

const settingsStore = new JsonStore(path.join(DATA, 'settings.json'), SETTINGS_INITIAL)
const settingsOf = () => settingsStore.get()
if (!settingsOf().forwardKey) {
  settingsStore.update({ forwardKey: generateKey() })
  settingsStore.flush()
}
health.setHealthFile(path.join(DATA, 'node-health.json'))

// 设置迁移：旧版 countries 存的是国家码，分桶制后统一映射到固定分组
const GROUP_IDS = new Set(GROUPS)
if (Array.isArray(settingsOf().countries) && !settingsOf().countries.every(c => GROUP_IDS.has(c))) {
  const migrated = [...new Set(settingsOf().countries.map(c => bucketOf(c)))]
  settingsStore.update({ countries: migrated })
  settingsStore.flush()
  log(`设置迁移: 出口国家码 → 固定分组 ${migrated.join(',')}`)
}

// ---- live state ---------------------------------------------------------------

let catalog = []
let membership = { 'our-free-model': [] }
let pool = []           // [{tag, country}] — nodes of the current instance
let ports = {}          // tag -> local inbound port
let singboxProc = null
let watchdogTimer = null
let rebuilding = false
let rebuildAgain = null
let probeRunning = false
let probeRerun = false

const state = () => ({ catalog, membership, settings: settingsOf(), attributionUserAgent: '', limitsById })

// 用量记账（README 亮点"用量看板，全部留在本机"的轻量版）：按天 + 按模型 +
// 最近采样。stats 绝不能影响一次回答，全程 fail-soft。
const statsStore = new JsonStore(path.join(DATA, 'stats.json'), { requests: 0, days: {}, models: {}, samples: [] })
function recordUsage(record) {
  try {
    statsStore.edit(st => {
      const day = new Date(record.at ?? Date.now()).toISOString().slice(0, 10)
      const d = st.days[day] ??= { req: 0, in: 0, out: 0 }
      d.req += 1
      d.in += record.input ?? 0
      d.out += record.output ?? 0
      const m = st.models[record.model] ??= { req: 0, in: 0, out: 0 }
      m.req += 1
      m.in += record.input ?? 0
      m.out += record.output ?? 0
      st.requests += 1
      st.samples.push({ t: record.at ?? Date.now(), model: record.model, ok: record.ok !== false, ttftMs: record.ttftMs ?? null, out: record.output ?? 0 })
      if (st.samples.length > 100) st.samples.splice(0, st.samples.length - 100)
      return st
    })
  } catch { /* stats must never break a turn */ }
}

const engine = createEngine({
  state,
  recordUsage,
  settingsOf,
  poolOf: () => pool,
  portOf: tag => ports[tag],
})

// ---- catalog ----------------------------------------------------------------

let catalogRetryTimer
let lastUpstreamIds = []
const LIMITS_CACHE_FILE = path.join(DATA, 'modelsdev.json')
const CATALOG_CACHE_FILE = path.join(DATA, 'catalog-ids.json')
// Boot with the disk cache so the first listing already carries Zen-lane
// numbers; the async refresh below then brings it up to date. Fail-soft:
// absent/corrupt cache = empty overlay = local CAPABILITIES table serves.
const _limitsCache = loadLimitsCache(LIMITS_CACHE_FILE)
let limitsById = _limitsCache?.byId ?? {}
let limitsFetchedAt = _limitsCache?.fetchedAt ?? null
let limitsStale = false

/** Last-good upstream id list (atomic temp+rename), so a boot with direct
 *  blocked still shows the model list immediately. */
function loadCatalogCache() {
  try {
    const j = JSON.parse(fs.readFileSync(CATALOG_CACHE_FILE, 'utf8'))
    return Array.isArray(j?.ids) && j.ids.length > 0 ? j.ids.filter(id => typeof id === 'string') : null
  } catch {
    return null
  }
}
function saveCatalogCache(ids) {
  try {
    fs.mkdirSync(path.dirname(CATALOG_CACHE_FILE), { recursive: true })
    const tmp = `${CATALOG_CACHE_FILE}.${process.pid}.tmp`
    fs.writeFileSync(tmp, JSON.stringify({ fetchedAt: Date.now(), ids }))
    fs.renameSync(tmp, CATALOG_CACHE_FILE)
  } catch { /* cache must never break a refresh */ }
}

// Boot straight from disk so the panel shows the list even when direct is
// blocked on this machine (measured: direct fetch failed on the user's host
// while models.dev + node exits worked).
const _bootIds = loadCatalogCache()
if (_bootIds) {
  lastUpstreamIds = _bootIds
  catalog = applyLimitsOverlay(buildCatalog(_bootIds), limitsById)
  membership = { 'our-free-model': catalog.map(entry => entry.id) }
  health.seedRestrictedModels(catalog.filter(entry => entry.regionSensitive).map(entry => entry.id))
}

function applyIds(ids, via) {
  lastUpstreamIds = ids
  saveCatalogCache(ids)
  catalog = applyLimitsOverlay(buildCatalog(ids), limitsById)
  membership = { 'our-free-model': catalog.map(entry => entry.id) }
  // catalog 的 regionSensitive 名单（muse-spark 系）预置为受限模型，补探据此发现特殊节点
  health.seedRestrictedModels(catalog.filter(entry => entry.regionSensitive).map(entry => entry.id))
  clearTimeout(catalogRetryTimer)
  log(`catalog: ${catalog.length} free models via ${via}`)
}

/**
 * Refresh the free-model catalog. DIRECT first (anonymous-200, fastest when
 * the machine's route to opencode.ai is open), then a healthy node exit
 * (pool/port table may be empty before the first rebuild — then skip), then
 * the models.dev overlay ids as last resort (proves nothing about the lane,
 * but keeps the picker + budgets working with Zen-lane numbers).
 */
async function refreshCatalog(attempt = 0) {
  try {
    const ids = await fetchUpstreamIds({ exitAddr: 'direct', timeoutMs: 12000 })
    applyIds(ids, 'direct')
  } catch (directError) {
    // Fallback 1: a live node exit. ports/pool may be empty on first boot.
    const aliveTags = Object.keys(ports)
    if (aliveTags.length > 0) {
      const shuffled = aliveTags.sort(() => Math.random() - 0.5).slice(0, 3)
      for (const tag of shuffled) {
        try {
          const ids = await fetchUpstreamIds({ exitAddr: `http://127.0.0.1:${ports[tag]}`, timeoutMs: 20000 })
          applyIds(ids, `node ${tag.slice(0, 32)}`)
          return
        } catch { /* try next exit */ }
      }
    }
    // Fallback 2: models.dev overlay ids (in-memory, refreshed separately).
    const overlayIds = Object.keys(limitsById)
    if (overlayIds.length > 0) {
      applyIds(overlayIds, 'models.dev overlay')
    } else {
      log(`catalog refresh failed (${directError?.message ?? directError}); retry ${Math.min(attempt + 1, 5)}/5 in 60s`)
      if (attempt < 5) {
        clearTimeout(catalogRetryTimer)
        catalogRetryTimer = setTimeout(() => void refreshCatalog(attempt + 1), 60000)
        catalogRetryTimer.unref?.()
      }
    }
  }
}

/**
 * Refresh the models.dev limit overlay (DIRECT fetch, ~5MB snapshot, 24h TTL).
 * Never throws: a fetch failure keeps serving the stale disk cache, and a
 * missing cache keeps serving the local table. On success the current catalog
 * is rebuilt from the last upstream ids so the new numbers take effect
 * without waiting for the next catalog poll.
 */
async function refreshLimitsOverlay({ force = false } = {}) {
  try {
    const r = await refreshLimits({ cacheFile: LIMITS_CACHE_FILE, force })
    limitsById = r.byId
    limitsFetchedAt = r.fetchedAt
    limitsStale = r.stale === true
    const n = Object.keys(limitsById).length
    if (r.error && n === 0) {
      log(`limits overlay refresh failed (${r.error}) — serving local CAPABILITIES`)
      return r
    }
    if (lastUpstreamIds.length > 0) {
      catalog = applyLimitsOverlay(buildCatalog(lastUpstreamIds), limitsById)
      membership = { 'our-free-model': catalog.map(entry => entry.id) }
    } else if (Object.keys(limitsById).length > 0) {
      // catalog 拉取从未成功（如本机直连被封）时，用 overlay 的 id 先把
      // 列表撑起来：picker 和预算都能工作，lane 真实性由后续 probe 校验。
      applyIds(Object.keys(limitsById), 'models.dev overlay')
    }
    log(`limits overlay: ${n} opencode rows${r.stale ? ' (stale cache)' : ''}${r.error ? ` (fetch failed: ${r.error})` : ''}`)
    return r
  } catch (error) {
    log(`limits overlay refresh failed (${error?.message ?? error}) — keeping previous overlay`)
    return { byId: limitsById, fetchedAt: limitsFetchedAt, stale: true, error: String(error?.message ?? error).slice(0, 160) }
  }
}

// ---- rebuild ----------------------------------------------------------------

const portBlacklist = new Set() // ports that made sing-box FATAL (bind conflict); never reused this session

// PID 台账：只杀我们亲自拉起过的 sing-box（孤儿）；用户自己的 sing-box 客户端一律不碰。
const PID_LEDGER_FILE = path.join(DATA, 'sing-box-pids.json')
function loadPidLedger() {
  try { return JSON.parse(fs.readFileSync(PID_LEDGER_FILE, 'utf8')) } catch { return { current: null, history: [] } }
}
function recordPid(pid) {
  const l = loadPidLedger()
  l.current = pid
  l.history = [...new Set([...(l.history ?? []), pid])].slice(-50)
  try { fs.writeFileSync(PID_LEDGER_FILE, JSON.stringify(l)) } catch { /* fail-soft */ }
}
function isOurPid(pid) {
  const l = loadPidLedger()
  return l.current === pid || (l.history ?? []).includes(pid)
}

async function rebuild(attempt = 0) {
  if (rebuilding) { rebuildAgain = Math.max(rebuildAgain ?? 0, attempt); return }
  rebuilding = true
  try {
    const s = settingsOf()
    // 订阅完全由用户在面板配置。未配置或全部拉取失败时：注册表历史节点继续服务；
    // 注册表也为空则以纯直连兜底模式启动（sing-box 照常运行，所有出站直连）。
    const cacheFile = path.join(DATA, 'subs_cache.json')
    let sub = null
    let stats = { total: 0, matched: 0 }
    // `picked` is read again by the final log line, which sits OUTSIDE this
    // branch — it has to outlive the `if (sub)` block (a block-scoped const
    // there threw `picked is not defined` at the end of every rebuild).
    let picked = []
    if (s.subUrls.length > 0) {
      // 健康出口复拉：本机直连被封时，用上一轮存活节点的本地端口去拉订阅。
      // 优先探测实测 alive 的节点；无 alive 时退化用全部已知端口（尽力而为）。
      const snap = health.nodeSnapshot()
      const alivePorts = Object.entries(snap)
        .filter(([, v]) => v?.state === 'alive')
        .map(([tag]) => ports[tag])
        .filter(p => p != null)
      const poolPorts = (alivePorts.length > 0 ? alivePorts : Object.values(ports).filter(p => p != null)).slice(0, 12)
      const exitAddrs = poolPorts.map(p => `http://127.0.0.1:${p}`)
      sub = await fetchSub({ sources: s.subUrls, exitAddrs }).catch(() => null)
      if (!sub) sub = loadCache(cacheFile)
    }
    if (sub) {
      saveCache(sub, cacheFile)
      if (sub.details) {
        log('订阅源: ' + sub.details.map(d => `${d.url.slice(0, 48)} → ${d.ok ? d.nodes + ' 节点' : '失败(' + d.error + ')'}`).join(' | '))
      }
      const filtered = filterByGroups(sub.outbounds, s.countries)
      picked = filtered.picked
      stats = filtered.stats
      // 拉取结果只是注册表的增量输入：新节点加入、重复丢弃；源抖动不会缩小池子
      const merged = registry.mergeNodes(picked)
      registry.prune()
      log(`国家分桶: 本轮 ${stats.total} 出站（匹配 ${stats.matched}）→ 新增 ${merged.added}、变更 ${merged.updated}、重复丢弃 ${merged.duplicate}；注册表现有 ${registry.all().length} 节点`)
      if (stats.matched === 0) {
        log('样例 tag: ' + sub.outbounds.slice(0, 4).map(o => o.tag).join(' | ').slice(0, 220))
      }
    } else {
      log('未配置订阅或全部拉取失败 — 使用注册表历史节点；注册表为空则以纯直连兜底模式启动')
    }
    const candidates = registry.all()
    // No subscription this round (registry history / direct fallback): the final
    // log line still wants a "共 N" figure, so it falls back to the pool size.
    if (picked.length === 0) picked = candidates
    const sanitized = candidates.map(o => sanitizeOutbound(structuredClone(o))).filter(Boolean)
    ports = assignPorts(sanitized, ports, { base: s.portBase, span: s.portSpan, avoid: portBlacklist })
    const ported = sanitized.filter(o => ports[o.tag] != null)
    if (ported.length < candidates.length) {
      log(`端口段 ${s.portBase}+${s.portSpan} 已满：${candidates.length - ported.length} 个节点未启用 — 可在面板调大"端口段容量"`)
    }
    pool = ported.map(o => ({ tag: o.tag, country: countryOf(o.tag) }))
    // 兜底口被占（可能是你自己的代理软件）时自动顺延，不强抢。
    // 并行扫描：串行 100×400ms 最坏卡启动 40s。
    let catchAllPort = s.catchAllPort
    if (singboxProc === null || singboxProc.exitCode !== null) {
      const scanned = await Promise.all(
        Array.from({ length: 100 }, (_, k) => s.catchAllPort + k).map(async p => ({ p, free: await waitPortFree(p, 400) })),
      )
      const hit = scanned.find(r => r.free)
      if (hit && hit.p !== s.catchAllPort) {
        catchAllPort = hit.p
        log(`兜底口 ${s.catchAllPort} 被占用（可能是本机其他代理软件），改用 ${catchAllPort}`)
      }
    }
    const configPath = path.join(DATA, 'singbox.json')
    writeConfig(configPath, buildConfig(ported, ports, { catchAllPort }))

    // Static validation BEFORE touching the serving instance, with self-heal:
    // any single bad node (bad uuid / unknown field / …) fails the whole config
    // decode, and the error names `outbounds[N]` — drop that node and re-check,
    // up to 5 rounds. Async: a 1000+ inbound check blocks for seconds.
    let checkList = ported
    let checkError = ''
    for (let checkAttempt = 0; checkAttempt < 5; checkAttempt++) {
      writeConfig(configPath, buildConfig(checkList, ports, { catchAllPort }))
      try {
        await new Promise((resolve, reject) => {
          execFile(path.join(ROOT, 'bin', 'sing-box.exe'), ['check', '-c', configPath], { timeout: 30000, maxBuffer: 8 * 1024 * 1024 }, (error, _stdout, stderr) => {
            if (error) reject(Object.assign(error, { stderr }))
            else resolve()
          })
        })
        checkError = ''
        break
      } catch (error) {
        checkError = String(error?.stderr ?? error?.message ?? error)
        const m = /outbounds\[(\d+)\]/.exec(checkError)
        if (!m) {
          logger.error('新配置未通过 sing-box check — 保留当前实例继续服务:', checkError.slice(0, 300))
          return
        }
        const idx = Number(m[1])
        const dropped = checkList[idx]
        checkList = checkList.filter((_, i) => i !== idx)
        if (dropped?.tag) { delete ports[dropped.tag]; registry.remove(dropped.tag); pool = pool.filter(n => n.tag !== dropped.tag) }
        logger.warn(`check 剔除坏节点 outbounds[${idx}] ${dropped?.tag?.slice(0, 44) ?? ''}: ${(checkError.match(/FATAL.*/) ?? [''])[0].slice(0, 130)}`)
      }
    }
    if (checkError) {
      logger.error('新配置未通过 sing-box check — 保留当前实例继续服务:', checkError.slice(0, 300))
      return
    }
    // checkList 可以为空：空节点列表 = 纯直连兜底模式（catch-all + direct 照常启动）
    if (checkList.length === 0) log('直连兜底模式：无可用节点，所有出站直连')
    const previous0 = singboxProc
    if (watchdogTimer) { clearInterval(watchdogTimer); watchdogTimer = null }
    previous0?.kill()
    await waitPortFree(s.catchAllPort, 3000).catch(() => {})
    const proc = startSingbox(path.join(ROOT, 'bin', 'sing-box.exe'), configPath)
    let stderrTail = ''
    // 拨号失败噪音抑制：一轮探测打几百个死节点，每条连接失败都打 2-4 行 ERROR，
    // 实测能刷出 7000+ 行把日志和面板淹掉 —— 按窗口聚合成一行摘要。
    let dialFailures = 0
    let lastDialSummary = Date.now()
    proc.stderr?.on('data', chunk => {
      const line = String(chunk).trim()
      stderrTail = (stderrTail + '\n' + line).slice(-4000)
      if (/FATAL/.test(line)) logger.error('sing-box:', line)
      else if (/connection: open connection|connection download closed|unknown version/.test(line)) {
        dialFailures += 1
        if (Date.now() - lastDialSummary >= 30000) {
          logger.info(`sing-box: 拨号失败 ${dialFailures} 次（多为探测死节点，属正常噪音）`)
          dialFailures = 0
          lastDialSummary = Date.now()
        }
      } else if (/ERROR/.test(line)) logger.warn('sing-box:', line)
      else if (line) logger.info('sing-box:', line)
    })
    proc.stdout?.on('data', () => {})
    // Readiness = the catch-all port accepting (the definitive signal, and the
    // only scale-safe one: 1000+ inbounds take far longer to bind than any
    // fixed settle window — a fixed 2s window misjudged a healthy 1255-node
    // boot as dead, measured). Process exit during the wait = failure.
    const bootTimeout = Math.min(30000, 5000 + ported.length * 10)
    const outcome = await new Promise(resolve => {
      let settled = false
      const done = value => { if (!settled) { settled = true; resolve(value) } }
      proc.once('exit', code => done({ ok: false, code }))
      waitPort(catchAllPort, bootTimeout).then(ok => done({ ok })).catch(() => done({ ok: false }))
    })
    if (outcome.ok) {
      await new Promise(r => setTimeout(r, 400)) // 孤儿占口导致的假成功会在几百 ms 内显形（进程 FATAL 退出）
      if (proc.exitCode !== null) outcome.ok = false
    }
    if (!outcome.ok) {
      // Self-heal (Free-Router purgeStablePortsFromError): a bind conflict names
      // the port — blacklist it, free its node for a new port, retry once.
      const m = /listen tcp [^:]*:(\d+): bind/.exec(stderrTail)
      const bindPort = m ? Number(m[1]) : null
      if (bindPort && attempt < 3) {
        const pid = pidHoldingPort(bindPort)
        const image = pid ? pidImageName(pid) : ''
        // 端口段归属判定：sing-box.exe + 端口在本程序专用段（兜底口 ~ 端口段末尾）
        // = 本程序历次异常退出叠加的孤儿，直接清理；段外的 sing-box 可能是用户
        // 自己的客户端，不碰。
        const rangeStart = Math.min(s.catchAllPort, s.portBase)
        const inOurRange = bindPort >= rangeStart && bindPort <= s.portBase + s.portSpan
        if (image === 'sing-box.exe' && inOurRange) {
          killPid(pid)
          log(`清掉残留孤儿 sing-box (pid ${pid}，端口 ${bindPort} 在本程序专用段内)，换装重试`)
          rebuildAgain = attempt + 1; return
        }
        if (image === 'sing-box.exe') {
          log(`端口 ${bindPort} 被独立的 sing-box 进程占用（不属于本程序端口段）— 未动它，已为本节点换端口`)
          portBlacklist.add(bindPort)
          const t2 = Object.keys(ports).find(k => ports[k] === bindPort)
          if (t2) delete ports[t2]
          rebuildAgain = attempt + 1; return
        }
        const tag = Object.keys(ports).find(k => ports[k] === bindPort)
        portBlacklist.add(bindPort)
        if (tag) { delete ports[tag]; registry.remove(tag) }
        log(`端口 ${bindPort} 被其他程序占用（${image || '未知进程'}），节点已拉黑并换端口重试`)
        rebuildAgain = attempt + 1; return
      }
      log('新实例启动失败且无法自愈 — 网关空闲；可在面板点"刷新订阅并重建"重试')
      return
    }
    singboxProc = proc
    recordPid(proc.pid)
    await waitPort(catchAllPort, 15000).catch(() => log('catch-all port not accepting yet — watchdog will re-check'))
    if (watchdogTimer) { clearInterval(watchdogTimer); watchdogTimer = null }
    watchdogTimer = watchSingbox(proc, catchAllPort, () => {
      if (singboxProc !== proc) return // 陈旧看门狗：实例已被换装，忽略
      logger.error('sing-box exited (code ' + proc.exitCode + ', signal ' + (proc.signalCode ?? 'null') + ') stderr: ' + stderrTail.slice(-200))
      log('sing-box died — rebuilding in 5s')
      setTimeout(() => void rebuild().catch(() => {}), 5000)
    })
    health.pruneStale(ported.map(o => o.tag))
    health.persistHealth()
    // dispatcher 裁剪：端口表每轮 rebuild 都变，过期 ProxyAgent 必须关掉并丢弃，
    // 否则连接泄漏（Free-Router 27k 陈旧条目事故的 dispatcher 版）。
    pruneDispatchers(Object.values(ports).map(p => `http://127.0.0.1:${p}`))
    const portValues = Object.values(ports)
    log(`rebuild ok: ${ported.length} nodes${ported.length < picked.length ? ` (共 ${picked.length}，端口段不够)` : ''}, ports ${Math.min(...portValues)}-${Math.max(...portValues)}`)
    void refreshCatalog()
    setTimeout(() => void probeNow(), 3000) // 首探提前：让面板的"–"尽快变成实测结果
  } finally {
    rebuilding = false
    if (rebuildAgain !== null) { const a = rebuildAgain; rebuildAgain = null; void rebuild(a).catch(() => {}) }
  }
}

// ---- probing ------------------------------------------------------------------

async function probeNow() {
  if (!settingsOf().probeEnabled) return
  if (probeRunning) { probeRerun = true; return }
  probeRunning = true
  try {
    if (pool.length === 0) return
    const t0 = Date.now()
    let alive = 0
    // Workers scale with the pool: fixed 10 over 800+ nodes took longer than a
    // cycle (measured in Free-Router); floor = user setting, ceiling = 128.
    const workers = Math.min(128, Math.max(settingsOf().probeWorkers, Math.ceil(pool.length / 4)))
    await probeAll(pool, {
      addrOf: node => `http://127.0.0.1:${ports[node.tag]}`,
      workers,
      onResult: (node, result) => {
        health.markProbe(node.tag, result)
        registry.markProbeResult(node.tag, result.state === 'alive')
        if (result.state === 'alive') alive += 1
      },
    })
    registry.prune()
    health.persistHealth()
    log(`probe round: ${alive}/${pool.length} alive in ${((Date.now() - t0) / 1000).toFixed(1)}s`)

    // Region matrix supplement: for models measured region-restricted by real
    // traffic, probe up to 24 still-unknown alive exits with the smallest real
    // request (16 tokens). Throttled/unknown verdicts stay unknown.
    // Bounded concurrency (was: serial await — 2 models × 24 nodes × 20s worst
    // case ≈ 16min holding the probeRunning single-flight lock).
    const regionJobs = []
    for (const model of health.regionSnapshot().models) {
      for (const nodeKey of health.regionProbeCandidates(model, { max: 24 })) {
        const port = ports[nodeKey]
        if (port == null) continue
        regionJobs.push({ model, nodeKey, port })
      }
    }
    if (regionJobs.length > 0) {
      let regionCursor = 0
      const regionRun = async () => {
        while (regionCursor < regionJobs.length) {
          const { model, nodeKey, port } = regionJobs[regionCursor++]
          const result = await probeModel({ id: model }, { exitAddr: `http://127.0.0.1:${port}`, timeoutMs: 20000 }).catch(() => null)
          if (!result) continue
          if (result.state === 'available') { health.noteRegionOK(model, nodeKey); health.markRestrictedOk(nodeKey) }
          else if (result.state === 'region-blocked') health.noteRegionError(model, nodeKey)
        }
      }
      await Promise.all(Array.from({ length: Math.max(1, Math.min(8, regionJobs.length)) }, regionRun))
    }
  } finally {
    probeRunning = false
    if (probeRerun) { probeRerun = false; setTimeout(() => void probeNow(), 1000) }
  }
}

// ---- listeners ------------------------------------------------------------------

async function startForwardResilient(logForward) {
  const preferred = settingsOf().forwardPort
  try {
    return await startForwardServer({
      config: () => ({ host: '127.0.0.1', port: preferred, enabled: true, key: settingsOf().forwardKey }),
      complete: engine.complete,
      modelRows: engine.modelRows,
      log: logForward,
    })
  } catch (error) {
    log(`forward port ${preferred} unavailable (${error?.code ?? error}) — falling back to a random port`)
    return startForwardServer({
      config: () => ({ host: '127.0.0.1', port: 0, enabled: true, key: settingsOf().forwardKey }),
      complete: engine.complete,
      modelRows: engine.modelRows,
      log: logForward,
    })
  }
}

const forward = await startForwardResilient(message => logger.warn('forward:', message))

const panel = await startPanel({
  port: settingsOf().panelPort,
  logs: () => logger.recent(400),
  getSettings: settingsOf,
  applySettings: patch => {
    settingsStore.update(patch)
    settingsStore.flush()
    void rebuild().catch(error => logger.error('rebuild after settings change failed:', error?.message ?? error))
    return settingsOf()
  },
  status: () => {
    const st = statsStore.get()
    const today = new Date().toISOString().slice(0, 10)
    return {
      singbox: { running: singboxProc !== null && singboxProc.exitCode === null, pid: singboxProc?.pid ?? null, catchAllPort: settingsOf().catchAllPort },
      forward: { running: true, port: forward.port, key: settingsOf().forwardKey },
      models: catalog.map(entry => entry.id), // buildCatalog 只保留免费车道，付费模型不进目录
      modelCaps: Object.fromEntries(catalog.map(entry => [entry.id, { contextWindow: entry.contextWindow, maxOutput: entry.maxOutput }])),
      limits: { rows: Object.keys(limitsById).length, fetchedAt: limitsFetchedAt, stale: limitsStale },
      nodes: pool.map(node => ({ tag: node.tag, country: node.country, port: ports[node.tag], ...(health.nodeSnapshot()[node.tag] ?? { state: 'unknown', latencyMs: -1 }) })),
      regionModels: health.regionSnapshot().models,
      usage: { today: st.days?.[today] ?? { req: 0, in: 0, out: 0 }, requests: st.requests, byModel: st.models ?? {} },
    }
  },
  actions: { probeNow, refresh: rebuild, refreshLimits: refreshLimitsOverlay },
  log: message => logger.warn('panel:', message),
})

log(`panel   : http://127.0.0.1:${panel.port}`)
log(`forward : http://127.0.0.1:${forward.port}/v1 (key ${settingsOf().forwardKey.slice(0, 8)}…)`)
if (limitsFetchedAt) log(`limits overlay: ${Object.keys(limitsById).length} opencode rows from disk cache`)
void refreshCatalog() // 直连立即拉模型列表：程序一开就能看到，不等订阅/节点
void refreshLimitsOverlay() // models.dev Zen 行 overlay（24h TTL，失败则沿用本地表/磁盘缓存）
await rebuild().catch(error => log(`initial rebuild failed (idle state): ${error?.message ?? error}`))
setInterval(() => void probeNow().catch(() => {}), Math.max(5, settingsOf().probeIntervalMin) * 60_000).unref?.()
setInterval(() => void rebuild().catch(() => {}), 6 * 3600_000).unref?.()
setInterval(() => void refreshLimitsOverlay().catch(() => {}), 24 * 3600_000).unref?.()

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    log(`received ${signal} — shutting down`)
    try { singboxProc?.kill() } catch { /* already gone */ }
    process.exit(0)
  })
}
process.on('unhandledRejection', reason => {
  logger.error('unhandledRejection:', reason?.stack ?? reason)
})
process.on('uncaughtException', error => {
  logger.error('uncaughtException:', error?.stack ?? error)
  // 状态已不可信：落盘后退出，托盘重启即可恢复
  try { singboxProc?.kill() } catch { /* already gone */ }
  settingsStore.flush()
  process.exit(1)
})
process.on('exit', () => {
  try { singboxProc?.kill() } catch { /* already gone */ }
  settingsStore.flush()
  statsStore.flush()
})
