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
import { fileURLToPath } from 'node:url'
import { execFile } from 'node:child_process'
import { generateKey, startForwardServer } from './forward.js'
import { createEngine } from './engine.js'
import { startPanel } from './panel.js'
import { JsonStore, SETTINGS_INITIAL } from './store.js'
import { fetchSub, filterByGroups, loadCache, saveCache, bucketOf, countryOf, GROUPS } from './sub.js'
import { assignPorts, sanitizeOutbound, buildConfig, writeConfig, startSingbox, watchSingbox, waitPort, waitPortFree } from './singbox.js'
import { fetchUpstreamIds, probeModel } from './probe.js'
import { probeAll } from './nodeprobe.js'
import { buildCatalog } from './catalog.js'
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

const state = () => ({ catalog, membership, settings: settingsOf(), attributionUserAgent: '' })

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

/**
 * Refresh the free-model catalog. Always via DIRECT connection: the models
 * listing is anonymous-200 and direct is the fastest, most deterministic path —
 * the user sees the model list seconds after the program opens, without
 * waiting for nodes to be probed.
 */
async function refreshCatalog(attempt = 0) {
  try {
    const ids = await fetchUpstreamIds({ exitAddr: 'direct' })
    catalog = buildCatalog(ids)
    membership = { 'our-free-model': catalog.map(entry => entry.id) }
    clearTimeout(catalogRetryTimer)
    log(`catalog: ${catalog.length} free models via direct`)
  } catch (error) {
    log(`catalog refresh failed (${error?.message ?? error}); retry ${Math.min(attempt + 1, 5)}/5 in 60s`)
    if (attempt < 5) {
      clearTimeout(catalogRetryTimer)
      catalogRetryTimer = setTimeout(() => void refreshCatalog(attempt + 1), 60000)
      catalogRetryTimer.unref?.()
    }
  }
}

// ---- rebuild ----------------------------------------------------------------

const portBlacklist = new Set() // ports that made sing-box FATAL (bind conflict); never reused this session

async function rebuild(attempt = 0) {
  if (rebuilding) { rebuildAgain = Math.max(rebuildAgain ?? 0, attempt); return }
  rebuilding = true
  try {
    const s = settingsOf()
    const sources = s.subUrls.length > 0 ? s.subUrls : undefined
    const cacheFile = path.join(DATA, 'subs_cache.json')
    let sub = await fetchSub({ sources }).catch(() => null)
    if (!sub) sub = loadCache(cacheFile)
    if (!sub) {
      log('no subscription available (settings empty, cache empty) — staying idle; open the panel to add one')
      return
    }
    saveCache(sub, cacheFile)
    if (sub.details) {
      log('订阅源: ' + sub.details.map(d => `${d.url.slice(0, 48)} → ${d.ok ? d.nodes + ' 节点' : '失败(' + d.error + ')'}`).join(' | '))
    }
    const { picked, stats } = filterByGroups(sub.outbounds, s.countries)
    // 拉取结果只是注册表的增量输入：新节点加入、重复丢弃；源抖动不会缩小池子
    const merged = registry.mergeNodes(picked)
    registry.prune()
    log(`国家分桶: 本轮 ${stats.total} 出站（匹配 ${stats.matched}）→ 新增 ${merged.added}、变更 ${merged.updated}、重复丢弃 ${merged.duplicate}；注册表现有 ${registry.all().length} 节点`)
    if (stats.matched === 0) {
      log('样例 tag: ' + sub.outbounds.slice(0, 4).map(o => o.tag).join(' | ').slice(0, 220))
    }
    const candidates = registry.all()
    const sanitized = candidates.map(o => sanitizeOutbound(structuredClone(o))).filter(Boolean)
    ports = assignPorts(sanitized, ports, { base: s.portBase, span: s.portSpan, avoid: portBlacklist })
    const ported = sanitized.filter(o => ports[o.tag] != null)
    if (ported.length < picked.length) {
      log(`端口段 ${s.portBase}+${s.portSpan} 已满：${candidates.length - ported.length} 个节点未启用 — 可在面板调大"端口段容量"`)
    }
    if (ported.length === 0 && singboxProc !== null) {
      log('no node fits the port range — keeping the serving instance')
      return
    }
    if (ported.length === 0) {
      log('no node fits the port range — enlarge 端口段容量 in the panel')
      return
    }
    pool = ported.map(o => ({ tag: o.tag, country: countryOf(o.tag) }))
    const configPath = path.join(DATA, 'singbox.json')
    writeConfig(configPath, buildConfig(ported, ports, { catchAllPort: s.catchAllPort }))

    // Static validation BEFORE touching the serving instance, with self-heal:
    // any single bad node (bad uuid / unknown field / …) fails the whole config
    // decode, and the error names `outbounds[N]` — drop that node and re-check,
    // up to 5 rounds. Async: a 1000+ inbound check blocks for seconds.
    let checkList = ported
    let checkError = ''
    for (let checkAttempt = 0; checkAttempt < 5; checkAttempt++) {
      writeConfig(configPath, buildConfig(checkList, ports, { catchAllPort: s.catchAllPort }))
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
    if (checkError || checkList.length === 0) {
      log('check 未收敛或无可用节点 — 保留当前实例继续服务')
      return
    }
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
      waitPort(s.catchAllPort, bootTimeout).then(ok => done({ ok })).catch(() => done({ ok: false }))
    })
    if (!outcome.ok) {
      // Self-heal (Free-Router purgeStablePortsFromError): a bind conflict names
      // the port — blacklist it, free its node for a new port, retry once.
      const m = /listen tcp [^:]*:(\d+): bind/.exec(stderrTail)
      if (m && attempt < 2) {
        const port = Number(m[1])
        const tag = Object.keys(ports).find(k => ports[k] === port)
        portBlacklist.add(port)
        if (tag) delete ports[tag]
        log(`端口 ${port} 被外部占用（${tag ? '节点 ' + tag.slice(0, 30) : '未知'}），已拉黑并换端口重试`)
        rebuildAgain = attempt + 1; return
      }
      log('新实例启动失败且无法自愈 — 网关空闲；可在面板点"刷新订阅并重建"重试')
      return
    }
    singboxProc = proc
    await waitPort(s.catchAllPort, 15000).catch(() => log('catch-all port not accepting yet — watchdog will re-check'))
    if (watchdogTimer) { clearInterval(watchdogTimer); watchdogTimer = null }
    watchdogTimer = watchSingbox(proc, s.catchAllPort, () => {
      if (singboxProc !== proc) return // 陈旧看门狗：实例已被换装，忽略
      logger.error('sing-box exited (code ' + proc.exitCode + ', signal ' + (proc.signalCode ?? 'null') + ') stderr: ' + stderrTail.slice(-200))
      log('sing-box died — rebuilding in 5s')
      setTimeout(() => void rebuild().catch(() => {}), 5000)
    })
    health.pruneStale(ported.map(o => o.tag))
    health.persistHealth()
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
    for (const model of health.regionSnapshot().models) {
      for (const nodeKey of health.regionProbeCandidates(model, { max: 24 })) {
        const port = ports[nodeKey]
        if (port == null) continue
        const result = await probeModel({ id: model }, { exitAddr: `http://127.0.0.1:${port}`, timeoutMs: 20000 })
        if (result.state === 'available') health.noteRegionOK(model, nodeKey)
        else if (result.state === 'region-blocked') health.noteRegionError(model, nodeKey)
      }
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
      nodes: pool.map(node => ({ tag: node.tag, country: node.country, port: ports[node.tag], ...(health.nodeSnapshot()[node.tag] ?? { state: 'unknown', latencyMs: -1 }) })),
      regionModels: health.regionSnapshot().models,
      usage: { today: st.days?.[today] ?? { req: 0, in: 0, out: 0 }, requests: st.requests, byModel: st.models ?? {} },
    }
  },
  actions: { probeNow, refresh: rebuild },
  log: message => logger.warn('panel:', message),
})

log(`panel   : http://127.0.0.1:${panel.port}`)
log(`forward : http://127.0.0.1:${forward.port}/v1 (key ${settingsOf().forwardKey.slice(0, 8)}…)`)
void refreshCatalog() // 直连立即拉模型列表：程序一开就能看到，不等订阅/节点
await rebuild().catch(error => log(`initial rebuild failed (idle state): ${error?.message ?? error}`))
setInterval(() => void probeNow().catch(() => {}), Math.max(5, settingsOf().probeIntervalMin) * 60_000).unref?.()
setInterval(() => void rebuild().catch(() => {}), 6 * 3600_000).unref?.()

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
