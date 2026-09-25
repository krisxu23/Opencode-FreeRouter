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
import { generateKey, startForwardServer } from './forward.js'
import { createEngine } from './engine.js'
import { startPanel } from './panel.js'
import { JsonStore, SETTINGS_INITIAL } from './store.js'
import { fetchSub, filterByCountries, loadCache, saveCache, countryOf } from './sub.js'
import { assignPorts, sanitizeOutbound, buildConfig, writeConfig, startSingbox, watchSingbox, waitPort } from './singbox.js'
import { fetchUpstreamIds, probeModel } from './probe.js'
import { probeAll } from './nodeprobe.js'
import { buildCatalog } from './catalog.js'
import { pickExit } from './health.js'
import * as health from './health.js'

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const DATA = path.join(ROOT, 'data')

function log(...parts) {
  console.log(new Date().toISOString().slice(11, 19), '[gateway]', ...parts)
}

// ---- settings ---------------------------------------------------------------

const settingsStore = new JsonStore(path.join(DATA, 'settings.json'), SETTINGS_INITIAL)
const settingsOf = () => settingsStore.get()
if (!settingsOf().forwardKey) {
  settingsStore.update({ forwardKey: generateKey() })
  settingsStore.flush()
}
health.setHealthFile(path.join(DATA, 'node-health.json'))

// ---- live state ---------------------------------------------------------------

let catalog = []
let membership = { 'our-free-model': [] }
let pool = []           // [{tag, country}] — nodes of the current instance
let ports = {}          // tag -> local inbound port
let singboxProc = null
let rebuilding = false
let rebuildAgain = false
let probeRunning = false
let probeRerun = false

const state = () => ({ catalog, membership, settings: settingsOf(), attributionUserAgent: '' })
const engine = createEngine({
  state,
  recordUsage: () => {},
  settingsOf,
  poolOf: () => pool,
  portOf: tag => ports[tag],
})

// ---- catalog ----------------------------------------------------------------

async function refreshCatalog() {
  const s = settingsOf()
  const picked = pool.length > 0 ? pickExit({ model: 'catalog:refresh', countries: s.countries, pool, portOf: tag => ports[tag] }) : null
  const exitAddr = picked?.addr ?? 'direct' // verified anonymous direct 200; a node exit is preferred when one exists
  try {
    const ids = await fetchUpstreamIds({ exitAddr })
    catalog = buildCatalog(ids)
    membership = { 'our-free-model': catalog.map(entry => entry.id) }
    log(`catalog: ${catalog.length} free models via ${exitAddr}`)
  } catch (error) {
    log(`catalog refresh failed: ${error?.message ?? error}`)
  }
}

// ---- rebuild ----------------------------------------------------------------

async function rebuild() {
  if (rebuilding) { rebuildAgain = true; return }
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
    const picked = filterByCountries(sub.outbounds, s.countries).map(o => sanitizeOutbound(structuredClone(o)))
    if (picked.length === 0 && singboxProc !== null) {
      log(`parsed 0 nodes for countries [${s.countries.join(',')}] (mid-refresh?) — keeping the serving instance`)
      return
    }
    if (picked.length === 0) {
      log(`parsed 0 nodes for countries [${s.countries.join(',')}] — check the country list in the panel`)
      return
    }
    ports = assignPorts(picked, ports)
    pool = picked.map(o => ({ tag: o.tag, country: countryOf(o.tag) }))
    const configPath = path.join(DATA, 'singbox.json')
    writeConfig(configPath, buildConfig(picked, ports, { catchAllPort: s.catchAllPort }))

    const proc = startSingbox(path.join(ROOT, 'bin', 'sing-box.exe'), configPath)
    proc.stderr?.on('data', chunk => {
      const line = String(chunk).trim()
      if (/ERROR|FATAL/.test(line)) log('sing-box:', line)
    })
    proc.stdout?.on('data', () => {})
    // Settle window: a config error exits within ms. Only a survivor replaces
    // the serving instance (failKeepOld).
    const alive = await new Promise(resolve => {
      if (proc.exitCode !== null) return resolve(false)
      const timer = setTimeout(() => resolve(true), 2000)
      proc.once('exit', () => { clearTimeout(timer); resolve(false) })
    })
    if (!alive) {
      log('new sing-box instance failed to start — keeping the previous one')
      return
    }
    const previous = singboxProc
    singboxProc = proc
    previous?.kill()
    await waitPort(s.catchAllPort, 15000).catch(() => log('catch-all port not accepting yet — watchdog will re-check'))
    watchSingbox(proc, s.catchAllPort, () => {
      log('sing-box died — rebuilding in 5s')
      setTimeout(() => void rebuild().catch(() => {}), 5000)
    })
    health.pruneStale(picked.map(o => o.tag))
    health.persistHealth()
    log(`rebuild ok: ${picked.length} nodes, ports ${Math.min(...Object.values(ports))}-${Math.max(...Object.values(ports))}`)
    void refreshCatalog()
    setTimeout(() => void probeNow(), 12000)
  } finally {
    rebuilding = false
    if (rebuildAgain) { rebuildAgain = false; void rebuild().catch(() => {}) }
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
    await probeAll(pool, {
      addrOf: node => `http://127.0.0.1:${ports[node.tag]}`,
      workers: Math.max(4, settingsOf().probeWorkers),
      onResult: (node, result) => {
        health.markProbe(node.tag, result)
        if (result.state === 'alive') alive += 1
      },
    })
    health.persistHealth()
    log(`probe round: ${alive}/${pool.length} alive in ${((Date.now() - t0) / 1000).toFixed(1)}s`)
    // First boot fetches the catalog through an unprobed (possibly dead) node;
    // once a round has measured real exits, refresh the catalog through a good one.
    if (alive > 0) void refreshCatalog()

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

async function startForwardResilient() {
  const preferred = settingsOf().forwardPort
  try {
    return await startForwardServer({
      config: () => ({ host: '127.0.0.1', port: preferred, enabled: true, key: settingsOf().forwardKey }),
      complete: engine.complete,
      modelRows: engine.modelRows,
      log: message => log('forward:', message),
    })
  } catch (error) {
    log(`forward port ${preferred} unavailable (${error?.code ?? error}) — falling back to a random port`)
    return startForwardServer({
      config: () => ({ host: '127.0.0.1', port: 0, enabled: true, key: settingsOf().forwardKey }),
      complete: engine.complete,
      modelRows: engine.modelRows,
      log: message => log('forward:', message),
    })
  }
}

const forward = await startForwardResilient()

const panel = await startPanel({
  port: settingsOf().panelPort,
  getSettings: settingsOf,
  applySettings: patch => {
    settingsStore.update(patch)
    settingsStore.flush()
    void rebuild().catch(error => log('rebuild after settings change failed:', error?.message ?? error))
    return settingsOf()
  },
  status: () => ({
    singbox: { running: singboxProc !== null && singboxProc.exitCode === null, pid: singboxProc?.pid ?? null, catchAllPort: settingsOf().catchAllPort },
    forward: { running: true, port: forward.port, key: settingsOf().forwardKey },
    models: catalog.map(entry => entry.id), // buildCatalog 只保留免费车道，付费模型不进目录
    nodes: pool.map(node => ({ tag: node.tag, country: node.country, port: ports[node.tag], ...(health.nodeSnapshot()[node.tag] ?? { state: 'unknown', latencyMs: -1 }) })),
    regionModels: health.regionSnapshot().models,
  }),
  actions: { probeNow, refresh: rebuild },
  log: message => log('panel:', message),
})

log(`panel   : http://127.0.0.1:${panel.port}`)
log(`forward : http://127.0.0.1:${forward.port}/v1 (key ${settingsOf().forwardKey.slice(0, 8)}…)`)
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
process.on('exit', () => {
  try { singboxProc?.kill() } catch { /* already gone */ }
  settingsStore.flush()
})
