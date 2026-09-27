/**
 * Per-node two-stage probe (cheap tier): liveness, then the upstream gate, then
 * an exit-country echo. Designed to run against every node in the pool on a
 * schedule without burning upstream quota:
 *
 *   stage 1  liveness   — 1.1.1.1/cdn-cgi/trace (IP literal: immune to the
 *                         measured DNS poisoning of gstatic/apple domains) and
 *                         cp.cloudflare.com/generate_204, concurrent, first
 *                         success wins, latency = fastest.
 *   stage 2  upstream   — GET /zen/v1/models on opencode.ai: verified anonymous
 *                         200, zero quota. Liveness only proves "the exit has
 *                         internet"; this proves "the exit reaches the upstream
 *                         we actually care about".
 *   stage 3  echo       — exit IP + country from the first responding source.
 *                         The tag's country claim is advisory; this is the
 *                         ground truth for grouping.
 *
 * Region gating (per model) is NOT detectable here — that needs a real model
 * call, which lives in health.js's region matrix backed by probe.js's
 * quota-burning probeModel, capped at 24 nodes per round.
 *
 * @module src/nodeprobe.js
 */

import { fetch as undiciFetch } from 'undici'
import { dispatcherFor } from './http.js'

export const LIVENESS_URLS = ['https://1.1.1.1/cdn-cgi/trace', 'https://cp.cloudflare.com/generate_204']
export const UPSTREAM_GATE_URL = 'https://opencode.ai/zen/v1/models'
// ip-api.com 的免费档只支持 http —— 明文出境暴露出口 IP，只留两个 https 源。
// 少一个 echo 候选的代价是可接受的：firstSuccess 本来就是"任一成功即够"。
export const ECHO_URLS = [
  'https://api.ip.sb/geoip',
  'https://ipinfo.io/json',
]

async function fetchVia(addr, url, timeoutMs, externalSignal) {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  timer.unref?.()
  if (externalSignal) {
    if (externalSignal.aborted) controller.abort()
    else externalSignal.addEventListener('abort', () => controller.abort(), { once: true })
  }
  const init = { signal: controller.signal, redirect: 'error', headers: { accept: 'application/json', 'user-agent': 'lite-gateway-probe/0.2' } }
  if (addr !== 'direct') init.dispatcher = dispatcherFor(addr)
  try {
    return await undiciFetch(url, init)
  } finally {
    clearTimeout(timer)
  }
}

async function drain(response) {
  try { await response.body?.cancel() } catch { /* already closed */ }
}

/** Concurrent shots; winner takes the verdict. The losers are aborted on
 *  first success — without this, ~half the peak concurrency is already-
 *  meaningless fetches running to timeout, holding sing-box inbounds and
 *  local ephemeral ports (measured under 128-worker rounds). All-fail ->
 *  undefined. Judges THROW on a non-verdict, so Promise.any resolves with
 *  the first *good* value — resolving `undefined` would win the race
 *  instantly and mark every node dead (measured incident). */
async function firstSuccess(urls, addr, judge, timeoutMs) {
  const controllers = urls.map(() => new AbortController())
  const shots = urls.map((url, i) => (async () => judge(await fetchVia(addr, url, timeoutMs, controllers[i].signal), url))())
  try {
    const winner = await Promise.any(shots)
    for (const c of controllers) { try { c.abort() } catch { /* already settled */ } }
    return winner
  } catch {
    return undefined
  }
}

function exitInfoOf(json) {
  const ip = json?.ip ?? json?.query
  const cc = json?.country_code ?? json?.countryCode ?? json?.country
  if (typeof ip !== 'string' || ip === '') return undefined
  return { exitIp: ip, exitCountry: String(cc ?? '').toUpperCase().slice(0, 2) }
}

/**
 * Probe one node exit.
 *
 * Stage 1 runs liveness AND the upstream gate concurrently (first success of
 * either wins): an exit that reaches opencode.ai but not Cloudflare is usable
 * for our purposes — the old serial gate ("Cloudflare first, gate only if
 * liveness passed") misjudged such exits as dead without ever trying the
 * upstream. The gate is an anonymous-200 zero-quota GET, so folding it into
 * stage 1 costs nothing.
 * @param {string} addr - `http://127.0.0.1:<port>` (or 'direct' for the catch-all)
 * @returns {Promise<{state:'alive'|'dead', latencyMs:number, exitIp?:string, exitCountry?:string}>}
 */
export async function probeNode(addr, { timeoutMs = 12000 } = {}) {
  const t0 = Date.now()
  const reachable = await firstSuccess([...LIVENESS_URLS, UPSTREAM_GATE_URL], addr, async r => {
    await drain(r)
    if (r.status !== 204 && r.status !== 200) throw new Error(`reachability HTTP ${r.status}`)
    return Date.now() - t0
  }, timeoutMs)
  if (reachable === undefined) return { state: 'dead', latencyMs: Date.now() - t0 }
  const lat = reachable
  const echo = await firstSuccess(ECHO_URLS, addr, async r => {
    const text = await r.text()
    const info = exitInfoOf(JSON.parse(text))
    if (!info) throw new Error('echo response missing ip')
    return info
  }, 8000)
  return { state: 'alive', latencyMs: lat, ...(echo ?? {}) }
}

/**
 * Bounded-concurrency probe across a node collection.
 * @param {Array<T>} items
 * @param {(item: T) => string} addrOf
 * @param {(item: T, result: object) => void} onResult
 */
export async function probeAll(items, { addrOf, workers = 24, onResult = () => {} }) {
  let cursor = 0
  const run = async () => {
    while (cursor < items.length) {
      const item = items[cursor++]
      const result = await probeNode(addrOf(item)).catch(() => ({ state: 'dead', latencyMs: -1 }))
      try { onResult(item, result) } catch { /* listener errors must not kill the round */ }
    }
  }
  await Promise.all(Array.from({ length: Math.max(1, Math.min(workers, items.length)) }, run))
}
