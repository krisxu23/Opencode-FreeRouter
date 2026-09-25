/**
 * In-memory health state with atomic persistence:
 *
 * - Node health: `alive | dead | unknown`. Unknown (never probed) is usable —
 *   unprobed must not mean unavailable. 429/RATE_LIMIT never enters this table:
 *   quota is accounted per session upstream, so it is a property of the
 *   conversation, not of the node; switching nodes does not reset it.
 * - Region matrix: `regionModels` (models observed to be region-gated, auto-
 *   discovered from real traffic) × `regionNodeOK` (model -> node -> ok).
 *   Real-traffic success is the authoritative `true`; RegionError the
 *   authoritative `false`; active probing is only a supplement and burns real
 *   quota, so callers cap it (24 nodes/round).
 * - Sticky sessions: conversation -> node, TTL 30min. Quota follows the
 *   session, so retries keep the same session id and only leave the sticky
 *   exit on region/transport failure.
 *
 * @module src/health.js
 */

import fs from 'node:fs'

const STICKY_TTL_MS = 30 * 60 * 1000

const nodes = new Map()          // nodeKey -> {state, latencyMs, exitIp, exitCountry, lastProbeAt}
const regionModels = new Set()   // modelId
const regionNodeOK = new Map()   // modelId -> Map<nodeKey, boolean>
const sticky = new Map()         // session -> {nodeKey, at}

let healthFile = 'data/node-health.json'

export function setHealthFile(file) {
  healthFile = file
  loadHealth()
}

// ---- node health -----------------------------------------------------------

export function markProbe(nodeKey, result) {
  const alive = result.state === 'alive'
  nodes.set(nodeKey, {
    state: alive ? 'alive' : 'dead',
    // Latency is only meaningful for a working exit: a node that connects but
    // cannot reach the upstream is dead for our purposes, and showing its
    // liveness latency there reads as a contradiction in the panel.
    latencyMs: alive ? (result.latencyMs ?? -1) : -1,
    exitIp: result.exitIp ?? '',
    exitCountry: String(result.exitCountry ?? '').toUpperCase().slice(0, 2),
    lastProbeAt: Date.now(),
  })
}

export function healthOf(nodeKey) {
  return nodes.get(nodeKey)?.state ?? 'unknown'
}

/** Unknown is usable; only a measured `dead` excludes a node. */
export function nodeUsable(nodeKey) {
  return healthOf(nodeKey) !== 'dead'
}

export function nodeSnapshot() {
  return Object.fromEntries(nodes)
}

// ---- persistence ------------------------------------------------------------

export function loadHealth() {
  try {
    const j = JSON.parse(fs.readFileSync(healthFile, 'utf8'))
    for (const [k, v] of Object.entries(j.nodes ?? {})) {
      if (v && typeof v === 'object' && (v.state === 'alive' || v.state === 'dead')) nodes.set(k, v)
    }
    for (const m of j.regionModels ?? []) if (typeof m === 'string') regionModels.add(m)
    for (const [model, map] of Object.entries(j.regionNodeOK ?? {})) {
      const inner = regionNodeOK.get(model) ?? new Map()
      for (const [node, ok] of Object.entries(map ?? {})) inner.set(node, ok === true)
      regionNodeOK.set(model, inner)
    }
  } catch { /* first boot or corrupt file: start empty */ }
}

export function persistHealth() {
  const payload = {
    nodes: Object.fromEntries(nodes),
    regionModels: [...regionModels],
    regionNodeOK: Object.fromEntries([...regionNodeOK].map(([m, inner]) => [m, Object.fromEntries(inner)])),
  }
  fs.mkdirSync(healthFile.replace(/[/\\][^/\\]+$/, ''), { recursive: true })
  const tmp = `${healthFile}.${process.pid}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(payload))
  fs.renameSync(tmp, healthFile)
}

/** Drop entries for nodes that left the pool (subscription churn). */
export function pruneStale(activeKeys) {
  const on = new Set(activeKeys)
  for (const k of [...nodes.keys()]) if (!on.has(k)) nodes.delete(k)
  for (const inner of regionNodeOK.values()) for (const k of [...inner.keys()]) if (!on.has(k)) inner.delete(k)
}

// ---- region matrix -----------------------------------------------------------

export function noteRegionError(model, nodeKey) {
  if (!model || !nodeKey) return
  regionModels.add(model)
  const inner = regionNodeOK.get(model) ?? new Map()
  inner.set(nodeKey, false)
  regionNodeOK.set(model, inner)
}

export function noteRegionOK(model, nodeKey) {
  if (!model || !nodeKey) return
  const inner = regionNodeOK.get(model) ?? new Map()
  inner.set(nodeKey, true)
  regionNodeOK.set(model, inner)
}

/** @returns {{usable:boolean, known:boolean}} */
export function regionUsable(model, nodeKey) {
  const inner = regionNodeOK.get(model)
  const v = inner?.get(nodeKey)
  if (v === undefined) return { usable: true, known: false }
  return { usable: v, known: true }
}

export function regionSnapshot() {
  return { models: [...regionModels], matrix: Object.fromEntries([...regionNodeOK].map(([m, inner]) => [m, Object.fromEntries(inner)])) }
}

/** Usable nodes whose region verdict for `model` is still unknown — probe targets. */
export function regionProbeCandidates(model, { max = 24 } = {}) {
  const inner = regionNodeOK.get(model) ?? new Map()
  const out = []
  for (const [nodeKey, n] of nodes) {
    if (n.state !== 'alive') continue
    if (inner.has(nodeKey)) continue
    out.push(nodeKey)
    if (out.length >= max) break
  }
  return out
}

// ---- sticky sessions ----------------------------------------------------------

export function noteSticky(session, nodeKey) {
  if (!session || !nodeKey) return
  sticky.set(session, { nodeKey, at: Date.now() })
}

export function exitForSession(session) {
  const hit = sticky.get(session)
  if (!hit) return null
  if (Date.now() - hit.at > STICKY_TTL_MS || !nodeUsable(hit.nodeKey)) {
    sticky.delete(session)
    return null
  }
  return hit.nodeKey
}

// ---- exit picking ---------------------------------------------------------------

/**
 * Pick the next exit for one request.
 *
 * @param {object} args
 * @param {string} args.model - base model id
 * @param {string[]} args.countries - user-selected countries, in fallback order
 * @param {Array<{tag:string, country?:string}>} args.pool - current node pool
 * @param {(tag: string) => number} args.portOf - node tag -> local inbound port
 * @param {string} [args.stickyNode] - node pinned by the session, tried first
 * @returns {{nodeKey:string, addr:string, country:string} | null}
 */
export function pickExit({ model, countries, pool, portOf, stickyNode }) {
  const effectiveCountry = node => {
    const measured = nodes.get(node.tag)?.exitCountry
    return (measured && measured.length === 2 ? measured : '') || String(node.country ?? '').toUpperCase().slice(0, 2)
  }
  const rank = node => {
    const h = nodes.get(node.tag)
    const usable = nodeUsable(node.tag)
    const region = regionUsable(model, node.tag)
    if (!usable || !region.usable) return null
    return { node, bucket: h?.state === 'alive' ? 0 : 1, latency: h?.latencyMs ?? Number.MAX_SAFE_INTEGER, country: effectiveCountry(node) }
  }
  const consider = candidate => {
    if (!candidate) return null
    if (portOf(candidate.node.tag) == null) return null
    return { nodeKey: candidate.node.tag, addr: `http://127.0.0.1:${portOf(candidate.node.tag)}`, country: candidate.country }
  }

  if (stickyNode) {
    const node = pool.find(n => n.tag === stickyNode)
    if (node) {
      const hit = consider(rank(node))
      if (hit) return hit
    }
  }

  const want = new Set((countries ?? []).map(c => String(c).toUpperCase()))
  const byCountry = new Map()
  for (const node of pool) {
    const r = rank(node)
    if (!r) continue
    if (!want.has(r.country)) continue
    const list = byCountry.get(r.country) ?? []
    list.push(r)
    byCountry.set(r.country, list)
  }
  for (const country of want) {
    const list = (byCountry.get(country) ?? []).sort((a, b) => a.bucket - b.bucket || a.latency - b.latency)
    const hit = consider(list[0])
    if (hit) return hit
  }
  // Selected countries all exhausted: still prefer any usable pool node over
  // failing outright — a wrong-but-working country beats no answer. Direct
  // dialing is never a candidate here.
  const any = pool.map(rank).filter(Boolean).sort((a, b) => a.bucket - b.bucket || a.latency - b.latency)
  return consider(any[0])
}
