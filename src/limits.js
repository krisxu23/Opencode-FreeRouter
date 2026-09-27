/**
 * models.dev limit overlay for the free lane.
 *
 * Chain of custody (verified live):
 *   - `GET /zen/v1/models` discloses an id and nothing else;
 *   - the numbers OpenChamber's Zen page renders (`100万 上下文 · 13.1万 输出`)
 *     come from the `opencode` provider rows in models.dev
 *     (`https://models.dev/providers/opencode`), i.e. the same table the
 *     `@opencode-ai/models` npm client serves as `providers()` from
 *     `GET https://models.dev/api.json`.
 *
 * This module fetches that snapshot (direct, never via a node), keeps only the
 * `opencode` provider's per-model `limit.context` / `limit.output`, caches it
 * to disk with a 24h TTL, and overlays it onto the catalog built from the
 * local CAPABILITIES table. Matching is by exact free-lane id
 * (`mimo-v2.6-flash-free`), never by the canonical row (`mimo-v2.6-flash`,
 * which carries the paid ceiling and is ~5x larger on some models).
 *
 * Fail-soft throughout: a fetch failure returns the stale disk cache, and a
 * missing cache returns an empty overlay so the local table keeps serving.
 *
 * @module src/limits.js
 */

import fs from 'node:fs'
import path from 'node:path'

export const LIMITS_URL = 'https://models.dev/api.json'
export const LIMITS_TTL_MS = 24 * 3600 * 1000

/** Generic fallback capacities in `catalog.js` — the marker of "unknown model". */
const FALLBACK_CONTEXT = 131072
const FALLBACK_OUTPUT = 32768

function positiveInt(value) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? Math.trunc(value) : undefined
}

/**
 * Normalise one models.dev model row into overlay capacities.
 * @returns {{contextWindow:number, maxOutput:number, reasoning?:boolean, vision?:boolean}|null}
 */
export function normalizeRow(row) {
  const contextWindow = positiveInt(row?.limit?.context)
  const maxOutput = positiveInt(row?.limit?.output)
  if (contextWindow === undefined || maxOutput === undefined) return null
  const out = { contextWindow, maxOutput }
  if (typeof row.reasoning === 'boolean') out.reasoning = row.reasoning
  // `attachment:true` = the provider declares image input; only used for
  // models the local table has never seen (see applyLimitsOverlay).
  if (typeof row.attachment === 'boolean') out.vision = row.attachment
  return out
}

/** Extract the `opencode` provider's per-model overlay map from an api.json payload. */
export function extractOpencodeOverlay(payload) {
  const models = payload?.opencode?.models
  if (!models || typeof models !== 'object') return {}
  const byId = {}
  for (const [id, row] of Object.entries(models)) {
    const norm = normalizeRow(row)
    if (norm) byId[id] = norm
  }
  return byId
}

/** Load the disk cache; null when absent or corrupt. */
export function loadLimitsCache(file) {
  try {
    const j = JSON.parse(fs.readFileSync(file, 'utf8'))
    if (!j || typeof j !== 'object' || typeof j.byId !== 'object' || !Number.isFinite(j.fetchedAt)) return null
    return { byId: j.byId, fetchedAt: j.fetchedAt }
  } catch {
    return null
  }
}

/** Atomic temp+rename write; fail-soft. */
export function saveLimitsCache(file, data) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true })
    const tmp = `${file}.${process.pid}.tmp`
    fs.writeFileSync(tmp, JSON.stringify(data))
    fs.renameSync(tmp, file)
  } catch { /* cache must never break a boot */ }
}

/**
 * Refresh the limits overlay: fresh cache wins, otherwise one direct fetch.
 *
 * @param {object} [opts]
 * @param {string} [opts.cacheFile] - disk cache path; no persistence when omitted
 * @param {number} [opts.ttlMs] - freshness window (default 24h)
 * @param {boolean} [opts.force] - skip the freshness check and fetch anyway
 * @param {function} [opts.fetchImpl] - injectable fetch (tests)
 * @param {number} [opts.timeoutMs] - fetch timeout (default 30s)
 * @returns {Promise<{byId:Record<string,object>, fetchedAt:number|null, stale:boolean, error?:string}>}
 */
export async function refreshLimits({ cacheFile, ttlMs = LIMITS_TTL_MS, force = false, fetchImpl = fetch, timeoutMs = 30000 } = {}) {
  const cached = cacheFile ? loadLimitsCache(cacheFile) : null
  const fresh = cached && !force && (Date.now() - cached.fetchedAt < ttlMs)
  if (fresh) return { byId: cached.byId, fetchedAt: cached.fetchedAt, stale: false }

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  timer.unref?.()
  try {
    const response = await fetchImpl(LIMITS_URL, { signal: controller.signal, headers: { accept: 'application/json' } })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const payload = await response.json()
    const byId = extractOpencodeOverlay(payload)
    if (Object.keys(byId).length === 0) throw new Error('empty opencode overlay')
    const data = { fetchedAt: Date.now(), byId, source: 'models.dev' }
    if (cacheFile) saveLimitsCache(cacheFile, data)
    return { byId, fetchedAt: data.fetchedAt, stale: false }
  } catch (error) {
    if (cached) return { byId: cached.byId, fetchedAt: cached.fetchedAt, stale: true, error: String(error?.message ?? error).slice(0, 160) }
    return { byId: {}, fetchedAt: null, stale: false, error: String(error?.message ?? error).slice(0, 160) }
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Overlay models.dev limits onto catalog entries built by `buildCatalog`.
 *
 * - `contextWindow` / `maxOutput` are overwritten whenever the overlay has
 *   the exact id (the Zen `-free` row, not the canonical row).
 * - `reasoning` follows the overlay when it declares a boolean.
 * - `vision` is probe-verified locally, so it is left alone — except for
 *   models the local table has never seen (generic 131072/32768 fallback),
 *   where the overlay's `attachment` bit is better than the default `false`.
 *
 * @param {Array<object>} entries - catalog rows from `buildCatalog`
 * @param {Record<string, object>} byId - overlay map from `refreshLimits`
 * @returns {Array<object>} new rows (input untouched)
 */
export function applyLimitsOverlay(entries, byId) {
  if (!byId || typeof byId !== 'object') return entries
  return (entries ?? []).map(entry => {
    const over = byId[entry?.id]
    if (!over) return entry
    const next = { ...entry }
    if (Number.isFinite(over.contextWindow) && over.contextWindow > 0) next.contextWindow = Math.trunc(over.contextWindow)
    if (Number.isFinite(over.maxOutput) && over.maxOutput > 0) next.maxOutput = Math.trunc(over.maxOutput)
    if (typeof over.reasoning === 'boolean') next.reasoning = over.reasoning
    const isFallback = entry.contextWindow === FALLBACK_CONTEXT && entry.maxOutput === FALLBACK_OUTPUT && entry.vision === false
    if (isFallback && typeof over.vision === 'boolean') next.vision = over.vision
    return next
  })
}
