/**
 * Upstream-facing probes (ported from upstream src/probe.js): the smallest
 * request that can produce a verdict, now with an explicit exit address so a
 * probe can be pointed at any single node.
 *
 * @module src/probe.js
 */

import { applyFingerprint, endpointFor, mintRequestId, sessionForConversation, wireFor } from './upstream.js'
import { CODE, getJson, postStreamed, dispatcherFor } from './http.js'
import { fetch as undiciFetch } from 'undici'

/** Public-echo sources, tried in order; any one answering is enough. */
const ECHO_SOURCES = [
  { url: 'https://api.ipify.org?format=json', pick: payload => payload?.ip },
  { url: 'https://ipinfo.io/json', pick: payload => payload?.ip, extra: payload => payload?.country },
  { url: 'https://ipapi.co/json/', pick: payload => payload?.ip, extra: payload => payload?.country_code },
]

/** Verdicts the probe can return. */
export const STATE = {
  available: 'available',
  regionBlocked: 'region-blocked',
  unavailable: 'unavailable',
  throttled: 'throttled',
  unknown: 'unknown',
}

const PING_PROMPT = 'ping'

/**
 * Ask the gateway for one model, once, through `exitAddr`.
 *
 * Real request: each probe burns a sliver of that exit's free quota, so callers
 * cap the fan-out (the region matrix probes at most 24 nodes per round).
 *
 * @returns {Promise<{state:string, detail?:string, latencyMs:number, ttftMs?:number}>}
 */
export async function probeModel(model, { exitAddr, attributionUserAgent, signal, timeoutMs = 45000 } = {}) {
  const started = Date.now()
  const session = sessionForConversation('probe:our-free-model')
  const wire = wireFor(model.id)
  const body = buildPing(model.id, wire)
  applyFingerprint(body, wire === 'responses')

  let firstDelta
  try {
    await postStreamed({
      path: endpointFor(model.id),
      body,
      session,
      requestId: mintRequestId(),
      exitAddr,
      attributionUserAgent,
      signal,
      timeoutMs,
      onData: payload => {
        if (firstDelta !== undefined) return
        if (/"(delta|content|text|output_item)"|response\.(output_item|output_text|function_call)/.test(payload)) firstDelta = Date.now()
      },
    })
    return { state: STATE.available, latencyMs: Date.now() - started, ttftMs: firstDelta === undefined ? undefined : firstDelta - started }
  } catch (error) {
    return {
      state: stateOf(error),
      detail: typeof error?.message === 'string' ? error.message.slice(0, 200) : String(error),
      latencyMs: Date.now() - started,
    }
  }
}

function buildPing(modelId, wire) {
  if (wire === 'responses') {
    return {
      model: modelId,
      input: [{ type: 'message', role: 'user', content: [{ type: 'input_text', text: PING_PROMPT }] }],
      stream: true,
      store: false,
      max_output_tokens: 16,
    }
  }
  if (wire === 'messages') {
    return {
      model: modelId,
      messages: [{ role: 'user', content: PING_PROMPT }],
      stream: true,
      max_tokens: 16,
    }
  }
  return { model: modelId, messages: [{ role: 'user', content: PING_PROMPT }], stream: true, max_tokens: 16 }
}

const ROUTING_REFUSAL_STATUS = new Set([400, 404, 422])

function stateOf(error) {
  switch (error?.code) {
    case CODE.region: return STATE.regionBlocked
    case CODE.quota: return STATE.throttled
    default: break
  }
  const message = String(error?.message ?? '')
  // 上游 1.2.2：503 的 reason phrase 是网关侧故障不是模型判决；400/404/422 才是
  // 路由层拒绝。误读会把活模型判死到下一轮探测。
  const gatewayTrouble = Number.isInteger(error?.status) && error.status >= 500
  const named = error?.unavailable === true
    || (!gatewayTrouble && /unavailable|not supported|no such model|unknown model|invalid model/i.test(message))
  if (named) return STATE.unavailable
  if (Number.isInteger(error?.status) && ROUTING_REFUSAL_STATUS.has(error.status)) return STATE.unavailable
  return STATE.unknown
}

/**
 * Resolve the public address one exit presents to the world. Fail-open: an
 * absent answer simply means "no egress signal".
 */
export async function detectEgress({ exitAddr, signal, timeoutMs = 8000 } = {}) {
  for (const source of ECHO_SOURCES) {
    try {
      const controller = new AbortController()
      const timer = setTimeout(() => controller.abort(), timeoutMs)
      timer.unref?.()
      signal?.addEventListener('abort', () => controller.abort(), { once: true })
      const response = await undiciFetch(source.url, { signal: controller.signal, redirect: 'error', headers: { accept: 'application/json' }, dispatcher: dispatcherFor(exitAddr) })
      clearTimeout(timer)
      if (!response.ok) continue
      const payload = await response.json()
      const ip = source.pick(payload)
      if (typeof ip !== 'string' || ip === '') continue
      const country = source.extra?.(payload)
      return { ip, ...country === undefined ? {} : { country: String(country) } }
    } catch {
      // try the next echo
    }
  }
  return undefined
}

/**
 * Refresh the model listing from the gateway.
 * @param {object} [opts]
 * @param {number} [opts.timeoutMs] - GET timeout (default 15000)
 * @returns {Promise<string[]>} ids, in upstream order
 */
export async function fetchUpstreamIds({ exitAddr, attributionUserAgent, signal, timeoutMs } = {}) {
  const payload = await getJson('/zen/v1/models', { exitAddr, session: sessionForConversation('catalog:our-free-model'), requestId: mintRequestId(), attributionUserAgent, signal, ...(timeoutMs === undefined ? {} : { timeoutMs }) })
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.models) ? payload.models : []
  return rows.map(row => (typeof row === 'string' ? row : row?.id)).filter(id => typeof id === 'string' && id !== '')
}
