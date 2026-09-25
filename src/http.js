/**
 * Outbound HTTP for the free lane: request posting, SSE line extraction, with
 * every call routed through a caller-chosen exit address.
 *
 * The one deliberate departure from upstream: `postStreamed`/`getJson` take an
 * `exitAddr` (`http://127.0.0.1:<port>` of a per-node sing-box inbound, or
 * `'direct'`) and dial through it via an undici ProxyAgent. Upstream traffic
 * never leaves through the OS default route unless the caller explicitly asks
 * for `'direct'`.
 *
 * @module src/http.js
 */

import { ProxyAgent, fetch as undiciFetch } from 'undici'
import { CLIENT_UA, UPSTREAM_BASE, gatewayHeaders, truncateSession } from './upstream.js'
import { CODE, UpstreamError, classifyFailure, retryAfter } from './errors.js'

export { CODE, UpstreamError, classifyFailure }

/**
 * All fetches go through undici's own fetch, not the global one: the global
 * fetch is Node's internal undici, whose dispatcher protocol rejects an
 * npm-undici ProxyAgent ("invalid onRequestStart method" — measured).
 */

/**
 * Proxy dispatchers, one per exit address, cached for the process lifetime.
 * `'direct'`/absent maps to `undefined` — plain fetch, no dispatcher.
 */
const dispatchers = new Map()
export function dispatcherFor(addr) {
  if (!addr || addr === 'direct') return undefined
  let agent = dispatchers.get(addr)
  if (agent === undefined) {
    agent = new ProxyAgent(addr)
    dispatchers.set(addr, agent)
  }
  return agent
}

/**
 * Compose the request User-Agent.
 *
 * Two independent requirements meet in one header: the harness mandates an
 * attribution User-Agent on every provider request, and the gateway identifies a
 * desktop client by an `opencode/<version>` token (>= 1.17). The gateway tests
 * with a search rather than an anchored match, so one value can satisfy both —
 * verified live against this lane.
 */
function userAgentWith(attribution) {
  if (typeof attribution !== 'string' || attribution === '') return CLIENT_UA
  return attribution.includes('opencode/') ? attribution : `${attribution} ${CLIENT_UA}`
}

/**
 * POST one request and stream back decoded SSE `data:` payloads.
 *
 * @param {object} options
 * @param {string} options.path - gateway path
 * @param {object} options.body - JSON request body
 * @param {string} options.session - canonical upstream session id
 * @param {string} options.requestId - per-turn request id
 * @param {string} [options.exitAddr] - local proxy exit (`http://127.0.0.1:<port>` | 'direct')
 * @param {string} [options.attributionUserAgent] - harness User-Agent merged into the request
 * @param {AbortSignal} [options.signal]
 * @param {(payload: string) => void} options.onData - one `data:` payload, in order
 * @returns {Promise<{status:number, headers:Headers}>}
 */
export async function postStreamed({ path, body, session, requestId, exitAddr, attributionUserAgent, signal, onData, timeoutMs = 300000 }) {
  const headers = gatewayHeaders({ session: truncateSession(session), requestId, stream: true })
  headers['user-agent'] = userAgentWith(attributionUserAgent)
  let response
  try {
    response = await undiciFetch(`${UPSTREAM_BASE}${path}`, { method: 'POST', headers, body: JSON.stringify(body), redirect: 'error', signal, dispatcher: dispatcherFor(exitAddr) })
  } catch (error) {
    if (error?.name === 'AbortError') throw new UpstreamError('request aborted', CODE.aborted)
    throw new UpstreamError(`our-free-model: upstream request failed: ${error?.message ?? error}`, CODE.transport)
  }

  const setRetry = retryAfter(response.headers.get('retry-after'))
  const contentType = String(response.headers.get('content-type') ?? '')
  if (!response.ok) {
    const text = await response.text().catch(() => '')
    let payload
    try { payload = JSON.parse(text) } catch { payload = { error: { message: text.slice(0, 300) || `HTTP ${response.status}` } } }
    throw classifyFailure(response.status, payload, setRetry)
  }
  if (response.body === null) throw new UpstreamError('our-free-model: upstream returned no body', CODE.empty)

  // 上游 issue #6：嗅探首块判形状（≥8 字节足够覆盖 `retry:` 前缀），
  // 不信任 Content-Type，也永不 response.text()——那会把流式响应整条吃掉。
  const reader = response.body.getReader()
  const head = []
  let headBytes = 0
  while (headBytes < 8) {
    const { value, done } = await reader.read()
    if (done) break
    head.push(value)
    headBytes += value.byteLength
  }
  const kind = classifyHead(head.length ? Buffer.concat(head) : new Uint8Array(0), contentType)
  if (kind === 'empty') throw new UpstreamError('our-free-model: upstream returned no body', CODE.empty)

  if (kind === 'json') {
    const rest = []
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      rest.push(value)
    }
    const text = Buffer.concat([...head, ...rest]).toString('utf8')
    let payload
    try { payload = JSON.parse(text) } catch { throw new UpstreamError(`our-free-model: unexpected non-SSE response: ${text.slice(0, 200)}`, CODE.server) }
    if (payload.error) throw classifyFailure(response.status, payload, setRetry)
    onData(JSON.stringify(payload))
    return { status: response.status, headers: response.headers }
  }

  await readSse(replayHead(head, reader), onData, signal, timeoutMs)
  return { status: response.status, headers: response.headers }
}

/** Split an SSE byte stream into `data:` payload strings; comment lines ignored.
 * Accepts any async iterable of Uint8Array chunks (a sniffed head can be
 * replayed ahead of the live stream); `chunks.cancel?.()` on abort. */
export async function readSse(chunks, onData, signal, timeoutMs = 300000) {
  const decoder = new TextDecoder()
  let buffer = ''
  let deadline = Date.now() + timeoutMs
  signal?.addEventListener('abort', () => { try { void chunks.cancel?.() } catch { /* not cancellable */ } }, { once: true })
  try {
    for await (const value of chunks) {
      if (Date.now() > deadline) throw new UpstreamError('our-free-model: upstream stream idle past its deadline', CODE.timeout)
      if (value !== undefined) buffer += decoder.decode(value, { stream: true })
      let newline = buffer.indexOf('\n')
      while (newline !== -1) {
        const line = buffer.slice(0, newline)
        buffer = buffer.slice(newline + 1)
        emit(line, onData)
        newline = buffer.indexOf('\n')
      }
      deadline = Date.now() + timeoutMs
    }
    buffer += decoder.decode()
    emit(buffer, onData)
  } catch (error) {
    if (error instanceof UpstreamError) throw error
    if (signal?.aborted) throw new UpstreamError('request aborted', CODE.aborted)
    throw new UpstreamError(`our-free-model: stream read failed: ${error?.message ?? error}`, CODE.transport)
  }
}

function emit(line, onData) {
  const text = line.trim()
  if (text === '' || text.startsWith(':')) return
  if (text.startsWith('data:')) {
    const payload = text.slice(5).trim()
    if (payload === '' || payload === '[DONE]') return
    onData(payload)
  }
}

/** 上游 issue #6：高负载下网关会返回非 event-stream 的 Content-Type 但 SSE 形状的
 *  响应体——仅凭 Content-Type 分支会把整条流当 JSON 误杀。按首块形状分类：
 *  SSE 帧（data:/event:/id:/retry:/: 注释开头）走流式；`{`/`[` 走单 JSON；空为
 *  EMPTY_RESPONSE。形状不明时回退 Content-Type 判断。永不 response.text()。 */
function classifyHead(bytes, contentType) {
  if (bytes.length === 0) return 'empty'
  const head = Buffer.from(bytes).toString('latin1').slice(0, 16)
  if (/^\s*(data:|event:|id:|retry:|:)/.test(head)) return 'sse'
  if (/^\s*[[{]/.test(head)) return 'json'
  return contentType.includes('event-stream') ? 'sse' : 'json'
}

async function* replayHead(head, reader) {
  for (const chunk of head) yield chunk
  while (true) {
    const { value, done } = await reader.read()
    if (done) return
    if (value !== undefined) yield value
  }
}

/** Fetch a small JSON document from the gateway with the fingerprint headers. */
export async function getJson(path, { session, requestId, exitAddr, attributionUserAgent, signal, timeoutMs = 15000 } = {}) {
  const headers = gatewayHeaders({ session: truncateSession(session ?? ''), requestId: requestId ?? '', stream: false, accept: 'application/json' })
  headers['user-agent'] = userAgentWith(attributionUserAgent)
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  timer.unref?.()
  signal?.addEventListener('abort', () => controller.abort(), { once: true })
  try {
    const response = await undiciFetch(`${UPSTREAM_BASE}${path}`, { headers, redirect: 'error', signal: controller.signal, dispatcher: dispatcherFor(exitAddr) })
    const text = await response.text()
    let payload
    try { payload = JSON.parse(text) } catch { payload = { error: { message: text.slice(0, 200) } } }
    if (!response.ok) throw classifyFailure(response.status, payload)
    return payload
  } catch (error) {
    if (error instanceof UpstreamError) throw error
    if (error?.name === 'AbortError') throw new UpstreamError('our-free-model: upstream GET timed out', CODE.timeout)
    throw new UpstreamError(`our-free-model: upstream GET failed: ${error?.message ?? error}`, CODE.transport)
  } finally {
    clearTimeout(timer)
  }
}
