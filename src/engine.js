/**
 * Engine assembly: wires the ported adapter chain (adapter/messages/stream/
 * effort/channel) into the `complete(request, onChunk)` contract the forward
 * listener consumes, and owns exit selection.
 *
 * Exit policy per request:
 *   1. sticky node for the session (TTL 30min) when still usable;
 *   2. otherwise health.pickExit over the user's ordered countries, skipping
 *      nodes the region matrix has measured as blocked for this model;
 *   3. on a pre-content REGION/TRANSPORT/TIMEOUT/EMPTY failure, retry ONCE on
 *      the next candidate — same session id (quota follows the session; a
 *      fresh session would only make the 429 worse), different exit.
 *   4. real-traffic success is the authoritative region-matrix `true`;
 *      a RegionError the authoritative `false`.
 *
 * The OpenAI-to-harness message translation below is ported verbatim from the
 * upstream plugin's index.js (runForwarded glue).
 *
 * @module src/engine.js
 */

import { FreeModelAdapter, ROUTE_MAIN } from './adapter.js'
import { baseModelId } from './upstream.js'
import { buildCatalog, isFreeLane } from './catalog.js'
import { toToolDefs } from './messages.js'
import { UpstreamError, CODE } from './errors.js'
import { pickExit, noteSticky, exitForSession, noteRegionError, noteRegionOK } from './health.js'

const RETRY_ON = new Set([CODE.region, CODE.transport, CODE.timeout, CODE.empty])

/**
 * @param {object} deps
 * @param {() => {catalog: Array<object>, membership: Record<string, string[]>, settings: object, attributionUserAgent: string}} deps.state
 * @param {(record: object) => void} deps.recordUsage
 * @param {() => object} deps.settingsOf - {countries, ...}
 * @param {() => Array<{tag: string, country?: string}>} deps.poolOf - current node pool
 * @param {(tag: string) => number|undefined} deps.portOf - node tag -> local inbound port
 * @param {({model, sessionId, exclude}) => {nodeKey, addr, country} | null} [deps.picker] - injectable for tests
 */
export function createEngine({ state, recordUsage, settingsOf, poolOf, portOf, picker }) {
  const adapter = new FreeModelAdapter({ state, recordUsage, warn: () => {} })

  const defaultPicker = ({ model, sessionId, exclude }) => {
    const stickyNode = sessionId && !exclude ? exitForSession(sessionId) : null
    const pool = exclude ? poolOf().filter(n => n.tag !== exclude) : poolOf()
    return pickExit({ model, countries: settingsOf().countries ?? [], pool, portOf, stickyNode })
  }
  const pick = picker ?? defaultPicker

  function modelRows() {
    const created = Math.floor(Date.now() / 1000)
    return state().catalog.map(entry => ({ id: entry.id, object: 'model', created, owned_by: 'lite-gateway' }))
  }

  async function complete(request, onChunk) {
    const snapshot = state()
    const openAi = request.openAi ?? {}
    const base = baseModelId(String(request.model ?? ''))
    let entry = snapshot.catalog.find(candidate => candidate.id === base)
    if (entry === undefined && isFreeLane(base)) entry = buildCatalog([base])[0]
    if (entry === undefined) throw new UpstreamError(`unknown model "${request.model}"`, CODE.server)

    const sessionId = `forward:${String(openAi.user ?? openAi.conversation ?? 'shared')}`
    const messages = fromOpenAiMessages(openAi, request.responses === true)
    const tools = toToolDefs((openAi.tools ?? []).map(normalizeTool).filter(Boolean), request.responses === true ? 'flat' : 'chat')
    const handler = typeof onChunk === 'function' ? onChunk : () => {}

    let exclude
    let lastFailure
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      const picked = pick({ model: base, sessionId: attempt === 1 ? sessionId : null, exclude })
      if (!picked) {
        throw new UpstreamError(
          lastFailure ? `no other healthy exit (last: ${lastFailure.message})` : 'no healthy exit for the selected countries',
          lastFailure?.code ?? CODE.server,
          { statusCode: 503 },
        )
      }
      noteSticky(sessionId, picked.nodeKey)

      const options = {
        provider: ROUTE_MAIN,
        model: entry.id,
        messages,
        tools: tools.length > 0 ? tools : undefined,
        ...typeof openAi.temperature === 'number' ? { temperature: openAi.temperature } : {},
        ...typeof openAi.max_tokens === 'number' ? { maxTokens: openAi.max_tokens } : {},
        ...typeof openAi.reasoning_effort === 'string' ? { reasoningEffort: openAi.reasoning_effort } : {},
        sessionId,
        exitAddr: picked.addr,
      }

      const outcome = { text: '', toolCalls: [], usage: undefined, truncated: false, error: undefined }
      let sawContent = false
      let finishKind
      let failure
      try {
        for await (const chunk of adapter.stream(options, entry, snapshot)) {
          handler(chunk)
          if (chunk.type === 'text-delta' || chunk.type === 'reasoning-delta' || chunk.type === 'tool-call-delta') sawContent = true
          if (chunk.type === 'finish') {
            finishKind = chunk.reason?.kind
            if (finishKind === 'error') failure = chunk.reason?.failure ?? {}
          }
          foldForwardOutcome(outcome, chunk)
        }
      } catch (error) {
        // readStream throws on in-band `{type:'error'}` payloads; treat like a finish error.
        failure = { message: String(error?.message ?? error), code: error?.llmCode ?? error?.code ?? CODE.server, status: error?.status }
      }

      const code = failure?.code
      if (code === CODE.region) noteRegionError(entry.id, picked.nodeKey)
      if (failure === undefined && (finishKind === 'stop' || finishKind === 'tool-calls' || finishKind === 'max-tokens')) {
        noteRegionOK(entry.id, picked.nodeKey)
      }

      if (failure !== undefined && !sawContent && RETRY_ON.has(code) && attempt < 2) {
        exclude = picked.nodeKey
        lastFailure = failure
        continue // same session id, next exit
      }
      if (failure !== undefined && !sawContent) {
        throw new UpstreamError(failure.message ?? 'upstream error', code ?? CODE.server, {
          status: failure.status,
          providerRetryAfterMs: failure.providerRetryAfterMs,
        })
      }

      // A max-tokens finish means the adapter judged a tool call unexecutable
      // (arguments cut mid-JSON); keep the OpenAI answer consistent with its
      // finish_reason by not reporting the broken call alongside `length`.
      if (outcome.truncated === true) {
        outcome.toolCalls = outcome.toolCalls.filter(call => {
          try { JSON.parse(call.arguments === '' ? '{}' : call.arguments); return true } catch { return false }
        })
      }
      return outcome
    }
    throw new UpstreamError('unreachable retry state', CODE.server)
  }

  return { complete, modelRows }
}

// ---- OpenAI -> harness translation (ported verbatim from upstream index.js) ----

/** OpenAI request messages -> harness messages, for the forward listener. */
export function fromOpenAiMessages(body, isResponses) {
  const out = []
  const rows = isResponses
    ? normaliseResponsesInput(body.input)
    : (Array.isArray(body.messages) ? body.messages : [])
  for (const row of rows) {
    const role = row.role ?? 'user'
    const content = []
    if (typeof row.content === 'string') {
      if (row.content !== '') content.push({ type: 'text', text: row.content })
    } else if (Array.isArray(row.content)) {
      for (const part of row.content) {
        if (typeof part === 'string') { if (part !== '') content.push({ type: 'text', text: part }); continue }
        const text = part?.text ?? part?.input_text ?? part?.output_text
        if (typeof text === 'string' && text !== '') content.push({ type: 'text', text })
        const image = part?.image_url?.url ?? part?.image_url
        if (typeof image === 'string' && image !== '') {
          content.push({ type: 'image', attachment: { attachmentId: `url:${image.slice(0, 64)}`, mediaType: 'image/png', bytes: 0, width: 0, height: 0, url: image } })
        }
      }
    }
    if (role === 'tool') {
      out.push({ role: 'tool', content: [{ type: 'text', text: typeof row.content === 'string' ? row.content : JSON.stringify(row.content ?? '') }], toolCallId: row.tool_call_id ?? '', source: { kind: 'tool', callId: row.tool_call_id ?? '' } })
      continue
    }
    if (role === 'assistant' && Array.isArray(row.tool_calls)) {
      for (const call of row.tool_calls) {
        content.push({ type: 'tool-call', id: call.id ?? '', name: call.function?.name ?? '', arguments: call.function?.arguments ?? '{}' })
      }
    }
    if (content.length === 0) continue
    out.push({
      role: role === 'developer' ? 'developer' : role === 'system' ? 'system' : role === 'assistant' ? 'assistant' : 'user',
      content,
      ...role === 'assistant' ? { source: { kind: 'model' } } : {},
    })
  }
  return out
}

function normaliseResponsesInput(input) {
  if (typeof input === 'string') return [{ role: 'user', content: input }]
  if (!Array.isArray(input)) return []
  return input.map(row => {
    if (typeof row === 'string') return { role: 'user', content: row }
    if (row.type === 'function_call') return { role: 'assistant', content: [], tool_calls: [{ id: row.call_id, function: { name: row.name, arguments: row.arguments } }] }
    if (row.type === 'function_call_output') return { role: 'tool', content: String(row.output ?? ''), tool_call_id: row.call_id }
    return row
  })
}

export function normalizeTool(tool) {
  const name = tool?.name ?? tool?.function?.name
  if (typeof name !== 'string' || name.trim() === '') return null
  const parameters = tool?.parameters ?? tool?.function?.parameters ?? { type: 'object', properties: {} }
  return { name, description: String(tool?.description ?? tool?.function?.description ?? ''), parameters }
}

function foldForwardOutcome(outcome, chunk) {
  switch (chunk.type) {
    case 'text-delta': outcome.text += chunk.text; break
    case 'tool-call-delta': {
      let call = outcome.toolCalls.find(candidate => candidate.slot === chunk.index)
      if (call === undefined) { call = { slot: chunk.index, id: chunk.id ?? '', name: chunk.name ?? '', arguments: chunk.argumentsDelta ?? '' }; outcome.toolCalls.push(call) }
      else call.arguments += chunk.argumentsDelta ?? ''
      if (chunk.name) call.name = chunk.name
      if (chunk.id) call.id = chunk.id
      break
    }
    case 'block-end':
      if (chunk.block?.type === 'tool-call') {
        const existing = outcome.toolCalls.find(candidate => candidate.id === chunk.block.id)
        if (existing === undefined) outcome.toolCalls.push({ slot: chunk.index, id: chunk.block.id, name: chunk.block.name, arguments: chunk.block.arguments })
      }
      break
    case 'usage': outcome.usage = chunk.usage; break
    case 'finish':
      if (chunk.reason?.kind === 'max-tokens') outcome.truncated = true
      if (chunk.reason?.kind === 'error') outcome.error = chunk.reason.failure?.message
      break
    default: break
  }
  return outcome
}
