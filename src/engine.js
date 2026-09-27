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
import { applyLimitsOverlay } from './limits.js'
import { DEFAULT_LEVEL } from './effort.js'
import { UpstreamError, CODE } from './errors.js'
import { pickExit, noteSticky, exitForSession, noteStickyFailure, clearStickyFailures, stickyBurned, noteRegionError, noteRegionOK, unavailableEverywhere, isRestrictedModel, markRestrictedOk } from './health.js'

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

  const defaultPicker = ({ model, sessionId, exclude, restricted }) => {
    const stickyNode = sessionId && !exclude ? exitForSession(sessionId) : null
    const pool = exclude ? poolOf().filter(n => n.tag !== exclude) : poolOf()
    return pickExit({ model, countries: settingsOf().countries ?? [], pool, portOf, stickyNode, restricted })
  }
  const pick = picker ?? defaultPicker

  function modelRows() {
    const created = Math.floor(Date.now() / 1000)
    // 上游 issue #3：在所有已探测的存活出口上都测得地区受限的模型，不再对外列出
    // （面板仍展示并标记，便于观察 region 矩阵恢复）。
    return state().catalog
      .filter(entry => !unavailableEverywhere(entry.id))
      .map(entry => ({ id: entry.id, object: 'model', created, owned_by: 'lite-gateway' }))
  }

  async function complete(request, onChunk) {
    const snapshot = state()
    const openAi = request.openAi ?? {}
    const base = baseModelId(String(request.model ?? ''))
    let entry = snapshot.catalog.find(candidate => candidate.id === base)
    // 未见过的新免费 id 也可直接建目录行（未知模型旁路），overlay 有命中就用 Zen 行数值
    if (entry === undefined && isFreeLane(base)) entry = applyLimitsOverlay(buildCatalog([base]), snapshot.limitsById ?? {})[0]
    if (entry === undefined) throw new UpstreamError(`unknown model "${request.model}"`, CODE.server)

    const sessionId = `forward:${String(openAi.user ?? openAi.conversation ?? 'shared')}`
    const messages = fromOpenAiMessages(openAi, request.responses === true)
    // Harness tool shape ({name, description, parameters}) goes straight to the
    // adapter, which projects it onto whichever wire THIS model speaks. Passing a
    // provider shape here (the old `toToolDefs(...)`) made the adapter's own
    // `toToolDefs` read `tool.name` off a `{function:{name}}` row, find nothing
    // and drop every real tool — the upstream then saw only the fingerprint
    // quartet (or `tool_choice: none`), and no model ever called a tool.
    const tools = (openAi.tools ?? []).map(normalizeTool).filter(Boolean)
    const handler = typeof onChunk === 'function' ? onChunk : () => {}

    let exclude
    let lastFailure
    // sticky 熔断只统计"从 sticky 出口吃到的会前失败"：同会话在同一 sticky 上
    // 连跪 2 次就换出口，而不是 30min 内每请求稳定多一次失败延迟。
    const startedSticky = sessionId ? exitForSession(sessionId) : null
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      const restricted = isRestrictedModel(base) || entry.regionSensitive === true
      const staleSticky = startedSticky && stickyBurned(sessionId) ? startedSticky : null
      if (staleSticky && !exclude) exclude = staleSticky
      const picked = pick({ model: base, sessionId: attempt === 1 ? sessionId : null, exclude, restricted })
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
        // 默认档来自设置（上游语义：harness 缺省 balanced），调用方可用
        // reasoning_effort 逐请求覆盖 —— 这条车道真正生效的旋钮是它导出的预算。
        reasoningEffort: typeof openAi.reasoning_effort === 'string' && openAi.reasoning_effort !== ''
          ? openAi.reasoning_effort
          : (settingsOf().effortLevel ?? DEFAULT_LEVEL),
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
        // 受限模型的真实成功：给该出口打上特殊节点标记（面板 ★）
        if (isRestrictedModel(entry.id) || entry.regionSensitive === true) markRestrictedOk(picked.nodeKey)
      }

      if (failure !== undefined && !sawContent && RETRY_ON.has(code) && attempt < 2) {
        exclude = picked.nodeKey
        lastFailure = failure
        // 失败来自本轮进入时的 sticky 出口 → 记一次熔断分（换出口后清零由成功路径负责）
        if (startedSticky && picked.nodeKey === startedSticky) noteStickyFailure(sessionId)
        continue // same session id, next exit
      }
      if (failure !== undefined && !sawContent) {
        // 第二次尝试也跪：若 sticky 还在且就是它，同样记分（下次请求直接换）
        if (startedSticky && picked.nodeKey === startedSticky) noteStickyFailure(sessionId)
        throw new UpstreamError(failure.message ?? 'upstream error', code ?? CODE.server, {
          status: failure.status,
          providerRetryAfterMs: failure.providerRetryAfterMs,
        })
      }
      // 出内容即清熔断分：sticky 出口恢复正常（非 RETRY_ON 的失败如配额/业务错
      // 不是"出口不行"，换出口也一样，不碰熔断计数）
      if (sawContent || (failure !== undefined && !RETRY_ON.has(code))) {
        if (sessionId) clearStickyFailures(sessionId)
      }

      // A max-tokens finish means the adapter judged a tool call unexecutable
      // (arguments cut mid-JSON); keep the OpenAI answer consistent with its
      // finish_reason by not reporting the broken call alongside `length`.
      if (outcome.truncated === true) {
        outcome.toolCalls = outcome.toolCalls.filter(call => {
          try { JSON.parse(call.arguments === '' ? '{}' : call.arguments); return true } catch { return false }
        })
      }
      // harness usage 形状 -> OpenAI 形状：非流式响应与 /v1/responses 的 usage
      // 都直接透传 outcome.usage，OpenAI 客户端只认 prompt_tokens/completion_tokens。
      if (outcome.usage) {
        const u = outcome.usage
        outcome.usage = {
          prompt_tokens: (u.inputTokens ?? 0) + (u.cacheReadTokens ?? 0),
          completion_tokens: u.outputTokens ?? 0,
          total_tokens: u.totalTokens ?? ((u.inputTokens ?? 0) + (u.cacheReadTokens ?? 0) + (u.outputTokens ?? 0)),
          prompt_tokens_details: { cached_tokens: u.cacheReadTokens ?? 0 },
          completion_tokens_details: { reasoning_tokens: u.reasoningTokens ?? 0 },
        }
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
