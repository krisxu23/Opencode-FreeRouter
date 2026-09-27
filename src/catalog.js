/**
 * Model catalog for the free lane.
 *
 * Three sources, deliberately layered so no single one can break the plugin:
 *
 * 1. the upstream listing itself (`/zen/v1/models`) — the authoritative set of
 *    ids the gateway will currently name;
 * 2. a vetted local capability table (context window / vision / reasoning),
 *    because the upstream listing discloses an id and nothing else;
 * 3. a models.dev limit overlay (src/limits.js) — the same `opencode`
 *    provider rows OpenChamber's Zen page renders, refreshed daily and matched
 *    by exact free-lane id. Applied in index.js/engine.js, not here, so this
 *    module stays offline-safe.
 *
 * @module src/catalog.js
 */

import { baseModelId, isResponsesModel } from './upstream.js'

export const UPSTREAM_MODELS_URL = 'https://opencode.ai/zen/v1/models'

/** Ids that are free-tier without carrying the `-free` suffix. */
const ALWAYS_FREE = new Set(['union-alpha', 'space-bunny-free', 'big-pickle'])

/**
 * Local capability baseline. Values track the `opencode` provider rows in
 * models.dev (`https://models.dev/providers/opencode`, i.e. the same table
 * OpenChamber's Zen page renders as `xx万`), NOT the canonical/OpenRouter
 * rows — the `-free` lane rows carry the lane quota (e.g. mimo-v2.6-flash-free
 * 200000/32000 vs the canonical 1048576/131072). `vision` is what this lane
 * actually accepted under a direct image-input probe, not what a model card
 * claims; `reasoning`/`attachment` follow the models.dev Zen row where known.
 *
 * A network overlay (src/limits.js) refreshes these from models.dev daily;
 * this table is the offline/first-boot fallback, so it must stay close.
 */
export const CAPABILITIES = [
  { match: /^longcat/, vision: true, reasoning: true, contextWindow: 1000000, maxOutput: 131072 },
  { match: /^space.?bunny/, vision: true, reasoning: true, contextWindow: 1048576, maxOutput: 524288 },
  { match: /^mimo.*v2\.6/, vision: true, reasoning: true, contextWindow: 200000, maxOutput: 32000, canDisableThinking: false },
  { match: /^mimo.*v2\.5/, vision: true, reasoning: true, contextWindow: 200000, maxOutput: 32000, canDisableThinking: false },
  { match: /^mimo/, vision: true, reasoning: true, contextWindow: 262144, maxOutput: 65536 },
  { match: /^muse.?spark/, vision: true, reasoning: true, contextWindow: 1048576, maxOutput: 131072 },
  { match: /^nemotron.*3\.5.*lightning/, vision: false, reasoning: true, contextWindow: 262144, maxOutput: 262144 },
  { match: /^nemotron.*ultra/, vision: false, reasoning: true, contextWindow: 1000000, maxOutput: 128000 },
  { match: /^nemotron/, vision: false, reasoning: true, contextWindow: 262144, maxOutput: 128000 },
  { match: /^ling/, vision: false, reasoning: true, contextWindow: 262144, maxOutput: 32768 },
  { match: /^big.?pickle/, vision: false, reasoning: true, contextWindow: 200000, maxOutput: 32000 },
  { match: /^union/, vision: true, reasoning: false, contextWindow: 262144, maxOutput: 131072 },
  { match: /^deepseek/, vision: false, reasoning: true, contextWindow: 200000, maxOutput: 128000 },
  { match: /^kimi/, vision: true, reasoning: true, contextWindow: 262144, maxOutput: 262144 },
  { match: /^qwen/, vision: true, reasoning: true, contextWindow: 262144, maxOutput: 65536 },
  { match: /^glm/, vision: false, reasoning: true, contextWindow: 204800, maxOutput: 131072 },
  { match: /^jev/, vision: false, reasoning: false, contextWindow: 32768, maxOutput: 4096 },
]

/** Human-facing display names, so a raw upstream id never reaches the picker. */
const DISPLAY_NAMES = {
  'mimo-v2.6-flash-free': 'MiMo V2.6 Flash',
  'mimo-v2.5-free': 'MiMo V2.5',
  'mimo-v2-pro-free': 'MiMo V2 Pro',
  'muse-spark-1.3-contributor-free': 'Muse Spark 1.3',
  'muse-spark-1.2-contributor-free': 'Muse Spark 1.2',
  'nemotron-3-ultra-free': 'Nemotron 3 Ultra',
  'nemotron-3.5-lightning-free': 'Nemotron 3.5 Lightning',
  'ling-3.0-flash-fin-free': 'Ling 3.0 Flash Fin',
  'space-bunny-free': 'Space Bunny',
  'union-alpha': 'Union Alpha',
  'deepseek-v4-flash-free': 'DeepSeek V4 Flash',
  'longcat-2.5-preview-free': 'LongCat 2.5 Preview',
  'longcat-2.0-free': 'LongCat 2.0',
  'kimi-k2.5-free': 'Kimi K2.5',
  'qwen3.6-plus-free': 'Qwen 3.6 Plus',
  'glm-5-free': 'GLM 5',
  'jev-1.13-free': 'Jev 1.13',
}

/** Ids whose regional availability is known to be egress-dependent. */
const REGION_SENSITIVE = [/^muse.?spark/]

/**
 * Is this id on the免密 lane? The gateway's listing mixes paid and free ids;
 * only these answer without a per-user key.
 */
export function isFreeLane(modelId) {
  const base = baseModelId(modelId)
  if (ALWAYS_FREE.has(base)) return true
  return /(?:^|[-_])free(?:$|[-_.])/.test(base)
}

/** Look up the baseline capacities for one model id. */
export function capabilitiesFor(modelId) {
  const base = baseModelId(modelId)
  for (const entry of CAPABILITIES) if (entry.match.test(base)) return entry
  return { vision: false, reasoning: true, contextWindow: 131072, maxOutput: 32768 }
}

export function isRegionSensitive(modelId) {
  const base = baseModelId(modelId)
  return REGION_SENSITIVE.some(pattern => pattern.test(base))
}

/** Title-case a bare upstream id into something a picker can show. */
export function displayModelName(modelId) {
  const base = baseModelId(modelId)
  const known = DISPLAY_NAMES[base]
  if (known !== undefined) return known
  const words = base
    .replace(/[-_.]+/g, ' ')
    .replace(/(\d)\s+/g, '$1 ')
    .trim()
    .split(' ')
    .map(word => (/^\d/.test(word) ? word : word.charAt(0).toUpperCase() + word.slice(1)))
    .join(' ')
  return words
}

/**
 * Merge the upstream listing with the local capability table.
 *
 * @param {string[]} ids - raw upstream model ids
 * @returns {Array<object>} catalog entries in listing order
 */
export function buildCatalog(ids) {
  const seen = new Set()
  const entries = []
  for (const raw of ids) {
    const id = String(raw ?? '').trim()
    if (id === '' || !isFreeLane(id)) continue
    const base = baseModelId(id)
    if (seen.has(base)) continue
    seen.add(base)
    const caps = capabilitiesFor(base)
    entries.push({
      id: base,
      name: displayModelName(base),
      wire: isResponsesModel(base) ? 'responses' : 'chat',
      vision: caps.vision === true,
      reasoning: caps.reasoning !== false,
      contextWindow: number(caps.contextWindow) ?? 131072,
      maxOutput: number(caps.maxOutput) ?? 32768,
      canDisableThinking: caps.canDisableThinking !== false,
      regionSensitive: isRegionSensitive(base),
    })
  }
  return entries
}

function number(value) {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? Math.trunc(value) : undefined
}

/** Parse the gateway's `{"data":[{"id":…}]}` listing. */
export function parseListing(payload) {
  const rows = Array.isArray(payload?.data) ? payload.data : Array.isArray(payload?.models) ? payload.models : Array.isArray(payload) ? payload : []
  return rows.map(row => (typeof row === 'string' ? row : row?.id)).filter(id => typeof id === 'string' && id !== '')
}
