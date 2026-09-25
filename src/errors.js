/**
 * Harness-neutral failure codes and the gateway JSON error envelope classifier.
 * Extracted verbatim from upstream src/http.js so both http.js and the engine
 * share one taxonomy.
 *
 * - `RegionError`  — the model exists but this egress country is excluded. It
 *   clears the moment the egress changes, so it feeds the region matrix rather
 *   than a retry with the same exit.
 * - `FreeUsageLimitError` / 429 — the per-session quota is spent; retrying with
 *   a fresh session makes it worse, which is why the session id is stable.
 * - `ModelError` / "Model is unavailable" — the pooled account no longer routes
 *   that id at all.
 *
 * @module src/errors.js
 */

/** Harness-neutral failure codes (packages/llm/llm/src/error.ts vocabulary). */
export const CODE = {
  region: 'REGION_BLOCKED',
  quota: 'RATE_LIMIT',
  credential: 'INVALID_CREDENTIAL',
  transport: 'TRANSPORT',
  timeout: 'TIMEOUT',
  server: 'SERVER',
  empty: 'EMPTY_RESPONSE',
  aborted: 'ABORTED',
}

export class UpstreamError extends Error {
  constructor(message, code, details = {}) {
    super(message)
    this.name = 'UpstreamError'
    this.code = code
    Object.assign(this, details)
  }
}

/** Turn a gateway JSON error envelope into a classified failure. */
export function classifyFailure(status, payload, retryAfterMs) {
  const error = payload?.error ?? payload ?? {}
  const type = typeof error.type === 'string' ? error.type : ''
  const message = typeof error.message === 'string' ? error.message : `upstream HTTP ${status}`
  const flat = message.toLowerCase()
  if (type === 'RegionError' || /not available in your country|region/i.test(flat)) {
    return new UpstreamError(message, CODE.region, { status, type })
  }
  if (status === 429 || type === 'FreeUsageLimitError' || /usage limit|rate limit/i.test(flat)) {
    return new UpstreamError(message, CODE.quota, { status, type, providerRetryAfterMs: retryAfterMs })
  }
  if (status === 401 || status === 403) return new UpstreamError(message, CODE.credential, { status, type })
  if (type === 'ModelError' || /model is unavailable|not supported/.test(flat)) {
    return new UpstreamError(message, CODE.server, { status, type, unavailable: true })
  }
  return new UpstreamError(message, CODE.server, { status, type })
}

/** Parse `Retry-After` into milliseconds, when the header carries a number. */
export function retryAfter(header) {
  const seconds = Number(header)
  return Number.isFinite(seconds) && seconds > 0 ? Math.trunc(seconds * 1000) : undefined
}
