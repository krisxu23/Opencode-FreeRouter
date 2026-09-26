/**
 * Settings persistence: the upstream JsonStore (coalesced writes landing
 * through temp+rename) plus the lite-gateway settings shape.
 *
 * @module src/store.js
 */

import fs from 'node:fs'
import path from 'node:path'

export class JsonStore {
  /**
   * @param {string} file - absolute path
   * @param {object} initial - value used when the file does not exist yet
   */
  constructor(file, initial) {
    this.file = file
    this.value = initial
    this.dirty = false
    this.timer = undefined
    this.disposed = false
    this.load()
  }

  load() {
    try {
      const raw = fs.readFileSync(this.file, 'utf8')
      const parsed = JSON.parse(raw)
      if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)) {
        this.value = { ...this.value, ...parsed }
      }
    } catch {
      // absent or corrupt: keep the initial value; the next write replaces it
    }
  }

  get() {
    return this.value
  }

  /** Merge a patch in and schedule the write. Returns the new value. */
  update(patch) {
    if (this.disposed) return this.value
    this.value = { ...this.value, ...patch }
    this.schedule()
    return this.value
  }

  /** Mutate through a callback; used for read-modify-write on nested state. */
  edit(mutate) {
    if (this.disposed) return this.value
    const next = mutate(structuredClone(this.value))
    if (next !== undefined) this.value = next
    this.schedule()
    return this.value
  }

  schedule(delayMs = 300) {
    if (this.disposed) return
    this.dirty = true
    if (this.timer !== undefined) return
    this.timer = setTimeout(() => {
      this.timer = undefined
      this.flush()
    }, delayMs)
    this.timer.unref?.()
  }

  flush() {
    if (!this.dirty || this.disposed) return
    this.dirty = false
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true })
      const temp = `${this.file}.${process.pid}.tmp`
      fs.writeFileSync(temp, JSON.stringify(this.value, undefined, 2), { mode: 0o600 })
      fs.renameSync(temp, this.file)
    } catch {
      // fail-soft: the next mutation retries, and nothing downstream depends on it
    }
  }

  dispose() {
    if (this.timer !== undefined) { clearTimeout(this.timer); this.timer = undefined }
    this.flush()
    this.disposed = true
    this.dirty = false
  }
}

export const SETTINGS_INITIAL = {
  /** Subscription URLs; empty = the built-in freesub source chain. */
  subUrls: [],
  /** 出口地区固定分组（US/JP/HK/TW/KR/SG/EU/OTHER），顺序即回退顺序。 */
  countries: ['US', 'JP', 'HK', 'TW', 'KR', 'SG'],
  forwardPort: 3457,
  panelPort: 3458,
  catchAllPort: 20900,
  /** Default thinking-effort tier (real max_tokens budget) when the caller
   *  does not pass reasoning_effort. Upstream semantics: balanced. */
  effortLevel: 'balanced',
  /** Cap a turn's output so a slow lane cannot run away. */
  defaultMaxTokens: 32768,
  /** Per-node local inbound range: [portBase, portBase + portSpan). Keep it
   *  below 49152 (Windows ephemeral client ports start there) and span >= the
   *  largest node count you expect — a few thousand needs span ~10000. */
  portBase: 21000,
  portSpan: 8000,
  probeEnabled: true,
  probeWorkers: 24,
  probeIntervalMin: 30,
  /** Minted on first start; the OpenAI-compatible listener's key. */
  forwardKey: '',
}
