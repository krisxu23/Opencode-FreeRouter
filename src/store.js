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
  /** Exit countries in fallback order. */
  countries: ['US', 'SG', 'JP'],
  forwardPort: 3457,
  panelPort: 3458,
  catchAllPort: 20900,
  probeEnabled: true,
  probeWorkers: 24,
  probeIntervalMin: 30,
  /** Minted on first start; the OpenAI-compatible listener's key. */
  forwardKey: '',
}
