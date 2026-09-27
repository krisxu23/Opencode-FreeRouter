import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { CAPABILITIES, capabilitiesFor, buildCatalog } from '../src/catalog.js'
import {
  normalizeRow, extractOpencodeOverlay, applyLimitsOverlay,
  refreshLimits, loadLimitsCache, saveLimitsCache,
} from '../src/limits.js'

// ---- local table tracks the models.dev Zen (opencode) rows, not OpenRouter ----

// User-copied Zen page values + models.dev `opencode` rows (2026-09-27).
const ZEN_ROWS = {
  'mimo-v2.6-flash-free': { contextWindow: 200000, maxOutput: 32000 },
  'muse-spark-1.3-contributor-free': { contextWindow: 1048576, maxOutput: 131072 },
  'nemotron-3.5-lightning-free': { contextWindow: 262144, maxOutput: 262144 },
  'nemotron-3-ultra-free': { contextWindow: 1000000, maxOutput: 128000 },
  'ling-3.0-flash-fin-free': { contextWindow: 262144, maxOutput: 32768 },
  'space-bunny-free': { contextWindow: 1048576, maxOutput: 524288 },
  'big-pickle': { contextWindow: 200000, maxOutput: 32000 },
  'longcat-2.5-preview-free': { contextWindow: 1000000, maxOutput: 131072 },
}

test('CAPABILITIES matches the Zen-lane (models.dev opencode) numbers', () => {
  for (const [id, want] of Object.entries(ZEN_ROWS)) {
    const caps = capabilitiesFor(id)
    assert.equal(caps.contextWindow, want.contextWindow, `${id} contextWindow`)
    assert.equal(caps.maxOutput, want.maxOutput, `${id} maxOutput`)
  }
})

test('buildCatalog carries the Zen-lane numbers end to end', () => {
  const rows = buildCatalog(Object.keys(ZEN_ROWS))
  assert.equal(rows.length, Object.keys(ZEN_ROWS).length)
  for (const row of rows) {
    const want = ZEN_ROWS[row.id]
    assert.equal(row.contextWindow, want.contextWindow, `${row.id} contextWindow`)
    assert.equal(row.maxOutput, want.maxOutput, `${row.id} maxOutput`)
  }
})

test('CAPABILITIES patterns are ordered specific-first (no early shadow)', () => {
  // nemotron ultra/lightning rows must precede the generic nemotron row,
  // mimo v2.6/v2.5 before generic mimo — otherwise the generic row shadows them.
  const idx = re => CAPABILITIES.findIndex(c => String(c.match) === String(re))
  assert.ok(idx(/^nemotron.*3\.5.*lightning/) < idx(/^nemotron/), 'lightning before generic nemotron')
  assert.ok(idx(/^nemotron.*ultra/) < idx(/^nemotron/), 'ultra before generic nemotron')
  assert.ok(idx(/^mimo.*v2\.6/) < idx(/^mimo/), 'mimo v2.6 before generic mimo')
})

// ---- limits.js overlay ----

test('normalizeRow accepts limit rows, rejects rows without limits', () => {
  assert.deepEqual(
    normalizeRow({ limit: { context: 200000, output: 32000 }, reasoning: true, attachment: true }),
    { contextWindow: 200000, maxOutput: 32000, reasoning: true, vision: true },
  )
  assert.equal(normalizeRow({ limit: { context: 0, output: 32000 } }), null)
  assert.equal(normalizeRow({}), null)
})

test('extractOpencodeOverlay keeps only the opencode provider rows', () => {
  const byId = extractOpencodeOverlay({ opencode: { models: { 'a-free': { limit: { context: 1, output: 2 } } } } })
  assert.deepEqual(byId, { 'a-free': { contextWindow: 1, maxOutput: 2 } })
  assert.deepEqual(extractOpencodeOverlay({}), {})
  assert.deepEqual(extractOpencodeOverlay(null), {})
})

test('applyLimitsOverlay overwrites caps by exact id, keeps probe-verified vision', () => {
  const entries = buildCatalog(['mimo-v2.6-flash-free', 'brand-new-free'])
  assert.equal(entries.find(e => e.id === 'brand-new-free').contextWindow, 131072) // fallback row
  const next = applyLimitsOverlay(entries, {
    'mimo-v2.6-flash-free': { contextWindow: 200000, maxOutput: 32000, reasoning: true, vision: false },
    'brand-new-free': { contextWindow: 500000, maxOutput: 64000, reasoning: true, vision: true },
  })
  const mimo = next.find(e => e.id === 'mimo-v2.6-flash-free')
  assert.equal(mimo.contextWindow, 200000)
  assert.equal(mimo.maxOutput, 32000)
  assert.equal(mimo.vision, true) // local probe result wins over attachment bit
  const fresh = next.find(e => e.id === 'brand-new-free')
  assert.equal(fresh.contextWindow, 500000)
  assert.equal(fresh.vision, true) // unknown model: overlay attachment bit beats default false
  assert.equal(entries.find(e => e.id === 'mimo-v2.6-flash-free').contextWindow, 200000) // input untouched
})

test('refreshLimits: fetch success persists, failure falls back to stale cache', async () => {
  const file = path.join(os.tmpdir(), `limits-test-${process.pid}.json`)
  try {
    const okFetch = async () => ({ ok: true, json: async () => ({ opencode: { models: { 'x-free': { limit: { context: 10, output: 20 } } } } }) })
    const r1 = await refreshLimits({ cacheFile: file, fetchImpl: okFetch })
    assert.deepEqual(r1.byId, { 'x-free': { contextWindow: 10, maxOutput: 20 } })
    assert.ok(loadLimitsCache(file))

    const badFetch = async () => { throw new Error('boom') }
    const r2 = await refreshLimits({ cacheFile: file, fetchImpl: badFetch, force: true })
    assert.equal(r2.stale, true)
    assert.deepEqual(r2.byId, { 'x-free': { contextWindow: 10, maxOutput: 20 } })

    // fresh cache short-circuits the fetch entirely
    let called = 0
    const counting = async (...a) => { called += 1; return okFetch(...a) }
    await refreshLimits({ cacheFile: file, fetchImpl: counting })
    assert.equal(called, 0)
  } finally {
    fs.rmSync(file, { force: true })
  }
})

test('refreshLimits with no cache and failed fetch returns empty overlay (local table serves)', async () => {
  const r = await refreshLimits({ fetchImpl: async () => { throw new Error('offline') } })
  assert.deepEqual(r.byId, {})
  assert.match(r.error, /offline/)
})

test('limits cache roundtrip', () => {
  const file = path.join(os.tmpdir(), `limits-rt-${process.pid}.json`)
  try {
    saveLimitsCache(file, { fetchedAt: 123, byId: { a: { contextWindow: 1, maxOutput: 2 } } })
    assert.deepEqual(loadLimitsCache(file), { byId: { a: { contextWindow: 1, maxOutput: 2 } }, fetchedAt: 123 })
    assert.equal(loadLimitsCache(`${file}.missing`), null)
  } finally {
    fs.rmSync(file, { force: true })
  }
})
