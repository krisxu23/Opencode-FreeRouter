import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  markProbe, healthOf, nodeUsable, pruneStale, persistHealth,
  noteRegionError, noteRegionOK, regionUsable, regionProbeCandidates,
  noteSticky, exitForSession,
  pickExit, setHealthFile,
} from '../src/health.js'

test('unknown is usable; only measured dead excludes', () => {
  assert.equal(healthOf('n1'), 'unknown')
  assert.equal(nodeUsable('n1'), true)
  markProbe('n1', { state: 'dead', latencyMs: -1 })
  assert.equal(nodeUsable('n1'), false)
  markProbe('n1', { state: 'alive', latencyMs: 300, exitCountry: 'US' })
  assert.equal(nodeUsable('n1'), true)
})

test('pruneStale removes nodes that left the pool', () => {
  markProbe('keep', { state: 'alive', latencyMs: 1 })
  markProbe('drop', { state: 'alive', latencyMs: 1 })
  pruneStale(['keep'])
  assert.equal(healthOf('keep'), 'alive')
  assert.equal(healthOf('drop'), 'unknown')
})

test('region matrix: unknown passes, false excludes, true authoritative', () => {
  assert.deepEqual(regionUsable('m', 'n2'), { usable: true, known: false })
  noteRegionError('m', 'n2')
  assert.deepEqual(regionUsable('m', 'n2'), { usable: false, known: true })
  noteRegionOK('m', 'n2')
  assert.deepEqual(regionUsable('m', 'n2'), { usable: true, known: true })
})

test('regionProbeCandidates only lists alive nodes with unknown verdict', () => {
  markProbe('rc-a', { state: 'alive', latencyMs: 1 })
  markProbe('rc-b', { state: 'dead', latencyMs: -1 })
  markProbe('rc-c', { state: 'alive', latencyMs: 2 })
  noteRegionOK('rc-model', 'rc-a')
  const cands = regionProbeCandidates('rc-model')
  assert.ok(cands.includes('rc-c'))
  assert.ok(!cands.includes('rc-a')) // verdict already known
  assert.ok(!cands.includes('rc-b')) // dead nodes are not probe targets
})

test('sticky session TTL', () => {
  noteSticky('s1', 'a')
  assert.equal(exitForSession('s1'), 'a')
  // TTL expiry: 30min window is internal; unknown node also invalidates
  assert.equal(exitForSession('never'), null)
})

test('pickExit: country order wins, dead and region-false excluded, sticky first', () => {
  const pool = [
    { tag: 'us-1', country: 'US' },
    { tag: 'sg-1', country: 'SG' },
    { tag: 'jp-1', country: 'JP' },
  ]
  const portOf = tag => ({ 'us-1': 21000, 'sg-1': 21001, 'jp-1': 21002 })[tag]
  markProbe('us-1', { state: 'dead', latencyMs: 10 })
  markProbe('sg-1', { state: 'alive', latencyMs: 500 })
  markProbe('jp-1', { state: 'alive', latencyMs: 100 })
  noteRegionError('blocked-model', 'jp-1')

  // US is dead -> falls to SG (second country), JP excluded for the blocked model
  const hit = pickExit({ model: 'blocked-model', countries: ['US', 'SG', 'JP'], pool, portOf })
  assert.equal(hit.nodeKey, 'sg-1')

  // same model, countries without a healthy node -> null from the selected set,
  // but the last-resort pool scan still returns a usable node (never direct)
  const jp = pickExit({ model: 'other-model', countries: ['US'], pool, portOf })
  assert.equal(jp.nodeKey, 'jp-1')

  // sticky wins when still usable
  noteSticky('sess', 'sg-1')
  const sticky = pickExit({ model: 'other-model', countries: ['US', 'SG', 'JP'], pool, portOf, stickyNode: 'sg-1' })
  assert.equal(sticky.nodeKey, 'sg-1')
})

test('health persists and reloads through a temp file', () => {
  const file = path.join(os.tmpdir(), `lite-health-${process.pid}.json`)
  setHealthFile(file)
  markProbe('px', { state: 'alive', latencyMs: 42, exitCountry: 'DE' })
  noteRegionError('pm', 'px')
  persistHealth()
  // reload from disk via a fresh setHealthFile roundtrip
  setHealthFile(file)
  assert.equal(healthOf('px'), 'alive')
  assert.deepEqual(regionUsable('pm', 'px'), { usable: false, known: true })
  fs.rmSync(file, { force: true })
})
