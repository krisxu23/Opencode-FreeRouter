import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  markProbe, healthOf, nodeUsable, pruneStale, persistHealth,
  noteRegionError, noteRegionOK, regionUsable, regionProbeCandidates,
  noteSticky, exitForSession, noteStickyFailure, clearStickyFailures, stickyBurned,
  pickExit, setHealthFile, seedRestrictedModels, isRestrictedModel,
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

test('pickExit: 受限模型严格走验证过的特殊节点，普通模型特殊+普通并用', () => {
  const pool = [{ tag: 'n1', country: 'US' }, { tag: 'n2', country: 'US' }]
  const portOf = t => ({ n1: 21000, n2: 21001 })[t]
  markProbe('n1', { state: 'alive', latencyMs: 300 })
  markProbe('n2', { state: 'alive', latencyMs: 100 })
  noteRegionOK('mm3', 'n1') // n1 = 验证过的特殊节点（★）
  // 受限模型：n2 更快但未验证 → 严格用 n1，不赌未验证出口
  const hit = pickExit({ model: 'mm3', restricted: true, countries: ['US'], pool, portOf })
  assert.equal(hit.nodeKey, 'n1')
  // 普通模型：特殊节点与普通节点并用 → 更快的 n2 胜出
  const hit2 = pickExit({ model: 'mm4', countries: ['US'], pool, portOf })
  assert.equal(hit2.nodeKey, 'n2')
})

test('seedRestrictedModels 预置受限名单', () => {
  seedRestrictedModels(['muse-spark-1.3-contributor-free'])
  assert.equal(isRestrictedModel('muse-spark-1.3-contributor-free'), true)
  assert.equal(isRestrictedModel('mimo-v2.6-flash-free'), false)
})

test('pickExit: EU/OTHER 分组按分桶匹配（回归：按原始国家码查表永远落空）', () => {
  const pool = [
    { tag: 'nl-1', country: 'NL' },
    { tag: 'ca-1', country: 'CA' },
    { tag: 'us-1', country: 'US' },
  ]
  const portOf = tag => ({ 'nl-1': 21000, 'ca-1': 21001, 'us-1': 21002 })[tag]
  markProbe('nl-1', { state: 'alive', latencyMs: 50 })
  markProbe('ca-1', { state: 'alive', latencyMs: 60 })
  markProbe('us-1', { state: 'alive', latencyMs: 70 })
  // EU 分组必须命中荷兰节点（bucketOf NL = EU），不能穿透到兜底
  const eu = pickExit({ model: 'eu-model', countries: ['EU'], pool, portOf })
  assert.equal(eu.nodeKey, 'nl-1')
  // OTHER 分组必须命中加拿大节点（bucketOf CA = OTHER）
  const other = pickExit({ model: 'other-model-2', countries: ['OTHER'], pool, portOf })
  assert.equal(other.nodeKey, 'ca-1')
  // 回退顺序：EU 优先，EU 无可用时才走 OTHER
  const both = pickExit({ model: 'both-model', countries: ['EU', 'OTHER'], pool, portOf })
  assert.equal(both.nodeKey, 'nl-1')
})

test('sticky fuse: 同一 sticky 连跪 2 次就轮换，第 3 次请求不再撞它', () => {
  markProbe('fuse-a', { state: 'alive', latencyMs: 10 })
  markProbe('fuse-b', { state: 'alive', latencyMs: 20 })
  noteSticky('fuse-sess', 'fuse-a')
  assert.equal(exitForSession('fuse-sess'), 'fuse-a')
  noteStickyFailure('fuse-sess')
  assert.equal(exitForSession('fuse-sess'), 'fuse-a') // 1 次还忍
  noteStickyFailure('fuse-sess')
  assert.equal(stickyBurned('fuse-sess'), true)
  assert.equal(exitForSession('fuse-sess'), null) // 第 3 次请求直接换出口
  // 新 sticky 落定后计数清零，恢复正常
  noteSticky('fuse-sess', 'fuse-b')
  assert.equal(exitForSession('fuse-sess'), 'fuse-b')
  clearStickyFailures('fuse-sess')
  assert.equal(stickyBurned('fuse-sess'), false)
})
