import test from 'node:test'
import assert from 'node:assert/strict'
import { isFreeLane, buildCatalog, displayModelName } from '../src/catalog.js'

test('free-lane detection: suffix ids, the ALWAYS_FREE set, and paid look-alikes', () => {
  assert.equal(isFreeLane('mimo-v2.6-flash-free'), true)
  assert.equal(isFreeLane('space-bunny-free'), true)
  // 固定免费模型，无 -free 后缀（用户实测确认，2026-09-26）
  assert.equal(isFreeLane('big-pickle'), true)
  // 无后缀的付费模型必须被排除
  assert.equal(isFreeLane('gpt-5'), false)
  assert.equal(isFreeLane('claude-opus-5'), false)
  assert.equal(isFreeLane('muse-spark-1.3'), false) // 免费的是 -contributor-free 变体
  assert.equal(isFreeLane('qwen3.8-max'), false)
})

test('buildCatalog includes big-pickle with display name and default caps', () => {
  const rows = buildCatalog(['big-pickle', 'mimo-v2.6-flash-free', 'gpt-5'])
  const ids = rows.map(r => r.id)
  assert.ok(ids.includes('big-pickle'))
  assert.ok(ids.includes('mimo-v2.6-flash-free'))
  assert.ok(!ids.includes('gpt-5'))
  const pickle = rows.find(r => r.id === 'big-pickle')
  assert.equal(pickle.name, 'Big Pickle')
  assert.equal(pickle.wire, 'chat')
  assert.ok(pickle.contextWindow > 0)
})
