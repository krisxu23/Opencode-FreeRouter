import test from 'node:test'
import assert from 'node:assert/strict'
import { probeAll, probeNode, ECHO_URLS, UPSTREAM_GATE_URL } from '../src/nodeprobe.js'

test('probeAll processes every item exactly once, dead exits reported as dead', async () => {
  const items = Array.from({ length: 12 }, (_, i) => i)
  const seen = []
  await probeAll(items, {
    addrOf: () => 'http://127.0.0.1:1', // nothing listens here -> dead, fast
    workers: 3,
    onResult: (item, result) => {
      seen.push(item)
      assert.equal(result.state, 'dead')
    },
  })
  assert.deepEqual(seen.sort((a, b) => a - b), items)
  assert.equal(seen.length, 12)
})

test('P1/P2: gate 并入存活判定 + echo 无明文源', () => {
  // gate（opencode.ai）与 liveness 同属第一阶段候选：能到上游即算可达
  assert.ok(UPSTREAM_GATE_URL.startsWith('https://'))
  // echo 只留 https 源：出口 IP 不得明文出境
  assert.ok(ECHO_URLS.length >= 1)
  assert.ok(ECHO_URLS.every(u => u.startsWith('https://')), `echo must be https-only: ${ECHO_URLS}`)
})

test('P1: 首阶段失败即判 dead，不浪费 echo', async () => {
  const t0 = Date.now()
  const r = await probeNode('http://127.0.0.1:1', { timeoutMs: 3000 })
  assert.equal(r.state, 'dead')
  assert.ok(Date.now() - t0 < 15000, 'dead verdict must not wait out stacked timeouts')
})
