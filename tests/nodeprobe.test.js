import test from 'node:test'
import assert from 'node:assert/strict'
import { probeAll } from '../src/nodeprobe.js'

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
