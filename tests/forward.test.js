import test from 'node:test'
import assert from 'node:assert/strict'
import { generateKey, keyMatches } from '../src/forward.js'
import { startForwardServer } from '../src/forward.js'

test('issued key roundtrip constant-time', () => {
  const k = generateKey()
  assert.match(k, /^ofm-/)
  assert.equal(keyMatches(k, k), true)
  assert.equal(keyMatches(`${k}x`, k), false)
  assert.equal(keyMatches(k, generateKey()), false)
})

test('listener: auth gate, health, and models listing', async () => {
  const key = generateKey()
  const rows = [{ id: 'mimo-v2.6-flash-free', object: 'model', created: 1, owned_by: 'lite-gateway' }]
  const { port, close } = await startForwardServer({
    config: () => ({ host: '127.0.0.1', port: 0, enabled: true, key }),
    complete: async () => { throw new Error('not dialled in this test') },
    modelRows: () => rows,
  })
  try {
    const base = `http://127.0.0.1:${port}`
    const health = await fetch(`${base}/health`)
    assert.equal(health.status, 200)

    const unauth = await fetch(`${base}/v1/models`)
    assert.equal(unauth.status, 401)

    const badKey = await fetch(`${base}/v1/models`, { headers: { authorization: `Bearer ofm-wrong` } })
    assert.equal(badKey.status, 401)

    const ok = await fetch(`${base}/v1/models`, { headers: { 'x-api-key': key } })
    assert.equal(ok.status, 200)
    assert.deepEqual((await ok.json()).data, rows)

    const missing = await fetch(`${base}/v1/nope`, { headers: { authorization: `Bearer ${key}` } })
    assert.equal(missing.status, 404)
  } finally {
    await close()
  }
})
