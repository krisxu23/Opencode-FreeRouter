import test from 'node:test'
import assert from 'node:assert/strict'
import { classifyFailure, CODE } from '../src/errors.js'
import { dispatcherFor } from '../src/http.js'

test('region gate', () => {
  const e = classifyFailure(403, { error: { type: 'RegionError', message: 'not available in your country' } })
  assert.equal(e.code, CODE.region)
})

test('quota carries retryAfter', () => {
  const e = classifyFailure(429, { error: { type: 'FreeUsageLimitError', message: 'usage limit' } }, 5000)
  assert.equal(e.code, CODE.quota)
  assert.equal(e.providerRetryAfterMs, 5000)
})

test('model unavailable flag', () => {
  const e = classifyFailure(400, { error: { type: 'ModelError', message: 'Model is unavailable' } })
  assert.equal(e.unavailable, true)
})

test('dispatcher cached per exit', () => {
  assert.equal(dispatcherFor('direct'), undefined)
  const a = dispatcherFor('http://127.0.0.1:21001')
  assert.equal(a, dispatcherFor('http://127.0.0.1:21001'))
  assert.notEqual(a, dispatcherFor('http://127.0.0.1:21002'))
})
