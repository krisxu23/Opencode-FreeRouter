import test from 'node:test'
import assert from 'node:assert/strict'
import { sessionForConversation, endpointFor, wireFor, gatewayHeaders, applyFingerprint } from '../src/upstream.js'

test('session stable per conversation', () => {
  const a = sessionForConversation('conv-1')
  assert.equal(a, sessionForConversation('conv-1'))
  assert.match(a, /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/)
})

test('endpoint split', () => {
  assert.equal(endpointFor('muse-spark-1.3-contributor-free'), '/zen/v1/responses')
  assert.equal(endpointFor('union-alpha'), '/zen/v1/messages')
  assert.equal(endpointFor('mimo-v2.6-flash-free'), '/zen/v1/chat/completions')
})

test('gateway headers carry pooled credential', () => {
  const h = gatewayHeaders({ session: 'ses_abc', requestId: 'msg_def', stream: true })
  assert.equal(h.authorization, 'Bearer public')
  assert.equal(h['x-opencode-client'], 'desktop')
  assert.match(h['user-agent'], /opencode\/1\.18/)
})

test('fingerprint quartet appended', () => {
  const body = { model: 'm', messages: [], tools: [] }
  applyFingerprint(body, false)
  assert.deepEqual(body.tools.map(t => t.function.name).sort(), ['bash', 'glob', 'grep', 'read'])
})
