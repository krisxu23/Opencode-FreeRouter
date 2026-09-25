// 上游 issue #6 的回归测试：postStreamed 按响应体首块形状分类，
// 不信任 Content-Type，也不把流式响应当 JSON 误杀。
import test from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'

let mode = 'sse-as-json'
const srv = http.createServer((req, res) => {
  if (mode === 'sse-as-json') {
    // 网关高负载实测形态：Content-Type 是 JSON，但响应体是 SSE 帧
    res.writeHead(200, { 'content-type': 'application/json' })
    res.write('data: {"choices":[{"delta":{"content":"hi"}}]}\n\n')
    res.write('data: [DONE]\n\n')
    res.end()
  } else if (mode === 'plain-json') {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ ok: true }))
  } else if (mode === 'error-json') {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ error: { type: 'ModelError', message: 'Model is unavailable' } }))
  } else if (mode === 'empty') {
    res.writeHead(200, { 'content-type': 'text/plain' })
    res.end()
  }
})
await new Promise(resolve => srv.listen(0, '127.0.0.1', resolve))
process.env.OUR_FREE_MODEL_BASE = `http://127.0.0.1:${srv.address().port}`
test.after(() => srv.close())

const { postStreamed, CODE } = await import('../src/http.js')

test('SSE body with a JSON content-type still streams (issue #6)', async () => {
  mode = 'sse-as-json'
  const chunks = []
  const r = await postStreamed({ path: '/zen/v1/chat/completions', body: {}, session: 'ses_x', requestId: 'msg_x', onData: p => chunks.push(p) })
  assert.equal(r.status, 200)
  assert.deepEqual(chunks, ['{"choices":[{"delta":{"content":"hi"}}]}'])
})

test('plain JSON success is parsed and forwarded once', async () => {
  mode = 'plain-json'
  const chunks = []
  await postStreamed({ path: '/x', body: {}, session: 'ses_x', requestId: 'msg_x', onData: p => chunks.push(p) })
  assert.deepEqual(chunks, [JSON.stringify({ ok: true })])
})

test('JSON error envelope on 200 still classifies', async () => {
  mode = 'error-json'
  await assert.rejects(
    postStreamed({ path: '/x', body: {}, session: 'ses_x', requestId: 'msg_x', onData: () => {} }),
    error => error.unavailable === true,
  )
})

test('empty body maps to EMPTY_RESPONSE', async () => {
  mode = 'empty'
  await assert.rejects(
    postStreamed({ path: '/x', body: {}, session: 'ses_x', requestId: 'msg_x', onData: () => {} }),
    error => error.code === CODE.empty,
  )
})
