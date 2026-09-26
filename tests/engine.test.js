import test from 'node:test'
import assert from 'node:assert/strict'
import http from 'node:http'

// The upstream base is a module-level constant — point it at the fake upstream
// before any module loads.
let armRegionOnce = false
const fakeRequests = []
const fake = http.createServer((req, res) => {
  let body = ''
  req.on('data', c => { body += c })
  req.on('end', () => {
    fakeRequests.push({ path: req.url, body: JSON.parse(body || '{}'), session: req.headers['x-opencode-session'] })
    if (armRegionOnce) {
      armRegionOnce = false
      res.writeHead(403, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ error: { type: 'RegionError', message: 'not available in your country' } }))
      return
    }
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    const send = payload => res.write(`data: ${JSON.stringify(payload)}\n\n`)
    send({ choices: [{ delta: { content: 'hello ' } }] })
    send({ choices: [{ delta: { content: 'world' } }] })
    send({ choices: [{ delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 } })
    res.write('data: [DONE]\n\n')
    res.end()
  })
})
await new Promise(resolve => fake.listen(0, '127.0.0.1', resolve))
process.env.OUR_FREE_MODEL_BASE = `http://127.0.0.1:${fake.address().port}`
test.after(() => fake.close())

const { createEngine } = await import('../src/engine.js')
const { buildCatalog } = await import('../src/catalog.js')
const { LEVELS, budgetFor } = await import('../src/effort.js')
const { CODE } = await import('../src/errors.js')

const entry = buildCatalog(['mimo-v2.6-flash-free'])[0]

function engineWithPicker(picker) {
  return createEngine({
    state: () => ({
      catalog: [entry],
      membership: { 'our-free-model': [entry.id] },
      settings: { enabled: true, defaultMaxTokens: 32768 },
      attributionUserAgent: '',
    }),
    recordUsage: () => {},
    settingsOf: () => ({ countries: ['US'] }),
    poolOf: () => [{ tag: 'node-a', country: 'US' }, { tag: 'node-b', country: 'SG' }],
    portOf: tag => ({ 'node-a': 21000, 'node-b': 21001 })[tag],
    picker,
  })
}

test('effort budgets: enforced max_tokens ceilings', () => {
  assert.equal(budgetFor('light', entry, undefined, 32768), 4096) // 不可关闭思考的模型预算翻倍
  assert.equal(budgetFor('balanced', entry, undefined, 32768), 16384) // 上游 issue #2: 8192 会被不可关闭的思考吃掉 82%
  assert.equal(budgetFor('deep', entry, undefined, 32768), 32768)
  assert.deepEqual(LEVELS.map(l => l.id), ['light', 'balanced', 'deep'])
})

test('complete streams text and usage through the adapter chain', async () => {
  let calls = 0
  const engine = engineWithPicker(() => ({ nodeKey: 'node-a', addr: 'direct', country: 'US' }))
  const deltas = []
  const outcome = await engine.complete({ model: 'mimo-v2.6-flash-free', openAi: { messages: [{ role: 'user', content: 'hi' }] } }, chunk => {
    if (chunk.type === 'text-delta') deltas.push(chunk.text)
    calls += 1
  })
  assert.equal(outcome.text, 'hello world')
  assert.deepEqual(deltas, ['hello ', 'world'])
  assert.ok(calls >= 2)
  assert.equal(outcome.usage.completion_tokens, 2) // OpenAI 形状
  assert.equal(outcome.usage.prompt_tokens, 10)
  assert.equal(fakeRequests.at(-1).body.messages.at(-1).content, 'hi')
  assert.equal(fakeRequests.at(-1).body.max_tokens, 16384) // 默认 balanced 档预算（思考不可关闭 ×2）
})

test('pre-content region failure retries once on the next exit, same session', async () => {
  armRegionOnce = true
  const picks = []
  const engine = engineWithPicker(({ exclude }) => {
    picks.push(exclude ?? null)
    return picks.length === 1 ? { nodeKey: 'node-a', addr: 'direct', country: 'US' } : { nodeKey: 'node-b', addr: 'direct', country: 'SG' }
  })
  const outcome = await engine.complete({ model: 'mimo-v2.6-flash-free', openAi: { messages: [{ role: 'user', content: 'hi' }] } }, () => {})
  assert.equal(outcome.text, 'hello world')
  assert.deepEqual(picks, [null, 'node-a']) // first pick, then exclude the failed node
  const [first, second] = fakeRequests.slice(-2)
  assert.equal(first.session, second.session) // quota follows the session across the retry
})

test('unknown model is refused without dialing', async () => {
  const engine = engineWithPicker(() => { throw new Error('should not be called') })
  await assert.rejects(
    engine.complete({ model: 'gpt-99-turbo', openAi: { messages: [] } }, () => {}),
    error => error.code === CODE.server,
  )
})

test('modelRows filters models measured region-blocked on every alive exit (issue #3)', async () => {
  const { markProbe, noteRegionError } = await import('../src/health.js')
  markProbe('node-a', { state: 'alive', latencyMs: 100 })
  noteRegionError('mimo-v2.6-flash-free', 'node-a')
  const engine = engineWithPicker(() => null)
  const ids = engine.modelRows().map(r => r.id)
  assert.ok(!ids.includes('mimo-v2.6-flash-free'), '受限模型不应出现在 /v1/models')
})
