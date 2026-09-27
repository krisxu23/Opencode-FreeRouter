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
  assert.equal(budgetFor('deep', entry, undefined, 32768), 32000) // Zen 车道限额 32K < 默认上限 32768，deep 取车道限额
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

test('tool definitions survive the engine -> adapter projection on both wires', async () => {
  // Regression: engine used to pre-project tools into a provider shape, and the
  // adapter's own toToolDefs then read `tool.name` off a `{function:{name}}` row
  // and dropped every real tool — the upstream saw only the fingerprint quartet
  // (chat wire: plus `tool_choice: none`), so no model ever called a tool.
  const tools = [
    { type: 'function', function: { name: 'read_file', description: 'read a file', parameters: { type: 'object', properties: {} } } },
    { type: 'function', function: { name: 'write_file', description: 'write a file', parameters: { type: 'object', properties: {} } } },
  ]
  const rows = [entry, buildCatalog(['muse-spark-1.3-contributor-free'])[0]]
  const mk = () => createEngine({
    state: () => ({ catalog: rows, membership: {}, settings: { enabled: true, defaultMaxTokens: 32768 }, attributionUserAgent: '' }),
    recordUsage: () => {},
    settingsOf: () => ({ countries: ['US'] }),
    poolOf: () => [{ tag: 'node-a', country: 'US' }],
    portOf: tag => (tag === 'node-a' ? 21000 : undefined),
    picker: () => ({ nodeKey: 'node-a', addr: 'direct', country: 'US' }),
  })
  const namesOf = body => (body.tools ?? []).map(t => t.name ?? t.function?.name)

  // chat wire: client spoke POST /v1/chat/completions
  await mk().complete({ model: 'mimo-v2.6-flash-free', openAi: { messages: [{ role: 'user', content: 'hi' }], tools } }, () => {})
  const chat = fakeRequests.at(-1).body
  assert.deepEqual(namesOf(chat).filter(n => n === 'read_file' || n === 'write_file'), ['read_file', 'write_file'])
  assert.notEqual(chat.tool_choice, 'none') // 'none' forbids the model from calling anything

  // responses wire (muse-spark): client spoke POST /v1/responses. The fake
  // upstream answers in chat spelling, so the turn itself comes back empty —
  // the assertion is about what WE sent.
  await mk().complete({ model: 'muse-spark-1.3-contributor-free', openAi: { messages: [{ role: 'user', content: 'hi' }], tools }, responses: true }, () => {}).catch(() => {})
  const responses = fakeRequests.at(-1).body
  assert.equal(fakeRequests.at(-1).path, '/zen/v1/responses')
  assert.ok(namesOf(responses).includes('read_file'), `flat tools must survive: ${JSON.stringify(namesOf(responses))}`)
  assert.ok(namesOf(responses).includes('bash'), 'fingerprint quartet still appended')
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
