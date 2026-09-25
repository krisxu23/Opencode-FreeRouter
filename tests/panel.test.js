import test from 'node:test'
import assert from 'node:assert/strict'
import vm from 'node:vm'
import { startPanel } from '../src/panel.js'
import { JsonStore, SETTINGS_INITIAL } from '../src/store.js'

test('settings roundtrip and status/actions wiring', async () => {
  const store = new JsonStore(`${process.env.TMP ?? '.'}/panel-test-${process.pid}.json`, SETTINGS_INITIAL)
  let refreshed = 0
  let probed = 0
  const { port, close } = await startPanel({
    getSettings: () => store.get(),
    applySettings: patch => {
      store.update(patch)
      return store.get()
    },
    status: () => ({
      singbox: { running: true, pid: 123, catchAllPort: 20900 },
      forward: { running: true, port: 3457 },
      nodes: [{ tag: 'a (Taiwan) 01', country: 'TW', port: 21000, state: 'alive', latencyMs: 200, exitIp: '1.2.3.4', lastProbeAt: 0 }],
      regionModels: ['muse-spark-1.3-contributor-free'],
    }),
    actions: { refresh: async () => { refreshed += 1 }, probeNow: async () => { probed += 1 } },
  })
  try {
    const base = `http://127.0.0.1:${port}`

    const page = await fetch(base)
    assert.equal(page.status, 200)
    const html = await page.text()
    assert.match(html, /Opencode-FreeRouter/)
    // 回归：PAGE 是模板字符串，源码里写 `\n` 会被求值成真实换行，让浏览器脚本报
    // SyntaxError、整个控制台静默变空板（实测事故）。这里对"模板求值后"的成品脚本
    // 做语法检查，堵死这一类错误。
    const script = html.split('<script>')[1].split('</' + 'script>')[0]
    assert.doesNotThrow(() => new vm.Script(script), '面板内联脚本必须能通过语法检查')

    const initial = await (await fetch(`${base}/api/settings`)).json()
    assert.deepEqual(initial.countries, SETTINGS_INITIAL.countries)

    const put = await fetch(`${base}/api/settings`, {
      method: 'PUT', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ countries: ['JP', 'SG'], probeEnabled: false }),
    })
    assert.deepEqual((await put.json()).countries, ['JP', 'SG'])
    assert.equal(store.get().probeEnabled, false)

    const status = await (await fetch(`${base}/api/status`)).json()
    assert.equal(status.nodes[0].state, 'alive')
    assert.deepEqual(status.regionModels, ['muse-spark-1.3-contributor-free'])

    await fetch(`${base}/api/probe`, { method: 'POST' })
    await fetch(`${base}/api/refresh`, { method: 'POST' })
    assert.equal(probed, 1)
    assert.equal(refreshed, 1)
  } finally {
    await close()
    store.dispose()
  }
})
