import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import vm from 'node:vm'
import { startPanel } from '../src/panel.js'
import { JsonStore, SETTINGS_INITIAL } from '../src/store.js'

test('settings roundtrip and status/actions wiring', async () => {
  // PID 在 Windows 上会被复用，历史残留的 panel-test-<pid>.json 会污染首断言——
  // 文件名必须全局唯一，且跑完删除，不留脏文件。
  const storeFile = path.join(os.tmpdir(), `panel-test-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}.json`)
  const store = new JsonStore(storeFile, SETTINGS_INITIAL)
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
      models: [],
      nodes: [{ tag: 'a (Taiwan) 01', country: 'TW', port: 21000, state: 'alive', latencyMs: 200, exitIp: '1.2.3.4', lastProbeAt: 0 }],
      regionModels: ['muse-spark-1.3-contributor-free'],
    }),
    actions: {
      refresh: async () => { refreshed += 1 },
      probeNow: async () => { probed += 1 },
      refreshLimits: async () => { refreshed += 10; return { byId: { 'mimo-v2.6-flash-free': { contextWindow: 200000, maxOutput: 32000 } }, stale: false } },
    },
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

    // 限额刷新路由：POST /api/limits 调 refreshLimits 并返回行数
    const lim = await (await fetch(`${base}/api/limits`, { method: 'POST' })).json()
    assert.equal(lim.ok, true)
    assert.equal(lim.rows, 1)
    assert.equal(refreshed, 11)
  } finally {
    await close()
    store.dispose()
    fs.rmSync(storeFile, { force: true })
  }
})
