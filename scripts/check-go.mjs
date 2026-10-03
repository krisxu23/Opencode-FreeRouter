/**
 * Layer + hygiene checker for the Go tree.
 *
 * The layer rule is the same one archive/node/scripts-js/check.mjs enforces for JS: a package
 * at layer n may import packages at layer <= n, every package must be listed in
 * LAYERS, and there must be no import cycle. Two Go-specific checks ride along
 * because they have no JS analogue: every non-test source file carries the GPL
 * SPDX header (constraint 6), and `go vet` must be silent.
 *
 * Why a separate script rather than extending check.mjs: check.mjs（现 archive/node/scripts-js/）parses JS
 * with regexes and most of its rules (browser purity, sync spawn, CSS contrast)
 * have no Go meaning; interleaving them would make one file half about a
 * language it cannot see.
 *
 * @module scripts/check-go.mjs
 */

import fs from 'node:fs'
import path from 'node:path'
import { TAGS, ROOT, runGo, goEnv } from './go-build.mjs'

/** Layer -> package names. A package at layer n may import layer <= n. */
// 修正案（docs/superpowers/plans/2026-10-01-plan-corrections.md §1）：
// forward 在 L5（其 Config.Complete 返回 engine.Outcome，L1 装不下）；
// upstream 在 L0（catalog.Model 引用 upstream.Wire）；effort/stats 在 L1；
// tray 在 L5。nodeprobe 与 sbx 同层但互不 import：sbx.Dialer 是类型别名，
// 拨号器由 app 组装注入。
export const LAYERS = {
  0: ['check', 'errors', 'persistence', 'logger', 'tracelog', 'parse', 'upstream'],
  1: ['catalog', 'limits', 'messages', 'stream', 'httpclient', 'gate', 'effort', 'stats'],
  2: ['sub', 'registry', 'nodeprobe', 'sbx'],
  3: ['health', 'adapter'],
  4: ['engine'],
  5: ['panel', 'app', 'forward', 'tray'],
}

function pkgDirs() {
  const base = path.join(ROOT, 'internal')
  if (!fs.existsSync(base)) return []
  return fs.readdirSync(base, { withFileTypes: true })
    .filter(d => d.isDirectory())
    .map(d => d.name)
    .sort()
}

function layerMap() {
  const map = new Map()
  for (const [layer, names] of Object.entries(LAYERS)) {
    for (const n of names) {
      if (map.has(n)) throw new Error(`LAYERS 里 ${n} 出现两次`)
      map.set(n, Number(layer))
    }
  }
  return map
}

/** Internal imports of a package, read from its non-test sources. */
function importsOf(pkg) {
  const dir = path.join(ROOT, 'internal', pkg)
  const found = new Set()
  for (const f of fs.readdirSync(dir)) {
    if (!f.endsWith('.go') || f.endsWith('_test.go')) continue
    const src = fs.readFileSync(path.join(dir, f), 'utf8')
    for (const m of src.matchAll(/"freerouter\/internal\/([a-z0-9]+)"/g)) found.add(m[1])
  }
  return found
}

/**
 * Static-asset checks for the console shell and browser bundle.
 *
 * These live here rather than in check.mjs（archive/node/scripts-js/） because the two files are the Go
 * product's assets: index.html must keep the boot marker intact (the server
 * replaces it with the snapshot, and a second copy would make the replacement
 * ambiguous) and app.js must stay free of the zero-port architecture the Go
 * rewrite deleted — a leftover clampPortBase would silently reinstate a
 * concept the gateway no longer has.
 *
 * @returns {string[]} errors
 */
function checkWebAssets() {
  const errors = []
  const webDir = path.join(ROOT, 'web')
  const htmlPath = path.join(webDir, 'index.html')
  const jsPath = path.join(webDir, 'app.js')

  if (!fs.existsSync(htmlPath)) {
    errors.push('web/index.html 缺失')
  } else {
    const html = fs.readFileSync(htmlPath, 'utf8')
    const marks = html.split('/*__BOOT_JSON__*/null').length - 1
    if (marks !== 1) errors.push(`web/index.html 里 /*__BOOT_JSON__*/null 出现 ${marks} 次，必须恰好 1 次`)
    if (!html.includes('<script src="/app.js">')) errors.push('web/index.html 缺 <script src="/app.js">')
  }

  if (!fs.existsSync(jsPath)) {
    errors.push('web/app.js 缺失')
  } else {
    const js = fs.readFileSync(jsPath, 'utf8')
    for (const needle of ['window.__BOOT__', 'ofr-theme', 'probeFromLogs']) {
      if (!js.includes(needle)) errors.push(`web/app.js 缺 ${needle}`)
    }
    if (js.includes('clampPortBase')) errors.push('web/app.js 仍含 clampPortBase（零端口架构应已删除）')
  }

  return errors
}

/**
 * @returns {Promise<{errors:number, warns:number, lines:string[]}>}
 */
export async function checkGo() {
  const errors = []
  const dirs = pkgDirs()
  const map = layerMap()

  for (const d of dirs) {
    if (!map.has(d)) errors.push(`internal/${d} 未在 LAYERS 中归层`)
  }
  for (const d of dirs) {
    if (!map.has(d)) continue
    for (const dep of importsOf(d)) {
      if (!map.has(dep)) { errors.push(`internal/${d} 导入了不存在的 internal/${dep}`); continue }
      const from = map.get(d)
      const to = map.get(dep)
      if (to > from) errors.push(`internal/${d}(L${from}) 不得导入 internal/${dep}(L${to})`)
    }
  }

  // Cycle detection. A cycle always implies a layer violation, but naming it
  // makes the failure legible instead of leaving the reader to infer it.
  const edges = new Map(dirs.filter(d => map.has(d)).map(d => [d, [...importsOf(d)].filter(x => map.has(x))]))
  const state = new Map()
  const stack = []
  const visit = n => {
    const s = state.get(n)
    if (s === 'done') return
    if (s === 'open') { errors.push(`导入环: ${[...stack, n].join(' -> ')}`); return }
    state.set(n, 'open'); stack.push(n)
    for (const m of edges.get(n) ?? []) visit(m)
    stack.pop(); state.set(n, 'done')
  }
  for (const d of edges.keys()) visit(d)

  // SPDX header, per constraint 6.
  for (const d of dirs) {
    for (const f of fs.readdirSync(path.join(ROOT, 'internal', d))) {
      if (!f.endsWith('.go') || f.endsWith('_test.go')) continue
      const src = fs.readFileSync(path.join(ROOT, 'internal', d, f), 'utf8')
      if (!src.includes('SPDX-License-Identifier: GPL-3.0-or-later')) {
        errors.push(`internal/${d}/${f} 缺 SPDX 头 (// SPDX-License-Identifier: GPL-3.0-or-later)`)
      }
    }
  }

  for (const e of checkWebAssets()) errors.push(e)

  let vet = ''
  // O19:vet 带上与构建/测试同一组 TAGS —— with_* 标签决定 include 注册哪些
  // 协议代码,不带标签的 vet 从未编译过那些文件(与 go-test.mjs 头注释的
  // 「测试必须同代码形状」同一理由)。
  try { vet = (await runGo(['vet', '-tags', TAGS, './...'], { env: goEnv() })).stdout } catch (e) { vet = String(e.stdout ?? e.message ?? e) }
  for (const line of String(vet).split('\n')) if (line.trim()) errors.push(`go vet: ${line.trim()}`)

  return { errors: errors.length, warns: 0, lines: errors }
}

import { fileURLToPath } from 'node:url'

/**
 * CLI 入口。同另两个脚本：`npm run check` 与 CI 跑的是 `node scripts/check-go.mjs`，
 * 没有 main 时它加载完模块就退出、恒退 0，分层与 SPDX 检查等于没跑。
 */
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { errors, warns, lines } = await checkGo()
  for (const line of lines) console.log(line)
  console.log(`${errors} 个 error，${warns} 个 warn`)
  if (errors > 0) process.exitCode = 1
}
