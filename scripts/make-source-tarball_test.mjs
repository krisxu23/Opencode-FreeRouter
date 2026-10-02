// scripts/make-source-tarball.mjs 的判据测试（计划任务 29 步骤 6 的 4 条 + 3 条）。
//
// 这几条的共同点：它们不是在测「函数返回什么」，而是在测「我们分发出去的那个
// 文件里有没有东西」。GPL 的合规是可被第三方机器验证的，所以测试也必须真的把
// tar 解开来看，而不是信打包函数的返回值。
import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import { test } from 'node:test'
import { execFileSync } from 'node:child_process'
import {
  classifyLicense, collectLicenses, makeTarball, sourceFiles, LICENSE_DIR,
} from './make-source-tarball.mjs'
import { ROOT } from './go-build.mjs'

const TMP = path.join(ROOT, 'difftest-tmp', 'srcpkg-test')
// 测试产物绝不写进 release/：那里放的是要分发的那个包，混进一个
// `freerouter-test-source.tar.gz` 就是等着哪天错发。
const OUT = path.join(ROOT, 'difftest-tmp', 'srcpkg-test-out')

/** 打一个测试专用的包，解到 TMP，返回 tar 条目列表与解包根。 */
async function buildAndExtract() {
  fs.rmSync(TMP, { recursive: true, force: true })
  const pkg = await makeTarball({ version: 'test', tmp: path.join(ROOT, 'difftest-tmp'), out: OUT })
  fs.mkdirSync(TMP, { recursive: true })
  // 相对路径 + cwd：MSYS 的 GNU tar 会把 Windows 绝对路径当文件名。
  execFileSync('tar', ['-xzf', path.relative(TMP, pkg.file).split(path.sep).join('/'), '-C', '.'],
    { cwd: TMP })
  const entries = execFileSync('tar', ['-tzf', path.relative(ROOT, pkg.file).split(path.sep).join('/')],
    { cwd: ROOT, maxBuffer: 32 * 1024 * 1024 }).toString().split('\n').filter(Boolean)
  return { entries, root: path.join(TMP, 'freerouter-test') }
}

function walkFiles(dir, prefix = '') {
  const out = []
  if (!fs.existsSync(dir)) return out
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const rel = prefix ? `${prefix}/${e.name}` : e.name
    if (e.isDirectory()) out.push(...walkFiles(path.join(dir, e.name), rel))
    else out.push(rel)
  }
  return out
}

test('源码包里每个 .go 都在，且一个不多一个不少', async () => {
  const { entries, root } = await buildAndExtract()
  const inTree = walkFiles(ROOT)
    .filter(f => f.endsWith('.go'))
    // 根目录现在就是 Go 模块，所以要手动剔掉「不在模块里」的那几棵：archive/ 是
    // 冻结的 JS 版（它连自己的 Go 托盘壳 launcher/main.go 一起归档了）、
    // difftest-tmp/ 是工装临时产物、data/ 与 release/ 是运行时状态和产物。
    .filter(f => !/^(node_modules|data|archive|release|difftest-tmp)\//.test(f))
    .sort()
  const inPkg = walkFiles(root)
    .filter(f => f.endsWith('.go'))
    .sort()
  assert.equal(inPkg.length, inTree.length, `包内 ${inPkg.length} 个 .go ≠ 源码树 ${inTree.length} 个`)
  const missing = inTree.filter(f => !inPkg.includes(f))
  assert.deepEqual(missing, [], `包里缺这些 .go: ${missing.join(', ')}`)
})

test('包内没有 data/、没有 .exe、没有 node_modules/', async () => {
  const { entries } = await buildAndExtract()
  const bad = entries.filter(e =>
    /(^|\/)data\//.test(e) || /\.exe$/i.test(e) || /(^|\/)node_modules\//.test(e) ||
    /(^|\/)archive\//.test(e))
  assert.deepEqual(bad, [], `包里出现了不该出现的条目: ${bad.join(', ')}`)
})

test('包内任何文件都不含现网 forward key', async () => {
  const { root } = await buildAndExtract()
  // 从真实数据目录取 key；测完不比字符串本身，只比「包里的字节有没有出现过它」。
  const settingsFile = path.join(ROOT, 'data', 'settings.json')
  const keys = new Set()
  if (fs.existsSync(settingsFile)) {
    const s = JSON.parse(fs.readFileSync(settingsFile, 'utf8'))
    if (s.forwardKey) keys.add(String(s.forwardKey))
    if (s.gateway?.forwardKey) keys.add(String(s.gateway.forwardKey))
  }
  assert.ok(keys.size > 0, 'data/settings.json 里没有 forwardKey，这条测试失去意义')
  const leaked = []
  for (const rel of walkFiles(root)) {
    const buf = fs.readFileSync(path.join(root, rel))
    for (const k of keys) if (buf.includes(k)) leaked.push(rel)
  }
  // 报错信息只带文件名与 key 的前缀，绝不把 key 打进日志。
  assert.deepEqual(leaked, [], `以下文件泄露了 forwardKey(${[...keys][0].slice(0, 4)}…): ${leaked.join(', ')}`)
})

test('每个依赖都有许可证全文，且 INDEX.md 里没有 UNKNOWN', async () => {
  const { rows, unknown } = await collectLicenses()
  assert.equal(unknown.length, 0, `未识别许可证: ${unknown.map(u => u.path).join(', ')}`)
  const missing = []
  for (const r of rows) {
    const abs = path.join(ROOT, r.file)
    if (!fs.existsSync(abs) || fs.statSync(abs).size === 0) missing.push(r.file)
  }
  assert.deepEqual(missing, [], `清单里有 ${missing.length} 个模块找不到许可证全文`)
  const index = fs.readFileSync(path.join(LICENSE_DIR, 'INDEX.md'), 'utf8')
  assert.ok(!index.includes('| UNKNOWN |'), 'INDEX.md 仍有 UNKNOWN 行')
  assert.equal(rows.length, 70, '依赖模块数变了 —— 说明构建标签或依赖树动过，需复核 NOTICE')
})

test('许可证类型判定：引用式 GPL、MPL 不被误判 LGPL、全文 GPL', async () => {
  const sagernet = fs.readFileSync(
    path.join(LICENSE_DIR, 'github.com/sagernet/sing-box@v1.14.0/LICENSE'), 'utf8')
  assert.equal(classifyLicense(sagernet), 'GPL-3.0-or-later')
  const yamux = fs.readFileSync(path.join(LICENSE_DIR, 'github.com/hashicorp/yamux@v0.1.2/LICENSE'), 'utf8')
  // MPL-2.0 的正文里点名了 GNU Lesser GPL，早先的子串判法把它标成了 LGPL。
  assert.equal(classifyLicense(yamux), 'MPL-2.0')
  const ours = fs.readFileSync(path.join(ROOT, 'LICENSE'), 'utf8')
  assert.equal(classifyLicense(ours), 'GPL-3.0-or-later')
  assert.ok(Buffer.byteLength(ours) > 30000, 'LICENSE 必须含 GPL-3.0 全文，不能是引用式声明')
})

test('打包可复跑：同一棵树打两次，字节级相同', async () => {
  const a = await makeTarball({ version: 'test-a', tmp: path.join(ROOT, 'difftest-tmp'), out: OUT })
  const ba = fs.readFileSync(a.file)
  const b = await makeTarball({ version: 'test-a', tmp: path.join(ROOT, 'difftest-tmp'), out: OUT })
  const bb = fs.readFileSync(b.file)
  assert.ok(ba.equals(bb), '两次打包字节不同 —— 时间戳/顺序/属主没有归零')
  for (const f of [a.file, b.file]) fs.rmSync(f, { force: true })
})

test('打包清单里的文件都真实存在，且不含产物', () => {
  const list = sourceFiles()
  assert.ok(list.length > 100, `清单只有 ${list.length} 个文件，像是漏了目录`)
  for (const [abs] of list) assert.ok(fs.existsSync(abs), `清单里有不存在的文件: ${abs}`)
  assert.ok(list.every(([, rel]) => !/\.exe$/i.test(rel)))
})
