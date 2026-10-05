/**
 * GPL-3.0 合规交付：依赖许可证聚合 + 可复跑的源码包。
 *
 * 为什么这件事必须由脚本做而不是手写：sing-box 的传递依赖有几十个，手写的
 * `THIRD-PARTY-LICENSES/` 与 `NOTICE` 必然漏，而 GPL 的分发义务是第三方可验证的
 * ——漏一个就是一次违规。所以依赖清单**只从 `go list -deps ./cmd/freerouter` 拿**
 * （每一项带 Path/Version/Dir，Dir 下就是该模块的许可证全文，不用猜目录结构），
 * 打包清单**只从工作树实测**。
 *
 * 两条容易被忽略的判据：
 *   - 找不到许可证 ⇒ **打包失败并指名模块**，不允许「大概没有就跳过」。
 *   - tarball 必须可复跑：`--sort=name --mtime=@0 --owner=0 --group=0
 *     --numeric-owner --format=gnu`（GNU tar 1.35 与 bsdtar 3.8 都支持这组）；
 *     不可复现的发布包没法验收，也没法让别人核对「源码就是二进制里那份」。
 *
 * @module scripts/make-source-tarball.mjs
 */
import fs from 'node:fs'
import path from 'node:path'
import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { ROOT, TAGS, runGo, goEnv } from './go-build.mjs'

export const LICENSE_DIR = path.join(ROOT, 'THIRD-PARTY-LICENSES')
export const RELEASE_DIR = path.join(ROOT, 'release')
const NOTICE = path.join(ROOT, 'NOTICE')

// 顺序即优先级：Go 模块目录里最常见的是 LICENSE，sing-box 系是 LICENSE，少数用
// COPYING（golang.org/x 用 LICENSE.txt）。
const CANDIDATES = ['LICENSE', 'LICENSE.txt', 'LICENSE.md', 'COPYING', 'COPYING.txt']

/**
 * 从文件头判许可证类型。识别不了返回 UNKNOWN —— 但仍原样拷进去（标不出来只是
 * 文档缺陷，漏拷贝才是违规）。
 *
 * 为什么规则写得这么啰嗦：Go 生态里大多数项目**不附 GPL 全文**，只写一段
 * 「按 GNU GPL v3（或更新版本）分发」的标准引用式声明（sing-box 全家 12 个模块
 * 都是这种 700 字节左右的通知）。只看 `GNU GENERAL PUBLIC LICENSE` +
 * `Version 3, 29 June 2007` 会把它们全判成 UNKNOWN，而 GPL 的判定错了就是
 * 合规错了，所以引用式与全文式都要认，LGPL 还要排在 GPL 之前（它的全文里同时
 * 出现两个名字）。
 */
export function classifyLicense(text) {
  const head = text.slice(0, 4000)
  // 许可证文本都是 72 列硬换行的，规则若按原始字符串匹配就会在换行处漏判
  // （实测 coder/websocket 的 ISC 头就被 "…for any / purpose with or without fee"
  // 这么断开）。归一化空白后再比。
  const low = head.toLowerCase().replace(/\s+/g, ' ')
  const t0 = low.trimStart()
  // 许可证的**标题行**才是身份，正文里的名字不是：MPL-2.0 的 §1.13 定义
  // "Copious License" 时就点名了 GNU Lesser GPL，按全文子串判会把 hashicorp/yamux
  // （MPL-2.0）误判成 LGPL。反过来，引用式声明（sing-box 那种 791B 通知）里的
  // "GNU General Public License" 出现在第二句，所以这里只认**开头**是不是标题，
  // 其余交给下面的引用式规则。合规产物里的类型标错比标不出来更糟。
  if (t0.startsWith('gnu lesser general public license')) return 'LGPL'
  if (t0.startsWith('gnu general public license')) return 'GPL-3.0-or-later'
  if (low.includes('mozilla public license')) return 'MPL-2.0'
  if (low.includes('either version 3 of the license, or (at your option) any later version') ||
    low.includes('gnu general public license, either version 3') ||
    (low.includes('gnu general public license') && low.includes('any later version'))) return 'GPL-3.0-or-later'
  if (low.includes('gnu general public license') &&
    (head.includes('Version 3, 29 June 2007') || low.includes('version 3 of the gnu general public license') || low.includes('gnu general public license, version 3'))) return 'GPL-3.0-only'
  if (low.includes('apache license') && head.includes('Version 2.0')) return 'Apache-2.0'
  if (low.includes('permission is hereby granted, free of charge')) return 'MIT'
  if (low.includes('permission to use, copy, modify, and/or distribute this software for any purpose with or without fee')) return 'ISC'
  if (low.includes('permission to use, copy, modify, and distribute this software for any purpose with or without fee')) return 'ISC'
  if (low.includes('redistribution and use in source and binary forms')) return 'BSD'
  if (low.includes('this is free and unencumbered software released into the public domain')) return 'Unlicense/Public-Domain'
  if (low.includes('cc0 1.0') || low.includes('creative commons zero')) return 'CC0-1.0'
  if (low.includes('mit license')) return 'MIT'
  return 'UNKNOWN'
}

/** 解析 `go list -json` 的输出：它是**一串首尾相接的 JSON 对象**，不是数组。 */
export function parseJSONObjectStream(out) {
  const items = []
  let depth = 0, start = -1, inStr = false, esc = false
  for (let i = 0; i < out.length; i++) {
    const c = out[i]
    if (inStr) {
      if (esc) esc = false
      else if (c === '\\') esc = true
      else if (c === '"') inStr = false
      continue
    }
    if (c === '"') { inStr = true; continue }
    if (c === '{') { if (depth++ === 0) start = i }
    else if (c === '}') { if (--depth === 0) items.push(JSON.parse(out.slice(start, i + 1))) }
  }
  return items
}

/**
 * 链接进产物的模块清单。
 *
 * 这里刻意**不用** `go list -m -json all`：它给的是 MVS 图上的全部模块，其中不少
 * 从没被解出到模块缓存（计划 §7 依赖清单那种「图上即需附」的口径会直接让
 * GOPROXY=off 的打包失败：`go: module lookup disabled by GOPROXY=off`）。
 * GPL 的义务对象是「链接进这个二进制的组件」，`go list -deps ./cmd/freerouter`
 * 恰好就是这个集合（且必须带与 buildGo 同一组 tags，否则 include 的协议不同、
 * 列出来的依赖也不同）。每一项自带 Dir，无需再猜缓存目录结构。
 * @returns {Promise<Array<{Path:string,Version?:string,Dir?:string,Main?:boolean}>>}
 */
export async function listModules() {
  const { stdout } = await runGo(
    ['list', '-deps', '-tags', TAGS, '-json', './cmd/freerouter'], { env: goEnv() })
  const seen = new Map()
  for (const pkg of parseJSONObjectStream(stdout)) {
    const m = pkg.Module
    if (!m || !m.Path || m.Main) continue
    if (!seen.has(m.Path)) seen.set(m.Path, m)
  }
  return [...seen.values()]
}

function findLicenseFile(dir) {
  for (const name of CANDIDATES) {
    const p = path.join(dir, name)
    if (fs.existsSync(p) && fs.statSync(p).size > 0) return p
  }
  return null
}

/**
 * 生成 `THIRD-PARTY-LICENSES/<module>@<version>/LICENSE` + `INDEX.md`。
 * 原样拷贝，一个字节都不改（许可证文本自己就是法律文件）。
 * @returns {Promise<{rows:object[], unknown:object[]}>}
 */
export async function collectLicenses() {
  const mods = (await listModules()).filter(m => m.Path && !m.Main)
  const rows = [], unknown = [], missing = []
  for (const m of mods) {
    if (!m.Dir) { missing.push(`${m.Path}@${m.Version ?? ''} (无 Dir，模块缓存里没解出来)`); continue }
    const src = findLicenseFile(m.Dir)
    if (!src) { missing.push(`${m.Path}@${m.Version ?? ''} (dir=${m.Dir})`); continue }
    const text = fs.readFileSync(src, 'utf8')
    const kind = classifyLicense(text)
    const destDir = path.join(LICENSE_DIR, `${m.Path}@${m.Version ?? 'local'}`)
    fs.mkdirSync(destDir, { recursive: true })
    // 保留源文件名：把 Apache 的 LICENSE.txt 改名成 LICENSE 会丢掉它自己引用的
    // 附属文件（NOTICE 之类）的相对位置。
    const dest = path.join(destDir, path.basename(src))
    // 模块缓存里的许可证文件是只读 0444，copyFile 会连属性一起带过来，于是第二
    // 次重跑就在同一个文件上 EPERM（Windows 连删除都拦）。先放权限再删再写，
    // 内容仍逐字节原样。
    if (fs.existsSync(dest)) { fs.chmodSync(dest, 0o644); fs.rmSync(dest) }
    fs.copyFileSync(src, dest)
    fs.chmodSync(dest, 0o644)
    const row = { path: m.Path, version: m.Version ?? '', kind, file: `THIRD-PARTY-LICENSES/${m.Path}@${m.Version ?? 'local'}/${path.basename(src)}`, gpl: kind.startsWith('GPL') }
    rows.push(row)
    if (kind === 'UNKNOWN') unknown.push(row)
  }
  if (missing.length) {
    throw new Error(`以下依赖找不到许可证文件，分发即违规，已中止打包：\n  ${missing.join('\n  ')}`)
  }
  rows.sort((a, b) => (a.path + a.version).localeCompare(b.path + b.version))
  writeIndex(rows, unknown)
  rewriteNotice(rows)
  return { rows, unknown }
}

/** INDEX.md 顶部必须先列 UNKNOWN：那是「还没人工确认」的债务清单，不能埋在表里。 */
function writeIndex(rows, unknown) {
  const lines = [
    '# THIRD-PARTY-LICENSES —— 由 scripts/make-source-tarball.mjs 生成，勿手改',
    '',
    `本目录共 ${rows.length} 个模块的许可证全文，原样拷贝自模块缓存目录。`,
    '本文件与这些全文都由 `npm run release` 重新生成，判据是 `go list -deps ./cmd/freerouter`（与 buildGo 同一组构建标签）。',
    '',
  ]
  if (unknown.length) {
    lines.push('## ⚠️ 许可证类型未识别（全文已原样附带，需人工确认）', '')
    for (const u of unknown) lines.push(`* ${u.path}@${u.version} → ${u.file}`)
    lines.push('')
  }
  lines.push('## 清单', '', '| 模块 | 版本 | 许可证 | 文件 | 是否 GPL |', '| --- | --- | --- | --- | --- |')
  for (const r of rows) {
    lines.push(`| ${r.path} | ${r.version} | ${r.kind} | \`${r.file}\` | ${r.gpl ? '是' : '否'} |`)
  }
  fs.mkdirSync(LICENSE_DIR, { recursive: true })
  fs.writeFileSync(path.join(LICENSE_DIR, 'INDEX.md'), lines.join('\n') + '\n')
}

/**
 * 重写 NOTICE 的依赖段。锚点式替换而不是插标记行：计划给定的 NOTICE 全文里没有
 * 「生成区」注释，硬塞标记会污染那份手写文本。
 */
function rewriteNotice(rows) {
  const text = fs.readFileSync(NOTICE, 'utf8')
  const head = '本程序链接了以下第三方组件（完整许可证见'
  const tailAnchor = '沿用与致谢：'
  const i = text.indexOf(head), j = text.indexOf(tailAnchor)
  if (i < 0 || j < 0) throw new Error('NOTICE 缺少依赖段的锚点，拒绝猜测要改哪一段')
  const counts = {}
  for (const r of rows) counts[r.kind] = (counts[r.kind] ?? 0) + 1
  const summary = Object.entries(counts).sort((a, b) => b[1] - a[1])
    .map(([k, n]) => `  ${k} × ${n}`).join('\n')
  const block = [
    head,
    'THIRD-PARTY-LICENSES/）：',
    '',
    '  github.com/sagernet/sing-box  — GPL-3.0-or-later',
    '      https://github.com/SagerNet/sing-box',
    '      sing-box 的源码以其许可证的附加条款发布：不得使用其名称或暗示',
    '      与本项目有关联。本项目因此不使用「sing-box」作为产品名。',
    '',
    `按许可证类型统计（共 ${rows.length} 个模块，逐个全文见 THIRD-PARTY-LICENSES/，`,
    '清单与类型判定见其中的 INDEX.md；本段由 scripts/make-source-tarball.mjs 生成）：',
    '',
    summary,
    '',
    '',
  ].join('\n')
  fs.writeFileSync(NOTICE, text.slice(0, i) + block + text.slice(j))
}

// Go 树扁平化到仓库根之后（2026-10-02），打包就是「走一遍根目录 + 排除清单」。
// 不再维护 include 列表：新加一个顶层目录时，include 列表会静默把它漏掉，而排除
// 清单不会 —— 排除项及理由：*.exe 与 dist/ 是产物不是源码（dist/ 是 CI 发布作业
// 放 zip 的地方，源码包混进二进制就没人核对「源码就是二进制里那份」了）；data/
// 里有用户的订阅 URL 与 forward key；node_modules/ 与 archive/ 属于已冻结的 MIT
// 作品；difftest/（fixtures 含别人订阅节点的身份与口令，.gitignore 同款裁定）与
// difftest-tmp/ 是工装目录；docs/ 整棵不要 —— 第一轮审计（d23a0d0）裁定它是
// 「重写计划、评审与验收记录，含本机路径与实例数据，不上公网」，判据测试
// （make-source-tarball_test 的 docs 断言）与 .gitignore 都按这条走。
// 初始提交 924def1 在这里留过一个 EXTRA_DIRS=docs/accept 的「单列验收报告」
// 例外，与上述裁定**直接矛盾**：CI 检出没有 docs/（gitignored）所以从未暴露，
// 但本地跑 npm run release 会把验收数据打进分发包。第七轮审计删除该例外。
const EXCLUDE_NAMES = new Set(['node_modules', 'data', 'archive', 'release', 'difftest', 'difftest-tmp', 'dist', '.git', 'docs'])
// rel 路径精确排除:scripts/diff-accept.mjs 是内部差分工装(.gitignore 裁定
// 不上公网),不能靠名字排除 —— scripts/ 整体要进包(构建契约进产物)。
const EXCLUDE_FILES = new Set(['scripts/diff-accept.mjs'])

/** 源码包的文件清单（确定性排序）。 */
export function sourceFiles() {
  const out = []
  const walk = (dir, prefix) => {
    for (const e of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      if (EXCLUDE_NAMES.has(e.name)) continue
      const abs = path.join(dir, e.name)
      const rel = prefix ? `${prefix}/${e.name}` : e.name
      if (EXCLUDE_FILES.has(rel)) continue
      if (e.isDirectory()) { walk(abs, rel); continue }
      if (e.name.endsWith('.exe')) continue
      out.push([abs, rel])
    }
  }
  walk(ROOT, '')
  out.sort((a, b) => a[1].localeCompare(b[1]))
  return out
}

function sh(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { maxBuffer: 64 * 1024 * 1024, ...opts }, (err, stdout, stderr) =>
      err ? reject(Object.assign(err, { stdout, stderr })) : resolve({ stdout, stderr }))
  })
}

/**
 * 产出 `release/freerouter-<version>-source.tar.gz`。
 * 顶层目录统一叫 `freerouter-<version>/`，解包即是一个完整源码树。
 * @returns {Promise<{file:string, files:number, bytes:number}>}
 */
export async function makeTarball({ version = readVersion(), tmp = path.join(ROOT, 'difftest-tmp'), out = RELEASE_DIR } = {}) {
  const stage = path.join(tmp, `srcpkg-${version}`)
  fs.rmSync(stage, { recursive: true, force: true })
  const top = path.join(stage, `freerouter-${version}`)
  fs.mkdirSync(top, { recursive: true })
  for (const [abs, rel] of sourceFiles()) {
    const dest = path.join(top, rel)
    fs.mkdirSync(path.dirname(dest), { recursive: true })
    fs.copyFileSync(abs, dest)
  }
  fs.mkdirSync(out, { recursive: true })
  const file = path.join(out, `freerouter-${version}-source.tar.gz`)
  // tar 的输入输出**一律给相对路径**（配合 cwd）：这台机器 PATH 里的 tar 是 MSYS
  // 的 GNU tar 1.35，把 `D:\…\out.tar.gz` 当普通文件名吃掉，报 "Cannot write:
  // Broken pipe"；换 bsdtar 又是另一套解释。相对路径两边行为一致。
  const relOut = path.relative(stage, file).split(path.sep).join('/')
  // gzip 走管道时不会把文件时间写进 gzip 头（本机实测两次产物字节级相同），
  // 所以 -cz 够用，不需要 --use-compress-program —— 那条在 bsdtar 上语义不同。
  const args = ['--format=gnu', '--sort=name', '--mtime=@0', '--owner=0', '--group=0',
    '--numeric-owner', '-czf', relOut, `freerouter-${version}`]
  await sh('tar', args, { cwd: stage })
  const bytes = fs.statSync(file).size
  fs.rmSync(stage, { recursive: true, force: true })
  return { file, files: sourceFiles().length, bytes }
}

function readVersion() {
  return JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8')).version
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const argv = process.argv.slice(2)
  if (argv[0] === '--list-only') {
    const { rows } = await collectLicenses()
    console.log(`modules with licenses: ${rows.length}`)
  } else {
    const { rows, unknown } = await collectLicenses()
    const pkg = await makeTarball()
    console.log(`THIRD-PARTY-LICENSES: ${rows.length} 个模块（UNKNOWN ${unknown.length}）`)
    console.log(`source package: ${pkg.file}`)
    console.log(`  files=${pkg.files} bytes=${pkg.bytes}`)
    if (unknown.length) process.exitCode = 2
  }
}
