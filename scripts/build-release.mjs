// 一键打包发布 zip：把运行所需的全部组件装配进一个文件夹并压缩。
// 产物：release/Opencode-FreeRouter-v<版本>-windows-x64.zip
// 解压后双击 Opencode-FreeRouter.exe 即可用，不需要安装 Node 或 sing-box。
//
// 组件清单：
//   Opencode-FreeRouter.exe   Go 托盘壳（已嵌图标）
//   runtime/node.exe          Node 运行时（取自本机正在使用的 node，优先于 PATH）
//   src/                      网关源码（ESM，含 limits.js 的 models.dev 限额同步）
//   node_modules/undici       唯一第三方依赖（零二级依赖）
//   node_modules/yaml         订阅 YAML 解析依赖
//   bin/sing-box.exe          sing-box v1.14.0 内核
//   package.json              ESM 标记 + 元数据
//   LICENSE / NOTICE / README.md
import { cpSync, mkdirSync, rmSync, statSync, existsSync, writeFileSync, readFileSync, readdirSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const { version } = JSON.parse(readFileSync(path.join(ROOT, 'package.json'), 'utf8'))
const NAME = 'Opencode-FreeRouter'
// 时间后缀：node scripts/build-release.mjs 20260927-1205
// 不传则保持旧行为（覆盖 release/Opencode-FreeRouter）。传时间戳则新旧共存，
// 避免运行中的旧 exe 锁住目录导致 rmSync EPERM。
const stampArg = (process.argv[2] ?? '').trim().replace(/[^0-9-]/g, '')
const suffix = stampArg ? `-${stampArg}` : ''
const stage = path.join(ROOT, 'release', `${NAME}${suffix}`)
const zipPath = path.join(ROOT, 'release', `${NAME}-v${version}${suffix}-windows-x64.zip`)

console.log('== 打包 Opencode-FreeRouter v' + version + ' ==')
rmSync(stage, { recursive: true, force: true })
mkdirSync(stage, { recursive: true })

// 1. 托盘 exe（发布脚本自行构建，含图标资源；不再要求根目录预置 exe）
execFileSync('go', ['build', '-trimpath', '-ldflags=-s -w -H=windowsgui', '-o', path.join(stage, `${NAME}.exe`), '.'], { cwd: path.join(ROOT, 'launcher'), env: { ...process.env, CGO_ENABLED: '0' } })

// 2. Node 运行时（取当前进程的 node，保证与开发验证同版本）
mkdirSync(path.join(stage, 'runtime'), { recursive: true })
cpSync(process.execPath, path.join(stage, 'runtime', 'node.exe'))

// 3. 源码 + 依赖 + 内核 + 文档
cpSync(path.join(ROOT, 'src'), path.join(stage, 'src'), { recursive: true })
cpSync(path.join(ROOT, 'node_modules', 'undici'), path.join(stage, 'node_modules', 'undici'), { recursive: true })
cpSync(path.join(ROOT, 'node_modules', 'yaml'), path.join(stage, 'node_modules', 'yaml'), { recursive: true })
cpSync(path.join(ROOT, 'bin', 'sing-box.exe'), path.join(stage, 'bin', 'sing-box.exe'), { recursive: false })
for (const f of ['package.json', 'LICENSE', 'NOTICE', 'README.md']) {
  cpSync(path.join(ROOT, f), path.join(stage, f))
}

// 4. 首次运行说明（解压即看的文字）
writeFileSync(path.join(stage, '运行说明.txt'), [
  'Opencode-FreeRouter v' + version,
  '',
  '1. 双击 Opencode-FreeRouter.exe（托盘出现图标）',
  '   * 首次运行如弹出 SmartScreen 提示：点"更多信息"→"仍要运行"（程序未做代码签名）',
  '2. 托盘菜单 → 打开面板（http://127.0.0.1:3458）',
  '3. 面板"接入信息"里有 API 地址和 Key，填进你的 agent 工具；',
  '   模型 id 在"免费模型"区一键复制',
  '4. 出口国家在"设置"里选，保存后等首轮探测（约 1 分钟）',
  '',
  '卸载：退出托盘程序后删除本文件夹即可（数据都在 data/ 子目录）',
].join('\r\n'))

// 5. 完整性校验
const required = [
  `${NAME}.exe`, 'runtime/node.exe', 'bin/sing-box.exe',
  'src/index.js', 'src/panel.js', 'src/engine.js', 'src/singbox.js', 'src/upstream.js',
  'src/catalog.js', 'src/limits.js', 'src/effort.js',
  'node_modules/undici/index.js', 'node_modules/yaml/dist/index.js', 'package.json', 'LICENSE', 'NOTICE', 'README.md', '运行说明.txt',
]
for (const rel of required) {
  const p = path.join(stage, rel)
  if (!existsSync(p) || statSync(p).size === 0) {
    console.error('校验失败：' + rel)
    process.exit(1)
  }
}
console.log('完整性校验通过（' + required.length + ' 项）')

// 6. 压缩
rmSync(zipPath, { force: true })
execFileSync('powershell', ['-Command', `Compress-Archive -Force -Path "${stage}" -DestinationPath "${zipPath}"`])
const dirSize = p => [...readdirSync(p, { recursive: true })].reduce((n, f) => {
  try { return n + statSync(path.join(p, f)).size } catch { return n }
}, 0)
const mb = n => (n / 1048576).toFixed(1) + 'MB'
console.log('目录：', stage, '(' + mb(dirSize(stage)) + ')')
console.log('发布包：', zipPath, '(' + mb(statSync(zipPath).size) + ')')
