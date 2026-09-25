// 下载 sing-box v1.14.0 官方二进制到 bin/（全局 AGENTS.md 加速链优先，失败回退直连）
import { mkdirSync, createWriteStream, renameSync, existsSync, rmSync } from 'node:fs'
import { pipeline } from 'node:stream/promises'
import { execFileSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const V = '1.14.0'
const asset = `https://github.com/SagerNet/sing-box/releases/download/v${V}/sing-box-${V}-windows-amd64.zip`
const bases = ['https://github.fxxk.dedyn.io/', 'https://gh-proxy.com/', '']
const binDir = path.join(ROOT, 'bin')
const exe = path.join(binDir, 'sing-box.exe')

if (existsSync(exe)) {
  console.log('sing-box 已存在:', execFileSync(exe, ['version']).toString().split('\n')[0])
  process.exit(0)
}
mkdirSync(binDir, { recursive: true })
for (const base of bases) {
  try {
    console.log(`通道: ${base || '直连'}`)
    const r = await fetch(`${base}${asset}`, { redirect: 'follow' })
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    await pipeline(r.body, createWriteStream(path.join(binDir, 'sb.zip')))
    execFileSync('powershell', ['-Command', `Expand-Archive -Force "${path.join(binDir, 'sb.zip')}" "${binDir}"`])
    renameSync(path.join(binDir, `sing-box-${V}-windows-amd64`, 'sing-box.exe'), exe)
    rmSync(path.join(binDir, 'sb.zip'), { force: true })
    rmSync(path.join(binDir, `sing-box-${V}-windows-amd64`), { recursive: true, force: true })
    console.log('OK:', execFileSync(exe, ['version']).toString().split('\n')[0])
    process.exit(0)
  } catch (e) {
    console.error(`通道 ${base || '直连'} 失败: ${e.message}`)
  }
}
process.exit(1)
