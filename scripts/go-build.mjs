/**
 * The single build entry point for the Go rewrite.
 *
 * Every part of the linker invocation is pinned here because none of it is
 * discoverable from `go build` itself:
 *
 *   - `-checklinkname=0` is MANDATORY. Without it the link fails with
 *     `link: github.com/database64128/tfo-go/v2: invalid reference to net.(*netFD).init`.
 *     sing-box's own cmd/internal/build_shared/flags.go:9 passes the same flag,
 *     so this is upstream's contract, not a local workaround.
 *   - `GOPROXY=off` is MANDATORY. proxy.golang.org times out in this
 *     environment (measured 8s) while the module cache is warm, so a single
 *     online lookup turns a 40-second build into a hang.
 *   - the `with_*` tags register sing-box's protocol build tags. Dropping one
 *     silently removes protocol support instead of failing.
 *   - `-X runtime.godebugDefault=multipathtcp=0,tlssha1=1` mirrors
 *     build_shared/flags.go: mpTCP changes socket behaviour and tlssha1 is
 *     required by the TLS stack sing-box embeds.
 *
 * @module scripts/go-build.mjs
 */

import { execFile } from 'node:child_process'
import { existsSync, statSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

export const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
export const TAGS = 'with_quic,with_wireguard,with_utls,with_clash_api,badlinkname,tfogo_checklinkname0'
export const LDFLAGS = '-checklinkname=0 -s -w -buildid= -X runtime.godebugDefault=multipathtcp=0,tlssha1=1'

/** Offline env. Every go invocation in this project goes through here. */
export function goEnv(extra = {}) {
  return { ...process.env, GOPROXY: 'off', CGO_ENABLED: '0', GOFLAGS: '-mod=mod', ...extra }
}

/**
 * Run `go <args>` at the repo root (the Go module lives there since 2026-10-02;
 * before that it was nested under opencode-FreeRouter-Go/).
 * Rejects with stdout/stderr attached so callers can print real diagnostics
 * instead of just an exit code.
 * @param {string[]} args
 * @param {object} [options]
 * @returns {Promise<{stdout: string, stderr: string}>}
 */
export function runGo(args, options = {}) {
  return new Promise((resolve, reject) => {
    const GO = process.env.GO || 'go'
    execFile(GO, args, { cwd: ROOT, maxBuffer: 64 * 1024 * 1024, ...options }, (error, stdout, stderr) => {
      if (error) reject(Object.assign(error, { stdout, stderr }))
      else resolve({ stdout, stderr })
    })
  })
}

/**
 * Build the Go binary.
 * @param {{dest?: string, extraTags?: string}} [options] dest defaults to
 *   `<repo root>/freerouter.console.exe`
 * @returns {Promise<number>} byte size of the produced binary
 */
export async function buildGo({ dest, extraTags = '' } = {}) {
  // dest 按**调用方的 cwd**解析（`path.resolve` 的默认语义），不按 runGo 的 cwd：
  // 2026-10-02 实测过按模块根拼的写法会把产物落进 `…/opencode-FreeRouter-Go/`
  // 里再套一层，**不报错**，于是验收跑的是上一个二进制。绝对路径两种语义同义。
  //
  // 名字必须与托盘产物 `FreeRouter.exe` 真实不同：Windows 文件系统大小写不敏感，
  // 原先默认的 `freerouter.exe` 与它就是同一个文件，`npm run build` 会把 GUI 版
  // 覆盖成带控制台窗口的版本（见修正案 §8 阶段 5 第 1 条）。
  const target = path.resolve(dest ?? path.join(ROOT, 'freerouter.console.exe'))
  const tags = extraTags ? `${TAGS},${extraTags}` : TAGS
  // `./cmd/freerouter`, not `.`: task 20 moved the entry point out of the
  // module root so `app` stays the only assembly point.
  await runGo(['build', '-trimpath', '-tags', tags, '-ldflags', LDFLAGS, '-o', target, './cmd/freerouter'], { env: goEnv() })
  if (!existsSync(target) || statSync(target).size === 0) throw new Error(`go build 未产出可用产物: ${target}`)
  return statSync(target).size
}

/**
 * CLI 入口。`package.json` 的 build 脚本与 .github/workflows 都以
 * `node scripts/go-build.mjs` 调本文件。此前它只有导出函数、没有 main，于是那条
 * 命令什么都不做还退 0（2026-10-02 实测）——门禁会假绿，所以 main 必须在脚本里，
 * 而不是只留一个导给别人的函数。
 */
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const dest = process.argv[2]
  // buildGo 返回字节数、不返回路径，所以这里按它的同一套解析规则算出目标名。
  const target = path.resolve(dest ?? path.join(ROOT, 'freerouter.console.exe'))
  const bytes = await buildGo(dest ? { dest } : {})
  console.log(`freerouter: ${target} (${bytes} bytes)`)
}
