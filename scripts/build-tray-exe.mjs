/**
 * Build the tray executable (windowsgui) — the gateway IS the exe.
 *
 * Task 25's deliverable replaces scripts/build-exe.mjs's role for the Go
 * rewrite: build-exe.mjs still builds the JS launcher (it spawns node), while
 * this script links sing-box into a single FreeRouter.exe with no console
 * window.
 *
 * 构建细节一律复用 scripts/go-build.mjs 的导出（ROOT/TAGS/LDFLAGS/goEnv/
 * runGo），不重复定义 flags —— `-checklinkname=0` 与 with_* 标签是链接期契约，
 * 两份定义迟早漂移。从这里继承的只有 build-exe.mjs 的两条真知识：
 *   1. 坚持异步 execFile：execFileSync 在部分环境下创建管道直接 EBUSY
 *      （实测 bash / PowerShell、沙箱开关、node 22 与 24 全部复现）；
 *   2. GO = process.env.GO || 'go'：Git Bash 会把 PATH 里 `C:/Go/bin` 的
 *      冒号当分隔符，届时子进程找不到 go。
 *
 * 产物固定在仓库根的 FreeRouter.exe（用户指定的交付位置，修正案 §4 任务 25）。
 * go-build.mjs 的常规产物叫 freerouter.console.exe：文件系统大小写不敏感，
 * 两个名字只差大小写的话就是同一个文件，构建会互相覆盖。
 *
 * @module scripts/build-tray-exe.mjs
 */

import { existsSync, statSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { ROOT, TAGS, LDFLAGS, goEnv, runGo } from './go-build.mjs'

const NAME = 'FreeRouter'

/**
 * Build the tray exe with a hidden console window.
 * @returns {Promise<number>} byte size of the produced exe
 */
export async function buildTrayExe() {
  const dest = path.join(ROOT, `${NAME}.exe`)
  // `-H=windowsgui` 是托盘版与控制台版唯一的差异：无控制台窗口。
  // 其余 flags（tags / ldflags / 离线 env）与 go-build.mjs 完全同源。
  await runGo(
    ['build', '-trimpath', '-tags', TAGS, '-ldflags', `${LDFLAGS} -H=windowsgui`, '-o', dest, './cmd/freerouter'],
    { env: goEnv() },
  )
  if (!existsSync(dest) || statSync(dest).size === 0) throw new Error(`go build 未产出可用的 exe: ${dest}`)
  return statSync(dest).size
}

async function main() {
  const dest = path.join(ROOT, `${NAME}.exe`)
  console.log(`== 构建 ${NAME}.exe（托盘版，windowsgui）到仓库根 ==`)
  const bytes = await buildTrayExe()
  console.log(`OK: ${dest} (${bytes} bytes)`)
}

if (process.argv[1] && fileURLToPath(import.meta.url) === path.resolve(process.argv[1])) {
  await main()
}
