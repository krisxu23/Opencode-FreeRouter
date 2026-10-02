/**
 * `go test ./...` for the Go tree, offline.
 *
 * Exists so no caller ever hand-rolls the env: forgetting GOPROXY=off turns a
 * dead network into a hang instead of a fast failure.
 *
 * tags 与 ldflags 和 buildGo 保持一致，原因有二：
 *   1. 任务 7 之后测试二进制也要链接 sing-box——`-checklinkname=0` 是链接期
 *      契约（缺了报 `invalid reference to net.(*netFD).init`），对 test 二进制
 *      同样生效，不是只有最终 exe 才需要。
 *   2. `with_*` 构建标签决定 include 注册哪些协议；测试必须测与出厂二进制
 *      同一份代码形状，否则「direct 能跑、hysteria2 没被链接」这类退化在
 *      测试阶段不可见。
 *
 * @module scripts/go-test.mjs
 */
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { runGo, goEnv, TAGS, LDFLAGS } from './go-build.mjs'

/** @returns {Promise<{stdout: string, stderr: string}>} */
export async function goTest() {
  return runGo(['test', '-tags', TAGS, '-ldflags', LDFLAGS, './...'], { env: goEnv() })
}

/**
 * CLI 入口。理由同 go-build.mjs：`npm test` 与 CI 跑的是
 * `node scripts/go-test.mjs`，没有 main 的导出函数会让那条命令假绿。
 * 失败必须反映成非 0 退出码，否则 CI 只是打印了一下日志。
 */
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { stdout } = await goTest()
    process.stdout.write(stdout)
  } catch (e) {
    process.stdout.write(String(e.stdout ?? ''))
    process.stderr.write(String(e.stderr ?? e.message ?? e) + '\n')
    process.exitCode = 1
  }
}
