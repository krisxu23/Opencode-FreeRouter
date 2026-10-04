# 审计笔记

本文记录审计过程中的事实更正与验证结果，纯文档、不随产品发版。

## 2026-10-04：-race 覆盖的更正与实测

**更正**：提交 e5a7aaf（v1.2.3）的提交信息写了「CI(Linux) 的 -race 兜底并发面」，这句不成立
——`release.yml` 的 gates/release 两个作业都跑在 `windows-latest`，且 `scripts/go-test.mjs`
没有传 `-race`。CI 从未跑过竞态检测。

**实测（本机，GCC 16.2.0 + cgo）**：此前「本机没有 gcc、-race 起不来」的说法同样不成立。
2026-10-04 用 `go test -race -tags <构建 TAGS>` 实测：

- **23 个纯包全部通过**：check、errors、persistence、logger、tracelog、parse、upstream、
  catalog、limits、messages、stream、httpclient、gate、effort、stats、sub、registry、
  nodeprobe、engine、adapter、health、forward、panel。
- **internal/app、internal/sbx、cmd/freerouter 无法构建 race 版**，原因是第三方依赖
  `github.com/database64128/tfo-go/v2` 对 `net.(*netFD).init` 的 linkname 引用与 race
  插桩不兼容（`link: ... invalid reference to net.(*netFD).init`），属上游限制而非本仓
  代码问题。这三个包的并发面（pendingWG、lanes 的调用侧、关停顺序）依赖确定性逻辑测试
  与纯包的 -race 间接覆盖。

**建议**：若未来想让 CI 真正跑 -race，需要 Linux 作业 + 纯包子集（不含 app/sbx/cmd），
或等 tfo-go 上游修复 linkname。在 CI 加一道 `go test -race`（Linux、纯包、不计入发布门）
是低成本高价值的兜底。

## 各轮审计摘要

- v1.1.0（d23a0d0）：第一轮全项目审查，3 Critical + ~20 Warning + 全部优化项。
- v1.2.0（366e082）：第二轮对抗审查。
- v1.2.1（4c6def7）：catalog 改为「上游实时列表为 id 唯一来源」。
- v1.2.2（ba11c08）：magpie 通用件移植（出口车道闸门 lanes + busy 计数 + 空 reasoning 守卫）。
- v1.2.3（e5a7aaf）：第三轮审计（5 名审计员），7 高危 + 8 中危 + 12 卫生项。
- v1.2.4：第五轮审查（4 名审计员 + 运行实况核对 + 性能专项 + 竞态实测）。修复清单见
  对应提交信息；性能基准：BenchmarkPickOverFullPool 941µs/3412 allocs → 759µs/1712 allocs。
