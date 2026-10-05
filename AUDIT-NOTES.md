# 审计笔记

本文记录审计过程中的事实更正与验证结果，纯文档、不随产品发版。

## 2026-10-05：第七轮审查（针对 v1.4.1 的六路对抗审计）与修复

六路审查（health/pick、engine/adapter、logger/tracelog/stats、rebuild/sbx/生命周期、
forward/panel/前端、跨包不变量）对 e328c01 (v1.4.1) 的 38 文件改动做了逐行对抗审查。
主干并发正确性（快照无指针外泄、池借用生命周期、懒删双路径、状态机全转移）全部通过；
修复批次要点：

- **改名丢节点（中）**：rebuildOnce 先 Merge 后差集删，与 boot 顺序相反——订阅商改名
  一轮内节点从池里蒸发。对齐为**先删后 Merge**（TestRebuildRenameKeepsMembership）。
- **池满换血停摆（中）**：EnforceCapRanked 的 rank 缺省 = map 零值 0 = 最先挤掉，新入池
  节点永远得不到首探。两个调用点显式给中间档 3（EvictRank 的 0 档语义同步改写）。
- **unknown 行探测盲区（中）**：NoteQuota 造的 unknown 行 hot/cold/first 三档 pass 全
  不收，整进程寿命不探测不删。首探圈定改为「无行或 unknown」；unknown 行首探判死
  补标 NeverAlive。
- **413 配额闸门回归（中）**：json.Decoder 只读首值，`{...}+巨大尾巴` 绕开
  MaxBytesReader 完整烧一轮上游配额。readBody 重写为首字节窥探 + 尾随 token 检查。
- **logger 轮转失败无封顶（中高）**：退避不解封顶，AV 常驻锁定 .old.log 时 gateway.log
  可涨到写满同盘；墙钟退避在时钟回拨下永久停转。加 2× 硬封顶（停文件保 ring）+
  单调时钟退避 + Init 锁序修复（fileMu 内不再取 mu）。
- **tracelog 保险丝两处失效（中）**：跨天块漏清 consecFails；OpenFile 成功即清零导致
  「开得出写不进」（盘满）永不熔断、失败行照扣配额。清零/扣账移到 Write 成功后。
- **Prebuilt.Body 共享别名（中·潜伏）**：engine 轮首预建的 body map 被 stale-reasoning
  重放就地剥改，污染同轮全部后续 attempt（当前不可达但契约已破）。adapter 取用改
  顶层浅拷贝。
- **Pick 读锁化收益被快照税吃回（中）**：为一次 sticky 查找在 RLock 内建 8000 项
  map；命中路径仍整池拷贝。改为两段式快照 + 线性定位；rankedPool 归还长成的数组。
- **rebuild 排队谎报成功（低）**：queued 短路恒 return nil，面板把「排上了」显示成
  「刷新成功」。改 errRebuildQueued，三个调用点按「已受理」消化；返回值以**末轮**为准
  （见下节「复审追加」：跨轮 Join 会让 /api/status 与 /api/refresh 结论相反，已撤回）。
- **Shutdown 只记第一个错误（低）**：errors.Join 聚合。
- **源码包泄漏 docs/（低·发布面）**：打包器的 EXTRA_DIRS 自初始提交就把 docs/accept
  单列进分发包，与第一轮审计「docs 不上公网」的裁定及判据测试相互矛盾——判据测试
  不在 CI 里跑，红了 7 个版本没人看见。删除该例外（验收数据留本地，发布面从此干净）。
- 其余：面板 help 对 180000 出厂默认说谎、keyBufPool 归还清零、argsSB 物化后断奶、
  NoteTtft 接受 0（与 stats 口径统一）、CSP frame-ancestors 收 none、engine 三处
  承重注释同步、幽灵集合 providerRetryPolicy 注释删除、check-go 新增 gofmt 闸
  （本轮 gofmt -l 实际抓到 7 个未格式化文件，「CI 从未检查过 gofmt」被证实）。

**事实更正**：v1.4.1 的提交信息写「gofmt 干净」，实际 e328c01 引入过对齐回归且
check-go 没有 gofmt 闸——上一轮的验证声明不实（只跑了 build/vet/test）。现在
check-go.mjs 有闸，make-source-tarball_test 7/7 通过。

**race 覆盖缺口（仍未闭合）**：本 agent 环境无 gcc/cgo，-race 跑不了；
commitPickSweep 删除循环与 fileMu 拆分新增了并发用例（sweep 非空、Recent×write
交错），但**未经检测器执行**。hermes 环境有 GCC 16.2.0（见下节），其 23 包 -race
通过是对 v1.3.x 代码的覆盖。v1.4.1/v1.5.0 的锁模型改动需要在下一次有 cgo 的环境补跑。

## 2026-10-05（v1.5.1 热修）：测试桩的「快速失败路径」把 CI 打成红

**症状**：bf0c52f（1.5.0）推送后 release 工作流 gates 作业在 `Go tests` 一步红
（`Process completed with exit code 1.`），Build/Smoke/release 全 skipped ⇒ **v1.5.0
没有发布**。CI 日志读不到（`gh` 不在 PATH、未鉴权的 job logs 接口一律 403），只能靠
本地复现：`go clean -testcache` 后跑 CI 同一条命令，复现
`--- FAIL: TestIncompleteIsNotDead (0.01s)`。

**根因**：测试桩 `stubbornConn.Read`（internal/nodeprobe/nodeprobe_test.go）在
`len(p) < len(stubbornHead)`（64 字节）时 **返回 error**。这条「防御」是上一轮为了
治另一个 flake（`copy(p,…)` 不看 p 长度 → Peek(1) 只取走 1 字节 → 残缺头）加的，
方向对但落点错：一旦 Transport 某次用小 buf 读，echo 请求就在**毫秒级合法失败**，
按设计「echo 合法失败回 alive」→ 探针给 alive，而用例断言 unknown ⇒ 0.01s 内 FATAL。
时长证据：失败包 10.163s vs 全绿 15.75s，正好差一个 5.60s 的兜底等待 —— 说明它压根
没等预算。本地十次未必复现（8 核），CI 2 核必现。

**修法**：桩改成**流式交付**假头（`sent int` 计数，要多少给多少，头交完才 `select{}`），
Read 只有 (n>0, nil) 与「永不返回」两种可能，不再有第三条错误路径。新增
`TestStubbornConnDeliversHeadAcrossPartialReads`（1 字节逐字节读满 + 3/4KB 混合形状 +
头交完后必须阻塞）直接钉住该契约；`TestIncompleteIsNotDead` 的失败信息补上 echo 拨号
次数与 LatencyMS/Incomplete/ExitIP 诊断。

**版本决策**：发布契约要求「动了产品路径就必须在同一次 push 里让 version 前进」，
bf0c52f 已把版本用到 1.5.0，再改 internal/** 只能继续前进 ⇒ 本版为 **v1.5.1**，
**v1.5.0 永远不会发布**（1.5.0 的代码内容除这条测试桩外与 1.5.1 相同）。force-push
不回退版本号也不行：gates 读的 `github.event.before` 仍是 bf0c52f 的 1.5.0。

## 2026-10-05：第七轮复审追加（两路对抗复审发现的 6 项）

对上一节同一批未提交改动再做两路只读对抗复审（app/engine/tracelog、pick/forward/logger），
发现 4 个真问题，均已修复并配**变异验证过**的回归用例（把修复还原成旧形状，用例必红）：

- **pick 两段式表/行不同刻（中）**：第一段取聚合表（busy/busyTbl/quotaIps）、第二段才取
  逐行数据，间隙无上界（写者持整表锁时第二段 RLock 全程阻塞）。间隙里节点重探换 exitIP、
  或旧 IP 被并发 NoteExitBusy/NoteQuota 推过阈值，rankNode 就用旧表算新行，把「正忙、
  已被限流」的节点算成零负载而赢下分组——硬不变量没破，查不出来。修法：无 sticky 的请求
  在 `pickSnapshot` 内**一次取全**（单次 RLock）；sticky 未定案时 `fillPoolSnapshot`
  **整体重取**（表+行+sweep 列表同刻同源），不再把第二段拼在第一段上。
  顺带消掉一个静默漏子：旧 fill 靠 `node.Tag == skip` 跳过已拷节点，而 sticky 不在池里时
  skip 是零值 ""，池中空 tag 的节点被一起跳过、从候选里永久消失。
  用例：TestPickSnapshotWithoutStickyTakesTheWholePoolAtOnce、
  TestFillPoolSnapshotKeepsEveryTagExactlyOnce（两处变异均验证）。
- **logger 封顶期源文件被删后永久停摆（中）**：杀软 quarantine 的动作就是删文件，而这正是
  硬封顶要对付的场景。旧实现只认「Rename 成功」为恢复判据，源不存在时 Rename 永远 ENOENT
  ⇒ capped 永为 true，而能重建文件的 O_CREATE 路径只在 recovered 时到达 ⇒ 文件日志永久
  停摆，只剩每小时一行内存 ring（`size>>20` 的告警数字也不再变化）。修法：两处 Rename 失败
  都特判 `errors.Is(err, os.ErrNotExist)` → 归零放开，由 O_CREATE 重建。
  另：恢复后 OpenFile 失败过去完全静默，现在按小时节流打一行 Warn（新 lastOpenWarn 字段）。
  用例：TestQuarantinedLogFileIsRebuiltWhileCapped（变异验证）。
- **Rebuild 返回值与落盘状态结论相反（中）**：各轮错误 Join 累积，于是「首轮失败 + 补跑轮
  成功」时 Rebuild 抛错，而 setRebuildResult 已被补跑轮覆写成 OK——/api/status 显示上次
  重建成功、/api/refresh 同一时刻 500 红字。轮换语义下补跑轮重做了同一件事，返回值必须与
  状态记录同源，改回**末轮为准**；前轮失败不静默（noteSubFailure 每失败轮已打 Warn，另补
  一条「已被排队补跑覆盖」的 Warn）。附带修掉调度循环的假 Warn 与面板红字。
  另一个诚实用例：ctx 取消时排队的一轮被丢弃，过去仍由调用方按「已排队补跑」处理——
  现在返回 `errors.Join(lastErr, ctx.Err())`，不再谎报受理。
- **coldPass 落盘挂在 sync 成功上（中）**：候选集来自内存 registry，被删 tag 当场离开候选，
  下一轮 coldPass 的 deletedTags **必然为空**（不是「必然非空」）⇒ Flush/Persist 永不重试；
  订阅长期全挂时 rebuildOnce 也不跑，重启后这批已删节点从盘上复活继续占位。修法：落盘移出
  成功分支，无条件执行（Sync 失败只影响 noteEgressChanged）。
- **tracelog 回拨可清零当日配额账（低）**：`day != lastPruneDay` 让一条历史 At 把
  lastPruneDay 拉回旧日期并重置 dayBytes/writeOff/consecFails——在两天之间交替的 At 能把
  32MB/天 的账反复清零（写的还是不同日名的文件，连撞限都不发生）。改为只在日期**前进**时
  轮转重置；历史日的记录照旧写自己那天的文件、字节仍记当前轮转日账上。
  用例：TestBackdatedRowDoesNotResetTheDayLedger（变异验证）。
- **forward 裸 Read 把 (0,nil) 判成 400（低）**：(0,nil) 是 io.Reader 的合法返回值（「什么
  都没发生，请重试」，不是 EOF），裸 Read 只调一次就拒——只有自定义 body 包装器（限速/
  解密/审计中间件）会这样返回。改用 io.ReadFull，与旧 io.ReadAll 版同语义。
  用例：TestBodyReaderReturningZeroNilIsRetriedNotRejected（+ 空体语义守卫用例）。

**复审确认无恙**（不再重复修复）：rebuild 先删后 Merge 的 pruned/present、静默哨兵在
main.go 与 9 处测试零命中、probe 落盘锁序（registry.flushMu 不在 r.mu 内取）、
tracelog 无死锁无共 fd 与 32MiB bound、app Shutdown 的 sync.Once happens-before、
MaxBytesReader 的 413/400 两形状、keyBufPool 双清零无副作用、logger 封顶递归深度有界。

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
- v1.4.1（e328c01）：第六轮迭代（hermes 产出的 38 文件改动）——性能专项、错误分类 5xx
  护栏、健康感知淘汰、订阅与运维补强。该提交信息声称 gofmt 干净，实际不实（见第七轮）。
- v1.5.0：第七轮六路对抗审计 + 两路复审（共 8 名审查员）。修复清单见上两节；基准
  BenchmarkPickOverFullPool 1.62ms/892KB/20 allocs → 990µs/434KB/16 allocs。
