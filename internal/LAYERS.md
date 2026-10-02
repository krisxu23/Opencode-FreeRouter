# 包分层

`scripts/check-go.mjs` 强制三条：层 n 的包可导入层 ≤ n 的包；每个包必须归层；不得有导入环。
同层之间不互相 import —— 需要方向时把消费方放到更高的层。

| 层 | 包 | 为什么在这层 |
| --- | --- | --- |
| 0 | `check` | 词法常量（失败码、地区名、数值常量），无任何内部依赖 |
| 0 | `errors` | 上游失败的分类（照抄 `src/errors.js`），只依赖 `check` |
| 0 | `persistence` | 原子写 + JSON 读写 + Store，只用标准库 |
| 0 | `logger` | 环形缓冲 + 追加文件 + 轮转，只用标准库 |
| 0 | `tracelog` | 路由决策 JSONL，只用标准库 |
| 0 | `parse` | 节点链接解析（11 种协议）、指纹、地址判定，无 I/O |
| 0 | `upstream` | 上游指纹表、端点与请求头（纯标准库；`catalog.Model.Wire` 引用其 `Wire` 类型，故必须在 catalog 之下） |
| 1 | `catalog` `limits` `messages` `stream` `httpclient` `gate` `effort` `stats` | 需要 L0 的词法/解析/IO 原语，但彼此不互相依赖 |
| 2 | `sub` `registry` `nodeprobe` `sbx` | 节点池与代理层，需要 L1 的客户端；四个包互不依赖 |
| 3 | `health` `adapter` | 调度与协议适配，需要节点池 |
| 4 | `engine` | 轮换编排 |
| 5 | `panel` `app` `forward` `tray` | 组装层 |

## 方向性说明

**`nodeprobe` 不 import `sbx`。** 探测要真的经出口发请求，但拨号器是一个普通函数值：
`sbx.Dialer` 与 `httpclient.Dialer` 都是 `func(ctx, network, addr string) (net.Conn, error)` 的类型别名，
同一个类型。sbx 产出拨号器，`app`（唯一组装点）把它们塞进探测项——依赖在组装层交汇，
包与包之间没有 import，同层规则因此成立。

**`forward` 在 L5。** 它的 `Config.Complete` 直接返回 `engine.Outcome`（L4），而它是转发端口的
组装点，与 `panel` 同级；`app` 启动它并注入 engine 回调。

**`app` 是唯一组装点。** 它是唯一允许「什么都 import」的包。`sbx`/`registry`/`nodeprobe`/`health`/`engine`/`panel` 之间通过 `app` 传递依赖，而不是互相 import。`panel` 靠 `PanelDeps` 依赖注入拿到所有能力（与 JS 版 `startPanel({status, getSettings, applySettings, actions, logs, routeRecent})` 同构），因此 `panel` 自己不 import 任何 L2/L3/L4。
