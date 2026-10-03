# FreeRouter

本机跑一个网关：把 `opencode.ai` 免费车道的对话请求，从**你自己挑的订阅节点**出口转出去，
对外提供 OpenAI 兼容接口。不改 `opencode.ai` 服务端，也不注入代理。

## 它替你做什么

* 订阅地址（sing-box / Clash 格式都行）→ 解析、合并、去重成一个出口池，按你选的地区顺序回退
* 批量探测出口通不通，分成 A / B 两级，面板上可以手动重探
* 一个 OpenAI 兼容端点：`GET /v1/models`、`POST /v1/chat/completions`（流式与非流式）、
  `POST /v1/responses`
* 出口失败自动换下一个接着试（只在首字节之前）；已经开始吐字的响应不回头
* 记用量（模型 × 出口的调用与失败）和每次请求走了哪个出口，面板可查
* 托盘常驻：打开面板 / 热重载（换出口不断线、在途连接不丢）/ 退出

## 上手

1. 到 [Releases](https://github.com/krisxu23/Opencode-FreeRouter/releases) 下载
   `FreeRouter-v<版本>-windows-x64.zip`，解压即可 —— 面板的前端已编进 exe，同级不需要任何目录；
   想自己构建就 `npm run build:exe` → 仓库根出现 `FreeRouter.exe`
2. 双击它 → 托盘出现图标（没有控制台窗口）→ 右键「打开面板」。exe 未签名，
   Windows 可能弹 SmartScreen，选「仍要运行」；运行期不需要 Node
3. 面板 **设置** 页填订阅地址、勾地区与回退顺序 → **保存并应用**
4. 等首轮探测跑完，在 **出口节点** 页确认健康池，然后按下面的接入信息去配你的客户端

## 接入

```
baseURL  http://127.0.0.1:3457/v1
apiKey   <面板「概览」页一键复制，就是 data/settings.json 里的 forwardKey>
```

`GET /health` 故意不校验密钥，只回答「我活着」。只监听 `127.0.0.1`，不对外网开放。

## 面板

`http://127.0.0.1:3458/`，六页：

| 页 | 内容 |
| --- | --- |
| 概览 | 健康池、当前出口、接入信息（复制 baseURL / key / `.env` / `curl`） |
| 出口节点 | 节点表 + 活性/分级/地区筛选 + 手动探测 |
| 免费模型 | 车道模型清单，可复制 id；限额表可手动刷新 |
| 用量 | 按模型 × 出口的调用与失败统计 |
| 日志 | 运行日志尾部 + 每次请求的轨迹 |
| 设置 | 订阅与接入、地区回退顺序、探测参数、模型与输出、危险操作 |

## 状态放在哪

全部在 `data/`：设置、节点池、健康度、订阅缓存、用量统计、模型清单缓存、请求轨迹、运行日志
（超 5MB 轮转）。这个目录不入 git，删掉它就是一个全新实例。

## 边界

上游说了算的东西它管不了：免费车道的模型 id、限额、可用性、鉴权方式。上游一旦调整（改 id、
加限额、按出口 IP 记账），本项目可能直接失效，那不是 bug。节点源为 krisxu23/freesub，
网关逻辑移植自 dsh-our-free-model（MIT）。

## 许可证

GPL-3.0-or-later（`LICENSE`）—— 因为 sing-box 是以库的形式链接进这个可执行文件的。

* `NOTICE`：sing-box 的名称条款、依赖按许可证类型的统计、上游致谢
* `THIRD-PARTY-LICENSES/`：70 个依赖模块的许可证全文 + `INDEX.md` 清单
* `npm run release` 重新汇总上面两项，并打出 GPL 源码包到 `release/`
* Release 页同时挂 zip 与同版本的源码包 —— 只发二进制就是一次 GPL 违规

> **发布契约**：版本号只认 `package.json` 的 `version`。往 `main` 传改动产品代码
> （`internal/`、`cmd/`、`scripts/`、`web/`、`go.mod`）的提交时，**必须在同一次推送里
> bump 这个版本号**，CI 的 gates 会拦住没 bump 的那次；bump 之后 `release` 作业会为该
> 版本号自动建 tag 并发布 zip + 源码包，不用再手动打 tag。只改文档或 CI 的推送不发新版，
> 这是刻意的。

> 构建契约（`GOPROXY=off`、链接标签、工具链版本）写在 `scripts/go-build.mjs` 的头注释里，
> 分层与依赖规则在 `internal/LAYERS.md`，本文件只讲这个程序做什么。
