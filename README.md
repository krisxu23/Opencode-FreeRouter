# Opencode-FreeRouter

把 [dsh-our-free-model](https://github.com/zouyuxuan122/dsh-our-free-model) 的免密网关逻辑移植成独立 Windows 桌面程序，
并缝合一台 [sing-box](https://github.com/SagerNet/sing-box) 边车接管全部出站：订阅节点 → 逐节点两级实测 →
按你在面板选择的国家顺序选出口 → OpenAI 兼容端口转发到 opencode.ai 免费车道。

## 快速开始

1. 双击 `Opencode-FreeRouter.exe`（托盘出现图标）
2. 托盘菜单 → **打开面板**（`http://127.0.0.1:3458`，右上角可切深色/浅色）
3. 面板 **接入信息** 区：一键复制 API 地址与 Key
4. 面板里：勾选出口国家（点国家按钮加入/移出，按回退顺序）→ **保存并应用**
5. 等首轮探测跑完（约 1 分钟，节点表出现 alive/dead 标记；免费模型目录在"免费模型"区，可一键复制模型 id）

```
baseURL: http://127.0.0.1:3457/v1
apiKey:  <data/settings.json 里的 forwardKey，ofm- 开头>
```

OpenAI 兼容路由：`GET /v1/models`、`POST /v1/chat/completions`（流式/非流式）、`POST /v1/responses`。

## 工作原理

```
订阅(freesub, 6h CI) ──多源回退──▶ 国家过滤 ──▶ 出站方言净化
    └─▶ sing-box：每节点一个本地 mixed 入站端口（"指定出口"="指定本地端口"）
                └─ catch-all 常驻入站：网关全部出站都过 sing-box，绝不直连
网关探测（每 30min，可手动）：
    活性(1.1.1.1 IP 字面量 + cp.cloudflare) → 上游可达门(opencode.ai/zen/v1/models 匿名 200) → 出口 IP 实证
对话请求 → 会话粘性(30min) + (模型×节点) region 矩阵 + 国家回退链
    → 经选中节点端口出站到 opencode.ai → SSE 回传
失败回退：Region/Transport/Timeout/Empty → 同 session id 换下一候选出口重试一次
    （429=配额耗尽，切节点无用，配额按 session 记账）
```

设计细节与出处见 `../Free-Router/docs/superpowers/plans/2026-09-26-lite-gateway-singbox-plan.md`（含 Free-Router 机制对照表）。

## 发布（给别人用）

```bash
node scripts/build-release.mjs
```

一条命令完成：**构建托盘 exe → 装配全部运行组件 → 完整性校验 → 压缩**，产出
`release/Opencode-FreeRouter-v<版本>-windows-x64.zip`（约 62MB），内含托盘 exe（嵌图标）、
自带 Node 运行时（`runtime/node.exe`，对方无需安装 Node）、网关源码、undici 依赖、
sing-box v1.14.0 内核、运行说明。**解压后双击 `release/Opencode-FreeRouter/Opencode-FreeRouter.exe`
即可使用**，数据都在解压目录的 `data/` 下，删除文件夹即卸载。上传 GitHub：Releases →
New release → 拖入 zip 附件。首次运行 SmartScreen 会提示未签名程序，点"更多信息 →
仍要运行"（消除需代码签名证书）。

## 目录结构

```
├── src/                 网关源码（ESM）
├── launcher/            Go 托盘壳（含 exe 图标资源）
├── scripts/             构建/下载/图标脚本
├── tests/               node:test 测试
├── bin/                 sing-box 内核（不入 git）
├── node_modules/        undici 依赖（不入 git）
├── data/                运行时状态与日志（不入 git）
└── release/             打包产物（不入 git）
    ├── Opencode-FreeRouter/   解压即可运行的完整程序目录
    └── *.zip                  发布包
```

## 开发

```bash
npm install            # 唯一第三方依赖 undici
npm test               # node:test 全量
npm run fetch-singbox  # 下载 sing-box v1.14.0 内核到 bin/
node src/index.js      # 前台跑网关
node scripts/build-release.mjs   # 构建托盘 exe + 发布 zip
```

`src/` 中 upstream/http/catalog/forward/channel/messages/stream/effort/adapter 等照抄上游 1:1，
singbox/health/engine/panel/nodeprobe/registry/logger 为本项目新增；`launcher/` Go 托盘壳
（`rsrc_windows_amd64.syso` 图标资源随构建自动链接）。

## 上游与许可

- 网关逻辑移植自 dsh-our-free-model（MIT），见 `NOTICE` 与 `LICENSE`
- 节点源 krisxu23/freesub；sing-box 以独立进程调用、不分发（GPLv3）
