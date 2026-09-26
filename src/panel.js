/**
 * Admin panel: a single-page settings + status console served on 127.0.0.1.
 *
 * Endpoints (all local-only):
 *   GET  /              single-page console (dark/light theme, copyable API
 *                       credentials, free-model list with copy ids, scrollable
 *                       node table)
 *   GET  /api/settings  current settings
 *   PUT  /api/settings  merge a patch, persist, apply (rebuild callback)
 *   GET  /api/status    sing-box state, node health table, region matrix,
 *                       free-model catalog, forward endpoint + key
 *   POST /api/probe     run one probe round now
 *   POST /api/refresh   refetch subscriptions and rebuild
 *
 * @module src/panel.js
 */

import http from 'node:http'

export async function startPanel({ status, getSettings, applySettings, actions = {}, logs, log = () => {}, port: desiredPort }) {
  const server = http.createServer((req, res) => {
    void handle(req, res).catch(error => {
      log(`panel request failed: ${error?.message ?? error}`)
      if (!res.headersSent) json(res, 500, { error: String(error?.message ?? error) })
      else res.end()
    })
  })

  async function handle(req, res) {
    const path = (req.url ?? '/').split('?')[0].replace(/\/+$/, '') || '/'
    if (req.method === 'GET' && path === '/') {
      const body = PAGE
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' })
      res.end(body)
      return
    }
    if (req.method === 'GET' && path === '/api/settings') {
      json(res, 200, getSettings())
      return
    }
    if (req.method === 'PUT' && path === '/api/settings') {
      const patch = await readJson(req)
      const applied = applySettings(patch ?? {})
      json(res, 200, applied)
      return
    }
    if (req.method === 'GET' && path === '/api/status') {
      json(res, 200, status())
      return
    }
    if (req.method === 'GET' && path === '/api/logs') {
      json(res, 200, { lines: logs?.() ?? [] })
      return
    }
    if (req.method === 'POST' && path === '/api/probe') {
      await actions.probeNow?.()
      json(res, 200, { ok: true })
      return
    }
    if (req.method === 'POST' && path === '/api/refresh') {
      await actions.refresh?.()
      json(res, 200, { ok: true })
      return
    }
    json(res, 404, { error: `no route for ${req.method} ${path}` })
  }

  const port = await new Promise((resolve, reject) => {
    const listenOn = candidate => {
      server.once('error', error => {
        if (candidate !== 0) {
          log(`panel port ${candidate} unavailable (${error?.code ?? error}) — falling back to a random port`)
          listenOn(0)
        } else reject(error)
      })
      server.listen(candidate, '127.0.0.1', () => resolve(server.address()?.port ?? 0))
    }
    listenOn(Number.isFinite(desiredPort) && desiredPort > 0 ? desiredPort : 0)
  })
  return { server, port, close: () => new Promise(resolve => { server.closeAllConnections?.(); server.close(() => resolve()) }) }
}

function json(res, statusCode, payload) {
  const body = JSON.stringify(payload)
  res.writeHead(statusCode, { 'content-type': 'application/json; charset=utf-8', 'content-length': Buffer.byteLength(body), 'cache-control': 'no-store' })
  res.end(body)
}

async function readJson(req) {
  const chunks = []
  let size = 0
  for await (const chunk of req) {
    size += chunk.length
    if (size > 1 << 20) throw new Error('request body too large')
    chunks.push(chunk)
  }
  if (size === 0) return {}
  return JSON.parse(Buffer.concat(chunks).toString('utf8'))
}

// 视觉刻度统一约定：字号 12/13/13.5/14/20，间距全部落在 4 的倍数上，圆角 6/8/10。
const PAGE = `<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opencode-FreeRouter 控制台</title>
<style>
/* 设计约定：色彩只承载状态语义；UI 用 Segoe UI，数据一律 Cascadia Mono；
   全页唯一加重视觉块是"接入信息"配置条，其余靠留白与细分隔线分组。 */
:root { --mono: "Cascadia Mono", Consolas, "Courier New", monospace }
body.dark { --bg:#10141c; --fg:#e2e8f2; --muted:#8b98ab; --line:#222b38; --surface:#171e29; --raised:#1d2634; --border:#2a3547; --accent:#79a8e6; --ok:#3ecf8e; --bad:#f0645c; --warn:#e0a640; --code:#0d1119 }
body.light { --bg:#f4f5f7; --fg:#212b36; --muted:#5b6874; --line:#e3e7ec; --surface:#fbfcfd; --raised:#ffffff; --border:#d6dde5; --accent:#2c66b8; --ok:#0f7444; --bad:#b03330; --warn:#8a6408; --code:#eef1f5 }
* { box-sizing:border-box }
body { font-family:"Segoe UI","Microsoft YaHei UI",system-ui,sans-serif; font-size:14px; line-height:1.55; background:var(--bg); color:var(--fg); margin:0 auto; max-width:1060px; padding:22px 20px 40px }
main { animation:arrive .24s ease-out }
@keyframes arrive { from { opacity:0; transform:translateY(6px) } }
@media (prefers-reduced-motion: reduce) { main { animation:none } }
:focus-visible { outline:2px solid var(--accent); outline-offset:2px }
h1 { font-size:18px; font-weight:650; letter-spacing:-.2px; margin:0 }
h2 { font-size:14px; font-weight:600; margin:0 0 10px }
.mono { font-family:var(--mono); font-size:13px }
.muted { color:var(--muted) }
header { display:flex; align-items:baseline; gap:12px }
header .spacer { flex:1 }
.statusline { display:flex; gap:20px; flex-wrap:wrap; align-items:center; margin:14px 0 4px; padding-bottom:14px; border-bottom:1px solid var(--line); font-family:var(--mono); font-size:13px }
.dot { display:inline-block; width:9px; height:9px; border-radius:50%; background:var(--warn); margin-right:6px; vertical-align:baseline }
.dot.ok { background:var(--ok) } .dot.bad { background:var(--bad) }
.statusline b { color:var(--muted); font-weight:400 }
.strip { background:var(--raised); border:1px solid var(--border); border-radius:10px; padding:16px; margin:16px 0 }
.kv { display:flex; align-items:center; gap:10px; margin:9px 0 }
.kv > span:first-child { min-width:76px; color:var(--muted); font-size:13px }
.kv code { flex:1 }
code { background:var(--code); border:1px solid var(--border); border-radius:6px; padding:5px 9px; font-family:var(--mono); font-size:13px; word-break:break-all }
section { margin:26px 0 }
section > h2 { padding-bottom:8px; border-bottom:1px solid var(--line) }
input,textarea,button { font:inherit; font-size:13.5px; background:var(--surface); color:var(--fg); border:1px solid var(--border); border-radius:8px; padding:7px 12px }
textarea { width:100%; resize:vertical }
button { cursor:pointer; transition:filter .12s, background .12s; white-space:nowrap }
button:hover { filter:brightness(1.15) }
.row { display:flex; gap:10px; align-items:center; flex-wrap:wrap }
.scrollbox { max-height:264px; overflow-y:auto; border:1px solid var(--border); border-radius:8px; background:var(--surface) }
table { border-collapse:collapse; width:100%; font-size:13px }
th,td { text-align:left; padding:7px 10px; border-bottom:1px solid var(--line); white-space:nowrap }
th { color:var(--muted); font-weight:400; position:sticky; top:0; background:var(--surface) }
td:first-child { white-space:normal; min-width:220px }
#nodes td:nth-child(3), #nodes td:nth-child(5), #nodes td:nth-child(6), #nodes td:nth-child(7) { font-family:var(--mono); font-variant-numeric:tabular-nums; font-size:12.5px }
.alive { color:var(--ok); font-weight:600 }
.dead { color:var(--bad); font-weight:600 }
.unknown { color:var(--warn); font-weight:600 }
.chip { display:inline-flex; align-items:center; gap:8px; margin:0 8px 8px 0; padding:6px 6px 6px 12px; border:1px solid var(--border); border-radius:8px; background:var(--surface); font-family:var(--mono); font-size:13px }
.chip button { font-size:12px; padding:3px 10px; border-radius:6px }
#msg { color:var(--ok); margin-left:8px; font-size:13.5px }
label { font-size:13.5px }
.formrow { margin:10px 0 }
</style></head><body class="dark">
<main>
<header>
  <h1>Opencode-FreeRouter</h1>
  <span class="spacer"></span>
  <button id="themeBtn"></button>
</header>
<div class="statusline">
  <span><span class="dot" id="sbDot"></span><span id="sb">…</span></span>
  <span><b>转发</b> <span id="fwd">…</span></span>
  <span><b>可用出口</b> <span id="aliveCount">–</span></span>
  <span><b>免费模型</b> <span id="modelCount">–</span></span>
  <span style="flex:1"></span>
  <span class="muted" id="rm"></span>
</div>
<div class="strip">
  <h2>接入信息</h2>
  <div class="kv"><span>API 地址</span><code id="apiBase">…</code> <button class="copy" data-for="apiBase">复制</button></div>
  <div class="kv"><span>API Key</span><code id="apiKey">…</code> <button class="copy" data-for="apiKey">复制</button></div>
</div>
<section>
  <h2>免费模型 <span class="muted mono" id="modelNote">点"复制"拿模型 id</span></h2>
  <div id="models"><span class="muted">直连拉取中，稍候几秒。</span></div>
</section>
<section>
  <h2>节点健康 <span class="muted mono" id="nodeCount"></span></h2>
  <div class="scrollbox"><table id="nodes"><thead><tr><th>节点</th><th>国家</th><th>端口</th><th>健康</th><th>延迟</th><th>出口 IP</th><th>最后探测</th></tr></thead><tbody></tbody></table></div>
</section>
<section>
  <h2>用量（全部留在本机）<span class="muted mono" id="usageToday"></span></h2>
  <div class="scrollbox"><table id="usage"><thead><tr><th>模型</th><th>请求</th><th>输入 tok</th><th>输出 tok</th></tr></thead><tbody></tbody></table></div>
</section>
<section>
  <h2>日志（最近 400 条，排错用）</h2>
  <div class="row" style="margin-bottom:8px">
    <button id="logRefresh">刷新</button>
    <button id="logCopy">复制全部</button>
    <span class="muted" style="font-size:12.5px">完整文件在 data/gateway.log（超 5MB 自动轮转为 gateway.old.log）</span>
  </div>
  <div class="scrollbox"><pre id="logs" style="margin:0;padding:10px;font-family:var(--mono);font-size:12px;white-space:pre-wrap;word-break:break-all"></pre></div>
</section>
<section>
  <h2>设置</h2>
  <label>订阅链接（每行一个，留空用内置 freesub 源）</label>
  <textarea id="subUrls" rows="3"></textarea>
  <div class="formrow">
    <label>出口地区（按回退顺序，点击加入或移出；"其他"含无名节点，由探测按出口 IP 实测归桶）</label>
    <div id="quick" style="margin-top:6px"></div>
    <div id="orderLine" class="muted mono" style="font-size:12.5px;margin-top:6px"></div>
  </div>
  <div class="row" style="margin-top:12px">
    <label><input type="checkbox" id="probeEnabled"> 自动探测</label>
    <label>默认思考强度
      <select id="effortLevel" style="margin-left:4px">
        <option value="light">Light 精简 (2K)</option>
        <option value="balanced">Balanced 均衡 (24K)</option>
        <option value="deep">Deep 深思 (模型上限)</option>
      </select>
    </label>
    <label>默认输出上限 <input id="defaultMaxTokens" type="number" style="width:88px"></label>
    <label>探测并发 <input id="probeWorkers" type="number" style="width:64px"></label>
    <label>探测周期(分) <input id="probeIntervalMin" type="number" style="width:64px"></label>
    <button id="save">保存并应用</button><span id="msg"></span>
  </div>
  <div class="row" style="margin-top:10px">
    <label>端口起始 <input id="portBase" type="number" style="width:88px"></label>
    <label>端口段容量 <input id="portSpan" type="number" style="width:88px"></label>
    <span class="muted" style="font-size:12.5px">每节点占一个端口；容量应 ≥ 节点数，起始+容量保持在 49152 以下</span>
  </div>
</section>
<section class="row">
  <button id="probeNow">立即探测</button>
  <button id="refreshSub">刷新订阅并重建</button>
</section>
</main>
<script>
const GROUPS = [['US','美国'],['JP','日本'],['HK','香港'],['TW','台湾'],['KR','韩国'],['SG','新加坡'],['EU','欧洲'],['OTHER','其他']];
let sel = [];
const body = document.body, themeBtn = document.getElementById('themeBtn');
function applyTheme(t) {
  body.className = t;
  themeBtn.textContent = t === 'dark' ? '浅色' : '深色';
  localStorage.setItem('ofr-theme', t);
}
themeBtn.onclick = () => applyTheme(body.className === 'dark' ? 'light' : 'dark');
applyTheme(localStorage.getItem('ofr-theme') || 'dark');

async function j(url, opt) { const r = await fetch(url, opt); if (!r.ok) throw new Error(await r.text()); return r.json() }
function esc(s) { return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])) }
async function copyText(text, btn) {
  try { await navigator.clipboard.writeText(text) }
  catch {
    const ta = document.createElement('textarea');
    ta.value = text; document.body.appendChild(ta); ta.select();
    document.execCommand('copy'); ta.remove();
  }
  const old = btn.textContent; btn.textContent = '已复制';
  setTimeout(() => btn.textContent = old, 1500);
}
document.addEventListener('click', e => {
  const b = e.target.closest('.copy'); if (!b) return;
  copyText(document.getElementById(b.dataset.for).textContent, b);
});

function renderStatus(s) {
  const up = s.singbox?.running === true;
  document.getElementById('sbDot').className = 'dot ' + (up ? 'ok' : 'bad');
  document.getElementById('sb').textContent = up ? 'sing-box 运行中' : 'sing-box 未运行';
  document.getElementById('fwd').textContent = ':' + (s.forward?.port ?? 3457);
  document.getElementById('aliveCount').textContent = (() => {
    const nodes = s.nodes ?? [];
    return nodes.filter(n => n.state === 'alive').length + ' / ' + nodes.length;
  })();
  const models = s.models ?? [];
  const restricted = new Set(s.regionModels ?? []);
  document.getElementById('modelCount').textContent = models.length;
  document.getElementById('rm').textContent = (s.regionModels ?? []).length ? '受限: ' + s.regionModels.join(', ') : '';
  document.getElementById('apiBase').textContent = 'http://127.0.0.1:' + (s.forward?.port ?? 3457) + '/v1';
  document.getElementById('apiKey').textContent = s.forward?.key ?? '';
  const wrap = document.getElementById('models');
  if (!models.length) { wrap.innerHTML = '<span class="muted">直连拉取中，稍候几秒。</span>'; }
  else {
    wrap.innerHTML = '';
    for (const id of models) {
      const chip = document.createElement('span'); chip.className = 'chip';
      const label = document.createElement('span'); label.textContent = id;
      if (restricted.has(id)) { chip.style.opacity = .55; chip.title = '该模型当前所有已测健康出口均报地区受限'; }
      const btn = document.createElement('button'); btn.textContent = '复制';
      btn.onclick = () => copyText(id, btn);
      chip.append(label, btn); wrap.appendChild(chip);
    }
    const all = document.createElement('button'); all.style.margin = '4px 0'; all.textContent = '复制全部模型 id';
    all.onclick = () => copyText(models.join('\\n'), all);
    wrap.appendChild(all);
  }
  const nodes = s.nodes ?? [];
  document.getElementById('nodeCount').textContent = '';
  const tb = document.querySelector('#nodes tbody'); tb.innerHTML = '';
  for (const n of nodes) {
    tb.insertAdjacentHTML('beforeend', '<tr><td>' + esc(n.tag) + '</td><td>' + esc(n.country) + '</td><td>' + (n.port ?? '') + '</td><td class="' + n.state + '">' + n.state + '</td><td>' + (n.latencyMs >= 0 ? n.latencyMs + 'ms' : '–') + '</td><td>' + esc(n.exitIp || '') + '</td><td>' + (n.lastProbeAt ? new Date(n.lastProbeAt).toLocaleTimeString() : '–') + '</td></tr>');
  }
  // 用量
  const u = s.usage ?? {};
  const today = u.today ?? { req: 0, in: 0, out: 0 };
  document.getElementById('usageToday').textContent = '今日 ' + today.req + ' 次 / ' + today.in + ' 进 / ' + today.out + ' 出';
  const ub = document.querySelector('#usage tbody'); ub.innerHTML = '';
  const rows = Object.entries(u.byModel ?? {}).sort((a, b) => b[1].req - a[1].req).slice(0, 20);
  for (const [model, m] of rows) {
    ub.insertAdjacentHTML('beforeend', '<tr><td>' + esc(model) + '</td><td>' + m.req + '</td><td>' + m.in + '</td><td>' + m.out + '</td></tr>');
  }
  if (!rows.length) ub.innerHTML = '<tr><td colspan="4" class="muted">还没有请求记录</td></tr>';
}
async function loadLogs() {
  const box = document.getElementById('logs');
  try {
    // 注意：局部变量不能命名为 j —— 会遮蔽页面级的 j() 函数（TDZ），调用直接抛错
    const data = await j('/api/logs');
    box.textContent = (data.lines ?? []).map(l => new Date(l.t).toLocaleTimeString() + ' [' + l.level + '] ' + l.msg).join('\\n');
    box.scrollTop = box.scrollHeight;
  } catch (e) {
    box.textContent = '日志加载失败: ' + (e?.message ?? e);
  }
}
async function loadSettings() {
  const s = await j('/api/settings');
  document.getElementById('subUrls').value = (s.subUrls ?? []).join('\\n');
  document.getElementById('countries')?.remove();
  sel = (s.countries ?? []).filter(c => GROUPS.some(g => g[0] === c));
  renderQuick();
  document.getElementById('probeEnabled').checked = s.probeEnabled !== false;
  document.getElementById('probeWorkers').value = s.probeWorkers ?? 24;
  document.getElementById('probeIntervalMin').value = s.probeIntervalMin ?? 30;
  document.getElementById('effortLevel').value = s.effortLevel ?? 'balanced';
  document.getElementById('defaultMaxTokens').value = s.defaultMaxTokens ?? 32768;
  document.getElementById('portBase').value = s.portBase ?? 21000;
  document.getElementById('portSpan').value = s.portSpan ?? 8000;
}
function save() {
  const body = {
    subUrls: document.getElementById('subUrls').value.split('\\n').map(x => x.trim()).filter(Boolean),
    countries: sel.slice(),
    probeEnabled: document.getElementById('probeEnabled').checked,
    probeWorkers: Number(document.getElementById('probeWorkers').value) || 24,
    probeIntervalMin: Number(document.getElementById('probeIntervalMin').value) || 30,
    effortLevel: document.getElementById('effortLevel').value || 'balanced',
    defaultMaxTokens: Number(document.getElementById('defaultMaxTokens').value) || 32768,
    portBase: Number(document.getElementById('portBase').value) || 21000,
    portSpan: Number(document.getElementById('portSpan').value) || 8000,
  };
  j('/api/settings', { method: 'PUT', headers: {'content-type':'application/json'}, body: JSON.stringify(body) })
    .then(() => { document.getElementById('msg').textContent = '已保存并应用'; setTimeout(() => document.getElementById('msg').textContent = '', 4000); loadStatus(); })
    .catch(e => document.getElementById('msg').textContent = '保存失败: ' + e.message);
}
const quick = document.getElementById('quick');
function renderQuick() {
  quick.innerHTML = '';
  for (const [id, name] of GROUPS) {
    const idx = sel.indexOf(id);
    const b = document.createElement('button');
    b.textContent = idx >= 0 ? '✓ ' + name + ' (' + (idx + 1) + ')' : name;
    if (idx >= 0) b.style.borderColor = 'var(--accent)';
    b.onclick = () => { idx >= 0 ? sel.splice(idx, 1) : sel.push(id); renderQuick(); };
    quick.appendChild(b);
  }
  const line = document.getElementById('orderLine');
  line.textContent = sel.length ? '回退顺序: ' + sel.map(s => (GROUPS.find(g => g[0] === s) ?? [])[1]).join(' → ') : '（尚未选择任何地区）';
}
renderQuick();
document.getElementById('save').onclick = save;
document.getElementById('probeNow').onclick = () => j('/api/probe', {method:'POST'}).then(loadStatus);
document.getElementById('refreshSub').onclick = () => j('/api/refresh', {method:'POST'}).then(loadStatus);
document.getElementById('logRefresh').onclick = loadLogs;
document.getElementById('logCopy').onclick = () => copyText(document.getElementById('logs').textContent || '（空）', document.getElementById('logCopy'));
async function loadStatus() { try { renderStatus(await j('/api/status')) } catch {} }
loadSettings(); loadStatus(); loadLogs(); setInterval(loadStatus, 5000); setInterval(loadLogs, 15000);
</script></body></html>
`
