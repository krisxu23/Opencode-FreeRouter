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

export async function startPanel({ status, getSettings, applySettings, actions = {}, log = () => {}, port: desiredPort }) {
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

const PAGE = `<!doctype html>
<html lang="zh"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Opencode-FreeRouter 控制台</title>
<style>
body.dark { --bg:#11151c; --fg:#dbe2ee; --muted:#7d8899; --box:#151b26; --border:#232b38; --accent:#8ab4ff; --code:#0e1420; --hbg:#1a2230; }
body.light { --bg:#f4f6fa; --fg:#1c2733; --muted:#5d6b7c; --box:#ffffff; --border:#dde4ee; --accent:#1b62d0; --code:#eef2f8; --hbg:#e7edf6; }
body { font-family:"Segoe UI",system-ui,sans-serif; background:var(--bg); color:var(--fg); margin:0 auto; max-width:980px; padding:16px }
h1 { font-size:20px; margin:0 } h2 { font-size:14px; margin:14px 0 8px; color:var(--accent) }
header.row { margin-bottom:12px }
table { border-collapse:collapse; width:100%; font-size:12.5px }
th,td { text-align:left; padding:4px 8px; border-bottom:1px solid var(--border) }
th { color:var(--muted); font-weight:600; position:sticky; top:0; background:var(--box) }
.alive{color:#3fae74}.dead{color:#e05252}.unknown{color:#b98a12}
body.light .alive{color:#1d7a4c} body.light .dead{color:#c22f2f} body.light .unknown{color:#9a730b}
input,textarea,button,code { font:inherit }
input,textarea,button { background:var(--hbg); color:var(--fg); border:1px solid var(--border); border-radius:6px; padding:6px 8px }
textarea { width:100%; box-sizing:border-box }
button { cursor:pointer } button:hover { filter:brightness(1.15) }
.row { display:flex; gap:12px; align-items:center; flex-wrap:wrap }
.box { background:var(--box); border:1px solid var(--border); border-radius:8px; padding:12px; margin:10px 0 }
code { background:var(--code); border:1px solid var(--border); border-radius:4px; padding:2px 6px; font-family:Consolas,monospace; font-size:12.5px; word-break:break-all }
.scrollbox { max-height:240px; overflow-y:auto; border:1px solid var(--border); border-radius:6px }
.chip { display:inline-flex; align-items:center; gap:6px; margin:3px; padding:3px 4px 3px 10px; border:1px solid var(--border); border-radius:14px; background:var(--hbg); font-family:Consolas,monospace; font-size:12px }
.chip button { padding:1px 8px; font-size:11px; border-radius:10px }
.kv { margin:6px 0 }
#msg { color:var(--accent); margin-left:8px }
</style></head><body class="dark">
<header class="row">
  <h1>Opencode-FreeRouter</h1><span style="flex:1"></span>
  <button id="themeBtn"></button>
</header>
<div class="box">
  <b>sing-box:</b> <span id="sb">…</span> &nbsp; <b>网关:</b> <span id="fwd">…</span> &nbsp;
  <b>受限模型:</b> <span id="rm">…</span>
</div>
<div class="box">
  <h2>接入信息（填到 agent 工具的 API 配置里）</h2>
  <div class="kv">API 地址：<code id="apiBase">…</code> <button class="copy" data-for="apiBase">复制</button></div>
  <div class="kv">API Key：<code id="apiKey">…</code> <button class="copy" data-for="apiKey">复制</button></div>
</div>
<div class="box">
  <h2>免费模型（付费模型不展示；点"复制"拿模型 id）</h2>
  <div id="models"><span style="color:var(--muted)">加载中…</span></div>
</div>
<div class="box">
  <h2>节点健康 <span id="nodeCount" style="color:var(--muted);font-weight:400"></span></h2>
  <div class="scrollbox"><table id="nodes"><thead><tr><th>节点</th><th>国家</th><th>端口</th><th>健康</th><th>延迟</th><th>出口IP</th><th>最后探测</th></tr></thead><tbody></tbody></table></div>
</div>
<div class="box">
  <h2>设置</h2>
  <label>订阅链接（每行一个，留空用内置 freesub 源）</label>
  <textarea id="subUrls" rows="3"></textarea>
  <label>出口国家（按回退顺序，点击按钮加入/移出）</label>
  <div id="quick"></div>
  <input id="countries" style="width:240px" placeholder="US,SG,JP">
  <div class="row" style="margin-top:8px">
    <label><input type="checkbox" id="probeEnabled"> 自动探测</label>
    <label>探测并发 <input id="probeWorkers" type="number" style="width:64px"></label>
    <label>探测周期(分) <input id="probeIntervalMin" type="number" style="width:64px"></label>
    <button id="save">保存并应用</button><span id="msg"></span>
  </div>
</div>
<div class="box">
  <button id="probeNow">立即探测</button> <button id="refreshSub">刷新订阅并重建</button>
</div>
<script>
const QUICK = ['US','SG','JP','TW','HK','NL','DE','GB','KR','TR'];
const body = document.body, themeBtn = document.getElementById('themeBtn');
function applyTheme(t) {
  body.className = t;
  themeBtn.textContent = t === 'dark' ? '☀️ 浅色' : '🌙 深色';
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
  const old = btn.textContent; btn.textContent = '已复制 ✓';
  setTimeout(() => btn.textContent = old, 1500);
}
document.addEventListener('click', e => {
  const b = e.target.closest('.copy'); if (!b) return;
  copyText(document.getElementById(b.dataset.for).textContent, b);
});

function renderStatus(s) {
  document.getElementById('sb').textContent = s.singbox?.running ? '运行中 (pid ' + s.singbox.pid + ')' : '未运行';
  document.getElementById('fwd').textContent = s.forward?.running ? '监听 ' + s.forward.port : '未监听';
  document.getElementById('rm').textContent = (s.regionModels ?? []).join(', ') || '（暂无）';
  document.getElementById('apiBase').textContent = 'http://127.0.0.1:' + (s.forward?.port ?? 3457) + '/v1';
  document.getElementById('apiKey').textContent = s.forward?.key ?? '';
  const models = s.models ?? [];
  const wrap = document.getElementById('models');
  if (!models.length) { wrap.innerHTML = '<span style="color:var(--muted)">暂无（等探测/目录刷新后出现）</span>'; }
  else {
    wrap.innerHTML = '';
    for (const id of models) {
      const chip = document.createElement('span'); chip.className = 'chip';
      const label = document.createElement('span'); label.textContent = id;
      const btn = document.createElement('button'); btn.textContent = '复制';
      btn.onclick = () => copyText(id, btn);
      chip.append(label, btn); wrap.appendChild(chip);
    }
    const all = document.createElement('button'); all.style.margin = '6px 3px'; all.textContent = '复制全部模型 id';
    all.onclick = () => copyText(models.join('\\n'), all);
    wrap.appendChild(all);
  }
  const nodes = s.nodes ?? [];
  document.getElementById('nodeCount').textContent = '(' + nodes.filter(n => n.state === 'alive').length + '/' + nodes.length + ' alive)';
  const tb = document.querySelector('#nodes tbody'); tb.innerHTML = '';
  for (const n of nodes) {
    tb.insertAdjacentHTML('beforeend', '<tr><td>' + esc(n.tag) + '</td><td>' + esc(n.country) + '</td><td>' + (n.port ?? '') + '</td><td class="' + n.state + '">' + n.state + '</td><td>' + (n.latencyMs >= 0 ? n.latencyMs + 'ms' : '-') + '</td><td>' + esc(n.exitIp || '') + '</td><td>' + (n.lastProbeAt ? new Date(n.lastProbeAt).toLocaleTimeString() : '-') + '</td></tr>');
  }
}
async function loadSettings() {
  const s = await j('/api/settings');
  document.getElementById('subUrls').value = (s.subUrls ?? []).join('\\n');
  document.getElementById('countries').value = (s.countries ?? []).join(',');
  document.getElementById('probeEnabled').checked = s.probeEnabled !== false;
  document.getElementById('probeWorkers').value = s.probeWorkers ?? 24;
  document.getElementById('probeIntervalMin').value = s.probeIntervalMin ?? 30;
}
function save() {
  const body = {
    subUrls: document.getElementById('subUrls').value.split('\\n').map(x => x.trim()).filter(Boolean),
    countries: document.getElementById('countries').value.split(',').map(x => x.trim().toUpperCase()).filter(Boolean),
    probeEnabled: document.getElementById('probeEnabled').checked,
    probeWorkers: Number(document.getElementById('probeWorkers').value) || 24,
    probeIntervalMin: Number(document.getElementById('probeIntervalMin').value) || 30,
  };
  j('/api/settings', { method: 'PUT', headers: {'content-type':'application/json'}, body: JSON.stringify(body) })
    .then(() => { document.getElementById('msg').textContent = '已保存并应用 ✓'; setTimeout(() => document.getElementById('msg').textContent = '', 4000); loadStatus(); })
    .catch(e => document.getElementById('msg').textContent = '失败: ' + e.message);
}
const quick = document.getElementById('quick');
for (const c of QUICK) {
  const b = document.createElement('button'); b.textContent = c;
  b.onclick = () => { const el = document.getElementById('countries'); const set = new Set(el.value.split(',').map(x=>x.trim().toUpperCase()).filter(Boolean)); set.has(c) ? set.delete(c) : set.add(c); el.value = [...set].join(','); };
  quick.appendChild(b);
}
document.getElementById('save').onclick = save;
document.getElementById('probeNow').onclick = () => j('/api/probe', {method:'POST'}).then(loadStatus);
document.getElementById('refreshSub').onclick = () => j('/api/refresh', {method:'POST'}).then(loadStatus);
async function loadStatus() { try { renderStatus(await j('/api/status')) } catch {} }
loadSettings(); loadStatus(); setInterval(loadStatus, 5000);
</script></body></html>`
