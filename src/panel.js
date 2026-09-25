/**
 * Admin panel: a single-page settings + status console served on 127.0.0.1.
 *
 * Endpoints (all local-only):
 *   GET  /              single-page console
 *   GET  /api/settings  current settings
 *   PUT  /api/settings  merge a patch, persist, apply (rebuild callback)
 *   GET  /api/status    sing-box state, node health table, region matrix
 *   POST /api/probe     run one probe round now
 *   POST /api/refresh   refetch subscriptions and rebuild
 *
 * @module src/panel.js
 */

import http from 'node:http'

export async function startPanel({ status, getSettings, applySettings, actions = {}, log = () => {} }) {
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
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => resolve(server.address()?.port ?? 0))
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
<title>Lite Gateway 控制台</title>
<style>
:root { color-scheme: dark }
body { font-family: "Segoe UI", system-ui, sans-serif; background:#11151c; color:#dbe2ee; margin:0 auto; max-width:980px; padding:16px }
h1 { font-size:20px } h2 { font-size:15px; margin:18px 0 8px; color:#8ab4ff }
table { border-collapse:collapse; width:100%; font-size:12.5px }
th,td { text-align:left; padding:4px 8px; border-bottom:1px solid #232b38 }
th { color:#7d8899; font-weight:600 }
.alive{color:#5bd08a}.dead{color:#ff6b6b}.unknown{color:#c9a227}
input,textarea,button,select { background:#1a2230; color:#dbe2ee; border:1px solid #2c3850; border-radius:6px; padding:6px 8px; font:inherit }
textarea { width:100%; box-sizing:border-box }
button { cursor:pointer } button:hover { background:#24304a }
#quick button { margin:2px; padding:2px 8px; font-size:12px }
.row { display:flex; gap:12px; align-items:center; flex-wrap:wrap }
#msg { color:#5bd08a; margin-left:8px }
.box { background:#151b26; border:1px solid #232b38; border-radius:8px; padding:12px; margin:8px 0 }
</style></head><body>
<h1>Lite Gateway 控制台</h1>
<div class="box"><b>sing-box:</b> <span id="sb">…</span> <b>网关:</b> <span id="fwd">…</span>
 <b>受限模型:</b> <span id="rm">…</span></div>
<div class="box"><table id="nodes"><thead><tr><th>节点</th><th>国家</th><th>端口</th><th>健康</th><th>延迟</th><th>出口IP</th><th>最后探测</th></tr></thead><tbody></tbody></table></div>
<div class="box">
 <h2>设置</h2>
 <label>订阅链接（每行一个，留空用内置 freesub 源）</label>
 <textarea id="subUrls" rows="3"></textarea>
 <label>出口国家（按回退顺序，逗号分隔）</label>
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
async function j(url, opt) { const r = await fetch(url, opt); if (!r.ok) throw new Error(await r.text()); return r.json() }
function renderStatus(s) {
  document.getElementById('sb').textContent = s.singbox?.running ? '运行中 (pid ' + s.singbox.pid + ')' : '未运行';
  document.getElementById('fwd').textContent = s.forward?.running ? '监听 ' + s.forward.port : '未监听';
  document.getElementById('rm').textContent = (s.regionModels ?? []).join(', ') || '（暂无）';
  const tb = document.querySelector('#nodes tbody'); tb.innerHTML = '';
  for (const n of s.nodes ?? []) {
    tb.insertAdjacentHTML('beforeend', '<tr><td>' + esc(n.tag) + '</td><td>' + esc(n.country) + '</td><td>' + (n.port ?? '') + '</td><td class="' + n.state + '">' + n.state + '</td><td>' + (n.latencyMs >= 0 ? n.latencyMs + 'ms' : '-') + '</td><td>' + esc(n.exitIp || '') + '</td><td>' + (n.lastProbeAt ? new Date(n.lastProbeAt).toLocaleTimeString() : '-') + '</td></tr>');
  }
}
function esc(s) { return String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])) }
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
