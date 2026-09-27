/**
 * sing-box sidecar: stable per-node local inbound ports, subscription outbound
 * sanitizing, config generation, and child-process management.
 *
 * Routing primitive: every node gets its own `mixed` inbound on 127.0.0.1, so
 * "send this through node X" is just "dial 127.0.0.1:<port>". No urltest,
 * no selector groups, no clash API. A permanent catch-all inbound keeps the
 * "everything goes through sing-box" invariant (its route.final is direct).
 *
 * Port table, sanitizer rules, and the failKeepOld contract mirror the
 * battle-tested Go implementation in Free-Router internal/app/nodes.go.
 *
 * @module src/singbox.js
 */

import fs from 'node:fs'
import net from 'node:net'
import { spawn, execFileSync } from 'node:child_process'

export const CATCHALL_TAG = 'in-catchall'

/** uTLS fingerprints sing-box accepts (option/uTLSFingerprint table). */
const UTLS_FP = new Set(['chrome', 'firefox', 'safari', 'ios', 'android', 'edge', '360', 'qq', 'random', 'randomized'])
const UUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/
/** Known Shadowsocks cipher names (used to detect the base64("method:password") malformation). */
const SS_METHODS = new Set([
  'aes-128-gcm', 'aes-192-gcm', 'aes-256-gcm', 'chacha20-ietf-poly1305', 'xchacha20-ietf-poly1305',
  'none', 'plain', '2022-blake3-aes-128-gcm', '2022-blake3-aes-256-gcm', '2022-blake3-chacha20-poly1305',
])

/**
 * Stable port table: existing keys keep their port (no drift across rebuilds),
 * new keys take the next free slot, vanished keys are pruned (an ever-growing
 * table was a measured incident in Free-Router: 27k stale entries).
 *
 * `base`/`span` come from settings (panel-editable) so large node counts fit —
 * never hardcode the range. When the range is exhausted, remaining nodes get
 * NO port (buildConfig omits them and the caller warns) instead of a duplicate
 * assignment that would make sing-box FATAL on bind.
 * Keep the range below 49152: Windows hands out ephemeral client ports from
 * 49152 up, and an ephemeral grab racing the bind means intermittent FATALs.
 */
export function assignPorts(outbounds, prev = {}, { base = 21000, span = 8000, avoid = new Set() } = {}) {
  const end = Math.min(Math.trunc(base) + Math.max(1, Math.trunc(span)) - 1, 65535)
  const table = { ...prev }
  const used = new Set([...Object.values(table), ...avoid])
  let next = base
  for (const o of outbounds) {
    if (table[o.tag] != null) continue
    while (next <= end && used.has(next)) next += 1
    if (next > end) break
    table[o.tag] = next
    used.add(next)
  }
  const keep = new Set(outbounds.map(o => o.tag))
  for (const k of Object.keys(table)) if (!keep.has(k)) delete table[k]
  return table
}

/**
 * Fix outbound shapes the subscription dialect gets wrong and sing-box rejects
 * outright (dropping the whole node) or panics on at first dial:
 *
 * 1. `tls` block without `enabled` — v1.14 vless/trojan build a nil-config TLS
 *    dialer and panic on first connection; missing enabled means enabled.
 * 2. reality requires uTLS — force a known fingerprint (chrome default).
 * 3. unknown uTLS fingerprint (v2ray's `unsafe`) — drop the utls block.
 * 4. `flow` — only xtls-rprx-vision exists in sing-box; `none` and the old
 *    `-udp443` spelling are normalized/dropped.
 * 5. `transport.type: tcp|raw` — v2ray spelling for "no transport"; omitting
 *    the field is the equivalent semantics. Unknown transports (xhttp…) are
 *    left for sing-box to reject explicitly — silent TCP downgrade is worse.
 * 6. `xtls` — legacy v2ray field, no sing-box counterpart.
 * 7. shadowsocks `method` carrying base64("method:password") — decode and
 *    restore; the userinfo is the authoritative source.
 */
export function sanitizeOutbound(ob) {
  // 0) 协议字段白名单（仅对已识别类型生效）：Clash 订阅会给 ss 标 tls:true，而
  //    sing-box 的 shadowsocks/socks/http/ssh 出站没有 tls 字段 —— 带着会整份
  //    配置 FATAL。transport 只属于 vless/vmess/trojan；flow 只属于 vless；
  //    server_ports/obfs 只属于 hysteria2。
  const type0 = String(ob.type ?? '')
  const KNOWN0 = ['vless', 'vmess', 'trojan', 'hysteria2', 'tuic', 'anytls', 'shadowsocks', 'socks', 'http', 'ssh'].includes(type0)
  if (KNOWN0) {
    // uuid 形状校验：免费订阅里偶见非法 uuid，sing-box 在 initialize 阶段
    // FATAL 掉整份配置 —— 直接剔除该节点。
    if ((type0 === 'vless' || type0 === 'vmess' || type0 === 'tuic') && typeof ob.uuid === 'string' && !UUID_RE.test(ob.uuid.trim())) return null
    const tlsCapable = ['vless', 'vmess', 'trojan', 'hysteria2', 'tuic', 'anytls'].includes(type0)
    if (!tlsCapable) delete ob.tls
    if (type0 !== 'vless') delete ob.flow
    if (type0 !== 'vmess') delete ob.alter_id
    if (type0 !== 'hysteria2') delete ob.server_ports
    if (type0 !== 'hysteria2') delete ob.obfs
  }

  const tls = ob.tls
  if (tls && typeof tls === 'object' && !Array.isArray(tls)) {
    if (tls.enabled === false) delete ob.tls
    else tls.enabled = true
    if (tls.reality?.enabled === true) {
      const fp = String(tls.utls?.fingerprint ?? '').toLowerCase()
      tls.utls = { enabled: true, fingerprint: UTLS_FP.has(fp) ? fp : 'chrome' }
    } else if (tls.utls?.fingerprint && !UTLS_FP.has(String(tls.utls.fingerprint).toLowerCase())) {
      delete tls.utls
    }
  }
  if (typeof ob.flow === 'string') {
    const f = ob.flow.trim().toLowerCase()
    if (f === 'xtls-rprx-vision' || f === 'xtls-rprx-vision-udp443') ob.flow = 'xtls-rprx-vision'
    else delete ob.flow
  }
  if (ob.transport && typeof ob.transport === 'object') {
    const t = String(ob.transport.type ?? '').toLowerCase()
    if (t === '' || t === 'tcp' || t === 'raw') delete ob.transport
    // 5b) 未知传输（xhttp 等 Xray 专属）会让 sing-box 在 decode 阶段 FATAL 掉
    //     整份配置 —— 只能丢弃该节点（等价 Free-Router 的"明确剔除"，不静默降级）。
    else if (!['ws', 'grpc', 'http', 'httpupgrade', 'quic'].includes(t)) return null
  }
  delete ob.xtls
  if (ob.type === 'shadowsocks' && typeof ob.method === 'string' && !SS_METHODS.has(ob.method.toLowerCase())) {
    try {
      const dec = Buffer.from(ob.method.replace(/%3D/gi, '='), 'base64').toString('utf8')
      const i = dec.indexOf(':')
      if (i > 0 && SS_METHODS.has(dec.slice(0, i).toLowerCase())) {
        ob.method = dec.slice(0, i)
        ob.password = dec.slice(i + 1)
      }
    } catch { /* leave as-is; sing-box rejects it explicitly */ }
  }
  return ob
}

/**
 * Full config: one mixed inbound per PORTED node (tags `in-0…in-N`), the
 * catch-all inbound, the matching outbounds, and one inbound->outbound rule
 * per node. Nodes without a port (port range exhausted) are omitted entirely.
 */
export function buildConfig(outbounds, ports, { catchAllPort }) {
  const byTag = new Map((outbounds ?? []).map(o => [o?.tag, o]))
  const list = []
  let i = 0
  for (const [tag, port] of Object.entries(ports)) {
    const ob = byTag.get(tag)
    if (!ob) continue
    list.push({ tag, port, ob, inTag: `in-${i++}` })
  }
  return {
    log: { level: 'warn' },
    // Local DNS + explicit resolver: direct-outbound domains (catch-all traffic)
    // and domain-server nodes must resolve, or the tunnel connects and the TLS
    // handshake silently hangs (measured: no DNS section => every probe 000).
    dns: { servers: [{ type: 'local', tag: 'local-dns' }] },
    inbounds: [
      ...list.map(e => ({ type: 'mixed', tag: e.inTag, listen: '127.0.0.1', listen_port: e.port })),
      { type: 'mixed', tag: CATCHALL_TAG, listen: '127.0.0.1', listen_port: catchAllPort },
    ],
    outbounds: [...list.map(e => e.ob), { type: 'direct', tag: 'direct' }],
    route: {
      rules: list.map(e => ({ inbound: [e.inTag], outbound: e.tag })),
      final: 'direct',
      default_domain_resolver: 'local-dns',
    },
  }
}

/** Atomic config write (temp + rename), so a restart never reads a half file. */
export function writeConfig(path, obj) {
  fs.mkdirSync(path.replace(/[/\\][^/\\]+$/, ''), { recursive: true })
  const tmp = `${path}.${process.pid}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2))
  fs.renameSync(tmp, path)
}

export function startSingbox(bin = 'bin/sing-box.exe', configPath = 'data/singbox.json') {
  return spawn(bin, ['run', '-c', configPath], { stdio: ['ignore', 'pipe', 'pipe'] })
}

/** Wait until a local TCP port accepts connections (or time out). */
export function waitPort(port, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const t0 = Date.now()
    const tryOnce = () => {
      const sock = net.connect({ port, host: '127.0.0.1' })
      sock.once('connect', () => { sock.destroy(); resolve(true) })
      sock.once('error', () => {
        sock.destroy()
        if (Date.now() - t0 > timeoutMs) reject(new Error(`port ${port} not accepting after ${timeoutMs}ms`))
        else setTimeout(tryOnce, 250)
      })
    }
    tryOnce()
  })
}

/** Wait until a local TCP port stops accepting (freed by a killed process). */
export function waitPortFree(port, timeoutMs = 3000) {
  return new Promise(resolve => {
    const t0 = Date.now()
    const tryOnce = () => {
      const sock = net.connect({ port, host: '127.0.0.1' })
      sock.once('connect', () => {
        sock.destroy()
        if (Date.now() - t0 > timeoutMs) return resolve(false)
        setTimeout(tryOnce, 150)
      })
      sock.once('error', () => { sock.destroy(); resolve(true) })
    }
    tryOnce()
  })
}

/** 找出占用本地端口并处于 LISTENING 的进程 PID；找不到返回 null。 */
export function pidHoldingPort(port) {
  try {
    const out = execFileSync('netstat', ['-ano'], { encoding: 'utf8', timeout: 15000 })
    const line = out.split('\n').find(l => l.includes(`:${port} `) && l.includes('LISTENING'))
    if (!line) return null
    return Number(line.trim().split(/\s+/).pop())
  } catch {
    return null
  }
}

/** 查询 PID 的进程映像名（小写，如 sing-box.exe）；查不到返回 ''。 */
export function pidImageName(pid) {
  try {
    const out = execFileSync('tasklist', ['/FI', `PID eq ${pid}`], { encoding: 'utf8', timeout: 15000 })
    const line = out.split('\n').find(l => l.includes(String(pid)))
    return line ? line.trim().split(/\s+/)[0].toLowerCase() : ''
  } catch {
    return ''
  }
}

/** 强杀指定 PID。 */
export function killPid(pid) {
  try {
    execFileSync('taskkill', ['/F', '/PID', String(pid)], { stdio: 'pipe', timeout: 15000 })
    return true
  } catch {
    return false
  }
}

/**
 * Watchdog: every interval, check the process is alive and the catch-all port
 * still accepts. `onDead` fires ONCE (timer self-clears) — a leaking timer kept
 * firing on the dead proc forever and caused a rebuild storm (measured).
 */
export function watchSingbox(proc, port, onDead, { intervalMs = 30000 } = {}) {
  let dead = false
  const timer = setInterval(() => {
    if (dead) return
    if (proc.exitCode !== null || !proc.pid) {
      dead = true
      clearInterval(timer)
      onDead()
      return
    }
    const sock = net.connect({ port, host: '127.0.0.1' })
    sock.once('connect', () => sock.destroy())
    sock.once('error', () => {
      sock.destroy()
      if (!dead && (proc.exitCode !== null || !proc.pid)) {
        dead = true
        clearInterval(timer)
        onDead()
      }
    })
  }, intervalMs)
  timer.unref?.()
  return timer
}
