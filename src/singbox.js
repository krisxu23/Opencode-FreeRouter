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
import { spawn } from 'node:child_process'

export const CATCHALL_TAG = 'in-catchall'

/** uTLS fingerprints sing-box accepts (option/uTLSFingerprint table). */
const UTLS_FP = new Set(['chrome', 'firefox', 'safari', 'ios', 'android', 'edge', '360', 'qq', 'random', 'randomized'])
/** Known Shadowsocks cipher names (used to detect the base64("method:password") malformation). */
const SS_METHODS = new Set([
  'aes-128-gcm', 'aes-192-gcm', 'aes-256-gcm', 'chacha20-ietf-poly1305', 'xchacha20-ietf-poly1305',
  'none', 'plain', '2022-blake3-aes-128-gcm', '2022-blake3-aes-256-gcm', '2022-blake3-chacha20-poly1305',
])

/**
 * Stable port table: existing keys keep their port (no drift across rebuilds),
 * new keys take the next free slot, vanished keys are pruned (an ever-growing
 * table was a measured incident in Free-Router: 27k stale entries).
 */
export function assignPorts(outbounds, prev = {}, { base = 21000, max = 29000 } = {}) {
  const table = { ...prev }
  const used = new Set(Object.values(table))
  let next = base
  for (const o of outbounds) {
    if (table[o.tag] == null) {
      while (used.has(next) && next < max) next += 1
      table[o.tag] = next
      used.add(next)
    }
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
 * Full config: one mixed inbound per node (tags `in-0…in-N`), the catch-all
 * inbound, node outbounds + direct, and one inbound->outbound rule per node.
 */
export function buildConfig(outbounds, ports, { catchAllPort }) {
  const list = outbounds.map((o, i) => ({ tag: o.tag, port: ports[o.tag], inTag: `in-${i}` }))
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
    outbounds: [...outbounds, { type: 'direct', tag: 'direct' }],
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

/**
 * Watchdog: every interval, check the process is alive and the catch-all port
 * still accepts. `onDead` fires when the process is gone; the caller decides
 * the restart policy (bounded retries live in index.js).
 */
export function watchSingbox(proc, port, onDead, { intervalMs = 30000 } = {}) {
  const timer = setInterval(() => {
    if (proc.exitCode !== null || !proc.pid) {
      onDead()
      return
    }
    const sock = net.connect({ port, host: '127.0.0.1' })
    sock.once('connect', () => sock.destroy())
    sock.once('error', () => {
      sock.destroy()
      if (proc.exitCode !== null || !proc.pid) onDead()
    })
  }, intervalMs)
  timer.unref?.()
  return timer
}
