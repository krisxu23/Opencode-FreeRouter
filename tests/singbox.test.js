import test from 'node:test'
import assert from 'node:assert/strict'
import { assignPorts, sanitizeOutbound, buildConfig, CATCHALL_TAG } from '../src/singbox.js'

test('ports: existing keys keep their port, vanished keys are pruned', () => {
  const a = { tag: 'a' }
  const b = { tag: 'b' }
  const t1 = assignPorts([a, b], {}, { base: 21000 })
  assert.deepEqual(t1, { a: 21000, b: 21001 })
  const t2 = assignPorts([b], t1)
  assert.deepEqual(t2, { b: 21001 })
  const t3 = assignPorts([{ tag: 'c' }], t2, { base: 21000 })
  assert.deepEqual(t3, { c: 21002 })
})

test('sanitize: tls block without enabled is treated as enabled (panic guard)', () => {
  const ob = sanitizeOutbound({ type: 'vless', tls: { server_name: 'x' } })
  assert.equal(ob.tls.enabled, true)
  const off = sanitizeOutbound({ type: 'vless', tls: { enabled: false } })
  assert.equal(off.tls, undefined)
})

test('sanitize: reality forces a known uTLS fingerprint', () => {
  const ob = sanitizeOutbound({ type: 'vless', tls: { server_name: 'x', reality: { enabled: true, public_key: 'k' } } })
  assert.deepEqual(ob.tls.utls, { enabled: true, fingerprint: 'chrome' })
  const fp = sanitizeOutbound({ type: 'vless', tls: { reality: { enabled: true }, utls: { enabled: true, fingerprint: 'Firefox' } } })
  assert.equal(fp.tls.utls.fingerprint, 'firefox')
})

test('sanitize: unknown uTLS fingerprint dropped (v2ray unsafe)', () => {
  const ob = sanitizeOutbound({ type: 'vless', tls: { enabled: true, utls: { enabled: true, fingerprint: 'unsafe' } } })
  assert.equal(ob.tls.utls, undefined)
  assert.equal(ob.tls.enabled, true)
})

test('sanitize: flow normalization (none dropped, vision-udp443 renamed)', () => {
  assert.equal(sanitizeOutbound({ flow: 'none' }).flow, undefined)
  assert.equal(sanitizeOutbound({ flow: 'xtls-rprx-vision-udp443' }).flow, 'xtls-rprx-vision')
  assert.equal(sanitizeOutbound({ flow: 'xtls-rprx-vision' }).flow, 'xtls-rprx-vision')
})

test('sanitize: bare-TCP transport marker dropped, unknown transport kept', () => {
  assert.equal(sanitizeOutbound({ transport: { type: 'tcp' } }).transport, undefined)
  assert.equal(sanitizeOutbound({ transport: { type: 'raw' } }).transport, undefined)
  assert.deepEqual(sanitizeOutbound({ transport: { type: 'ws', path: '/p' } }).transport, { type: 'ws', path: '/p' })
})

test('sanitize: legacy xtls field removed', () => {
  assert.equal(sanitizeOutbound({ xtls: { enabled: true } }).xtls, undefined)
})

test('sanitize: base64("method:password") shadowsocks method restored', () => {
  const bad = Buffer.from('aes-128-gcm:secretpw').toString('base64')
  const ob = sanitizeOutbound({ type: 'shadowsocks', method: bad, password: 'ignored' })
  assert.equal(ob.method, 'aes-128-gcm')
  assert.equal(ob.password, 'secretpw')
})

test('buildConfig: per-node inbounds, one rule each, catch-all final direct', () => {
  const outbounds = [{ tag: 'n1', type: 'vless' }, { tag: 'n2', type: 'ss' }]
  const ports = assignPorts(outbounds, {}, { base: 21000 })
  const cfg = buildConfig(outbounds, ports, { catchAllPort: 20900 })
  assert.equal(cfg.inbounds.length, 3)
  assert.equal(cfg.inbounds[0].listen_port, 21000)
  assert.equal(cfg.inbounds[2].tag, CATCHALL_TAG)
  assert.deepEqual(cfg.route.rules, [
    { inbound: ['in-0'], outbound: 'n1' },
    { inbound: ['in-1'], outbound: 'n2' },
  ])
  assert.equal(cfg.route.final, 'direct')
  assert.equal(cfg.outbounds.at(-1).type, 'direct')
})
