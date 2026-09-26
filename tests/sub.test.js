import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { countryOf, bucketOf, filterByGroups, parseSubscriptionBody, parseNodeUri, loadCache, saveCache, GROUPS } from '../src/sub.js'

test('countryOf: 四级识别（按订阅 tag 形态）', () => {
  // freesub 括号英文名（第一级）
  assert.equal(countryOf('🇹🇼 中国台湾 (Taiwan) 01 (家宽) - Example-Sub'), 'TW')
  assert.equal(countryOf('🇺🇸 美国 (United States) 03 - Example-Sub'), 'US')
  // ISO 码 token：标签里可能有两个国家码，取最后一个 → 取最后一个码
  assert.equal(countryOf('🇨🇳 Example-Node - TW-Wuri-249853-qqvu'), 'TW')
  assert.equal(countryOf('🇰🇷 Example-Node - KR-Incheon-h-416613-qquk'), 'KR')
  assert.equal(countryOf('🇯🇵 Example-Node - JP-Tokyo-2301147-qqvg'), 'JP')
  assert.equal(countryOf('HK-Central-h-2001728-qquu'), 'HK')
  // 中文关键词
  assert.equal(countryOf('🇺🇸 美国 01'), 'US')
  assert.equal(countryOf('香港 IEPL-01'), 'HK')
  // 无任何线索
  assert.equal(countryOf('Forum-Node-xx'), '')
})

test('bucketOf: 美日港台韩新直映、欧洲聚合、其余归其他', () => {
  assert.equal(bucketOf('US'), 'US')
  assert.equal(bucketOf('JP'), 'JP')
  assert.equal(bucketOf('HK'), 'HK')
  assert.equal(bucketOf('TW'), 'TW')
  assert.equal(bucketOf('KR'), 'KR')
  assert.equal(bucketOf('SG'), 'SG')
  assert.equal(bucketOf('NL'), 'EU')
  assert.equal(bucketOf('FR'), 'EU')
  assert.equal(bucketOf('TR'), 'OTHER')
  assert.equal(bucketOf(''), 'OTHER')
  assert.deepEqual(GROUPS, ['US', 'JP', 'HK', 'TW', 'KR', 'SG', 'EU', 'OTHER'])
})

test('filterByGroups: 分组匹配、无名节点随"其他"纳入探测、分组类出站剔除', () => {
  const obs = [
    { type: 'vless', tag: 'TW-Taipei-01', server: 'a', server_port: 1 },
    { type: 'vmess', tag: 'US-LA-02', server: 'b', server_port: 2 },
    { type: 'selector', tag: '🚀 选择', server: 'c', server_port: 3 },
    { type: 'ss', tag: 'Forum-UnknownNode', server: 'd', server_port: 4 },
  ]
  const { picked, stats } = filterByGroups(obs, ['TW', 'OTHER'])
  assert.deepEqual(picked.map(o => o.tag), ['TW-Taipei-01', 'Forum-UnknownNode'])
  assert.deepEqual(stats, { total: 3, matched: 1, unknown: 1, unknownKept: 1 })
  // 未选"其他"时无名节点不保留
  const { picked: p2 } = filterByGroups(obs, ['TW'])
  assert.deepEqual(p2.map(o => o.tag), ['TW-Taipei-01'])
})

test('parseNodeUri: vless reality / ss SIP002 / vmess ws / hysteria2 mport', () => {
  const vless = parseNodeUri('vless://b831381d-6324-4d53-ad4f-8cda48b30811@example.com:443?security=reality&sni=www.example.com&pbk=SbVKOEMjK0sIlbwg4akyBg5mL5KZwwB-ed4eEE7YnRc&sid=6ba85179&fp=chrome&flow=xtls-rprx-vision#TW-%E8%8A%82%E7%82%B91')
  assert.equal(vless.type, 'vless')
  assert.equal(vless.flow, 'xtls-rprx-vision')
  assert.equal(vless.tls.reality.public_key, 'SbVKOEMjK0sIlbwg4akyBg5mL5KZwwB-ed4eEE7YnRc')
  assert.equal(vless.tag, 'TW-节点1')

  const ss = parseNodeUri('ss://YWVzLTI1Ni1nY206cGFzc3dvcmQ=@1.2.3.4:8388#US-node')
  assert.equal(ss.type, 'shadowsocks')
  assert.equal(ss.method, 'aes-256-gcm')
  assert.equal(ss.password, 'password')

  const vmessJson = Buffer.from(JSON.stringify({ v: '2', ps: 'JP-01', add: '5.6.7.8', port: '443', id: 'u-1', aid: 0, net: 'ws', path: '/ws', host: 'cdn.example.com', tls: 'tls' })).toString('base64')
  const vmess = parseNodeUri(`vmess://${vmessJson}`)
  assert.equal(vmess.type, 'vmess')
  assert.equal(vmess.server, '5.6.7.8')
  assert.deepEqual(vmess.transport, { type: 'ws', path: '/ws', headers: { Host: 'cdn.example.com' } })
  assert.equal(vmess.tls.enabled, true)

  const hy2 = parseNodeUri('hy2://passw0rd@9.9.9.9:36712?sni=hk.example.com&insecure=1&mport=30000-40000#HK-hy2')
  assert.equal(hy2.type, 'hysteria2')
  assert.deepEqual(hy2.server_ports, ['30000:40000']) // freesub 实测: 裸单端口会 FATAL
  assert.equal(hy2.server_port, undefined)
})

test('parseSubscriptionBody: sing-box JSON / Clash YAML / Base64 链接表', () => {
  const sb = parseSubscriptionBody(JSON.stringify({ outbounds: [{ type: 'vless', tag: 'a', server: 'x', server_port: 1 }], log: { level: 'warn' } }))
  assert.equal(sb.length, 1)

  const clash = parseSubscriptionBody([
    'proxies:',
    '  - name: "美国 01"',
    '    type: ss',
    '    server: 1.1.1.1',
    '    port: 8388',
    '    cipher: aes-128-gcm',
    '    password: pw',
    '  - name: "日本 ws"',
    '    type: vless',
    '    server: jp.example.com',
    '    port: 443',
    '    uuid: uuid-1',
    '    network: ws',
    '    tls: true',
    '    servername: jp.example.com',
    '    ws-opts:',
    '      path: /wspath',
  ].join('\n'))
  assert.equal(clash.length, 2)
  assert.equal(clash[0].method, 'aes-128-gcm')
  assert.equal(clash[1].transport.path, '/wspath')
  assert.equal(clash[1].tls.server_name, 'jp.example.com')

  const link = 'vless://u-2@example.org:443#SG-01'
  const b64 = Buffer.from(link).toString('base64')
  const links = parseSubscriptionBody(b64)
  assert.equal(links.length, 1)
  assert.equal(links[0].tag, 'SG-01')
})

test('cache roundtrip through temp dir', () => {
  const file = path.join(os.tmpdir(), `lite-sub-test-${process.pid}.json`)
  const sub = { outbounds: [{ tag: 'x (Taiwan) 01' }], fetchedAt: 123, source: 'u' }
  saveCache(sub, file)
  assert.deepEqual(loadCache(file), sub)
  fs.rmSync(file, { force: true })
})
