/**
 * 节点分享链接 → sing-box 出站。字段映射逐条对照 freesub scripts/main_v2.py 的
 * parse_* 系列函数移植（含其踩坑注释：hysteria2 的 mport 必须写成 "start:end"
 * 区间、裸单端口会 FATAL；vmess 的 b64 是 URL-safe；ss 兼容 SIP002 与 legacy）。
 *
 * @module src/parse-links.js
 */

const NAME_BLACKLIST = /(剩余流量|流量重置|expire|expired|官网|套餐|telegram\.me|t\.me\/|获取订阅)/i

function b64decode(input) {
  const normalized = input.replace(/-/g, '+').replace(/_/g, '/').replace(/\s/g, '')
  const padded = normalized + '='.repeat((4 - (normalized.length % 4)) % 4)
  try {
    return Buffer.from(padded, 'base64').toString('utf8')
  } catch {
    return ''
  }
}

function queryDict(query) {
  const out = {}
  for (const pair of String(query ?? '').split('&')) {
    if (!pair) continue
    const i = pair.indexOf('=')
    const key = i === -1 ? pair : pair.slice(0, i)
    try { out[key] = decodeURIComponent(pair.slice(i + 1)) } catch { out[key] = pair.slice(i + 1) }
  }
  return out
}

function nodeName(uri) {
  const hash = uri.indexOf('#')
  if (hash === -1) return ''
  try { return decodeURIComponent(uri.slice(hash + 1)) } catch { return uri.slice(hash + 1) }
}

function hostPort(hostinfo) {
  const m = /^(\[[^\]]+\]|[^:/?#]+):(\d+)/.exec(hostinfo)
  if (!m) return null
  return { server: m[1].replace(/^\[|\]$/g, ''), port: Number(m[2]) }
}

function tlsParams(params, host) {
  const security = String(params.security ?? '').toLowerCase()
  const sni = params.sni || params.peer || host
  if (security === 'reality' || params.pbk) {
    if (!params.pbk) return null // reality 缺 pbk 无法测（freesub 同规则）
    return {
      enabled: true,
      server_name: sni,
      utls: { enabled: true, fingerprint: params.fp || 'chrome' },
      reality: { enabled: true, public_key: params.pbk, short_id: params.sid || '' },
    }
  }
  if (security === 'tls' || params.tls === '1' || params.tls === 'true' || params.pbk === undefined && security === '') return null
  return {
    enabled: true,
    server_name: sni,
    insecure: params.insecure === '1' || params.allowInsecure === '1',
    ...(params.fp ? { utls: { enabled: true, fingerprint: params.fp } } : {}),
    ...(params.alpn ? { alpn: params.alpn.split(',').filter(Boolean) } : {}),
  }
}

function transportParams(params) {
  const net = String(params.type ?? '').toLowerCase()
  if (net === 'ws') {
    const t = { type: 'ws', ...(params.path ? { path: params.path } : {}) }
    if (params.host) t.headers = { Host: params.host }
    return t
  }
  if (net === 'grpc' || net === 'gun') return { type: 'grpc', ...(params.serviceName || params.path ? { service_name: params.serviceName || params.path } : {}) }
  if (net === 'h2' || net === 'http') return { type: 'http', ...(params.path ? { path: params.path } : {}), ...(params.host ? { host: [params.host] } : {}) }
  if (net === 'httpupgrade') return { type: 'httpupgrade', ...(params.path ? { path: params.path } : {}), ...(params.host ? { host: params.host } : {}) }
  return null
}

function parseVless(uri) {
  const m = /^vless:\/\/([^@#]+)@(\[[^\]]+\]|[^:@/]+):(\d+)(?:\/?\?([^#]*))?(?:#(.*))?$/.exec(uri)
  if (!m) return null
  const [, user, host, port, query] = m
  const params = queryDict(query)
  const ob = { type: 'vless', server: host.replace(/^\[|\]$/g, ''), server_port: Number(port), uuid: user }
  const flow = params.flow ?? ''
  if (flow && (flow.includes('vision') || flow.includes('xtls'))) ob.flow = flow
  const tls = tlsParams(params, ob.server)
  if (tls) ob.tls = tls
  const transport = transportParams(params)
  if (transport) ob.transport = transport
  return ob
}

function parseVmess(uri) {
  const data = JSON.parse(b64decode(uri.slice(8)))
  const server = String(data.add ?? '').trim()
  const port = Number(data.port ?? 0)
  if (!server || !(port > 0)) return null
  const ob = {
    type: 'vmess',
    server,
    server_port: port,
    uuid: String(data.id ?? '').trim(),
    security: 'auto',
  }
  const aid = Number(data.aid ?? 0)
  if (aid > 0) ob.alter_id = aid
  if (data.tls === 'tls' || data.tls === '1' || data.tls === true) {
    ob.tls = { enabled: true, server_name: String(data.sni || data.host || server).trim() }
  }
  const params = queryDict('')
  params.type = String(data.net ?? 'tcp').toLowerCase()
  params.path = data.path
  params.host = data.host
  const transport = transportParams(params)
  if (transport) ob.transport = transport
  return ob
}

function parseTrojan(uri) {
  const m = /^trojan:\/\/([^@#]+)@(\[[^\]]+\]|[^:@/]+):(\d+)(?:\/?\?([^#]*))?(?:#(.*))?$/.exec(uri)
  if (!m) return null
  const [, password, host, port, query] = m
  const params = queryDict(query)
  return {
    type: 'trojan',
    server: host.replace(/^\[|\]$/g, ''),
    server_port: Number(port),
    password: decodeURIComponentSafe(password),
    tls: {
      enabled: true,
      server_name: params.sni || host.replace(/^\[|\]$/g, ''),
      insecure: params.insecure === '1' || params.allowInsecure === '1',
      ...(params.alpn ? { alpn: params.alpn.split(',').filter(Boolean) } : {}),
    },
  }
}

function decodeURIComponentSafe(v) {
  try { return decodeURIComponent(v) } catch { return v }
}

function ssOutbound(server, port, method, password, tag) {
  return {
    type: 'shadowsocks',
    server,
    server_port: Number(port),
    method: String(method).trim().toLowerCase(),
    password,
  }
}

function parseSs(uri) {
  const body = uri.slice(5).split('#', 1)[0]
  // SIP002: ss://base64(method:password)@host:port 或 ss://method:password@host:port
  if (body.includes('@')) {
    const at = body.lastIndexOf('@')
    const userinfo = body.slice(0, at)
    const hp = hostPort(body.slice(at + 1).split('/')[0].split('?')[0])
    if (!hp) return null
    let method = ''
    let password = ''
    if (userinfo.includes(':')) {
      ;[method, password] = userinfo.split(/:(.*)/, 2)
    } else {
      const dec = b64decode(userinfo)
      if (!dec.includes(':')) return null
      ;[method, password] = dec.split(/:(.*)/, 2)
    }
    method = decodeURIComponentSafe(method)
    password = decodeURIComponentSafe(password)
    if (!method || !password) return null
    return ssOutbound(hp.server, hp.port, method, password)
  }
  // legacy: ss://base64(method:password@host:port)
  const dec = b64decode(body)
  const at = dec.lastIndexOf('@')
  if (at === -1) return null
  const [userinfo, hostinfo] = [dec.slice(0, at), dec.slice(at + 1)]
  const hp = hostPort(hostinfo.trim())
  if (!hp) return null
  const i = userinfo.indexOf(':')
  if (i === -1) return null
  return ssOutbound(hp.server, hp.port, decodeURIComponentSafe(userinfo.slice(0, i)), decodeURIComponentSafe(userinfo.slice(i + 1)))
}

function parseHysteria2(uri) {
  const prefix = uri.startsWith('hysteria2://') ? 'hysteria2://' : 'hy2://'
  const body = uri.slice(prefix.length).split('#', 1)[0]
  const at = body.lastIndexOf('@')
  if (at <= 0) return null
  const auth = body.slice(0, at)
  const m = /^(\[[^\]]+\]|[^:/?#]+):(\d+)(?:\/?\?([^#]*))?$/.exec(body.slice(at + 1))
  if (!m) return null
  const [, host, port, query] = m
  const params = queryDict(query)
  const ob = {
    type: 'hysteria2',
    server: host.replace(/^\[|\]$/g, ''),
    server_port: Number(port),
    password: decodeURIComponentSafe(auth),
    tls: {
      enabled: true,
      server_name: params.sni || params.peer || host.replace(/^\[|\]$/g, ''),
      insecure: params.insecure === '1' || params.allowInsecure === '1',
      ...(params.alpn ? { alpn: params.alpn.split(',').filter(Boolean) } : {}),
    },
  }
  if (params.obfs && params.obfs !== 'none') {
    ob.obfs = { type: params.obfs, password: params['obfs-password'] ?? '' }
  }
  const mport = params.mport || params.ports
  if (mport) {
    // freesub 实测: server_ports 只接受 "start:end" 区间, 裸单端口会 FATAL
    const ranges = []
    for (const part of String(mport).split(',')) {
      const piece = part.trim()
      if (!piece) continue
      if (piece.includes('-')) {
        const [a, b] = piece.split('-')
        if (/^\d+$/.test(a.trim()) && /^\d+$/.test(b.trim())) ranges.push(`${a.trim()}:${b.trim()}`)
      } else if (/^\d+$/.test(piece)) {
        ranges.push(`${piece}:${piece}`)
      }
    }
    if (ranges.length) {
      ob.server_ports = ranges
      delete ob.server_port
    }
  }
  return ob
}

function parseTuic(uri) {
  const m = /^tuic:\/\/([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):(\d+)(?:\/?\?([^#]*))?$/.exec(uri.split('#', 1)[0])
  if (!m) return null
  const [, userinfo, host, port, query] = m
  if (!userinfo.includes(':')) return null
  const [uuid, password] = userinfo.split(/:(.*)/, 2)
  const params = queryDict(query)
  return {
    type: 'tuic',
    server: host.replace(/^\[|\]$/g, ''),
    server_port: Number(port),
    uuid: decodeURIComponentSafe(uuid),
    password: decodeURIComponentSafe(password ?? ''),
    congestion_control: params.congestion_control ?? 'bbr',
    udp_relay_mode: params.udp_relay_mode ?? 'native',
    tls: {
      enabled: true,
      server_name: params.sni || host.replace(/^\[|\]$/g, ''),
      insecure: params.allow_insecure === '1' || params.insecure === '1',
      alpn: (params.alpn ?? 'h3').split(',').filter(Boolean),
    },
  }
}

function parseAnytls(uri) {
  const m = /^anytls:\/\/([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):(\d+)(?:\/?\?([^#]*))?$/.exec(uri.split('#', 1)[0])
  if (!m) return null
  const [, password, host, port, query] = m
  const params = queryDict(query)
  return {
    type: 'anytls',
    server: host.replace(/^\[|\]$/g, ''),
    server_port: Number(port),
    password: decodeURIComponentSafe(password),
    tls: {
      enabled: true,
      server_name: params.sni || host.replace(/^\[|\]$/g, ''),
      insecure: params.insecure === '1' || params.allowInsecure === '1',
      ...(params.alpn ? { alpn: params.alpn.split(',').filter(Boolean) } : {}),
    },
  }
}

function parseSsh(uri) {
  const m = /^ssh:\/\/([^@#/?]+)@(\[[^\]]+\]|[^:@/?]+):?(\d+)?/.exec(uri.split('#', 1)[0])
  if (!m) return null
  const [, userinfo, host, port] = m
  const ob = { type: 'ssh', server: host, server_port: Number(port || 22), user: decodeURIComponentSafe(userinfo.split(':')[0]) }
  if (userinfo.includes(':')) ob.password = decodeURIComponentSafe(userinfo.split(/:(.*)/, 2)[1])
  return ob
}

function parseSocks(uri) {
  const m = /^(?:socks5h?|socks):\/\/(?:([^@#/?]+)@)?(\[[^\]]+\]|[^:@/?]+):(\d+)/.exec(uri.split('#', 1)[0])
  if (!m) return null
  const [, userinfo, host, port] = m
  const ob = { type: 'socks', server: host, server_port: Number(port), version: '5' }
  if (userinfo) {
    const [user, pw] = userinfo.split(':')
    ob.username = decodeURIComponentSafe(user)
    if (pw) ob.password = decodeURIComponentSafe(pw)
  }
  return ob
}

const PARSERS = [
  ['vless://', parseVless],
  ['vmess://', parseVmess],
  ['trojan://', parseTrojan],
  ['ss://', parseSs],
  ['hysteria2://', parseHysteria2],
  ['hy2://', parseHysteria2],
  ['tuic://', parseTuic],
  ['anytls://', parseAnytls],
  ['ssh://', parseSsh],
  ['socks5://', parseSocks],
  ['socks5h://', parseSocks],
  ['socks://', parseSocks],
]

/** 解析一条节点链接 → sing-box 出站（tag 取 # 名，缺省 host:port）；不认识的 scheme 返回 null。 */
export function parseNodeUri(uri) {
  const trimmed = String(uri ?? '').trim()
  if (!trimmed) return null
  let matched = null
  let parser = null
  for (const [prefix, fn] of PARSERS) {
    if (trimmed.toLowerCase().startsWith(prefix)) {
      matched = prefix
      parser = fn
      break
    }
  }
  if (!parser) return null
  let ob
  try {
    ob = parser(trimmed)
  } catch {
    return null
  }
  if (!ob || !ob.server || !(ob.server_port > 0) && !Array.isArray(ob.server_ports)) return null
  const name = nodeName(trimmed).trim()
  ob.tag = name && !NAME_BLACKLIST.test(name) ? name : `${ob.type}:${ob.server}:${ob.server_port}`
  if (NAME_BLACKLIST.test(ob.tag)) return null
  return ob
}

/** 解析一段订阅文本里的全部节点链接（整段 base64 或明文行列表）。 */
export function parseLinks(text) {
  let body = String(text ?? '').trim()
  if (!body.includes('://')) {
    const decoded = b64decode(body)
    if (decoded.includes('://')) body = decoded
  }
  const out = []
  for (const line of body.split(/\r?\n/)) {
    const ob = parseNodeUri(line)
    if (ob) out.push(ob)
  }
  return out
}
