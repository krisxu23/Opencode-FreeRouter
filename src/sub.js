/**
 * 订阅拉取（多源并行、全量合并）+ 多格式解析 + 国家分桶。
 *
 * 支持的订阅格式（参考 freesub scripts/main_v2.py 的解析器逐字段移植）：
 *   1. sing-box JSON（outbounds 数组）
 *   2. Clash / Clash.Meta YAML（proxies 列表，常见字段映射）
 *   3. Base64 整段编码的节点链接列表（v2rayN 形态）
 *   4. 明文节点链接（vless/vmess/trojan/ss/hy2/tuic/anytls/ssh/socks）
 *
 * 国家检测双管齐下（freesub 思路的本机化）：
 *   - 静态：tag 四级识别（freesub 括号英文名 → ISO 码 token → 中英文关键词 → 旗帜）
 *   - 动态：探测时按出口 IP 实测（nodeprobe 的 echo 源），实测国籍覆盖 tag 推断
 * 没有名字的节点不会被丢弃 —— 纳入"其他"桶参与探测，由实测国籍归桶。
 *
 * @module src/sub.js
 */

import fs from 'node:fs'
import path from 'node:path'
import YAML from 'yaml'
import { parseLinks } from './parse-links.js'

export { parseNodeUri, parseLinks } from './parse-links.js'

export const DEFAULT_SOURCES = [
  'https://cdn.jsdelivr.net/gh/krisxu23/freesub@main/output/singbox.json',
  'https://gh-proxy.com/https://raw.githubusercontent.com/krisxu23/freesub/main/output/singbox.json',
  'https://raw.githubusercontent.com/krisxu23/freesub/main/output/singbox.json',
]

export const COUNTRY_SUB = c => `https://cdn.jsdelivr.net/gh/krisxu23/freesub@main/output/by-country/singbox-${c}.json`

/** 出口地区固定分组（面板按此多选，顺序即回退顺序）。 */
export const GROUPS = ['US', 'JP', 'HK', 'TW', 'KR', 'SG', 'EU', 'OTHER']

/** 欧洲桶包含的国家码。 */
const EU_CCS = new Set(['NL', 'DE', 'GB', 'FR', 'SE', 'CH', 'AT', 'PL', 'ES', 'IT', 'IE', 'FI', 'NO', 'UA', 'RO', 'BG', 'GR', 'HU', 'CZ', 'DK', 'BE', 'PT'])

/** 无名节点纳入探测的数量上限（避免超大订阅把入站撑爆）。 */
const UNKNOWN_KEEP_LIMIT = 80

/** freesub 括号英文名 → ISO-3166 alpha-2。 */
const NAME2CC = {
  'Taiwan': 'TW', 'Hong Kong': 'HK', 'Japan': 'JP', 'Singapore': 'SG',
  'United States': 'US', 'Netherlands': 'NL', 'Germany': 'DE', 'United Kingdom': 'GB',
  'France': 'FR', 'Canada': 'CA', 'South Korea': 'KR', 'Korea': 'KR',
  'Turkey': 'TR', 'Thailand': 'TH', 'Australia': 'AU', 'Russia': 'RU',
  'Malaysia': 'MY', 'India': 'IN', 'Vietnam': 'VN', 'Philippines': 'PH',
  'Brazil': 'BR', 'Argentina': 'AR', 'Chile': 'CL', 'Mexico': 'MX',
  'Sweden': 'SE', 'Switzerland': 'CH', 'Austria': 'AT', 'Poland': 'PL',
  'Spain': 'ES', 'Italy': 'IT', 'Ireland': 'IE', 'Finland': 'FI', 'Norway': 'NO',
  'Ukraine': 'UA', 'Romania': 'RO', 'Bulgaria': 'BG', 'Greece': 'GR', 'Hungary': 'HU',
  'Czechia': 'CZ', 'Denmark': 'DK', 'Belgium': 'BE', 'Portugal': 'PT',
  'Israel': 'IL', 'UAE': 'AE', 'Bangladesh': 'BD', 'Pakistan': 'PK',
  'Indonesia': 'ID', 'Cambodia': 'KH', 'Laos': 'LA', 'Myanmar': 'MM',
  'New Zealand': 'NZ', 'South Africa': 'ZA', 'Kazakhstan': 'KZ',
}

/** 中文/英文地名关键词（兜底层级，在 ISO 码 token 之后）。 */
const KEYWORDS = [
  [/香港|Hong ?Kong/i, 'HK'], [/台湾|臺灣|Taiwan/i, 'TW'], [/日本|Japan/i, 'JP'],
  [/新加坡|獅城|狮城|Singapore/i, 'SG'], [/韩国|韓國|Korea/i, 'KR'],
  [/美国|美國|United States|Los ?Angeles|San ?Jose|Dallas|Seattle/i, 'US'],
  [/英国|英國|United ?Kingdom|London/i, 'GB'], [/德国|德國|Germany|Frankfurt/i, 'DE'],
  [/法国|法國|France|Paris/i, 'FR'], [/加拿大|Canada|Toronto/i, 'CA'],
  [/土耳其|Turkey/i, 'TR'], [/泰国|Thailand/i, 'TH'], [/澳大利亚|澳洲|Australia/i, 'AU'],
  [/俄罗斯|Russia/i, 'RU'], [/马来西亚|Malaysia/i, 'MY'], [/印度(?!尼)|India/i, 'IN'],
  [/越南|Vietnam/i, 'VN'], [/菲律宾|Philippines/i, 'PH'], [/巴西|Brazil/i, 'BR'],
  [/荷兰|Netherlands|Amsterdam/i, 'NL'], [/印尼|Indonesia/i, 'ID'],
  [/瑞典|Sweden/i, 'SE'], [/瑞士|Switzerland/i, 'CH'], [/波兰|Poland/i, 'PL'],
  [/西班牙|Spain/i, 'ES'], [/意大利|Italy/i, 'IT'], [/爱尔兰|Ireland/i, 'IE'],
]

const PROXY_TYPES = new Set(['vless', 'vmess', 'trojan', 'shadowsocks', 'ss', 'hysteria2', 'hy2', 'tuic', 'anytls', 'ssh', 'socks', 'http'])
const ISO_TOKEN_RE = /(?:^|[^A-Z])([A-Z]{2})(?=[^A-Z]|$)/g
const FLAG_RE = /[\u{1F1E6}-\u{1F1FF}]{2}/u

function flagToCC(flag) {
  const cps = [...flag].map(c => c.codePointAt(0))
  if (cps.length !== 2) return ''
  return String.fromCharCode(cps[0] - 0x1F1E6 + 65) + String.fromCharCode(cps[1] - 0x1F1E6 + 65)
}

function isoTokens(tag) {
  const out = []
  let m
  const re = new RegExp(ISO_TOKEN_RE.source, 'g')
  while ((m = re.exec(tag)) !== null) {
    if (CC_KNOWN.has(m[1])) out.push(m[1])
  }
  return out
}

const CC_KNOWN = new Set(['US', 'JP', 'HK', 'TW', 'KR', 'SG', 'NL', 'DE', 'GB', 'FR', 'CA', 'TR', 'TH', 'AU', 'RU', 'MY', 'IN', 'VN', 'PH', 'BR', 'AR', 'CL', 'MX', 'SE', 'CH', 'AT', 'PL', 'ES', 'IT', 'IE', 'FI', 'NO', 'UA', 'RO', 'BG', 'GR', 'HU', 'CZ', 'DK', 'BE', 'PT', 'IL', 'AE', 'BD', 'PK', 'ID', 'KH', 'LA', 'MM', 'NZ', 'ZA', 'KZ', 'CN'])

/** ISO 码 token 提取：按 " - " 分段取最后一段、段内取第一个码 ——
 *  "🇨🇳 Example-Node - TW-Wuri-…" 的标签名里带国家码、真实国家 TW 在后；
 *  "US-LA-02"（洛杉矶）的 LA 是老挝，国家码打头。两种实测形态都覆盖。 */
function isoCountry(text) {
  const segment = text.includes(' - ') ? text.split(' - ').pop() : text
  const tokens = isoTokens(segment)
  return tokens[0] ?? ''
}

/**
 * tag 四级国家识别：
 *   1. freesub 括号英文名（权威，订阅方自己标的）
 *   2. ISO 码 token —— 取"最后一个"独立两字母码（用户的订阅里论坛名 NL 在前、
 *      真实国家 TW 在后，实测首个会误判；旗帜也可能错，文本优先）
 *   3. 中英文地名关键词
 *   4. 旗帜 emoji（最不可靠，兜底）
 */
export function countryOf(tag) {
  const text = String(tag ?? '')
  const m = /\(([^()]+)\)/.exec(text)
  if (m && NAME2CC[m[1].trim()]) return NAME2CC[m[1].trim()]
  const iso = isoCountry(text)
  if (iso) return iso
  for (const [re, cc] of KEYWORDS) {
    if (re.test(text)) return cc
  }
  const flag = FLAG_RE.exec(text)
  if (flag) {
    const cc = flagToCC(flag[0])
    if (CC_KNOWN.has(cc)) return cc
  }
  return ''
}

/** 国家码 → 出口地区分组（美日港台韩新/欧洲/其他）；空码归"其他"。 */
export function bucketOf(cc) {
  const code = String(cc ?? '').toUpperCase().slice(0, 2)
  if (GROUPS.includes(code)) return code
  if (EU_CCS.has(code)) return 'EU'
  return 'OTHER'
}

/**
 * 按所选分组过滤出站。语义：
 *   - tag 可识别国家的节点：所属分组被选中才保留
 *   - 无名节点（tag 识别不出国家）：只要选了"其他"就保留（最多 UNKNOWN_KEEP_LIMIT
 *     个，探测后由出口 IP 实测归桶）——这就是 freesub 式的运行时国家检测入口
 */
export function filterByGroups(outbounds, groups) {
  const want = new Set((groups ?? []).map(g => String(g).toUpperCase()))
  const seen = new Set()
  const matched = []
  const unknown = []
  let total = 0
  for (const o of outbounds ?? []) {
    if (!o || !PROXY_TYPES.has(String(o.type ?? ''))) continue // selector/urltest/direct/block 不进池
    if (seen.has(o.tag)) continue
    seen.add(o.tag)
    total += 1
    const cc = countryOf(o.tag)
    if (cc === '') {
      unknown.push(o)
      continue
    }
    if (want.has(bucketOf(cc))) matched.push(o)
  }
  const unknownKept = want.has('OTHER') ? unknown.slice(0, UNKNOWN_KEEP_LIMIT) : []
  return {
    picked: [...matched, ...unknownKept],
    stats: { total, matched: matched.length, unknown: unknown.length, unknownKept: unknownKept.length },
  }
}

// ---- 多格式订阅体解析 --------------------------------------------------------

export function parseSubscriptionBody(text) {
  const body = String(text ?? '').trim()
  if (!body) return []
  if (body.startsWith('{')) {
    try {
      const j = JSON.parse(body)
      if (Array.isArray(j?.outbounds) && j.outbounds.length) return j.outbounds
    } catch { /* fall through */ }
  }
  if (/^\s*proxies:\s*$/m.test(body) || body.includes('\nproxies:') || body.startsWith('proxies:')) {
    try {
      const doc = YAML.parse(body)
      if (Array.isArray(doc?.proxies) && doc.proxies.length) {
        return doc.proxies.map(clashProxyToOutbound).filter(Boolean)
      }
    } catch { /* fall through */ }
  }
  const links = parseLinks(body)
  if (links.length) return links
  return []
}

const CLASH_TYPE = { ss: 'shadowsocks', vmess: 'vmess', vless: 'vless', trojan: 'trojan', hysteria2: 'hysteria2', hy2: 'hysteria2', tuic: 'tuic', anytls: 'anytls', socks5: 'socks', http: 'http' }

/** Clash proxy 字段 → sing-box 出站（常用字段子集，本仓库 sanitize 会再兜底）。 */
export function clashProxyToOutbound(p) {
  if (!p || typeof p !== 'object' || !p.server || !p.name) return null
  const type = CLASH_TYPE[String(p.type ?? '').toLowerCase()]
  if (!type) return null
  const ob = { type, tag: String(p.name), server: String(p.server), server_port: Number(p.port) }
  if (!(ob.server_port > 0)) return null
  if (type === 'shadowsocks') {
    ob.method = String(p.cipher ?? '').toLowerCase()
    ob.password = String(p.password ?? '')
  }
  if (type === 'vmess') {
    ob.uuid = String(p.uuid ?? '')
    ob.security = String(p.cipher ?? 'auto')
    if (Number(p.alterId ?? 0) > 0) ob.alter_id = Number(p.alterId)
  }
  if (type === 'vless') {
    ob.uuid = String(p.uuid ?? '')
    if (p.flow) ob.flow = String(p.flow)
  }
  if (type === 'trojan' || type === 'hysteria2' || type === 'anytls') ob.password = String(p.password ?? p.auth ?? '')
  if (type === 'tuic') {
    ob.uuid = String(p.uuid ?? '')
    ob.password = String(p.password ?? '')
    ob.congestion_control = String(p['congestion-control'] ?? 'bbr')
    ob.udp_relay_mode = String(p['udp-relay-mode'] ?? 'native')
  }
  if (type === 'socks') ob.version = '5'
  if (p.username) ob.username = String(p.username)
  if (p.password && type !== 'shadowsocks' && type !== 'vless' && type !== 'vmess' && type !== 'trojan' && type !== 'hysteria2' && type !== 'anytls' && type !== 'tuic') ob.password = String(p.password)

  const TLS_CAPABLE = ['vless', 'vmess', 'trojan', 'hysteria2', 'tuic', 'anytls'].includes(type)
  const needsTls = TLS_CAPABLE && (p.tls === true || ['trojan', 'hysteria2', 'tuic', 'anytls'].includes(type))
  if (needsTls) {
    ob.tls = {
      enabled: true,
      server_name: String(p.servername || p.sni || p.server),
      insecure: p['skip-cert-verify'] === true,
      ...(Array.isArray(p.alpn) && p.alpn.length ? { alpn: p.alpn.map(String) } : {}),
    }
    if (p['client-fingerprint']) ob.tls.utls = { enabled: true, fingerprint: String(p['client-fingerprint']) }
    if (p['reality-opts']?.['public-key']) {
      ob.tls.reality = { enabled: true, public_key: String(p['reality-opts']['public-key']), short_id: String(p['reality-opts']['short-id'] ?? '') }
    }
  }
  const net = String(p.network ?? '')
  if (net === 'ws') {
    const t = { type: 'ws' }
    if (p['ws-opts']?.path) t.path = String(p['ws-opts'].path)
    const host = p['ws-opts']?.headers?.Host
    if (host) t.headers = { Host: String(host) }
    ob.transport = t
  } else if (net === 'grpc') {
    const sn = p['grpc-opts']?.['grpc-service-name']
    ob.transport = { type: 'grpc', ...(sn ? { service_name: String(sn) } : {}) }
  } else if (net === 'h2') {
    ob.transport = { type: 'http', ...(p['h2-opts']?.path ? { path: String(p['h2-opts'].path) } : {}) }
  }
  return ob
}

// ---- 拉取与合并 ---------------------------------------------------------------

/** 拉取全部订阅源并合并（不再"首个成功即用"）——多订阅共存，按 tag+server 去重。 */
export async function fetchSub({ sources = DEFAULT_SOURCES, signal } = {}) {
  const list = (sources ?? DEFAULT_SOURCES).map(s => String(s).trim()).filter(Boolean)
  if (!list.length) throw new Error('no subscription sources configured')
  const results = await Promise.allSettled(list.map(async url => {
    const r = await fetch(url, { redirect: 'follow', signal, headers: { accept: 'application/json, text/yaml, text/plain, */*', 'user-agent': 'clash.meta/1.18.1' } })
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    const outbounds = parseSubscriptionBody(await r.text())
    if (!outbounds.length) throw new Error('没有识别出任何节点（格式不受支持？）')
    return outbounds
  }))
  const merged = []
  const seen = new Set()
  const details = []
  for (const [i, res] of results.entries()) {
    if (res.status === 'fulfilled') {
      let nodes = 0
      for (const o of res.value) {
        const key = `${o.tag}|${o.type}|${o.server}:${o.server_port}`
        if (seen.has(key)) continue
        seen.add(key)
        merged.push(o)
        nodes += 1
      }
      details.push({ url: list[i], ok: true, nodes })
    } else {
      details.push({ url: list[i], ok: false, error: String(res.reason?.message ?? res.reason).slice(0, 120) })
    }
  }
  if (!merged.length) {
    throw new Error('全部订阅源均失败: ' + details.map(d => `${d.url.slice(0, 60)} → ${d.error ?? ''}`).join(' | ').slice(0, 400))
  }
  return { outbounds: merged, fetchedAt: Date.now(), sources: list, details }
}

/** Last-good subscription cache (atomic temp+rename), for offline boots. */
export function loadCache(file) {
  try {
    const j = JSON.parse(fs.readFileSync(file, 'utf8'))
    return Array.isArray(j?.outbounds) && j.outbounds.length > 0 ? j : null
  } catch {
    return null
  }
}

export function saveCache(sub, file) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const tmp = `${file}.${process.pid}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(sub))
  fs.renameSync(tmp, file)
}
