/**
 * Subscription fetch (direct, before sing-box exists — the chicken-and-egg path)
 * plus country classification from freesub tags.
 *
 * Sources are tried in order and any one answering is enough: jsDelivr first,
 * then the gh acceleration prefix, then raw direct. Fetches deliberately do NOT
 * go through sing-box — at boot it does not exist yet.
 *
 * @module src/sub.js
 */

import fs from 'node:fs'
import path from 'node:path'

export const DEFAULT_SOURCES = [
  'https://cdn.jsdelivr.net/gh/krisxu23/freesub@main/output/singbox.json',
  'https://gh-proxy.com/https://raw.githubusercontent.com/krisxu23/freesub/main/output/singbox.json',
  'https://raw.githubusercontent.com/krisxu23/freesub/main/output/singbox.json',
]

export const COUNTRY_SUB = c => `https://cdn.jsdelivr.net/gh/krisxu23/freesub@main/output/by-country/singbox-${c}.json`

/** English parenthetical in a freesub tag -> ISO-3166 alpha-2. */
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

/** First parenthetical of a tag -> country code ('' when untagged). */
export function countryOf(tag) {
  const m = /\(([^()]+)\)/.exec(String(tag ?? ''))
  if (!m) return ''
  return NAME2CC[m[1].trim()] ?? m[1].trim().toUpperCase().slice(0, 2)
}

/**
 * Keep outbounds whose tag claims one of the wanted countries; drop selector/
 * urltest/direct/block groups (their tags carry no country parenthetical) and
 * duplicate tags (first wins).
 */
export function filterByCountries(outbounds, want) {
  const set = new Set((want ?? []).map(s => String(s).toUpperCase()))
  const seen = new Set()
  return outbounds.filter(o => {
    if (!set.has(countryOf(o.tag))) return false
    if (seen.has(o.tag)) return false
    seen.add(o.tag)
    return true
  })
}

/** Fetch the full subscription; first source to answer wins. */
export async function fetchSub({ sources = DEFAULT_SOURCES, signal } = {}) {
  let lastErr
  for (const url of sources) {
    try {
      const r = await fetch(url, { redirect: 'follow', signal, headers: { accept: 'application/json', 'user-agent': 'clash.meta/1.18.1' } })
      if (!r.ok) throw new Error(`HTTP ${r.status}`)
      const j = await r.json()
      const outbounds = Array.isArray(j?.outbounds) ? j.outbounds : []
      if (!outbounds.length) throw new Error('subscription has no outbounds')
      return { outbounds, fetchedAt: Date.now(), source: url }
    } catch (error) {
      lastErr = error
    }
  }
  throw lastErr ?? new Error('no subscription sources')
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
