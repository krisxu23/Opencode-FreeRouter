import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { countryOf, filterByCountries, loadCache, saveCache } from '../src/sub.js'

test('country from tag parenthetical (real freesub tags)', () => {
  assert.equal(countryOf('🇹🇼 中国台湾 (Taiwan) 01 (家宽) - Example-Sub'), 'TW')
  assert.equal(countryOf('🇺🇸 美国 (United States) 03 - Example-Sub'), 'US')
  assert.equal(countryOf('auto'), '')
})

test('filter keeps selected countries and drops groups/duplicates', () => {
  const rows = [
    { tag: 'a (Taiwan) 01', type: 'vless' },
    { tag: 'b (Japan) 01', type: 'vless' },
    { tag: 'a (Taiwan) 01', type: 'vless' },
    { tag: 'select', type: 'selector' },
    { tag: 'c (Taiwan) 02', type: 'ss' },
  ]
  const picked = filterByCountries(rows, ['tw'])
  assert.deepEqual(picked.map(o => o.tag), ['a (Taiwan) 01', 'c (Taiwan) 02'])
})

test('cache roundtrip through temp dir', () => {
  const file = path.join(os.tmpdir(), `lite-sub-test-${process.pid}.json`)
  const sub = { outbounds: [{ tag: 'x (Taiwan) 01' }], fetchedAt: 123, source: 'u' }
  saveCache(sub, file)
  assert.deepEqual(loadCache(file), sub)
  fs.rmSync(file, { force: true })
})
