/**
 * 节点注册表：订阅源抖动下的稳定层（用户模型的落地）。
 *
 *   - 每轮拉取的结果只是"增量输入"：新节点加入、重复丢弃、已有节点永不因
 *     某次拉取失败而消失（源抖动不再引起池子横跳）。
 *   - 淘汰只看探测结论：连续 N 轮（默认 8 轮 ≈ 4 小时）探测死亡才剔除；
 *     拉取成功与否不参与淘汰。
 *   - 持久化到 data/node-registry.json，重启不丢节点、不重测全量。
 *
 * @module src/registry.js
 */

import fs from 'node:fs'

const FILE = 'data/node-registry.json'
const MAX_DEAD_CONSECUTIVE = 8

let entries = new Map() // tag -> { outbound, addedAt, lastSeenAt, consecutiveDead }
let registryFile = 'data/node-registry.json'
let dirty = false

export function initRegistry(file) {
  registryFile = file
  try {
    const j = JSON.parse(fs.readFileSync(registryFile, 'utf8'))
    for (const [tag, e] of Object.entries(j.entries ?? {})) {
      if (e?.outbound?.tag === tag) entries.set(tag, e)
    }
  } catch { /* 首次启动或损坏：空表起步 */ }
}

function persist() {
  try {
    fs.mkdirSync(registryFile.replace(/[/\\][^/\\]+$/, ''), { recursive: true })
    const tmp = `${registryFile}.${process.pid}.tmp`
    fs.writeFileSync(tmp, JSON.stringify({ entries: Object.fromEntries(entries) }))
    fs.renameSync(tmp, registryFile)
    dirty = false
  } catch { /* fail-soft */ }
}

/** 增量合并：新节点加入；同 tag 的节点若 server/port/type 变了就原位更新；
 *  其余重复一律丢弃。返回统计。 */
export function mergeNodes(outbounds) {
  let added = 0
  let updated = 0
  let duplicate = 0
  const now = Date.now()
  for (const ob of outbounds ?? []) {
    if (!ob?.tag) continue
    const existing = entries.get(ob.tag)
    if (!existing) {
      entries.set(ob.tag, { outbound: ob, addedAt: now, lastSeenAt: now, consecutiveDead: 0 })
      added += 1
      continue
    }
    existing.lastSeenAt = now
    const changed = existing.outbound.type !== ob.type
      || existing.outbound.server !== ob.server
      || existing.outbound.server_port !== ob.server_port
    if (changed) {
      // 同名但换了服务器：原位更新（保留端口位与探测历史）
      existing.outbound = ob
      updated += 1
    } else {
      duplicate += 1
    }
  }
  if (added || updated) dirty = true
  return { added, updated, duplicate }
}

/** 当前注册表里的全部出站（订阅源抖动不影响本列表）。 */
export function all() {
  return [...entries.values()].map(e => e.outbound)
}

/** 探测结论回写：alive 清零连死计数，dead 累加。 */
export function markProbeResult(tag, alive) {
  const e = entries.get(tag)
  if (!e) return
  e.consecutiveDead = alive ? 0 : (e.consecutiveDead ?? 0) + 1
  dirty = true
}

/** 淘汰：连续死亡达阈值的先出；总量超上限时继续剔除死亡最多的老节点。 */
export function prune({ maxDeadConsecutive = 8, cap = 8000 } = {}) {
  const before = entries.size
  for (const [tag, e] of [...entries.entries()]) {
    if ((e.consecutiveDead ?? 0) >= maxDeadConsecutive) entries.delete(tag)
  }
  if (entries.size > cap) {
    const byDeath = [...entries.entries()]
      .sort((a, b) => (b[1].consecutiveDead ?? 0) - (a[1].consecutiveDead ?? 0) || a[1].addedAt - b[1].addedAt)
    for (const [tag] of byDeath) {
      if (entries.size <= cap) break
      entries.delete(tag)
    }
  }
  const removed = before - entries.size
  if (removed > 0 || dirty) persist()
  return removed
}

/** check 自愈剔除的坏节点：从注册表同步移除。 */
export function remove(tag) {
  if (entries.delete(tag)) dirty = true
}

export function flush() {
  if (dirty) persist()
}
