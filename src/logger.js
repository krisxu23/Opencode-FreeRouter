/**
 * Logging: in-memory ring buffer + append-only file with size-capped rotation
 * (Free-Router logs.go 的环形缓冲 + 面板展示模式，按本项目的规模裁剪)。
 *
 * Every line is mirrored to stdout so the launcher's redirection
 * (data/gateway.log) keeps working unchanged — the ring powers the panel's
 * 日志 section, the file is the durable copy for post-mortem debugging.
 *
 * @module src/logger.js
 */

import fs from 'node:fs'
import path from 'node:path'

const RING_MAX = 800
const MAX_BYTES = 5 * 1024 * 1024

const ring = []
let logFile = ''
let fileSize = 0

export function initLogger(file) {
  try {
    fs.mkdirSync(path.dirname(file), { recursive: true })
    logFile = file
    fileSize = fs.existsSync(file) ? fs.statSync(file).size : 0
  } catch {
    logFile = '' // no file logging; the ring still works
  }
}

function fmt(value) {
  if (typeof value === 'string') return value
  try { return JSON.stringify(value) ?? String(value) } catch { return String(value) }
}

function write(level, parts) {
  const msg = parts.map(fmt).join(' ').slice(0, 2000)
  const t = Date.now()
  ring.push({ t, level, msg })
  if (ring.length > RING_MAX) ring.shift()

  const text = new Date(t).toISOString().slice(0, 19).replace('T', ' ') + ` [${level}] ${msg}\n`
  try { console.log(text.trimEnd()) } catch { /* stdout closed (shutdown) */ }

  if (!logFile) return
  try {
    fs.appendFileSync(logFile, text)
    fileSize += Buffer.byteLength(text)
    if (fileSize > MAX_BYTES) {
      fs.renameSync(logFile, logFile.replace(/\.log$/, '.old.log'))
      fileSize = 0
    }
  } catch { /* fail-soft: the next line retries */ }
}

export const logger = {
  info: (...parts) => write('info', parts),
  warn: (...parts) => write('warn', parts),
  error: (...parts) => write('error', parts),
  /** Most recent lines, oldest first. */
  recent: (limit = 300) => ring.slice(-limit),
}
