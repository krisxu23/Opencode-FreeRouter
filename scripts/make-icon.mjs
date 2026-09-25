// 生成托盘图标 launcher/icon.ico（32x32 32bpp：深蓝底 + 亮蓝箭头式渐变方块）
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const W = 32, H = 32
const xor = Buffer.alloc(W * H * 4)
const and = Buffer.alloc(W * H / 8) // 全 0 = 全不透明（透明度由 alpha 通道管）

for (let y = 0; y < H; y++) {
  for (let x = 0; x < W; x++) {
    // 圆角遮罩
    const dx = Math.min(x, W - 1 - x), dy = Math.min(y, H - 1 - y)
    if (dx + dy < 4) continue
    const i = (y * W + x) * 4
    const edge = dx === 0 || dy === 0 || dx + dy === 4
    if (edge) { xor[i] = 0xff; xor[i + 1] = 0x90; xor[i + 2] = 0x4a; xor[i + 3] = 0xff } // #4a90ff BGRA
    else if (y >= 14 && y <= 17 && x >= 7 && x <= 24) { xor[i] = 0xff; xor[i + 1] = 0xff; xor[i + 2] = 0xff; xor[i + 3] = 0xff } // 白色横杠
    else { xor[i] = 0xd0; xor[i + 1] = 0x62; xor[i + 2] = 0x1b; xor[i + 3] = 0xff } // #1b62d0
  }
}

const header = Buffer.alloc(6)
header.writeUInt16LE(0, 0); header.writeUInt16LE(1, 2); header.writeUInt16LE(1, 4)
const entry = Buffer.alloc(16)
entry.writeUInt8(W, 0); entry.writeUInt8(H, 1); entry.writeUInt8(0, 2); entry.writeUInt8(0, 3)
entry.writeUInt16LE(1, 4); entry.writeUInt16LE(32, 6)
const bmpInfo = Buffer.alloc(40)
bmpInfo.writeUInt32LE(40, 0); bmpInfo.writeInt32LE(W, 4); bmpInfo.writeInt32LE(H * 2, 8)
bmpInfo.writeUInt16LE(1, 12); bmpInfo.writeUInt16LE(32, 14)
const image = Buffer.concat([bmpInfo, xor, and])
entry.writeUInt32LE(image.length, 8); entry.writeUInt32LE(22, 12)

const out = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'launcher', 'icon.ico')
fs.writeFileSync(out, Buffer.concat([header, entry, image]))
console.log('icon written:', out, fs.statSync(out).size, 'bytes')
