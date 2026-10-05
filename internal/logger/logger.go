// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package logger is the program's only writer of the log file: a bounded
// in-memory ring the panel reads, plus an append-only file that rotates.
//
// Everything goes through one package because scripts/check.mjs enforces a
// single-writer rule for the JS version, and two writers racing on the same
// file produce interleaved, unreadable lines.
//
// 逐字对齐 src/logger.js 的三个决定（迁移时最容易"顺手优化"坏的地方）：
//   - recent() 返回**时间正序**的最近 N 行（ring.slice(-limit)），面板按数组
//     顺序渲染，最新在下——不是新序。
//   - 文件写入是**每行 open-append-close**（JS 的 appendFileSync），不持有
//     常驻句柄。持句柄在 Windows 上会让测试的 TempDir 清理与外部的
//     tail/复制全部撞 EBUSY，这正是 JS 版选择每行开合的原因。
//   - 截断按字符数切到 2000 **不追加省略号**（JS 是 slice(0, 2000)）；
//     追加省略号会让行变成 2001 字符，长度断言全部失效。
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// ringMax is how many lines the panel can scroll back through. Measured
	// against a busy probe round: 800 covers a full 65s round plus the
	// rotation chatter around it.
	ringMax = 800
	// maxFileBytes rotates the file so a long-running gateway cannot fill the
	// disk. 5MB is roughly two weeks of normal use.
	maxFileBytes = 5 << 20
	// maxMsgRunes caps one line. Truncating on runes rather than bytes keeps a
	// multi-byte character from being cut in half.
	maxMsgRunes = 2000
)

// Line is one log record. T is milliseconds since the epoch, matching the
// tracelog field of the same name so the panel renders both with one formatter.
type Line struct {
	T     int64  `json:"t"`
	Level string `json:"level"`
	Msg   string `json:"msg"`
}

var (
	mu   sync.RWMutex
	ring []Line
	file string
	// fileMu 串行化文件写:write 过去全程持 mu 做 open/write/close/rename,
	// 面板 Recent(RLock 轮询)被磁盘 IO 排队。ring 锁只保内存;size/轮转/退避
	// 的文件状态统一收进 fileMu,与 ring 锁解耦 —— fileMu 内永不取 mu。
	fileMu sync.Mutex
	size   int64
	// lastStamp 按秒缓存时间戳:800 行/轮 × Format 是浪费,同一秒内复用。
	lastStampSec  int64
	lastStampStr string
	// lastRenameFailMS 是上次 rename 失败的时刻:AV 常驻锁定时每行重试一次
	// rename 是每行一次失败 syscall,1s 内不再试。
	lastRenameFailMS int64
)

// Init points the logger at file. An empty path or an unopenable path leaves
// the ring working: the panel must still show something when the disk is full.
//
// Init 同时清空 ring：JS 版每个测试文件跑在独立进程里，天然拿到空 ring；
// Go 的包内测试共享进程，不清空会把上一个测试的行数漏进下一个断言。
func Init(f string) {
	mu.Lock()
	ring = nil
	file = f
	mu.Unlock()
	fileMu.Lock()
	defer fileMu.Unlock()
	size = 0
	lastRenameFailMS = 0
	lastStampSec = 0
	lastStampStr = ""
	if f == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
		mu.Lock()
		file = ""
		mu.Unlock()
		return
	}
	if st, err := os.Stat(f); err == nil {
		size = st.Size()
	}
}

func write(level string, parts ...any) {
	msg := join(parts)
	if r := []rune(msg); len(r) > maxMsgRunes {
		// JS 是 .slice(0, 2000)：不追加省略号，行长恰好封顶。
		msg = string(r[:maxMsgRunes])
	}
	now := time.Now()
	line := Line{T: now.UnixMilli(), Level: level, Msg: msg}

	mu.Lock()
	ring = append(ring, line)
	if len(ring) > ringMax {
		ring = ring[len(ring)-ringMax:]
	}
	fpath := file
	mu.Unlock()
	if fpath == "" {
		return
	}
	// 先写、再轮转。旧顺序是「先 rename 轮转、再 OpenFile 写这一行」：
	// rename 成功后紧接着的 OpenFile 若失败（杀软正锁着新名字、日志查看器
	// 抢在前面打开、磁盘抖一下），这一行就被整条吞掉 —— 而且当时 size 已经
	// 归零，没有任何机制会重试这一行。改成写完再轮转：rename 之后即使
	// 重开失败，丢的也只是「新建一个空文件」这件事，本行已经落在盘上。
	//
	// 代价是轮转边界后移一行（文件最多到 maxFileBytes + 一行），
	// 这与「不丢日志」相比是可以接受的取舍。
	//
	// 文件写走 fileMu,不占 ring 锁(mu):面板 Recent(RLock 轮询)不再被
	// 磁盘 IO 排队。fileMu 内永不取 mu —— Init 改 file/size 时短暂持 fileMu,
	// 不存在 ABBA。
	fileMu.Lock()
	defer fileMu.Unlock()
	// 本地时间戳按秒缓存:同秒复用 Format(800 行/探测轮是浪费)。
	sec := now.Unix()
	var stamp string
	if sec == lastStampSec {
		stamp = lastStampStr
	} else {
		stamp = now.Format("2006-01-02 15:04:05")
		lastStampSec, lastStampStr = sec, stamp
	}
	f, err := os.OpenFile(fpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return // fail-soft: the next line retries
	}
	n, err := fmt.Fprintf(f, "%s [%s] %s\n", stamp, level, msg)
	_ = f.Close()
	if err != nil {
		return
	}
	size += int64(n)
	if size < maxFileBytes {
		return
	}
	// 与 JS 同款单文件轮转：rename 到 <base>.old.log（Windows 的
	// MoveFileEx REPLACE_EXISTING 语义与 libuv 一致，可直接覆盖）。
	//
	// R10:只有 rename **成功**才把计数归零。JS 版无论成败都归零,于是
	// 目标 .old.log 被占用(MoveFileEx 失败)时计数被清零、继续往没轮转
	// 的同一个文件追加 ⇒ gateway.log 无界增长。失败时保留 size,下一行
	// 会再试一次 rename;只有真轮转过去,size 才重新从零开始计。
	//
	// 失败退避:AV 常驻锁定时每行重试一次 rename 是每行一次失败 syscall,
	// 1s 内不再试。
	if time.Now().UnixMilli()-lastRenameFailMS < 1000 {
		return
	}
	if err := os.Rename(fpath, strings.TrimSuffix(fpath, ".log")+".old.log"); err == nil {
		size = 0
	} else {
		lastRenameFailMS = time.Now().UnixMilli()
	}
}

// Info records a normal event.
func Info(parts ...any) { write("info", parts...) }

// Warn records something the operator should look at but that is not fatal.
func Warn(parts ...any) { write("warn", parts...) }

// Error records a failure. It never panics and never returns an error: logging
// must not be able to break the call it is describing.
func Error(parts ...any) { write("error", parts...) }

// Recent returns up to limit lines in chronological order (oldest of the
// window first), exactly matching the JS version's `ring.slice(-limit)` and
// the panel's render order. limit <= 0 returns nothing.
func Recent(limit int) []Line {
	mu.RLock()
	defer mu.RUnlock()
	if limit <= 0 || len(ring) == 0 {
		return nil
	}
	n := limit
	if n > len(ring) {
		n = len(ring)
	}
	out := make([]Line, n)
	copy(out, ring[len(ring)-n:])
	return out
}

// join concatenates parts with a single space, matching the JS version's
// `parts.join(' ')` so log text stays greppable across the two builds.
func join(parts []any) string {
	strs := make([]string, 0, len(parts))
	for _, p := range parts {
		strs = append(strs, fmt.Sprint(p))
	}
	return strings.Join(strs, " ")
}
