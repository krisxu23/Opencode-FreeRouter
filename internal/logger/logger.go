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
//
// 掉电裁决(六审 F11,有意为之):每行 Close 不含 FlushFileBuffers(Go 的
// Windows Close 就是 CloseHandle,见 internal/poll/fd_windows.go),数据进的是
// 系统缓存而不是介质 —— 进程崩溃/被杀一行不丢,掉电或 BSOD 丢最后 ~1s 窗口
// 的行。日志是排障素材,不是交易账本,这个窗口不值得为每行一次 fsync 的代价
// (每行多一次内核往返);账本类文件(stats/registry/health)全部走
// persistence.WriteJSONFile,那条路径有数据 fsync 与目录 fsync。
package logger

import (
	"errors"
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
	// hardCapFactor 是轮转失败期间的增长封顶:rename 退避只限重试频率,
	// 不解封顶 —— 杀软/备份常驻锁住 .old.log 一整天时,退避期行照写、
	// size 照涨,5MB 的轮转目标变成数十 MB 直到同盘写满(第六轮审计 F2:
	// 连带 stats 落盘与 route 证据一起死,正是轮转要防的事故)。越过
	// 2×maxFileBytes 后这一路停写文件(内存 ring 不受限),恢复探测降到
	// 每分钟一次,成功即恢复;每挡一小时打一行 Warn,不静默。
	hardCapFactor = 2
	// renameProbeInterval 是封顶后的恢复探测节奏。
	renameProbeInterval = time.Minute
)

// writeLine 是「往已打开的文件写一行」的测试接缝:生产恒为真写。真盘上
// 「open 成功而写失败」只有盘满对既有文件/坏盘能造出来,测试无法进这个象限;
// 与 nodeprobe 的 echoBudgetMS/backstopSlackMS 同款做法(第八轮 R3 中-3 的
// 回归钉子)。
var writeLine = func(f *os.File, s string) (int, error) { return fmt.Fprintf(f, "%s", s) }

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
	// file 只在 mu 里读写(Recent 与 write 都经它)。
	file string
	// fileMu 串行化文件写:write 过去全程持 mu 做 open/write/close/rename,
	// 面板 Recent(RLock 轮询)被磁盘 IO 排队。ring 锁只保内存;size/轮转/退避
	// 的文件状态统一收进 fileMu,与 ring 锁解耦 —— **fileMu 内永不取 mu**,
	// 这条契约靠 lock-free 的 failFile() 兑现(Init 的 MkdirAll 失败路径
	// 曾在 fileMu 里套 mu,第六轮审计 F1:当前全仓无反向边所以不死锁,
	// 但它是留给后人的 ABBA 引信)。
	//
	// 代价(六审 F3,接受):ring append 与文件落盘不再同锁,两个并发 write 的
	// 行在**盘上**可以比 ring 里乱序 —— 每行 open-append-close 仍原子,行不
	// 会被撕;判序以每行的 t 字段为准,不以任何一方的物理顺序为准。面板读
	// 的是 ring,不受影响。
	fileMu sync.Mutex
	size   int64
	// lastStamp 按秒缓存时间戳:800 行/轮 × Format 是浪费,同一秒内复用。
	lastStampSec int64
	lastStampStr string
	// lastRenameFail 是上次 rename 失败的时刻。存 time.Time 而不是毫秒数:
	// time.Since 走单调时钟读数,墙钟被向后调(或 NTP 校正)一次也不会把退避
	// 窗口拉成「永久不再试」—— 旧写法用 UnixMilli 差值,墙钟回拨后差值恒小
	// 于 1s,rename 从此每一行都被退避吃掉,文件永不轮转(第六轮审计 F2 附雷)。
	lastRenameFail time.Time
	// capped 是硬封顶已启用的标志(见 hardCapFactor)。fileMu 域。
	capped bool
	// lastCapWarn/nextProbe 是封顶期的节流记账(也归 fileMu)。
	lastCapWarn time.Time
	nextProbe   time.Time
	// lastOpenWarn 节流「打开日志文件失败」的告警(一小时一行)。这条路径
	// 过去是完全静默的:恢复成功(rename 掉旧文件)之后若 O_CREATE 也被拒
	// (目录只读、盘满、AV 隔离新名字),capped 已清、size 停在 0 —— 封顶
	// 机制的告警再也不介入,故障由响亮立刻变永久静默(第七轮复审 A)。
	lastOpenWarn time.Time
	// lastWriteWarn 节流「写入日志文件失败」的告警(一小时一行)。open 成功
	// 而写失败(盘满对既有文件、坏盘 IO error)时 n=0、size 不涨,轮转与封顶
	// 机制都推不到 —— open 侧的 lastOpenWarn 管不到这一半(第八轮 R3 中-3):
	// 没有这条告警,文件日志的死是无声的。
	lastWriteWarn time.Time
)

// failFile 把日志文件降级为「只写 ring」。独立函数、只取 mu:Init 的建目录
// 失败路径需要在 fileMu **之外**改 file,直接在锁里 mu.Lock 就构成了
// fileMu→mu 的嵌套边(见 fileMu 的契约注释)。
func failFile() {
	mu.Lock()
	file = ""
	mu.Unlock()
}

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
	// MkdirAll 是文件系统调用,放任何锁外都安全(不与并发 writer 共享状态,
	// 失败时只降级一次)。放进 fileMu 内再套 mu 就是 F1 的引信形状。
	if f != "" {
		if err := os.MkdirAll(filepath.Dir(f), 0o755); err != nil {
			failFile()
			return
		}
	}
	fileMu.Lock()
	defer fileMu.Unlock()
	size = 0
	lastRenameFail = time.Time{}
	lastStampSec = 0
	lastStampStr = ""
	capped = false
	nextProbe = time.Time{}
	lastCapWarn = time.Time{}
	lastOpenWarn = time.Time{}
	lastWriteWarn = time.Time{}
	if f == "" {
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
	// 磁盘 IO 排队。fileMu 内永不取 mu(契约见 var 块)——封顶期的 Warn
	// 不能在锁内调 write→mu,一律攒到 pendingWarn、放锁后再发。
	fileMu.Lock()
	// 硬封顶:越过 2×阈值还轮转不动(AV/备份常驻锁定 .old.log),停文件写
	// 保 ring;恢复探测降频到每分钟,成功即开闸(第六轮审计 F2)。
	if capped {
		recovered := false
		if now.After(nextProbe) {
			err := os.Rename(fpath, strings.TrimSuffix(fpath, ".log")+".old.log")
			switch {
			case err == nil:
				size = 0
				capped = false
				lastRenameFail = time.Time{}
				recovered = true // 这一行不再被封顶丢弃,落到下面的正常写路径
			case errors.Is(err, os.ErrNotExist):
				// 源文件已经不在了 —— 杀软 quarantine 的动作就是删文件,正是
				// 本机制要对付的场景。继续对不存在的源做 Rename 只会永远
				// ENOENT,而能重建文件的 O_CREATE 路径只在 recovered 时到达
				// ⇒ capped 永为 true、文件写永久停摆,只剩每小时一行内存 ring,
				// 面板之外完全看不到(第七轮复审 A)。源不在就不是「轮转被
				// 挡住」,归零放开,让下面 O_CREATE 立刻重建。
				size = 0
				capped = false
				lastRenameFail = time.Time{}
				recovered = true
			default:
				nextProbe = now.Add(renameProbeInterval)
			}
		}
		if !recovered {
			var pending string
			if capped && now.Sub(lastCapWarn) >= time.Hour {
				lastCapWarn = now
				pending = fmt.Sprintf("[logger] 轮转持续失败,gateway.log 已封顶于 %d MB(仅写内存 ring,磁盘写暂停;恢复探测 %v 一次)", size>>20, renameProbeInterval)
			}
			fileMu.Unlock()
			if pending != "" {
				Warn(pending) // 放锁后再发:write 内不可嵌套取 fileMu
			}
			return
		}
	}
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
		// fail-soft: the next line retries。但不能**静默**:恢复轮转之后这里
		// 失败的话(size 已归零、capped 已清)封顶机制的告警不会再介入,
		// 故障会从响亮变永久静默。一小时一行,放锁后再发(fileMu 内不可取 mu)。
		var pending string
		if now.Sub(lastOpenWarn) >= time.Hour {
			lastOpenWarn = now
			pending = fmt.Sprintf("[logger] 打开日志文件失败,本行只进内存 ring: %v", err)
		}
		fileMu.Unlock()
		if pending != "" {
			Warn(pending)
		}
		return
	}
	n, werr := writeLine(f, fmt.Sprintf("%s [%s] %s\n", stamp, level, msg))
	_ = f.Close()
	size += int64(n)
	// 写失败(n=0,盘满对既有文件/坏盘/AV 锁字节段)时 size 不涨,轮转与封顶
	// 机制都推不到 —— 故障会从响亮变永久静默(第八轮 R3 中-3),文件日志的死
	// 只剩内存 ring 能看见。一小时一行,放锁后再发(fileMu 内不可取 mu)。
	var writePending string
	if werr != nil && now.Sub(lastWriteWarn) >= time.Hour {
		lastWriteWarn = now
		writePending = fmt.Sprintf("[logger] 写日志文件失败(size=%d 停滞,轮转/封顶均触不到,本行只进内存 ring): %v", size, werr)
	}
	finish := func() {
		fileMu.Unlock()
		if writePending != "" {
			Warn(writePending)
		}
	}
	// 只看 size 决定要不要轮转:部分写/写失败(n>0 而 err!=nil)同样入账后
	// 走这条 ——「每行都部分失败」的坏盘恰恰是唯一会越过阈值还不进轮转/
	// 封顶分支的形状,短路它就等于把无界增长留给了 F4 要防的事故。
	if size < maxFileBytes {
		finish()
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
	// 1s 内不再试。退避用单调时钟差(time.Since),墙钟回拨不会把窗口拉成
	// 永久。越过硬封顶则转入上面的 capped 分支。
	if !lastRenameFail.IsZero() && now.Sub(lastRenameFail) < time.Second {
		finish()
		return
	}
	if err := os.Rename(fpath, strings.TrimSuffix(fpath, ".log")+".old.log"); err == nil {
		size = 0
		lastRenameFail = time.Time{}
		finish()
	} else if errors.Is(err, os.ErrNotExist) {
		// 源已被外部删除(见 capped 分支的同类注释):不当作轮转失败,归零
		// 继续写,下一行 O_CREATE 就把文件重建出来。若不特判,它会一路走
		// 「保留 size + 退避 + 最终 capped」,而源永远不存在 ⇒ 永久停摆。
		size = 0
		lastRenameFail = time.Time{}
		finish()
	} else {
		lastRenameFail = now
		if size >= int64(hardCapFactor)*maxFileBytes {
			capped = true
			nextProbe = now.Add(renameProbeInterval)
		}
		finish()
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
