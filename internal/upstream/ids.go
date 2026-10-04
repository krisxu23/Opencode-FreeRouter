// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package upstream

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// SessionRe is the canonical 1.x session shape: ses_ + 12 LOWERCASE hex +
// 14 mixed-case base62. The lowercase hex is required, not incidental ——
// Go 的 regexp 与 JS 一样区分大小写,这里**不要**加 (?i);反过来
// [0-9A-Za-z]{14} 混大小写是故意的。
//
// v2 会话形状(ses_ + 64 个小写 hex,取自真 v2 CLI 二进制的 promptCacheKey
// 正则)刻意**不**实现:实测两条 wire 都回 403 FreeTierError,与所有其它
// 非规范形状一样 —— 闸门就是本正则的位置模式,没有别的(src/upstream.js:
// 202-210;曾经的 SESSION_RE_V2/isV2Session 对因无生产调用方被删除,免得
// 留下一个看起来承重的陷阱)。
var SessionRe = regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$`)

// RequestRe is the same shape under the msg_ prefix.
var RequestRe = regexp.MustCompile(`^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$`)

const base62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// idClock 把时间戳折叠进**单一原子计数器**:高 44 位毫秒 + 低 20 位毫秒内序。
// 旧实现 (lastStamp, seq) 两个原子的 Store/Add 有一个真实的撞车窗口 —— 同一
// 毫秒的第二个调用者把「换毫秒」误判到自己头上时,它的 Store(0) 会落在第一个
// 调用者的 Add 之后,把别人刚加过的序号清掉,两个 id 的 48 位头部相同,唯一性
// 只剩 14 字节随机熵兜底。单一 CAS 把「换毫秒」与「毫秒内递增」合成一个
// 不可分割的加法(_unix ms 到 2582 年都在 44 位内;序号每毫秒 100 万个足够)。
var idClock atomic.Int64

// mintID mints a fresh id: a time prefix folded into 48 bits (so ids minted in
// the same millisecond still order), then 14 random base62 characters for
// entropy. Same wire layout as src/upstream.js:147-162 (内部计数器是 Go 侧的
// 单原子实现,见 idClock)。
func mintID(prefix string, timestamp int64) string {
	var v uint64
	for {
		cur := idClock.Load()
		curMilli := int64(uint64(cur) >> 20)
		var next int64
		switch {
		case timestamp > curMilli:
			// 新毫秒从 1 起。
			next = timestamp<<20 | 1
		case timestamp == curMilli && uint64(cur)&0xFFFFF < 0xFFFFF:
			next = cur + 1
		default:
			// 时钟回拨(或本毫秒序号耗尽):沿现有时间线继续递增 —— 身份
			// 仍然唯一、头部仍然有序,只是时间前缀不再精确等于当前毫秒。
			next = cur + 1
		}
		if idClock.CompareAndSwap(cur, next) {
			// 直接用计数值:旧实现写的是 `^uint64(next)`（按位取反）。
			// 取反对正数就是把高 44 位全翻成 1 —— 头部 6 字节于是对每个 id
			// 都长得一样（0xffff…），注释承诺的「同一毫秒内仍然有序」正好
			// 反过来：后铸的 id 头部更小。任何按头部排序/比新旧的下游逻辑
			// （日志、轨迹行的时序、客户端的会话内排序）读到的都是逆序。
			v = uint64(next)
			break
		}
	}
	var head strings.Builder
	for i := 0; i < 6; i++ {
		var b [1]byte
		b[0] = byte((v >> (40 - 8*uint(i))) & 0xff)
		head.WriteString(hex.EncodeToString(b[:]))
	}
	entropy := make([]byte, 14)
	if _, err := rand.Read(entropy); err != nil {
		// crypto/rand 在 Windows 上实际不会失败;真失败了,时间派生的 id
		// 仍然形状合法,只是唯一性变弱 —— 比 panic 拖垮整个请求循环强。
		for i := range entropy {
			entropy[i] = byte(v >> (8 * uint(i%8)))
		}
	}
	return prefix + head.String() + base62From(entropy)
}

func base62From(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b))
	for _, c := range b {
		sb.WriteByte(base62[int(c)%62])
	}
	return sb.String()
}

// digestID renders a sha256 digest in the canonical identifier shape: the
// first 6 bytes as hex, bytes 6..20 as base62.
//
// 把 20 字节全渲染成 hex 会得到更长、形状不同的串,SessionRe 直接拒绝 ——
// 而闸门对坏形状的拒绝文案恰好是 "free tier can only be used from within
// OpenCode",每个出口都一样,完全不会提示是形状出了问题;那是最难查的一类
// 故障,所以摘要在构造时就必须落在正确形状上。
func digestID(prefix string, sum []byte, re *regexp.Regexp) string {
	id := prefix + hex.EncodeToString(sum[:6]) + base62From(sum[6:20])
	if re.MatchString(id) {
		return id
	}
	return ""
}

// SessionForConversation maps one downstream conversation onto one stable
// upstream session: a pure function of the id.
//
// 一个会话跨轮次、跨重启保持同一个 id —— 真客户端就是这么发的;每请求一个
// 新会话被实测换来 429 + 递增的 retry-after(src/upstream.js:164-178)。配额
// 到底按会话还是按出口 IP 计数并无定论(见 src/probe.js 的注),但稳定会话
// 规则在两种解读下都对,因为它也是真客户端发送的形状。空 id 回落 "global"
// 桶(js :180)。
func SessionForConversation(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		id = "global"
	}
	sum := sha256.Sum256([]byte("our-free-model\x00" + id))
	if out := digestID("ses_", sum[:], SessionRe); out != "" {
		return out
	}
	return mintID("ses_", time.Now().UnixMilli())
}

// RequestIDFor is stable per turn: retries of the same turn must share it.
// turnSeed 为空时没有稳定素材,退回即时铸造(js :187);摘要派生串万一过不了
// 正则,同样退回铸造而不是发一个必被 403 的形状。
func RequestIDFor(sessionID, turnSeed string) string {
	if turnSeed == "" {
		return mintID("msg_", time.Now().UnixMilli())
	}
	sum := sha256.Sum256([]byte("our-free-model-req\x00" + sessionID + "\x00" + turnSeed))
	if out := digestID("msg_", sum[:], RequestRe); out != "" {
		return out
	}
	return mintID("msg_", time.Now().UnixMilli())
}

// UserIDFor mints a fresh per-turn **request** id（x-opencode-request）。
// A stable per-session value was tried (matching the official input.user.id)
// and the gated models refused it on every exit; random-per-turn passes
// (src/upstream.js:193-200)。
//
// 名字与它铸的东西对不上:铸出的是 msg_ 前缀的**请求 id**，不是 user id。
// 官方 wire 的 input.user.id 我们根本不填。旧名沿用至今,调用点与测试都跟着
// 叫 UserIDFor —— 改名会让「这里在填 user id」的误读再传一遍,所以名字保留,
// 语义写在注释里:要 user id 的地方去别处找,这里没有。
func UserIDFor() string { return mintID("msg_", time.Now().UnixMilli()) }

// TruncateSession 把会话值绑定在 maxSessionLength 内并去首尾空白
// (src/upstream.js:435-439)。JS 的 .length/.slice 按 UTF-16 码元计;Go 侧按
// rune 计数 —— BMP 字符上等价,且不会在多字节序列中间切出半个字符。
func TruncateSession(v string) string {
	trimmed := strings.TrimSpace(v)
	if utf8.RuneCountInString(trimmed) > maxSessionLength {
		runes := []rune(trimmed)
		return string(runes[:maxSessionLength])
	}
	return trimmed
}
