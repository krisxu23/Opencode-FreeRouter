// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package limits 是免费车道按出口 IP 的配额视图:每个出口 IP 用了多少、
// 上限多少、在途多少、最近一次落在哪个模型上。
//
// 为什么按出口 IP 计:用户 m09171 拍板 —— 换 IP 额度就是全新的。所以 Row
// 的主键是原样 IP,引擎 stats 里的 IP 字段是唯一依据,不做任何 trim/归一化
// (任何归一化都会把两个真实不同的出口串成一行;与 node-registry 墓碑键
// 的「大小写敏感、禁规范化」是同一条纪律)。
//
// 引擎依赖用 any + 运行期断言注入而不是直接 import:limits 在 L1,engine
// 在 L4,分层不允许反向 import;测试也因此能用 fake 注入。
package limits

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"freerouter/internal/persistence"
)

// Row 是一个出口 IP 的配额快照行。JSON 形状与总纲 §5 的 snake_case 风格
// 一致,落盘(data/limits.json)与读回共用这一份结构。
type Row struct {
	ExitIP   string `json:"exit_ip"`
	Used     int    `json:"used"`
	Cap      int    `json:"cap"`
	InFlight int    `json:"in_flight"`
	Model    string `json:"model"`
	At       int64  `json:"at"`
}

// historyTTL:jsonl 历史保留 7 天,过期行在追加时顺手裁掉。
const historyTTL = 7 * 24 * time.Hour

// statsEngine 是引擎被依赖的最小运行期形状。任何持有它的类型只要实现
// `Stats(ctx) (any, error)` 就能喂给 limits,避免 L1 import L4。
type statsEngine interface {
	Stats(context.Context) (any, error)
}

// Limits 持有按原样 IP 键控的配额行。New 不读盘,Load 显式调用。
type Limits struct {
	mu     sync.Mutex
	file   string
	engine any
	rows   map[string]Row
}

// New 返回一个 limits 实例。file 是快照 JSON 路径(常规 data/limits.json),
// 历史文件由此派生为同目录同名 .jsonl;file 为空表示纯内存态(测试用)。
// engine 是运行期断言成 statsEngine 的任意值。
func New(file string, engine any) *Limits {
	return &Limits{file: file, engine: engine, rows: map[string]Row{}}
}

// historyFile 由快照路径派生:data/limits.json → data/limits.jsonl。
func (l *Limits) historyFile() string {
	if l.file == "" {
		return ""
	}
	return strings.TrimSuffix(l.file, filepath.Ext(l.file)) + ".jsonl"
}

// Load 从快照恢复内存态。文件不存在是首次运行,不是错误;坏文件是错误,
// 因为静默丢弃会让「上次明明有数据」变成无法排查的凭空消失。
func (l *Limits) Load() error {
	if l.file == "" {
		return nil
	}
	var snap struct {
		At   int64 `json:"at"`
		Rows []Row `json:"rows"`
	}
	err := persistence.ReadJSONFile(l.file, &snap)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	rows := make(map[string]Row, len(snap.Rows))
	for _, r := range snap.Rows {
		rows[r.ExitIP] = r // 键 = 原样 IP,不归一化
	}
	l.mu.Lock()
	l.rows = rows
	l.mu.Unlock()
	return nil
}

// Refresh 从引擎拉一次 stats 并整体替换当前快照。
//
// 期望的 stats 形状是 {"usage":{ip:{used,cap,in_flight,model,at}}}。失败
// (引擎报错、形状不对)时保留旧数据并返回 error —— 清空会让面板把
// 「暂时查不到」显示成「额度全满」。成功后顺手落盘快照与 jsonl 历史,
// 两者的写失败都静默:它们是重启加速与观测数据,不是状态本体。
func (l *Limits) Refresh(ctx context.Context) error {
	eng, ok := l.engine.(statsEngine)
	if !ok {
		return fmt.Errorf("limits: engine (%T) 未实现 Stats(context.Context) (any, error)", l.engine)
	}
	raw, err := eng.Stats(ctx)
	if err != nil {
		return fmt.Errorf("limits: 引擎 stats 拉取失败: %w", err)
	}
	// 走一遍 JSON 往返再解析:engine 返回 any(通常是 map[string]any),
	// 序列化一次换来对键名/数值类型的严格校验,坏形状在这里现形。
	b, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("limits: 序列化 stats: %w", err)
	}
	var payload struct {
		Usage map[string]struct {
			Used     int    `json:"used"`
			Cap      int    `json:"cap"`
			InFlight int    `json:"in_flight"`
			Model    string `json:"model"`
			At       int64  `json:"at"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return fmt.Errorf("limits: 解析 stats: %w", err)
	}
	if payload.Usage == nil {
		return fmt.Errorf("limits: stats 缺少 usage 对象")
	}
	now := time.Now().UnixMilli()
	rows := make(map[string]Row, len(payload.Usage))
	for ip, u := range payload.Usage {
		at := u.At
		if at == 0 {
			at = now
		}
		rows[ip] = Row{ExitIP: ip, Used: u.Used, Cap: u.Cap, InFlight: u.InFlight, Model: u.Model, At: at}
	}
	l.mu.Lock()
	l.rows = rows // 整体替换,不是合并:消失的出口必须消失
	l.mu.Unlock()
	l.saveSnapshot(now)
	l.appendHistory(now)
	return nil
}

// Total 合计所有出口的 used 与 cap。cap==0 的行不计入 cap:未知额度不等于
// 无限额度,把「没披露」合计成无穷大会让面板的余量显示凭空翻倍。
func (l *Limits) Total() (used, cap int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.rows {
		used += r.Used
		if r.Cap > 0 {
			cap += r.Cap
		}
	}
	return used, cap
}

// Snapshot 返回全部行的深拷贝,按新→旧排序(At 降序;同刻按 IP,排序
// 稳定可断言)。Row 全是值字段,逐值拷贝即是深拷贝。
func (l *Limits) Snapshot() []Row {
	l.mu.Lock()
	out := make([]Row, 0, len(l.rows))
	for _, r := range l.rows {
		out = append(out, r)
	}
	l.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].At != out[j].At {
			return out[i].At > out[j].At
		}
		return out[i].ExitIP < out[j].ExitIP
	})
	return out
}

// InFlightDelta 调整一个出口的在途计数(d 可正可负)。exitIP 为空直接忽略,
// 与 JS noteExitBusy 对空 IP 的处理一致。
//
// 两个不可让步的性质:
//   - 不删除归零条目:删掉再建会把并发 InFlightDelta(+1) 的那一次丢掉
//     (src/health.js releaseExitBusy 的同一教训);条目由下一轮 Refresh
//     整体替换时自然回收。
//   - 计数不许为负:Dec 多于 Inc 是调用方 bug,钳到 0 而不是让面板显示 -1。
func (l *Limits) InFlightDelta(exitIP string, d int) {
	if exitIP == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	row := l.rows[exitIP]
	row.ExitIP = exitIP
	row.InFlight += d
	if row.InFlight < 0 {
		row.InFlight = 0
	}
	if row.At == 0 {
		row.At = time.Now().UnixMilli()
	}
	l.rows[exitIP] = row
}

// saveSnapshot 把当前快照原子写到 data/limits.json。写失败静默:内存态
// 已经更新成功,快照只是「重启后不用等下一轮 Refresh」的加速;Refresh 的
// error 语义保留给引擎拉取失败。
func (l *Limits) saveSnapshot(now int64) {
	if l.file == "" {
		return
	}
	snap := struct {
		At   int64 `json:"at"`
		Rows []Row `json:"rows"`
	}{At: now, Rows: l.Snapshot()}
	_ = persistence.WriteJSONFile(l.file, snap, true)
}

// appendHistory 追加一行快照摘要到 data/limits.jsonl,并顺手裁掉 7 天前的
// 旧行(照 JS saveHistory 的保留窗口语义)。整个文件按「读-滤-重写」处理:
// 裁剪本来就需要重写,统一成一条路径;追加失败静默 —— 历史是观测数据,
// 不是状态,写失败最多损失几行摘要。
func (l *Limits) appendHistory(now int64) {
	hist := l.historyFile()
	if hist == "" {
		return
	}
	rows := l.Snapshot()
	used, cap := l.Total()
	line, err := json.Marshal(struct {
		At    int64 `json:"at"`
		Exits int   `json:"exits"`
		Used  int   `json:"used"`
		Cap   int   `json:"cap"`
	}{At: now, Exits: len(rows), Used: used, Cap: cap})
	if err != nil {
		return
	}
	cut := now - historyTTL.Milliseconds()
	var fresh []string
	for _, ln := range readLines(hist) {
		var probe struct {
			At int64 `json:"at"`
		}
		if json.Unmarshal([]byte(ln), &probe) == nil && probe.At > 0 && probe.At < cut {
			continue // 7 天窗口之外,丢弃
		}
		fresh = append(fresh, ln)
	}
	fresh = append(fresh, string(line))
	_ = writeFileText(hist, strings.Join(fresh, "\n")+"\n")
}

// readLines 读整个历史文件为非空行;文件不存在/读失败按空处理(首行历史)。
func readLines(file string) []string {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil
	}
	lines := strings.Split(string(b), "\n")
	out := make([]string, 0, len(lines))
	for _, ln := range lines {
		ln = strings.TrimRight(ln, "\r")
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}

// writeFileText 用临时文件 + rename 原子替换文本文件(jsonl 无法走
// persistence.WriteJSONFile 的 JSON 通道,但原子性纪律一致)。
//
// 临时名必须唯一(B5):固定用 file+".tmp" 时两个并发写者会抢同一个路径,
// 先 rename 走的人把后来者的目标抽走,后者 rename 报 ENOENT 或 Windows
// 共享冲突。os.CreateTemp 的随机后缀 + O_EXCL 从根上排除了撞名。
func writeFileText(file, content string) error {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, filepath.Base(file)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
