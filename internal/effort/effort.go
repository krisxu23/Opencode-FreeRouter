// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors
//
// Package effort resolves one model entry plus one request into a concrete
// reasoning effort and an output-token ceiling.
package effort

// Entry is the only part of a catalog row this package needs. Taking a
// narrow struct instead of the catalog row keeps effort (L1) free of any
// dependency on catalog (L1) — same layer, and the layer check forbids the
// sibling import.

import (
	"math"
	"sync"

	"freerouter/internal/logger"
)

type Entry struct {
	ID            string
	ContextWindow int64
	// MaxOutput is the model's own ceiling. It is the fallback for MaxTokens
	// whenever the settings leave it unset.
	MaxOutput int64
	// SupportsReasoning is false for models that reject a reasoning effort
	// field outright; sending one turns a 200 into a 400.
	SupportsReasoning bool
	// AlwaysThinking 置 true 表示关不掉思考(mimo 家族):推理先从同一块输出
	// 上限里扣,档位上限不翻倍的话 balanced 只给答案留约 1500 token。
	// ceilingOf 内翻倍,见下。
	AlwaysThinking bool
}

// Level is a reasoning effort level as spelled on the wire.
type Level string

const (
	LevelNone  Level = "none"
	LevelLow   Level = "low"
	LevelHigh  Level = "high"
	LevelExtra Level = "extra"
)

// DefaultLevel is the level used when the request and the settings are both
// silent. "balanced" is a virtual level meaning "medium if the model has one,
// otherwise none": it is resolved away by ResolveLevel and never sent.
const DefaultLevel = "balanced"

// 实现说明(端口语义逐字对 src/effort.js,JS 源是行为的最终事实):
//
// 词汇表换成了上面四档,两套词的对照是差分验收的契约,改动任何一侧都要同步
// 另一侧:
//
//	JS light    (ceiling 2048)          ↔ LevelLow   (2048)
//	JS balanced (ceiling 8192, 默认档)  ↔ LevelHigh  (8192;"balanced" 解析落点)
//	JS deep     (无档位上限 → 模型容量) ↔ LevelExtra
//	JS 无菜单模型(resolveLevel→undefined) ↔ LevelNone
//	JS settings.effortLevel 的实际取值 "balanced"          ↔ 虚拟档,解析后绝不外发
//
// 已知偏离:JS 对 canDisableThinking === false 的模型把每一档上限翻倍
// (ALWAYS_THINKING_FACTOR,mimo 家族实测 82% 输出被推理吃掉)。计划的 Entry
// 类型块不携带该位,本包无法表达 —— 翻倍须由上层在构造 Entry 之前补偿。
const (
	minBudget        = 512         // JS MIN_BUDGET:再低答案本身落不下来
	defaultMaxOutput = 32768       // JS model?.maxOutput ?? 32768
	levelLowCeiling  = int64(2048) // JS light.ceiling
	levelHighCeiling = int64(8192) // JS balanced.ceiling
)

// canonicalLevel 把一个档位词归一成四档之一。"balanced"、空串与未知词都是
// 虚拟默认档:模型有推理菜单时落到中档(high,与 JS balanced 同一预算),
// 否则 none。light/deep 是 JS 词汇的别名(settings 文件里存的就是那套词)。
// 未知词降级时打一次 Warn(每词一次):静默落默认让错误配置零反馈。
func canonicalLevel(l Level, e Entry) Level {
	switch string(l) {
	case string(LevelNone):
		return LevelNone
	case string(LevelLow), "light":
		return LevelLow
	case string(LevelHigh):
		return LevelHigh
	case string(LevelExtra), "deep":
		return LevelExtra
	default: // ""、"balanced"、未知词 —— 全部降级为默认,不报错
		if s := string(l); s != "" && s != DefaultLevel {
			warnUnknownLevelOnce(s)
		}
		if e.SupportsReasoning {
			return LevelHigh
		}
		return LevelNone
	}
}

var (
	unknownLevelWarnedMu sync.Mutex
	unknownLevelWarned   = map[string]bool{}
)

// unknownLevelWarnCap 是未知档词去重表的上限:档词来自客户端请求体,是
// 客户端可控键 —— 无上限等于任由调用方把内存撑大。超限后不再记(最坏多打
// 几行 warn,日志级别的影响可忽略)。
const unknownLevelWarnCap = 1024

func warnUnknownLevelOnce(word string) {
	unknownLevelWarnedMu.Lock()
	defer unknownLevelWarnedMu.Unlock()
	if unknownLevelWarned[word] {
		return
	}
	if len(unknownLevelWarned) >= unknownLevelWarnCap {
		logger.Warn("effort: 未知档位词 " + word + ",已回落默认档")
		return
	}
	unknownLevelWarned[word] = true
	logger.Warn("effort: 未知档位词 " + word + ",已回落默认档")
}

// ceilingOf 返回一个档位在某个模型上的预算上限;ok=false 表示该档没有自己的
// 上限(JS 的 ceiling undefined),容量原样通过。无菜单模型任何档位都没有
// 上限:整窗属于答案,一个从别处继承来的档位不得压缩它(JS resolveLevel 对
// supportsEffort==false 返回 undefined,后续 ceiling 查找落空 —— 两次失败
// 方式不同,见 src/effort.js 的长注释)。
func ceilingOf(l Level, e Entry) (int64, bool) {
	if !e.SupportsReasoning {
		return 0, false
	}
	switch canonicalLevel(l, e) {
	case LevelLow:
		if e.AlwaysThinking {
			return 2 * levelLowCeiling, true
		}
		return levelLowCeiling, true
	case LevelHigh:
		if e.AlwaysThinking {
			return 2 * levelHighCeiling, true
		}
		return levelHighCeiling, true
	default: // none / extra:档位继承模型容量
		return 0, false
	}
}

// EffortsFor lists the levels Entry supports, in ascending order.
//
// 可推理模型给全四档(none 在内:Entry 不携带 canDisableThinking,关不掉思考
// 的 mimo 家族在 Go 侧区分不出来);无菜单模型只给 none —— JS effortsFor 对
// 它返回 undefined(没有菜单),Go 的切片没有 undefined,单一 none 档是同一
// 句话的表达。
func EffortsFor(e Entry) []Level {
	if !e.SupportsReasoning {
		return []Level{LevelNone}
	}
	return []Level{LevelNone, LevelLow, LevelHigh, LevelExtra}
}

// BudgetFor returns the output-token ceiling for one turn. requested <= 0
// means "unspecified" and falls back to settingsDefault, then to
// e.MaxOutput. A zero budget is never returned: upstream reads max_tokens: 0
// as "produce nothing at all" and answers with an empty completion, which
// then looks like an upstream outage rather than a configuration mistake.
//
// 算术照抄 JS budgetFor:capacity = min(模型上限, requested, settingsDefault)
// 只取**正值**项(JS usableTokens:非正数 = 未设置 = Infinity,所以显式 0 与
// 「未设置」同一条路),档位上限只能压低容量、不能抬高,最后 MAX(MIN_BUDGET)
// 兜底。小数截断在 Go 里天然成立(int)。
func BudgetFor(l Level, e Entry, requested int, settingsDefault *int) int {
	capacity := e.MaxOutput
	if capacity <= 0 {
		capacity = defaultMaxOutput
	}
	if requested > 0 && int64(requested) < capacity {
		capacity = int64(requested)
	}
	if settingsDefault != nil && *settingsDefault > 0 && int64(*settingsDefault) < capacity {
		capacity = int64(*settingsDefault)
	}
	if ceiling, ok := ceilingOf(l, e); ok && ceiling < capacity {
		capacity = ceiling
	}
	// 下限不超模型上限:小模型(MaxOutput<512)被抬到 512 会超上游上限 → 400。
	// 大模型仍是 512 兜底。
	floor := int64(minBudget)
	if e.MaxOutput > 0 && e.MaxOutput < floor {
		floor = e.MaxOutput
	}
	if capacity < floor {
		capacity = floor
	}
	// int 是平台相关的:32 位构建上 MaxInt 是 2^31-1,而 capacity 来自
	// int64(e.MaxOutput)。一个上限 8 万 token 的模型（远未到 2^31）在 32 位
	// 上不会溢出,但一个上游报了荒谬 max_output、或默认上限被改成 GB 量级的
	// 场景会 —— 溢出的 int 变负数,而调用方按「非正 = 未设置」处理,于是这个
	// 预算被静默丢弃、请求不带 max_tokens。钳在 MaxInt 上,语义不变。
	if capacity > math.MaxInt {
		return math.MaxInt
	}
	return int(capacity)
}

// ResolveLevel folds the request's reasoning_effort, the settings' level and
// the model's own support into one level. Unknown levels degrade to the
// default rather than failing the request.
//
// 折叠顺序与 JS 一致:请求带的 reasoning_effort 非空就赢(哪怕是未知词 ——
// resolveLevel('turbo') 落到默认档,而不是换用设置档);为空才轮到设置档;
// 再为空用 DefaultLevel。无菜单模型没有档位,恒返回 none:整窗属于答案。
func ResolveLevel(requested string, settingsLevel string, e Entry) Level {
	if !e.SupportsReasoning {
		return LevelNone
	}
	word := requested
	if word == "" {
		word = settingsLevel
	}
	if word == "" {
		word = DefaultLevel
	}
	return canonicalLevel(Level(word), e)
}
