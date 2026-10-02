// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package upstream

import "strings"

// RequiredTools are the two lowercase names the gate requires to be declared.
//
// 实测 2026-09-30(两条 wire、五个真实出口的现场 A/B):必需项就是 bash 与
// read 两个名字**同时出现**,没有更多 —— 大小写敏感、顺序无关、重复无害、
// 额外名字无害,四件套与五件套也都能过(src/upstream.js:112-124)。这里以前
// 写过"需要四个名字、opencode2api 的五件套会被拒"的断言,都不成立:那些
// 失败的请求另有原因(缺 read,或头/会话/流式维度),与工具数量无关。只有
// 下面两个名字承重,所以只有它们被强制进 body;调用方真声明的 glob/grep
// 原样透传。
var RequiredTools = []string{"bash", "read"}

// toolNameOf:flat(Responses,顶层 name)与非 flat(chat,function.name)两种
// 工具形状取名不同(src/upstream.js:294-300)。function 分支不要求 trim 后
// 非空(照抄 JS:fn.name.trim() 可以是空串),顶层 name 分支要求非空。
func toolNameOf(tool any) string {
	tm, ok := tool.(map[string]any)
	if !ok {
		return "" // 数组/标量不是工具对象,与 JS 的 typeof 判定同效
	}
	if name, ok := tm["name"].(string); ok && strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	if fn, ok := tm["function"].(map[string]any); ok {
		name, _ := fn["name"].(string)
		return strings.TrimSpace(name)
	}
	return ""
}

// fingerprintKey:调用方工具名的规范键;闸门不关心的名字返回空串
// (src/upstream.js:302-307)。
func fingerprintKey(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	for _, t := range RequiredTools {
		if lower == t {
			return lower
		}
	}
	return ""
}

// falsy 近似 JS 的 !value:JSON 解码出来的 tool_choice 只有这几种"假"形态
// (缺失、null、空串、false、0);对象与非空串一律算"已有值,不覆盖"。
func falsy(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case bool:
		return !t
	case string:
		return t == ""
	case float64:
		return t == 0
	default:
		return false
	}
}

// ApplyFingerprint satisfies the free-tier fingerprint gate on body.tools。
//
// 闸门要求两个小写名 bash、read 被声明(实测 2026-09-30,见 RequiredTools)。
// 调用方声明的其它任何工具 —— 包括 glob/grep —— 原样透传:额外名字无害,
// 改写它们什么也买不到(src/upstream.js:309-324)。真正缺席的必需名以"自我
// 禁用诱饵"补入,description 逐字照抄 js :351-352 —— 模型读到的就是这句,
// 据此知道不该调它们。
//
// 必需名的大小写变体做归一而不是复制:调用方发 Bash,线上是小写 bash;
// renames 记录「线上小写名 → 调用方原名」,响应侧用 RestoreToolName 把调用方
// 的拼写恢复回去。重名(含大小写变体)去重,只留第一个(js :337)。
//
// body 就地修改。请求体一律 map[string]any + encoding/json 而不是 Go 结构体:
// 三条上游线路字段面完全不同且随上游演进,透传未知字段必须保持自动,否则
// 每加一个上游字段就要改一次 Go 类型。flat=true 是 Responses 形状({name}),
// false 是 chat 形状({function:{name}})。
func ApplyFingerprint(body map[string]any, flat bool) map[string]string {
	renames := map[string]string{}
	if body == nil {
		return renames
	}
	var tools []any
	if raw, ok := body["tools"].([]any); ok {
		tools = raw
	}
	hadClientTools := len(tools) > 0
	seen := make(map[string]bool, len(RequiredTools))
	out := make([]any, 0, len(tools)+len(RequiredTools))

	for _, tool := range tools {
		current := toolNameOf(tool)
		key := fingerprintKey(current)
		if key == "" {
			out = append(out, tool) // 闸门不关心的名字原样透传
			continue
		}
		if seen[key] {
			continue // 重名去重:第二个(含大小写变体)整条丢弃
		}
		seen[key] = true
		if current != key {
			renames[key] = current
			// 复制而不是就地改:调用方的原 tool 还要用于响应侧回放
			src, _ := tool.(map[string]any)
			clone := make(map[string]any, len(src)+1)
			for k, v := range src {
				clone[k] = v
			}
			if fn, ok := src["function"].(map[string]any); ok {
				fnClone := make(map[string]any, len(fn)+1)
				for k, v := range fn {
					fnClone[k] = v
				}
				fnClone["name"] = key
				clone["function"] = fnClone
			} else {
				clone["name"] = key
			}
			out = append(out, clone)
		} else {
			out = append(out, tool)
		}
	}

	// 缺名补诱饵:形状与 description 逐字照抄 src/upstream.js:350-352。
	for _, name := range RequiredTools {
		if seen[name] {
			continue
		}
		description := "This tool is currently unavailable and must not be used."
		parameters := map[string]any{"type": "object", "properties": map[string]any{}}
		if flat {
			out = append(out, map[string]any{
				"type":        "function",
				"name":        name,
				"description": description,
				"parameters":  parameters,
			})
		} else {
			out = append(out, map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        name,
					"description": description,
					"parameters":  parameters,
				},
			})
		}
	}

	body["tools"] = out
	if falsy(body["tool_choice"]) {
		if flat {
			body["tool_choice"] = "auto"
		} else if !hadClientTools {
			body["tool_choice"] = "none"
		}
	}
	return renames
}

// RestoreToolName restores the caller's tool spelling in a streaming delta or
// a final payload (src/upstream.js:364-367).
func RestoreToolName(name string, renames map[string]string) string {
	if len(renames) == 0 {
		return name
	}
	if orig, ok := renames[name]; ok {
		return orig
	}
	return name
}

// EnsureChatUsage forces the Chat wire onto SSE with a usage trailer (ported
// from opencode2api's ensureAnonymousChatUsage, src/upstream.js:369-387).
//
// 免费 lane 只服务 agent 形状的流式请求;非流式客户端仍拿到纯 JSON 回复,
// 因为 forward 监听器会把流折叠回来。就地修改,有改动才返回 true。
func EnsureChatUsage(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	if stream, ok := payload["stream"].(bool); !ok || !stream {
		return false // stream !== true 一律不动
	}
	if options, ok := payload["stream_options"].(map[string]any); ok {
		if include, ok := options["include_usage"].(bool); ok && include {
			return false // 已是 true:幂等
		}
		options["include_usage"] = true
		return true
	}
	payload["stream_options"] = map[string]any{"include_usage": true}
	return true
}

// IsStaleReasoningReference reports whether a 400 body describes a reasoning
// item/reference the server no longer recognizes (ported from opencode2api's
// isStaleReasoningReference, src/upstream.js:389-400).
//
// 目标短语与 gone/expired 标记**缺一不可**:仅仅提到 reasoning 的通用校验
// 错误绝不许匹配,否则会把不该重放的请求也洗掉。
func IsStaleReasoningReference(text string) bool {
	flat := strings.ToLower(text)
	if !strings.Contains(flat, "reasoning item") && !strings.Contains(flat, "reasoning reference") {
		return false
	}
	return strings.Contains(flat, "not found") || strings.Contains(flat, "expir") ||
		strings.Contains(flat, "does not exist") || strings.Contains(flat, "no longer")
}

// StripStaleReasoningInputs removes server-issued reasoning references from a
// Responses payload so a replay can succeed (ported from opencode2api's
// stripStaleReasoningInputs, src/upstream.js:402-423): drop
// previous_response_id and any input items of type "reasoning".
// 就地修改,有任何改动才返回 true。
func StripStaleReasoningInputs(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	changed := false
	if _, ok := payload["previous_response_id"]; ok {
		delete(payload, "previous_response_id")
		changed = true
	}
	input, ok := payload["input"].([]any)
	if ok {
		kept := make([]any, 0, len(input))
		for _, item := range input {
			tm, isMap := item.(map[string]any)
			if isMap {
				if t, isStr := tm["type"].(string); isStr && t == "reasoning" {
					continue
				}
			}
			kept = append(kept, item)
		}
		if len(kept) != len(input) {
			payload["input"] = kept
			changed = true
		}
	}
	return changed
}

// DeclaredToolNames returns the session-scoped tool names declared in body,
// in declaration order and deduplicated — the response side uses it to pick a
// decoy out of a real tool call (src/upstream.js:425-433).
func DeclaredToolNames(body map[string]any) []string {
	var names []string
	seen := map[string]bool{}
	tools, _ := body["tools"].([]any)
	for _, tool := range tools {
		n := toolNameOf(tool)
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		names = append(names, n)
	}
	return names
}
