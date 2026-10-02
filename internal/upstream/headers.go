// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

package upstream

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// headerOverrides 解析 OUR_FREE_MODEL_HEADERS_JSON(src/upstream.js:76-92)。
//
// 闸门的阈值至少动过两次(2026-09-16、2026-09-27),每次都要重新跑一轮现场
// A/B 才能找回;这些覆盖项让下一次变动只需改配置而不是重新编译 exe:
//
//	OUR_FREE_MODEL_UA           - 替换 user-agent 的值
//	OUR_FREE_MODEL_HEADERS_JSON - 合并到默认头之上的 JSON 对象
//
// 畸形 JSON 必须**忽略**而不是致命(js :82 的中文注释):可选覆盖项里的一个
// 笔误不能拦住网关启动,默认值本来就是实测能过的那组。JS 在模块加载时读
// 一次;Go 侧改为每次调用时解析 —— 环境变量在进程生命周期内不变,行为一致,
// 代价只是每请求一次极小的 JSON 解析,且 t.Setenv 的测试因此可行。
func headerOverrides() map[string]string {
	raw := os.Getenv("OUR_FREE_MODEL_HEADERS_JSON")
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil // 不是致命错误:覆盖项写错时应当退回默认值,而不是拒绝启动
	}
	if len(parsed) == 0 {
		return nil // JSON null / 空对象与"没有覆盖"同义
	}
	out := make(map[string]string, len(parsed))
	for key, value := range parsed {
		name := strings.ToLower(strings.TrimSpace(key))
		if name == "" || value == nil {
			continue // js :88:空键与 null 值跳过
		}
		out[name] = jsString(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// jsString 近似 JS 的 String(value):字符串原样,布尔 true/false,数字按最短
// 十进制格式。头覆盖的值实际上只会是字符串;其余类型给个稳定表示即可。
func jsString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

// Config is what the fingerprint layer is actually using, for diagnostics.
type Config struct {
	UserAgent  string
	ClientKind string
	Tools      []string
	Headers    map[string]string
	Overridden []string
}

// FingerprintConfig 供启动日志显示哪些头被环境变量覆盖了(src/upstream.js:
// 99-110),让一个失效的旧覆盖项在日志里可见。Overridden 排序输出:Go map
// 无序,诊断列表稳定可比对 JS 的插入序更重要。
func FingerprintConfig() Config {
	ov := headerOverrides()
	overridden := make([]string, 0, len(ov)+1)
	if os.Getenv("OUR_FREE_MODEL_UA") != "" {
		overridden = append(overridden, "user-agent")
	}
	for name := range ov {
		overridden = append(overridden, name)
	}
	sort.Strings(overridden)
	headers := make(map[string]string, len(ov))
	for name, value := range ov {
		headers[name] = value
	}
	tools := make([]string, len(RequiredTools))
	copy(tools, RequiredTools)
	return Config{
		UserAgent:  UserAgent(),
		ClientKind: ClientKind,
		Tools:      tools,
		Headers:    headers,
		Overridden: overridden,
	}
}

// HeaderOptions carries the per-request values GatewayHeaders needs。
// Accept 为零值时按 Stream 推导(js :285 的 accept ?? (stream ? …)):
// Go 的零值即"未提供";JS 的 ?? 对空串不回退是 JS 特性,Go 调用方不存在
// "故意发空 accept"的场景。
type HeaderOptions struct {
	Session       string
	RequestID     string
	ParentSession string
	Stream        bool
	Accept        string
}

// GatewayHeaders returns the headers the gateway fingerprints a genuine
// desktop client by (src/upstream.js:276-292)。
//
// 实测 2026-09-30(两条 wire 现场A/B,见
// docs/reviews/REVIEW-opencode2dsh-comparison.md §6),闸门真正检查的只有:
//
//   - user-agent —— 必须含空白分隔的 `opencode/<2-3 段点分数字>` token,
//     大小写不敏感、任意位置;裸 opencode/1.18.31 是能过的最小值,某些客户端
//     发的平台三元组形式也能过,所以三元组是装饰不是要求。
//   - 会话值 —— 必须精确匹配 SessionRe,且必须到达 x-opencode-session(或
//     X-Session-Id);两处都有时 x-opencode-session 优先:那里放垃圾值,
//     X-Session-Id 再规范也救不回来。
//   - body 里的 stream:true,加上必需工具名。
//
// 其余头全是惰性的:去掉 authorization、x-opencode-client、x-opencode-request
// 或 x-opencode-project 依然回 200;保留是因为真客户端会发且零成本。旧代码
// 同时发两族头,因"真客户端发不出这种形状"被回退 —— 实测既不加分也不扣分,
// 只保留单族是因为多余的头什么都买不到,不是因为它们有害。
// Authorization: Bearer public 是池化公共凭据 —— 这条 lane 没有每用户密钥;
// x-opencode-client 在这是 desktop(OpenChamber 路径;CLI 默认是 cli,见
// runtime-flags.ts)。
func GatewayHeaders(o HeaderOptions) map[string]string {
	headers := map[string]string{
		"content-type":       "application/json",
		"authorization":      "Bearer public",
		"user-agent":         UserAgent(),
		"x-opencode-client":  ClientKind,
		"x-opencode-session": o.Session,
		"x-opencode-request": o.RequestID,
		"x-opencode-project": "global",
	}
	if o.Accept != "" {
		headers["accept"] = o.Accept
	} else if o.Stream {
		headers["accept"] = "text/event-stream"
	} else {
		headers["accept"] = "*/*"
	}
	for name, value := range headerOverrides() {
		headers[name] = value // 覆盖键已归一成小写,直接压过默认值
	}
	if o.ParentSession != "" {
		headers["x-parent-session-id"] = o.ParentSession
	}
	return headers
}
