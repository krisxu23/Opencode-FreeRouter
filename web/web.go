// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 FreeRouter contributors

// Package web carries the console's static assets into the binary.
//
// 资产放在仓库根的 web/ 是给活人读的（改面板不用碰 Go 代码），而 `//go:embed`
// 只能嵌本包目录及其子目录，所以由这个包把它们编进 exe —— 一个只有这一件事的包，
// 不是抽象层。
package web

import "embed"

// FS 里的两个文件与磁盘上的字节完全相同：embed 不改内容，`.gitattributes` 又
// 把换行钉成 LF，所以编进二进制的就是差分 B10 逐字节比对过的那份。
//
//go:embed index.html app.js
var FS embed.FS
