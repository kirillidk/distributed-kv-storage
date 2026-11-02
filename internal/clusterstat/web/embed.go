package web

import "embed"

//go:embed index.html logs.html
var FS embed.FS
