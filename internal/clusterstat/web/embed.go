package web

import "embed"

//go:embed index.html logs.html metrics.html
var FS embed.FS
