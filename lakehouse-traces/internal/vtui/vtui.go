// Package vtui embeds VictoriaTraces' own web UI (VTUI), the trace explorer that
// replaced the log-based UI in VictoriaTraces v0.12.0, for lakehouse-traces to
// serve at /select/vmui/ with the Lakehouse tab injected (internal/ui).
//
// The bundle is VictoriaTraces' build output, copied unmodified from
// lakehouse-traces/deps/VictoriaTraces/app/vtselect/vmui by `make
// sync-vmui-traces` (and by the same inline copy in Dockerfile.traces). Only
// index.html is tracked in git: it names the content-hashed asset files, which
// makes it the drift marker vtui_test.go compares against the vendored tree. The
// rest (assets/, favicon, manifest, ...) is .gitignore'd and comes from the
// vendored VictoriaTraces at build time, so the repository never carries a second
// copy of the minified bundle.
package vtui

import (
	"embed"
	"io/fs"
)

//go:embed vmui
var files embed.FS

// FS returns the VTUI bundle rooted at the upstream UI directory
// (index.html, assets/, ...).
func FS() fs.FS {
	// fs.Sub fails only on an invalid path; "vmui" is a constant.
	sub, _ := fs.Sub(files, "vmui")
	return sub
}
