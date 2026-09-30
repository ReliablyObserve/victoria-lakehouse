package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed vmui
var vmuiFiles embed.FS

// RegisterVMUI registers the VMUI handler at /select/vmui/ with Lakehouse tab injection.
// The VMUI assets are embedded from internal/ui/vmui/ which should contain VL's VMUI
// build output (copied at build time from deps/VictoriaLogs/app/vlselect/vmui/).
func RegisterVMUI(mux *http.ServeMux, enabled bool) {
	if !enabled {
		return
	}
	sub, _ := fs.Sub(vmuiFiles, "vmui")
	RegisterVMUIFS(mux, enabled, sub)
}

// RegisterVMUIFS is RegisterVMUI over an explicit bundle: the root of the
// upstream UI build (index.html, assets/, ...). The logs binary serves
// VictoriaLogs' vmui from this package's own embed; the traces binary serves
// VictoriaTraces' UI (VTUI, which replaced the log-based UI in VictoriaTraces
// v0.12.0) from lakehouse-traces/internal/vtui. Both are the upstream build
// output, never modified: the Lakehouse tab is injected into the HTML on the
// way out.
func RegisterVMUIFS(mux *http.ServeMux, enabled bool, bundle fs.FS) {
	if !enabled {
		return
	}

	fileServer := http.FileServer(http.FS(bundle))

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/select/vmui")
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
		if strings.HasPrefix(r.URL.Path, "/static/") {
			w.Header().Set("Cache-Control", "max-age=31536000")
		}
		fileServer.ServeHTTP(w, r)
	})

	injected := InjectLakehouseTab(upstream)

	mux.HandleFunc("/select/vmui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/select/vmui/", http.StatusMovedPermanently)
	})
	mux.Handle("/select/vmui/", injected)
}
