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
	fileServer := http.FileServer(http.FS(sub))

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
