package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var staticFiles embed.FS

// HandlerConfig controls the Lakehouse UI handler behavior.
type HandlerConfig struct {
	Enabled bool
}

// Handler serves the embedded Lakehouse Explorer UI.
type Handler struct {
	cfg HandlerConfig
}

// NewHandler creates a new UI handler with the given configuration.
func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{cfg: cfg}
}

// Register adds the UI routes to the given ServeMux.
func (h *Handler) Register(mux *http.ServeMux) {
	if !h.cfg.Enabled {
		mux.HandleFunc("/lakehouse/ui/", func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
		return
	}
	sub, _ := fs.Sub(staticFiles, "static")
	fileServer := http.FileServer(http.FS(sub))
	mux.HandleFunc("/lakehouse/ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/lakehouse/ui/", http.StatusMovedPermanently)
	})
	// The embedded UI is a single-file app with inline JS that ships with each
	// deploy. Without a cache directive the browser heuristically caches index.html
	// (no Last-Modified on a go:embed FS), so a reload keeps running the OLD JS —
	// e.g. tab state / fixes don't appear until a hard refresh. Force revalidation.
	mux.Handle("/lakehouse/ui/", http.StripPrefix("/lakehouse/ui/", noCache(fileServer)))
}

// noCache forbids the browser from serving a stale copy of the (small, inline-JS)
// UI so deploys take effect on a normal reload.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, must-revalidate")
		next.ServeHTTP(w, r)
	})
}
