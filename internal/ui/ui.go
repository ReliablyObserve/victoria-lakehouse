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
	mux.Handle("/lakehouse/ui/", http.StripPrefix("/lakehouse/ui/", fileServer))
}
