package insertapi

import (
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

type BufferQuerier interface {
	QueryBuffer(startNs, endNs int64) (interface{}, error)
}

type StorageWriter interface {
	Writer() BufferQuerier
}

type Handler struct{}

func NewHandler(store interface{}, cfg config.Config, bq BufferQuerier) *Handler {
	return &Handler{}
}

func (h *Handler) Register(mux *http.ServeMux) {}
