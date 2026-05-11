package selectapi

import (
	"context"
	"net/http"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

type Handler struct {
	store   storage.Storage
	cfg     *config.Config
	timeout time.Duration
	sem     chan struct{}
}

func NewHandler(store storage.Storage, cfg *config.Config) *Handler {
	maxConcurrent := cfg.Query.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 32
	}
	return &Handler{
		store:   store,
		cfg:     cfg,
		timeout: cfg.Query.Timeout,
		sem:     make(chan struct{}, maxConcurrent),
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/select/logsql/query", h.wrapVL(logsql.ProcessQueryRequest))
	mux.HandleFunc("/select/logsql/query_time_range", h.wrapVL(logsql.ProcessQueryTimeRangeRequest))
	mux.HandleFunc("/select/logsql/facets", h.wrapVL(logsql.ProcessFacetsRequest))
	mux.HandleFunc("/select/logsql/field_names", h.wrapVL(logsql.ProcessFieldNamesRequest))
	mux.HandleFunc("/select/logsql/field_values", h.wrapVL(logsql.ProcessFieldValuesRequest))
	mux.HandleFunc("/select/logsql/stream_field_names", h.wrapVL(logsql.ProcessStreamFieldNamesRequest))
	mux.HandleFunc("/select/logsql/stream_field_values", h.wrapVL(logsql.ProcessStreamFieldValuesRequest))
	mux.HandleFunc("/select/logsql/streams", h.wrapVL(logsql.ProcessStreamsRequest))
	mux.HandleFunc("/select/logsql/stream_ids", h.wrapVL(logsql.ProcessStreamIDsRequest))
	mux.HandleFunc("/select/logsql/hits", h.wrapVL(logsql.ProcessHitsRequest))
	mux.HandleFunc("/select/logsql/stats_query", h.wrapVL(logsql.ProcessStatsQueryRequest))
	mux.HandleFunc("/select/logsql/stats_query_range", h.wrapVL(logsql.ProcessStatsQueryRangeRequest))
	mux.HandleFunc("/select/logsql/tail", h.handleTailNoop)
	mux.HandleFunc("/select/tenant_ids", h.wrapVL(logsql.ProcessTenantIDsRequest))

	if h.cfg.Mode == config.ModeTraces {
		mux.HandleFunc("/select/jaeger/api/traces/", h.handleJaegerTrace)
		mux.HandleFunc("/select/jaeger/api/traces", h.handleJaegerSearch)
		mux.HandleFunc("/select/jaeger/api/services", h.handleJaegerServices)
		mux.HandleFunc("/select/jaeger/api/services/", h.handleJaegerOperations)
		mux.HandleFunc("/select/jaeger/api/dependencies", h.handleJaegerDependencies)
		mux.HandleFunc("/api/traces/", h.handleJaegerTrace)
		mux.HandleFunc("/api/traces", h.handleJaegerSearch)
		mux.HandleFunc("/api/services", h.handleJaegerServices)
		mux.HandleFunc("/api/services/", h.handleJaegerOperations)
		mux.HandleFunc("/api/dependencies", h.handleJaegerDependencies)
	}
}

func (h *Handler) wrapVL(fn func(ctx context.Context, w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case h.sem <- struct{}{}:
			defer func() { <-h.sem }()
		default:
			metrics.QueryRejectedTotal.Inc()
			http.Error(w, "too many concurrent queries, please retry later", http.StatusTooManyRequests)
			return
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		fn(ctx, w, r)
		dur := time.Since(start)
		metrics.QueryDuration.Observe(dur.Seconds())
		if h.cfg.Query.SlowThreshold > 0 && dur >= h.cfg.Query.SlowThreshold {
			metrics.SlowQueriesTotal.Inc()
			logger.Warnf("slow query: path=%s duration=%s query=%s", r.URL.Path, dur, r.FormValue("query"))
		}
	}
}

func (h *Handler) handleTailNoop(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "live tail not supported on cold storage", http.StatusNotImplemented)
}
