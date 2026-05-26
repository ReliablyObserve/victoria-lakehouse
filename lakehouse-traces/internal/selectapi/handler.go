package selectapi

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Handler struct {
	store    storage.Storage
	cfg      *config.Config
	resolver *tenant.TenantResolver
	timeout  time.Duration
	sem      chan struct{}
}

type HandlerOption func(*Handler)

func WithResolver(r *tenant.TenantResolver) HandlerOption {
	return func(h *Handler) { h.resolver = r }
}

func NewHandler(store storage.Storage, cfg *config.Config, opts ...HandlerOption) *Handler {
	maxConcurrent := cfg.Query.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 32
	}
	h := &Handler{
		store:   store,
		cfg:     cfg,
		timeout: cfg.Query.Timeout,
		sem:     make(chan struct{}, maxConcurrent),
	}
	for _, o := range opts {
		o(h)
	}
	return h
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
		normalizeTimeParams(r)
		start := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
		defer cancel()
		ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "vl.handler."+r.URL.Path)
		defer span.End()
		span.SetAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.path", r.URL.Path),
		)
		fn(ctx, w, r)
		dur := time.Since(start)
		metrics.QueryDuration.Observe(dur.Seconds())
		if h.cfg.Query.SlowThreshold > 0 && dur >= h.cfg.Query.SlowThreshold {
			metrics.SlowQueriesTotal.Inc()
			logger.Warnf("slow query: path=%s duration=%s query=%s", r.URL.Path, dur, r.FormValue("query"))
		}
	}
}

func normalizeTimeParams(r *http.Request) {
	if err := r.ParseForm(); err != nil {
		return
	}
	changed := false
	for _, key := range []string{"start", "end", "time"} {
		v := r.Form.Get(key)
		if v == "" {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		if n > 1e12 {
			r.Form.Set(key, strconv.FormatInt(n/1000, 10))
			changed = true
		}
	}
	if changed {
		r.URL.RawQuery = r.Form.Encode()
	}
}

func (h *Handler) handleTailNoop(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "live tail not supported on cold storage", http.StatusNotImplemented)
}
