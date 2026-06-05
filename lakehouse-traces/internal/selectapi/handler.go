package selectapi

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/jaeger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tempo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
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
		mux.HandleFunc("/select/jaeger/", func(w http.ResponseWriter, r *http.Request) {
			if !jaeger.RequestHandler(r.Context(), w, r) {
				// Upstream VT's main HTTP dispatcher writes the same 400
				// for paths Jaeger doesn't know about. Without this,
				// Grafana sees HTTP 200 + 0 bytes (silent empty) and
				// renders the cold tier as "no data".
				http.Error(w, fmt.Sprintf("unsupported path requested: %q", r.URL.Path), http.StatusBadRequest)
			}
		})
		mux.HandleFunc("/select/tempo/", func(w http.ResponseWriter, r *http.Request) {
			normalizeTempoSearchParams(r)
			if !tempo.RequestHandler(r.Context(), w, r) {
				// Same parity fix as Jaeger above — tempo.RequestHandler
				// returns false for unknown paths (e.g. /api/v2/search
				// — TraceQL v2 — which upstream VT itself rejects with
				// 400). Without the explicit error, Grafana's modern
				// Tempo datasource (which probes /api/v2/search before
				// falling back to /api/search) silently shows zero data.
				http.Error(w, fmt.Sprintf("unsupported path requested: %q", r.URL.Path), http.StatusBadRequest)
			}
		})
		mux.HandleFunc("/api/traces/", rewriteToJaeger)
		mux.HandleFunc("/api/traces", rewriteToJaeger)
		mux.HandleFunc("/api/services", rewriteToJaeger)
		mux.HandleFunc("/api/services/", rewriteToJaeger)
		mux.HandleFunc("/api/dependencies", rewriteToJaeger)
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

func rewriteToJaeger(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = "/select/jaeger" + r.URL.Path
	jaeger.RequestHandler(r.Context(), w, r)
}

// normalizeTempoSearchParams works around an upstream VT quirk in
// parseTempoAPIParam (deps/VictoriaTraces/app/vtselect/traces/tempo/tempo.go
// ~L557): the parser sets `q="{}"` as the default but then unconditionally
// overwrites it with the URL `q` value, even when that value is empty.
// `traceql.ParseQuery("")` fails, and `searchTraces` returns nil/empty —
// so clients that omit `q` (or send an empty `q`) get an empty result.
//
// This shim runs BEFORE the upstream Tempo handler and:
//
//  1. If `q` is missing or blank but `tags` is present (Tempo HTTP search
//     also accepts logfmt-style `tags=key=value key=value`, per the
//     public Grafana spec), it converts each pair into a TraceQL filter
//     fragment using the same scope-prefix rules the upstream Tempo
//     handler already applies to /v2/search/tag/*/values
//     (service.name → resource.service.name, span.* → span attributes,
//     etc.) and writes `q={ ... }` into the request URL.
//  2. Otherwise, if `q` is still empty, it defaults `q` to `{}` so the
//     upstream parser ends up with a noop TraceQL filter (the same value
//     it documents as the default at the top of parseTempoAPIParam).
//
// Upstream VT source is unmodified. This is a pure HTTP-layer normalizer
// in the LH adapter.
func normalizeTempoSearchParams(r *http.Request) {
	// Only normalize the /search endpoint — leave /v2/search/tags,
	// /v2/search/tag/*/values, /api/echo, /api/metrics/query_range and
	// /v2/traces/<id> untouched (they have their own semantics).
	if !strings.HasSuffix(r.URL.Path, "/select/tempo/api/search") {
		return
	}
	if err := r.ParseForm(); err != nil {
		return
	}

	q := strings.TrimSpace(r.Form.Get("q"))
	if q != "" {
		return
	}

	tags := r.Form.Get("tags")
	if traceQL := tempoTagsToTraceQL(tags); traceQL != "" {
		r.Form.Set("q", traceQL)
	} else {
		r.Form.Set("q", "{}")
	}
	r.URL.RawQuery = r.Form.Encode()
}

// tempoTagsToTraceQL converts a Tempo-style logfmt `tags` query parameter
// (e.g. `service.name=api-gateway db.system=postgres`) into a TraceQL
// filter expression (e.g. `{resource.service.name="api-gateway" && db.system="postgres"}`).
//
// It uses the same scope-prefix mapping that upstream VT applies in
// /select/tempo/api/v2/search/tag/<name>/values (see tempo.go's
// processSearchTagValuesRequest): bare service.name / name / status map
// to the canonical resource/span fields; resource.* / span.* / event.*
// prefixes are passed through; everything else is left bare so TraceQL
// matches against span attributes by default.
//
// Returns "" if `tags` is empty or contains no valid key=value pairs.
func tempoTagsToTraceQL(tags string) string {
	tags = strings.TrimSpace(tags)
	if tags == "" {
		return ""
	}
	// Tempo HTTP search `tags` parameter is logfmt-encoded — pairs are
	// space-separated, key/value joined by `=`. Values may be quoted with
	// double quotes. We do a minimal logfmt parse here: split on spaces
	// outside of quotes.
	pairs := splitLogfmt(tags)
	if len(pairs) == 0 {
		return ""
	}

	type kv struct{ k, v string }
	var fragments []kv
	for _, p := range pairs {
		eq := strings.IndexByte(p, '=')
		if eq <= 0 || eq == len(p)-1 {
			continue
		}
		k := strings.TrimSpace(p[:eq])
		v := strings.TrimSpace(p[eq+1:])
		v = strings.Trim(v, `"`)
		if k == "" || v == "" {
			continue
		}

		var mapped string
		switch k {
		case "service.name", ".service.name":
			mapped = "resource.service.name"
		case "name", ".name":
			mapped = "name"
		case "status":
			mapped = "status"
		default:
			// resource.* / span.* / event.* prefixes pass through as-is;
			// any other key is treated as a span attribute (TraceQL
			// default), which matches upstream tempo.go behavior for the
			// /v2/search/tag/*/values handler.
			mapped = k
		}

		fragments = append(fragments, kv{k: mapped, v: v})
	}
	if len(fragments) == 0 {
		return ""
	}

	// Sort for stable output (helps tests + cache keys).
	sort.Slice(fragments, func(i, j int) bool {
		return fragments[i].k < fragments[j].k
	})

	var b strings.Builder
	b.WriteByte('{')
	for i, f := range fragments {
		if i > 0 {
			b.WriteString(" && ")
		}
		b.WriteString(f.k)
		b.WriteByte('=')
		b.WriteByte('"')
		// Escape any embedded double quotes the user might pass through.
		b.WriteString(strings.ReplaceAll(f.v, `"`, `\"`))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// splitLogfmt splits a logfmt-ish string on spaces, respecting double-quoted
// values. It is intentionally permissive — Tempo's `tags` parameter is not
// strictly logfmt in practice, and the upstream Tempo handler ignores it
// entirely, so any pair we can't parse is silently dropped.
func splitLogfmt(s string) []string {
	var out []string
	var cur strings.Builder
	inQuotes := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			inQuotes = !inQuotes
			cur.WriteByte(c)
		case c == ' ' && !inQuotes:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}
