package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ParityResponse is the side-by-side comparison between the VL view
// (queries against LH's cold-tier Parquet) and the LH view
// (manifest-derived aggregates). Both are answering the same
// question — "how many rows do we hold?" — from different code
// paths, so any drift is a real signal worth surfacing.
type ParityResponse struct {
	// Window the comparison covered.
	StartUnixNano int64 `json:"start_unix_nano"`
	EndUnixNano   int64 `json:"end_unix_nano"`

	// VL view: `*` stats count over the window via the embedded
	// vlselect path. Counts rows as VL's index reports them.
	VLRows int64 `json:"vl_rows"`

	// LH view: manifest's LiveAggregateWindow over the same window.
	// Counts rows as the per-file FileInfo.RowCount sums, restricted
	// to files whose time range overlaps the window.
	ManifestRows  int64 `json:"manifest_rows"`
	ManifestBytes int64 `json:"manifest_bytes"`
	ManifestFiles int64 `json:"manifest_files"`

	// Drift = VL - LH. Positive means VL sees more rows than the
	// manifest tracks (typically VT-internal index rows the writer
	// dropped); negative means the manifest claims rows VL can't
	// find (post-compaction lag or stale index).
	RowsDelta int64   `json:"rows_delta"`
	// JSON field intentionally named `rows_delta_pct`.
	RowsDeltaPct float64 `json:"rows_delta_pct"`

	// VTInternalDropped is the cumulative count of VT-internal
	// stream rows (trace_id_idx, service_graph) the writer dropped
	// at insert time, keyed by "kind". Lifetime counter since
	// process start — not window-bounded — but accounts for the
	// expected VL/manifest gap on the traces side.
	VTInternalDropped map[string]uint64 `json:"vt_internal_dropped,omitempty"`

	// ExpectedDrift sums VTInternalDropped values — the predicted
	// VL−Manifest gap if the writer is dropping internal rows as
	// designed. Compare against RowsDelta:
	//   |RowsDelta - ExpectedDrift| ≈ 0   → parity verified
	//   significant residual                → real divergence to chase
	ExpectedDrift int64 `json:"expected_drift,omitempty"`

	// VerifiedDrift = RowsDelta - ExpectedDrift. The residual after
	// accounting for known-dropped rows. Anything more than a few
	// percent of VL's count is operationally interesting.
	VerifiedDrift    int64   `json:"verified_drift"`
	VerifiedDriftPct float64 `json:"verified_drift_pct"`

	// Per-tenant parity is currently not supported — account_id /
	// project_id are plain Parquet columns, not stream-tagged
	// fields VL can group on. Surfaced explicitly so operators
	// don't expect drill-down that isn't here yet.
	PerTenantSupported bool   `json:"per_tenant_supported"`
	PerTenantNote      string `json:"per_tenant_note,omitempty"`
}

// VTInternalCounter is the read-only side of metrics.VTInternalRowsDropped.
// Defined as an interface so the stats package can consume the
// per-kind value without taking a dependency on the metrics package
// (which would create an import cycle with how metrics.lakehouse.go
// is wired). Production wires a concrete reader; tests sub fakes.
type VTInternalCounter interface {
	Get(kind string) uint64
}

// VLQuerier is the subset of the in-process VL stats endpoint the
// parity check needs. Defined as an interface so tests can sub a
// fake without spinning the whole select pipeline.
type VLQuerier interface {
	StatsCountAll(ctx context.Context, startNs, endNs int64) (int64, error)
}

// parityConfig is the per-handler config the parity endpoint
// reads at request time. Kept tiny so the registration path stays
// readable.
type parityConfig struct {
	vl       VLQuerier
	auth     func(*http.Request) bool
	internal VTInternalCounter
	kinds    []string // kinds to read from internal counter
}

// handleParity wires GET /api/v1/admin/parity. Auth-gated like the
// other admin endpoints (reuses the global-read header). Default
// window: last 24h. Override via ?window=1h | 6h | 24h | 7d.
func (a *API) handleParity(w http.ResponseWriter, r *http.Request, vl VLQuerier, auth func(*http.Request) bool) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	if auth != nil && !auth(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "admin auth required"})
		return
	}

	window := 24 * time.Hour
	if w := r.URL.Query().Get("window"); w != "" {
		if d, err := time.ParseDuration(w); err == nil && d > 0 {
			window = d
		}
	}
	now := time.Now()
	startNs := now.Add(-window).UnixNano()
	endNs := now.UnixNano()

	resp := ParityResponse{
		StartUnixNano:      startNs,
		EndUnixNano:        endNs,
		PerTenantSupported: false,
		PerTenantNote:      "account_id / project_id are plain Parquet columns, not VL stream tags — per-tenant group-by isn't supported by the embedded VL stats path yet",
	}

	if a.cfg.Manifest != nil {
		// Window the manifest aggregate to match VL's query window —
		// without this, manifest counts all-time cold data while VL
		// counts only the windowed slice (bounded by retention) and
		// the comparison is structurally apples-to-oranges.
		live := a.cfg.Manifest.LiveAggregateWindow(startNs, endNs)
		resp.ManifestRows = live.Rows
		resp.ManifestBytes = live.Bytes
		resp.ManifestFiles = int64(live.Files)
	}

	if vl != nil {
		vlRows, err := vl.StatsCountAll(r.Context(), startNs, endNs)
		if err != nil {
			// Surface partial answer with the error inline rather
			// than 500-ing — operators want to see the LH side
			// even if the VL side is down or rejects the query.
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"start_unix_nano": startNs,
				"end_unix_nano":   endNs,
				"manifest_rows":   resp.ManifestRows,
				"manifest_bytes":  resp.ManifestBytes,
				"manifest_files":  resp.ManifestFiles,
				"vl_error":        err.Error(),
			})
			return
		}
		resp.VLRows = vlRows
		resp.RowsDelta = vlRows - resp.ManifestRows
		if resp.ManifestRows > 0 {
			resp.RowsDeltaPct = float64(resp.RowsDelta) / float64(resp.ManifestRows) * 100.0
		}

		// Account for the expected drift from VT-internal rows the
		// writer dropped at insert time. Without this the trace
		// parity check reports ~90% drift forever even though the
		// system is working as designed; with it, the residual
		// (VerifiedDrift) is what an operator should chase.
		if cfg, ok := r.Context().Value(parityCtxKey{}).(*parityConfig); ok && cfg.internal != nil {
			resp.VTInternalDropped = make(map[string]uint64, len(cfg.kinds))
			for _, k := range cfg.kinds {
				v := cfg.internal.Get(k)
				if v > 0 {
					resp.VTInternalDropped[k] = v
					resp.ExpectedDrift += int64(v)
				}
			}
		}
		resp.VerifiedDrift = resp.RowsDelta - resp.ExpectedDrift
		if resp.ManifestRows > 0 {
			resp.VerifiedDriftPct = float64(resp.VerifiedDrift) / float64(resp.ManifestRows) * 100.0
		}
	}

	writeJSON(w, resp)
}

// parityCtxKey scopes the handler config to the request context so
// the existing handleParity signature doesn't need a third parameter.
type parityCtxKey struct{}

// RegisterParity wires the parity endpoint into mux. Kept separate
// from Register() so callers can opt out (e.g. tests) and so the
// VLQuerier dependency is explicit at registration time.
func (a *API) RegisterParity(mux *http.ServeMux, vl VLQuerier, auth func(*http.Request) bool) {
	a.RegisterParityWithInternal(mux, vl, auth, nil, nil)
}

// RegisterParityWithInternal extends RegisterParity with an optional
// VT-internal counter reader. When supplied, the response includes
// vt_internal_dropped (per kind) + expected_drift + verified_drift,
// so the trace-mode parity check can sanity-check itself against
// the writer's drop counter instead of relying on a fat tolerance.
func (a *API) RegisterParityWithInternal(mux *http.ServeMux, vl VLQuerier, auth func(*http.Request) bool, internal VTInternalCounter, kinds []string) {
	cfg := &parityConfig{vl: vl, auth: auth, internal: internal, kinds: kinds}
	mux.HandleFunc("/lakehouse/api/v1/admin/parity", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), parityCtxKey{}, cfg)
		a.handleParity(w, r.WithContext(ctx), vl, auth)
	})
}

// vlStatsCountAdapter wraps the in-process VL select endpoint with
// the simple int64 return shape the parity check needs.
type vlStatsCountAdapter struct {
	baseURL string
	query   string
	client  *http.Client
}

// NewLocalVLQuerier returns a VLQuerier backed by an HTTP call to
// the same process's vlselect path. baseURL is the URL the parity
// check hits — typically "http://127.0.0.1:9428" for logs or
// "http://127.0.0.1:10428" for traces. The same-process loopback
// keeps this a pure read-only stats path without inter-service
// dependencies.
//
// Defaults to `* | stats count()` (all rows). Use
// NewLocalVLQuerierWithQuery to apply a mode-specific filter — e.g.
// the traces parity check excludes VT-internal index streams
// (trace_id_idx, service_graph) that VL sees but the writer drops
// before manifest accounting.
func NewLocalVLQuerier(baseURL string) VLQuerier {
	return NewLocalVLQuerierWithQuery(baseURL, "* | stats count() as n")
}

// NewLocalVLQuerierWithQuery lets callers override the LogsQL query
// used for the count. The query MUST end with a single
// `| stats count() as n` step or an equivalent producing a single
// numeric value — the adapter parses that single value.
func NewLocalVLQuerierWithQuery(baseURL, query string) VLQuerier {
	return &vlStatsCountAdapter{
		baseURL: baseURL,
		query:   query,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// TracesParityQuery is the LogsQL query the trace-mode parity check
// runs against the embedded VL stats endpoint.
//
// Caveat (surfaced in ParityResponse.PerTenantNote): VL's stats path
// counts every row in the index — including VT-internal streams
// (trace_id_idx, service_graph) that the writer drops in
// vtInternalRowKind before manifest accounting. The simple LogsQL
// surface doesn't expose a clean way to filter those internal streams
// without knowing their exact stream-tag shape, so the trace-mode
// parity check reports the raw VL count and the operator interprets
// the drift against the known-dropped vt-internal metric counter
// (lakehouse_vt_internal_rows_dropped_total). Tightening this is a
// follow-up.
const TracesParityQuery = `* | stats count() as n`

func (a *vlStatsCountAdapter) StatsCountAll(ctx context.Context, startNs, endNs int64) (int64, error) {
	q := a.query
	if q == "" {
		q = "* | stats count() as n"
	}
	// Embed _time: into the query so VL counts the same scope as
	// manifest.LiveAggregateWindow. stats_query is an INSTANT
	// evaluator — without an explicit _time filter VL counts every
	// row it knows about, which on the cold-tier-via-LH path means
	// the entire Parquet history, not the window the caller asked
	// for. The bracket-form `_time:[<rfc3339>, <rfc3339>]` is the
	// shape VL's parser accepts; unix-suffix forms (s, ms, ns) were
	// tested and return 0 rows or 422 in v1.50.0.
	if startNs > 0 && endNs > startNs {
		startStr := time.Unix(0, startNs).UTC().Format(time.RFC3339Nano)
		endStr := time.Unix(0, endNs).UTC().Format(time.RFC3339Nano)
		q = fmt.Sprintf("_time:[%s, %s] %s", startStr, endStr, q)
	}
	// #nosec G107,G704 -- baseURL is operator-configured (-stats.parity.vl-url flag); not user input.
	req, _ := http.NewRequestWithContext(ctx, "GET", a.baseURL+"/select/logsql/stats_query", nil)
	qs := req.URL.Query()
	qs.Set("query", q)
	qs.Set("time", fmt.Sprintf("%d", endNs/1e9))
	req.URL.RawQuery = qs.Encode()

	// #nosec G107,G704 -- request URL derives from operator-configured baseURL above.
	resp, err := a.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("vl stats_query: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("vl stats_query status %d", resp.StatusCode)
	}

	var parsed struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, fmt.Errorf("decode vl response: %w", err)
	}
	if len(parsed.Data.Result) == 0 {
		return 0, nil
	}
	v := parsed.Data.Result[0].Value
	if len(v) < 2 {
		return 0, fmt.Errorf("unexpected vl value shape: %+v", v)
	}
	switch s := v[1].(type) {
	case string:
		var n int64
		_, err := fmt.Sscanf(s, "%d", &n)
		return n, err
	case float64:
		return int64(s), nil
	}
	return 0, fmt.Errorf("vl value type %T not understood", v[1])
}
