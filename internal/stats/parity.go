package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// ParityResponse is the side-by-side comparison between the VL view
// (a row-precise `stats count()` through the embedded select path) and the
// LH view (manifest-derived file aggregates). Both answer the same question
// ("how many rows do we hold?") for the SAME scope, so any residual after the
// explained term is a real signal worth surfacing:
//
//   - Tenants: the loopback VL query carries the caller's global-read
//     credential, so it counts every tenant, like the manifest aggregate.
//   - Window: the start is aligned so no file straddles it (see
//     manifest.AlignedWindowStart); a file-level sum then equals the row-precise
//     count over the same window.
//   - Buffer: rows the insert buffer serves that the manifest does not hold yet
//     are reported as buffer_unflushed_rows and are the expected drift.
type ParityResponse struct {
	// Window the comparison covered. StartUnixNano is the aligned start
	// (<= RequestedStartUnixNano): a file that straddled the requested start
	// moves it back to the file's first row.
	StartUnixNano          int64 `json:"start_unix_nano"`
	EndUnixNano            int64 `json:"end_unix_nano"`
	RequestedStartUnixNano int64 `json:"requested_start_unix_nano"`

	// Scope is the tenant scope both views cover: every tenant.
	Scope string `json:"scope"`

	// VL view: `* | stats count()` over the window via the embedded select
	// path, every tenant, including rows only the insert buffer holds.
	VLRows int64 `json:"vl_rows"`

	// LH view: manifest's LiveAggregateWindow over the same window: the
	// per-file FileInfo.RowCount sums of every tenant's files overlapping it.
	ManifestRows  int64 `json:"manifest_rows"`
	ManifestBytes int64 `json:"manifest_bytes"`
	ManifestFiles int64 `json:"manifest_files"`

	// Drift = VL - LH. Positive means VL sees more rows than the manifest
	// tracks (the unflushed rows, below); negative means the manifest claims
	// rows VL can't find (a lost or unreadable file, or a stale index).
	RowsDelta int64 `json:"rows_delta"`
	// JSON field intentionally named `rows_delta_pct`.
	RowsDeltaPct float64 `json:"rows_delta_pct"`

	// BufferRows is what the live insert-buffer segments hold in the window
	// (every tenant). BufferObjectRows is what the manifest holds for the
	// committed objects of those same segments: a query counts those rows from
	// the buffer and skips the objects, so they cancel.
	// BufferUnflushedRows = BufferRows - BufferObjectRows is the rows the
	// manifest does not hold yet. For traces it includes the VT-internal index
	// rows (trace_id_idx) the buffer still holds and the flush drops.
	BufferRows          int64  `json:"buffer_rows"`
	BufferObjectRows    int64  `json:"buffer_object_rows"`
	BufferUnflushedRows int64  `json:"buffer_unflushed_rows"`
	BufferError         string `json:"buffer_error,omitempty"`

	// VTInternalDropped is the cumulative count of VT-internal stream rows
	// (trace_id_idx, service_graph) the writer dropped at insert time, keyed
	// by "kind". INFORMATIONAL: a process-lifetime counter over all tenants
	// and all time, so it is not part of the expected drift (the buffer term
	// already covers the internal rows in the window).
	VTInternalDropped map[string]uint64 `json:"vt_internal_dropped,omitempty"`

	// ExpectedDrift is BufferUnflushedRows: the predicted VL-Manifest gap.
	//   |VerifiedDrift| ~ 0   -> parity verified
	//   a residual           -> rows ingested or flushed between the reads, or
	//                           a real divergence (a lost file shows here).
	ExpectedDrift int64 `json:"expected_drift"`

	// VerifiedDrift = RowsDelta - ExpectedDrift, the residual an operator
	// should chase; VerifiedDriftPct is relative to the manifest rows.
	VerifiedDrift    int64   `json:"verified_drift"`
	VerifiedDriftPct float64 `json:"verified_drift_pct"`

	// SampleAttempts is how many times the three reads were repeated because
	// the manifest changed (a flush or compaction commit) while they ran;
	// UnstableSample is true if it still changed on the last attempt.
	SampleAttempts int  `json:"sample_attempts"`
	UnstableSample bool `json:"unstable_sample,omitempty"`

	// Per-tenant parity is not reported: the comparison is the total over
	// every tenant. Surfaced explicitly so operators don't expect a
	// drill-down that isn't here.
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

// BufferSource reports the insert buffer for the parity check: every tenant's
// rows in [startNs, endNs] held by the live segments, and those segments'
// nonces. parquets3.Storage implements it.
type BufferSource interface {
	BufferedRows(ctx context.Context, startNs, endNs int64) (int64, map[string]struct{}, error)
}

// parityAuthKey carries the caller's (already validated) request headers to
// the loopback VL query, so it presents the same global-read credential.
type parityAuthKey struct{}

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
	reqStartNs := now.Add(-window).UnixNano()
	endNs := now.UnixNano()

	resp := ParityResponse{
		RequestedStartUnixNano: reqStartNs,
		EndUnixNano:            endNs,
		Scope:                  "all_tenants",
		PerTenantSupported:     false,
		PerTenantNote:          "the comparison covers every tenant (the loopback query carries the caller's global-read credential); per-tenant rows are not reported",
	}

	// The loopback VL query must see every tenant like the manifest does: it
	// presents the caller's credential, which the auth gate above validated.
	ctx := context.WithValue(r.Context(), parityAuthKey{}, r.Header.Clone())

	// The three reads (buffer, manifest, VL) are not one atomic snapshot. A
	// flush or compaction commit between them moves rows from the buffer term
	// to the manifest, so repeat the sample until the manifest is the same
	// before and after the VL read (bounded).
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp.SampleAttempts = attempt
		var before manifest.LiveAggregate
		startNs := reqStartNs
		if a.cfg.Manifest != nil {
			startNs = a.cfg.Manifest.AlignedWindowStart(reqStartNs, endNs)
		}
		resp.StartUnixNano = startNs

		var nonces map[string]struct{}
		if a.cfg.Buffer != nil {
			n, ns, err := a.cfg.Buffer.BufferedRows(ctx, startNs, endNs)
			if err != nil {
				resp.BufferError = err.Error()
			} else {
				resp.BufferError = ""
				resp.BufferRows, nonces = n, ns
			}
		}
		if a.cfg.Manifest != nil {
			before = a.cfg.Manifest.LiveAggregateWindow(startNs, endNs)
			resp.ManifestRows = before.Rows
			resp.ManifestBytes = before.Bytes
			resp.ManifestFiles = int64(before.Files)
			resp.BufferObjectRows = a.cfg.Manifest.RowsOfSegments(nonces, startNs, endNs)
		}
		resp.BufferUnflushedRows = resp.BufferRows - resp.BufferObjectRows

		if vl == nil {
			break
		}
		vlRows, err := vl.StatsCountAll(ctx, startNs, endNs)
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
		resp.UnstableSample = false
		if a.cfg.Manifest != nil {
			after := a.cfg.Manifest.LiveAggregateWindow(startNs, endNs)
			if after.Rows != before.Rows || after.Files != before.Files {
				resp.UnstableSample = true
				continue
			}
		}
		break
	}

	if vl != nil {
		resp.RowsDelta = resp.VLRows - resp.ManifestRows
		if resp.ManifestRows > 0 {
			resp.RowsDeltaPct = float64(resp.RowsDelta) / float64(resp.ManifestRows) * 100.0
		}
		resp.ExpectedDrift = resp.BufferUnflushedRows
		resp.VerifiedDrift = resp.RowsDelta - resp.ExpectedDrift
		if resp.ManifestRows > 0 {
			resp.VerifiedDriftPct = float64(resp.VerifiedDrift) / float64(resp.ManifestRows) * 100.0
		}
		// Informational lifetime counter; see ParityResponse.VTInternalDropped.
		if cfg, ok := r.Context().Value(parityCtxKey{}).(*parityConfig); ok && cfg.internal != nil {
			resp.VTInternalDropped = make(map[string]uint64, len(cfg.kinds))
			for _, k := range cfg.kinds {
				if v := cfg.internal.Get(k); v > 0 {
					resp.VTInternalDropped[k] = v
				}
			}
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
// vt_internal_dropped (per kind) as information; it does not enter the
// expected drift (a process-lifetime counter is not window-correct).
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

// TracesParityQuery is the LogsQL query the trace-mode parity check runs
// against the embedded VL stats endpoint. It counts every row in the index,
// including the VT-internal rows (trace_id_idx) the insert buffer still holds
// and the writer drops at flush; those are part of buffer_unflushed_rows, so
// the traces side needs no stream filter.
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
	// The admin parity check counts everything up to the window's end. The
	// traces binary's LogsQL hides spans younger than -search.latencyOffset
	// (VictoriaTraces v0.12.0) unless told not to, which would leave the newest
	// rows out of the count and read as drift against the manifest. Other
	// servers ignore the argument.
	qs.Set("disable_latency_offset", "true")
	req.URL.RawQuery = qs.Encode()
	// Present the caller's validated credential (global-read header or bearer
	// token) so the count covers every tenant, as the manifest side does.
	if h, ok := ctx.Value(parityAuthKey{}).(http.Header); ok {
		for k, vs := range h {
			switch http.CanonicalHeaderKey(k) {
			case "Accept-Encoding", "Content-Length", "Host", "Connection":
				continue
			}
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

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
