package stats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
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
//   - Buffer: rows of LIVE UNCOMMITTED insert-buffer segments that the manifest
//     does not hold yet are reported as buffer_unflushed_rows and are the
//     expected drift. A COMMITTED live segment is expected to hold exactly the
//     rows of its own objects plus the rows the flush drops (traces:
//     trace_id_idx, expected drift too): any other difference
//     (segment_mismatches) is NOT expected drift, it stays in verified_drift.
type ParityResponse struct {
	// Window the comparison covered. StartUnixNano is the aligned start
	// (<= RequestedStartUnixNano): a file that straddled the requested start
	// moves it back to the file's first row.
	StartUnixNano          int64 `json:"start_unix_nano"`
	EndUnixNano            int64 `json:"end_unix_nano"`
	RequestedStartUnixNano int64 `json:"requested_start_unix_nano"`
	// EndUnixNano is the aligned end (>= RequestedEndUnixNano): a file with rows
	// dated after now moves it forward to the file's last row.
	RequestedEndUnixNano int64 `json:"requested_end_unix_nano"`

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
	// objects of those same segments: a query counts those rows from the buffer
	// and skips the objects, so they cancel.
	//
	// BufferUnflushedRows is the expected drift: for each live UNCOMMITTED
	// segment, its buffered rows minus the rows of the objects it has already
	// written (never negative). A COMMITTED segment contributes only the rows
	// the flush drops (traces: the VT-internal trace_id_idx rows the buffer
	// still holds, which VL counts and the manifest never holds): its buffered
	// rows must equal its objects' rows plus those, and when they do not the
	// segment is listed in SegmentMismatches and the difference stays in
	// VerifiedDrift.
	BufferRows          int64 `json:"buffer_rows"`
	BufferObjectRows    int64 `json:"buffer_object_rows"`
	BufferUnflushedRows int64 `json:"buffer_unflushed_rows"`
	// BufferAttribution is "per_segment" when the segments are co-located (each
	// one is checked on its own), "aggregate" when the rows come from insert
	// peers through the buffer bridge (rows and nonces only: a committed segment
	// the buffer under-serves cannot be told from an unflushed one unless the
	// total goes negative), and "none" when the node has no buffer.
	BufferAttribution string `json:"buffer_attribution"`
	// SegmentMismatches lists every live segment whose buffered rows disagree
	// with its objects where they must not: a committed segment whose buffered
	// rows differ from its objects' rows, or any segment with more object rows
	// than buffered rows. Nonce "*" is the aggregate of the peers' segments.
	SegmentMismatches []SegmentMismatch `json:"segment_mismatches,omitempty"`
	// BufferError is set when the buffer could not be read completely (an insert
	// peer failed): the buffer term is then not trustworthy and verified_drift
	// must not be read as parity.
	BufferError string `json:"buffer_error,omitempty"`

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

// SegmentMismatch is one live insert-buffer segment (or "*" for the peers'
// total) whose buffered rows disagree with the manifest rows of its objects.
type SegmentMismatch struct {
	Nonce      string `json:"nonce"`
	Committed  bool   `json:"committed"`
	BufferRows int64  `json:"buffer_rows"`
	ObjectRows int64  `json:"object_rows"`
	// DroppedRows are the buffered rows the flush never writes (traces:
	// trace_id_idx); a committed segment is expected to hold object_rows +
	// dropped_rows.
	DroppedRows int64 `json:"dropped_rows,omitempty"`
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
// rows in [startNs, endNs] held by the live segments, per segment when they are
// co-located. parquets3.Storage implements it. A non-nil error with a report
// means the report is partial (an insert peer failed).
type BufferSource interface {
	BufferedRows(ctx context.Context, startNs, endNs int64) (buffer.WindowReport, error)
}

// parityAuthKey carries the credential headers of the caller's (already
// validated) request to the loopback VL query, so it presents the same
// global-read credential. Only Authorization and the configured global-read
// header are carried (parityForwardHeaders).
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
	reqEndNs := now.UnixNano()

	resp := ParityResponse{
		RequestedStartUnixNano: reqStartNs,
		RequestedEndUnixNano:   reqEndNs,
		Scope:                  "all_tenants",
		PerTenantSupported:     false,
		PerTenantNote:          "the comparison covers every tenant (the loopback query carries the caller's global-read credential); per-tenant rows are not reported",
	}

	// The loopback VL query must see every tenant like the manifest does: it
	// presents the caller's credential, which the auth gate above validated.
	// Only the credential headers travel: no cookies, tenant headers, Host or
	// Connection of the caller's request.
	ctx := context.WithValue(r.Context(), parityAuthKey{}, parityForwardHeaders(r, a.cfg.ParityForwardHeader))

	// The reads (buffer, manifest, VL) are not one atomic snapshot. The buffer
	// and the manifest are read before AND after the VL read; if either changed
	// (a flush or compaction commit, a row ingested, a segment retired, a peer
	// failing in only one of the two reads) the sample is repeated, bounded.
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp.SampleAttempts = attempt
		startNs := reqStartNs
		if a.cfg.Manifest != nil {
			startNs = a.cfg.Manifest.AlignedWindowStart(reqStartNs)
		}
		resp.StartUnixNano = startNs
		// The end moves forward the same way: a file holding rows dated after
		// now (clock skew, -futureRetention) overlaps the window and the manifest
		// counts it whole, so the window must reach its last row.
		endNs := reqEndNs
		if a.cfg.Manifest != nil {
			endNs = a.cfg.Manifest.AlignedWindowEnd(reqEndNs)
		}
		resp.EndUnixNano = endNs

		rep1, berr1 := a.readBuffer(ctx, startNs, endNs)
		var ws1 manifest.WindowSample
		if a.cfg.Manifest != nil {
			ws1 = a.cfg.Manifest.WindowSample(startNs, endNs, rep1.Nonces)
		}
		resp.fill(rep1, ws1, berr1)

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

		rep2, berr2 := a.readBuffer(ctx, startNs, endNs)
		changed := !reflect.DeepEqual(rep1, rep2) || (berr1 == nil) != (berr2 == nil)
		if a.cfg.Manifest != nil {
			changed = changed ||
				a.cfg.Manifest.AlignedWindowStart(reqStartNs) != startNs ||
				a.cfg.Manifest.AlignedWindowEnd(reqEndNs) != endNs ||
				!reflect.DeepEqual(ws1, a.cfg.Manifest.WindowSample(startNs, endNs, rep1.Nonces))
		}
		resp.UnstableSample = changed
		if !changed {
			break
		}
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

// readBuffer reads the insert buffer; a nil source is an empty report.
func (a *API) readBuffer(ctx context.Context, startNs, endNs int64) (buffer.WindowReport, error) {
	if a.cfg.Buffer == nil {
		return buffer.WindowReport{}, nil
	}
	return a.cfg.Buffer.BufferedRows(ctx, startNs, endNs)
}

// fill sets the manifest and buffer terms from one sample.
func (resp *ParityResponse) fill(rep buffer.WindowReport, ws manifest.WindowSample, berr error) {
	resp.ManifestRows = ws.Agg.Rows
	resp.ManifestBytes = ws.Agg.Bytes
	resp.ManifestFiles = int64(ws.Agg.Files)
	resp.BufferError = ""
	if berr != nil {
		resp.BufferError = berr.Error()
	}
	resp.BufferRows, resp.BufferObjectRows, resp.BufferUnflushedRows, resp.SegmentMismatches, resp.BufferAttribution = bufferTerms(rep, ws.SegmentRows)
}

// bufferTerms compares the buffer with the objects of its segments.
//
// A query counts a live segment's rows from the buffer and skips the segment's
// objects, so per segment the VL view holds the buffered rows and the manifest
// holds the object rows. For a segment that is not committed yet the buffer
// is expected to hold at least what was already written: the difference is the
// unflushed rows. For a committed segment the buffer is expected to hold its
// objects' rows plus the rows the flush drops (traces: trace_id_idx, counted by
// the buffer read as Dropped; the VL view counts them, the manifest never holds
// them), so those are expected drift and any other difference is a divergence
// (an object lost or merged away while the segment is still live, rows the
// buffer no longer serves), never expected drift.
func bufferTerms(rep buffer.WindowReport, objRows map[string]int64) (bufRows, objRowsSum, unflushed int64, mism []SegmentMismatch, attribution string) {
	if rep.Segments == nil {
		// Peers (or no buffer): totals only.
		for n := range rep.Nonces {
			objRowsSum += objRows[n]
		}
		bufRows = rep.Rows
		switch {
		case len(rep.Nonces) == 0 && rep.Rows == 0:
			return bufRows, objRowsSum, 0, nil, "none"
		case bufRows >= objRowsSum:
			return bufRows, objRowsSum, bufRows - objRowsSum, nil, "aggregate"
		}
		return bufRows, objRowsSum, 0, []SegmentMismatch{{Nonce: "*", BufferRows: bufRows, ObjectRows: objRowsSum}}, "aggregate"
	}
	for _, g := range rep.Segments {
		obj := objRows[g.Nonce]
		bufRows += g.Rows
		objRowsSum += obj
		bad := SegmentMismatch{Nonce: g.Nonce, Committed: g.Committed, BufferRows: g.Rows, ObjectRows: obj, DroppedRows: g.Dropped}
		switch {
		case g.Committed && g.Rows != obj+g.Dropped, !g.Committed && g.Rows < obj:
			mism = append(mism, bad)
		case g.Committed:
			// The rows the flush drops are counted by VL from the buffer and
			// are not in the manifest: expected, and exactly this many.
			unflushed += g.Dropped
		default:
			unflushed += g.Rows - obj
		}
	}
	sort.Slice(mism, func(i, j int) bool { return mism[i].Nonce < mism[j].Nonce })
	return bufRows, objRowsSum, unflushed, mism, "per_segment"
}

// parityForwardHeaders is the part of the caller's request the loopback query
// carries: its credential, and nothing else. Authorization (a bearer token) and
// the configured global-read header are the two ways the select path accepts a
// cross-tenant credential. Cookies, tenant headers (AccountID, ProjectID,
// X-Scope-OrgID), Host, Connection and every other header stay behind.
func parityForwardHeaders(r *http.Request, globalReadHeader string) http.Header {
	h := http.Header{}
	if v := r.Header.Values("Authorization"); len(v) > 0 {
		h["Authorization"] = append([]string(nil), v...)
	}
	if name := strings.TrimSpace(globalReadHeader); name != "" {
		if v := r.Header.Values(name); len(v) > 0 {
			h[http.CanonicalHeaderKey(name)] = append([]string(nil), v...)
		}
	}
	return h
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

// LoopbackBaseURL turns the address the HTTP server actually listens on (the
// -httpListenAddr value) into the URL a loopback request reaches that server
// at. An unspecified host (":9428", "0.0.0.0:9428", "[::]:9428") is the
// loopback interface; a specific host is used as is, so a server bound to one
// address is asked there. A Unix domain socket ("unix:/path") returns the
// socket path as well, and a base URL whose host is a placeholder.
func LoopbackBaseURL(listenAddr string) (baseURL, unixSocket string, err error) {
	addr := strings.TrimSpace(listenAddr)
	if sock, ok := strings.CutPrefix(addr, "unix:"); ok {
		if sock == "" {
			return "", "", errors.New("empty unix socket path in the listen address")
		}
		return "http://unix", sock, nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", "", fmt.Errorf("cannot derive the loopback address from listen address %q: %w", listenAddr, err)
	}
	if port == "" || port == "0" {
		return "", "", fmt.Errorf("listen address %q has no fixed port to reach the server at", listenAddr)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port), "", nil
}

// failedVLQuerier answers every count with err.
type failedVLQuerier struct{ err error }

func (f failedVLQuerier) StatsCountAll(context.Context, int64, int64) (int64, error) {
	return 0, f.err
}

// NewLoopbackVLQuerier returns a VLQuerier that reaches the server listening on
// listenAddr (the -httpListenAddr value, not a default), over TCP or a Unix
// socket. query is the LogsQL count query ("" for NewLocalVLQuerier's). An
// address that cannot be reached by loopback gives a querier whose answer is
// that error, so the parity endpoint says so (vl_error) rather than asking some
// other server.
func NewLoopbackVLQuerier(listenAddr, query string) VLQuerier {
	base, sock, err := LoopbackBaseURL(listenAddr)
	if err != nil {
		return failedVLQuerier{err: err}
	}
	if query == "" {
		query = "* | stats count() as n"
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if sock != "" {
		client.Transport = &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		}
	}
	return &vlStatsCountAdapter{baseURL: base, query: query, client: client}
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
	// #nosec G107,G704 -- baseURL is derived from the process's own -httpListenAddr, not from request input.
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
	// token) so the count covers every tenant, as the manifest side does. The
	// handler has already reduced the request's headers to those two.
	if h, ok := ctx.Value(parityAuthKey{}).(http.Header); ok {
		for k, vs := range h {
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
