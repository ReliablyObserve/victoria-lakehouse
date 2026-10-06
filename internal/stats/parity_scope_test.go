package stats

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// row is one stored row of the model both fake sides read.
type row struct {
	ts     int64
	tenant string // "0:0", "1:0", ...
}

// modelVL answers `* | stats count()` row-precisely over the model, like
// VictoriaLogs does: tenant 0:0 only unless the request carried the global-read
// credential, and including rows only the insert buffer holds.
type modelVL struct {
	rows []row
}

func (m *modelVL) StatsCountAll(ctx context.Context, startNs, endNs int64) (int64, error) {
	_, all := ctx.Value(parityAuthKey{}).(http.Header)
	var n int64
	for _, r := range m.rows {
		if r.ts < startNs || r.ts > endNs {
			continue
		}
		if !all && r.tenant != "0:0" {
			continue
		}
		n++
	}
	return n, nil
}

// spread returns n rows of tenant evenly in [min, max].
func spread(tenant string, n int, min, max int64) []row {
	out := make([]row, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, row{ts: min + (max-min)*int64(i)/int64(n-1), tenant: tenant})
	}
	return out
}

type modelBuffer struct {
	rows   int64
	nonces map[string]struct{}
}

func (b *modelBuffer) BufferedRows(context.Context, int64, int64) (int64, map[string]struct{}, error) {
	return b.rows, b.nonces, nil
}

func getParity(t *testing.T, api *API, vl VLQuerier, internal VTInternalCounter) ParityResponse {
	t.Helper()
	mux := http.NewServeMux()
	api.RegisterParityWithInternal(mux, vl, nil, internal, []string{"trace_id_idx", "service_graph"})
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity?window=24h", nil)
	req.Header.Set("X-Lakehouse-Global-Read", "k")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var r ParityResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	return r
}

// addFile records the rows into the manifest the way a flush does: one file
// whose time range is the rows' own range.
func addFile(mf *manifest.Manifest, partition, key string, rows []row) {
	min, max := rows[0].ts, rows[0].ts
	for _, r := range rows {
		if r.ts < min {
			min = r.ts
		}
		if r.ts > max {
			max = r.ts
		}
	}
	mf.AddFile(partition, manifest.FileInfo{Key: key, RowCount: int64(len(rows)), Size: 1, MinTimeNs: min, MaxTimeNs: max})
}

// The three terms that made the endpoint disagree with itself, one test each.
// Each fails on the pre-fix handler (tenant 0:0 only on the VL side, an
// unaligned window, no buffer term) and passes with the scope/window/buffer
// fix.

func TestParity_OtherTenantsCountedOnBothSides(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	base := now - 5*h
	var rows []row
	rows = append(rows, spread("0:0", 1000, base, base+h-1)...)
	rows = append(rows, spread("1:0", 400, base, base+h-1)...)
	rows = append(rows, spread("7:3", 250, base, base+h-1)...)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/a.parquet", rows[:1000])
	addFile(mf, "p1", "1/0/logs/b.parquet", rows[1000:1400])
	addFile(mf, "p2", "7/3/logs/c.parquet", rows[1400:])

	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), &modelVL{rows: rows}, nil)
	if r.ManifestRows != 1650 || r.VLRows != 1650 {
		t.Fatalf("vl=%d manifest=%d, want 1650/1650 (every tenant on both sides)", r.VLRows, r.ManifestRows)
	}
	if r.VerifiedDrift != 0 {
		t.Fatalf("verified_drift=%d, want 0", r.VerifiedDrift)
	}
	if r.Scope != "all_tenants" {
		t.Fatalf("scope=%q", r.Scope)
	}
}

func TestParity_StraddlingHourFileAlignsWindowStart(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	reqStart := now - 24*h
	// One hour file whose rows straddle the requested start: 30 min before,
	// 30 min after.
	straddle := spread("0:0", 1000, reqStart-30*int64(time.Minute), reqStart+30*int64(time.Minute))
	later := spread("0:0", 500, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/a.parquet", straddle)
	addFile(mf, "p1", "0/0/logs/b.parquet", later)

	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), &modelVL{rows: append(straddle, later...)}, nil)
	if r.StartUnixNano != straddle[0].ts {
		t.Fatalf("aligned start=%d, want the straddling file's first row %d", r.StartUnixNano, straddle[0].ts)
	}
	if r.RequestedStartUnixNano != r.StartUnixNano && r.RequestedStartUnixNano < r.StartUnixNano {
		t.Fatalf("aligned start must not move forward: requested=%d aligned=%d", r.RequestedStartUnixNano, r.StartUnixNano)
	}
	if r.VLRows != 1500 || r.ManifestRows != 1500 || r.VerifiedDrift != 0 {
		t.Fatalf("vl=%d manifest=%d verified=%d, want 1500/1500/0", r.VLRows, r.ManifestRows, r.VerifiedDrift)
	}
}

func TestParity_UnflushedBufferRowsAreTheExpectedDrift(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	flushed := spread("0:0", 800, now-3*h, now-2*h)
	// A committed segment the buffer still serves: its 200 rows are in the
	// manifest AND the buffer; a query counts them once (from the buffer).
	committed := spread("0:0", 200, now-90*int64(time.Minute), now-80*int64(time.Minute))
	// 120 rows in uncommitted segments: buffer only.
	unflushed := spread("0:0", 120, now-int64(time.Minute), now-1)

	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/aaaaaaaaaaaaaaaa-1.parquet", flushed)
	const nonce = "bbbbbbbbbbbbbbbb"
	addFile(mf, "p1", "0/0/logs/"+nonce+"-1.parquet", committed)
	buf := &modelBuffer{rows: int64(len(committed) + len(unflushed)), nonces: map[string]struct{}{nonce: {}, "cccccccccccccccc": {}}}

	all := append(append(append([]row{}, flushed...), committed...), unflushed...)
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &modelVL{rows: all}, nil)
	if r.BufferRows != 320 || r.BufferObjectRows != 200 || r.BufferUnflushedRows != 120 {
		t.Fatalf("buffer rows=%d object rows=%d unflushed=%d, want 320/200/120", r.BufferRows, r.BufferObjectRows, r.BufferUnflushedRows)
	}
	if r.RowsDelta != 120 || r.ExpectedDrift != 120 || r.VerifiedDrift != 0 {
		t.Fatalf("delta=%d expected=%d verified=%d, want 120/120/0", r.RowsDelta, r.ExpectedDrift, r.VerifiedDrift)
	}
}

// A file the manifest lists but VL cannot read must show as a residual: the
// gate that the tests assert on must not be explainable away.
func TestParity_LostFileIsAResidual(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	kept := spread("0:0", 4000, now-5*h, now-4*h)
	lost := spread("0:0", 1000, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/a.parquet", kept)
	addFile(mf, "p1", "0/0/logs/b.parquet", lost)

	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), &modelVL{rows: kept}, nil)
	if r.VerifiedDrift != -1000 {
		t.Fatalf("verified_drift=%d, want -1000 (the lost file)", r.VerifiedDrift)
	}
	if r.VerifiedDriftPct > -15 {
		t.Fatalf("verified_drift_pct=%.2f, want about -20", r.VerifiedDriftPct)
	}
}

// The traces lifetime counter (all tenants, all time) must not enter the
// expected drift: with it large and the window clean, verified drift stays 0.
func TestParity_TracesLifetimeCounterIsNotSubtracted(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	spans := spread("0:0", 1000, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/traces/a.parquet", spans)
	counter := &fakeCounter{values: map[string]uint64{"trace_id_idx": 5_000_000, "service_graph": 100}}

	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), &modelVL{rows: spans}, counter)
	if r.VerifiedDrift != 0 || r.ExpectedDrift != 0 {
		t.Fatalf("verified=%d expected=%d, want 0/0 (the lifetime counter is informational)", r.VerifiedDrift, r.ExpectedDrift)
	}
	if r.VTInternalDropped["trace_id_idx"] != 5_000_000 {
		t.Fatalf("vt_internal_dropped not reported: %v", r.VTInternalDropped)
	}
}

// Traces: the buffer still holds the VT-internal index rows the flush drops, so
// a committed live segment's objects hold fewer rows than the buffer: that gap
// is part of buffer_unflushed_rows.
func TestParity_TracesBufferIndexRowsAreInTheBufferTerm(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	spans := spread("0:0", 300, now-3*h, now-2*h)
	idx := spread("0:0", 80, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	const nonce = "dddddddddddddddd"
	addFile(mf, "p0", "0/0/traces/"+nonce+"-1.parquet", spans) // idx rows dropped at flush
	buf := &modelBuffer{rows: 380, nonces: map[string]struct{}{nonce: {}}}

	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &modelVL{rows: append(append([]row{}, spans...), idx...)}, nil)
	if r.BufferUnflushedRows != 80 || r.VerifiedDrift != 0 {
		t.Fatalf("unflushed=%d verified=%d, want 80/0", r.BufferUnflushedRows, r.VerifiedDrift)
	}
}

// A commit landing between the reads moves rows from the buffer term to the
// manifest; the handler repeats the sample until the manifest is steady.
type commitDuringVL struct {
	inner  *modelVL
	mf     *manifest.Manifest
	calls  atomic.Int32
	commit func()
}

func (c *commitDuringVL) StatsCountAll(ctx context.Context, s, e int64) (int64, error) {
	if c.calls.Add(1) == 1 {
		c.commit()
	}
	return c.inner.StatsCountAll(ctx, s, e)
}

func TestParity_ResamplesWhenManifestChangesDuringRead(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	a := spread("0:0", 500, now-3*h, now-2*h)
	b := spread("0:0", 100, now-int64(time.Minute), now-1)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/a.parquet", a)
	vl := &commitDuringVL{inner: &modelVL{rows: append(append([]row{}, a...), b...)}, mf: mf}
	vl.commit = func() { addFile(mf, "p1", "0/0/logs/b.parquet", b) }

	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), vl, nil)
	if r.SampleAttempts != 2 || r.UnstableSample {
		t.Fatalf("attempts=%d unstable=%v, want 2/false", r.SampleAttempts, r.UnstableSample)
	}
	if r.VerifiedDrift != 0 || r.ManifestRows != 600 {
		t.Fatalf("verified=%d manifest=%d, want 0/600", r.VerifiedDrift, r.ManifestRows)
	}
}

// The loopback query presents the caller's credential and nothing the HTTP
// client must set itself.
func TestParity_LoopbackForwardsCallerCredential(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_, _ = w.Write([]byte(`{"data":{"result":[{"value":[0,"3"]}]}}`))
	}))
	defer srv.Close()
	hdr := http.Header{}
	hdr.Set("X-Lakehouse-Global-Read", "secret")
	hdr.Set("Authorization", "Bearer tok")
	hdr.Set("Accept-Encoding", "gzip")
	ctx := context.WithValue(context.Background(), parityAuthKey{}, hdr)
	n, err := NewLocalVLQuerier(srv.URL).StatsCountAll(ctx, 1_000_000_000, 2_000_000_000)
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if got.Get("X-Lakehouse-Global-Read") != "secret" || got.Get("Authorization") != "Bearer tok" {
		t.Fatalf("credential not forwarded: %v", got)
	}
}
