package stats

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// segFixture is a manifest and VL model with one live segment: n rows flushed
// into one object keyed by the segment's nonce, plus rows only VL has.
const segNonce = "eeeeeeeeeeeeeeee"

func segFixture(objRows int, extraVL []row) (*manifest.Manifest, *modelVL) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	seg := spread("0:0", objRows, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/"+segNonce+"-1.parquet", seg)
	return mf, &modelVL{rows: append(append([]row{}, seg...), extraVL...)}
}

// A committed segment must serve exactly its objects' rows. 14 rows the buffer
// does not serve (the CI shape vl=55986 manifest=56000) are a divergence that
// stays in verified_drift: before the per-segment split the buffer term went to
// -14 and cancelled it (verified=0).
func TestParity_CommittedSegmentThatUnderServesIsAMismatch(t *testing.T) {
	mf, _ := segFixture(6000, nil)
	// VL sees 5986: it counts the segment from the buffer, which holds 5986.
	vl := &fixedVL{rows: 5986}
	buf := &modelBuffer{segs: []buffer.SegmentRows{{Nonce: segNonce, Committed: true, Rows: 5986}}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), vl, nil)
	if r.VerifiedDrift != -14 {
		t.Fatalf("verified_drift=%d, want -14 (not cancelled by a negative buffer term)", r.VerifiedDrift)
	}
	if r.BufferUnflushedRows != 0 || r.ExpectedDrift != 0 {
		t.Fatalf("unflushed=%d expected=%d, want 0/0: a committed segment has no expected drift", r.BufferUnflushedRows, r.ExpectedDrift)
	}
	want := SegmentMismatch{Nonce: segNonce, Committed: true, BufferRows: 5986, ObjectRows: 6000}
	if len(r.SegmentMismatches) != 1 || r.SegmentMismatches[0] != want {
		t.Fatalf("segment_mismatches=%+v, want [%+v]", r.SegmentMismatches, want)
	}
	if r.BufferAttribution != "per_segment" {
		t.Fatalf("attribution=%q", r.BufferAttribution)
	}
}

type fixedVL struct{ rows int64 }

func (f *fixedVL) StatsCountAll(context.Context, int64, int64) (int64, error) { return f.rows, nil }

// #379 shape: compaction merged a live committed segment's object into a
// nonce-less object. The buffer still serves 1000 rows and the manifest holds
// them under no nonce, so VL counts them twice. Before the split this read as
// 1000 expected unflushed rows (verified=0).
func TestParity_CompactionMergedLiveSegmentObjectIsAMismatch(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	merged := spread("0:0", 6000, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/plain-merged.parquet", merged) // no nonce
	// VL: 6000 from the merged object + 1000 from the buffer = 7000.
	buf := &modelBuffer{segs: []buffer.SegmentRows{{Nonce: segNonce, Committed: true, Rows: 1000}}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &fixedVL{rows: 7000}, nil)
	if r.VerifiedDrift != 1000 {
		t.Fatalf("verified_drift=%d, want 1000 (rows counted twice)", r.VerifiedDrift)
	}
	if len(r.SegmentMismatches) != 1 || r.SegmentMismatches[0].ObjectRows != 0 || r.SegmentMismatches[0].BufferRows != 1000 {
		t.Fatalf("segment_mismatches=%+v", r.SegmentMismatches)
	}
}

// An uncommitted segment may hold more than it has written, never less.
func TestParity_UncommittedSegmentWithMoreObjectRowsThanBufferIsAMismatch(t *testing.T) {
	mf, _ := segFixture(100, nil)
	buf := &modelBuffer{segs: []buffer.SegmentRows{{Nonce: segNonce, Committed: false, Rows: 90}}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &fixedVL{rows: 90}, nil)
	if len(r.SegmentMismatches) != 1 || r.SegmentMismatches[0].Committed {
		t.Fatalf("segment_mismatches=%+v", r.SegmentMismatches)
	}
	if r.ExpectedDrift != 0 || r.VerifiedDrift != -10 {
		t.Fatalf("expected=%d verified=%d, want 0/-10 (the gap is not explained away)", r.ExpectedDrift, r.VerifiedDrift)
	}
}

// One lost file of 200 rows reads exactly -200 in the residual, however many
// other rows the window holds; the endpoint has no tolerance of its own.
func TestParity_LostSmallFileIsExactlyItsRows(t *testing.T) {
	now := time.Now().UnixNano()
	h := int64(time.Hour)
	kept := spread("0:0", 9000, now-5*h, now-4*h)
	lost := spread("0:0", 200, now-3*h, now-2*h)
	mf := manifest.New("b", "")
	addFile(mf, "p0", "0/0/logs/a.parquet", kept)
	addFile(mf, "p1", "0/0/logs/b.parquet", lost)
	buf := &modelBuffer{segs: []buffer.SegmentRows{}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &modelVL{rows: kept}, nil)
	if r.VerifiedDrift != -200 {
		t.Fatalf("verified_drift=%d, want -200", r.VerifiedDrift)
	}
}

// Peers report totals only. A total below the objects' rows cannot be right
// and is flagged; a total above them is the unflushed drift.
func TestParity_PeerBufferIsAnAggregateAndANegativeTermIsFlagged(t *testing.T) {
	mf, _ := segFixture(6000, nil)
	n := map[string]struct{}{segNonce: {}}

	ok := &modelBuffer{peers: &buffer.WindowReport{Rows: 6120, Nonces: n}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: ok}), &fixedVL{rows: 6120}, nil)
	if r.BufferAttribution != "aggregate" || r.BufferUnflushedRows != 120 || r.VerifiedDrift != 0 || len(r.SegmentMismatches) != 0 {
		t.Fatalf("aggregate ok: %+v", r)
	}

	low := &modelBuffer{peers: &buffer.WindowReport{Rows: 5986, Nonces: n}}
	r = getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: low}), &fixedVL{rows: 5986}, nil)
	if len(r.SegmentMismatches) != 1 || r.SegmentMismatches[0].Nonce != "*" || r.VerifiedDrift != -14 || r.BufferUnflushedRows != 0 {
		t.Fatalf("aggregate negative: %+v", r)
	}
}

func TestParity_NoBufferIsReportedAsNone(t *testing.T) {
	mf, vl := segFixture(100, nil)
	r := getParity(t, NewAPI(APIConfig{Manifest: mf}), vl, nil)
	if r.BufferAttribution != "none" || r.VerifiedDrift != 0 {
		t.Fatalf("attribution=%q verified=%d", r.BufferAttribution, r.VerifiedDrift)
	}
}

// A peer failing surfaces as buffer_error (the buffer term is partial), also
// when it fails in only one of the two reads: that is a changed sample, so it
// is repeated, and the error is still reported when it persists.
func TestParity_BufferErrorIsReported(t *testing.T) {
	mf, vl := segFixture(100, nil)
	buf := &modelBuffer{peers: &buffer.WindowReport{Rows: 100, Nonces: map[string]struct{}{segNonce: {}}}, err: errors.New("peer 10.0.0.2: buffer query returned 500")}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), vl, nil)
	if !strings.Contains(r.BufferError, "returned 500") {
		t.Fatalf("buffer_error=%q, want the peer's failure", r.BufferError)
	}
}

func TestParity_PeerFailingInOnlyOneReadIsResampled(t *testing.T) {
	mf, vl := segFixture(100, nil)
	rep := buffer.WindowReport{Rows: 100, Nonces: map[string]struct{}{segNonce: {}}}
	buf := &modelBuffer{read: func(call int) (buffer.WindowReport, error) {
		if call == 2 { // the read after the VL read of attempt 1
			return buffer.WindowReport{}, errors.New("peer timeout")
		}
		return rep, nil
	}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), vl, nil)
	if r.SampleAttempts != 2 || r.UnstableSample || r.BufferError != "" || r.VerifiedDrift != 0 {
		t.Fatalf("attempts=%d unstable=%v err=%q verified=%d, want 2/false/\"\"/0", r.SampleAttempts, r.UnstableSample, r.BufferError, r.VerifiedDrift)
	}
}

// The buffer is bracketed like the manifest: rows ingested while VL counts
// make the two buffer reads differ, and the sample is repeated.
func TestParity_ResamplesWhenBufferChangesDuringRead(t *testing.T) {
	mf, _ := segFixture(100, nil)
	rows := int64(100)
	buf := &modelBuffer{read: func(call int) (buffer.WindowReport, error) {
		if call == 2 {
			rows += 5 // ingested during the VL read
		}
		return buffer.WindowReport{Rows: rows, Nonces: map[string]struct{}{segNonce: {}}, Segments: []buffer.SegmentRows{{Nonce: segNonce, Rows: rows}}}, nil
	}}
	vl := &fixedVL{rows: 105}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), vl, nil)
	if r.SampleAttempts != 2 || r.UnstableSample || r.BufferRows != 105 || r.BufferUnflushedRows != 5 || r.VerifiedDrift != 0 {
		t.Fatalf("attempts=%d unstable=%v buffer=%d unflushed=%d verified=%d", r.SampleAttempts, r.UnstableSample, r.BufferRows, r.BufferUnflushedRows, r.VerifiedDrift)
	}
}

func TestParity_UnstableWhenTheBufferNeverSettles(t *testing.T) {
	mf, _ := segFixture(100, nil)
	rows := int64(100)
	buf := &modelBuffer{read: func(int) (buffer.WindowReport, error) {
		rows++
		return buffer.WindowReport{Rows: rows}, nil
	}}
	r := getParity(t, NewAPI(APIConfig{Manifest: mf, Buffer: buf}), &fixedVL{rows: 100}, nil)
	if r.SampleAttempts != 3 || !r.UnstableSample {
		t.Fatalf("attempts=%d unstable=%v, want 3/true", r.SampleAttempts, r.UnstableSample)
	}
}

// The loopback carries Authorization and the configured global-read header and
// NOTHING else of the caller's request, and reaches the server it was told to.
func TestParity_LoopbackForwardsOnlyTheCredentialToTheListenAddr(t *testing.T) {
	var lh http.Header
	var lhHost string
	var lhHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lhHits.Add(1)
		lh, lhHost = r.Header.Clone(), r.Host
		_, _ = w.Write([]byte(`{"data":{"result":[{"value":[0,"3"]}]}}`))
	}))
	defer target.Close()
	// A different server on the loopback interface, like a hot VictoriaLogs on
	// the mode's default port. It must see nothing.
	var decoyHits atomic.Int32
	var decoyAuth atomic.Value
	decoy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoyHits.Add(1)
		decoyAuth.Store(r.Header.Get("X-Lakehouse-Global-Read") + r.Header.Get("Authorization"))
	}))
	defer decoy.Close()

	// ":<port>" (unspecified host) is the loopback interface.
	_, port, _ := net.SplitHostPort(target.Listener.Addr().String())
	api := NewAPI(APIConfig{ParityForwardHeader: "X-Lakehouse-Global-Read"})
	mux := http.NewServeMux()
	api.RegisterParity(mux, NewLoopbackVLQuerier(":"+port, ""), nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity?window=1h", nil)
	req.Host = "attacker.example"
	req.Header.Set("X-Lakehouse-Global-Read", "secret")
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Cookie", "session=abc")
	req.Header.Set("AccountID", "7")
	req.Header.Set("ProjectID", "8")
	req.Header.Set("X-Scope-OrgID", "tenant-x")
	req.Header.Set("Connection", "close")
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	mux.ServeHTTP(httptest.NewRecorder(), req)

	if decoyHits.Load() != 0 {
		t.Fatalf("the server on the other port got %d request(s) (credential %v): the loopback must go to the listen address only", decoyHits.Load(), decoyAuth.Load())
	}
	if lhHits.Load() != 1 {
		t.Fatalf("the listen address got %d requests, want 1", lhHits.Load())
	}
	if lh.Get("X-Lakehouse-Global-Read") != "secret" || lh.Get("Authorization") != "Bearer tok" {
		t.Fatalf("credential not forwarded: %v", lh)
	}
	for _, name := range []string{"Cookie", "AccountID", "ProjectID", "X-Scope-OrgID", "Connection", "X-Forwarded-For"} {
		if v := lh.Get(name); v != "" {
			t.Errorf("%s=%q reached the loopback query; only the credential may", name, v)
		}
	}
	if strings.Contains(lhHost, "attacker") {
		t.Errorf("loopback Host=%q: the caller's Host must not be forwarded", lhHost)
	}
}

// Only the credential headers, and only when present and named.
func TestParityForwardHeaders(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer t")
	r.Header.Set("x-global", "g")
	r.Header.Set("Cookie", "c")
	if h := parityForwardHeaders(r, "X-Global"); len(h) != 2 || h.Get("Authorization") != "Bearer t" || h.Get("X-Global") != "g" {
		t.Errorf("got %v", h)
	}
	if h := parityForwardHeaders(r, ""); len(h) != 1 || h.Get("Authorization") == "" {
		t.Errorf("no configured header: got %v", h)
	}
	if h := parityForwardHeaders(httptest.NewRequest("GET", "/", nil), "X-Global"); len(h) != 0 {
		t.Errorf("empty request: got %v", h)
	}
}

func TestLoopbackBaseURL(t *testing.T) {
	cases := []struct {
		in, want, sock string
		wantErr        bool
	}{
		{in: ":9428", want: "http://127.0.0.1:9428"},
		{in: ":19429", want: "http://127.0.0.1:19429"},
		{in: "0.0.0.0:9428", want: "http://127.0.0.1:9428"},
		{in: "[::]:10428", want: "http://127.0.0.1:10428"},
		{in: "127.0.0.1:1", want: "http://127.0.0.1:1"},
		{in: "10.1.2.3:9428", want: "http://10.1.2.3:9428"},
		{in: "localhost:9428", want: "http://localhost:9428"},
		{in: "[fe80::1]:9428", want: "http://[fe80::1]:9428"},
		{in: "unix:/run/lh.sock", want: "http://unix", sock: "/run/lh.sock"},
		{in: "unix:", wantErr: true},
		{in: "", wantErr: true},
		{in: "9428", wantErr: true},
		{in: ":0", wantErr: true},
	}
	for _, c := range cases {
		got, sock, err := LoopbackBaseURL(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: want an error, got %q", c.in, got)
			}
			continue
		}
		if err != nil || got != c.want || sock != c.sock {
			t.Errorf("%q: got %q %q %v, want %q %q", c.in, got, sock, err, c.want, c.sock)
		}
	}
}

// An address that cannot be reached by loopback says so; it never falls back to
// some default server.
func TestNewLoopbackVLQuerier_BadAddressIsAnError(t *testing.T) {
	_, err := NewLoopbackVLQuerier("not-an-address", "").StatsCountAll(context.Background(), 1, 2)
	if err == nil || !strings.Contains(err.Error(), "not-an-address") {
		t.Fatalf("err=%v, want one naming the address", err)
	}
}

// -httpListenAddr=unix:/path (v1.53.0): the loopback query goes over the socket.
func TestNewLoopbackVLQuerier_UnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "lhp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "lh.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var auth atomic.Value
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		_, _ = io.WriteString(w, `{"data":{"result":[{"value":[0,"42"]}]}}`)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	h := http.Header{"Authorization": {"Bearer tok"}}
	ctx := context.WithValue(context.Background(), parityAuthKey{}, h)
	n, err := NewLoopbackVLQuerier("unix:"+sock, "").StatsCountAll(ctx, 1_000_000_000, 2_000_000_000)
	if err != nil || n != 42 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if auth.Load() != "Bearer tok" {
		t.Fatalf("auth over the socket = %v", auth.Load())
	}
}
