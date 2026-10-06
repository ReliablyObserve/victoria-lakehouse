package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func addBufferedRows(segs *membuffer.Segments, now int64, tenant uint32, n int) {
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(logstorage.TenantID{AccountID: tenant}, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "svc"},
			{Name: "_msg", Value: fmt.Sprintf("m%d", i)},
		}, 1)
	}
	segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
}

// BufferedRows serves the admin parity check: every tenant's buffered rows in
// the window, per live segment with its committed state.
func TestBufferedRows_LocalBufferPerSegment(t *testing.T) {
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()

	now := time.Now().UnixNano()
	addBufferedRows(segs, now, 0, 6)
	addBufferedRows(segs, now, 1, 3)
	sealed, ok := segs.Seal()
	if !ok {
		t.Fatal("seal")
	}
	segs.Commit(sealed, time.Now())
	addBufferedRows(segs, now, 9, 2) // the active segment, uncommitted
	segs.DebugFlush()

	s := &Storage{localBuffer: segs}
	rep, err := s.BufferedRows(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Rows != 11 {
		t.Fatalf("rows=%d, want 11 (three tenants, two segments)", rep.Rows)
	}
	if len(rep.Nonces) != 2 || len(rep.Segments) != 2 {
		t.Fatalf("nonces=%v segments=%v, want the two live segments", rep.Nonces, rep.Segments)
	}
	byNonce := map[string]buffer.SegmentRows{}
	for _, g := range rep.Segments {
		byNonce[g.Nonce] = g
	}
	if g := byNonce[sealed.Nonce()]; g.Rows != 9 || !g.Committed {
		t.Errorf("sealed segment = %+v, want 9 rows, committed", g)
	}
	for n, g := range byNonce {
		if n != sealed.Nonce() && (g.Rows != 2 || g.Committed) {
			t.Errorf("active segment = %+v, want 2 rows, uncommitted", g)
		}
	}
}

func TestBufferedRows_NoBufferNoPeers(t *testing.T) {
	s := &Storage{}
	rep, err := s.BufferedRows(context.Background(), 0, time.Now().UnixNano())
	if err != nil || rep.Rows != 0 || len(rep.Nonces) != 0 || rep.Segments != nil {
		t.Fatalf("report=%+v err=%v, want empty, nil", rep, err)
	}
}

// peerAnswering serves n log rows of segment nonce for every tenant, or fails.
func peerAnswering(n int, nonce string, fail bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set(buffer.TenantScopeHeader, buffer.AllTenantsScope)
		w.Header().Set(buffer.SegmentsHeader, nonce)
		enc := json.NewEncoder(w)
		for i := 0; i < n; i++ {
			_ = enc.Encode(schema.LogRow{TimestampUnixNano: 1, Body: "row", ServiceName: "svc"})
		}
	}))
}

func bridgeStorage(mode config.Mode, endpoints ...string) *Storage {
	br := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, mode)
	br.SetEndpoints(endpoints)
	return &Storage{cfg: &config.Config{Mode: mode}, bufferBridge: br}
}

// With insert peers the buffer is read through the bridge: BufferedRows is the
// sum of every peer's rows, with their segment nonces and no per-segment split.
func TestBufferedRows_PeerBridgeCountsRowsAndNonces(t *testing.T) {
	a := peerAnswering(4, "aaaaaaaaaaaaaaaa", false)
	defer a.Close()
	b := peerAnswering(3, "bbbbbbbbbbbbbbbb", false)
	defer b.Close()

	s := bridgeStorage(config.ModeLogs, a.URL, b.URL)
	rep, err := s.BufferedRows(context.Background(), 0, time.Now().UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Rows != 7 {
		t.Errorf("rows=%d, want 7 (4+3 from two peers)", rep.Rows)
	}
	if _, ok := rep.Nonces["aaaaaaaaaaaaaaaa"]; !ok || len(rep.Nonces) != 2 {
		t.Errorf("nonces=%v, want both peers' segments", rep.Nonces)
	}
	if rep.Segments != nil {
		t.Errorf("segments=%v, want none: peers report totals only", rep.Segments)
	}
}

// A peer that fails is an error with the other peer's rows in the report: a
// smaller answer must not read as a whole one.
func TestBufferedRows_PeerFailureIsAnError(t *testing.T) {
	ok := peerAnswering(4, "aaaaaaaaaaaaaaaa", false)
	defer ok.Close()
	bad := peerAnswering(0, "", true)
	defer bad.Close()

	s := bridgeStorage(config.ModeLogs, ok.URL, bad.URL)
	rep, err := s.BufferedRows(context.Background(), 0, time.Now().UnixNano())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err=%v, want the failing peer's status", err)
	}
	if rep.Rows != 4 {
		t.Errorf("rows=%d, want the answering peer's 4", rep.Rows)
	}
}
