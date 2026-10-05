package parquets3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fvcPeer is an insert peer holding unflushed rows, answering
// /internal/buffer/query for the default tenant.
func fvcPeer(t *testing.T, rows []schema.LogRow, nonces ...string) *BufferBridge {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		if len(nonces) > 0 {
			w.Header().Set(buffer.SegmentsHeader, strings.Join(nonces, ","))
		}
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	t.Cleanup(srv.Close)
	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})
	return bridge
}

// Values and hits cover what is not flushed yet: a peer's rows are merged as a
// query merges them — the objects flushed from the segments the peer serves are
// not read (no row counted twice) — through the request's filter, re-checked for
// tenant.
func TestFieldValues_IncludeUnflushedRowsFromPeers(t *testing.T) {
	const nonce = "65000000aaaabbbb"
	flushed := []schema.LogRow{fvcRow(5*time.Minute, "INFO"), fvcRow(6*time.Minute, "INFO")}
	// The peer's segment was flushed into the first object; the peer still
	// serves its rows (in grace) and names the segment.
	s := fvcStorageNonce(t, nonce, flushed)
	other := fvcRow(22*time.Minute, "ERROR")
	other.AccountID = 7 // a peer that answers for the wrong tenant must not leak
	s.bufferBridge = fvcPeer(t, []schema.LogRow{
		fvcRow(5*time.Minute, "INFO"), // in the segment's object too
		fvcRow(6*time.Minute, "INFO"),
		fvcRow(20*time.Minute, "INFO"),
		fvcRow(21*time.Minute, "WARN"),
		other,
	}, nonce)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())

	got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.ValueWithHits{{Value: "INFO", Hits: 3}, {Value: "WARN", Hits: 1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}

	streams, err := s.GetStreams(context.Background(), nil, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantStreams := []logstorage.ValueWithHits{{Value: `{service.name="svc-INFO"}`, Hits: 3}, {Value: `{service.name="svc-WARN"}`, Hits: 1}}
	if !reflect.DeepEqual(streams, wantStreams) {
		t.Fatalf("streams = %v, want %v", streams, wantStreams)
	}

	filtered := mustParseQueryWithTime(t, `service.name:="svc-WARN"`, fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	got, err = s.GetFieldValues(context.Background(), nil, filtered, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "WARN", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("filtered field_values level = %v, want %v", got, want)
	}
}

// A delete hides unflushed rows from the enumeration as it hides them from
// the query.
func TestFieldValues_TombstoneHidesUnflushedRows(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{fvcRow(5*time.Minute, "INFO")})
	s.bufferBridge = fvcPeer(t, []schema.LogRow{fvcRow(20*time.Minute, "INFO"), fvcRow(21*time.Minute, "WARN")})
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-buffer", Query: `service.name:="svc-WARN"`,
		StartNs: fvcBase.UnixNano(), EndNs: fvcBase.Add(time.Hour).UnixNano(), Mode: "hide",
	})
	s.SetTombstoneStore(store)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "INFO", Hits: 2}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}
}

// A single node reads its co-located buffer directly; a window holding no
// flushed object at all is answered from it alone.
func TestFieldValues_IncludeTheLocalBuffer(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i, level := range []string{"INFO", "INFO", "WARN"} {
		lr.MustAdd(logstorage.TenantID{}, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "checkout"}, {Name: "level", Value: level}, {Name: "_msg", Value: "event"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)
	q := mustParseQueryWithTime(t, "*", now-int64(time.Hour), now+int64(time.Hour))
	got, err := s.GetFieldValues(context.Background(), nil, q, "level", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "INFO", Hits: 2}, {Value: "WARN", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values level = %v, want %v", got, want)
	}
}
