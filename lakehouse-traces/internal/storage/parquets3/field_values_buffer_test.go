package parquets3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// fvcPeer is an insert peer holding unflushed spans, answering
// /internal/buffer/query for the default tenant.
func fvcPeer(t *testing.T, rows []schema.TraceRow) *BufferBridge {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	t.Cleanup(srv.Close)
	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})
	return bridge
}

// Values and hits cover a peer's unflushed spans, merged as a query merges
// them: newer than the flushed objects only, filtered, tenant re-checked.
func TestTraceFieldValues_IncludeUnflushedSpansFromPeers(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{fvcSpan(5*time.Minute, "GET"), fvcSpan(6*time.Minute, "GET")})
	other := fvcSpan(22*time.Minute, "DELETE")
	other.AccountID = 7
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{
		fvcSpan(4*time.Minute, "GET"), // already in Parquet
		fvcSpan(20*time.Minute, "GET"),
		fvcSpan(21*time.Minute, "PUT"),
		other,
	})
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "GET", Hits: 3}, {Value: "PUT", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
	streams, err := s.GetStreams(context.Background(), nil, q, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantStreams := []logstorage.ValueWithHits{
		{Value: `{resource_attr:service.name="svc-GET"}`, Hits: 3},
		{Value: `{resource_attr:service.name="svc-PUT"}`, Hits: 1},
	}
	if !reflect.DeepEqual(streams, wantStreams) {
		t.Fatalf("streams = %v, want %v", streams, wantStreams)
	}
}

func TestTraceFieldValues_TombstoneHidesUnflushedSpans(t *testing.T) {
	s := fvcStorage(t, []schema.TraceRow{fvcSpan(5*time.Minute, "GET")})
	s.bufferBridge = fvcPeer(t, []schema.TraceRow{fvcSpan(20*time.Minute, "GET"), fvcSpan(21*time.Minute, "PUT")})
	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{
		Tenants: []delete.TenantRef{{}}, ID: "ts-buffer", Query: `name:="PUT"`,
		StartNs: fvcBase.UnixNano(), EndNs: fvcBase.Add(time.Hour).UnixNano(), Mode: "hide",
	})
	s.SetTombstoneStore(store)
	q := mustParseQueryWithTime(t, "*", fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "GET", Hits: 2}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
}

// A window nothing has been flushed for is answered from the co-located
// buffer alone.
func TestTraceFieldValues_IncludeTheLocalBuffer(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i, name := range []string{"GET", "GET", "PUT"} {
		lr.MustAdd(logstorage.TenantID{}, now+int64(i), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "checkout"}, {Name: "name", Value: name}, {Name: "trace_id", Value: "t"},
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
	got, err := s.GetFieldValues(context.Background(), nil, q, "name", 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []logstorage.ValueWithHits{{Value: "GET", Hits: 2}, {Value: "PUT", Hits: 1}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("field_values name = %v, want %v", got, want)
	}
}
