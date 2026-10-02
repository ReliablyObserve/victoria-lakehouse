package parquets3

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestBufferBridge_QueryLogs(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "hello", ServiceName: "svc"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})

	got, nonces := bridge.QueryLogs(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano(), tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	if got[0].Body != "hello" {
		t.Errorf("Body = %q, want hello", got[0].Body)
	}
}

func TestBufferBridge_QueryTraces(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.TraceRow{
		{TimestampUnixNano: base.UnixNano(), TraceID: "t1", SpanName: "op1"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for _, row := range rows {
			_ = enc.Encode(row)
		}
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})

	got, nonces := bridge.QueryTraces(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano(), tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
	if got[0].TraceID != "t1" {
		t.Errorf("TraceID = %q, want t1", got[0].TraceID)
	}
}

func TestBufferBridge_Disabled(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
	}, config.ModeLogs)

	got, nonces := bridge.QueryLogs(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("disabled bridge should return empty")
	}
}

func TestBufferBridge_DisabledTraces(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
	}, config.ModeTraces)

	got, nonces := bridge.QueryTraces(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("disabled bridge should return empty")
	}
}

func TestBufferBridge_NoEndpoints(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)

	got, nonces := bridge.QueryLogs(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("no endpoints should return empty")
	}
}

func TestBufferBridge_MultipleEndpoints(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		_ = json.NewEncoder(w).Encode(schema.LogRow{TimestampUnixNano: base.UnixNano(), Body: "from-1"})
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		_ = json.NewEncoder(w).Encode(schema.LogRow{TimestampUnixNano: base.UnixNano(), Body: "from-2"})
	}))
	defer srv2.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv1.URL, srv2.URL})

	got, nonces := bridge.QueryLogs(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano(), tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 2 {
		t.Errorf("got %d rows, want 2 (one from each endpoint)", len(got))
	}
}

func TestBufferBridge_EndpointError(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 1 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{"http://localhost:1"}) // unreachable

	got, nonces := bridge.QueryLogs(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("unreachable endpoint should return empty")
	}
}

func TestBufferBridge_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})

	got, nonces := bridge.QueryLogs(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("500 response should return empty")
	}
}

func TestBufferBridge_TraceServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})

	got, nonces := bridge.QueryTraces(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("500 response should return empty for traces")
	}
}

func TestBufferBridge_TraceEndpointError(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 1 * time.Second,
	}, config.ModeTraces)
	bridge.SetEndpoints([]string{"http://localhost:1"})

	got, nonces := bridge.QueryTraces(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("unreachable endpoint should return empty for traces")
	}
}

func TestBufferBridge_NoEndpointsTraces(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeTraces)

	got, nonces := bridge.QueryTraces(context.Background(), 0, 1000, tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Error("no endpoints should return empty for traces")
	}
}

func TestBufferBridge_SetEndpoints(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)

	bridge.SetEndpoints([]string{"http://a:9428", "http://b:9428"})
	bridge.mu.RLock()
	if len(bridge.endpoints) != 2 {
		t.Errorf("endpoints = %d, want 2", len(bridge.endpoints))
	}
	bridge.mu.RUnlock()

	bridge.SetEndpoints([]string{"http://c:9428"})
	bridge.mu.RLock()
	if len(bridge.endpoints) != 1 {
		t.Errorf("endpoints after update = %d, want 1", len(bridge.endpoints))
	}
	bridge.mu.RUnlock()
}

// A span stream that breaks off drops that peer's answer and is counted — it
// is never returned as a smaller, complete-looking answer.
func TestBufferBridge_BrokenTraceStreamIsAnErrorNotAPartialAnswer(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for i := 0; i < 3; i++ {
			_ = enc.Encode(schema.TraceRow{TimestampUnixNano: base.UnixNano(), TraceID: "t", SpanName: "ok"})
		}
		_, _ = io.WriteString(w, `{"timestamp_unix_nano": 1, "trace_id": "cut`)
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})
	before := metrics.BufferBridgeErrors.Get("decode")
	got, nonces := bridge.QueryTraces(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano(), tenantScope{account: "0", project: "0"})
	if len(nonces) != 0 {
		t.Errorf("segments excluded = %v; an answer without a segment header excludes none", nonces)
	}
	if len(got) != 0 {
		t.Fatalf("got %d spans from a broken stream, want none", len(got))
	}
	if metrics.BufferBridgeErrors.Get("decode") <= before {
		t.Error("the broken stream was not counted")
	}
}

// The nonces of the segments a peer read its spans from come back with the
// spans, from every peer, and a peer that fails contributes neither spans nor
// nonces (so none of its objects is dropped from the scan).
func TestBufferBridge_NoncesComeWithTheRows(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	peer := func(nonces string, status int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(buffer.TenantScopeHeader, "0:0")
			if nonces != "" {
				w.Header().Set(buffer.SegmentsHeader, nonces)
			}
			if status != 0 {
				w.WriteHeader(status)
				return
			}
			_ = json.NewEncoder(w).Encode(schema.TraceRow{TimestampUnixNano: base.UnixNano(), TraceID: "t", SpanName: "op"})
		}))
	}
	a, b, failing := peer("65000000aaaabbbb,65000000ccccdddd", 0), peer("65000000eeeeffff", 0), peer("65000000deadbeef", http.StatusInternalServerError)
	defer a.Close()
	defer b.Close()
	defer failing.Close()

	bridge := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	bridge.SetEndpoints([]string{a.URL, b.URL, failing.URL})
	rows, nonces := bridge.QueryTraces(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano(), tenantScope{account: "0", project: "0"})
	if len(rows) != 2 {
		t.Errorf("%d spans, want 1 from each answering peer", len(rows))
	}
	want := []string{"65000000aaaabbbb", "65000000ccccdddd", "65000000eeeeffff"}
	if len(nonces) != len(want) {
		t.Fatalf("nonces = %v, want %v (and none of the failing peer's)", nonces, want)
	}
	for _, n := range want {
		if _, ok := nonces[n]; !ok {
			t.Errorf("nonce %s missing from %v", n, nonces)
		}
	}
}
