package parquets3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestBufferBridge_QueryLogs(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: base.UnixNano(), Body: "hello", ServiceName: "svc"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	got, err := bridge.QueryLogs(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
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

	got, err := bridge.QueryTraces(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
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

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Error("disabled bridge should return empty")
	}
}

func TestBufferBridge_DisabledTraces(t *testing.T) {
	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: false,
	}, config.ModeTraces)

	got, err := bridge.QueryTraces(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
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

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Error("no endpoints should return empty")
	}
}

func TestBufferBridge_MultipleEndpoints(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)

	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(schema.LogRow{TimestampUnixNano: base.UnixNano(), Body: "from-1"})
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(schema.LogRow{TimestampUnixNano: base.UnixNano(), Body: "from-2"})
	}))
	defer srv2.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv1.URL, srv2.URL})

	got, err := bridge.QueryLogs(context.Background(), base.UnixNano(), base.Add(time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
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

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal("should not return error for endpoint failures (graceful degradation)")
	}
	if len(got) != 0 {
		t.Error("unreachable endpoint should return empty")
	}
}

func TestBufferBridge_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeLogs)
	bridge.SetEndpoints([]string{srv.URL})

	got, err := bridge.QueryLogs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal("should handle server errors gracefully")
	}
	if len(got) != 0 {
		t.Error("500 response should return empty")
	}
}

func TestBufferBridge_TraceServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	bridge := NewBufferBridge(&config.SelectConfig{
		BufferQueryEnabled: true,
		BufferQueryTimeout: 2 * time.Second,
	}, config.ModeTraces)
	bridge.SetEndpoints([]string{srv.URL})

	got, err := bridge.QueryTraces(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal("should handle server errors gracefully")
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

	got, err := bridge.QueryTraces(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal("should not return error for trace endpoint failures")
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

	got, err := bridge.QueryTraces(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
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
