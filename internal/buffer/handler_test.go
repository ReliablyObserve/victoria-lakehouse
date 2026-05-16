package buffer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type mockBufferStore struct {
	logRows   []schema.LogRow
	traceRows []schema.TraceRow
}

func (m *mockBufferStore) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	var result []schema.LogRow
	for _, r := range m.logRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func (m *mockBufferStore) BufferedTraceRows(startNs, endNs int64) []schema.TraceRow {
	var result []schema.TraceRow
	for _, r := range m.traceRows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			result = append(result, r)
		}
	}
	return result
}

func TestBufferQuery_Logs(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "svc"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "svc"},
			{TimestampUnixNano: base.Add(time.Hour).UnixNano(), Body: "out of range"},
		},
	}

	h := NewHandler(store)
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start="+
		fmt.Sprintf("%d", base.UnixNano())+"&end="+fmt.Sprintf("%d", base.Add(time.Minute).UnixNano())+
		"&mode=logs", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 2 {
		t.Errorf("got %d rows, want 2", len(lines))
	}
}

func TestBufferQuery_Traces(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: base.UnixNano(), TraceID: "t1", SpanName: "op1"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), TraceID: "t2", SpanName: "op2"},
		},
	}

	h := NewHandler(store)
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start="+
		fmt.Sprintf("%d", base.UnixNano())+"&end="+fmt.Sprintf("%d", base.Add(time.Minute).UnixNano())+
		"&mode=traces", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 2 {
		t.Errorf("got %d rows, want 2", len(lines))
	}
}

func TestBufferQuery_MissingParams(t *testing.T) {
	h := NewHandler(&mockBufferStore{})

	tests := []struct {
		name string
		url  string
	}{
		{"no params", "/internal/buffer/query"},
		{"missing end", "/internal/buffer/query?start=0&mode=logs"},
		{"missing start", "/internal/buffer/query?end=1000&mode=logs"},
		{"missing mode", "/internal/buffer/query?start=0&end=1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tt.url, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestBufferQuery_InvalidStart(t *testing.T) {
	h := NewHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=abc&end=1000&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_InvalidEnd(t *testing.T) {
	h := NewHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=abc&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_InvalidMode(t *testing.T) {
	h := NewHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=invalid", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_Empty(t *testing.T) {
	h := NewHandler(&mockBufferStore{})
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if len(body) != 0 {
		t.Errorf("empty buffer should return empty body, got %d bytes", len(body))
	}
}

func splitNDJSON(data []byte) []json.RawMessage {
	var result []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		result = append(result, raw)
	}
	return result
}
