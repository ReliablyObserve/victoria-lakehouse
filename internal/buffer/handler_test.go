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

	h := NewHandler(store, "")
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

	h := NewHandler(store, "")
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
	h := NewHandler(&mockBufferStore{}, "")

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
	h := NewHandler(&mockBufferStore{}, "")
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=abc&end=1000&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_InvalidEnd(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "")
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=abc&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_InvalidMode(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "")
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=invalid", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestBufferQuery_Empty(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "")
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

func TestBufferQuery_MethodNotAllowed(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "")
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("status = %d, want 405", rec.Code)
			}
		})
	}
}

func TestBufferQuery_AuthRequired(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "secret-key")

	t.Run("no auth header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("wrong key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want 401", rec.Code)
		}
	})

	t.Run("correct key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
		req.Header.Set("Authorization", "Bearer secret-key")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("status = %d, want 200", rec.Code)
		}
	})
}

func TestBufferQuery_NoAuthWhenKeyEmpty(t *testing.T) {
	h := NewHandler(&mockBufferStore{}, "")
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when no auth key configured", rec.Code)
	}
}

// --- Edge case tests ---

func TestBufferQuery_InvertedTimeRange(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: base.UnixNano(), Body: "a", ServiceName: "svc"},
			{TimestampUnixNano: base.Add(time.Second).UnixNano(), Body: "b", ServiceName: "svc"},
		},
	}

	h := NewHandler(store, "")
	// startNs > endNs: inverted range should return empty results
	startNs := base.Add(time.Minute).UnixNano()
	endNs := base.UnixNano()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/internal/buffer/query?start=%d&end=%d&mode=logs", startNs, endNs), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 0 {
		t.Errorf("inverted time range: got %d rows, want 0", len(lines))
	}
}

func TestBufferQuery_InvertedTimeRange_Traces(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: base.UnixNano(), TraceID: "t1", SpanName: "op1"},
		},
	}

	h := NewHandler(store, "")
	startNs := base.Add(time.Hour).UnixNano()
	endNs := base.UnixNano()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/internal/buffer/query?start=%d&end=%d&mode=traces", startNs, endNs), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 0 {
		t.Errorf("inverted time range traces: got %d rows, want 0", len(lines))
	}
}

func TestBufferQuery_Int64Boundaries(t *testing.T) {
	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: 0, Body: "zero", ServiceName: "svc"},
			{TimestampUnixNano: 9223372036854775806, Body: "near-max", ServiceName: "svc"},
		},
	}

	tests := []struct {
		name     string
		start    int64
		end      int64
		wantRows int
	}{
		{"MaxInt64 range", 0, 9223372036854775807, 2},
		{"MinInt64 to zero", -9223372036854775808, 0, 0},
		{"MinInt64 to MaxInt64", -9223372036854775808, 9223372036854775807, 2},
		{"MaxInt64 to MaxInt64", 9223372036854775807, 9223372036854775807, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(store, "")
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
				"/internal/buffer/query?start=%d&end=%d&mode=logs", tt.start, tt.end), nil)
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			body, _ := io.ReadAll(rec.Body)
			lines := splitNDJSON(body)
			if len(lines) != tt.wantRows {
				t.Errorf("got %d rows, want %d", len(lines), tt.wantRows)
			}
		})
	}
}

func TestBufferQuery_ZeroTimeRange(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: base.UnixNano(), Body: "exact", ServiceName: "svc"},
			{TimestampUnixNano: base.UnixNano() + 1, Body: "after", ServiceName: "svc"},
		},
	}

	h := NewHandler(store, "")
	// start == end: since the filter is >= start && < end, zero-width range returns nothing
	ts := base.UnixNano()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/internal/buffer/query?start=%d&end=%d&mode=logs", ts, ts), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 0 {
		t.Errorf("zero-width time range: got %d rows, want 0", len(lines))
	}
}

func TestBufferQuery_ZeroTimeRange_Traces(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	store := &mockBufferStore{
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: base.UnixNano(), TraceID: "t1", SpanName: "op1"},
		},
	}

	h := NewHandler(store, "")
	ts := base.UnixNano()
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf(
		"/internal/buffer/query?start=%d&end=%d&mode=traces", ts, ts), nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	lines := splitNDJSON(body)
	if len(lines) != 0 {
		t.Errorf("zero-width time range traces: got %d rows, want 0", len(lines))
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
