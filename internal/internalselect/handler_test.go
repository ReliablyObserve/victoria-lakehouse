package internalselect

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/protocol"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type mockStorage struct {
	runQueryFn             func(ctx context.Context, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error
	getFieldNamesFn        func(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error)
	getFieldValuesFn       func(ctx context.Context, qctx *storage.QueryContext, fieldName string, limit int) ([]storage.ValueWithHits, error)
	getStreamFieldNamesFn  func(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error)
	getStreamFieldValuesFn func(ctx context.Context, qctx *storage.QueryContext, fieldName string) ([]storage.ValueWithHits, error)
	getStreamsFn           func(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error)
	getStreamIDsFn         func(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error)
	getTenantIDsFn         func(ctx context.Context, qctx *storage.QueryContext) ([]storage.TenantID, error)
}

func (m *mockStorage) RunQuery(ctx context.Context, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	if m.runQueryFn != nil {
		return m.runQueryFn(ctx, qctx, writeBlock)
	}
	return nil
}
func (m *mockStorage) GetFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	if m.getFieldNamesFn != nil {
		return m.getFieldNamesFn(ctx, qctx)
	}
	return nil, nil
}
func (m *mockStorage) GetFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string, limit int) ([]storage.ValueWithHits, error) {
	if m.getFieldValuesFn != nil {
		return m.getFieldValuesFn(ctx, qctx, fieldName, limit)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	if m.getStreamFieldNamesFn != nil {
		return m.getStreamFieldNamesFn(ctx, qctx)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string) ([]storage.ValueWithHits, error) {
	if m.getStreamFieldValuesFn != nil {
		return m.getStreamFieldValuesFn(ctx, qctx, fieldName)
	}
	return nil, nil
}
func (m *mockStorage) GetStreams(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	if m.getStreamsFn != nil {
		return m.getStreamsFn(ctx, qctx)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	if m.getStreamIDsFn != nil {
		return m.getStreamIDsFn(ctx, qctx)
	}
	return nil, nil
}
func (m *mockStorage) GetTenantIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.TenantID, error) {
	if m.getTenantIDsFn != nil {
		return m.getTenantIDsFn(ctx, qctx)
	}
	return nil, nil
}
func (m *mockStorage) Close() error { return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandler_Query_EmptyResult(t *testing.T) {
	h := NewHandler(&mockStorage{}, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/internal/select/query?start=1000&end=2000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if enc := rec.Header().Get("Content-Encoding"); enc != "zstd" {
		t.Errorf("Content-Encoding = %q, want %q", enc, "zstd")
	}
}

func TestHandler_Query_WithDataBlocks(t *testing.T) {
	store := &mockStorage{
		runQueryFn: func(_ context.Context, _ *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
			writeBlock(0, &storage.DataBlock{
				RowsCount: 2,
				Columns: []storage.BlockColumn{
					{Name: "_msg", Values: []string{"hello", "world"}},
				},
			})
			return nil
		},
	}

	h := NewHandler(store, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/internal/select/query?start=1000&end=2000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	dec, err := zstd.NewReader(bytes.NewReader(rec.Body.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()

	decompressed, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}

	db, err := protocol.ReadDataBlockStream(bytes.NewReader(decompressed))
	if err != nil {
		t.Fatal(err)
	}

	if db.RowsCount != 2 {
		t.Errorf("RowsCount = %d, want 2", db.RowsCount)
	}
	if len(db.Columns) != 1 {
		t.Fatalf("columns = %d, want 1", len(db.Columns))
	}
	if db.Columns[0].Name != "_msg" {
		t.Errorf("column name = %q, want %q", db.Columns[0].Name, "_msg")
	}
}

func TestHandler_Query_MethodNotAllowed(t *testing.T) {
	h := NewHandler(&mockStorage{}, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/internal/select/query", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandler_FieldNames(t *testing.T) {
	store := &mockStorage{
		getFieldNamesFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{
				{Value: "_time", Hits: 100},
				{Value: "_msg", Hits: 50},
			}, nil
		},
	}

	h := NewHandler(store, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/internal/select/field_names?start=1000&end=2000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	vals, err := protocol.UnmarshalValueWithHits(rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Fatalf("len = %d, want 2", len(vals))
	}
	if vals[0].Value != "_time" {
		t.Errorf("vals[0] = %q, want %q", vals[0].Value, "_time")
	}
}

func TestHandler_FieldValues(t *testing.T) {
	store := &mockStorage{
		getFieldValuesFn: func(_ context.Context, _ *storage.QueryContext, fieldName string, limit int) ([]storage.ValueWithHits, error) {
			if fieldName != "service" {
				t.Errorf("fieldName = %q, want %q", fieldName, "service")
			}
			if limit != 10 {
				t.Errorf("limit = %d, want 10", limit)
			}
			return []storage.ValueWithHits{{Value: "api-gw", Hits: 42}}, nil
		},
	}

	h := NewHandler(store, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/internal/select/field_values?start=1000&end=2000&field=service&limit=10", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	vals, err := protocol.UnmarshalValueWithHits(rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0].Value != "api-gw" {
		t.Errorf("unexpected vals: %+v", vals)
	}
}

func TestHandler_TenantIDs(t *testing.T) {
	store := &mockStorage{
		getTenantIDsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.TenantID, error) {
			return []storage.TenantID{{AccountID: 1, ProjectID: 2}}, nil
		},
	}

	h := NewHandler(store, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/internal/select/tenant_ids?start=1000&end=2000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	ids, err := protocol.UnmarshalTenantIDs(rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0].AccountID != 1 || ids[0].ProjectID != 2 {
		t.Errorf("unexpected ids: %+v", ids)
	}
}

func TestHandler_DeleteNoop(t *testing.T) {
	h := NewHandler(&mockStorage{}, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, path := range []string{"/internal/select/delete_run", "/internal/select/delete_stop", "/internal/select/delete_active_tasks"} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want %d", path, rec.Code, http.StatusOK)
		}
	}
}

func TestHandler_StreamEndpoints(t *testing.T) {
	store := &mockStorage{
		getStreamFieldNamesFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: "service.name", Hits: 1}}, nil
		},
		getStreamFieldValuesFn: func(_ context.Context, _ *storage.QueryContext, _ string) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: "api-gw", Hits: 5}}, nil
		},
		getStreamsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: `{service="api"}`, Hits: 10}}, nil
		},
		getStreamIDsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: "abc123", Hits: 1}}, nil
		},
	}

	h := NewHandler(store, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	endpoints := []string{
		"/internal/select/stream_field_names?start=1&end=2",
		"/internal/select/stream_field_values?start=1&end=2&field=service.name",
		"/internal/select/streams?start=1&end=2",
		"/internal/select/stream_ids?start=1&end=2",
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d", ep, rec.Code)
		}
		vals, err := protocol.UnmarshalValueWithHits(rec.Body.Bytes())
		if err != nil {
			t.Errorf("%s: unmarshal error: %v", ep, err)
		}
		if len(vals) != 1 {
			t.Errorf("%s: len = %d, want 1", ep, len(vals))
		}
	}
}

func TestParseQueryContext_QueryParams(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/internal/select/query?start=1000000000&end=2000000000&query=service:api&AccountID=1&ProjectID=2&columns=_time,_msg",
		nil)

	qctx, err := parseQueryContext(req)
	if err != nil {
		t.Fatal(err)
	}
	if qctx.StartNs != 1000000000 {
		t.Errorf("StartNs = %d", qctx.StartNs)
	}
	if qctx.EndNs != 2000000000 {
		t.Errorf("EndNs = %d", qctx.EndNs)
	}
	if qctx.Query != "service:api" {
		t.Errorf("Query = %q", qctx.Query)
	}
	if len(qctx.TenantIDs) != 1 || qctx.TenantIDs[0].AccountID != 1 {
		t.Errorf("TenantIDs = %+v", qctx.TenantIDs)
	}
	if len(qctx.RequestedColumns) != 2 {
		t.Errorf("RequestedColumns = %v", qctx.RequestedColumns)
	}
}

func TestParseQueryContext_BinaryBody(t *testing.T) {
	body := make([]byte, 0, 32)

	start := make([]byte, 8)
	binary.BigEndian.PutUint64(start, 1000000000)
	body = append(body, start...)

	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, 2000000000)
	body = append(body, end...)

	query := "test query"
	qlen := make([]byte, 4)
	binary.BigEndian.PutUint32(qlen, uint32(len(query)))
	body = append(body, qlen...)
	body = append(body, []byte(query)...)

	req := httptest.NewRequest(http.MethodPost, "/internal/select/query", bytes.NewReader(body))

	qctx, err := parseQueryContext(req)
	if err != nil {
		t.Fatal(err)
	}
	if qctx.StartNs != 1000000000 {
		t.Errorf("StartNs = %d", qctx.StartNs)
	}
	if qctx.EndNs != 2000000000 {
		t.Errorf("EndNs = %d", qctx.EndNs)
	}
	if qctx.Query != "test query" {
		t.Errorf("Query = %q", qctx.Query)
	}
}

func TestHandler_AllEndpointsRegistered(t *testing.T) {
	h := NewHandler(&mockStorage{}, testLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/internal/select/query"},
		{http.MethodGet, "/internal/select/field_names"},
		{http.MethodGet, "/internal/select/field_values"},
		{http.MethodGet, "/internal/select/stream_field_names"},
		{http.MethodGet, "/internal/select/stream_field_values"},
		{http.MethodGet, "/internal/select/streams"},
		{http.MethodGet, "/internal/select/stream_ids"},
		{http.MethodGet, "/internal/select/tenant_ids"},
		{http.MethodPost, "/internal/select/delete_run"},
		{http.MethodPost, "/internal/select/delete_stop"},
		{http.MethodPost, "/internal/select/delete_active_tasks"},
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(ep.method, ep.path+"?start=1&end=2", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code == http.StatusNotFound {
			t.Errorf("%s %s: not found (404)", ep.method, ep.path)
		}
	}
}
