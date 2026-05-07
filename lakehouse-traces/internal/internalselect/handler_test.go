package internalselect

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding/zstd"
	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type mockStorage struct {
	runQueryFn             func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error
	getFieldNamesFn        func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error)
	getFieldValuesFn       func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	getStreamFieldNamesFn  func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error)
	getStreamFieldValuesFn func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	getStreamsFn           func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error)
	getStreamIDsFn         func(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error)
}

func (m *mockStorage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	if m.runQueryFn != nil {
		return m.runQueryFn(ctx, tenantIDs, q, writeBlock)
	}
	return nil
}
func (m *mockStorage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	if m.getFieldNamesFn != nil {
		return m.getFieldNamesFn(ctx, tenantIDs, q)
	}
	return nil, nil
}
func (m *mockStorage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	if m.getFieldValuesFn != nil {
		return m.getFieldValuesFn(ctx, tenantIDs, q, fieldName, limit)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	if m.getStreamFieldNamesFn != nil {
		return m.getStreamFieldNamesFn(ctx, tenantIDs, q)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	if m.getStreamFieldValuesFn != nil {
		return m.getStreamFieldValuesFn(ctx, tenantIDs, q, fieldName, limit)
	}
	return nil, nil
}
func (m *mockStorage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	if m.getStreamsFn != nil {
		return m.getStreamsFn(ctx, tenantIDs, q, limit)
	}
	return nil, nil
}
func (m *mockStorage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	if m.getStreamIDsFn != nil {
		return m.getStreamIDsFn(ctx, tenantIDs, q, limit)
	}
	return nil, nil
}
func (m *mockStorage) Close() error { return nil }

func vlQueryURL() string {
	return "/internal/select/query?version=" + netselect.QueryProtocolVersion +
		"&tenant_ids=[]&timestamp=0&query=*" +
		"&disable_compression=false&allow_partial_response=false&hidden_fields_filters=[]"
}

func vlMetadataURL(path, version string) string {
	return path + "?version=" + version +
		"&tenant_ids=[]&timestamp=0&query=*" +
		"&disable_compression=false&allow_partial_response=false&hidden_fields_filters=[]"
}

func TestHandler_Query_EmptyResult(t *testing.T) {
	h := NewHandler(&mockStorage{}, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, vlQueryURL(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestHandler_Query_WithDataBlocks(t *testing.T) {
	store := &mockStorage{
		runQueryFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
			db := &logstorage.DataBlock{}
			db.SetColumns([]logstorage.BlockColumn{
				{Name: "_msg", Values: []string{"hello", "world"}},
			})
			writeBlock(0, db)
			return nil
		},
	}

	h := NewHandler(store, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, vlQueryURL(), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	body := rec.Body.Bytes()
	if len(body) < 8 {
		t.Fatal("response too short")
	}

	blockLen := encoding.UnmarshalUint64(body[:8])
	blockData := body[8 : 8+blockLen]

	decompressed, err := zstd.Decompress(nil, blockData)
	if err != nil {
		t.Fatal(err)
	}

	if len(decompressed) < 1 {
		t.Fatal("decompressed data too short")
	}
	if decompressed[0] != 0 {
		t.Errorf("expected data block flag 0x00, got 0x%02x", decompressed[0])
	}

	var db logstorage.DataBlock
	tail, _, err := db.UnmarshalInplace(decompressed[1:], nil)
	if err != nil {
		t.Fatal(err)
	}

	if db.RowsCount() != 2 {
		t.Errorf("RowsCount = %d, want 2", db.RowsCount())
	}
	cols := db.GetColumns(false)
	if len(cols) != 1 {
		t.Fatalf("columns = %d, want 1", len(cols))
	}
	if cols[0].Name != "_msg" {
		t.Errorf("column name = %q, want %q", cols[0].Name, "_msg")
	}

	if len(tail) > 0 {
		if tail[0] != 1 {
			t.Errorf("expected stats block flag 0x01 in remaining data, got 0x%02x", tail[0])
		}
	}
}

func TestHandler_FieldNames(t *testing.T) {
	store := &mockStorage{
		getFieldNamesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{
				{Value: "_time", Hits: 100},
				{Value: "_msg", Hits: 50},
			}, nil
		},
	}

	h := NewHandler(store, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	url := vlMetadataURL("/internal/select/field_names", netselect.FieldNamesProtocolVersion)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	data, err := zstd.Decompress(nil, rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	count := encoding.UnmarshalUint64(data[:8])
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestHandler_FieldValues(t *testing.T) {
	store := &mockStorage{
		getFieldValuesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
			if fieldName != "service" {
				t.Errorf("fieldName = %q, want %q", fieldName, "service")
			}
			if limit != 10 {
				t.Errorf("limit = %d, want 10", limit)
			}
			return []logstorage.ValueWithHits{{Value: "api-gw", Hits: 42}}, nil
		},
	}

	h := NewHandler(store, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	url := vlMetadataURL("/internal/select/field_values", netselect.FieldValuesProtocolVersion) + "&field=service&limit=10"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rec.Code, rec.Body.String())
	}

	data, err := zstd.Decompress(nil, rec.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	count := encoding.UnmarshalUint64(data[:8])
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestHandler_TenantIDs(t *testing.T) {
	h := NewHandler(&mockStorage{}, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/internal/select/tenant_ids?start=1000&end=2000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %s", rec.Code, rec.Body.String())
	}
}

func TestHandler_DeleteNoop(t *testing.T) {
	h := NewHandler(&mockStorage{}, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	for _, tc := range []struct {
		path    string
		version string
	}{
		{"/internal/delete/run_task", netselect.DeleteRunTaskProtocolVersion},
		{"/internal/delete/stop_task", netselect.DeleteStopTaskProtocolVersion},
		{"/internal/delete/active_tasks", netselect.DeleteActiveTasksProtocolVersion},
	} {
		req := httptest.NewRequest(http.MethodPost, tc.path+"?version="+tc.version+"&task_id=test&timestamp=0&tenant_ids=[]&filter=*", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d, want %d; body: %s", tc.path, rec.Code, http.StatusOK, rec.Body.String())
		}
	}
}

func TestHandler_StreamEndpoints(t *testing.T) {
	store := &mockStorage{
		getStreamFieldNamesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: "service.name", Hits: 1}}, nil
		},
		getStreamFieldValuesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: "api-gw", Hits: 5}}, nil
		},
		getStreamsFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: `{service="api"}`, Hits: 10}}, nil
		},
		getStreamIDsFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: "abc123", Hits: 1}}, nil
		},
	}

	h := NewHandler(store, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	endpoints := []struct {
		path    string
		version string
	}{
		{"/internal/select/stream_field_names", netselect.StreamFieldNamesProtocolVersion},
		{"/internal/select/stream_field_values", netselect.StreamFieldValuesProtocolVersion},
		{"/internal/select/streams", netselect.StreamsProtocolVersion},
		{"/internal/select/stream_ids", netselect.StreamIDsProtocolVersion},
	}

	for _, ep := range endpoints {
		url := vlMetadataURL(ep.path, ep.version) + "&field=service.name&limit=100"
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: status = %d; body: %s", ep.path, rec.Code, rec.Body.String())
			continue
		}

		data, err := zstd.Decompress(nil, rec.Body.Bytes())
		if err != nil {
			t.Errorf("%s: decompress error: %v", ep.path, err)
			continue
		}

		count := encoding.UnmarshalUint64(data[:8])
		if count != 1 {
			t.Errorf("%s: count = %d, want 1", ep.path, count)
		}
	}
}

func TestHandler_AllEndpointsRegistered(t *testing.T) {
	h := NewHandler(&mockStorage{}, 30*time.Second)
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
		{http.MethodPost, "/internal/delete/run_task"},
		{http.MethodPost, "/internal/delete/stop_task"},
		{http.MethodPost, "/internal/delete/active_tasks"},
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
