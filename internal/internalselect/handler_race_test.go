package internalselect

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func testRaceLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestHandler_Race_MaxGoroutines(t *testing.T) {
	ms := &mockStorage{
		runQueryFn: func(_ context.Context, _ *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
			writeBlock(0, &storage.DataBlock{
				RowsCount: 1,
				Columns: []storage.BlockColumn{
					{Name: "_msg", Values: []string{"test"}},
				},
			})
			return nil
		},
		getFieldNamesFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: "f", Hits: 1}}, nil
		},
		getFieldValuesFn: func(_ context.Context, _ *storage.QueryContext, _ string, _ int) ([]storage.ValueWithHits, error) {
			return []storage.ValueWithHits{{Value: "v", Hits: 1}}, nil
		},
		getStreamsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamIDsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamFieldNamesFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamFieldValuesFn: func(_ context.Context, _ *storage.QueryContext, _ string) ([]storage.ValueWithHits, error) {
			return nil, nil
		},
		getTenantIDsFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.TenantID, error) {
			return nil, nil
		},
	}

	h := NewHandler(ms, testRaceLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	endpoints := []struct {
		method string
		path   string
	}{
		{"POST", "/internal/select/query?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/field_names?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/field_values?start=0&end=9999999999999999999&query=*&field=test"},
		{"GET", "/internal/select/streams?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/stream_ids?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/stream_field_names?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/stream_field_values?start=0&end=9999999999999999999&query=*&field=test"},
		{"GET", "/internal/select/tenant_ids?start=0&end=9999999999999999999&query=*"},
		{"POST", "/internal/select/delete_run?start=0&end=9999999999999999999&query=*"},
		{"POST", "/internal/select/delete_stop?start=0&end=9999999999999999999&query=*"},
		{"GET", "/internal/select/delete_active_tasks"},
	}

	const goroutines = 200
	const ops = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			client := &http.Client{Timeout: 5 * time.Second}
			for i := 0; i < ops; i++ {
				ep := endpoints[rng.Intn(len(endpoints))]
				url := srv.URL + ep.path
				req, err := http.NewRequest(ep.method, url, nil)
				if err != nil {
					continue
				}
				resp, err := client.Do(req)
				if err != nil {
					continue
				}
				_, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
				if i%10 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkHandler_Query(b *testing.B) {
	ms := &mockStorage{
		runQueryFn: func(_ context.Context, _ *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
			writeBlock(0, &storage.DataBlock{
				RowsCount: 10,
				Columns: []storage.BlockColumn{
					{Name: "_msg", Values: make([]string, 10)},
				},
			})
			return nil
		},
	}

	h := NewHandler(ms, testRaceLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/internal/select/query?start=0&end=9999999999999999999&query=*", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
	}
}

func BenchmarkHandler_FieldNames(b *testing.B) {
	ms := &mockStorage{
		getFieldNamesFn: func(_ context.Context, _ *storage.QueryContext) ([]storage.ValueWithHits, error) {
			vals := make([]storage.ValueWithHits, 50)
			for i := range vals {
				vals[i] = storage.ValueWithHits{Value: fmt.Sprintf("field-%d", i), Hits: uint64(i)}
			}
			return vals, nil
		},
	}

	h := NewHandler(ms, testRaceLogger(), 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/internal/select/field_names?start=0&end=9999999999999999999&query=*", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
	}
}
