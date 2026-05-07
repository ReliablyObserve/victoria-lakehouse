package internalselect

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage/netselect"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func vlParams(version string) string {
	return "version=" + version +
		"&tenant_ids=[]&timestamp=0&query=*" +
		"&disable_compression=false&allow_partial_response=false&hidden_fields_filters=[]"
}

func TestHandler_Race_MaxGoroutines(t *testing.T) {
	ms := &mockStorage{
		runQueryFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
			db := &logstorage.DataBlock{}
			db.SetColumns([]logstorage.BlockColumn{
				{Name: "_msg", Values: []string{"test"}},
			})
			writeBlock(0, db)
			return nil
		},
		getFieldNamesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: "f", Hits: 1}}, nil
		},
		getFieldValuesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
			return []logstorage.ValueWithHits{{Value: "v", Hits: 1}}, nil
		},
		getStreamsFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamIDsFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamFieldNamesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
			return nil, nil
		},
		getStreamFieldValuesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
			return nil, nil
		},
	}

	h := NewHandler(ms, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	endpoints := []struct {
		method string
		path   string
	}{
		{"POST", "/internal/select/query?" + vlParams(netselect.QueryProtocolVersion)},
		{"GET", "/internal/select/field_names?" + vlParams(netselect.FieldNamesProtocolVersion)},
		{"GET", "/internal/select/field_values?" + vlParams(netselect.FieldValuesProtocolVersion) + "&field=test&limit=100"},
		{"GET", "/internal/select/streams?" + vlParams(netselect.StreamsProtocolVersion) + "&limit=100"},
		{"GET", "/internal/select/stream_ids?" + vlParams(netselect.StreamIDsProtocolVersion) + "&limit=100"},
		{"GET", "/internal/select/stream_field_names?" + vlParams(netselect.StreamFieldNamesProtocolVersion)},
		{"GET", "/internal/select/stream_field_values?" + vlParams(netselect.StreamFieldValuesProtocolVersion) + "&field=test&limit=100"},
		{"GET", "/internal/select/tenant_ids?start=0&end=9999999999999999999"},
		{"POST", "/internal/delete/run_task?version=" + netselect.DeleteRunTaskProtocolVersion + "&task_id=t1&timestamp=0&tenant_ids=[]&filter=*"},
		{"POST", "/internal/delete/stop_task?version=" + netselect.DeleteStopTaskProtocolVersion + "&task_id=t1"},
		{"GET", "/internal/delete/active_tasks?version=" + netselect.DeleteActiveTasksProtocolVersion},
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
				_ = resp.Body.Close()
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
		runQueryFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
			db := &logstorage.DataBlock{}
			db.SetColumns([]logstorage.BlockColumn{
				{Name: "_msg", Values: make([]string, 10)},
			})
			writeBlock(0, db)
			return nil
		},
	}

	h := NewHandler(ms, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/internal/select/query?"+vlParams(netselect.QueryProtocolVersion), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
	}
}

func BenchmarkHandler_FieldNames(b *testing.B) {
	ms := &mockStorage{
		getFieldNamesFn: func(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
			vals := make([]logstorage.ValueWithHits, 50)
			for i := range vals {
				vals[i] = logstorage.ValueWithHits{Value: fmt.Sprintf("field-%d", i), Hits: uint64(i)}
			}
			return vals, nil
		},
	}

	h := NewHandler(ms, 30*time.Second)
	mux := http.NewServeMux()
	h.Register(mux)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/internal/select/field_names?"+vlParams(netselect.FieldNamesProtocolVersion), nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
	}
}
