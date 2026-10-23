package selectapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// engineStore answers RunQuery with upstream's query engine over an in-process
// buffer (the membuffer store the insert path writes), so the Jaeger handlers
// get the blocks the way upstream hands them out: in memory each worker reuses
// for its next block (WriteDataBlockFunc may not keep a block's values after it
// returns, lib/logstorage/storage_search.go).
type engineStore struct {
	dataStore
	st *membuffer.Store
}

func (e *engineStore) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, tenantIDs, q, false, nil)
	return e.st.RunQuery(qctx, writeBlock)
}

type testSpan struct {
	traceID, spanID, service, op string
	start                        time.Time
}

func newEngineStore(t *testing.T, spans []testSpan) *engineStore {
	t.Helper()
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	// VictoriaTraces' stream fields: one stream per (service, operation).
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name", "name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	for _, s := range spans {
		lr.MustAdd(logstorage.TenantID{}, s.start.UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: s.service},
			{Name: "name", Value: s.op},
			{Name: "trace_id", Value: s.traceID},
			{Name: "span_id", Value: s.spanID},
			{Name: "start_time_unix_nano", Value: fmt.Sprint(s.start.UnixNano())},
			{Name: "duration", Value: "1000"},
		}, -1)
	}
	st.MustAddRows(lr)
	st.DebugFlush()
	return &engineStore{st: st}
}

func jaegerGet(t *testing.T, h *Handler, path string) jaegerTracesResponseForTest {
	t.Helper()
	mux := http.NewServeMux()
	h.Register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", path, rec.Code, rec.Body.String())
	}
	var resp jaegerTracesResponseForTest
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("GET %s: %v: %s", path, err, rec.Body.String())
	}
	return resp
}

type jaegerTracesResponseForTest struct {
	Data []struct {
		TraceID string `json:"traceID"`
		Spans   []struct {
			TraceID       string `json:"traceID"`
			SpanID        string `json:"spanID"`
			OperationName string `json:"operationName"`
			ProcessID     string `json:"processID"`
		} `json:"spans"`
		Processes map[string]struct {
			ServiceName string `json:"serviceName"`
		} `json:"processes"`
	} `json:"data"`
}

// spanSet is the (trace, span, operation, service) of every span of resp.
func spanSet(resp jaegerTracesResponseForTest) []string {
	var out []string
	for _, tr := range resp.Data {
		for _, s := range tr.Spans {
			out = append(out, fmt.Sprintf("%s/%s/%s/%s/%s", tr.TraceID, s.TraceID, s.SpanID, s.OperationName, tr.Processes[s.ProcessID].ServiceName))
		}
	}
	sort.Strings(out)
	return out
}

// blockMemoryDay is the day of the test spans.
func blockMemoryDay() time.Time { return time.Date(2023, 7, 29, 1, 13, 0, 0, time.UTC) }

// Trace by ID: the spans of one trace in two streams on one day are two blocks
// of one part, read one after the other by one worker. The spans of the first
// block must keep their own ids, operations and services.
func TestJaegerTrace_KeepsTheSpansOfEveryBlock(t *testing.T) {
	var spans []testSpan
	var want []string
	for i := 0; i < 4; i++ {
		for _, svc := range []string{"svc-a", "svc-b"} {
			s := testSpan{traceID: "trace-0001", spanID: fmt.Sprintf("%s-span-%d", svc, i), service: svc, op: "op-" + svc, start: blockMemoryDay().Add(time.Duration(i) * time.Millisecond)}
			spans = append(spans, s)
			want = append(want, fmt.Sprintf("%s/%s/%s/%s/%s", s.traceID, s.traceID, s.spanID, s.op, s.service))
		}
	}
	sort.Strings(want)
	store := newEngineStore(t, spans)
	h := NewHandler(store, testConfig(config.ModeTraces))
	for read := 0; read < 10; read++ {
		got := spanSet(jaegerGet(t, h, "/select/jaeger/api/traces/trace-0001"))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("read %d: spans\n got %v\nwant %v", read, got, want)
		}
	}
}

// Search: the traces of one service in two operations are two blocks of one
// part. Every trace of the first block must be listed under its own id.
func TestJaegerSearch_KeepsTheTracesOfEveryBlock(t *testing.T) {
	var spans []testSpan
	var want []string
	for i := 0; i < 4; i++ {
		for _, op := range []string{"op-a", "op-b"} {
			s := testSpan{traceID: fmt.Sprintf("%s-trace-%d", op, i), spanID: fmt.Sprintf("%s-span-%d", op, i), service: "svc", op: op, start: blockMemoryDay().Add(time.Duration(i) * time.Millisecond)}
			spans = append(spans, s)
			want = append(want, fmt.Sprintf("%s/%s/%s/%s/%s", s.traceID, s.traceID, s.spanID, s.op, s.service))
		}
	}
	sort.Strings(want)
	store := newEngineStore(t, spans)
	h := NewHandler(store, testConfig(config.ModeTraces))
	path := fmt.Sprintf("/select/jaeger/api/traces?service=svc&limit=100&start=%d&end=%d",
		blockMemoryDay().Add(-time.Hour).UnixMicro(), blockMemoryDay().Add(time.Hour).UnixMicro())
	for read := 0; read < 10; read++ {
		got := spanSet(jaegerGet(t, h, path))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("read %d: spans\n got %v\nwant %v", read, got, want)
		}
	}
}

// parallelStore calls writeBlock from two workers at once, which upstream's
// WriteDataBlockFunc allows (the workerID argument), so the handlers must not
// share unguarded state between calls. Run with -race.
type parallelStore struct {
	dataStore
	blocks [2][]map[string]string
}

func (p *parallelStore) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range p.blocks {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for _, row := range p.blocks[w] {
				cols := make([]logstorage.BlockColumn, 0, len(row))
				for k, v := range row {
					cols = append(cols, logstorage.BlockColumn{Name: k, Values: []string{v}})
				}
				var db logstorage.DataBlock
				db.SetColumns(cols)
				writeBlock(uint(w), &db)
			}
		}(w)
	}
	close(start)
	wg.Wait()
	return nil
}

func TestJaegerHandlers_WriteBlockFromParallelWorkers(t *testing.T) {
	p := &parallelStore{}
	for w := range p.blocks {
		for i := 0; i < 50; i++ {
			p.blocks[w] = append(p.blocks[w], map[string]string{
				"trace_id":                   "trace-0001",
				"span_id":                    fmt.Sprintf("w%d-span-%02d", w, i),
				"name":                       "op",
				"resource_attr:service.name": "svc",
				"start_time_unix_nano":       "1690593180000000000",
				"duration":                   "1000",
			})
		}
	}
	h := NewHandler(p, testConfig(config.ModeTraces))
	if got := spanSet(jaegerGet(t, h, "/select/jaeger/api/traces/trace-0001")); len(got) != 100 {
		t.Fatalf("trace by ID returned %d spans, want 100", len(got))
	}
	if got := spanSet(jaegerGet(t, h, "/select/jaeger/api/traces?service=svc&limit=100&start=1&end=9999999999999999")); len(got) != 100 {
		t.Fatalf("search returned %d spans, want 100", len(got))
	}
}
