package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/jaeger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tempo"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// VictoriaTraces' vtselect.RequestHandler runs every non-tail request under
// context.WithTimeout(ctx, getMaxQueryDuration(r)): -search.maxQueryDuration
// (default 30s), lowered per request by the 'timeout' arg. The traces binary
// cannot link vtselect (its flags collide with VictoriaLogs' internalselect), so
// the Jaeger and Tempo routes carry the same deadline here. These tests drive
// every one of those routes over the Lakehouse storage adapter against a store
// whose reads stall until the context they were handed ends, which is what a
// stalled S3 read does.

// stallStore blocks every read until its context ends and records the deadline
// the context carried.
type stallStore struct {
	mockStore
	mu        sync.Mutex
	deadlines []time.Time
	global    []bool
	noLimit   int
	calls     int
	inflight  int
}

func (s *stallStore) stall(ctx context.Context) error {
	s.mu.Lock()
	s.calls++
	s.inflight++
	s.global = append(s.global, storage.IsGlobalRead(ctx))
	if d, ok := ctx.Deadline(); ok {
		s.deadlines = append(s.deadlines, d)
	} else {
		s.noLimit++
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()
	<-ctx.Done()
	return ctx.Err()
}

func (s *stallStore) stats() (calls, inflight, noLimit int, deadlines []time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.inflight, s.noLimit, append([]time.Time(nil), s.deadlines...)
}

func (s *stallStore) RunQuery(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	return s.stall(ctx)
}
func (s *stallStore) GetFieldNames(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}
func (s *stallStore) GetFieldValues(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}
func (s *stallStore) GetStreamFieldNames(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}
func (s *stallStore) GetStreamFieldValues(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ string, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}
func (s *stallStore) GetStreams(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}
func (s *stallStore) GetStreamIDs(ctx context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ uint64) ([]logstorage.ValueWithHits, error) {
	return nil, s.stall(ctx)
}

const dlTraceID = "0af7651916cd43dd8448eb211c80319c"

// deadlineRoutes is every Jaeger and Tempo route a traces pod serves, with the
// arguments that make it reach the store. group names the surface group.
func deadlineRoutes() []struct{ group, name, path, query string } {
	return []struct{ group, name, path, query string }{
		{"jaeger-trace-by-id", "select", "/select/jaeger/api/traces/" + dlTraceID, ""},
		{"jaeger-trace-by-id", "alias", "/api/traces/" + dlTraceID, ""},
		{"jaeger-search", "select", "/select/jaeger/api/traces", "service=checkout&limit=5"},
		{"jaeger-search", "alias", "/api/traces", "service=checkout&limit=5"},
		{"jaeger-services", "select", "/select/jaeger/api/services", ""},
		{"jaeger-services", "alias", "/api/services", ""},
		{"jaeger-operations", "select", "/select/jaeger/api/services/checkout/operations", ""},
		{"jaeger-operations", "alias", "/api/services/checkout/operations", ""},
		{"jaeger-dependencies", "select", "/select/jaeger/api/dependencies", "endTs=1790838001234&lookback=3600000"},
		{"jaeger-dependencies", "alias", "/api/dependencies", "endTs=1790838001234&lookback=3600000"},
		{"tempo-search", "traceql", "/select/tempo/api/search", "q=%7B%7D&limit=5"},
		{"tempo-search", "tags-param", "/select/tempo/api/search", "tags=service.name%3Dcheckout&limit=5"},
		{"tempo-tags", "tags", "/select/tempo/api/v2/search/tags", ""},
		{"tempo-tags", "values", "/select/tempo/api/v2/search/tag/service.name/values", ""},
		{"tempo-trace-by-id", "v1", "/select/tempo/api/traces/" + dlTraceID, ""},
		{"tempo-trace-by-id", "v2", "/select/tempo/api/v2/traces/" + dlTraceID, ""},
		{"traceql-metrics", "query_range", "/select/tempo/api/metrics/query_range", "q=%7B%7D+%7C+rate%28%29&step=60s"},
	}
}

func newDeadlineMux(t *testing.T, st storage.Storage, opts ...HandlerOption) *http.ServeMux {
	t.Helper()
	vtstorageadapter.Init(st)
	mux := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces), opts...).Register(mux)
	return mux
}

// serve runs one request with a safety context 10x longer than the deadline
// under test, so a handler that carries no deadline fails the assertions
// instead of hanging the test.
func serve(mux *http.ServeMux, path, rawQuery string, safety time.Duration) (*httptest.ResponseRecorder, time.Duration, time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), safety)
	defer cancel()
	req := httptest.NewRequest("GET", path+"?"+rawQuery, nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	began := time.Now()
	mux.ServeHTTP(rec, req)
	return rec, time.Since(began), began
}

// Every route ends a stalled read at the deadline: the store sees a context that
// carries it, the request fails with the deadline error, and the worker is
// free afterwards.
func TestJaegerTempoRoutes_StalledReadEndsAtTheDeadline(t *testing.T) {
	const timeout = 300 * time.Millisecond
	for _, rt := range deadlineRoutes() {
		t.Run(rt.group+"/"+rt.name, func(t *testing.T) {
			st := &stallStore{}
			mux := newDeadlineMux(t, st)
			q := rt.query
			if q != "" {
				q += "&"
			}
			q += "timeout=" + timeout.String()

			rec, took, began := serve(mux, rt.path, q, 10*timeout)

			calls, inflight, noLimit, deadlines := st.stats()
			if calls == 0 {
				t.Fatalf("the route never reached the store (status %d %q): the test does not exercise it", rec.Code, rec.Body.String())
			}
			if noLimit != 0 {
				t.Fatalf("%d of %d store reads got a context with no deadline", noLimit, calls)
			}
			for _, d := range deadlines {
				if d.After(began.Add(timeout + 100*time.Millisecond)) {
					t.Fatalf("the store's context ends at +%s, want at most the %s deadline", d.Sub(began), timeout)
				}
			}
			if took < timeout-50*time.Millisecond || took > timeout+time.Second {
				t.Fatalf("answered after %s, want about %s", took, timeout)
			}
			if rec.Code == http.StatusOK {
				t.Fatalf("answered 200 %q for a read that never finished", rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "deadline exceeded") {
				t.Fatalf("answer %d %q does not carry the deadline error", rec.Code, rec.Body.String())
			}
			if inflight != 0 {
				t.Fatalf("%d store reads still running after the answer", inflight)
			}
		})
	}
}

// The wrapper adds nothing to the answer: the status and body are exactly those
// the upstream handler gives a context that has hit its deadline when it is called
// directly. This is NOT a claim about hot VictoriaTraces, whose storage returns nil
// for an expired context and so answers an empty 200 (docs/read-path.md).
func TestJaegerTempoRoutes_WrapperAddsNothingToTheHandlerAnswer(t *testing.T) {
	const timeout = 200 * time.Millisecond
	direct := func(path, rawQuery string) (int, string) {
		st := &stallStore{}
		vtstorageadapter.Init(st)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req := httptest.NewRequest("GET", path+"?"+rawQuery, nil).WithContext(ctx)
		rec := httptest.NewRecorder()
		if strings.HasPrefix(path, "/select/tempo/") {
			normalizeTempoSearchParams(req)
			tempo.RequestHandler(ctx, rec, req)
		} else {
			jaeger.RequestHandler(ctx, rec, req)
		}
		return rec.Code, rec.Body.String()
	}
	for _, rt := range deadlineRoutes() {
		if !strings.HasPrefix(rt.path, "/select/") {
			continue
		}
		t.Run(rt.group+"/"+rt.name, func(t *testing.T) {
			q := rt.query
			if q != "" {
				q += "&"
			}
			wantCode, wantBody := direct(rt.path, q+"timeout=1s")

			st := &stallStore{}
			mux := newDeadlineMux(t, st)
			rec, _, _ := serve(mux, rt.path, q+"timeout="+timeout.String(), 10*timeout)
			if rec.Code != wantCode {
				t.Fatalf("status %d, upstream's handler answers %d", rec.Code, wantCode)
			}
			if rec.Body.String() != wantBody {
				t.Fatalf("body %q, upstream's handler answers %q", rec.Body.String(), wantBody)
			}
		})
	}
}

// 'timeout' lowers the deadline; a larger one is clamped to
// -search.maxQueryDuration; a bad one is upstream's 400.
func TestJaegerTempoRoutes_TimeoutArgument(t *testing.T) {
	t.Run("clamped to the flag", func(t *testing.T) {
		st := &stallStore{}
		mux := newDeadlineMux(t, st, WithMaxQueryDuration(250*time.Millisecond))
		rec, took, _ := serve(mux, "/select/jaeger/api/services", "timeout=1h", 5*time.Second)
		if took > time.Second || took < 200*time.Millisecond {
			t.Fatalf("timeout=1h was not clamped to the 250ms flag: answered after %s (%d %q)", took, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "deadline exceeded") {
			t.Fatalf("answer %d %q does not carry the deadline error", rec.Code, rec.Body.String())
		}
	})
	t.Run("flag applies without the argument", func(t *testing.T) {
		st := &stallStore{}
		mux := newDeadlineMux(t, st, WithMaxQueryDuration(250*time.Millisecond))
		_, took, _ := serve(mux, "/select/tempo/api/v2/search/tags", "", 5*time.Second)
		if took > time.Second || took < 200*time.Millisecond {
			t.Fatalf("the 250ms flag did not apply: answered after %s", took)
		}
	})
	t.Run("zero and negative use the flag", func(t *testing.T) {
		for _, v := range []string{"0s", "-5s"} {
			st := &stallStore{}
			mux := newDeadlineMux(t, st, WithMaxQueryDuration(250*time.Millisecond))
			_, took, _ := serve(mux, "/select/jaeger/api/services", "timeout="+v, 5*time.Second)
			if took > time.Second || took < 200*time.Millisecond {
				t.Fatalf("timeout=%s did not fall back to the flag: answered after %s", v, took)
			}
		}
	})
	t.Run("unparsable is upstream's 400", func(t *testing.T) {
		for _, p := range []string{"/select/jaeger/api/services", "/api/services", "/select/tempo/api/v2/search/tags"} {
			st := &stallStore{}
			mux := newDeadlineMux(t, st)
			rec, _, _ := serve(mux, p, "timeout=soon", 5*time.Second)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s: status %d, want 400", p, rec.Code)
			}
			if want := "cannot parse duration at 'timeout=soon' arg"; !strings.Contains(rec.Body.String(), want) {
				t.Fatalf("%s: body %q does not contain %q", p, rec.Body.String(), want)
			}
			if calls, _, _, _ := st.stats(); calls != 0 {
				t.Fatalf("%s: the store was read for a request that was refused", p)
			}
		}
	})
}

// Without any argument the default is upstream's: 30s.
func TestJaegerTempoRoutes_DefaultIsUpstreams(t *testing.T) {
	if DefaultMaxQueryDuration != 30*time.Second {
		t.Fatalf("DefaultMaxQueryDuration = %s, upstream's -search.maxQueryDuration is 30s", DefaultMaxQueryDuration)
	}
	st := &stallStore{}
	mux := newDeadlineMux(t, st)
	_, _, _ = serve(mux, "/select/jaeger/api/services", "timeout=200ms", 2*time.Second)
	_, _, noLimit, deadlines := st.stats()
	if noLimit != 0 || len(deadlines) == 0 {
		t.Fatalf("store reads: %d without a deadline, %d with", noLimit, len(deadlines))
	}
}

// A stalled request frees its place: after a run of deadline errors the next
// request is served, and no goroutine is left behind.
func TestJaegerTempoRoutes_DeadlineFreesTheWorkers(t *testing.T) {
	before := runtime.NumGoroutine()
	st := &stallStore{}
	mux := newDeadlineMux(t, st)
	// The Tempo tag routes are left out of the concurrent run: VictoriaTraces v0.12.0's
	// searchTags calls StripHintPipes on its package-level defaultNoopFilter, so two
	// concurrent tag requests race inside upstream (found by -race here; present on a
	// hot VictoriaTraces node too, and independent of the deadline; issue #801).
	all := deadlineRoutes()
	var routes []int
	for i, rt := range all {
		if rt.group != "tempo-tags" {
			routes = append(routes, i)
		}
	}
	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rt := all[routes[i%len(routes)]]
			q := rt.query
			if q != "" {
				q += "&"
			}
			serve(mux, rt.path, q+url.Values{"timeout": {"100ms"}}.Encode(), 5*time.Second)
		}(i)
	}
	wg.Wait()
	if _, inflight, _, _ := st.stats(); inflight != 0 {
		t.Fatalf("%d store reads still running", inflight)
	}

	// A normal request is served at once.
	ok := newDeadlineMux(t, &spanStore{start: time.Now().Add(-time.Minute), end: time.Now()})
	rec, took, _ := serve(ok, "/select/tempo/api/echo", "", 2*time.Second)
	if rec.Code != http.StatusOK || took > time.Second {
		t.Fatalf("a normal request after the stalls: %d in %s", rec.Code, took)
	}

	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before+4 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before+4 {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines: %d before, %d after the stalled requests ended\n%s", before, after, buf[:runtime.Stack(buf, true)])
	}
}

// The deadline hangs off the request's own context, as upstream's does: a client
// that goes away ends the read at once, without waiting for the deadline.
func TestJaegerTempoRoutes_ClientCancelEndsTheRead(t *testing.T) {
	for _, p := range []string{"/select/jaeger/api/services", "/api/services", "/select/tempo/api/v2/search/tags"} {
		st := &stallStore{}
		mux := newDeadlineMux(t, st)
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(150*time.Millisecond, cancel)
		rec := httptest.NewRecorder()
		began := time.Now()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", p, nil).WithContext(ctx))
		if took := time.Since(began); took > 2*time.Second {
			t.Fatalf("%s: a cancelled request ran for %s (the default deadline is %s)", p, took, DefaultMaxQueryDuration)
		}
		if _, inflight, _, _ := st.stats(); inflight != 0 {
			t.Fatalf("%s: %d store reads still running", p, inflight)
		}
		cancel()
	}
}

// The deadline context is derived from the request's scoped context: a request
// that proved the global-read credential keeps its cross-tenant scope on the
// Jaeger and the Tempo routes, and one that did not never gets it.
func TestJaegerTempoRoutes_KeepTheGlobalReadScope(t *testing.T) {
	cfg := testConfig(config.ModeTraces)
	cfg.Tenant.GlobalReadHeader = "X-Lakehouse-Global-Read"
	cfg.Tenant.GlobalReadValue = "s3cr3t"
	for _, p := range []string{"/select/jaeger/api/services", "/api/services", "/select/tempo/api/v2/search/tags"} {
		for _, withCred := range []bool{true, false} {
			st := &stallStore{}
			vtstorageadapter.Init(st)
			mux := http.NewServeMux()
			NewHandler(st, cfg).Register(mux)
			req := httptest.NewRequest("GET", p+"?timeout=100ms", nil)
			if withCred {
				req.Header.Set("X-Lakehouse-Global-Read", "s3cr3t")
			}
			mux.ServeHTTP(httptest.NewRecorder(), req)
			st.mu.Lock()
			got := append([]bool(nil), st.global...)
			st.mu.Unlock()
			if len(got) == 0 {
				t.Fatalf("%s: the store was not read", p)
			}
			for _, g := range got {
				if g != withCred {
					t.Fatalf("%s: credential=%v but the store's context has global read = %v", p, withCred, g)
				}
			}
		}
	}
}

// The flag value is used as upstream uses it: 0 makes every request fail at once
// with the deadline error (context.WithTimeout of a duration that has passed).
func TestJaegerTempoRoutes_ZeroFlagFailsEveryRequestAtOnce(t *testing.T) {
	for _, p := range []string{"/select/jaeger/api/services", "/select/tempo/api/v2/search/tags"} {
		st := &stallStore{}
		mux := newDeadlineMux(t, st, WithMaxQueryDuration(0))
		rec, took, _ := serve(mux, p, "", 5*time.Second)
		if took > time.Second || rec.Code == http.StatusOK || !strings.Contains(rec.Body.String(), "deadline exceeded") {
			t.Fatalf("%s with a 0 flag: %d %q after %s, want an immediate deadline error", p, rec.Code, rec.Body.String(), took)
		}
	}
}
