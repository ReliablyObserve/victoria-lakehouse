package parquets3

// The Jaeger and Tempo query deadline (issue #778) over the real cold read path:
// a stalled S3 read of an object the query selected ends with the deadline error
// at the deadline, and the file-budget slot it held is free again — a normal query
// served right after succeeds, and no goroutine is left behind. A consumer that
// stalls while a worker holds a row-group decode slot is released the same way
// (store level: the consumer is the only thing that holds a decode slot while it
// waits, since the bytes of a row group are fetched before it is decoded).
//
// The deadline is VictoriaTraces' (vtselect.RequestHandler runs every Jaeger and
// Tempo request under -search.maxQueryDuration, lowered by 'timeout'); the traces
// binary applies the same one in selectapi (query_deadline.go).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

func qdMux(t *testing.T, fx *rcFixture) *http.ServeMux {
	t.Helper()
	vtstorageadapter.Init(fx.s)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	mux := http.NewServeMux()
	selectapi.NewHandler(fx.s, cfg).Register(mux)
	return mux
}

type qdRoute struct {
	group, path string
	args        url.Values
	// holdsBudget: the route opens a file under the file budget before its first
	// S3 read, so the budget slot is held for as long as the read stalls. The
	// others (trace-by-ID, services, operations) read through the trace index and
	// the stream catalog and take no file-budget slot.
	holdsBudget bool
}

func qdRoutes(base time.Time) []qdRoute {
	us := func(t time.Time) string { return fmt.Sprint(t.UnixMicro()) }
	sec := func(t time.Time) string { return fmt.Sprint(t.Unix()) }
	jaegerRange := url.Values{"start": {us(base.Add(-time.Hour))}, "end": {us(base.Add(time.Hour))}}
	tempoRange := url.Values{"start": {sec(base.Add(-time.Hour))}, "end": {sec(base.Add(time.Hour))}}
	with := func(base url.Values, kv ...string) url.Values {
		out := url.Values{}
		for k, v := range base {
			out[k] = v
		}
		for i := 0; i+1 < len(kv); i += 2 {
			out.Set(kv[i], kv[i+1])
		}
		return out
	}
	return []qdRoute{
		{"jaeger-trace-by-id", "/select/jaeger/api/traces/" + rcTraceID(1, 0), jaegerRange, false},
		{"jaeger-search", "/select/jaeger/api/traces", with(jaegerRange, "service", "svc-1", "limit", "5"), true},
		{"jaeger-services", "/select/jaeger/api/services", jaegerRange, false},
		{"jaeger-operations", "/select/jaeger/api/services/svc-1/operations", jaegerRange, false},
		{"tempo-search", "/select/tempo/api/search", with(tempoRange, "q", "{}", "limit", "5"), true},
		{"tempo-tags", "/select/tempo/api/v2/search/tags", tempoRange, true},
		{"tempo-tag-values", "/select/tempo/api/v2/search/tag/service.name/values", tempoRange, true},
		{"tempo-trace-by-id", "/select/tempo/api/v2/traces/" + rcTraceID(1, 0), tempoRange, false},
		{"traceql-metrics", "/select/tempo/api/metrics/query_range", with(tempoRange, "q", "{} | rate()", "step", "60s"), true},
	}
}

func qdGet(mux *http.ServeMux, rt qdRoute, timeout string, safety time.Duration) (code int, body string, took time.Duration) {
	args := url.Values{}
	for k, v := range rt.args {
		args[k] = v
	}
	if timeout != "" {
		args.Set("timeout", timeout)
	}
	req := httptest.NewRequest("GET", rt.path+"?"+args.Encode(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), safety)
	defer cancel()
	rec := httptest.NewRecorder()
	began := time.Now()
	mux.ServeHTTP(rec, req.WithContext(ctx))
	return rec.Code, rec.Body.String(), time.Since(began)
}

// qdFree waits for the file budget and the row-group decode slots to be free.
func qdFree(t *testing.T, fx *rcFixture) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		b, n := fileBudgetOutstanding()
		if b == 0 && n == 0 && len(rgDecodeSem) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slots still held after the deadline: file budget %d bytes / %d files, %d decode slots", b, n, len(rgDecodeSem))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// qdSampleMax samples the slot gauges every 2 ms until stop is called and
// returns the highest file-budget count and decode-slot count seen.
func qdSampleMax() (stop func() (budget, decode int)) {
	var maxB, maxD atomic.Int64
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for {
			_, n := fileBudgetOutstanding()
			if int64(n) > maxB.Load() {
				maxB.Store(int64(n))
			}
			if d := int64(len(rgDecodeSem)); d > maxD.Load() {
				maxD.Store(d)
			}
			select {
			case <-done:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	return func() (int, int) {
		close(done)
		<-finished
		return int(maxB.Load()), int(maxD.Load())
	}
}

func TestJaegerTempoDeadline_StalledObjectReadEndsAtTheDeadlineAndFreesSlots(t *testing.T) {
	const timeout = 250 * time.Millisecond
	// One worker (the sequential path) for the Jaeger search only: the deadline
	// cancels the same way for every worker count, and this package's CI shard
	// runs close to its time limit (#788), so each extra variant is paid for.
	for _, workers := range []int{1, 4} {
		for gi, g := range qdRoutes(time.Now()) {
			if workers == 1 && g.group != "jaeger-search" {
				continue
			}
			t.Run(fmt.Sprintf("%s/workers=%d", g.group, workers), func(t *testing.T) {
				fx := newRCFixture(t, workers, rcKeyFmt)
				mux := qdMux(t, fx)
				route := qdRoutes(fx.base)[gi]
				// The control and the follow-up query answer in full. For the Tempo tag
				// routes they are a Jaeger services query instead: VictoriaTraces
				// v0.12.0's singleFieldQueryHelper appends to a slice from the block
				// callback, which the uniq pipe calls from several goroutines, so a tag
				// query that returns rows is a data race inside upstream (reported by
				// -race; independent of the deadline; issue #316). A stalled tag query returns
				// no rows and is unaffected.
				probe := route
				if strings.HasPrefix(route.group, "tempo-tag") {
					probe = qdRoutes(fx.base)[2]
				}

				// control: no fault, the route answers
				code, body, _ := qdGet(mux, probe, "", 10*time.Second)
				if code != http.StatusOK {
					t.Fatalf("control: %d %.200q", code, body)
				}

				// stall the objects: every GET of a data object hangs until the client gives up
				var stalled atomic.Int64
				for _, k := range fx.keys {
					fx.fs.setFault(k, func(w http.ResponseWriter, r *http.Request) {
						stalled.Add(1)
						hang(w, r)
					})
				}
				// drop what the control read cached, so the stalled read has to go to S3
				fx.s.ClearCaches()

				goroutines := runtime.NumGoroutine()
				stopSampling := qdSampleMax()
				code, body, took := qdGet(mux, route, timeout.String(), 10*timeout)
				heldBudget, _ := stopSampling()
				if route.holdsBudget && heldBudget == 0 {
					t.Fatalf("the file budget was never held during the stall: the test does not show it being freed")
				}
				if stalled.Load() == 0 {
					t.Fatalf("the route never read an object (%d %.200q): it does not exercise the cold read", code, body)
				}
				if took > timeout+time.Second {
					t.Fatalf("answered after %s with a %s deadline (%d %.200q)", took, timeout, code, body)
				}
				if took < timeout-100*time.Millisecond {
					t.Fatalf("answered after %s, before the %s deadline (%d %.200q)", took, timeout, code, body)
				}
				if code == http.StatusOK && !strings.Contains(body, "deadline exceeded") {
					t.Fatalf("answered 200 %.200q for a read that never finished", body)
				}
				if !strings.Contains(body, "deadline exceeded") {
					t.Fatalf("%d %.200q does not carry the deadline error", code, body)
				}
				qdFree(t, fx)

				// the slots are usable again: objects readable, the query answers in full
				for _, k := range fx.keys {
					fx.fs.clearFault(k)
				}
				code, body, _ = qdGet(mux, probe, "", 10*time.Second)
				if code != http.StatusOK {
					t.Fatalf("a normal query after the stall: %d %.200q", code, body)
				}
				qdFree(t, fx)

				end := time.Now().Add(3 * time.Second)
				for runtime.NumGoroutine() > goroutines+2 && time.Now().Before(end) {
					time.Sleep(20 * time.Millisecond)
				}
				if got := runtime.NumGoroutine(); got > goroutines+2 {
					buf := make([]byte, 1<<16)
					t.Fatalf("goroutines: %d before the stalled request, %d after the next one\n%s", goroutines, got, buf[:runtime.Stack(buf, true)])
				}
			})
		}
	}
}

// A consumer that stalls while workers hold a row-group decode slot is released by
// the deadline: RunQuery returns the deadline error, and the decode slots and the
// file budget are free again. The slots are shown held while the consumer waits,
// so the zero afterwards is a release, not an idle gauge.
func TestJaegerTempoDeadline_StalledConsumerFreesDecodeSlots(t *testing.T) {
	const timeout = 250 * time.Millisecond
	fx := newRCFixture(t, 4, rcKeyFmt)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	stopSampling := qdSampleMax()
	began := time.Now()
	_, err := fx.query(t, ctx, "*", func(*logstorage.DataBlock) { <-ctx.Done() })
	took := time.Since(began)
	heldBudget, heldDecode := stopSampling()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunQuery = %v, want the deadline error", err)
	}
	if took > timeout+time.Second {
		t.Fatalf("RunQuery ended after %s with a %s deadline", took, timeout)
	}
	if heldBudget == 0 || heldDecode == 0 {
		t.Fatalf("while the consumer stalled the file budget held %d files and %d decode slots: the test does not show them freed", heldBudget, heldDecode)
	}
	qdFree(t, fx)

	rows, err := fx.query(t, context.Background(), "*", nil)
	if err != nil || rows != rcFiles*rcRowsPerFile {
		t.Fatalf("a normal query after the stall: %d rows, %v", rows, err)
	}
}
