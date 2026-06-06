package buffer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// concurrentLogStore is a minimal Querier whose internal slice is guarded
// by a mutex. It models the BatchWriter contract (BufferedLogRows takes a
// snapshot under lock) so the race detector can flag any handler-side
// aliasing of the underlying buffer.
type concurrentLogStore struct {
	mu      sync.RWMutex
	rows    []schema.LogRow
	written atomic.Int64 // count of rows ever appended
}

func (c *concurrentLogStore) Append(r schema.LogRow) {
	c.mu.Lock()
	c.rows = append(c.rows, r)
	c.mu.Unlock()
	c.written.Add(1)
}

func (c *concurrentLogStore) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Return a *copy* of the matching slice. Sharing the underlying array
	// across goroutines would be a real bug; this test exists in part to
	// guard against regressing to that.
	var out []schema.LogRow
	for _, r := range c.rows {
		if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
			out = append(out, r)
		}
	}
	return out
}

func (c *concurrentLogStore) BufferedTraceRows(_, _ int64) []schema.TraceRow {
	return nil
}

// TestStress_ConcurrentReadsDuringWrites verifies the handler is race-clean
// under sustained producer/consumer load and never returns rows that were
// not written. The test must under-count at worst (eventually-consistent),
// never over-count.
func TestStress_ConcurrentReadsDuringWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test under -short")
	}

	store := &concurrentLogStore{}
	h := NewHandler(store, "")

	done := make(chan struct{})
	duration := 5 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline) - 2*time.Second; remaining < duration {
			duration = remaining
		}
	}
	if duration <= 0 {
		duration = 500 * time.Millisecond
	}

	// Writer: continuously append rows with monotonically increasing ts.
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		var ts int64 = 1
		for {
			select {
			case <-done:
				return
			default:
			}
			store.Append(schema.LogRow{
				TimestampUnixNano: ts,
				Body:              "row",
				ServiceName:       "svc",
			})
			ts++
		}
	}()

	// Readers: hammer the handler with a maximal time-range query, count
	// the NDJSON lines returned, and check none of the rows reports a
	// timestamp beyond writer.written at the time of decode.
	const readers = 32
	var readerWG sync.WaitGroup
	var totalRead atomic.Int64
	var maxObservedTS atomic.Int64
	startStr := strconv.FormatInt(0, 10)
	endStr := strconv.FormatInt(int64(1)<<62, 10)
	urlPath := fmt.Sprintf("/internal/buffer/query?start=%s&end=%s&mode=logs", startStr, endStr)

	for i := 0; i < readers; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				req := httptest.NewRequest(http.MethodGet, urlPath, nil)
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("unexpected status %d", rec.Code)
					return
				}
				body := rec.Body.Bytes()
				dec := json.NewDecoder(bytes.NewReader(body))
				var n int64
				for dec.More() {
					var row schema.LogRow
					if err := dec.Decode(&row); err != nil {
						break
					}
					n++
					if row.TimestampUnixNano > maxObservedTS.Load() {
						maxObservedTS.Store(row.TimestampUnixNano)
					}
				}
				totalRead.Add(n)
			}
		}()
	}

	time.Sleep(duration)
	close(done)
	readerWG.Wait()
	writerWG.Wait()

	written := store.written.Load()
	maxTS := maxObservedTS.Load()

	// Over-count guard: max timestamp observed by a reader must never
	// exceed the writer's final count. Writer assigns ts = N for the
	// N-th row, so maxTS <= written at all times.
	if maxTS > written {
		t.Errorf("observed timestamp %d > rows written %d — buffer returned unwritten data", maxTS, written)
	}

	// Sanity: we did some work.
	if totalRead.Load() == 0 {
		t.Error("no rows read across any reader — test did not exercise the handler")
	}
	if written == 0 {
		t.Error("writer made no progress")
	}
}
