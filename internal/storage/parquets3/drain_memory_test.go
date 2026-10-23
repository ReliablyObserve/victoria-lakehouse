package parquets3

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// The memory of a drain (#386). heapPeak samples the heap while a segment is
// drained into a store that keeps nothing; the benchmarks report it as peak-MiB
// next to the usual numbers, so that benchstat compares it too:
//
//	go test ./internal/storage/parquets3 -run '^$' -bench 'DrainHeap|PlanDistinctSeconds' -count=10
//
// The test asserts a ratio, never an absolute number: the heap of a segment whose
// rows share a second must stay close to the heap of the same rows spread over
// seconds.

// discardPool is an object store that keeps nothing but the sizes.
type discardPool struct {
	mu      sync.Mutex
	objects int
	largest int
}

func (d *discardPool) Upload(_ context.Context, _ string, data []byte) error {
	d.mu.Lock()
	d.objects++
	if len(data) > d.largest {
		d.largest = len(data)
	}
	d.mu.Unlock()
	return nil
}

// poolWriter is a logs BatchWriter whose uploads go to p.
func poolWriter(tb testing.TB, p PoolWriter) *BatchWriter {
	tb.Helper()
	s3srv := mockS3()
	tb.Cleanup(s3srv.Close)
	pool, err := s3reader.NewClientPool(context.Background(), &config.S3Config{Bucket: "test-bucket", Region: "us-east-1", Endpoint: s3srv.URL, ForcePathStyle: true, AccessKey: "test", SecretKey: "test"})
	if err != nil {
		tb.Fatal(err)
	}
	bw := NewBatchWriter(testInsertConfig(), pool, manifest.New("test-bucket", "logs/"), "logs/", config.ModeLogs)
	bw.SetTenantBucket(func(uint32, uint32) string { return "durability" })
	bw.SetTenantPool(func(string) PoolWriter { return p })
	return bw
}

// heapPeak samples the heap until the returned function is called, which returns
// the highest sample.
func heapPeak() (stop func() uint64) {
	var peak atomic.Uint64
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var ms runtime.MemStats
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapAlloc > peak.Load() {
					peak.Store(ms.HeapAlloc)
				}
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-finished
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		if ms.HeapAlloc > peak.Load() {
			peak.Store(ms.HeapAlloc)
		}
		return peak.Load()
	}
}

// liveHeap is the heap after a collection.
func liveHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// sealedBurst seals a segment of rows rows of one tenant with perSecond rows in
// each second of _time (the rows of a second 1 ns apart).
func sealedBurst(tb testing.TB, dir string, rows, perSecond int) (*membuffer.Segments, *membuffer.Segment) {
	tb.Helper()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(dir, "buffer")})
	if err != nil {
		tb.Fatal(err)
	}
	base := hourAgo.UnixNano()
	const batch = 10000
	for off := 0; off < rows; off += batch {
		lr := logstorage.GetLogRows(crowdedStreamFields, nil, nil, nil, "")
		for i := off; i < off+batch && i < rows; i++ {
			lr.MustAdd(segTenantA, base+int64(i/perSecond)*int64(time.Second)+int64(i%perSecond), crowdedFields(fmt.Sprintf("GET /api/v1/items/%d status=200 latency=%dms", i, i%97)), 1)
		}
		segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
	}
	g, ok := segs.Seal()
	if !ok {
		tb.Fatal("nothing sealed")
	}
	return segs, g
}

// drainHeap drains a sealed segment of rows rows (perSecond per second, groups
// of at most maxRows) and returns the heap growth in bytes and the objects
// written.
func drainHeap(tb testing.TB, rows, perSecond int, maxRows int64, gcPercent int) (growth uint64, objects int, took time.Duration) {
	tb.Helper()
	dir := tb.TempDir()
	segs, g := sealedBurst(tb, dir, rows, perSecond)
	defer segs.Close()
	d := &discardPool{}
	bw := poolWriter(tb, d)
	f := newBufferFlusher(bw, segs, filepath.Join(dir, "buffer"), nil, BufferFlusherConfig{TargetBytes: maxRows * estBytesPerLogRow, MaxAge: time.Hour, Grace: time.Minute})
	old := debug.SetGCPercent(gcPercent)
	defer debug.SetGCPercent(old)
	base := liveHeap()
	stop := heapPeak()
	start := time.Now()
	err := f.drain(context.Background(), g)
	took = time.Since(start)
	peak := stop()
	if err != nil {
		tb.Fatal(err)
	}
	if peak < base {
		return 0, d.objects, took
	}
	return peak - base, d.objects, took
}

func BenchmarkDrainHeap(b *testing.B) {
	const rows = 100000
	for _, c := range []struct {
		name      string
		perSecond int
	}{{"spread-1000-per-second", 1000}, {"one-second", rows}} {
		b.Run(c.name, func(b *testing.B) {
			var peak uint64
			var took time.Duration
			for i := 0; i < b.N; i++ {
				g, _, d := drainHeap(b, rows, c.perSecond, 25000, 25)
				if g > peak {
					peak = g
				}
				took += d
			}
			b.ReportMetric(float64(peak)/(1<<20), "peak-MiB")
			b.ReportMetric(float64(took.Milliseconds())/float64(b.N), "drain-ms")
		})
	}
}

// BenchmarkDrainTime is the wall time of a drain under the default GC settings
// (the heap benchmark runs a tight GC to read the peak, which costs CPU).
func BenchmarkDrainTime(b *testing.B) {
	const rows = 100000
	for _, c := range []struct {
		name      string
		perSecond int
	}{{"spread-1000-per-second", 1000}, {"one-second", rows}} {
		b.Run(c.name, func(b *testing.B) {
			var took time.Duration
			for i := 0; i < b.N; i++ {
				_, _, d := drainHeap(b, rows, c.perSecond, 25000, 100)
				took += d
			}
			b.ReportMetric(float64(took.Milliseconds())/float64(b.N), "drain-ms")
		})
	}
}

// BenchmarkPlanDistinctSeconds is the planning pass alone: rows rows, each in a
// second of its own, in groups of 262,144 rows.
func BenchmarkPlanDistinctSeconds(b *testing.B) {
	for _, rows := range []int{100000, 1000000} {
		b.Run(fmt.Sprintf("seconds=%d", rows), func(b *testing.B) {
			segs, g := sealedBurst(b, b.TempDir(), rows, 1)
			defer segs.Close()
			f := &BufferFlusher{maxRows: 262144}
			old := debug.SetGCPercent(25)
			defer debug.SetGCPercent(old)
			var peak uint64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				base := liveHeap()
				stop := heapPeak()
				if _, _, err := f.planSegment(context.Background(), g); err != nil {
					b.Fatal(err)
				}
				if p := stop(); p > base && p-base > peak {
					peak = p - base
				}
			}
			b.ReportMetric(float64(peak)/(1<<20), "peak-MiB")
		})
	}
}

// The heap of a drain does not grow with the rows that share a second: the same
// rows in one second peak close to the same rows spread over seconds. It is small
// on purpose: it also runs under -race in the heavy job, which has a time budget.
func TestDrainHeap_DoesNotGrowWithRowsPerSecond(t *testing.T) {
	const rows, maxRows = 30000, 5000
	spread, _, _ := drainHeap(t, rows, 1000, maxRows, 25)
	crowded, objects, _ := drainHeap(t, rows, rows, maxRows, 25)
	t.Logf("heap growth: spread %d MiB, one second %d MiB, %d objects (measured)", spread>>20, crowded>>20, objects)
	if objects < rows/maxRows {
		t.Errorf("%d objects for %d rows at %d per group", objects, rows, maxRows)
	}
	// A smoke check, not the guarantee (the cap on the objects, asserted above and
	// in drain_crowded_test.go, is): measured at this size, 0.5 to 1.5 over 18 runs
	// with and without -race, and 2.2 to 2.7 on main.
	if float64(crowded) > 3*float64(spread) {
		t.Errorf("the rows of one second peaked at %d MiB against %d MiB spread over seconds: more than three times", crowded>>20, spread>>20)
	}
}
