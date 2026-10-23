package parquets3

// A scaled model of the production read (128 MiB objects of 8-16 MiB row groups, a
// 512 MiB query.max_live_bytes, a 256 MiB file budget, 20 ms of S3 latency) at
// 1/64: objects of about 2 MiB of 150-200 KiB row groups, a ceiling of 4.5 objects,
// a file budget of 2.5 objects. It guards the throughput of the read path against
// a ceiling that holds workers back more than the memory it saves: compare with
// the base branch using benchstat (the file compiles against both).

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

const (
	scaledObjects  = 40
	scaledRows     = 14000 // about 2 MiB of incompressible rows per object
	scaledRowGroup = 1400  // about 200 KiB per row group, ten per object
	scaledLatency  = 20 * time.Millisecond
	scaledWorkers  = 8
)

type scaledFixture struct {
	mock     *mockS3Server
	latent   *httptest.Server
	keys     []string
	sizes    []int64
	objBytes int64
	lo, hi   int64
	now      time.Time
}

func newScaledFixture(b *testing.B) *scaledFixture {
	b.Helper()
	mock := newMockS3Server()
	b.Cleanup(mock.close)
	latent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(scaledLatency)
		mock.handler(w, r)
	}))
	b.Cleanup(latent.Close)
	fx := &scaledFixture{mock: mock, latent: latent, now: time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)}
	fx.lo, fx.hi = fx.now.Add(-time.Hour).UnixNano(), fx.now.Add(time.Hour).UnixNano()
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < scaledObjects; i++ {
		rows := make([]logRow, scaledRows)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: fx.now.Add(time.Duration(i*scaledRows+j) * time.Microsecond).UnixNano(),
				Body:              fmt.Sprintf("padding file=%d row=%d %016x%016x%016x%016x", i, j, rng.Uint64(), rng.Uint64(), rng.Uint64(), rng.Uint64()),
				SeverityText:      "INFO",
				ServiceName:       fmt.Sprintf("svc-%d", j%8),
			}
		}
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[logRow](&buf, parquet.Compression(&parquet.Zstd), parquet.MaxRowsPerRowGroup(scaledRowGroup))
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		key := fmt.Sprintf("logs/dt=2026-05-10/hour=14/scaled-%03d.parquet", i)
		mock.putFile(key, buf.Bytes())
		fx.keys = append(fx.keys, key)
		fx.sizes = append(fx.sizes, int64(buf.Len()))
		fx.objBytes = int64(buf.Len())
	}
	return fx
}

// storage is a fresh reader (cold caches) over the fixture.
func (fx *scaledFixture) storage(b *testing.B) *Storage {
	s := testStorageWithS3(b, fx.latent.URL)
	s.cfg.Query.FileWorkers = scaledWorkers
	s.cfg.Query.MaxLiveBytes = fx.objBytes * 9 / 2 // 4.5 objects
	for i, k := range fx.keys {
		s.manifest.AddFile("dt=2026-05-10/hour=14", manifest.FileInfo{Key: k, Size: fx.sizes[i], MinTimeNs: fx.now.Add(-time.Minute).UnixNano(), MaxTimeNs: fx.now.Add(time.Minute).UnixNano()})
	}
	return s
}

func scaledRun(b *testing.B, s *Storage, fx *scaledFixture, qs string) {
	b.Helper()
	q, err := logstorage.ParseQuery(qs)
	if err != nil {
		b.Fatal(err)
	}
	q.AddTimeFilter(fx.lo, fx.hi)
	var rows atomic.Int64
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) { rows.Add(int64(db.RowsCount())) }); err != nil {
		b.Fatal(err)
	}
	if rows.Load() == 0 {
		b.Fatal("no rows")
	}
}

// withScaledFileBudget sets the process-wide file budget to 2.5 objects.
func withScaledFileBudget(b *testing.B, objBytes int64) {
	old := fileBudgetSem
	fileBudgetSem = newFileBudget(objBytes*5/2, 8)
	b.Cleanup(func() { fileBudgetSem = old })
}

// BenchmarkScaledRead_ColdWildcard: a wildcard read on cold caches (footers and
// bodies fetched through 20 ms of latency).
func BenchmarkScaledRead_ColdWildcard(b *testing.B) {
	fx := newScaledFixture(b)
	withScaledFileBudget(b, fx.objBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := fx.storage(b)
		b.StartTimer()
		scaledRun(b, s, fx, "*")
	}
}

// BenchmarkScaledRead_WarmPhrase: a phrase read once the footers are cached.
func BenchmarkScaledRead_WarmPhrase(b *testing.B) {
	fx := newScaledFixture(b)
	withScaledFileBudget(b, fx.objBytes)
	s := fx.storage(b)
	scaledRun(b, s, fx, "padding")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scaledRun(b, s, fx, "padding")
	}
}
