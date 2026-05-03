package manifest

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestManifest_Race_MaxGoroutines(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 10; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 1024})
		}
	}

	const goroutines = 50
	const ops = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	startNs := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC).UnixNano()

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(id)))
			for i := 0; i < ops; i++ {
				switch rng.Intn(7) {
				case 0:
					partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", rng.Intn(28)+1, rng.Intn(24))
					m.AddFile(partition, FileInfo{
						Key:  fmt.Sprintf("%s/f-%d-%d.parquet", partition, id, i),
						Size: int64(rng.Intn(10000)),
					})
				case 1:
					m.HasDataForRange(startNs, endNs)
				case 2:
					_ = m.GetFilesForRange(startNs, endNs)
				case 3:
					_ = m.TotalFiles()
				case 4:
					_ = m.TotalBytes()
				case 5:
					_ = m.PartitionCount()
				case 6:
					_ = m.MinTime()
					_ = m.MaxTime()
				}
				if i%100 == 0 {
					runtime.Gosched()
				}
			}
		}(g)
	}
	wg.Wait()

	if m.TotalFiles() < 0 {
		t.Errorf("negative total files: %d", m.TotalFiles())
	}
}

func TestManifest_Race_AddFileInvariant(t *testing.T) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	const goroutines = 100
	const ops = 1000
	var wg sync.WaitGroup
	wg.Add(goroutines + 1)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				partition := fmt.Sprintf("dt=2026-05-%02d/hour=%02d", (i%28)+1, i%24)
				m.AddFile(partition, FileInfo{
					Key:  fmt.Sprintf("%s/f-%d-%d.parquet", partition, id, i),
					Size: 100,
				})
			}
		}(g)
	}

	go func() {
		defer wg.Done()
		for i := 0; i < ops*10; i++ {
			tf := m.TotalFiles()
			tb := m.TotalBytes()
			if tf < 0 {
				t.Errorf("negative total files: %d", tf)
			}
			if tb < 0 {
				t.Errorf("negative total bytes: %d", tb)
			}
			runtime.Gosched()
		}
	}()

	wg.Wait()
}

func BenchmarkManifest_ConcurrentReads(b *testing.B) {
	l := testLogger()
	m := New("bucket", "logs/", l)

	for d := 0; d < 30; d++ {
		for h := 0; h < 24; h++ {
			date := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d)
			partition := fmt.Sprintf("dt=%s/hour=%02d", date.Format("2006-01-02"), h)
			m.AddFile(partition, FileInfo{Key: fmt.Sprintf("%s/f.parquet", partition), Size: 1024})
		}
	}

	startNs := time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC).UnixNano()
	endNs := time.Date(2026, 5, 15, 11, 0, 0, 0, time.UTC).UnixNano()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.HasDataForRange(startNs, endNs)
		}
	})
}
