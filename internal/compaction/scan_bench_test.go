package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// BenchmarkScanSettled is the cost of a compaction scan over a settled
// manifest (nothing to merge): 2,000 hour partitions × 50 tenants, one 64 MiB
// L2 object per tenant-hour = 100,000 files. It reports ns, bytes and
// allocations per file per scan. It compiles against older planners too, so
// benchstat can compare branches (no object exists in the pool: a merge an
// older planner attempts fails at once and does not distort the timing much).
func BenchmarkScanSettled(b *testing.B) {
	const parts, tenants = 2000, 50
	m := manifest.New("test-bucket", "logs/")
	base := time.Now().Add(-60 * 24 * time.Hour)
	for p := 0; p < parts; p++ {
		ts := base.Add(time.Duration(p) * time.Hour).UTC()
		part := fmt.Sprintf("dt=%s/hour=%02d", ts.Format("2006-01-02"), ts.Hour())
		for tn := 0; tn < tenants; tn++ {
			m.AddFile(part, manifest.FileInfo{
				Key:  fmt.Sprintf("%d/0/logs/%s/compacted-L2-%d.parquet", 1000+tn, part, p),
				Size: 64 << 20, CompactionLevel: 2, SchemaFingerprint: "fp",
			})
		}
	}
	d := config.Default().Compaction
	p := NewLevelPolicy(d.MinFilesL0, d.MinFilesL1, d.MinAge)
	p.DailyRollupAge = d.DailyRollupAge
	s := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: newMockPool(), Ownership: soleOwnerResolver(),
		FairShare: NewFairShareScheduler(1), Policy: p,
		Mode: config.ModeLogs, Interval: d.Interval, MaxConcurrent: d.MaxConcurrent, RowGroupSize: 100,
		CurrentSchemaFingerprint: "fp", CompactionConfig: d,
	})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Scan(ctx)
	}
	b.StopTimer()
	files := float64(parts * tenants)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/files, "ns/file")
}
