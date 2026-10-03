package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// BenchmarkScanSettledWithFreeze is BenchmarkScanSettled with the lifecycle
// freeze wired the way production wires it: global rules, one per-tenant
// override, extra rules, and the listed class on every object.
func BenchmarkScanSettledWithFreeze(b *testing.B) {
	const parts, tenants = 2000, 50
	m := manifest.New("test-bucket", "logs/")
	base := time.Now().Add(-60 * 24 * time.Hour)
	for p := 0; p < parts; p++ {
		ts := base.Add(time.Duration(p) * time.Hour).UTC()
		part := fmt.Sprintf("dt=%s/hour=%02d", ts.Format("2006-01-02"), ts.Hour())
		for tn := 0; tn < tenants; tn++ {
			m.AddFile(part, manifest.FileInfo{
				Key:  fmt.Sprintf("%d/0/logs/%s/compacted-L2-%d.parquet", 1000+tn, part, p),
				Size: 64 << 20, CompactionLevel: 2, SchemaFingerprint: "fp", StorageClass: "STANDARD",
			})
		}
	}
	d := config.Default().Compaction
	det := delete.NewStorageClassDetector([]delete.LifecycleRule{{TransitionDays: 90, Class: delete.ClassGlacier}})
	det.SetTenantRules(map[uint32]map[uint32][]delete.LifecycleRule{1001: {0: {{TransitionDays: 30, Class: delete.ClassStandardIA}}}})
	s := NewScheduler(SchedulerConfig{
		Manifest: m, Pool: newMockPool(), Ownership: soleOwnerResolver(),
		FairShare: NewFairShareScheduler(1), Policy: shippedPolicy(),
		Mode: config.ModeLogs, Interval: d.Interval, MaxConcurrent: d.MaxConcurrent, RowGroupSize: 100,
		CurrentSchemaFingerprint: "fp", CompactionConfig: d,
		Freeze: &LifecycleFreeze{Detector: det, ExtraRules: []delete.LifecycleRule{{TransitionDays: 120, Class: delete.ClassDeepArchive}}},
	})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n, err := s.Scan(ctx); err != nil || n != 0 {
			b.Fatalf("settled scan merged: n=%d err=%v", n, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(parts*tenants), "ns/file")
}
