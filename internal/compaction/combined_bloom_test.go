package compaction

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestCombinedBloomOnCompaction_E2E proves the compactor extracts the COMBINED
// bloom — the union of every merged input's bloom-column values — from the merged
// rows and surfaces it on CompactResult.OutputBlooms (and through onCompacted),
// keyed by the output file. That is the data the embedder feeds into the pmeta bloom
// facet so a compacted file stays file-level bloom-prunable across all its inputs.
func TestCombinedBloomOnCompaction_E2E(t *testing.T) {
	const partition = "dt=2026-06-07/hour=00"
	m := manifest.New("test", "logs/")
	pool := newMockPool()

	// Two real L2 parquet files with DISJOINT trace_ids + service names.
	fileA := []schema.LogRow{
		{TimestampUnixNano: 1_000_000_000, Body: "a1", TraceID: "trace-A1", ServiceName: "svc-a"},
		{TimestampUnixNano: 2_000_000_000, Body: "a2", TraceID: "trace-A2", ServiceName: "svc-a"},
	}
	fileB := []schema.LogRow{
		{TimestampUnixNano: 3_000_000_000, Body: "b1", TraceID: "trace-B1", ServiceName: "svc-b"},
	}
	for i, rows := range [][]schema.LogRow{fileA, fileB} {
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("100/1/logs/%s/L2-%02d.parquet", partition, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatalf("upload: %v", err)
		}
		m.AddFile(partition, manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          int64(len(rows)),
			MinTimeNs:         rows[0].TimestampUnixNano,
			MaxTimeNs:         rows[len(rows)-1].TimestampUnixNano,
			CompactionLevel:   2,
			SchemaFingerprint: "v1",
		})
	}

	var gotBlooms map[string]map[string][]string
	sched := NewScheduler(SchedulerConfig{
		Manifest:                 m,
		Pool:                     pool,
		Ownership:                NewOwnershipResolver("self", staticPeers("self")),
		Policy:                   NewLevelPolicy(10, 10, 0),
		Prefix:                   "logs/",
		Mode:                     config.ModeLogs,
		Interval:                 time.Minute,
		RowGroupSize:             1000,
		CompressionLevel:         1,
		MaxConcurrent:            4,
		CurrentSchemaFingerprint: "v2",
		OnCompacted: func(_ []manifest.FileInfo, _ []string, blooms map[string]map[string][]string) {
			gotBlooms = blooms
		},
	})

	result, err := sched.ForceCompactPartition(context.Background(), partition, 0)
	if err != nil {
		t.Fatalf("ForceCompactPartition: %v", err)
	}

	outKey := result.OutputFile
	// The result carries the combined bloom for the merged output...
	bl := result.OutputBlooms[outKey]
	if bl == nil {
		t.Fatalf("no combined bloom on result for output %q (have %v)", outKey, keysOf(result.OutputBlooms))
	}
	assertBloomSet(t, "trace_id", bl["trace_id"], []string{"trace-A1", "trace-A2", "trace-B1"})
	assertBloomSet(t, "service.name", bl["service.name"], []string{"svc-a", "svc-b"})

	// ...and onCompacted received the identical map (the pmeta feed path).
	if gotBlooms == nil || gotBlooms[outKey] == nil {
		t.Fatalf("onCompacted did not receive the combined bloom; got %v", gotBlooms)
	}
	assertBloomSet(t, "trace_id (onCompacted)", gotBlooms[outKey]["trace_id"], []string{"trace-A1", "trace-A2", "trace-B1"})

	// The compacted output's footer-bloom footprint was captured into the manifest
	// (this is the /stats/compaction bloom_bytes source).
	out := m.FilesForPartition(partition)
	if len(out) != 1 || out[0].BloomBytes <= 0 {
		t.Errorf("compacted output BloomBytes = %v, want > 0 (footer blooms captured)", out)
	}
}

func keysOf(m map[string]map[string][]string) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func assertBloomSet(t *testing.T, label string, got, want []string) {
	t.Helper()
	g := append([]string(nil), got...)
	w := append([]string(nil), want...)
	sort.Strings(g)
	sort.Strings(w)
	if len(g) != len(w) {
		t.Errorf("%s: got %v, want %v", label, g, w)
		return
	}
	for i := range w {
		if g[i] != w[i] {
			t.Errorf("%s: got %v, want %v", label, g, w)
			return
		}
	}
}
