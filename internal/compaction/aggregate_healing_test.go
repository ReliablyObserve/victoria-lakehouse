package compaction

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// compactLogFixtures uploads each row slice as one parquet file, registers it
// in the manifest with the given LabelAggregates (nil = the pre-#138-fix wiped
// state), runs Compact over the batch, and returns the single output FileInfo.
func compactLogFixtures(t *testing.T, rowSets [][]schema.LogRow, aggs []map[string]map[string]int64) manifest.FileInfo {
	t.Helper()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	const partition = "dt=2026-06-10/hour=10"
	const fp = "agg-fp"

	var infos []manifest.FileInfo
	for i, rows := range rowSets {
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, i)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          int64(len(rows)),
			SchemaFingerprint: fp,
			LabelAggregates:   aggs[i],
		}
		m.AddFile(partition, fi)
		infos = append(infos, fi)
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:         pool,
		Manifest:     m,
		Prefix:       "logs/",
		Mode:         config.ModeLogs,
		RowGroupSize: 1000,
	})
	if _, err := compactor.Compact(context.Background(), partition, infos, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	out := m.FilesForPartition(partition)
	if len(out) != 1 {
		t.Fatalf("expected 1 output file in manifest, got %d", len(out))
	}
	return out[0]
}

// TestCompactor_HealsWipedLabelAggregates is the regression guard for the
// compactor aggregate-healing fix: inputs whose FileInfo carries NO
// LabelAggregates (every file written before the #138 fix) must still produce
// an output FileInfo with correct per-(field,value) row counts, because the
// compactor extracts them from the merged ROWS it already holds — not from the
// (empty) input maps. Under the old mergeFileLabelAggregates(g.Files) path this
// output would carry nil aggregates forever, so `count() by (field)` fast-paths
// kept missing compacted data no matter how many compaction cycles ran.
func TestCompactor_HealsWipedLabelAggregates(t *testing.T) {
	rowSets := [][]schema.LogRow{
		{
			{TimestampUnixNano: 1000, Body: "a", ServiceName: "api-gateway", SeverityText: "INFO", DeployEnv: "prod"},
			{TimestampUnixNano: 2000, Body: "b", ServiceName: "api-gateway", SeverityText: "ERROR", DeployEnv: "prod"},
		},
		{
			{TimestampUnixNano: 3000, Body: "c", ServiceName: "user-service", SeverityText: "INFO", K8sNamespaceName: "default"},
		},
	}
	// Both inputs carry NO aggregates — the wiped pre-fix state.
	out := compactLogFixtures(t, rowSets, []map[string]map[string]int64{nil, nil})

	want := map[string]map[string]int64{
		"service.name":           {"api-gateway": 2, "user-service": 1},
		"severity_text":          {"INFO": 2, "ERROR": 1},
		"deployment.environment": {"prod": 2},
		"k8s.namespace.name":     {"default": 1},
	}
	if !reflect.DeepEqual(out.LabelAggregates, want) {
		t.Errorf("healed aggregates mismatch:\n got=%v\nwant=%v", out.LabelAggregates, want)
	}
}

// TestCompactor_HealsWipedTraceLabelAggregates is the traces-mode twin of the
// healing regression: the ModeTraces branch must extract from merged rows too.
func TestCompactor_HealsWipedTraceLabelAggregates(t *testing.T) {
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	const partition = "dt=2026-06-10/hour=11"
	const fp = "trace-agg-fp"

	rows := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "GET /users", ServiceName: "api-gateway"},
		{TimestampUnixNano: 2000, TraceID: "t2", SpanID: "s2", SpanName: "GET /users", ServiceName: "api-gateway"},
		{TimestampUnixNano: 3000, TraceID: "t3", SpanID: "s3", SpanName: "SELECT", ServiceName: "db"},
	}
	data := makeTestTraceParquet(t, rows)
	key := "traces/" + partition + "/batch-000.parquet"
	if err := pool.Upload(context.Background(), key, data); err != nil {
		t.Fatal(err)
	}
	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(data)),
		RowCount:          int64(len(rows)),
		SchemaFingerprint: fp,
		LabelAggregates:   nil, // wiped pre-fix state
	}
	m.AddFile(partition, fi)

	compactor := NewCompactor(CompactorConfig{
		Pool:         pool,
		Manifest:     m,
		Prefix:       "traces/",
		Mode:         config.ModeTraces,
		RowGroupSize: 1000,
	})
	if _, err := compactor.Compact(context.Background(), partition, []manifest.FileInfo{fi}, 0); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	out := m.FilesForPartition(partition)
	if len(out) != 1 {
		t.Fatalf("expected 1 output file, got %d", len(out))
	}
	want := map[string]map[string]int64{
		"service.name": {"api-gateway": 2, "db": 1},
		"span.name":    {"GET /users": 2, "SELECT": 1},
	}
	if !reflect.DeepEqual(out[0].LabelAggregates, want) {
		t.Errorf("healed trace aggregates mismatch:\n got=%v\nwant=%v", out[0].LabelAggregates, want)
	}
}

// TestCompactor_RowExtractionEquivalentToInputMerge pins the equivalence
// invariant: when the inputs DO carry correct aggregates (post-fix files),
// extraction-from-rows must equal the old merge-of-input-maps for the same
// data — row extraction is a strict generalization, not a behavior change.
func TestCompactor_RowExtractionEquivalentToInputMerge(t *testing.T) {
	rowSets := [][]schema.LogRow{
		{
			{TimestampUnixNano: 1000, Body: "a", ServiceName: "api-gateway", SeverityText: "INFO", CloudRegion: "eu-west-1"},
			{TimestampUnixNano: 2000, Body: "b", ServiceName: "user-service", SeverityText: "WARN", CloudRegion: "eu-west-1"},
		},
		{
			{TimestampUnixNano: 3000, Body: "c", ServiceName: "api-gateway", SeverityText: "INFO", CloudRegion: "us-east-1"},
			{TimestampUnixNano: 4000, Body: "d", ServiceName: "order-service", SeverityText: "ERROR"},
		},
	}
	// Inputs carry the aggregates the flush writer would have produced.
	aggs := []map[string]map[string]int64{
		schema.ExtractLogLabelAggregates(rowSets[0]),
		schema.ExtractLogLabelAggregates(rowSets[1]),
	}
	wantMerged := mergeFileLabelAggregates([]manifest.FileInfo{
		{LabelAggregates: aggs[0]},
		{LabelAggregates: aggs[1]},
	})

	out := compactLogFixtures(t, rowSets, aggs)
	if !reflect.DeepEqual(out.LabelAggregates, wantMerged) {
		t.Errorf("row extraction != input-map merge for identical data:\n got=%v\nwant=%v", out.LabelAggregates, wantMerged)
	}
}

// TestCompactor_AggregateCapMatchesFlush pins the absent-value contract: a
// field whose merged distinct-value count exceeds the per-field cap is DROPPED
// from the compacted output's aggregates — exactly like the flush writer drops
// it (shared schema.MaxLabelAggregateValues) — while low-cardinality fields
// survive. Asserts absence, not just presence.
func TestCompactor_AggregateCapMatchesFlush(t *testing.T) {
	var rows []schema.LogRow
	for i := 0; i <= schema.MaxLabelAggregateValues; i++ { // cap+1 distinct namespaces
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: int64(1000 + i),
			Body:              "x",
			ServiceName:       "api-gateway",
			K8sNamespaceName:  fmt.Sprintf("ns-%d", i),
		})
	}

	out := compactLogFixtures(t, [][]schema.LogRow{rows}, []map[string]map[string]int64{nil})

	if _, present := out.LabelAggregates["k8s.namespace.name"]; present {
		t.Error("field exceeding the per-field value cap must be ABSENT from compacted aggregates")
	}
	if got := out.LabelAggregates["service.name"]["api-gateway"]; got != int64(len(rows)) {
		t.Errorf("service.name count = %d, want %d", got, len(rows))
	}

	// Exactly like flush: the compacted output equals what the flush writer
	// would have extracted from the same rows.
	if want := schema.ExtractLogLabelAggregates(rows); !reflect.DeepEqual(out.LabelAggregates, want) {
		t.Errorf("compacted aggregates diverge from flush extraction:\n got=%v\nwant=%v", out.LabelAggregates, want)
	}
}
