// Tests for the per-output-level row-group size schedule (compression
// step 4): the compactor must size row groups from
// Compaction.RowGroupSizeByOutputLevel — keeping the historical size
// for L0/L1 outputs and doubling for L2+ rollups under the default
// schedule — and must fall back to the static CompactorConfig
// RowGroupSize when the schedule is absent (the absent-value
// contract).

package compaction

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// compactSyntheticLogs builds a 2N-row corpus (two N-row input files),
// compacts it at the given source level with the given compaction
// config, and returns the output parquet bytes.
func compactSyntheticLogs(t *testing.T, n int, sourceLevel int, ccfg config.CompactionConfig, staticRowGroupSize int) []byte {
	t.Helper()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	partition := "dt=2026-06-10/hour=10"
	fp := "rg-fp"

	var infos []manifest.FileInfo
	for f := 0; f < 2; f++ {
		rows := make([]schema.LogRow, n)
		for i := range rows {
			ts := int64((f*n + i + 1) * 1000)
			rows[i] = schema.LogRow{
				TimestampUnixNano: ts,
				Body:              fmt.Sprintf("body-%d-%d", f, i),
				ServiceName:       "svc-a",
			}
		}
		data := makeTestParquet(t, rows)
		key := fmt.Sprintf("logs/%s/batch-%03d.parquet", partition, f)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          int64(n),
			MinTimeNs:         int64((f*n + 1) * 1000),
			MaxTimeNs:         int64((f + 1) * n * 1000),
			SchemaFingerprint: fp,
			CompactionLevel:   sourceLevel,
		}
		m.AddFile(partition, fi)
		infos = append(infos, fi)
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		RowGroupSize:     staticRowGroupSize,
		CompressionLevel: 3,
		CompactionConfig: ccfg,
	})
	result, err := compactor.Compact(context.Background(), partition, infos, sourceLevel)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}

	pool.mu.Lock()
	out, ok := pool.uploaded[result.OutputFile]
	pool.mu.Unlock()
	if !ok {
		t.Fatal("output file not found in pool")
	}
	return out
}

func countRowGroups(t *testing.T, data []byte) int {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	return len(f.RowGroups())
}

// TestCompactor_RowGroupSchedule_L2Halves pins the headline behavior:
// on the same 2N-row corpus, an L1→L2 compaction under the default-
// shaped schedule [N, N, 2N] produces HALF the row groups of an L0→L1
// compaction (1 group of 2N rows vs 2 groups of N rows).
func TestCompactor_RowGroupSchedule_L2Halves(t *testing.T) {
	const n = 1000
	ccfg := config.CompactionConfig{RowGroupSizeByOutputLevel: []int{n, n, 2 * n}}

	outL1 := compactSyntheticLogs(t, n, 0, ccfg, n) // output level 1 → slot 1 = n
	outL2 := compactSyntheticLogs(t, n, 1, ccfg, n) // output level 2 → slot 2 = 2n

	gotL1 := countRowGroups(t, outL1)
	gotL2 := countRowGroups(t, outL2)
	if gotL1 != 2 {
		t.Errorf("L1 output row groups = %d, want 2 (2N rows at N rows/group)", gotL1)
	}
	if gotL2 != 1 {
		t.Errorf("L2 output row groups = %d, want 1 (2N rows at 2N rows/group)", gotL2)
	}
	if gotL2 != gotL1/2 {
		t.Errorf("L2 row-group count (%d) must be half of L1 (%d)", gotL2, gotL1)
	}

	// Same rows in both outputs — only the row-group layout differs.
	rowsL1, err := readLogRows(outL1)
	if err != nil {
		t.Fatalf("readLogRows(L1): %v", err)
	}
	rowsL2, err := readLogRows(outL2)
	if err != nil {
		t.Fatalf("readLogRows(L2): %v", err)
	}
	if len(rowsL1) != 2*n || len(rowsL2) != 2*n {
		t.Errorf("row counts = %d / %d, want %d in both", len(rowsL1), len(rowsL2), 2*n)
	}
}

// TestCompactor_RowGroupSchedule_AbsentFallsBackToStatic pins the
// absent-value contract: with NO schedule configured, the compactor
// keeps the static CompactorConfig.RowGroupSize for every output
// level — pre-schedule deployments see no layout change.
func TestCompactor_RowGroupSchedule_AbsentFallsBackToStatic(t *testing.T) {
	const n = 1000
	// Empty CompactionConfig — RowGroupSizeForOutput returns 0,
	// compactor must use the static n rows/group even at L2.
	outL2 := compactSyntheticLogs(t, n, 1, config.CompactionConfig{}, n)
	if got := countRowGroups(t, outL2); got != 2 {
		t.Errorf("L2 output row groups = %d, want 2 (static fallback, no schedule)", got)
	}
}

// TestCompactor_RowGroupSchedule_SaturatesBeyondLastSlot: deeper
// rollups than the schedule covers keep the last slot's size instead
// of panicking or falling back.
func TestCompactor_RowGroupSchedule_SaturatesBeyondLastSlot(t *testing.T) {
	const n = 1000
	ccfg := config.CompactionConfig{RowGroupSizeByOutputLevel: []int{n, n, 2 * n}}
	// sourceLevel 4 → outputLevel 5, beyond the 3-slot schedule →
	// saturates to slot 2 (2N rows/group) → single row group.
	outL5 := compactSyntheticLogs(t, n, 4, ccfg, n)
	if got := countRowGroups(t, outL5); got != 1 {
		t.Errorf("L5 output row groups = %d, want 1 (saturated to last slot 2N)", got)
	}
}

// TestCompactor_RowGroupSchedule_TracesL2Halves: the traces write path
// (writeCompactedTraces) honors the same schedule — both modules share
// this compactor, so the twins stay in sync by construction; this pins
// the trace-row branch explicitly.
func TestCompactor_RowGroupSchedule_TracesL2Halves(t *testing.T) {
	const n = 500
	pool := newMockPool()
	m := manifest.New("test-bucket", "traces/")
	partition := "dt=2026-06-10/hour=11"
	fp := "rg-trace-fp"

	var infos []manifest.FileInfo
	for f := 0; f < 2; f++ {
		rows := make([]schema.TraceRow, n)
		for i := range rows {
			ts := int64((f*n + i + 1) * 1000)
			rows[i] = schema.TraceRow{
				TimestampUnixNano: ts,
				StartTimeUnixNano: ts,
				TraceID:           fmt.Sprintf("trace-%d-%d", f, i),
				SpanID:            fmt.Sprintf("span-%d-%d", f, i),
				SpanName:          "op",
				ServiceName:       "svc-t",
			}
		}
		data := makeTestTraceParquet(t, rows)
		key := fmt.Sprintf("traces/%s/batch-%03d.parquet", partition, f)
		if err := pool.Upload(context.Background(), key, data); err != nil {
			t.Fatal(err)
		}
		fi := manifest.FileInfo{
			Key:               key,
			Size:              int64(len(data)),
			RowCount:          int64(n),
			MinTimeNs:         int64((f*n + 1) * 1000),
			MaxTimeNs:         int64((f + 1) * n * 1000),
			SchemaFingerprint: fp,
			CompactionLevel:   1,
		}
		m.AddFile(partition, fi)
		infos = append(infos, fi)
	}

	compactor := NewCompactor(CompactorConfig{
		Pool:             pool,
		Manifest:         m,
		Prefix:           "traces/",
		Mode:             config.ModeTraces,
		RowGroupSize:     n,
		CompressionLevel: 3,
		CompactionConfig: config.CompactionConfig{RowGroupSizeByOutputLevel: []int{n, n, 2 * n}},
	})
	result, err := compactor.Compact(context.Background(), partition, infos, 1)
	if err != nil {
		t.Fatalf("Compact error: %v", err)
	}

	pool.mu.Lock()
	out, ok := pool.uploaded[result.OutputFile]
	pool.mu.Unlock()
	if !ok {
		t.Fatal("output file not found in pool")
	}
	if got := countRowGroups(t, out); got != 1 {
		t.Errorf("traces L2 output row groups = %d, want 1 (2N rows at 2N rows/group)", got)
	}
}
