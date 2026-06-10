package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Trap 1+2 regression tests (parquet-compression-research.md, "The three
// correctness traps under item 1"): manifest FileInfo MinTimeNs/MaxTimeNs must
// be the TRUE min/max of the flushed rows, not the first/last row's
// timestamps. The tests call the tenant-group flush directly (below the
// partition-level time sort) with deliberately shuffled timestamps — exactly
// what the flush sees once rows are ordered (stream_id, timestamp) for
// compression. With positional bounds the manifest would understate MaxTimeNs
// → range pruning skips files containing matches AND bufferWatermark re-opens
// the buffer↔Parquet double-count.
//
// Mirror of lakehouse-traces/internal/storage/parquets3/writer_timebounds_test.go.

// shuffledOffsetsSec places the true MIN and MAX in middle positions so the
// positional derivation (rows[0]/rows[len-1]) yields provably wrong values.
var shuffledOffsetsSec = []int64{30, 90, 10, 60, 40} // true min=+10s (idx 2), true max=+90s (idx 1)

func TestFlushLogTenantGroup_ShuffledRows_ManifestHoldsTrueBounds(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	bw, m := testWriter(t, s3srv.URL)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := make([]schema.LogRow, len(shuffledOffsetsSec))
	for i, off := range shuffledOffsetsSec {
		rows[i] = schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(off) * time.Second).UnixNano(),
			Body:              fmt.Sprintf("msg-%d", i),
			SeverityText:      "INFO",
			ServiceName:       "test-svc",
		}
	}
	wantMin := base.Add(10 * time.Second).UnixNano()
	wantMax := base.Add(90 * time.Second).UnixNano()

	if err := bw.flushLogTenantGroup(context.Background(), "dt=2026-05-03/hour=14", 0, 0, rows); err != nil {
		t.Fatalf("flushLogTenantGroup: %v", err)
	}

	files := m.GetFilesForRange(base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("manifest files = %d, want 1", len(files))
	}
	fi := files[0]
	if fi.MinTimeNs != wantMin || fi.MaxTimeNs != wantMax {
		t.Errorf("manifest bounds = (%d, %d), want true bounds (%d, %d)",
			fi.MinTimeNs, fi.MaxTimeNs, wantMin, wantMax)
	}
	// Absent-value guards: the old positional derivation would have stored
	// rows[0]/rows[len-1] — assert the fixture keeps those distinct AND that
	// the manifest did NOT store them.
	first, last := rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	if first == wantMin || last == wantMax {
		t.Fatal("fixture regressed: true bounds must not sit at first/last positions")
	}
	if fi.MinTimeNs == first {
		t.Errorf("MinTimeNs %d equals rows[0] timestamp — positional derivation regressed", fi.MinTimeNs)
	}
	if fi.MaxTimeNs == last {
		t.Errorf("MaxTimeNs %d equals rows[len-1] timestamp — positional derivation regressed", fi.MaxTimeNs)
	}

	// Trap 2: bufferWatermark is max(MaxTimeNs) of the scanned files — with
	// positional bounds it would sit at the LAST row's timestamp (+40s),
	// re-opening the 2× buffer↔Parquet double-count for rows in (+40s, +90s].
	tenants := []logstorage.TenantID{{AccountID: 0, ProjectID: 0}}
	if wm := bufferWatermark(files, tenants); wm != wantMax {
		t.Errorf("bufferWatermark = %d, want true max %d (positional bounds would give %d)",
			wm, wantMax, last)
	}
}

func TestFlushTraceTenantGroup_ShuffledRows_ManifestHoldsTrueBounds(t *testing.T) {
	s3srv := mockS3()
	defer s3srv.Close()
	pool := testPool(t, s3srv.URL)
	m := manifest.New("test-bucket", "traces/")
	bw := NewBatchWriter(testInsertConfig(), pool, m, "traces/", config.ModeTraces)

	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	rows := make([]schema.TraceRow, len(shuffledOffsetsSec))
	for i, off := range shuffledOffsetsSec {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: base.Add(time.Duration(off) * time.Second).UnixNano(),
			TraceID:           fmt.Sprintf("trace-%d", i),
			SpanID:            fmt.Sprintf("span-%d", i),
			SpanName:          "test-span",
			ServiceName:       "test-svc",
			DurationNs:        int64(i+1) * 1000,
		}
	}
	wantMin := base.Add(10 * time.Second).UnixNano()
	wantMax := base.Add(90 * time.Second).UnixNano()

	if err := bw.flushTraceTenantGroup(context.Background(), "dt=2026-05-03/hour=14", 0, 0, rows); err != nil {
		t.Fatalf("flushTraceTenantGroup: %v", err)
	}

	files := m.GetFilesForRange(base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	if len(files) != 1 {
		t.Fatalf("manifest files = %d, want 1", len(files))
	}
	fi := files[0]
	if fi.MinTimeNs != wantMin || fi.MaxTimeNs != wantMax {
		t.Errorf("manifest bounds = (%d, %d), want true bounds (%d, %d)",
			fi.MinTimeNs, fi.MaxTimeNs, wantMin, wantMax)
	}
	first, last := rows[0].TimestampUnixNano, rows[len(rows)-1].TimestampUnixNano
	if fi.MinTimeNs == first {
		t.Errorf("MinTimeNs %d equals rows[0] timestamp — positional derivation regressed", fi.MinTimeNs)
	}
	if fi.MaxTimeNs == last {
		t.Errorf("MaxTimeNs %d equals rows[len-1] timestamp — positional derivation regressed", fi.MaxTimeNs)
	}
}
