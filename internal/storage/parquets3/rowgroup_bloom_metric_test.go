package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// TestRowGroupBloomSkip_RecordsEverySkippedRowGroup pins the row-group bloom
// skip and its metric on a controlled object: a full-row read of a
// multi-row-group object whose row groups carry Parquet bloom filters opens
// only the row group that can hold the queried trace_id, and records every
// other row group as lakehouse_parquet_row_groups_skipped_total{reason="bloom"}.
//
// The e2e settings check can no longer infer this from a live stack: a request
// without tenant headers reads only tenant 0:0, and manifest column statistics
// prune 0:0's objects for an absent trace_id before any row group is opened —
// the `reason="bloom"` series it looked for came from other tenants' objects,
// which an unscoped read used to include.
func TestRowGroupBloomSkip_RecordsEverySkippedRowGroup(t *testing.T) {
	const known = "deadbeefcafe00010203040506070809"

	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGBloomLogParquet(t, baseTime, known, 200*1024, 600)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rowGroups := len(f.RowGroups())
	if rowGroups < 3 {
		t.Fatalf("fixture has %d row groups; the skip needs several", rowGroups)
	}
	if tidx := findColumnIndex(f.Root(), "trace_id"); tidx < 0 || f.RowGroups()[1].ColumnChunks()[tidx].BloomFilter() == nil {
		t.Fatal("fixture row groups carry no trace_id bloom filter")
	}

	key := "logs/dt=2026-06-01/hour=10/rg-bloom.parquet"
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       key,
		Size:      int64(len(data)),
		MinTimeNs: baseTime.Add(-time.Minute).UnixNano(),
		MaxTimeNs: baseTime.Add(time.Hour).UnixNano(),
	})

	start := baseTime.Add(-time.Hour).UnixNano()
	end := baseTime.Add(2 * time.Hour).UnixNano()
	before := metrics.ParquetRowGroupsSkipped.Get("bloom")

	q := mustParseQueryWithTime(t, fmt.Sprintf(`trace_id:=%q`, known), start, end)
	var rows int
	var mu sync.Mutex
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		rows += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if rows != 1 {
		t.Fatalf("full-row read returned %d rows, want the 1 row holding the trace_id", rows)
	}
	// Other tests in the package may skip row groups concurrently, so require
	// at least this object's skips rather than an exact delta.
	if delta := metrics.ParquetRowGroupsSkipped.Get("bloom") - before; delta < uint64(rowGroups-1) {
		t.Fatalf(`row_groups_skipped_total{reason="bloom"} rose by %d, want at least %d (every row group but the one holding the value)`, delta, rowGroups-1)
	}
}
