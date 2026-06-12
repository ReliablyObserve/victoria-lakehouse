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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// makeMultiRGBloomLogParquet builds a multi-row-group LogRow parquet written
// with the PRODUCTION footer-bloom config (service.name + trace_id), embedding
// `known` as the trace_id of exactly one row. It grows to >= minBytes so the
// projected range-read path engages (shouldUseRangeRead needs a non-trivial
// body). Returns the encoded bytes.
func makeMultiRGBloomLogParquet(t *testing.T, baseTime time.Time, known string, minBytes, maxRowsPerRG int) []byte {
	t.Helper()
	var rows []schema.LogRow
	i := 0
	for {
		tid := fmt.Sprintf("trace-%016x-%016x", i, i*2654435761)
		if i == 0 {
			tid = known
		}
		rows = append(rows, schema.LogRow{
			TimestampUnixNano: baseTime.Add(time.Duration(i) * time.Microsecond).UnixNano(),
			Body:              fmt.Sprintf("row-%d-payload-%x-%x-%x", i, i*2654435761, i*1442695040, i*8675309),
			SeverityText:      []string{"INFO", "WARN", "ERROR", "DEBUG"}[i%4],
			ServiceName:       fmt.Sprintf("service-%d", i%16),
			TraceID:           tid,
			SpanID:            fmt.Sprintf("span-%016x", i),
		})
		i++
		if i%500 == 0 {
			var buf bytes.Buffer
			w := parquet.NewGenericWriter[schema.LogRow](&buf,
				parquet.Compression(&parquet.Zstd),
				parquet.MaxRowsPerRowGroup(int64(maxRowsPerRG)),
				parquet.BloomFilters(bloomFilters(schema.LogBloomColumns())...),
			)
			if _, err := w.Write(rows); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if buf.Len() >= minBytes {
				return buf.Bytes()
			}
		}
		if i > 100000 {
			t.Fatal("could not grow test parquet to minBytes")
		}
	}
}

// TestRangeRead_ExactBloomColumn_DoesNotFalseNegative is the regression guard
// for the projected range-read × footer-bloom data-loss bug.
//
// Root cause: on the projected range-read path (a column-selecting pipe like
// `| stats count()` / `| fields trace_id` reduces the projection, so
// openParquetFileWithPlan fetches ONLY the projected column-chunk byte ranges),
// ColumnChunk.BloomFilter() lazily reads the footer bloom from byte offsets that
// were never fetched and returns a non-empty but all-zero bloom whose Check()
// FALSE-NEGATIVES every value. bloomFilterSkip then wrongly skips every in-range
// row group, so `trace_id:=X | stats count()` returned 0 while the full-download
// `trace_id:=X` retrieval returned the rows.
//
// The fix gates bloomFilterSkip on bloom residency (projectedCols == nil): the
// row-group footer-bloom skip runs only when the whole file body is resident.
// This test exercises the PLANNED range-read path end-to-end and asserts the
// present trace_id is still found under a reduced projection.
func TestRangeRead_ExactBloomColumn_DoesNotFalseNegative(t *testing.T) {
	const known = "deadbeefcafe00010203040506070809"

	mock := newRangeLoggingS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())
	// Force the planned range-read path (default is window): planned mode +
	// a 1-byte whole-file threshold takes the plan-cold-footer rung.
	s.cfg.S3.ProjectedFetchMode = config.ProjectedFetchModePlanned
	s.cfg.S3.WholeFileThresholdBytes = 1

	baseTime := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	data := makeMultiRGBloomLogParquet(t, baseTime, known, 400*1024, 600)
	size := int64(len(data))

	// sanity: the file must actually carry a trace_id footer bloom, else the
	// test would pass vacuously (nothing to false-negative).
	fLocal, err := parquet.OpenFile(bytes.NewReader(data), size)
	if err != nil {
		t.Fatal(err)
	}
	tidx := findColumnIndex(fLocal.Root(), "trace_id")
	if tidx < 0 || fLocal.RowGroups()[0].ColumnChunks()[tidx].BloomFilter() == nil {
		t.Fatal("fixture has no trace_id footer bloom — cannot exercise the bug")
	}

	key := "logs/dt=2026-06-01/hour=10/file0.parquet"
	mock.putFile(key, data)
	s.manifest.AddFile("dt=2026-06-01/hour=10", manifest.FileInfo{
		Key:       key,
		Size:      size,
		MinTimeNs: baseTime.Add(-time.Minute).UnixNano(),
		MaxTimeNs: baseTime.Add(time.Hour).UnixNano(),
	})

	start := baseTime.Add(-time.Hour).UnixNano()
	end := baseTime.Add(2 * time.Hour).UnixNano()

	count := func(queryStr string) int {
		q := mustParseQueryWithTime(t, queryStr, start, end)
		var n int
		var mu sync.Mutex
		if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			n += db.RowsCount()
			mu.Unlock()
		}); err != nil {
			t.Fatalf("RunQuery(%q): %v", queryStr, err)
		}
		return n
	}

	// Full-projection retrieval (full download → bloom resident) is the
	// reference: it always found the row, even before the fix.
	full := count(fmt.Sprintf(`trace_id:=%q`, known))
	if full == 0 {
		t.Fatalf("retrieval (full projection) found 0 rows for present trace_id %q", known)
	}

	// Reduced projection (column-selecting pipe → range-read). BEFORE the fix
	// this returned 0; it must now equal the full-projection count.
	reduced := count(fmt.Sprintf(`trace_id:=%q | fields trace_id`, known))
	if reduced != full {
		t.Errorf("range-read reduced projection found %d rows, want %d (footer-bloom false-negative regression)", reduced, full)
	}
}
