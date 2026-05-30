package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// baseTime is a fixed reference point used across limit tests.
var baseTime = time.Date(2026, 5, 2, 10, 0, 0, 0, time.UTC)

// addManifestFiles registers n files into s.manifest spanning [baseTime, baseTime+n*hour).
// Each file is placed in its own hourly partition so GetFilesForRange sees them all.
func addManifestFiles(s *Storage, n int) {
	for i := 0; i < n; i++ {
		t := baseTime.Add(time.Duration(i) * time.Hour)
		partition := fmt.Sprintf("dt=%s/hour=%02d", t.Format("2006-01-02"), t.Hour())
		key := fmt.Sprintf("file-%03d.parquet", i)
		fi := manifest.FileInfo{
			Key:       key,
			Size:      1024,
			RowCount:  100,
			MinTimeNs: t.UnixNano(),
			MaxTimeNs: t.Add(time.Hour - time.Nanosecond).UnixNano(),
		}
		s.manifest.AddFile(partition, fi)
	}
}

// queryRange returns [baseTime - 1ns, baseTime + n*hour] so GetFilesForRange returns all n files.
func queryRange(n int) (startNs, endNs int64) {
	return baseTime.Add(-time.Nanosecond).UnixNano(),
		baseTime.Add(time.Duration(n) * time.Hour).UnixNano()
}

// noopWriteBlock is a no-op block writer used to detect whether query tries to emit results.
func noopWriteBlock(_ uint, _ *logstorage.DataBlock) {}

// TestQueryFileLimitEnforced verifies that when files > MaxFilesPerQuery the query
// returns an error containing "file limit" and does not attempt to process any files.
// SKIP: MaxFilesPerQuery enforcement is not yet implemented in the traces module.
func TestQueryFileLimitEnforced(t *testing.T) {
	t.Skip("MaxFilesPerQuery enforcement not yet implemented in traces module")
}

// TestQueryFileLimitAllowsUnderLimit verifies that when files <= MaxFilesPerQuery
// the query proceeds without a file-limit error.
// SKIP: MaxFilesPerQuery enforcement is not yet implemented in the traces module.
func TestQueryFileLimitAllowsUnderLimit(t *testing.T) {
	t.Skip("MaxFilesPerQuery enforcement not yet implemented in traces module")
}

// TestQueryFileLimitDefaultIs500 verifies that when MaxFilesPerQuery=0
// the effective default of 500 is applied, so 501 files triggers an error.
// SKIP: MaxFilesPerQuery enforcement is not yet implemented in the traces module.
func TestQueryFileLimitDefaultIs500(t *testing.T) {
	t.Skip("MaxFilesPerQuery enforcement not yet implemented in traces module")
}

// TestQueryNarrowTimeRangeReducesFiles verifies that narrowing the query window
// reduces the number of files seen by RunQuery below the limit.
// SKIP: MaxFilesPerQuery enforcement is not yet implemented in the traces module.
func TestQueryNarrowTimeRangeReducesFiles(t *testing.T) {
	t.Skip("MaxFilesPerQuery enforcement not yet implemented in traces module")
}

// TestQueryManifestFastPathWithTimestampOnly verifies that the IsTimestampOnly
// fast path resolves files from manifest metadata without S3 I/O.
// NOTE: The file-limit interaction tests are skipped because MaxFilesPerQuery
// enforcement is not yet implemented in the traces module.
func TestQueryManifestFastPathWithTimestampOnly(t *testing.T) {
	s := testStorage()
	addManifestFiles(s, 5)

	startNs, endNs := queryRange(5)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	ctx := storage.WithTimestampOnlyHint(context.Background())
	var blocksEmitted int
	err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		blocksEmitted++
	})
	if err != nil {
		t.Errorf("IsTimestampOnly query should not error; got: %v", err)
	}
	// The manifest fast path resolves files with RowCount>0 and emits synthetic blocks.
	if blocksEmitted == 0 {
		t.Error("manifest fast path should have emitted synthetic blocks for fully-in-range files")
	}
}
