package parquets3

import (
	"context"
	"fmt"
	"strings"
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
func TestQueryFileLimitEnforced(t *testing.T) {
	s := testStorage()
	s.cfg.Query.MaxFilesPerQuery = 10

	addManifestFiles(s, 20)

	startNs, endNs := queryRange(20)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	var blocksCalled int
	err := s.RunQuery(context.Background(), nil, q, func(workerID uint, db *logstorage.DataBlock) {
		blocksCalled++
	})

	if err == nil {
		t.Fatal("expected error when files exceed MaxFilesPerQuery, got nil")
	}
	if !strings.Contains(err.Error(), "file limit") && !strings.Contains(err.Error(), "limit") {
		t.Errorf("error message should mention limit; got: %v", err)
	}
	if blocksCalled > 0 {
		t.Errorf("writeBlock should not be called when file limit exceeded; called %d times", blocksCalled)
	}
}

// TestQueryFileLimitAllowsUnderLimit verifies that when files <= MaxFilesPerQuery
// the query proceeds without a file-limit error.
//
// A pre-cancelled context is used so that file workers exit immediately after the
// limit check passes (no S3 pool is present in unit tests). We only verify that
// no "file limit" error is returned — context.Canceled is acceptable.
func TestQueryFileLimitAllowsUnderLimit(t *testing.T) {
	s := testStorage()
	s.cfg.Query.MaxFilesPerQuery = 100

	addManifestFiles(s, 5)

	startNs, endNs := queryRange(5)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	// Cancel context immediately so file workers exit without attempting S3 I/O.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.RunQuery(ctx, nil, q, noopWriteBlock)
	// context.Canceled is acceptable; file-limit error is not.
	if err != nil && (strings.Contains(err.Error(), "file limit") || strings.Contains(err.Error(), "limit")) {
		t.Errorf("unexpected file-limit error with 5 files and limit=100: %v", err)
	}
}

// TestQueryFileLimitDefaultIsUnlimited verifies that when
// MaxFilesPerQuery=0 (the new default) the file count is NOT capped —
// VL upstream has no such cap and the memory budget
// (query.max-live-bytes + rgDecodeSem semaphore) is the real safety net.
// A 7-day wildcard at 600+ files used to be rejected at the 500-file
// fallback; now it proceeds and the memory budget bounds resource use.
func TestQueryFileLimitDefaultIsUnlimited(t *testing.T) {
	s := testStorage()
	// Leave MaxFilesPerQuery=0 — RunQuery treats 0 as "unlimited".
	s.cfg.Query.MaxFilesPerQuery = 0

	addManifestFiles(s, 501)

	startNs, endNs := queryRange(501)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	// Cancel context immediately so file workers exit without S3 I/O.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.RunQuery(ctx, nil, q, noopWriteBlock)
	if err != nil && strings.Contains(err.Error(), "file limit") {
		t.Errorf("default (0) must NOT enforce a file cap; got: %v", err)
	}
}

// TestQueryNarrowTimeRangeReducesFiles verifies that narrowing the query window
// reduces the number of files seen by RunQuery below the limit.
//
// Setup: 24 hourly files, MaxFilesPerQuery=10.
// - Full 24h query → fails (24 files > 10).
// - 1h query → succeeds (only ~1 file returned by GetFilesForRange).
func TestQueryNarrowTimeRangeReducesFiles(t *testing.T) {
	s := testStorage()
	s.cfg.Query.MaxFilesPerQuery = 10

	addManifestFiles(s, 24)

	// Full 24h range: expect limit error.
	startFull, endFull := queryRange(24)
	qFull := mustParseQueryWithTime(t, "*", startFull, endFull)
	err := s.RunQuery(context.Background(), nil, qFull, noopWriteBlock)
	if err == nil {
		t.Fatal("expected file-limit error for full 24h query with 24 files and limit=10")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("expected limit error; got: %v", err)
	}

	// Narrow 1h range: only the file at baseTime+0h falls in this window.
	// Use a pre-cancelled context so file workers exit without S3 I/O (no pool in unit tests).
	start1h := baseTime.Add(-time.Nanosecond).UnixNano()
	end1h := baseTime.Add(time.Hour).UnixNano()
	q1h := mustParseQueryWithTime(t, "*", start1h, end1h)

	ctx1h, cancel1h := context.WithCancel(context.Background())
	cancel1h()
	err = s.RunQuery(ctx1h, nil, q1h, noopWriteBlock)
	// context.Canceled is acceptable; file-limit error is not.
	if err != nil && strings.Contains(err.Error(), "limit") {
		t.Errorf("narrow 1h query should not hit file limit; got: %v", err)
	}
}

// TestQueryManifestFastPathBypassesLimit verifies how the IsTimestampOnly fast path
// interacts with the file limit check.
//
// Important: the file limit check runs BEFORE the manifest fast path in RunQuery.
// So metadata-only queries still fail when files > MaxFilesPerQuery. This is by design:
// the limit prevents OOM for any query type, including lightweight metadata queries.
// This test documents and enforces that expected behaviour.
func TestQueryManifestFastPathBypassesLimit(t *testing.T) {
	s := testStorage()
	s.cfg.Query.MaxFilesPerQuery = 10

	// Add files that are all fully within the query range so they could use
	// the manifest fast path (RowCount > 0, MinTimeNs/MaxTimeNs set).
	addManifestFiles(s, 20) // 20 > limit of 10

	startNs, endNs := queryRange(20)
	q := mustParseQueryWithTime(t, "*", startNs, endNs)

	// With IsTimestampOnly hint set, the manifest fast path would be eligible,
	// but the file limit check fires first and returns an error.
	ctx := storage.WithTimestampOnlyHint(context.Background())
	err := s.RunQuery(ctx, nil, q, noopWriteBlock)

	if err == nil {
		t.Fatal("expected file-limit error even with IsTimestampOnly hint; limit check precedes fast path")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("expected limit error; got: %v", err)
	}

	// Contrast: with files under the limit, the manifest fast path runs and
	// returns successfully (no S3 I/O needed).
	s2 := testStorage()
	s2.cfg.Query.MaxFilesPerQuery = 10
	addManifestFiles(s2, 5) // 5 < limit of 10

	startNs2, endNs2 := queryRange(5)
	q2 := mustParseQueryWithTime(t, "*", startNs2, endNs2)

	ctx2 := storage.WithTimestampOnlyHint(context.Background())
	var blocksEmitted int
	err = s2.RunQuery(ctx2, nil, q2, func(_ uint, db *logstorage.DataBlock) {
		blocksEmitted++
	})
	if err != nil {
		t.Errorf("under-limit query with IsTimestampOnly should not error; got: %v", err)
	}
	// The manifest fast path resolves files with RowCount>0 and emits synthetic blocks.
	if blocksEmitted == 0 {
		t.Error("manifest fast path should have emitted synthetic blocks for fully-in-range files")
	}
}
