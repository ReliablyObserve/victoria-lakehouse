package parquets3

// What a read does with a listed object that is gone (#772, R1): it answers from
// the manifest only when the manifest gives the object's rows exactly, and fails
// with 502 in every other case. Mirror of
// internal/storage/parquets3/row_read_gone_object_test.go for spans.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// goneRecoveryCase is one input of handle404Recovery. The base case is the only
// one whose rows the manifest gives exactly; every other case changes one thing.
type goneRecoveryCase struct {
	name        string
	mutate      func(fi *manifest.FileInfo)
	ctx         func(ctx context.Context) context.Context
	filter      *logstorage.Filter
	tombstones  bool
	start, end  int64
	wantRecover bool
}

func TestGoneObject_ManifestAnswersOnlyWhenExact(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	const rows = 7
	base := time.Date(2026, 10, 23, 10, 30, 0, 0, time.UTC)
	inRange := manifest.FileInfo{Key: "traces/dt=2026-10-23/hour=10/gone.parquet", RowCount: rows, MinTimeNs: base.UnixNano(), MaxTimeNs: base.Add(time.Minute).UnixNano()}
	from, to := base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano()
	tsOnly := func(q string) func(context.Context) context.Context {
		return func(ctx context.Context) context.Context {
			return withMetadataOnlyPlan(storage.WithTimestampOnlyHint(ctx), planMetadataOnly(mustParseQuery(t, q)))
		}
	}
	filter := parseFilterFromQuery(mustParseQuery(t, "error"))

	cases := []goneRecoveryCase{
		{name: "exact_covered_in_window", ctx: tsOnly("* | stats count() n"), start: from, end: to, wantRecover: true},
		{name: "window_cuts_the_object", ctx: tsOnly("* | stats count() n"), start: base.Add(10 * time.Second).UnixNano(), end: to},
		{name: "not_timestamp_only", ctx: func(ctx context.Context) context.Context {
			return withMetadataOnlyPlan(ctx, planMetadataOnly(mustParseQuery(t, "* | stats count() n")))
		}, start: from, end: to},
		{name: "no_plan", ctx: func(ctx context.Context) context.Context { return storage.WithTimestampOnlyHint(ctx) }, start: from, end: to},
		{name: "row_filter", ctx: tsOnly("* | stats count() n"), filter: filter, start: from, end: to},
		{name: "tombstone", ctx: tsOnly("* | stats count() n"), tombstones: true, start: from, end: to},
		{name: "bounds_inferred", ctx: tsOnly("* | stats count() n"), mutate: func(fi *manifest.FileInfo) { fi.BoundsInferred = true }, start: from, end: to},
		{name: "no_row_count", ctx: tsOnly("* | stats count() n"), mutate: func(fi *manifest.FileInfo) { fi.RowCount = 0 }, start: from, end: to},
		{name: "implausible_row_count", ctx: tsOnly("* | stats count() n"), mutate: func(fi *manifest.FileInfo) { fi.RowCount = maxPlausibleRowCount + 1 }, start: from, end: to},
		{name: "unknown_bounds", ctx: tsOnly("* | stats count() n"), mutate: func(fi *manifest.FileInfo) { fi.MinTimeNs = 0 }, start: from, end: to},
		// A query that groups by a time bucket the object spans cannot be answered
		// from the object's row count.
		{name: "spans_two_buckets", ctx: tsOnly("* | stats by (_time:10s) count() n"), start: from, end: to},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fi := inRange
			if tc.mutate != nil {
				tc.mutate(&fi)
			}
			emitted := 0
			cause := fmt.Errorf("s3 GetObject %s: %w", fi.Key, &types.NoSuchKey{})
			err := s.handle404Recovery(tc.ctx(context.Background()), fi, tc.start, tc.end, tc.filter, tc.tombstones,
				func(_ uint, db *logstorage.DataBlock) { emitted += db.RowsCount() }, cause)
			if tc.wantRecover {
				if err != nil || emitted != rows {
					t.Fatalf("handle404Recovery = %v with %d rows emitted, want nil and %d (the manifest gives the rows exactly)", err, emitted, rows)
				}
				return
			}
			goneReadError(t, err, fi.Key)
			if emitted != 0 {
				t.Fatalf("handle404Recovery emitted %d rows and still failed: a failed read must not answer from the manifest", emitted)
			}
		})
	}

	// A caller that stopped the read while the rows were emitted is reported by
	// RunQuery, not as a removed object.
	t.Run("caller_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(tsOnly("* | stats count() n")(context.Background()))
		cancel()
		err := s.handle404Recovery(ctx, inRange, from, to, nil, false, func(uint, *logstorage.DataBlock) {}, &types.NoSuchKey{})
		if err != nil {
			t.Fatalf("a cancelled read was reported as a removed object: %v", err)
		}
	})
}

// Through RunQuery: a timestamp-only count over objects whose rows the manifest
// knows is exact although one object was removed (the manifest answers, no read),
// and fails with 502 as soon as the window cuts the removed object (its rows inside
// the window are unknown).
func TestGoneObject_CountFromManifest_ThroughRunQuery(t *testing.T) {
	fx := newRCFixture(t, 4, rcKeyFmt)
	for i, key := range fx.keys {
		fi := manifest.FileInfo{Key: key, Size: int64(len(fx.datas[i])), RowCount: rcRowsPerFile,
			MinTimeNs: fx.base.Add(-time.Minute).UnixNano(), MaxTimeNs: fx.base.Add(time.Minute).UnixNano()}
		fx.s.manifest.RemoveFile(fx.partitionOf(key), key) // AddFile ignores a key it already holds
		fx.s.manifest.AddFile(fx.partitionOf(key), fi)
	}
	fx.removeObject(fx.keys[1])
	run := func(start, end int64) (int, error) {
		q := mustParseQueryWithTime(t, "* | stats count() n", start, end)
		rows := 0
		err := fx.s.RunQuery(storage.WithTimestampOnlyHint(context.Background()), nil, q, func(_ uint, db *logstorage.DataBlock) { rows += db.RowsCount() })
		return rows, err
	}

	rows, err := run(fx.start, fx.end) // the window holds every object
	if err != nil || rows != rcFiles*rcRowsPerFile {
		t.Fatalf("count over a window that holds the removed object: %d rows, err %v; want %d rows and no error", rows, err, rcFiles*rcRowsPerFile)
	}
	_, err = run(fx.base.UnixNano(), fx.end) // the window cuts every object
	goneReadError(t, err, fx.keys[1])
}

// The other signal is unaffected: an object of the logs signal that was removed
// from the bucket is not in this binary's listing, so it neither fails nor changes
// a read of this signal's objects (the logs binary has its own copy of the rule,
// with its own tests, and reads only its own prefix).
func TestGoneObject_OtherSignalIsUnaffected(t *testing.T) {
	fx := newRCFixture(t, 4, rcKeyFmt)
	fx.fs.mock.putFile("logs/dt=2026-10-23/hour=10/other-signal.parquet", fx.datas[0])
	fx.removeObject("logs/dt=2026-10-23/hour=10/other-signal.parquet")
	gone := metrics.QueryFileNotFoundTotal.Get()
	rows, err := fx.query(t, context.Background(), "*", nil)
	if err != nil || rows != rcFiles*rcRowsPerFile {
		t.Fatalf("%d rows, err %v; want %d rows and no error", rows, err, rcFiles*rcRowsPerFile)
	}
	if got := metrics.QueryFileNotFoundTotal.Get(); got != gone {
		t.Fatalf("lakehouse_query_file_not_found_total moved (%d -> %d) for an object this signal never listed", gone, got)
	}
}
