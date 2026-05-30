package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// TestRunQuery_HotBoundary_SelectRoleDoesNotSuppress verifies that the
// hot-boundary suppression only applies when the running node has role=insert.
// The select-role and all-role nodes are responsible for the cold tier and
// must scan files even when the query range overlaps the hot boundary.
//
// Regression: previously the suppression dropped rows for ALL roles when the
// query was inside the hot range, silently dropping ~15% of select-only
// results from the cold path.
func TestRunQuery_HotBoundary_SelectRoleDoesNotSuppress(t *testing.T) {
	now := time.Now().UTC()
	partitionTime := now.AddDate(0, 0, -1).Truncate(time.Hour)
	partition := "dt=" + partitionTime.Format("2006-01-02") + "/hour=" + partitionTime.Format("15")
	fileStartNs := partitionTime.Add(10 * time.Minute).UnixNano()
	fileEndNs := partitionTime.Add(50 * time.Minute).UnixNano()
	queryStartNs := partitionTime.Add(5 * time.Minute).UnixNano()
	queryEndNs := partitionTime.Add(55 * time.Minute).UnixNano()

	for _, role := range []config.Role{config.RoleSelect, config.RoleAll} {
		t.Run(string(role), func(t *testing.T) {
			s := testStorage()
			s.cfg.Role = role

			// Hot boundary covers a wide range that the query falls strictly inside.
			s.discovery = discovery.New("", nil, "", "", "9428", 5*time.Second)
			s.discovery.SetHotBoundaryForTest(&discovery.HotBoundary{
				MinTime: partitionTime.Add(-1 * time.Hour),
				MaxTime: partitionTime.Add(2 * time.Hour),
			})

			s.manifest.AddFile(partition, manifest.FileInfo{
				Key:       "logs/" + partition + "/test.parquet",
				Size:      1024,
				RowCount:  5,
				MinTimeNs: fileStartNs,
				MaxTimeNs: fileEndNs,
			})

			// Query strictly inside hot boundary AND covers the seeded file.
			q := mustParseQueryWithTime(t, "*", queryStartNs, queryEndNs)

			var blocks int
			writeBlock := func(_ uint, _ *logstorage.DataBlock) {
				blocks++
			}

			ctx := storage.WithTimestampOnlyHint(context.Background())
			err := s.RunQuery(ctx, nil, q, writeBlock)
			if err != nil {
				t.Fatalf("RunQuery returned error: %v", err)
			}
			// For select/all roles we must NOT suppress — the manifest fast
			// path should fire and emit at least one synthetic block.
			if blocks == 0 {
				t.Fatalf("role=%s: expected RunQuery to scan/emit blocks, got 0 (hot-boundary suppression incorrectly fired)", role)
			}
		})
	}
}

// TestRunQuery_HotBoundary_InsertRoleSuppresses verifies that the
// hot-boundary suppression DOES apply for role=insert nodes. These nodes
// host the hot tier and must not double-serve results that select nodes
// will fetch via the hot path.
func TestRunQuery_HotBoundary_InsertRoleSuppresses(t *testing.T) {
	s := testStorage()
	s.cfg.Role = config.RoleInsert

	now := time.Now().UTC()
	partitionTime := now.AddDate(0, 0, -1).Truncate(time.Hour)
	partition := "dt=" + partitionTime.Format("2006-01-02") + "/hour=" + partitionTime.Format("15")
	fileStartNs := partitionTime.Add(10 * time.Minute).UnixNano()
	fileEndNs := partitionTime.Add(50 * time.Minute).UnixNano()
	queryStartNs := partitionTime.Add(5 * time.Minute).UnixNano()
	queryEndNs := partitionTime.Add(55 * time.Minute).UnixNano()

	s.discovery = discovery.New("", nil, "", "", "9428", 5*time.Second)
	s.discovery.SetHotBoundaryForTest(&discovery.HotBoundary{
		MinTime: partitionTime.Add(-1 * time.Hour),
		MaxTime: partitionTime.Add(2 * time.Hour),
	})

	s.manifest.AddFile(partition, manifest.FileInfo{
		Key:       "logs/" + partition + "/test.parquet",
		Size:      1024,
		RowCount:  5,
		MinTimeNs: fileStartNs,
		MaxTimeNs: fileEndNs,
	})

	q := mustParseQueryWithTime(t, "*", queryStartNs, queryEndNs)

	var blocks int
	writeBlock := func(_ uint, _ *logstorage.DataBlock) {
		blocks++
	}

	ctx := storage.WithTimestampOnlyHint(context.Background())
	if err := s.RunQuery(ctx, nil, q, writeBlock); err != nil {
		t.Fatalf("RunQuery returned error: %v", err)
	}
	if blocks != 0 {
		t.Fatalf("role=insert: expected hot-boundary suppression (0 blocks), got %d", blocks)
	}
}
