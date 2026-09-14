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

// A partition that is compacted AND still receiving flushes holds, at every
// moment, a compacted object whose row range spans the whole hour (its inputs
// were backfilled across it) next to the objects flushed after that compaction
// ran. The enumeration paths used to drop "redundant" objects before scanning,
// and the drop was decided from the time ranges and the compaction levels
// alone: a later flush whose rows fall inside the compacted object's range
// looked redundant, so every value that only the fresh flush holds — the last
// minutes of a live tenant — disappeared from field_values and field_names
// while /select/logsql/query kept returning that object's rows.
//
// That is what the e2e tenant-scope suite caught on tenant 0:0, the only tenant
// whose partition is both compacted and continuously written: the marker
// service was returned by query and by hits, and was missing from field_values
// for as long as the run lasted (the compacted object kept being re-compacted
// to a higher level, so the drop never healed).

const overlapPartition = "dt=2026-09-14/hour=16"

func overlapHour() time.Time { return time.Date(2026, 9, 14, 16, 0, 0, 0, time.UTC) }

// compactedPlusFreshFlush builds the shape above: one compacted object holding
// backfilled rows across the whole hour, and one later flush holding the last
// two minutes of rows plus the marker rows nothing else has.
func compactedPlusFreshFlush(t *testing.T) (*Storage, []manifest.FileInfo) {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	hour := overlapHour()

	merged := make([]logRow, 0, 600)
	for i := 0; i < 600; i++ {
		merged = append(merged, logRow{
			TimestampUnixNano: hour.Add(time.Duration(i) * 6 * time.Second).UnixNano(),
			Body:              fmt.Sprintf("background %d", i),
			SeverityText:      "INFO",
			ServiceName:       "svc-datagen",
		})
	}
	fresh := make([]logRow, 0, 207)
	for i := 0; i < 200; i++ {
		fresh = append(fresh, logRow{
			TimestampUnixNano: hour.Add(43*time.Minute + time.Duration(i)*600*time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("background %d", 1000+i),
			SeverityText:      "INFO",
			ServiceName:       "svc-datagen",
		})
	}
	for i := 0; i < overlapMarkerRows; i++ {
		fresh = append(fresh, logRow{
			TimestampUnixNano: hour.Add(44*time.Minute + time.Duration(i)*time.Millisecond).UnixNano(),
			Body:              overlapMarker,
			SeverityText:      "INFO",
			ServiceName:       overlapMarkerService,
		})
	}

	add := func(key string, rows []logRow, level int) manifest.FileInfo {
		data := writeParquetToBytes(t, rows)
		mock.putFile(key, data)
		minNs, maxNs := rows[0].TimestampUnixNano, rows[0].TimestampUnixNano
		for _, r := range rows {
			if r.TimestampUnixNano < minNs {
				minNs = r.TimestampUnixNano
			}
			if r.TimestampUnixNano > maxNs {
				maxNs = r.TimestampUnixNano
			}
		}
		fi := manifest.FileInfo{
			Key:             key,
			Size:            int64(len(data)),
			RowCount:        int64(len(rows)),
			MinTimeNs:       minNs,
			MaxTimeNs:       maxNs,
			CompactionLevel: level,
		}
		s.manifest.AddFile(overlapPartition, fi)
		return fi
	}

	// The compacted object is written first and is re-compacted to a higher
	// level while the tenant keeps flushing, exactly as the scheduler does.
	compacted := add("logs/"+overlapPartition+"/compacted-L4-a735e271.parquet", merged, 4)
	flushed := add("logs/"+overlapPartition+"/d64720378d7b87cc.parquet", fresh, 0)
	return s, []manifest.FileInfo{compacted, flushed}
}

const (
	overlapMarker        = "tscope-marker-row"
	overlapMarkerService = "svc-marker"
	overlapMarkerRows    = 7
)

func overlapQuery(t *testing.T) *logstorage.Query {
	t.Helper()
	hour := overlapHour()
	return mustParseQueryWithTime(t, fmt.Sprintf("_msg:=%q", overlapMarker),
		hour.Add(-time.Hour).UnixNano(), hour.Add(2*time.Hour).UnixNano())
}

// TestGetFieldValues_FreshFlushInsideACompactedRange is the regression: a value
// that only the newest object holds must be enumerated, whatever a compacted
// neighbour's time range and compaction level look like.
func TestGetFieldValues_FreshFlushInsideACompactedRange(t *testing.T) {
	s, files := compactedPlusFreshFlush(t)
	ctx := context.Background()
	q := overlapQuery(t)

	// Contrast: the query path reads the very same object list and finds the
	// rows, so a missing value below is an enumeration defect, not selection.
	rows := 0
	if err := s.RunQuery(ctx, []logstorage.TenantID{{}}, q, func(_ uint, db *logstorage.DataBlock) {
		rows += db.RowsCount()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if rows != overlapMarkerRows {
		t.Fatalf("query returned %d rows, want %d — the fixture does not hold the marker", rows, overlapMarkerRows)
	}

	values, err := s.GetFieldValues(ctx, []logstorage.TenantID{{}}, q, "service.name", 0)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	got := valuesToCounts(values)
	if got[overlapMarkerService] == 0 {
		t.Errorf("field_values lost %q, the value only %s holds (got %v)", overlapMarkerService, files[1].Key, got)
	}
}

// TestGetFieldValues_NeverDropsAnotherTenantsObject: two tenants writing the
// same seconds are not a compaction pair. The drop heuristic compared time
// ranges and sizes, so a big tenant's object could swallow a small tenant's —
// this keeps the cross-tenant case pinned now that the manifest, not the read
// path, decides what is redundant.
func TestGetFieldValues_NeverDropsAnotherTenantsObject(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	hour := overlapHour()

	put := func(key, service string, n int, spread time.Duration) {
		rows := make([]logRow, 0, n)
		for i := 0; i < n; i++ {
			rows = append(rows, logRow{
				TimestampUnixNano: hour.Add(10*time.Minute + time.Duration(i)*spread).UnixNano(),
				Body:              overlapMarker,
				SeverityText:      "INFO",
				ServiceName:       service,
			})
		}
		data := writeParquetToBytes(t, rows)
		mock.putFile(key, data)
		s.manifest.AddFile(overlapPartition, manifest.FileInfo{
			Key:       key,
			Size:      int64(len(data)),
			RowCount:  int64(len(rows)),
			MinTimeNs: rows[0].TimestampUnixNano,
			MaxTimeNs: rows[len(rows)-1].TimestampUnixNano,
		})
	}
	put("0/0/logs/"+overlapPartition+"/big.parquet", "svc-noisy-tenant", 500, time.Second)
	put("1001/0/logs/"+overlapPartition+"/small.parquet", "svc-quiet-tenant", 3, time.Millisecond)

	values, err := s.GetFieldValues(storage.WithGlobalRead(context.Background()), nil, overlapQuery(t), "service.name", 0)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	got := valuesToCounts(values)
	for _, want := range []string{"svc-noisy-tenant", "svc-quiet-tenant"} {
		if got[want] == 0 {
			t.Errorf("cross-tenant read lost %q (got %v)", want, got)
		}
	}
}

// TestGetFieldNames_FreshFlushInsideACompactedRange: the same drop hid the
// fields of the newest object from field_names.
func TestGetFieldNames_FreshFlushInsideACompactedRange(t *testing.T) {
	s, files := compactedPlusFreshFlush(t)
	names, err := s.GetFieldNames(context.Background(), []logstorage.TenantID{{}}, overlapQuery(t))
	if err != nil {
		t.Fatalf("GetFieldNames: %v", err)
	}
	var hits uint64
	for _, n := range names {
		if n.Value == "service.name" {
			hits = n.Hits
		}
	}
	// Both objects carry service.name, so the hits must cover both; the drop
	// showed only the compacted object's rows.
	want := uint64(files[0].RowCount + files[1].RowCount)
	if hits != want {
		t.Errorf("field_names service.name hits=%d, want %d (both objects)", hits, want)
	}
}
