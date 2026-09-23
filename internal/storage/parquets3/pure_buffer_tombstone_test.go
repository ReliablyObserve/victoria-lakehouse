package parquets3

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
)

// TestRunQuery_PureBufferWindowHonoursTombstones: a window no Parquet file
// covers is answered from the co-located buffer, and the pure-buffer fast path
// hands the WHOLE query — aggregation pipes included — to the buffer's engine.
// Tombstones are applied to the blocks the storage emits, which on that path
// are already aggregated: a count over the unflushed window came back with the
// deleted rows in it. With a tombstone over the window the storage must take
// the raw-row path, where the tombstone filter sees every row.
func TestRunQuery_PureBufferWindowHonoursTombstones(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open buffer: %v", err)
	}
	defer bs.Close()

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 6; i++ {
		svc := "checkout"
		if i%3 == 2 {
			svc = "secret"
		}
		lr.MustAdd(logstorage.TenantID{}, now, []logstorage.Field{
			{Name: "service.name", Value: svc},
			{Name: "_msg", Value: "event-" + strconv.Itoa(i)},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)

	startNs, endNs := now-int64(time.Hour), now+int64(time.Hour)
	if files := s.manifest.GetFilesForRange(startNs, endNs); len(files) != 0 {
		t.Fatalf("fixture: the window must be pure-buffer, found %d files", len(files))
	}

	count := func() int64 {
		t.Helper()
		q := mustParseQueryWithTime(t, "* | stats count() as rows", startNs, endNs)
		var effective int64
		err := s.RunQuery(context.Background(), []logstorage.TenantID{{}}, q, func(_ uint, db *logstorage.DataBlock) {
			// What the caller ends up counting: an aggregated block carries the
			// count, a raw block contributes its rows (the caller aggregates).
			if c := db.GetColumnByName("rows"); c != nil {
				for _, v := range c.Values {
					n, _ := strconv.ParseInt(v, 10, 64)
					effective += n
				}
				return
			}
			effective += int64(db.RowsCount())
		})
		if err != nil {
			t.Fatalf("RunQuery: %v", err)
		}
		return effective
	}

	if got := count(); got != 6 {
		t.Fatalf("fixture: want 6 buffered rows counted before any delete, got %d", got)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "recent", Query: `service.name:="secret"`, StartNs: startNs, EndNs: endNs, Mode: "hide"})
	s.SetTombstoneStore(store)

	if got := count(); got != 4 {
		t.Fatalf("count over the unflushed window = %d, want 4: the 2 tombstoned rows must not be counted", got)
	}
}
