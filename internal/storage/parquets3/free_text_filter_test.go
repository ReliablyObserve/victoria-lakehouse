package parquets3

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestInteg_FreeTextFilter_TimestampOnly reproduces the cold full-text search bug:
// `error | stats count()` sets the timestamp-only hint, which reduced the scan
// projection to just _time — dropping _msg, so the free-text word filter (which
// has no bloom to push down) matched ZERO rows instead of the real count. The fix
// keeps the message column projected whenever the filter is free-text.
func TestInteg_FreeTextFilter_TimestampOnly(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
	rows := []logRow{
		{TimestampUnixNano: now.UnixNano(), Body: "connection error to db", ServiceName: "api"},
		{TimestampUnixNano: now.Add(1).UnixNano(), Body: "fatal error in handler", ServiceName: "api"},
		{TimestampUnixNano: now.Add(2).UnixNano(), Body: "request ok", ServiceName: "api"},
		{TimestampUnixNano: now.Add(3).UnixNano(), Body: "another error here", ServiceName: "worker"},
		{TimestampUnixNano: now.Add(4).UnixNano(), Body: "all good", ServiceName: "worker"},
	}
	data := writeParquetToBytes(t, rows)
	registerFileInMockS3(t, s, mock, "logs/dt=2026-05-10/hour=14/ft.parquet", data, now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()
	q := mustParseQueryWithTime(t, "error", startNs, endNs)

	// The timestamp-only hint is what the count()/hits endpoints set — the exact
	// condition under which the bug dropped _msg.
	ctx := storage.WithTimestampOnlyHint(context.Background())
	var got int
	var mu sync.Mutex
	if err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		got += db.RowsCount()
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if got != 3 {
		t.Fatalf("free-text `error` under timestamp-only matched %d rows, want 3 "+
			"(cold full-text search returns 0 regression)", got)
	}
}
