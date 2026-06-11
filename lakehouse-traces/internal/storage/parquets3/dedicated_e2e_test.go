package parquets3

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestDedicated_E2E_PromotedColumn_Traces is the full-stack proof for traces: a
// TraceRow carrying promoted OTel attributes in dedicated COLUMNS (container.id,
// url.full) is written to (mock) S3, then queried back — the values must surface
// under their bare OTel field names, identical to how the map attributes did.
func TestDedicated_E2E_PromotedColumn_Traces(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), TraceID: "t1", SpanID: "s1", ServiceName: "api",
			ContainerID: "ctr-AAA", URLFull: "https://x/a"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), TraceID: "t2", SpanID: "s2", ServiceName: "api",
			ContainerID: "ctr-BBB", URLFull: "https://x/b"},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf,
		parquet.BloomFilters(bloomFilters(schema.TraceBloomColumns())...))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	key := "traces/dt=2026-05-10/hour=14/batch-ded.parquet"
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), now)

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())
	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	total, sawContainer, sawURL := 0, false, false
	for _, b := range blocks {
		total += b.RowsCount()
		if c := b.GetColumnByName("container.id"); c != nil {
			for _, v := range c.Values {
				if v == "ctr-AAA" || v == "ctr-BBB" {
					sawContainer = true
				}
			}
		}
		if c := b.GetColumnByName("url.full"); c != nil {
			for _, v := range c.Values {
				if v == "https://x/a" || v == "https://x/b" {
					sawURL = true
				}
			}
		}
	}
	if total != 2 {
		t.Errorf("query matched %d rows, want 2", total)
	}
	if !sawContainer {
		t.Error("promoted resource column container.id not surfaced under its OTel name")
	}
	if !sawURL {
		t.Error("promoted span column url.full not surfaced under its OTel name")
	}
}
