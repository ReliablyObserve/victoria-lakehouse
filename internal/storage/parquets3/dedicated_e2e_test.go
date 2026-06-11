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

// TestDedicated_E2E_PromotedColumn is the full-stack proof: a LogRow carrying a
// promoted OTel attribute in its dedicated COLUMN (container.id) is written to
// (mock) S3, then queried back by the attribute's bare name. It must (1) match
// the filter via the promoted column and (2) surface the value under its OTel
// field name — i.e. the column is queryable exactly as the map attribute was.
func TestDedicated_E2E_PromotedColumn(t *testing.T) {
	mock := newMockS3Server()
	defer mock.close()
	s := testStorageWithS3(t, mock.url())

	now := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "a", ServiceName: "api", ContainerID: "ctr-AAA", K8sClusterName: "prod-east"},
		{TimestampUnixNano: now.Add(time.Second).UnixNano(), Body: "b", ServiceName: "api", ContainerID: "ctr-BBB", K8sClusterName: "prod-east"},
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns())...))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	key := "logs/dt=2026-05-10/hour=14/batch-ded.parquet"
	registerFileInMockS3(t, s, mock, key, buf.Bytes(), now)

	startNs := now.Add(-time.Minute).UnixNano()
	endNs := now.Add(time.Minute).UnixNano()

	// Filter on the promoted column by its bare OTel name.
	q := mustParseQueryWithTime(t, `container.id:="ctr-AAA"`, startNs, endNs)
	var blocks []*logstorage.DataBlock
	var mu sync.Mutex
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		blocks = append(blocks, db)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	total, sawValue, sawCluster := 0, false, false
	for _, b := range blocks {
		total += b.RowsCount()
		if c := b.GetColumnByName("container.id"); c != nil {
			for _, v := range c.Values {
				if v == "ctr-AAA" {
					sawValue = true
				}
			}
		}
		if c := b.GetColumnByName("k8s.cluster.name"); c != nil {
			for _, v := range c.Values {
				if v == "prod-east" {
					sawCluster = true
				}
			}
		}
	}
	if total != 1 {
		t.Errorf("filter container.id=ctr-AAA matched %d rows, want 1 (promoted-column filter)", total)
	}
	if !sawValue {
		t.Error("container.id value not surfaced under its bare OTel field name")
	}
	if !sawCluster {
		t.Error("co-located promoted column k8s.cluster.name not surfaced")
	}
}
