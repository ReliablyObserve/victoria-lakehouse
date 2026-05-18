package parquets3

import (
	"context"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestExtractLogBloomValues(t *testing.T) {
	rows := []schema.LogRow{
		{TraceID: "trace-aaa", ServiceName: "api-gw"},
		{TraceID: "trace-bbb", ServiceName: "api-gw"},
		{TraceID: "trace-ccc", ServiceName: "order-svc"},
		{TraceID: "", ServiceName: ""},
	}

	vals := extractLogBloomValues(rows)
	if vals == nil {
		t.Fatal("expected non-nil bloom values")
	}

	traceIDs := vals["trace_id"]
	if len(traceIDs) != 3 {
		t.Errorf("want 3 trace_ids, got %d", len(traceIDs))
	}

	services := vals["service.name"]
	if len(services) != 2 {
		t.Errorf("want 2 services, got %d", len(services))
	}
}

func TestExtractLogBloomValues_Empty(t *testing.T) {
	vals := extractLogBloomValues(nil)
	if vals != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestExtractTraceBloomValues(t *testing.T) {
	rows := []schema.TraceRow{
		{TraceID: "trace-111", ServiceName: "user-svc"},
		{TraceID: "trace-222", ServiceName: "user-svc"},
		{TraceID: "trace-333", ServiceName: "payment-svc"},
	}

	vals := extractTraceBloomValues(rows)
	if vals == nil {
		t.Fatal("expected non-nil bloom values")
	}

	traceIDs := vals["trace_id"]
	if len(traceIDs) != 3 {
		t.Errorf("want 3 trace_ids, got %d", len(traceIDs))
	}

	services := vals["service.name"]
	if len(services) != 2 {
		t.Errorf("want 2 services, got %d", len(services))
	}
}

func TestExtractTraceBloomValues_Empty(t *testing.T) {
	vals := extractTraceBloomValues(nil)
	if vals != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestStorageBloomObserver_OnFileFlush(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	obs := &storageBloomObserver{bloom: pi, pool: nil}

	obs.OnFileFlush("dt=2026-05-02/hour=10", "dt=2026-05-02/hour=10/file1.parquet",
		map[string][]string{
			"trace_id":     {"aaa", "bbb"},
			"service.name": {"api-gw"},
		})

	idx := pi.GetPartition("dt=2026-05-02/hour=10")
	if idx == nil {
		t.Fatal("partition should exist after OnFileFlush")
	}
	if idx.Len() != 1 {
		t.Errorf("want 1 entry, got %d", idx.Len())
	}

	result := idx.MayContain([]string{"dt=2026-05-02/hour=10/file1.parquet"}, "trace_id", "aaa")
	if len(result) != 1 {
		t.Error("bloom should contain trace_id=aaa")
	}
}

func TestStorageBloomObserver_NilBloom(t *testing.T) {
	obs := &storageBloomObserver{bloom: nil, pool: nil}
	obs.OnFileFlush("p1", "f1", map[string][]string{"trace_id": {"aaa"}})
}

func TestStorageBloomObserver_EmptyValues(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	obs := &storageBloomObserver{bloom: pi, pool: nil}

	obs.OnFileFlush("p1", "f1", nil)
	obs.OnFileFlush("p1", "f1", map[string][]string{})

	if pi.Len() != 0 {
		t.Error("no partitions should be created for empty values")
	}
}

func TestStorageBloomObserver_MultipleFlushes(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	obs := &storageBloomObserver{bloom: pi, pool: nil}

	for i := 0; i < 10; i++ {
		obs.OnFileFlush("dt=2026-05-02/hour=10",
			"dt=2026-05-02/hour=10/file"+string(rune('A'+i))+".parquet",
			map[string][]string{
				"trace_id": {
					"trace-" + string(rune('A'+i)) + "-0",
					"trace-" + string(rune('A'+i)) + "-1",
				},
			})
	}

	idx := pi.GetPartition("dt=2026-05-02/hour=10")
	if idx == nil {
		t.Fatal("partition should exist")
	}
	if idx.Len() != 10 {
		t.Errorf("want 10 entries, got %d", idx.Len())
	}

	dirties := pi.DirtyPartitions()
	if len(dirties) != 1 {
		t.Errorf("want 1 dirty partition, got %d", len(dirties))
	}
}

func TestStorageBloomObserver_ConcurrentFlushes(t *testing.T) {
	pi := bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01)
	obs := &storageBloomObserver{bloom: pi, pool: nil}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			partition := "dt=2026-05-02/hour=10"
			key := "dt=2026-05-02/hour=10/concurrent-" + string(rune('0'+n%10)) + ".parquet"
			obs.OnFileFlush(partition, key, map[string][]string{
				"trace_id": {"trace-concurrent-" + string(rune('0'+n%10))},
			})
		}(i)
	}
	wg.Wait()

	idx := pi.GetPartition("dt=2026-05-02/hour=10")
	if idx == nil {
		t.Fatal("partition should exist after concurrent flushes")
	}
}

func TestBloomS3Loader_NonExistent(t *testing.T) {
	loader := bloomS3Loader(nil, "prefix/")
	idx, err := loader(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("non-existent bloom should return nil, not error: %v", err)
	}
	if idx != nil {
		t.Error("non-existent bloom should return nil index")
	}
}

func TestBloomObserverInterface(t *testing.T) {
	var obs BloomObserver = &storageBloomObserver{
		bloom: bloomindex.NewPartitionedIndex(bloomindex.GranularityHour, 0.01),
	}
	obs.OnFileFlush("p1", "k1", map[string][]string{"trace_id": {"a"}})
	obs.PersistDirty(context.Background(), "prefix/")
}
