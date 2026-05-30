package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestComputeTraceIndex(t *testing.T) {
	rows := []schema.TraceRow{
		{TraceID: "trace-a", StartTimeUnixNano: 1000, DurationNs: 100},
		{TraceID: "trace-a", StartTimeUnixNano: 900, DurationNs: 200},
		{TraceID: "trace-b", StartTimeUnixNano: 2000, DurationNs: 50},
		{TraceID: "", StartTimeUnixNano: 3000, DurationNs: 10},
	}

	entries := computeTraceIndex(rows)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	idx := make(map[string]TraceIndexEntry)
	for _, e := range entries {
		idx[e.TraceID] = e
	}

	a := idx["trace-a"]
	if a.StartNs != 900 {
		t.Errorf("trace-a startNs: want 900, got %d", a.StartNs)
	}
	if a.EndNs != 1100 {
		t.Errorf("trace-a endNs: want 1100, got %d", a.EndNs)
	}

	b := idx["trace-b"]
	if b.StartNs != 2000 {
		t.Errorf("trace-b startNs: want 2000, got %d", b.StartNs)
	}
	if b.EndNs != 2050 {
		t.Errorf("trace-b endNs: want 2050, got %d", b.EndNs)
	}
}

func TestTraceIDPartition(t *testing.T) {
	p := traceIDPartition("abc123")
	if p >= 1024 {
		t.Errorf("partition %d >= 1024", p)
	}
	p2 := traceIDPartition("abc123")
	if p != p2 {
		t.Error("partition not deterministic")
	}
	p3 := traceIDPartition("different-trace")
	if p == p3 {
		t.Log("different traces have same partition (unlikely but possible)")
	}
}

func TestMarshalUnmarshalTraceIndex(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "trace-1", Partition: 42, StartNs: 1000, EndNs: 2000},
		{TraceID: "trace-2", Partition: 99, StartNs: 3000, EndNs: 4000},
	}

	data := marshalTraceIndex(entries)
	got, err := unmarshalTraceIndex(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(got))
	}
	for i, e := range got {
		if e.TraceID != entries[i].TraceID {
			t.Errorf("entry %d traceID: want %q, got %q", i, entries[i].TraceID, e.TraceID)
		}
		if e.Partition != entries[i].Partition {
			t.Errorf("entry %d partition: want %d, got %d", i, entries[i].Partition, e.Partition)
		}
		if e.StartNs != entries[i].StartNs {
			t.Errorf("entry %d startNs: want %d, got %d", i, entries[i].StartNs, e.StartNs)
		}
		if e.EndNs != entries[i].EndNs {
			t.Errorf("entry %d endNs: want %d, got %d", i, entries[i].EndNs, e.EndNs)
		}
	}
}

func TestUnmarshalTraceIndexErrors(t *testing.T) {
	if _, err := unmarshalTraceIndex([]byte{1, 2}); err == nil {
		t.Error("expected error for short data")
	}
	if _, err := unmarshalTraceIndex([]byte{99, 0, 0, 0, 0}); err == nil {
		t.Error("expected error for wrong version")
	}
}

func TestTraceIndexFromMetadata(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "t1", Partition: 10, StartNs: 100, EndNs: 200},
	}
	data := marshalTraceIndex(entries)
	meta := map[string]string{traceIndexMetadataKey: string(data)}

	got, ok := traceIndexFromMetadata(meta)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if len(got) != 1 || got[0].TraceID != "t1" {
		t.Errorf("unexpected: %+v", got)
	}

	_, ok = traceIndexFromMetadata(map[string]string{})
	if ok {
		t.Error("expected ok=false for missing key")
	}
}

func TestLookupTraceInIndex(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "t1", StartNs: 100, EndNs: 200},
		{TraceID: "t2", StartNs: 300, EndNs: 500},
	}

	fields, found := lookupTraceInIndex(entries, "t1")
	if !found {
		t.Fatal("expected found=true")
	}
	if fields["start_time"] != "100" || fields["end_time"] != "200" || fields["duration"] != "100" {
		t.Errorf("unexpected fields: %v", fields)
	}

	_, found = lookupTraceInIndex(entries, "missing")
	if found {
		t.Error("expected found=false for missing trace")
	}
}
