package traceindex

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestPartition_Deterministic(t *testing.T) {
	// Same input must always produce the same bucket, and the bucket
	// must be in range [0, PartitionCount).
	tid := "0123456789abcdef0123456789abcdef"
	p1 := Partition(tid)
	p2 := Partition(tid)
	if p1 != p2 {
		t.Fatalf("Partition() not deterministic: %d != %d", p1, p2)
	}
	if uint64(p1) >= PartitionCount {
		t.Errorf("Partition()=%d out of range [0,%d)", p1, PartitionCount)
	}
}

func TestPartition_DifferentInputs(t *testing.T) {
	// Different trace IDs typically produce different buckets; we don't
	// assert that strictly (collisions exist over 1024 buckets) but we
	// do check the implementation handles short/long inputs without
	// panicking and that empty input is well-defined.
	for _, tid := range []string{"", "a", "short", "0123456789abcdef0123456789abcdef", "x"} {
		_ = Partition(tid)
	}
}

func TestCompute_AggregatesByTraceID(t *testing.T) {
	rows := []schema.TraceRow{
		{TraceID: "trace-a", StartTimeUnixNano: 1000, DurationNs: 100}, // ends at 1100
		{TraceID: "trace-a", StartTimeUnixNano: 900, DurationNs: 200},  // ends at 1100
		{TraceID: "trace-b", StartTimeUnixNano: 2000, DurationNs: 50},  // ends at 2050
		{TraceID: "", StartTimeUnixNano: 3000, DurationNs: 10},         // skipped: no trace_id
	}
	entries := Compute(rows)
	if len(entries) != 2 {
		t.Fatalf("expected 2 distinct traces, got %d", len(entries))
	}
	by := map[string]Entry{}
	for _, e := range entries {
		by[e.TraceID] = e
	}
	if a, ok := by["trace-a"]; !ok {
		t.Error("trace-a missing")
	} else if a.StartNs != 900 || a.EndNs != 1100 {
		t.Errorf("trace-a got {%d,%d}, want {900,1100}", a.StartNs, a.EndNs)
	}
	if b, ok := by["trace-b"]; !ok {
		t.Error("trace-b missing")
	} else if b.StartNs != 2000 || b.EndNs != 2050 {
		t.Errorf("trace-b got {%d,%d}, want {2000,2050}", b.StartNs, b.EndNs)
	}
}

func TestCompute_PartitionMatchesPartition(t *testing.T) {
	rows := []schema.TraceRow{{TraceID: "deadbeef", StartTimeUnixNano: 5, DurationNs: 1}}
	entries := Compute(rows)
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Partition != Partition("deadbeef") {
		t.Errorf("Compute partition %d != Partition() %d", entries[0].Partition, Partition("deadbeef"))
	}
}

func TestCompute_EmptyInput(t *testing.T) {
	if got := Compute(nil); len(got) != 0 {
		t.Errorf("Compute(nil) should be empty, got %d", len(got))
	}
	if got := Compute([]schema.TraceRow{}); len(got) != 0 {
		t.Errorf("Compute(empty) should be empty, got %d", len(got))
	}
}

func TestCompute_DurationZero(t *testing.T) {
	// DurationNs <= 0: end equals start, single-point trace.
	rows := []schema.TraceRow{{TraceID: "t", StartTimeUnixNano: 100, DurationNs: 0}}
	entries := Compute(rows)
	if len(entries) != 1 || entries[0].StartNs != 100 || entries[0].EndNs != 100 {
		t.Errorf("zero duration: got %+v", entries)
	}
}

func TestMarshalUnmarshal_Roundtrip(t *testing.T) {
	want := []Entry{
		{TraceID: "trace-1", Partition: 42, StartNs: 1_000_000, EndNs: 2_000_000},
		{TraceID: "trace-2", Partition: 99, StartNs: 3_000_000, EndNs: 4_000_000},
		{TraceID: "trace-3-with-a-longer-id-than-typical", Partition: 7, StartNs: 5, EndNs: 6},
	}
	got, err := Unmarshal(Marshal(want))
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("entry count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestMarshalUnmarshal_Empty(t *testing.T) {
	got, err := Unmarshal(Marshal(nil))
	if err != nil {
		t.Fatalf("Unmarshal(Marshal(nil)): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 entries, got %d", len(got))
	}
}

func TestUnmarshal_ErrorsOnTruncated(t *testing.T) {
	// Empty bytes — even the version+count header is missing.
	if _, err := Unmarshal(nil); err == nil {
		t.Error("expected error for nil input")
	}
	if _, err := Unmarshal([]byte{1, 2}); err == nil {
		t.Error("expected error for short input")
	}
}

func TestUnmarshal_ErrorsOnUnknownVersion(t *testing.T) {
	// 5-byte payload with version=99 (we only know Version=1).
	if _, err := Unmarshal([]byte{99, 0, 0, 0, 0}); err == nil {
		t.Error("expected error for unknown version")
	}
}

func TestUnmarshal_ErrorsOnTruncatedEntry(t *testing.T) {
	// Header claims 1 entry but the entry payload is missing.
	if _, err := Unmarshal([]byte{Version, 1, 0, 0, 0}); err == nil {
		t.Error("expected truncated-entry error")
	}
	// Header claims 1 entry, has the tidLen prefix, but no remaining bytes.
	if _, err := Unmarshal([]byte{Version, 1, 0, 0, 0, 5, 0}); err == nil {
		t.Error("expected truncated-payload error")
	}
}

func TestFromMetadata_HitAndMiss(t *testing.T) {
	entries := []Entry{{TraceID: "t1", Partition: 10, StartNs: 100, EndNs: 200}}
	meta := map[string]string{MetadataKey: string(Marshal(entries))}

	got, ok := FromMetadata(meta)
	if !ok || len(got) != 1 || got[0].TraceID != "t1" {
		t.Errorf("hit case: ok=%v entries=%+v", ok, got)
	}

	// Missing key.
	if _, ok := FromMetadata(map[string]string{"other_key": "x"}); ok {
		t.Error("missing-key case should return ok=false")
	}

	// Empty map.
	if _, ok := FromMetadata(map[string]string{}); ok {
		t.Error("empty-map case should return ok=false")
	}

	// Nil map.
	if _, ok := FromMetadata(nil); ok {
		t.Error("nil-map case should return ok=false")
	}
}

func TestFromMetadata_CorruptedPayloadIsMiss(t *testing.T) {
	// Caller contract: a corrupted blob is treated as "no index" rather
	// than an error so one bad file doesn't break a lookup other files
	// could answer.
	meta := map[string]string{MetadataKey: "not-a-valid-marshal-payload"}
	if _, ok := FromMetadata(meta); ok {
		t.Error("corrupted payload should return ok=false")
	}
}

func TestLookupFields_Hit(t *testing.T) {
	entries := []Entry{
		{TraceID: "t1", StartNs: 100, EndNs: 200},
		{TraceID: "t2", StartNs: 300, EndNs: 500},
	}
	fields, found := LookupFields(entries, "t1")
	if !found {
		t.Fatal("expected found=true for t1")
	}
	if fields["start_time_unix_nano"] != "100" {
		t.Errorf("start_time_unix_nano = %q, want 100", fields["start_time_unix_nano"])
	}
	if fields["end_time_unix_nano"] != "200" {
		t.Errorf("end_time_unix_nano = %q, want 200", fields["end_time_unix_nano"])
	}
	if fields["duration"] != "100" {
		t.Errorf("duration = %q, want 100", fields["duration"])
	}
}

func TestLookupFields_Miss(t *testing.T) {
	entries := []Entry{{TraceID: "t1", StartNs: 100, EndNs: 200}}
	if _, found := LookupFields(entries, "missing"); found {
		t.Error("expected found=false for missing trace_id")
	}
	if _, found := LookupFields(nil, "anything"); found {
		t.Error("expected found=false for nil entries")
	}
}
