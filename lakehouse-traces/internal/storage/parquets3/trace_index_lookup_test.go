package parquets3

import (
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

func TestFindTraceIDInFooterMeta_Hit(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "trace-target", Partition: 7, StartNs: 1_000_000_000, EndNs: 2_000_000_000},
		{TraceID: "trace-other", Partition: 9, StartNs: 500_000_000, EndNs: 1_500_000_000},
	}
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: string(marshalTraceIndex(entries))},
		},
	}

	got, ok, err := findTraceIDInFooterMeta(meta, "trace-target")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.TraceID != "trace-target" || got.StartNs != 1_000_000_000 || got.EndNs != 2_000_000_000 {
		t.Errorf("got %+v; want {TraceID:trace-target StartNs:1e9 EndNs:2e9}", got)
	}
}

func TestFindTraceIDInFooterMeta_NotInFile(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "trace-other", Partition: 9, StartNs: 500, EndNs: 1500},
	}
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: string(marshalTraceIndex(entries))},
		},
	}
	_, ok, err := findTraceIDInFooterMeta(meta, "trace-missing")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false for trace not in file")
	}
}

func TestFindTraceIDInFooterMeta_NoIndexMetadata(t *testing.T) {
	// File predates the embedded index (or compaction dropped it):
	// lookup must report a clean miss, not an error.
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: "some_other_key", Value: "irrelevant"},
		},
	}
	_, ok, err := findTraceIDInFooterMeta(meta, "trace-anything")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Error("expected ok=false when _trace_idx metadata is absent")
	}
}

func TestFindTraceIDInFooterMeta_NilMeta(t *testing.T) {
	_, ok, err := findTraceIDInFooterMeta(nil, "x")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Error("expected ok=false for nil meta")
	}
}

func TestFindTraceIDInFooterMeta_CorruptedIndex(t *testing.T) {
	// Garbage payload — unmarshalTraceIndex fails. We must return a miss,
	// not propagate the parse error to the caller, so a single corrupted
	// file can't break a trace-by-ID lookup that other files could answer.
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: "not-a-valid-index"},
		},
	}
	_, ok, err := findTraceIDInFooterMeta(meta, "anything")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Error("expected ok=false for corrupted index payload")
	}
}
