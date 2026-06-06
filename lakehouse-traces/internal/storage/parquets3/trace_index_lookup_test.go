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

// TestTraceIdxKeepFile_NoMetaOrEmpty pins the conservative fallback:
// a file with no metadata at all (or no KV entries) MUST be kept so
// older parquets that pre-date the _trace_idx feature aren't silently
// dropped from a trace-by-ID lookup.
func TestTraceIdxKeepFile_NoMetaOrEmpty(t *testing.T) {
	tidSet := map[string]bool{"any": true}
	if !traceIdxKeepFile(nil, tidSet) {
		t.Error("nil meta must be kept (conservative)")
	}
	if !traceIdxKeepFile(&format.FileMetaData{}, tidSet) {
		t.Error("empty KV must be kept (conservative)")
	}
}

// TestTraceIdxKeepFile_OtherKVsOnly pins that the presence of any
// non-_trace_idx KV entries doesn't change the verdict — a file
// that carries unrelated footer metadata but no _trace_idx still
// has to be kept so the index feature is opt-in for callers and
// older writers stay queryable.
func TestTraceIdxKeepFile_OtherKVsOnly(t *testing.T) {
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: "schema_fingerprint", Value: "abc123"},
			{Key: "writer_version", Value: "1.4.2"},
		},
	}
	if !traceIdxKeepFile(meta, map[string]bool{"any": true}) {
		t.Error("file with KVs but no _trace_idx must be kept (conservative)")
	}
}

// TestTraceIdxKeepFile_IndexHit pins the hit case: file's _trace_idx
// lists at least one of the queried trace IDs → keep.
func TestTraceIdxKeepFile_IndexHit(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "trace-a", Partition: 1, StartNs: 1, EndNs: 2},
		{TraceID: "trace-b", Partition: 1, StartNs: 3, EndNs: 4},
	}
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: string(marshalTraceIndex(entries))},
		},
	}
	if !traceIdxKeepFile(meta, map[string]bool{"trace-b": true}) {
		t.Error("file's _trace_idx contains queried trace id — must keep")
	}
	if !traceIdxKeepFile(meta, map[string]bool{"trace-a": true, "trace-z": true}) {
		t.Error("any one queried tid matching must keep")
	}
}

// TestTraceIdxKeepFile_IndexMiss is the load-bearing case: the file
// has a clean _trace_idx that lists none of the queried trace IDs,
// so we DROP it. This is the only path that actually narrows the
// candidate set — every other branch errs on the side of preserving.
// Regressing this back to "keep" would re-introduce the 30s Jaeger
// timeout on non-existent trace IDs.
func TestTraceIdxKeepFile_IndexMiss(t *testing.T) {
	entries := []TraceIndexEntry{
		{TraceID: "trace-a", Partition: 1, StartNs: 1, EndNs: 2},
		{TraceID: "trace-b", Partition: 1, StartNs: 3, EndNs: 4},
	}
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: string(marshalTraceIndex(entries))},
		},
	}
	if traceIdxKeepFile(meta, map[string]bool{"trace-z": true}) {
		t.Error("file's _trace_idx misses the queried trace id — must drop")
	}
	if traceIdxKeepFile(meta, map[string]bool{"trace-x": true, "trace-y": true}) {
		t.Error("multi-tid query with no matches — must drop")
	}
}

// TestTraceIdxKeepFile_CorruptedIndex pins the defensive parse:
// a corrupted/garbage _trace_idx value must NOT be allowed to drop
// the file silently. A bad payload means we don't know — keep so
// the bloom-positive path still gets to try.
func TestTraceIdxKeepFile_CorruptedIndex(t *testing.T) {
	meta := &format.FileMetaData{
		KeyValueMetadata: []format.KeyValue{
			{Key: traceIndexMetadataKey, Value: "garbage-not-an-index"},
		},
	}
	if !traceIdxKeepFile(meta, map[string]bool{"trace-a": true}) {
		t.Error("corrupted _trace_idx must be treated as keep (defensive)")
	}
}
