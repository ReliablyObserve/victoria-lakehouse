package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// This file used to define the trace-index codec inline. It has been
// hoisted to internal/traceindex so the root-module compactor can write
// the same `_trace_idx` KV metadata on compacted trace Parquet files
// without an import cycle through the lakehouse-traces module.
//
// The aliases below keep this package's call sites and tests unchanged
// while routing every operation through the single shared implementation.

const traceIndexMetadataKey = traceindex.MetadataKey

// TraceIndexEntry mirrors the public traceindex.Entry. Aliased so the
// existing parquets3 tests and helpers don't need to change shape.
type TraceIndexEntry = traceindex.Entry

// traceIDIndexable reports whether the index can say a queried id is absent
// from a file. It cannot for an id longer than the uint16 length limit (the
// writer never lists one) and not for the empty id: spans without a trace_id
// are stored, but traceindex.Compute skips them, so the index lists no empty
// id. For both the file must be scanned (#768).
func traceIDIndexable(id string) bool { return id != "" && traceindex.Indexable(id) }

// listedTraceIndexEntries is the number of entries marshalTraceIndex lists:
// the over-long IDs it leaves out are not counted (#768).
func listedTraceIndexEntries(entries []TraceIndexEntry) int { return traceindex.Listed(entries) }

func computeTraceIndex(rows []schema.TraceRow) []TraceIndexEntry {
	return traceindex.Compute(rows)
}

func traceIDPartition(traceID string) uint16 {
	return traceindex.Partition(traceID)
}

func marshalTraceIndex(entries []TraceIndexEntry) []byte {
	return traceindex.Marshal(entries)
}

func unmarshalTraceIndex(data []byte) ([]TraceIndexEntry, error) {
	return traceindex.Unmarshal(data)
}

func traceIndexFromMetadata(metadata map[string]string) ([]TraceIndexEntry, bool) {
	return traceindex.FromMetadata(metadata)
}

func lookupTraceInIndex(entries []TraceIndexEntry, traceID string) (map[string]string, bool) {
	return traceindex.LookupFields(entries, traceID)
}
