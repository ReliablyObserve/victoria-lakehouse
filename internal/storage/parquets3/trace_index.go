package parquets3

import (
	"strconv"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// The trace-index codec lives in internal/traceindex, shared with the
// traces module and the compactor (one codec, one bounded decoder). The
// `_trace_idx` value is untrusted bucket data, so decoding must go through
// traceindex.Unmarshal, which bounds the declared entry count (issue #745).
// The aliases below keep this package's call sites and tests unchanged.

const traceIndexMetadataKey = traceindex.MetadataKey

// TraceIndexEntry mirrors the public traceindex.Entry.
type TraceIndexEntry = traceindex.Entry

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

// lookupTraceInIndex finds a trace by ID in index entries and returns its
// time range using VT's index field names (start_time, end_time, duration).
func lookupTraceInIndex(entries []TraceIndexEntry, traceID string) (fields map[string]string, found bool) {
	for _, e := range entries {
		if e.TraceID == traceID {
			return map[string]string{
				"start_time": strconv.FormatInt(e.StartNs, 10),
				"end_time":   strconv.FormatInt(e.EndNs, 10),
				"duration":   strconv.FormatInt(e.EndNs-e.StartNs, 10),
			}, true
		}
	}
	return nil, false
}
