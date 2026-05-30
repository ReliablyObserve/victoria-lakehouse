package parquets3

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const traceIndexMetadataKey = "_trace_idx"
const traceIndexVersion byte = 1

// TraceIndexEntry holds per-trace time range metadata matching VT's
// trace_id_idx_stream fields (start_time, end_time, duration).
type TraceIndexEntry struct {
	TraceID   string
	Partition uint16 // xxhash(traceID) % 1024, matches VT's TraceIDIndexPartitionCount
	StartNs   int64
	EndNs     int64
}

// computeTraceIndex aggregates per-trace time ranges from a batch of spans.
// Each entry represents the min(start_time) and max(end_time) across all
// spans for that trace_id within this batch.
func computeTraceIndex(rows []schema.TraceRow) []TraceIndexEntry {
	type acc struct {
		startNs int64
		endNs   int64
	}
	m := make(map[string]*acc, len(rows)/10)
	for i := range rows {
		tid := rows[i].TraceID
		if tid == "" {
			continue
		}
		start := rows[i].StartTimeUnixNano
		end := start
		if rows[i].DurationNs > 0 {
			end += rows[i].DurationNs
		}
		a, ok := m[tid]
		if !ok {
			m[tid] = &acc{startNs: start, endNs: end}
		} else {
			if start < a.startNs {
				a.startNs = start
			}
			if end > a.endNs {
				a.endNs = end
			}
		}
	}

	entries := make([]TraceIndexEntry, 0, len(m))
	for tid, a := range m {
		entries = append(entries, TraceIndexEntry{
			TraceID:   tid,
			Partition: traceIDPartition(tid),
			StartNs:   a.startNs,
			EndNs:     a.endNs,
		})
	}
	return entries
}

// traceIDPartition computes VT's trace_id_idx_stream partition value.
// Replicates VT's insertutil/index_helper.go: xxhash.Sum64(tb[:]) % 1024
// where tb is a [32]byte copy of the trace ID string.
func traceIDPartition(traceID string) uint16 {
	var tb [32]byte
	copy(tb[:], traceID)
	return uint16(xxhash.Sum64(tb[:]) % 1024)
}

// marshalTraceIndex encodes trace index entries to binary.
// Format: [version:uint8][count:uint32] then per entry:
//
//	[traceIDLen:uint16][traceID:bytes][partition:uint16][startNs:int64][endNs:int64]
func marshalTraceIndex(entries []TraceIndexEntry) []byte {
	size := 1 + 4
	for _, e := range entries {
		size += 2 + len(e.TraceID) + 2 + 8 + 8
	}
	buf := make([]byte, size)
	buf[0] = traceIndexVersion
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(entries)))
	off := 5
	for _, e := range entries {
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(e.TraceID)))
		off += 2
		copy(buf[off:], e.TraceID)
		off += len(e.TraceID)
		binary.LittleEndian.PutUint16(buf[off:off+2], e.Partition)
		off += 2
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(e.StartNs))
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(e.EndNs))
		off += 8
	}
	return buf
}

// unmarshalTraceIndex decodes trace index entries from binary.
func unmarshalTraceIndex(data []byte) ([]TraceIndexEntry, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("trace index too short: %d bytes", len(data))
	}
	if data[0] != traceIndexVersion {
		return nil, fmt.Errorf("unknown trace index version: %d", data[0])
	}
	count := int(binary.LittleEndian.Uint32(data[1:5]))
	entries := make([]TraceIndexEntry, 0, count)
	off := 5
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, fmt.Errorf("trace index truncated at entry %d", i)
		}
		tidLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+tidLen+2+8+8 > len(data) {
			return nil, fmt.Errorf("trace index truncated at entry %d", i)
		}
		tid := string(data[off : off+tidLen])
		off += tidLen
		partition := binary.LittleEndian.Uint16(data[off : off+2])
		off += 2
		startNs := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		endNs := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		entries = append(entries, TraceIndexEntry{
			TraceID:   tid,
			Partition: partition,
			StartNs:   startNs,
			EndNs:     endNs,
		})
	}
	return entries, nil
}

// traceIndexFromMetadata extracts the trace index from Parquet file metadata.
func traceIndexFromMetadata(metadata map[string]string) ([]TraceIndexEntry, bool) {
	raw, ok := metadata[traceIndexMetadataKey]
	if !ok {
		return nil, false
	}
	entries, err := unmarshalTraceIndex([]byte(raw))
	if err != nil {
		return nil, false
	}
	return entries, true
}

// lookupTraceInIndex finds a trace by ID in index entries and returns its
// time range using VT's index field names (start_time, end_time, duration).
func lookupTraceInIndex(entries []TraceIndexEntry, traceID string) (fields map[string]string, found bool) {
	for _, e := range entries {
		if e.TraceID == traceID {
			return map[string]string{
				"start_time_unix_nano": strconv.FormatInt(e.StartNs, 10),
				"end_time_unix_nano":   strconv.FormatInt(e.EndNs, 10),
				"duration":             strconv.FormatInt(e.EndNs-e.StartNs, 10),
			}, true
		}
	}
	return nil, false
}
