// Package traceindex implements the per-file trace-ID time-bound index
// embedded in lakehouse trace Parquet files' standard FileMetaData
// key_value_metadata footer slot.
//
// Wire format mirrors VictoriaTraces' trace_id_idx_stream semantics:
// each entry carries (trace_id, partition, start_ns, end_ns) where
// partition = xxhash64(traceID[:32]) % 1024 — identical to upstream's
// vtinsert/insertutil/index_helper.go so the per-trace partition value
// LH stores matches what VT computes during query planning.
//
// The index is read on the cold-tier trace-by-ID path so VT's first-stage
// time-bound lookup (vtselect/traces/tempo/query.go) can be served
// without scanning span data. Compaction must preserve it across file
// merges; the writer and the compactor share this package so the codec
// and the metadata key stay in lock-step.
package traceindex

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// MetadataKey is the Parquet FileMetaData.key_value_metadata key under
// which we store the marshaled trace-ID index.
//
// `_` prefix marks it as an LH-internal extension; standard Parquet
// readers (duckdb, pyarrow, parquet-tools) see it as an opaque KV pair
// and pass over it — the file remains 100% spec-compliant.
const MetadataKey = "_trace_idx"

// Version identifies the marshal format; bumped on any wire change.
const Version byte = 1

// PartitionCount mirrors VT's TraceIDIndexPartitionCount (otelpb).
// Defined here to avoid importing the VT module from compaction.
const PartitionCount = uint64(1024)

// Entry is a single trace's index record.
type Entry struct {
	TraceID   string
	Partition uint16 // xxhash(traceID) % PartitionCount, matches VT
	StartNs   int64
	EndNs     int64
}

// Compute aggregates per-trace time ranges from a batch of spans.
// Each entry represents the min(start_time) and max(end_time) across
// all spans for that trace_id within the batch. Spans without a
// trace_id (e.g. degenerate rows that escaped the insert-side filter)
// are skipped.
func Compute(rows []schema.TraceRow) []Entry {
	type acc struct {
		startNs int64
		endNs   int64
	}
	m := make(map[string]*acc, len(rows)/10+1)
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
			continue
		}
		if start < a.startNs {
			a.startNs = start
		}
		if end > a.endNs {
			a.endNs = end
		}
	}

	entries := make([]Entry, 0, len(m))
	for tid, a := range m {
		entries = append(entries, Entry{
			TraceID:   tid,
			Partition: Partition(tid),
			StartNs:   a.startNs,
			EndNs:     a.endNs,
		})
	}
	return entries
}

// Partition replicates VT's vtinsert/insertutil/index_helper.go:
// xxhash.Sum64(tb[:]) % PartitionCount where tb is a [32]byte copy of
// the trace ID. Two different LH instances must produce the same
// partition for the same trace ID; this function is the single source
// of truth.
func Partition(traceID string) uint16 {
	var tb [32]byte
	copy(tb[:], traceID)
	return uint16(xxhash.Sum64(tb[:]) % PartitionCount)
}

// Marshal encodes index entries to binary.
//
// Wire format:
//
//	[version:uint8][count:uint32]
//	  then per entry:
//	[traceIDLen:uint16][traceID:bytes][partition:uint16][startNs:int64][endNs:int64]
//
// Little-endian. Stable across releases so long as Version is honored.
func Marshal(entries []Entry) []byte {
	size := 1 + 4
	for _, e := range entries {
		size += 2 + len(e.TraceID) + 2 + 8 + 8
	}
	buf := make([]byte, size)
	buf[0] = Version
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

// Unmarshal decodes a Marshal payload. Returns an error on truncation
// or an unknown version, never on a missing trace ID — callers must
// check the returned slice's length.
func Unmarshal(data []byte) ([]Entry, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("trace index too short: %d bytes", len(data))
	}
	if data[0] != Version {
		return nil, fmt.Errorf("unknown trace index version: %d", data[0])
	}
	count := int(binary.LittleEndian.Uint32(data[1:5]))
	// Defensive cap on the preallocation. count is attacker-controlled
	// (it comes from the footer KV which any parquet writer could
	// produce, including a corrupted or hostile one). Without this
	// guard, a flipped count MSB asks for ~2.1 B entries and
	// `make([]Entry, 0, count)` triggers a multi-GB allocation —
	// either a runtime panic or an OOM-kill on the daemon, neither
	// acceptable on the hot query path. 1<<20 = ~1M entries is well
	// above any realistic _trace_idx footer (a single parquet
	// partition typically lists 10 k–100 k traces) while bounding
	// the worst-case allocation to ~32 MB.
	const maxEntries = 1 << 20
	if count > maxEntries {
		return nil, fmt.Errorf("trace index count too large: %d (max %d)", count, maxEntries)
	}
	entries := make([]Entry, 0, count)
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
		entries = append(entries, Entry{
			TraceID:   tid,
			Partition: partition,
			StartNs:   startNs,
			EndNs:     endNs,
		})
	}
	return entries, nil
}

// FromMetadata pulls and decodes an index out of a Parquet KV metadata
// map. Returns ok=false on absence or any decode failure (corrupted
// payload is silently treated as "no index" rather than an error: a
// single bad file must not break a lookup other files can answer).
func FromMetadata(metadata map[string]string) ([]Entry, bool) {
	raw, ok := metadata[MetadataKey]
	if !ok {
		return nil, false
	}
	entries, err := Unmarshal([]byte(raw))
	if err != nil {
		return nil, false
	}
	return entries, true
}

// LookupFields returns the VT-compatible field strings for a trace ID.
// Used by the trace-by-ID handler to construct a synthetic stats result
// that matches VT's index-stream response shape.
func LookupFields(entries []Entry, traceID string) (fields map[string]string, found bool) {
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
