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
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/cespare/xxhash/v2"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Limits of the wire format. Marshal and Unmarshal share them, so the
// writer never emits an index the decoder would refuse or, worse, decode
// into something else (issue #768).
const (
	// MaxTraceIDLen is the longest trace ID the uint16 length prefix can
	// carry. VictoriaTraces itself puts no cap on a trace ID (it hashes the
	// first 32 bytes for the index stream), so a longer ID is legal input
	// that the footer index simply cannot describe.
	MaxTraceIDLen = math.MaxUint16

	// MaxEntries bounds the entry count of one index: the decoder refuses
	// anything above it (the footer value is untrusted bucket data, #745).
	MaxEntries = 1 << 20
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

// minEntrySize is the smallest encoded entry: an empty trace ID
// (traceIDLen:2 + partition:2 + startNs:8 + endNs:8).
const minEntrySize = 2 + 2 + 8 + 8

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
//
// Entries come back sorted by trace ID, so the marshaled `_trace_idx`
// footer value — and with it the whole Parquet file — is byte-identical
// for the same rows on every run (Go map iteration order is random).
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
		start := schema.Int64Value(rows[i].StartTimeUnixNano)
		end := start
		if d := schema.Int64Value(rows[i].DurationNs); d > 0 {
			end += d
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
	slices.SortFunc(entries, func(a, b Entry) int { return strings.Compare(a.TraceID, b.TraceID) })
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

// Indexable reports whether a trace ID can be listed in the index: its
// length must fit the uint16 prefix. Readers use it on the QUERIED id: an id
// the writer cannot list is never in any index, so its absence from an index
// proves nothing and the file must be scanned.
func Indexable(traceID string) bool { return len(traceID) <= MaxTraceIDLen }

// Listed returns how many of the entries Marshal lists (those with an
// Indexable ID).
func Listed(entries []Entry) int {
	n := 0
	for _, e := range entries {
		if Indexable(e.TraceID) {
			n++
		}
	}
	return n
}

// Marshal encodes index entries to binary.
//
// The index lists every non-empty trace ID of at most MaxTraceIDLen bytes in
// the file (Compute never produces an entry for the empty ID), and no other: an ID the uint16 length prefix cannot describe is left
// out (its length would wrap and the payload would decode into other
// entries, #768), and the rest are still listed. A reader may therefore
// conclude that an Indexable ID absent from the index is absent from the
// file; for an ID that is not Indexable it must scan the file.
//
// It returns nil, and writes no index, only when more than MaxEntries IDs
// would be listed (the decoder refuses such a payload, and a prefix of the
// IDs would break the rule above). Callers treat an empty result as "no index
// for this file". Omissions are counted in
// lakehouse_writer_trace_idx_omitted_total{reason} (ids over the length limit
// are counted per id, a file over the entry cap once) and logged.
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
	kept, tooLong, longest := 0, 0, 0
	for _, e := range entries {
		if !Indexable(e.TraceID) {
			tooLong++
			longest = max(longest, len(e.TraceID))
			continue
		}
		kept++
		size += 2 + len(e.TraceID) + 2 + 8 + 8
	}
	if tooLong > 0 {
		metrics.WriterTraceIdxOmittedTotal.Add("id_over_limit", tooLong)
		logger.Warnf("trace index: left out %d trace ID(s) longer than %d bytes (longest %d); trace-by-ID scans this file for such an ID", tooLong, MaxTraceIDLen, longest)
	}
	if kept > MaxEntries {
		metrics.WriterTraceIdxOmittedTotal.Inc("entries_over_cap")
		logger.Warnf("trace index omitted: %d entries exceed the limit of %d; trace-by-ID uses a span scan for this file", kept, MaxEntries)
		return nil
	}
	buf := make([]byte, size)
	buf[0] = Version
	binary.LittleEndian.PutUint32(buf[1:5], uint32(kept))
	off := 5
	for _, e := range entries {
		if !Indexable(e.TraceID) {
			continue
		}
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

// Unmarshal decodes a Marshal payload. Returns an error on truncation,
// trailing bytes or an unknown version, never on a missing trace ID —
// callers must check the returned slice's length. The payload must be
// consumed exactly: a payload that decodes with bytes left over was not
// produced by Marshal (an index written by an older release with a wrapped
// ID length is one) and is refused, so the caller falls back to a span scan
// instead of trusting garbage entries (#768).
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
	if count > MaxEntries {
		return nil, fmt.Errorf("trace index count too large: %d (max %d)", count, MaxEntries)
	}
	// The cap above still lets a 5-byte value ask for ~42 MB (1<<20
	// entries x 40 B). Bound the declared count by what the remaining
	// bytes can hold: every entry takes at least minEntrySize bytes, so a
	// count that cannot fit is truncated input and is rejected before
	// anything is allocated. Valid payloads always satisfy this.
	if remaining := len(data) - 5; count > remaining/minEntrySize {
		return nil, fmt.Errorf("trace index count %d exceeds payload: %d bytes hold at most %d entries", count, remaining, remaining/minEntrySize)
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
	if off != len(data) {
		return nil, fmt.Errorf("trace index has %d trailing bytes after %d entries", len(data)-off, count)
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
