// Package parquetwriter is the ONE encoder of Lakehouse Parquet objects. The
// flush writer (internal/storage/parquets3, both binaries) and the compactor
// (internal/compaction, and every other producer of a replacement object) call
// WriteLogs / WriteTraces with explicit Options, so a file carries the same
// pruning structures whichever path wrote it: the writer options, the Parquet
// split-block blooms, the footer key-value entries (body token blooms, Tier-2
// slot mapping, trace index, lh.trace_id_hex attestation), the statistics and
// the page index. MAIN RULE 4 ("compaction never reduces prunability") holds by
// construction: there is no second place to forget a structure in (#344).
//
// The package is a pure encode library, by contract:
//
//   - no package-level mutable state: everything the output depends on (the
//     Tier-2 slot resolver, the row-group size, the compression level, the
//     lh.trace_id_hex attestation) is a field of Options, so two callers with
//     different settings cannot influence each other;
//   - deterministic: the same rows and Options produce the same bytes;
//   - no I/O, no goroutines, no recover: a panic reaches the caller, and the
//     caller that must not crash on one (compaction) recovers at its own
//     boundary;
//   - the only side effects are the diagnostics of the shared trace-index codec
//     (internal/traceindex), a log line and a counter when an index has to be
//     left out, which compaction already produced before this package existed.
//
// internal/parquetwriter/structcheck decodes a written footer into a comparable
// description, the golden tests of flush versus compacted structure use it.
package parquetwriter

import (
	"bytes"
	"fmt"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// Options is everything the encoding depends on. There is no default and no
// other input: a field left at its zero value means that zero value.
type Options struct {
	// RowGroupSize is the number of rows per row group (> 0). The body token
	// blooms are keyed by row-group index, so the writer cuts row groups by row
	// count only.
	RowGroupSize int
	// CompressionLevel is the Lakehouse level (1..22), mapped by ZstdLevel.
	CompressionLevel int
	// Slots is the Tier-2 slot binding: it adds the slot columns that are
	// configured for a bloom to the Parquet blooms and records the slot to name
	// mapping in the footer. nil = no custom promotions.
	Slots *schema.SlotResolver
	// TraceIDHex is the lh.trace_id_hex attestation recorded in the footer. The
	// flush writer derives it from the rows it writes, a merge from its inputs
	// (schema.CompactedTraceIDHex), a delete rewrite from the rows it keeps; the
	// caller decides, the writer records it.
	TraceIDHex bool
}

// Result is a written object.
type Result struct {
	Data []byte
	// TraceIndex describes the `_trace_idx` footer entry of a traces object; nil
	// when no entry was written (logs, or no listable trace id).
	TraceIndex *TraceIndexInfo
}

// TraceIndexInfo reports the trace index that went into a traces object, for the
// caller's metrics. The writer itself records nothing.
type TraceIndexInfo struct {
	// Listed is the number of entries the index lists (traceindex.Listed): trace
	// ids too long to index are not counted.
	Listed int
	// Build is how long computing and marshalling the index took. It is a
	// measurement for the caller, not an input of the output bytes.
	Build time.Duration
}

// ZstdLevel maps a Lakehouse compression level to a zstd speed.
func ZstdLevel(level int) zstd.Level {
	switch {
	case level <= 1:
		return zstd.SpeedFastest
	case level <= 5:
		return zstd.SpeedDefault
	case level <= 10:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedBestCompression
	}
}

// BloomFilters builds the split-block filter columns (10 bits/value, about 1 %
// false positives) from the strict per-signal bloom set in internal/schema
// (cardinality-aligned: high-cardinality, equality-queried columns only).
func BloomFilters(cols []string) []parquet.BloomFilterColumn {
	bf := make([]parquet.BloomFilterColumn, 0, len(cols))
	for _, c := range cols {
		bf = append(bf, parquet.SplitBlockFilter(10, c))
	}
	return bf
}

// bloomValue and traceIndexValue turn a footer key-value payload into the string
// Parquet stores. Today both store the bytes as they are, which is not valid
// UTF-8 (#340). The two payloads need different encodings (the token bloom base64,
// `_trace_idx` base64 of a zstd frame via traceindex.MetadataValue), so each has
// its own function, and the slot mapping (JSON, already UTF-8) has none.
func bloomValue(raw []byte) string { return string(raw) }

func traceIndexValue(raw []byte) string { return string(raw) }

// WriteLogs encodes rows as a logs object.
func WriteLogs(rows []schema.LogRow, o Options) (Result, error) {
	if o.RowGroupSize <= 0 {
		return Result{}, fmt.Errorf("parquetwriter: RowGroupSize must be positive, got %d", o.RowGroupSize)
	}
	opts := commonOptions(o, schema.LogBloomColumns(o.Slots.BloomSlots()...))
	opts = append(opts, tokenBloomOptions(len(rows), o.RowGroupSize, func(i int) string { return rows[i].Body })...)

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf, opts...)
	if _, err := writer.Write(rows); err != nil {
		return Result{}, err
	}
	if err := writer.Close(); err != nil {
		return Result{}, err
	}
	return Result{Data: buf.Bytes()}, nil
}

// WriteTraces encodes spans as a traces object. Besides what a logs object has,
// it carries the `_trace_idx` footer entry, recomputed from the rows written, so
// trace-by-ID keeps its fast path on every object whichever path wrote it.
func WriteTraces(rows []schema.TraceRow, o Options) (Result, error) {
	if o.RowGroupSize <= 0 {
		return Result{}, fmt.Errorf("parquetwriter: RowGroupSize must be positive, got %d", o.RowGroupSize)
	}
	opts := commonOptions(o, schema.TraceBloomColumns(o.Slots.BloomSlots()...))
	opts = append(opts, tokenBloomOptions(len(rows), o.RowGroupSize, func(i int) string { return rows[i].Body })...)

	var info *TraceIndexInfo
	start := time.Now()
	entries := traceindex.Compute(rows)
	if idx := traceindex.Marshal(entries); len(idx) > 0 {
		opts = append(opts, parquet.KeyValueMetadata(traceindex.MetadataKey, traceIndexValue(idx)))
		info = &TraceIndexInfo{Listed: traceindex.Listed(entries), Build: time.Since(start)}
	}

	var buf bytes.Buffer
	writer := schema.NewTraceWriter(&buf, opts...)
	if _, err := writer.Write(rows); err != nil {
		return Result{}, err
	}
	if err := writer.Close(); err != nil {
		return Result{}, err
	}
	return Result{Data: buf.Bytes(), TraceIndex: info}, nil
}

// commonOptions are the writer options both signals share, in the order the
// flush writer has always applied them (the footer key-value entries are written
// in this order, so it is part of the byte-identity contract).
func commonOptions(o Options, bloomColumns []string) []parquet.WriterOption {
	opts := []parquet.WriterOption{
		parquet.Compression(&zstd.Codec{Level: ZstdLevel(o.CompressionLevel)}),
		parquet.MaxRowsPerRowGroup(int64(o.RowGroupSize)),
		schema.ParquetCreatedBy(),
		parquet.BloomFilters(BloomFilters(bloomColumns)...),
		schema.TraceIDHexOption(o.TraceIDHex),
	}
	// Tier-2: record the slot to name binding in the footer so the file is
	// self-describing: read-back remaps ded_sNN to the configured attribute name
	// by ITS OWN footer, correct even if the config later changes.
	if kv := schema.MarshalSlotMapping(o.Slots.Mapping()); kv != nil {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}
	return opts
}

// tokenBloomOptions builds one body token bloom per row group (footer key
// `_bloom_body_rg_<N>`); a row group with no non-empty body gets none.
func tokenBloomOptions(n, rowGroupSize int, body func(i int) string) []parquet.WriterOption {
	var opts []parquet.WriterOption
	for rg := 0; rg*rowGroupSize < n; rg++ {
		start := rg * rowGroupSize
		end := min(start+rowGroupSize, n)
		bodies := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			if b := body(i); b != "" {
				bodies = append(bodies, b)
			}
		}
		if len(bodies) > 0 {
			key, value := BuildTokenBloomMetadata(bodies, rg)
			opts = append(opts, parquet.KeyValueMetadata(key, bloomValue(value)))
		}
	}
	return opts
}
