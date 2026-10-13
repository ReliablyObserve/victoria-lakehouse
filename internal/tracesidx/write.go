package tracesidx

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"math/bits"
	"slices"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Info describes a written object. The flush and the compaction both fill it from
// the same function, which is what the golden test compares.
type Info struct {
	Rows      int64
	Groups    int
	Distinct  int64 // distinct trace_id_idx values, summed over row groups
	MinTime   int64
	MaxTime   int64
	MinID     string // first trace_id_idx of the object: the fence
	MaxID     string // last trace_id_idx
	BloomSize []int
}

// Sort puts rows in the table's order: by trace id, then time, start, end, stream so
// that equal ids have one deterministic order and the same rows always give the same
// bytes, whichever way they arrived (flush or merge).
func Sort(rows []Row) {
	slices.SortStableFunc(rows, func(a, b Row) int {
		if c := cmp.Compare(a.TraceID, b.TraceID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Time, b.Time); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Start, b.Start); c != 0 {
			return c
		}
		if c := cmp.Compare(a.End, b.End); c != 0 {
			return c
		}
		return cmp.Compare(a.Stream, b.Stream)
	})
}

// ndvFilter is the standard split-block bloom filter sized by the distinct values
// of the row group being written instead of its row count, rounded up to a power of
// two (the filter then reads in every engine, including Arrow C++ and so
// ClickHouse, and a unique-id column wastes nothing). The writer is told the
// distinct ratio of each row group before it writes it.
type ndvFilter struct {
	parquet.BloomFilterColumn
	distinct int64
}

// Size is called by parquet-go with the number of values of the row group.
func (f *ndvFilter) Size(numValues int64) int { return FilterBytes(minInt64(f.distinct, numValues)) }

// FilterBytes is the size of the split-block filter for n distinct values: 10.5
// bits per value, in 32-byte blocks, a power of two blocks.
func FilterBytes(n int64) int {
	if n < 1 {
		n = 1
	}
	blocks := int(float64(n)*BloomBitsPerValue/256) + 1
	return (1 << bits.Len(uint(blocks-1))) * 32
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// writerOptions are the options every writer of the table uses.
func writerOptions(f *ndvFilter, level int) []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(RowsPerGroup),
		parquet.PageBufferSize(PageBytes),
		parquet.ColumnIndexSizeLimit(func([]string) int { return ColumnIndexLimit }),
		parquet.Compression(&zstd.Codec{Level: zstdLevel(level)}),
		parquet.BloomFilters(f),
		parquet.KeyValueMetadata(FormatKey, FormatValue),
		schema.ParquetCreatedBy(),
	}
}

// zstdLevel maps the repo's numeric compression level the way the span writer does.
func zstdLevel(level int) zstd.Level {
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

// Marshal writes rows as one object. The rows are sorted by Marshal (on a copy of
// the slice header: the caller's rows are reordered in place). Flush and
// compaction call this one function, so their objects have the same structure.
func Marshal(rows []Row, compressionLevel int) ([]byte, Info, error) {
	Sort(rows)
	f := &ndvFilter{BloomFilterColumn: parquet.SplitBlockFilter(10, IDField), distinct: 1}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[Row](&buf, writerOptions(f, compressionLevel)...)
	info := Info{Rows: int64(len(rows))}
	if len(rows) > 0 {
		info.MinID, info.MaxID, info.MinTime, info.MaxTime = rows[0].TraceID, rows[len(rows)-1].TraceID, rows[0].Time, rows[0].Time
	}
	for i := range rows {
		info.MinTime = min(info.MinTime, rows[i].Time)
		info.MaxTime = max(info.MaxTime, rows[i].Time)
	}
	for lo := 0; lo < len(rows); lo += RowsPerGroup {
		hi := min(lo+RowsPerGroup, len(rows))
		d := distinctIDs(rows[lo:hi])
		f.distinct = d
		info.Distinct += d
		if _, err := w.Write(rows[lo:hi]); err != nil {
			return nil, Info{}, fmt.Errorf("tracesidx: write rows: %w", err)
		}
		if err := w.Flush(); err != nil {
			return nil, Info{}, fmt.Errorf("tracesidx: flush row group: %w", err)
		}
		info.Groups++
		info.BloomSize = append(info.BloomSize, FilterBytes(d))
	}
	if err := w.Close(); err != nil {
		return nil, Info{}, fmt.Errorf("tracesidx: close: %w", err)
	}
	return buf.Bytes(), info, nil
}

// distinctIDs counts the distinct trace ids of rows sorted by id.
func distinctIDs(rows []Row) int64 {
	var n int64
	for i := range rows {
		if i == 0 || rows[i].TraceID != rows[i-1].TraceID {
			n++
		}
	}
	return n
}

// ReadAll decodes every row of an object (compaction and tests; a lookup never does).
func ReadAll(data []byte) ([]Row, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("tracesidx: open: %w", err)
	}
	r := parquet.NewGenericReader[Row](f)
	defer r.Close()
	out := make([]Row, 0, f.NumRows())
	buf := make([]Row, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tracesidx: read: %w", err)
		}
	}
	for i := range out { // detach strings from the reader's buffers
		out[i].TraceID = string(append([]byte(nil), out[i].TraceID...))
	}
	return out, nil
}
