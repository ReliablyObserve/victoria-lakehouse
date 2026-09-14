package parquets3

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Regression cover for the three parquet-go writer defects fixed in v0.32.0,
// each exercised through the LH writer shape (zstd, the production row-group
// size, `,dict` columns, split-block row-group blooms) rather than a
// hand-rolled minimal schema — a fix that holds for a two-column toy file and
// not for a 48-column file with maps is not a fix we can rely on.
//
//   - #577 later row groups overwrote earlier row groups' page counts
//   - #578 bloom filters were mis-sized when a row group hit the row limit
//   - #536 the dictionary fallback dropped values once the dictionary
//     outgrew its threshold
//
// These assert the corrected behaviour directly, so they keep protecting the
// property if a future upstream release regresses it.

// pinSlotResolver pins the writer's Tier-2 slot binding (a package global) for
// the duration of a test so the written shape cannot depend on test ordering.
func pinSlotResolver(t *testing.T) {
	t.Helper()
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })
}

// TestWriterPageCountsPerRowGroup covers parquet-go #577: the offset index of
// every row group must describe that row group's own pages. Before the fix a
// later row group's page list overwrote an earlier one's, so row group 0
// reported page offsets that lived inside row group 2's byte range — page
// pruning then read the wrong bytes or skipped live pages.
//
// The dataset deliberately gives each row group a different page count (bodies
// shrink row group by row group) so a clobber cannot hide behind identical
// counts.
func TestWriterPageCountsPerRowGroup(t *testing.T) {
	pinSlotResolver(t)

	const rowGroupSize = 4000
	const rowGroups = 4
	rows := make([]schema.LogRow, 0, rowGroups*rowGroupSize)
	for rg := 0; rg < rowGroups; rg++ {
		// Row group 0 gets the widest bodies and therefore the most pages;
		// each later row group gets narrower ones.
		width := 512 >> rg
		for i := 0; i < rowGroupSize; i++ {
			idx := rg*rowGroupSize + i
			rows = append(rows, schema.LogRow{
				AccountID:         1,
				ProjectID:         1,
				TimestampUnixNano: int64(1_760_000_000_000_000_000) + int64(idx)*1_000_000,
				Body:              fmt.Sprintf("%0*d", width, idx),
				SeverityText:      "INFO",
				SeverityNumber:    9,
				ServiceName:       fmt.Sprintf("svc-%03d", idx%250),
				TraceID:           fmt.Sprintf("%032x", idx),
				SpanID:            fmt.Sprintf("%016x", idx),
				Stream:            `{service.name="pagecount"}`,
				StreamID:          "stream-0000",
			})
		}
	}

	res, err := writeLogsParquet(rows, rowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	md := f.Metadata()
	if len(md.RowGroups) != rowGroups {
		t.Fatalf("row groups = %d, want %d", len(md.RowGroups), rowGroups)
	}

	bodyIdx := findColumnIndex(f.Root(), "body")
	if bodyIdx < 0 {
		t.Fatal("body column not found")
	}

	bodyPages := make([]int, 0, rowGroups)
	for rgIdx, rg := range f.RowGroups() {
		meta := &md.RowGroups[rgIdx]
		for ci, chunk := range rg.ColumnChunks() {
			cm := &meta.Columns[ci].MetaData
			name := cm.PathInSchema[len(cm.PathInSchema)-1]

			oidx, err := chunk.OffsetIndex()
			if err != nil {
				t.Fatalf("rg %d col %s: OffsetIndex: %v", rgIdx, name, err)
			}
			n := oidx.NumPages()
			if n < 1 {
				t.Errorf("rg %d col %s: offset index has %d pages", rgIdx, name, n)
				continue
			}

			// The page count in the offset index must equal the pages the
			// chunk actually holds. This is the direct #577 assertion: a
			// clobbered offset index reports another row group's count.
			actual := countPages(t, chunk)
			if actual != n {
				t.Errorf("rg %d col %s: offset index reports %d pages, chunk holds %d",
					rgIdx, name, n, actual)
			}

			// Every page the offset index points at must lie inside THIS row
			// group's column chunk. A clobbered index points into a later row
			// group's byte range.
			lo := cm.DataPageOffset
			if cm.DictionaryPageOffset != 0 && cm.DictionaryPageOffset < lo {
				lo = cm.DictionaryPageOffset
			}
			hi := lo + cm.TotalCompressedSize
			prevOff := int64(-1)
			prevRow := int64(-1)
			for p := 0; p < n; p++ {
				off := oidx.Offset(p)
				size := oidx.CompressedPageSize(p)
				if off < lo || off+size > hi {
					t.Errorf("rg %d col %s page %d: [%d,%d) outside chunk range [%d,%d)",
						rgIdx, name, p, off, off+size, lo, hi)
				}
				if off <= prevOff {
					t.Errorf("rg %d col %s page %d: offset %d not after %d", rgIdx, name, p, off, prevOff)
				}
				prevOff = off

				first := oidx.FirstRowIndex(p)
				if p == 0 && first != 0 {
					t.Errorf("rg %d col %s: first page starts at row %d, want 0", rgIdx, name, first)
				}
				if first <= prevRow && p > 0 {
					t.Errorf("rg %d col %s page %d: first row %d not after %d", rgIdx, name, p, first, prevRow)
				}
				if first >= meta.NumRows {
					t.Errorf("rg %d col %s page %d: first row %d beyond row group size %d",
						rgIdx, name, first, p, meta.NumRows)
				}
				prevRow = first
			}

			if ci == bodyIdx {
				bodyPages = append(bodyPages, n)
			}
		}
	}

	// The dataset was built so the page counts differ; if they came out equal
	// the test would pass vacuously even with the clobber present.
	if len(bodyPages) != rowGroups {
		t.Fatalf("collected %d body page counts, want %d", len(bodyPages), rowGroups)
	}
	distinct := map[int]bool{}
	for _, n := range bodyPages {
		distinct[n] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("body page counts %v are uniform — the dataset no longer distinguishes row groups", bodyPages)
	}
	t.Logf("body pages per row group: %v", bodyPages)
}

func countPages(t *testing.T, chunk parquet.ColumnChunk) int {
	t.Helper()
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()
	n := 0
	for {
		p, err := pages.ReadPage()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n
			}
			t.Fatalf("ReadPage: %v", err)
		}
		parquet.Release(p)
		n++
	}
}

// maxBloomFalsePositiveRate is the per-row-group bound asserted below. The
// writer configures parquet.SplitBlockFilter(10, col) — 10 bits per value,
// whose theoretical false-positive rate is a little under 1%. 3% leaves room
// for sampling noise on a few thousand probes while still failing loudly if a
// filter is sized for fewer values than the row group actually holds (the
// #578 shape: a row group that stops at MaxRowsPerRowGroup rather than at a
// flush boundary).
const maxBloomFalsePositiveRate = 0.03

// TestWriterBloomSizedAtRowGroupLimit covers parquet-go #578. Every row group
// here is closed by MaxRowsPerRowGroup, not by the end of the input, and every
// row carries a distinct value in `service.name` — a `,dict` column that is
// also a bloom column — so each row group's filter must be sized for a full
// row group of distinct values.
func TestWriterBloomSizedAtRowGroupLimit(t *testing.T) {
	pinSlotResolver(t)

	const rowGroupSize = 10000
	const rowGroups = 5
	rows := goldenHighCardLogRows(rowGroupSize * rowGroups)

	res, err := writeLogsParquet(rows, rowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if got := len(f.RowGroups()); got != rowGroups {
		t.Fatalf("row groups = %d, want %d (every one closed by the row limit)", got, rowGroups)
	}
	colIdx := findColumnIndex(f.Root(), "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	absent := absentKeys(4000)
	for rgIdx, rg := range f.RowGroups() {
		bf := rg.ColumnChunks()[colIdx].BloomFilter()
		if bf == nil {
			t.Fatalf("rg %d: service.name has no bloom filter", rgIdx)
		}

		// No false negatives: every value written into this row group must
		// test positive.
		lo := rgIdx * rowGroupSize
		for i := lo; i < lo+rowGroupSize; i += 97 {
			ok, err := bf.Check(parquet.ValueOf(highCardValue(i)))
			if err != nil {
				t.Fatalf("rg %d: bloom check: %v", rgIdx, err)
			}
			if !ok {
				t.Fatalf("rg %d: false negative for value %d — the filter is undersized", rgIdx, i)
			}
		}

		// Bounded false positives: an undersized filter saturates and starts
		// answering yes to everything.
		hits := 0
		for _, k := range absent {
			ok, err := bf.Check(parquet.ValueOf(k))
			if err != nil {
				t.Fatalf("rg %d: bloom check: %v", rgIdx, err)
			}
			if ok {
				hits++
			}
		}
		rate := float64(hits) / float64(len(absent))
		if rate > maxBloomFalsePositiveRate {
			t.Errorf("rg %d: bloom false-positive rate %.4f exceeds bound %.4f (%d/%d)",
				rgIdx, rate, maxBloomFalsePositiveRate, hits, len(absent))
		}
		t.Logf("rg %d: bloom size %d bytes, false-positive rate %.4f", rgIdx, bf.Size(), rate)
	}
}

// TestWriterDictionaryFallbackKeepsEveryValue covers parquet-go #536: when a
// `,dict` column's dictionary outgrows its threshold the writer falls back to
// plain encoding, and that fallback used to drop values. 200,000 distinct
// `service.name` values force the fallback in a column that is dictionary-
// encoded AND bloom-filtered, so both the dictionary and the filter have to
// survive the transition.
func TestWriterDictionaryFallbackKeepsEveryValue(t *testing.T) {
	pinSlotResolver(t)

	const n = 200_000
	rows := goldenHighCardLogRows(n)
	res, err := writeLogsParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet: %v", err)
	}

	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(res.Data))
	defer func() { _ = reader.Close() }()
	if got := reader.NumRows(); got != int64(n) {
		t.Fatalf("NumRows = %d, want %d", got, n)
	}

	readBack := make([]schema.LogRow, n)
	total := 0
	for total < n {
		read, err := reader.Read(readBack[total:])
		total += read
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Read: %v", err)
		}
		if read == 0 {
			break
		}
	}
	if total != n {
		t.Fatalf("read back %d rows, want %d", total, n)
	}

	// Every row keeps its own value, in order — a dropped or shifted value
	// shows up as a mismatch, not merely as a smaller distinct set.
	seen := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		want := highCardValue(i)
		if readBack[i].ServiceName != want {
			t.Fatalf("row %d: service.name = %q, want %q", i, readBack[i].ServiceName, want)
		}
		if seen[readBack[i].ServiceName] {
			t.Fatalf("row %d: duplicate value %q", i, readBack[i].ServiceName)
		}
		seen[readBack[i].ServiceName] = true
	}
	if len(seen) != n {
		t.Fatalf("distinct service.name values = %d, want %d", len(seen), n)
	}
}

// TestRowGroupCopyBloomSizedPerOutputRowGroup drives the same #578 shape
// through parquet-go's Writer.WriteRowGroup path — the path a verbatim
// column-chunk copy would take — where an input row group larger than
// MaxRowsPerRowGroup is split across several output row groups. Each output
// group's filter has to be sized for the rows that group actually holds, not
// for the whole input.
//
// This is the only place the defect is reachable at all: the LH flush and
// compaction writers call Write(rows), which sizes filters at flush time and
// was never affected. The test is here so the property is pinned before any
// future adoption of the copy path, not because LH is exposed today.
func TestRowGroupCopyBloomSizedPerOutputRowGroup(t *testing.T) {
	pinSlotResolver(t)

	const rowGroupSize = 10000
	const rowGroups = 5
	const total = rowGroupSize * rowGroups

	src, err := writeLogsParquet(goldenHighCardLogRows(total), rowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet(source): %v", err)
	}
	sf, err := parquet.OpenFile(bytes.NewReader(src.Data), int64(len(src.Data)))
	if err != nil {
		t.Fatalf("OpenFile(source): %v", err)
	}

	// One logical row group spanning the whole input, written out through a
	// writer bounded at rowGroupSize — the split case #578 mis-sized.
	merged := parquet.MultiRowGroup(sf.RowGroups()...)

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.Compression(&zstd.Codec{Level: zstdLevel(goldenCompressionLevel)}),
		parquet.MaxRowsPerRowGroup(rowGroupSize),
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns())...),
	)
	if _, err := w.WriteRowGroup(merged); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	out, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("OpenFile(out): %v", err)
	}
	if got := out.Metadata().NumRows; got != total {
		t.Fatalf("copied row count = %d, want %d", got, total)
	}
	colIdx := findColumnIndex(out.Root(), "service.name")
	if colIdx < 0 {
		t.Fatal("service.name column not found")
	}

	// 10 bits per value over rowGroupSize values, rounded up to whole
	// split-blocks (32 bytes each), plus one block of slack.
	wantBytes := int64(10 * rowGroupSize / 8)
	maxBytes := wantBytes + 2048

	for rgIdx, rg := range out.RowGroups() {
		if rg.NumRows() != rowGroupSize {
			t.Errorf("out rg %d: %d rows, want %d", rgIdx, rg.NumRows(), rowGroupSize)
		}
		bf := rg.ColumnChunks()[colIdx].BloomFilter()
		if bf == nil {
			t.Fatalf("out rg %d: service.name has no bloom filter", rgIdx)
		}
		if bf.Size() > maxBytes {
			t.Errorf("out rg %d: bloom is %d bytes for %d rows; a filter sized for this row group is ~%d bytes",
				rgIdx, bf.Size(), rg.NumRows(), wantBytes)
		}
		t.Logf("out rg %d: %d rows, bloom %d bytes", rgIdx, rg.NumRows(), bf.Size())
	}
}
