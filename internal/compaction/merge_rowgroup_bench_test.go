package compaction

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Measures what parquet-go v0.32.0's Writer.WriteRowGroup fast paths (verbatim
// column-chunk copy for whole row groups, per-segment copy for in-order
// concatenations, column-wise re-encode when only the config differs) would
// buy the compaction merge, against the merge LH actually runs today:
// decode every row into []schema.LogRow, heal it, sort it, write it back.
//
// Both benchmarks produce a Parquet file with the same rows and the same
// production writer options from the same three 100k-row inputs. The
// difference is only how the rows get from the inputs to the output, so the
// delta is the library path's contribution and nothing else.
//
// The library path is NOT wired into the compactor: the merge is not a pure
// row union. It drops trace-shaped rows, backfills SeverityText, re-promotes
// dedicated columns and Tier-2 slots, then re-sorts globally, and it feeds the
// decoded rows to schema.LogRowTimeBounds, schema.ExtractLogLabelAggregates
// and schema.ExtractLogBloomValues for the manifest and the pmeta bloom. A
// verbatim column-chunk copy skips the decode those five consumers need, so
// adopting it means building a second metadata path out of footer statistics
// and giving up the healing passes. These benchmarks size that trade so the
// decision rests on a number.

const (
	benchMergeFiles        = 3
	benchMergeRowsPerFile  = 100_000
	benchMergeRowGroupSize = 10_000
	benchMergeCompression  = 3
)

// benchMergeRows builds one input file's rows. Timestamps are strided by
// fileIdx so the three files interleave — the shape a real merge sees, where
// no whole input row group can be emitted verbatim.
func benchMergeRows(fileIdx, n int, interleave bool) []schema.LogRow {
	const base = int64(1_760_000_000_000_000_000)
	rows := make([]schema.LogRow, n)
	for i := range rows {
		var ts int64
		if interleave {
			ts = base + int64(i*benchMergeFiles+fileIdx)*1_000_000
		} else {
			// Disjoint ranges: file 0 entirely precedes file 1, and so on —
			// the shape where a merge IS an in-order concatenation and the
			// copy fast paths can engage.
			ts = base + int64(fileIdx*n+i)*1_000_000
		}
		svc := fmt.Sprintf("svc-%02d", i%12)
		rows[i] = schema.LogRow{
			AccountID:         1,
			ProjectID:         1,
			TimestampUnixNano: ts,
			Body:              fmt.Sprintf("processed request %d on file %d", i, fileIdx),
			SeverityText:      []string{"DEBUG", "INFO", "WARN", "ERROR"}[i%4],
			SeverityNumber:    int32(5 + 4*(i%4)),
			ServiceName:       svc,
			TraceID:           fmt.Sprintf("%032x", fileIdx*n+i),
			SpanID:            fmt.Sprintf("%016x", fileIdx*n+i),
			K8sNamespaceName:  fmt.Sprintf("ns-%d", i%4),
			K8sPodName:        fmt.Sprintf("%s-pod-%d", svc, i%50),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-%d", i%8),
			DeployEnv:         []string{"prod", "staging"}[i%2],
			CloudRegion:       []string{"eu-west-1", "us-east-1"}[i%2],
			HostName:          fmt.Sprintf("host-%d", i%8),
			Stream:            fmt.Sprintf("{service.name=%q}", svc),
			StreamID:          fmt.Sprintf("stream-%04d", i%24),
			ScopeName:         fmt.Sprintf("scope-%d", i%3),
		}
	}
	return rows
}

func benchMergeInputs(tb testing.TB, interleave bool) [][]byte {
	tb.Helper()
	out := make([][]byte, benchMergeFiles)
	for f := 0; f < benchMergeFiles; f++ {
		data, err := writeCompactedLogs(benchMergeRows(f, benchMergeRowsPerFile, interleave),
			benchMergeRowGroupSize, benchMergeCompression)
		if err != nil {
			tb.Fatalf("build input %d: %v", f, err)
		}
		out[f] = data
	}
	return out
}

// mergeViaRows is the merge the compactor runs today, minus the LH-specific
// healing passes (which no library path can do at all) so the comparison
// isolates the parquet-go work.
func mergeViaRows(tb testing.TB, inputs [][]byte) []byte {
	tb.Helper()
	var merged []schema.LogRow
	for _, data := range inputs {
		rows, err := readLogRows(data)
		if err != nil {
			tb.Fatalf("readLogRows: %v", err)
		}
		merged = append(merged, rows...)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].TimestampUnixNano != merged[j].TimestampUnixNano {
			return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
		}
		return merged[i].ServiceName < merged[j].ServiceName
	})
	out, err := writeCompactedLogs(merged, benchMergeRowGroupSize, benchMergeCompression)
	if err != nil {
		tb.Fatalf("writeCompactedLogs: %v", err)
	}
	return out
}

// mergeViaRowGroups is the library path: parquet-go merges the inputs' row
// groups into one logical, still-sorted row group and the writer decides per
// segment whether it can be copied verbatim.
func mergeViaRowGroups(tb testing.TB, inputs [][]byte) []byte {
	tb.Helper()
	rowGroups := make([]parquet.RowGroup, 0, benchMergeFiles*(benchMergeRowsPerFile/benchMergeRowGroupSize))
	for _, data := range inputs {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			tb.Fatalf("OpenFile: %v", err)
		}
		rowGroups = append(rowGroups, f.RowGroups()...)
	}
	sorting := parquet.SortingRowGroupConfig(
		parquet.SortingColumns(parquet.Ascending("timestamp_unix_nano")),
	)
	merged, err := parquet.MergeRowGroups(rowGroups, sorting)
	if err != nil {
		tb.Fatalf("MergeRowGroups: %v", err)
	}

	// The writer has to be built on the MERGED row group's schema, not on
	// schema.LogRow: a schema read back from a file is not EqualNodes to the
	// one derived from the Go struct tags, and WriteRowGroup rejects the
	// mismatch outright. Any adoption of this path inherits that constraint.
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf,
		merged.Schema(),
		parquet.Compression(&zstd.Codec{Level: zstdLevel(benchMergeCompression)}),
		parquet.MaxRowsPerRowGroup(benchMergeRowGroupSize),
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns())...),
		parquet.SortingWriterConfig(
			parquet.SortingColumns(parquet.Ascending("timestamp_unix_nano")),
		),
	)
	if _, err := w.WriteRowGroup(merged); err != nil {
		tb.Fatalf("WriteRowGroup: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// TestMergePathsAgree is the correctness gate the benchmarks lean on: both
// paths must produce the same row count and the same time bounds, otherwise
// the timings compare different work.
func TestMergePathsAgree(t *testing.T) {
	for _, tc := range []struct {
		name       string
		interleave bool
	}{
		{"interleaved", true},
		{"disjoint", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs := benchMergeInputs(t, tc.interleave)
			viaRows := mergeViaRows(t, inputs)
			viaRowGroups := mergeViaRowGroups(t, inputs)

			rowsA, err := readLogRows(viaRows)
			if err != nil {
				t.Fatalf("read row-merge output: %v", err)
			}
			rowsB, err := readLogRows(viaRowGroups)
			if err != nil {
				t.Fatalf("read row-group-merge output: %v", err)
			}
			if len(rowsA) != len(rowsB) {
				t.Fatalf("row counts differ: %d vs %d", len(rowsA), len(rowsB))
			}
			minA, maxA := schema.LogRowTimeBounds(rowsA)
			minB, maxB := schema.LogRowTimeBounds(rowsB)
			if minA != minB || maxA != maxB {
				t.Fatalf("time bounds differ: [%d,%d] vs [%d,%d]", minA, maxA, minB, maxB)
			}
			for i := range rowsA {
				if rowsA[i].TraceID != rowsB[i].TraceID {
					t.Fatalf("row %d differs: trace_id %q vs %q", i, rowsA[i].TraceID, rowsB[i].TraceID)
				}
			}
			t.Logf("%s: %d rows, row-merge %d bytes, row-group-merge %d bytes",
				tc.name, len(rowsA), len(viaRows), len(viaRowGroups))
		})
	}
}

func BenchmarkMergeInterleaved(b *testing.B) {
	inputs := benchMergeInputs(b, true)
	b.Run("rows", func(b *testing.B) { benchMerge(b, inputs, mergeViaRows) })
	b.Run("rowgroups", func(b *testing.B) { benchMerge(b, inputs, mergeViaRowGroups) })
}

func BenchmarkMergeDisjoint(b *testing.B) {
	inputs := benchMergeInputs(b, false)
	b.Run("rows", func(b *testing.B) { benchMerge(b, inputs, mergeViaRows) })
	b.Run("rowgroups", func(b *testing.B) { benchMerge(b, inputs, mergeViaRowGroups) })
}

func benchMerge(b *testing.B, inputs [][]byte, fn func(testing.TB, [][]byte) []byte) {
	b.ReportAllocs()
	b.SetBytes(int64(benchMergeFiles * benchMergeRowsPerFile))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := fn(b, inputs)
		if len(out) == 0 {
			b.Fatal("empty output")
		}
	}
}
