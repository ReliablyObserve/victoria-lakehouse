package parquets3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// This file covers two properties the readback goldens alone cannot:
//
//  1. the golden comparison actually NAMES what changed, so a future
//     parquet-go bump produces a diagnosis instead of two 2000-line JSON
//     blobs to eyeball (TestGoldenDiffNamesTamperedFields), and
//  2. the log writer is byte-reproducible for a fixed input, with a footer
//     diff helper that says exactly which metadata fields differ when two
//     files are not identical (TestLogsParquetWriteIsByteReproducible,
//     TestFooterMetadataDiff*).
//
// Together they make "did the library change anything?" a question the test
// suite answers by itself at the next bump.

// ---------------------------------------------------------------------------
// 1. Field-level golden diff
// ---------------------------------------------------------------------------

// diffGolden compares two goldenFile values field by field and returns one
// entry per difference, each naming the JSON path that changed. It walks the
// marshalled form rather than the Go struct so a field added to goldenFile /
// goldenColumn / goldenBloomProbe is covered without touching this code.
func diffGolden(want, got goldenFile) []string {
	var wv, gv any
	mustRemarshal(want, &wv)
	mustRemarshal(got, &gv)
	var out []string
	diffJSON("", wv, gv, &out)
	sort.Strings(out)
	return out
}

func mustRemarshal(in any, out *any) {
	data, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		panic(err)
	}
}

// diffJSON walks two decoded JSON values in parallel, appending a
// "path: want -> got" line for every leaf that differs. Paths read like
// `columns[3].encodings[0]` and `bloom_probes[1].present_hits`.
func diffJSON(path string, want, got any, out *[]string) {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: type changed (%T -> %T)", path, want, got))
			return
		}
		keys := map[string]bool{}
		for k := range w {
			keys[k] = true
		}
		for k := range g {
			keys[k] = true
		}
		names := make([]string, 0, len(keys))
		for k := range keys {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			diffJSON(joinPath(path, k), w[k], g[k], out)
		}
	case []any:
		g, ok := got.([]any)
		if !ok {
			*out = append(*out, fmt.Sprintf("%s: type changed (%T -> %T)", path, want, got))
			return
		}
		if len(w) != len(g) {
			*out = append(*out, fmt.Sprintf("%s: length %d -> %d", path, len(w), len(g)))
		}
		n := len(w)
		if len(g) < n {
			n = len(g)
		}
		for i := 0; i < n; i++ {
			diffJSON(fmt.Sprintf("%s[%d]", path, i), w[i], g[i], out)
		}
	default:
		if !reflect.DeepEqual(want, got) {
			*out = append(*out, fmt.Sprintf("%s: %v -> %v", path, want, got))
		}
	}
}

// cloneGolden deep-copies every slice in a goldenFile. A plain struct
// assignment shares the Columns / BloomProbes / RowsPerRowGroup backing
// arrays, so "tampering with the copy" would silently tamper with the
// original and the diff would come back empty.
func cloneGolden(f goldenFile) goldenFile {
	out := f
	out.FooterKVKeys = append([]string(nil), f.FooterKVKeys...)
	out.RowsPerRowGroup = append([]int64(nil), f.RowsPerRowGroup...)
	out.BloomProbes = append([]goldenBloomProbe(nil), f.BloomProbes...)
	out.Columns = make([]goldenColumn, len(f.Columns))
	for i, c := range f.Columns {
		c.Encodings = append([]string(nil), c.Encodings...)
		c.Codecs = append([]string(nil), c.Codecs...)
		c.PagesPerRowGroup = append([]int(nil), c.PagesPerRowGroup...)
		out.Columns[i] = c
	}
	return out
}

func joinPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

// TestGoldenDiffNamesTamperedFields is the tamper test: take a real golden
// file, change one thing in a copy, and require the diff to name exactly that
// field. Without it the golden comparison is only known to detect a
// whole-blob difference — which is also what it reports, leaving whoever runs
// the next parquet-go bump to find the changed line by eye.
func TestGoldenDiffNamesTamperedFields(t *testing.T) {
	files := buildGoldenFiles(t)
	if len(files) == 0 {
		t.Fatal("no golden files were built")
	}
	base := files[0]
	if len(base.Columns) == 0 || len(base.BloomProbes) == 0 || len(base.RowsPerRowGroup) < 2 {
		t.Fatalf("golden file %s is too small to tamper with (columns=%d probes=%d row groups=%d)",
			base.Name, len(base.Columns), len(base.BloomProbes), len(base.RowsPerRowGroup))
	}

	cases := []struct {
		name     string
		tamper   func(f *goldenFile)
		wantPath string
	}{
		{
			name: "column encoding",
			tamper: func(f *goldenFile) {
				f.Columns[0].Encodings[0] = "PLAIN_TAMPERED"
			},
			wantPath: "columns[0].encodings[0]",
		},
		{
			name: "bloom probe hit count",
			tamper: func(f *goldenFile) {
				f.BloomProbes[0].PresentHits--
			},
			wantPath: "bloom_probes[0].present_hits",
		},
		{
			name: "row-group boundary",
			tamper: func(f *goldenFile) {
				f.RowsPerRowGroup[0]++
				f.RowsPerRowGroup[1]--
			},
			wantPath: "rows_per_row_group[0]",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tampered := cloneGolden(base)
			c.tamper(&tampered)
			if reflect.DeepEqual(base, tampered) {
				t.Fatalf("tamper for %s did not change the copy — a shallow copy is sharing the slice it mutates", c.wantPath)
			}

			diffs := diffGolden(base, tampered)
			if len(diffs) == 0 {
				t.Fatalf("tampering with %s produced no diff — the golden comparison would not catch this change", c.wantPath)
			}
			found := false
			for _, d := range diffs {
				if strings.HasPrefix(d, c.wantPath+":") {
					found = true
				}
			}
			if !found {
				t.Fatalf("diff does not name %s; got:\n  %s", c.wantPath, strings.Join(diffs, "\n  "))
			}
		})
	}

	// An untampered copy must produce no diff at all, otherwise every bump
	// would report noise.
	if diffs := diffGolden(base, cloneGolden(base)); len(diffs) != 0 {
		t.Fatalf("identical golden files reported %d differences: %s", len(diffs), strings.Join(diffs, "; "))
	}
}

// ---------------------------------------------------------------------------
// 2. Byte reproducibility and footer diffing
// ---------------------------------------------------------------------------

// TestLogsParquetWriteIsByteReproducible writes the same rows twice, in the
// same process against the same library, and requires the two files to be
// identical byte for byte. That is what makes a byte-level comparison across
// parquet-go versions meaningful: any difference then belongs to the library,
// not to the writer.
//
// Traces are deliberately excluded, see
// TestTracesParquetIsStructurallyStableButNotByteStable.
func TestLogsParquetWriteIsByteReproducible(t *testing.T) {
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })

	rows := goldenLogRows(2*goldenRowGroupSize + 321)

	first, err := writeLogsParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	second, err := writeLogsParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("second write: %v", err)
	}

	if bytes.Equal(first.Data, second.Data) {
		return
	}

	off := firstByteDifference(first.Data, second.Data)
	diffs := footerMetadataDiff(t, first.Data, second.Data)
	t.Fatalf("writing the same %d rows twice produced different bytes (len %d vs %d, first difference at offset %d).\n"+
		"Footer metadata differences (%d):\n  %s\n"+
		"A non-reproducible writer makes cross-version byte comparison meaningless — find the nondeterministic input (map iteration order is the usual cause).",
		len(rows), len(first.Data), len(second.Data), off, len(diffs), strings.Join(diffs, "\n  "))
}

// TestTracesParquetIsStructurallyStableButNotByteStable records why the trace
// writer is not part of the byte-reproducibility gate: the `_trace_idx` footer
// key/value is built by iterating a Go map, so its entry order varies from run
// to run WITHIN one library version. That is harmless for readers — the index
// is self-describing and the read path sorts — but it makes byte-level
// provenance impossible for trace files, so the property asserted here is the
// structural one instead: two writes of the same rows must describe the same
// file.
//
// This is not a claim that the bytes always differ (an unlucky map order could
// match); it is a claim that the STRUCTURE never does.
func TestTracesParquetIsStructurallyStableButNotByteStable(t *testing.T) {
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })

	rows := goldenTraceRows(goldenRowGroupSize + 777)

	first, err := writeTracesParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("first write: %v", err)
	}
	second, err := writeTracesParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("second write: %v", err)
	}

	probes := goldenProbeSet{
		present: map[string][]string{"service.name": {"svc-00"}},
		absent:  map[string][]string{"service.name": absentKeys(16)},
	}
	a := profileParquet(t, "traces-a", first.Data, "timestamp_unix_nano", probes)
	b := profileParquet(t, "traces-b", second.Data, "timestamp_unix_nano", probes)
	a.Name, b.Name = "traces", "traces"

	if diffs := diffGolden(a, b); len(diffs) != 0 {
		t.Fatalf("two writes of the same %d trace rows describe different files:\n  %s", len(rows), strings.Join(diffs, "\n  "))
	}

	if diffs := footerMetadataDiff(t, first.Data, second.Data); len(diffs) != 0 {
		t.Fatalf("two writes of the same trace rows differ in footer metadata (only the byte order of the _trace_idx KV may vary, never its described content):\n  %s", strings.Join(diffs, "\n  "))
	}
}

// TestFooterMetadataDiffNamesRealDifferences exercises the helper itself
// against two files that are genuinely different — same rows, different
// row-group size — so a silently-broken diff cannot make the tests above pass
// by reporting nothing.
func TestFooterMetadataDiffNamesRealDifferences(t *testing.T) {
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })

	rows := goldenLogRows(2*goldenRowGroupSize + 100)
	a, err := writeLogsParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("write a: %v", err)
	}
	b, err := writeLogsParquet(rows, goldenRowGroupSize/2, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("write b: %v", err)
	}

	diffs := footerMetadataDiff(t, a.Data, b.Data)
	if len(diffs) == 0 {
		t.Fatal("footerMetadataDiff reported no difference between files written with different row-group sizes")
	}
	joined := strings.Join(diffs, "\n")
	if !strings.Contains(joined, "row_groups") {
		t.Errorf("diff does not mention the row-group count:\n%s", joined)
	}

	// Identical inputs must produce no noise.
	if d := footerMetadataDiff(t, a.Data, a.Data); len(d) != 0 {
		t.Errorf("footerMetadataDiff reported %d differences for a file against itself: %s", len(d), strings.Join(d, "; "))
	}
}

// TestFooterMetadataDiffAgainstBaseline is the re-runnable half: point
// PARQUET_FOOTER_BASELINE at a .parquet file written by another parquet-go
// version and it reports exactly which footer fields moved, so a bump can
// record the real diff instead of asserting "nothing changed" from memory.
// Produce a baseline by running this package's writer on the pinned version
// and keeping the file. At parquet-go v0.30.1 -> v0.32.0 the only difference
// was `created_by`.
func TestFooterMetadataDiffAgainstBaseline(t *testing.T) {
	path := os.Getenv("PARQUET_FOOTER_BASELINE")
	if path == "" {
		t.Skip("set PARQUET_FOOTER_BASELINE=<logs .parquet written by another parquet-go version> to diff against it")
	}
	baseline, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read baseline %s: %v", path, err)
	}

	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })

	rows := goldenLogRows(2*goldenRowGroupSize + 321)
	current, err := writeLogsParquet(rows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("write current: %v", err)
	}

	diffs := footerMetadataDiff(t, baseline, current.Data)
	t.Logf("footer metadata differences vs %s (%d):\n  %s", path, len(diffs), strings.Join(diffs, "\n  "))
	for _, d := range diffs {
		if !strings.HasPrefix(d, "created_by:") {
			t.Errorf("unexpected footer difference beyond created_by: %s", d)
		}
	}
	if bytes.Equal(baseline, current.Data) {
		t.Logf("files are byte-identical")
	} else {
		t.Logf("files differ at byte offset %d (len %d vs %d)", firstByteDifference(baseline, current.Data), len(baseline), len(current.Data))
	}
}

// footerMetadataDiff reports every footer-metadata field that differs between
// two Parquet files, naming the field. It reads only the public parquet-go
// surface LH's own read path uses.
func footerMetadataDiff(t *testing.T, a, b []byte) []string {
	t.Helper()
	fa := openForDiff(t, "a", a)
	fb := openForDiff(t, "b", b)

	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }

	ma, mb := fa.Metadata(), fb.Metadata()
	if ma.CreatedBy != mb.CreatedBy {
		add("created_by: %q -> %q", ma.CreatedBy, mb.CreatedBy)
	}
	if ma.Version != mb.Version {
		add("format_version: %d -> %d", ma.Version, mb.Version)
	}
	if ma.NumRows != mb.NumRows {
		add("num_rows: %d -> %d", ma.NumRows, mb.NumRows)
	}
	if len(ma.RowGroups) != len(mb.RowGroups) {
		add("row_groups: %d -> %d", len(ma.RowGroups), len(mb.RowGroups))
	}

	kvA, kvB := footerKVKeys(ma.KeyValueMetadata), footerKVKeys(mb.KeyValueMetadata)
	if !reflect.DeepEqual(kvA, kvB) {
		add("footer_kv_keys: %v -> %v", kvA, kvB)
	}

	n := len(ma.RowGroups)
	if len(mb.RowGroups) < n {
		n = len(mb.RowGroups)
	}
	for i := 0; i < n; i++ {
		ra, rb := ma.RowGroups[i], mb.RowGroups[i]
		if ra.NumRows != rb.NumRows {
			add("row_groups[%d].num_rows: %d -> %d", i, ra.NumRows, rb.NumRows)
		}
		if len(ra.Columns) != len(rb.Columns) {
			add("row_groups[%d].columns: %d -> %d", i, len(ra.Columns), len(rb.Columns))
			continue
		}
		for j := range ra.Columns {
			ca, cb := ra.Columns[j].MetaData, rb.Columns[j].MetaData
			name := strings.Join(ca.PathInSchema, ".")
			if got := strings.Join(cb.PathInSchema, "."); name != got {
				add("row_groups[%d].columns[%d].path: %q -> %q", i, j, name, got)
				continue
			}
			if ca.Codec != cb.Codec {
				add("row_groups[%d].columns[%s].codec: %v -> %v", i, name, ca.Codec, cb.Codec)
			}
			if ca.NumValues != cb.NumValues {
				add("row_groups[%d].columns[%s].num_values: %d -> %d", i, name, ca.NumValues, cb.NumValues)
			}
			if ea, eb := encodingNames(ca.Encoding), encodingNames(cb.Encoding); ea != eb {
				add("row_groups[%d].columns[%s].encodings: %s -> %s", i, name, ea, eb)
			}
			if (ca.DictionaryPageOffset != 0) != (cb.DictionaryPageOffset != 0) {
				add("row_groups[%d].columns[%s].has_dictionary_page: %v -> %v", i, name,
					ca.DictionaryPageOffset != 0, cb.DictionaryPageOffset != 0)
			}
			if ca.Statistics.NullCount != cb.Statistics.NullCount {
				add("row_groups[%d].columns[%s].null_count: %d -> %d", i, name, ca.Statistics.NullCount, cb.Statistics.NullCount)
			}
			if ca.Statistics.DistinctCount != cb.Statistics.DistinctCount {
				add("row_groups[%d].columns[%s].distinct_count: %d -> %d", i, name, ca.Statistics.DistinctCount, cb.Statistics.DistinctCount)
			}
			if !bytes.Equal(ca.Statistics.MinValue, cb.Statistics.MinValue) {
				add("row_groups[%d].columns[%s].min_value: %x -> %x", i, name, ca.Statistics.MinValue, cb.Statistics.MinValue)
			}
			if !bytes.Equal(ca.Statistics.MaxValue, cb.Statistics.MaxValue) {
				add("row_groups[%d].columns[%s].max_value: %x -> %x", i, name, ca.Statistics.MaxValue, cb.Statistics.MaxValue)
			}
		}
	}
	sort.Strings(out)
	return out
}

// encodingNames renders a column chunk's encoding list in a stable order, so
// a reordering by the library is not reported as a change.
func encodingNames(encodings []format.Encoding) string {
	out := make([]string, 0, len(encodings))
	for _, e := range encodings {
		out = append(out, e.String())
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func openForDiff(t *testing.T, which string, data []byte) *parquet.File {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open %s: %v", which, err)
	}
	return f
}

// footerKVKeys returns the sorted footer key/value metadata KEYS. Only the
// keys are compared: the trace writer's `_trace_idx` VALUE is built from a Go
// map and its byte order varies run to run, while the set of keys does not.
func footerKVKeys(kvs []format.KeyValue) []string {
	out := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, kv.Key)
	}
	sort.Strings(out)
	return out
}

func firstByteDifference(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
