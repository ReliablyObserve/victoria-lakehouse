package parquets3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Readback goldens: structural facts about files produced by the REAL
// production writer (writeLogsParquet / writeTracesParquet — zstd, the
// production row-group size, `,dict` columns, split-block row-group blooms,
// the footer KV set) captured with one parquet-go release and asserted
// against on every later release.
//
// The goldens are deliberately structural, never byte-level: row counts,
// row-group boundaries, per-column encodings and codecs, bloom presence plus
// hit/miss on fixed samples of known-present and known-absent keys, page-index
// presence and per-row-group page counts, footer key-value keys, and the
// min/max of the time column. Those are exactly the properties a reader (ours,
// DuckDB, pyarrow) depends on; a library upgrade that leaves them identical
// cannot have changed what any reader sees.
//
// Regenerate with:
//
//	PARQUET_GOLDEN_UPDATE=1 go test ./internal/storage/parquets3/ -run TestReadbackGolden
//
// A regenerated golden must be justified in the commit message: either the
// difference is an upstream bug fix we want, or it is a regression to fix.
const readbackGoldenPath = "testdata/readback_golden.json"

// goldenRowGroupSize is the production Insert.RowGroupSize default
// (internal/config: RowGroupSize: 10000).
const goldenRowGroupSize = 10000

// goldenCompressionLevel is the production Insert.CompressionLevel default
// (balanced profile), mapped by zstdLevel to zstd.SpeedDefault.
const goldenCompressionLevel = 3

type goldenColumn struct {
	Name             string   `json:"name"`
	Type             string   `json:"type"`
	Encodings        []string `json:"encodings"`
	Codecs           []string `json:"codecs"`
	HasBloom         bool     `json:"has_bloom"`
	HasColumnIndex   bool     `json:"has_column_index"`
	HasOffsetIndex   bool     `json:"has_offset_index"`
	PagesPerRowGroup []int    `json:"pages_per_row_group"`
	NumValues        int64    `json:"num_values"`
	NullCount        int64    `json:"null_count"`
}

type goldenBloomProbe struct {
	Column          string `json:"column"`
	PresentKeys     int    `json:"present_keys"`
	PresentHits     int    `json:"present_hits"`
	AbsentKeys      int    `json:"absent_keys"`
	AbsentFalseHits int    `json:"absent_false_hits"`
}

type goldenFile struct {
	Name            string             `json:"name"`
	Rows            int64              `json:"rows"`
	RowGroups       int                `json:"row_groups"`
	RowsPerRowGroup []int64            `json:"rows_per_row_group"`
	FooterKVKeys    []string           `json:"footer_kv_keys"`
	TimeColumn      string             `json:"time_column"`
	TimeMin         int64              `json:"time_min"`
	TimeMax         int64              `json:"time_max"`
	Columns         []goldenColumn     `json:"columns"`
	BloomProbes     []goldenBloomProbe `json:"bloom_probes"`
}

type goldenSet struct {
	// Note: the parquet-go version that produced the goldens is recorded for
	// the reader's benefit only; it is not asserted (the whole point is that
	// a later version must reproduce the same structure).
	ParquetGoVersion string       `json:"parquet_go_version"`
	Files            []goldenFile `json:"files"`
}

// goldenProbeSet carries the fixed key samples a dataset is probed with.
type goldenProbeSet struct {
	present map[string][]string
	absent  map[string][]string
}

// profileParquet derives the structural facts recorded in the golden from a
// written Parquet file. Nothing here reaches into parquet-go internals: it is
// the same public surface (Metadata, ColumnChunks, ColumnIndex, OffsetIndex,
// BloomFilter) LH's own read path uses.
func profileParquet(t *testing.T, name string, data []byte, timeColumn string, probes goldenProbeSet) goldenFile {
	t.Helper()

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("%s: OpenFile: %v", name, err)
	}
	md := f.Metadata()

	out := goldenFile{
		Name:       name,
		Rows:       md.NumRows,
		RowGroups:  len(md.RowGroups),
		TimeColumn: timeColumn,
	}
	for _, kv := range md.KeyValueMetadata {
		out.FooterKVKeys = append(out.FooterKVKeys, kv.Key)
	}
	sort.Strings(out.FooterKVKeys)

	// Per-column accumulation, keyed by the leaf path so map columns
	// (key_value.key / key_value.value) stay distinguishable.
	type acc struct {
		typ       string
		encodings map[string]bool
		codecs    map[string]bool
		pages     []int
		numValues int64
		nullCount int64
		hasBloom  bool
		hasCIdx   bool
		hasOIdx   bool
		order     int
	}
	cols := map[string]*acc{}
	order := 0

	for rgIdx := range md.RowGroups {
		rg := &md.RowGroups[rgIdx]
		out.RowsPerRowGroup = append(out.RowsPerRowGroup, rg.NumRows)
		chunks := f.RowGroups()[rgIdx].ColumnChunks()
		for ci := range rg.Columns {
			cm := &rg.Columns[ci].MetaData
			path := strings.Join(cm.PathInSchema, ".")
			a := cols[path]
			if a == nil {
				a = &acc{
					typ:       cm.Type.String(),
					encodings: map[string]bool{},
					codecs:    map[string]bool{},
					order:     order,
				}
				order++
				cols[path] = a
			}
			for _, e := range cm.Encoding {
				a.encodings[e.String()] = true
			}
			a.codecs[cm.Codec.String()] = true
			a.numValues += cm.NumValues

			chunk := chunks[ci]
			if bf := chunk.BloomFilter(); bf != nil {
				a.hasBloom = true
			}
			if cidx, err := chunk.ColumnIndex(); err == nil && cidx != nil {
				a.hasCIdx = true
				for p := 0; p < cidx.NumPages(); p++ {
					a.nullCount += cidx.NullCount(p)
				}
			}
			if oidx, err := chunk.OffsetIndex(); err == nil && oidx != nil {
				a.hasOIdx = true
				a.pages = append(a.pages, oidx.NumPages())
			} else {
				a.pages = append(a.pages, -1)
			}
		}
	}

	paths := make([]string, 0, len(cols))
	for p := range cols {
		paths = append(paths, p)
	}
	sort.Slice(paths, func(i, j int) bool { return cols[paths[i]].order < cols[paths[j]].order })
	for _, p := range paths {
		a := cols[p]
		out.Columns = append(out.Columns, goldenColumn{
			Name:             p,
			Type:             a.typ,
			Encodings:        sortedKeys(a.encodings),
			Codecs:           sortedKeys(a.codecs),
			HasBloom:         a.hasBloom,
			HasColumnIndex:   a.hasCIdx,
			HasOffsetIndex:   a.hasOIdx,
			PagesPerRowGroup: a.pages,
			NumValues:        a.numValues,
			NullCount:        a.nullCount,
		})
	}

	out.TimeMin, out.TimeMax = columnMinMaxInt64(t, f, timeColumn)
	out.BloomProbes = runBloomProbes(t, f, probes)
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// columnMinMaxInt64 reads the file-wide min/max of an int64 column from the
// row-group statistics (the same source the query path's row-group pruning
// uses).
func columnMinMaxInt64(t *testing.T, f *parquet.File, column string) (int64, int64) {
	t.Helper()
	idx := findColumnIndex(f.Root(), column)
	if idx < 0 {
		t.Fatalf("column %q not found", column)
	}
	var min, max int64
	first := true
	for _, rg := range f.RowGroups() {
		chunk := rg.ColumnChunks()[idx]
		cidx, err := chunk.ColumnIndex()
		if err != nil {
			t.Fatalf("column index for %q: %v", column, err)
		}
		for p := 0; p < cidx.NumPages(); p++ {
			if cidx.NullPage(p) {
				continue
			}
			lo, hi := cidx.MinValue(p).Int64(), cidx.MaxValue(p).Int64()
			if first {
				min, max, first = lo, hi, false
				continue
			}
			if lo < min {
				min = lo
			}
			if hi > max {
				max = hi
			}
		}
	}
	if first {
		t.Fatalf("column %q has no non-null pages", column)
	}
	return min, max
}

// runBloomProbes checks every present key against the row-group blooms (a hit
// in ANY row group counts — a bloom must never produce a false negative) and
// every absent key (hits there are false positives, bounded by the configured
// 10 bits/value).
func runBloomProbes(t *testing.T, f *parquet.File, probes goldenProbeSet) []goldenBloomProbe {
	t.Helper()
	columns := make([]string, 0, len(probes.present))
	for c := range probes.present {
		columns = append(columns, c)
	}
	sort.Strings(columns)

	out := make([]goldenBloomProbe, 0, len(columns))
	for _, col := range columns {
		idx := findColumnIndex(f.Root(), col)
		if idx < 0 {
			t.Fatalf("bloom probe column %q not found", col)
		}
		probe := goldenBloomProbe{
			Column:      col,
			PresentKeys: len(probes.present[col]),
			AbsentKeys:  len(probes.absent[col]),
		}
		for _, key := range probes.present[col] {
			if bloomContains(t, f, idx, key) {
				probe.PresentHits++
			}
		}
		for _, key := range probes.absent[col] {
			if bloomContains(t, f, idx, key) {
				probe.AbsentFalseHits++
			}
		}
		out = append(out, probe)
	}
	return out
}

func bloomContains(t *testing.T, f *parquet.File, colIdx int, key string) bool {
	t.Helper()
	v := parquet.ValueOf(key)
	for _, rg := range f.RowGroups() {
		bf := rg.ColumnChunks()[colIdx].BloomFilter()
		if bf == nil {
			continue
		}
		ok, err := bf.Check(v)
		if err != nil {
			t.Fatalf("bloom check: %v", err)
		}
		if ok {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Deterministic datasets. No rand, no time.Now, no map iteration in the
// values themselves — the same input rows on any machine, any Go version.
// ---------------------------------------------------------------------------

func goldenLogRows(n int) []schema.LogRow {
	const base = int64(1_760_000_000_000_000_000)
	rows := make([]schema.LogRow, n)
	for i := range rows {
		svc := fmt.Sprintf("svc-%02d", i%12)
		ns := fmt.Sprintf("ns-%d", i%4)
		rows[i] = schema.LogRow{
			AccountID:         uint32(i % 3),
			ProjectID:         uint32(i % 5),
			TimestampUnixNano: base + int64(i)*1_000_000,
			// Multi-line body: the `_msg` shape LH actually stores for
			// stack traces; exercises the plain byte-array path with
			// embedded newlines.
			Body: fmt.Sprintf("processed request %d\n\tat handler.serve(handler.go:%d)\n\tat mux.dispatch(mux.go:%d)",
				i, i%400, i%77),
			SeverityText:      []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}[i%5],
			SeverityNumber:    int32(5 + 4*(i%5)),
			ServiceName:       svc,
			TraceID:           fmt.Sprintf("%032x", i),
			SpanID:            fmt.Sprintf("%016x", i),
			K8sNamespaceName:  ns,
			K8sPodName:        fmt.Sprintf("%s-pod-%d", svc, i%50),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-%d", i%8),
			DeployEnv:         []string{"prod", "staging"}[i%2],
			CloudRegion:       []string{"eu-west-1", "us-east-1"}[i%2],
			HostName:          fmt.Sprintf("host-%d", i%8),
			Stream:            fmt.Sprintf("{service.name=%q,k8s.namespace.name=%q}", svc, ns),
			StreamID:          fmt.Sprintf("stream-%04d", i%24),
			ScopeName:         fmt.Sprintf("scope-%d", i%3),
			ServiceVersion:    fmt.Sprintf("1.%d.%d", i%3, i%7),
			ContainerID:       fmt.Sprintf("%064x", i),
			ResourceAttributes: map[string]string{
				"telemetry.sdk.language": "go",
				"cloud.zone":             fmt.Sprintf("zone-%d", i%3),
			},
			LogAttributes: map[string]string{
				"http.path":  fmt.Sprintf("/api/v%d/items", i%5),
				"request.id": fmt.Sprintf("req-%08d", i),
			},
			ScopeAttributes: map[string]string{
				"lib.version": fmt.Sprintf("1.2.%d", i%4),
			},
		}
	}
	return rows
}

// goldenHighCardLogRows gives service.name — a `,dict` column that is ALSO a
// bloom column — a distinct value per row, so a 200k-row file pushes the
// dictionary past the fallback threshold (parquet-go #536) while the
// row-group blooms are sized at the row-group row limit (#578).
func goldenHighCardLogRows(n int) []schema.LogRow {
	const base = int64(1_770_000_000_000_000_000)
	rows := make([]schema.LogRow, n)
	for i := range rows {
		rows[i] = schema.LogRow{
			AccountID:         1,
			ProjectID:         1,
			TimestampUnixNano: base + int64(i)*1_000,
			Body:              fmt.Sprintf("event %d", i),
			SeverityText:      "INFO",
			SeverityNumber:    9,
			ServiceName:       highCardValue(i),
			TraceID:           fmt.Sprintf("%032x", i),
			SpanID:            fmt.Sprintf("%016x", i),
			Stream:            `{service.name="highcard"}`,
			StreamID:          "stream-0000",
		}
	}
	return rows
}

// highCardValue is the canonical distinct value for row i of the
// high-cardinality datasets. Shared with the dictionary-fallback regression
// test so both probe the same key space.
func highCardValue(i int) string {
	return fmt.Sprintf("svc-%08d-%s", i, strings.Repeat("x", 1+i%7))
}

func goldenTraceRows(n int) []schema.TraceRow {
	const base = int64(1_760_000_000_000_000_000)
	rows := make([]schema.TraceRow, n)
	for i := range rows {
		svc := fmt.Sprintf("svc-%02d", i%12)
		rows[i] = schema.TraceRow{
			AccountID:         uint32(i % 3),
			ProjectID:         uint32(i % 5),
			TimestampUnixNano: base + int64(i)*1_000_000,
			StartTimeUnixNano: base + int64(i)*1_000_000,
			TraceID:           fmt.Sprintf("%032x", i/4),
			SpanID:            fmt.Sprintf("%016x", i),
			ParentSpanID:      fmt.Sprintf("%016x", i/4),
			SpanName:          fmt.Sprintf("op-%d", i%40),
			ServiceName:       svc,
			DurationNs:        int64(1_000 * (1 + i%900)),
			StatusCode:        int32(i % 3),
			StatusMessage:     []string{"", "deadline exceeded", "ok"}[i%3],
			SpanKind:          int32(1 + i%5),
			HTTPMethod:        []string{"GET", "POST", "PUT", "DELETE"}[i%4],
			HTTPStatusCode:    []string{"200", "404", "500"}[i%3],
			HTTPUrl:           fmt.Sprintf("https://example.test/api/v%d/items/%d", i%5, i),
			DBSystem:          []string{"", "postgresql", "redis"}[i%3],
			DBStatement:       fmt.Sprintf("SELECT * FROM items WHERE id = %d", i),
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
			URLFull:           fmt.Sprintf("https://example.test/api/v%d/items/%d", i%5, i),
			ClientAddress:     fmt.Sprintf("10.%d.%d.%d", i%256, (i/256)%256, i%251),
			ContainerID:       fmt.Sprintf("%064x", i),
			ResourceAttributes: map[string]string{
				"telemetry.sdk.language": "go",
				"cloud.zone":             fmt.Sprintf("zone-%d", i%3),
			},
			SpanAttributes: map[string]string{
				"peer.service": fmt.Sprintf("peer-%d", i%9),
				"retry.count":  fmt.Sprintf("%d", i%3),
			},
			ScopeAttributes: map[string]string{
				"lib.version": fmt.Sprintf("1.2.%d", i%4),
			},
		}
	}
	return rows
}

// absentKeys builds a fixed sample of values guaranteed not to be in any
// dataset (the "absent-" prefix appears in no generator above).
func absentKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("absent-%06d-not-in-any-dataset", i)
	}
	return out
}

func sampleEvery(values []string, step int) []string {
	out := make([]string, 0, len(values)/step+1)
	for i := 0; i < len(values); i += step {
		out = append(out, values[i])
	}
	return out
}

// buildGoldenFiles writes every dataset with the production writer and
// profiles it. Called by both the assert and the update paths so the two can
// never diverge.
func buildGoldenFiles(t *testing.T) []goldenFile {
	t.Helper()

	// The writer reads the Tier-2 slot binding from a package global; pin it
	// so the golden never depends on test ordering.
	saved := activeSlotResolver
	activeSlotResolver = nil
	t.Cleanup(func() { activeSlotResolver = saved })

	var files []goldenFile

	// 1. logs-basic — 3 full row groups + a partial one, so later row groups
	//    cannot overwrite earlier page counts unnoticed (#577).
	logRows := goldenLogRows(3*goldenRowGroupSize + 1234)
	logRes, err := writeLogsParquet(logRows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet(logs-basic): %v", err)
	}
	presentSvc := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		presentSvc = append(presentSvc, fmt.Sprintf("svc-%02d", i))
	}
	presentTID := make([]string, 0, 64)
	for i := 0; i < len(logRows); i += len(logRows) / 64 {
		presentTID = append(presentTID, fmt.Sprintf("%032x", i))
	}
	files = append(files, profileParquet(t, "logs-basic", logRes.Data, "timestamp_unix_nano", goldenProbeSet{
		present: map[string][]string{"service.name": presentSvc, "trace_id": presentTID},
		absent:  map[string][]string{"service.name": absentKeys(512), "trace_id": absentKeys(512)},
	}))

	// 2. logs-highcard — 200k distinct values in a `,dict` bloom column.
	highRows := goldenHighCardLogRows(200_000)
	highRes, err := writeLogsParquet(highRows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeLogsParquet(logs-highcard): %v", err)
	}
	highPresent := make([]string, 0, 2048)
	for i := 0; i < len(highRows); i += 100 {
		highPresent = append(highPresent, highCardValue(i))
	}
	files = append(files, profileParquet(t, "logs-highcard", highRes.Data, "timestamp_unix_nano", goldenProbeSet{
		present: map[string][]string{"service.name": highPresent},
		absent:  map[string][]string{"service.name": absentKeys(2048)},
	}))

	// 3. traces-basic — the trace writer shape, including the `_trace_idx`
	//    footer KV and the trace-specific bloom set.
	traceRows := goldenTraceRows(2*goldenRowGroupSize + 777)
	traceRes, err := writeTracesParquet(traceRows, goldenRowGroupSize, goldenCompressionLevel)
	if err != nil {
		t.Fatalf("writeTracesParquet(traces-basic): %v", err)
	}
	traceTIDs := make([]string, 0, 64)
	for i := 0; i < len(traceRows)/4; i += (len(traceRows) / 4) / 64 {
		traceTIDs = append(traceTIDs, fmt.Sprintf("%032x", i))
	}
	files = append(files, profileParquet(t, "traces-basic", traceRes.Data, "timestamp_unix_nano", goldenProbeSet{
		present: map[string][]string{"service.name": sampleEvery(presentSvc, 1), "trace_id": traceTIDs},
		absent:  map[string][]string{"service.name": absentKeys(512), "trace_id": absentKeys(512)},
	}))

	return files
}

func TestReadbackGolden(t *testing.T) {
	files := buildGoldenFiles(t)

	if os.Getenv("PARQUET_GOLDEN_UPDATE") == "1" {
		set := goldenSet{ParquetGoVersion: parquetGoVersion(t), Files: files}
		data, err := json.MarshalIndent(set, "", "  ")
		if err != nil {
			t.Fatalf("marshal golden: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(readbackGoldenPath), 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(readbackGoldenPath, append(data, '\n'), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden regenerated at %s (parquet-go %s)", readbackGoldenPath, set.ParquetGoVersion)
		return
	}

	raw, err := os.ReadFile(readbackGoldenPath)
	if err != nil {
		t.Fatalf("read golden (regenerate with PARQUET_GOLDEN_UPDATE=1): %v", err)
	}
	var want goldenSet
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	if len(want.Files) != len(files) {
		t.Fatalf("golden has %d files, test produced %d", len(want.Files), len(files))
	}
	for i := range files {
		// diffGolden (readback_reproducibility_test.go) names each field that
		// moved instead of printing two multi-thousand-line JSON blobs, so a
		// parquet-go bump reports a diagnosis rather than homework. Its own
		// sensitivity is proved by TestGoldenDiffNamesTamperedFields.
		if diffs := diffGolden(want.Files[i], files[i]); len(diffs) != 0 {
			t.Errorf("readback golden mismatch for %s (golden written by parquet-go %s), %d field(s) changed:\n  %s",
				want.Files[i].Name, want.ParquetGoVersion, len(diffs), strings.Join(diffs, "\n  "))
		}
	}
}

// parquetGoVersion reads the parquet-go version out of the module's build
// info so the golden records what produced it without a hand-maintained
// constant.
func parquetGoVersion(t *testing.T) string {
	t.Helper()
	v, err := moduleVersion("github.com/parquet-go/parquet-go")
	if err != nil {
		t.Fatalf("resolve parquet-go version: %v", err)
	}
	return v
}

func moduleVersion(path string) (string, error) {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, d := range info.Deps {
			if d.Path == path {
				return d.Version, nil
			}
		}
	}
	// Test binaries do not always carry dependency build info; fall back to
	// the module's own go.mod, walking up from the package directory.
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) >= 2 && fields[0] == path {
					return fields[1], nil
				}
			}
			return "", fmt.Errorf("module %s not required by %s/go.mod", path, dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod above working directory")
		}
		dir = parent
	}
}
