package parquets3

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// countOnlyPlan is the plan a bare `* | stats count()` produces: metadata may
// answer the query and no `_time` bucketing constrains which files qualify.
var countOnlyPlan = metadataOnlyPlan{eligible: true}

// ---------------------------------------------------------------------------
// Decision table: which queries may be answered without reading Parquet
// ---------------------------------------------------------------------------

// TestPlanMetadataOnly_DecisionTable enumerates every branch of the
// classifier. A metadata-only block carries one column (`_time`) holding one
// constant value, so it is an honest stand-in ONLY while the query cannot tell
// the rows apart. Each row below states which branch of
// GetQueryTimeBucketing the query lands in and why.
func TestPlanMetadataOnly_DecisionTable(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantEligibl bool
		wantBuckets int
		why         string
	}{
		// --- eligible: the answer is a pure row count -------------------
		{"count", `* | stats count()`, true, 0, "no by-fields, no per-row value read"},
		{"count_named", `* | stats count() total`, true, 0, "renaming the aggregate changes nothing"},
		{"count_bucket_1h", `* | stats by (_time:1h) count()`, true, 1, "grouped only by a bucketed _time"},
		{"count_bucket_1d", `* | stats by (_time:1d) count()`, true, 1, "calendar-day bucket, same rule"},
		{"count_bucket_offset", `* | stats by (_time:1h offset 30m) count()`, true, 1, "bucket offset kept for truncation"},
		{"count_bucket_then_sort", `* | stats by (_time:1h) count() | sort by (_time)`, true, 1, "the sort runs on group keys, not raw rows"},
		{"count_bucket_then_limit", `* | stats by (_time:1h) count() | limit 5`, true, 1, "post-stats pipes never see raw rows"},
		{"stats_count_if", `* | stats count() if (service.name:="api") hits`, false, 0, "the per-function if-filter reads a column"},

		// --- not eligible: a per-row value is needed --------------------
		{"bare_filter", `*`, false, 0, "no stats pipe: the rows themselves are the answer"},
		{"sort_by_time", `* | sort by (_time)`, false, 0, "ordering needs each row's real timestamp"},
		{"fields_msg", `* | fields _msg`, false, 0, "projects a column metadata does not hold"},
		{"stats_by_time_unbucketed", `* | stats by (_time) count()`, false, 0, "every distinct timestamp becomes its own group"},
		{"stats_by_field", `* | stats by (service.name) count()`, false, 0, "groups by a column metadata does not hold"},
		{"stats_by_time_and_field", `* | stats by (_time:1h, service.name) count()`, false, 0, "same, alongside a legal bucket"},
		{"stats_sum_field", `* | stats sum(duration)`, false, 0, "the aggregate reads a value column"},
		{"stats_count_field", `* | stats count(service.name)`, false, 0, "counts non-empty values of a column"},
		{"stats_count_uniq_field", `* | stats count_uniq(trace_id)`, false, 0, "needs the distinct values themselves"},
		{"uniq_pipe", `* | uniq by (service.name)`, false, 0, "a uniq pipe ahead of any stats reads a column"},
		{"top_pipe", `* | top 5 by (service.name)`, false, 0, "same"},
		{"sort_then_stats", `* | sort by (_time) | limit 1 | stats count()`, false, 0, "a pre-stats sort keeps a different row when timestamps are real"},
		{"limit_then_stats", `* | limit 10 | stats count()`, false, 0, "any pipe ahead of stats disqualifies: raw rows reach it"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planMetadataOnly(mustParseQuery(t, tt.query))
			if plan.eligible != tt.wantEligibl {
				t.Fatalf("eligible = %v, want %v (%s)\n  query: %s", plan.eligible, tt.wantEligibl, tt.why, tt.query)
			}
			if len(plan.buckets) != tt.wantBuckets {
				t.Errorf("buckets = %d, want %d (%s)\n  query: %s", len(plan.buckets), tt.wantBuckets, tt.why, tt.query)
			}
		})
	}
}

// TestPlanMetadataOnly_RowFilterIsTheOtherGate documents the second gate.
// A leading `| filter ...` pipe is folded into the query's top-level filter by
// VL's parser, so the pipe shape alone still classifies as eligible — it is
// RunQuery's `filter == nil` condition that keeps such a query off the
// metadata-only path, because metadata cannot evaluate a row filter.
func TestPlanMetadataOnly_RowFilterIsTheOtherGate(t *testing.T) {
	q := mustParseQuery(t, `* | filter service.name:="api" | stats count()`)
	if !planMetadataOnly(q).eligible {
		t.Fatal("fixture changed: the folded query no longer classifies on pipe shape alone")
	}
	if parseFilterFromQuery(q) == nil {
		t.Error("a folded row filter must be visible to RunQuery, which refuses the fast path when one is present")
	}
	if parseFilterFromQuery(mustParseQuery(t, `* | stats count()`)) != nil {
		t.Error("an unfiltered query must not report a row filter")
	}
}

// TestPlanMetadataOnly_NilQuery locks the safe default: an absent query is
// never eligible.
func TestPlanMetadataOnly_NilQuery(t *testing.T) {
	if planMetadataOnly(nil).eligible {
		t.Error("a nil query must not be eligible for the metadata-only path")
	}
}

// TestMetadataOnlyPlan_FromContextDefaultsToDisabled locks the other safe
// default: a reader that never got a plan must not serve blocks it did not
// read.
func TestMetadataOnlyPlan_FromContextDefaultsToDisabled(t *testing.T) {
	if metadataOnlyPlanFromContext(context.Background()).eligible {
		t.Error("a context with no plan must yield a disabled plan")
	}
	p := metadataOnlyPlan{eligible: true, buckets: []logstorage.TimeBucket{{SizeStr: "1h", Size: int64(time.Hour)}}}
	got := metadataOnlyPlanFromContext(withMetadataOnlyPlan(context.Background(), p))
	if !got.eligible || len(got.buckets) != 1 {
		t.Errorf("plan did not survive the context round trip: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Bucket containment: which FILES a bucketed query may take from metadata
// ---------------------------------------------------------------------------

const testBase = int64(1767225600_000000000) // 2026-01-01T00:00:00Z

// TestMetadataOnlyPlan_CoversSpan walks the containment rule that keeps
// bucketed counts exact: a file may be answered from metadata only when its
// whole span falls in ONE bucket. A file straddling a bucket boundary must be
// read, because a constant timestamp would pile all its rows into one bucket
// and empty the other — the fabricated-distribution bug this rule prevents.
func TestMetadataOnlyPlan_CoversSpan(t *testing.T) {
	hour := testBase
	tests := []struct {
		name   string
		query  string
		minNs  int64
		maxNs  int64
		covers bool
	}{
		{"no bucket, any span", `* | stats count()`, hour, hour + int64(72*time.Hour), true},
		{"1h bucket, inside one hour", `* | stats by (_time:1h) count()`, hour + int64(time.Minute), hour + int64(59*time.Minute), true},
		{"1h bucket, exactly one hour", `* | stats by (_time:1h) count()`, hour, hour + int64(time.Hour) - 1, true},
		{"1h bucket, straddles the boundary", `* | stats by (_time:1h) count()`, hour + int64(59*time.Minute), hour + int64(61*time.Minute), false},
		{"1h bucket, spans days", `* | stats by (_time:1h) count()`, hour, hour + int64(30*time.Hour), false},
		{"5m bucket, inside", `* | stats by (_time:5m) count()`, hour, hour + int64(4*time.Minute), true},
		{"5m bucket, straddles", `* | stats by (_time:5m) count()`, hour + int64(4*time.Minute), hour + int64(6*time.Minute), false},
		{"1m bucket, straddles", `* | stats by (_time:1m) count()`, hour, hour + int64(90*time.Second), false},
		{"5s bucket, inside", `* | stats by (_time:5s) count()`, hour, hour + int64(4*time.Second), true},
		{"5s bucket, straddles", `* | stats by (_time:5s) count()`, hour + int64(4*time.Second), hour + int64(6*time.Second), false},
		{"1d bucket, inside one day", `* | stats by (_time:1d) count()`, hour, hour + int64(23*time.Hour), true},
		{"1d bucket, spans two days", `* | stats by (_time:1d) count()`, hour + int64(23*time.Hour), hour + int64(25*time.Hour), false},
		{"ineligible query never covers", `*`, hour, hour, false},
		{"inverted span never covers", `* | stats count()`, hour + 1, hour, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planMetadataOnly(mustParseQuery(t, tt.query))
			if got := plan.coversSpan(tt.minNs, tt.maxNs); got != tt.covers {
				t.Errorf("coversSpan(%d, %d) = %v, want %v (query %s)", tt.minNs, tt.maxNs, got, tt.covers, tt.query)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Exactness matrix — fast path + scan of what it left == pure scan
// ---------------------------------------------------------------------------

// fakeFile is a file whose CONTENT is known to the test: trueTimes are the
// timestamps a real scan would read, and the manifest entry is derived from
// them so metadata and data agree the way the writer guarantees they do.
type fakeFile struct {
	fi        manifest.FileInfo
	trueTimes []int64
	// services holds each row's service.name, so the scan oracle can also answer
	// queries that group by a column (hits with `field=`), which metadata can't.
	services []string
}

// fakeServiceField is the column the oracle carries besides the timestamp.
const fakeServiceField = "service.name"

// newFakeFile spreads rowCount timestamps over [minNs, maxNs] in a
// deliberately NON-uniform pattern, so a fast path that fabricates an evenly
// spaced series cannot accidentally agree with the truth.
func newFakeFile(key string, rowCount int64, minNs, maxNs int64) fakeFile {
	times := make([]int64, rowCount)
	services := make([]string, rowCount)
	span := maxNs - minNs
	for i := range times {
		services[i] = "svc-" + strconv.Itoa(i%3)
		if rowCount == 1 || span == 0 {
			times[i] = minNs
			continue
		}
		// Cluster rows towards the start of the span, then force the
		// extremes so the manifest bounds are the true bounds.
		off := (int64(i) * int64(i)) % (span + 1)
		times[i] = minNs + off
	}
	if rowCount > 0 {
		times[0] = minNs
		times[rowCount-1] = maxNs
	}
	return fakeFile{
		fi:        manifest.FileInfo{Key: key, Size: rowCount * 64, RowCount: rowCount, MinTimeNs: minNs, MaxTimeNs: maxNs},
		trueTimes: times,
		services:  services,
	}
}

func fileInfos(files []fakeFile) []manifest.FileInfo {
	out := make([]manifest.FileInfo, len(files))
	for i, f := range files {
		out[i] = f.fi
	}
	return out
}

// emitTrueRows writes the file's real rows the way a Parquet scan would — one
// formatted timestamp per row plus its service.name — filtered to the window.
func (s *Storage) emitTrueRows(f fakeFile, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc) {
	name := s.timestampFieldName()
	times := make([]string, 0, syntheticChunkSize)
	services := make([]string, 0, syntheticChunkSize)
	flush := func() {
		if len(times) == 0 {
			return
		}
		db := &logstorage.DataBlock{}
		db.SetColumns([]logstorage.BlockColumn{
			{Name: name, Values: times},
			{Name: fakeServiceField, Values: services},
		})
		writeBlock(0, db)
		times = make([]string, 0, syntheticChunkSize)
		services = make([]string, 0, syntheticChunkSize)
	}
	for i, ts := range f.trueTimes {
		if ts < startNs || ts > endNs {
			continue
		}
		times = append(times, s.registry.FormatField(name, ts))
		services = append(services, f.services[i])
		if len(times) >= syntheticChunkSize {
			flush()
		}
	}
	flush()
}

// runPipes feeds the blocks produced by search through VL's own pipe
// machinery and returns the result rows as "col=value,col=value" strings.
func runPipes(t *testing.T, queryStr string, search func(writeBlock logstorage.WriteDataBlockFunc)) []string {
	t.Helper()
	q := mustParseQuery(t, queryStr)
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)

	// VL runs the pipe pipeline with several workers, so the result writer can
	// be called concurrently — collect under a lock.
	var mu sync.Mutex
	var rows []string
	err := logstorage.RunQueryExternal(qctx,
		func(wb logstorage.WriteDataBlockFunc) error {
			search(wb)
			return nil
		},
		func(_ uint, db *logstorage.DataBlock) {
			mu.Lock()
			defer mu.Unlock()
			cols := db.GetColumns(true)
			for i := 0; i < db.RowsCount(); i++ {
				parts := make([]string, 0, len(cols))
				for _, c := range cols {
					parts = append(parts, c.Name+"="+strings.Clone(c.Values[i]))
				}
				rows = append(rows, strings.Join(parts, ","))
			}
		})
	if err != nil {
		t.Fatalf("RunQueryExternal(%q): %s", queryStr, err)
	}
	sort.Strings(rows)
	return rows
}

// answerViaFastPath runs the query with the manifest fast path enabled: files
// it can serve come from metadata, the rest are scanned for real. It also
// returns how many files were answered from metadata, so a test can prove the
// comparison is not vacuous.
func answerViaFastPath(t *testing.T, s *Storage, queryStr string, files []fakeFile, startNs, endNs int64) ([]string, int) {
	t.Helper()
	plan := planMetadataOnly(mustParseQuery(t, queryStr))
	served := 0
	rows := runPipes(t, queryStr, func(wb logstorage.WriteDataBlockFunc) {
		remaining := s.manifestFastPath(context.Background(), fileInfos(files), startNs, endNs, plan, wb)
		served = len(files) - len(remaining)
		byKey := map[string]fakeFile{}
		for _, f := range files {
			byKey[f.fi.Key] = f
		}
		for _, fi := range remaining {
			s.emitTrueRows(byKey[fi.Key], startNs, endNs, wb)
		}
	})
	return rows, served
}

// wantServedFromMetadata is an INDEPENDENT restatement of the containment rule
// (plain epoch-aligned truncation, not VL's), used to cross-check the
// production decision and to keep the exactness matrix from passing vacuously.
func wantServedFromMetadata(fi manifest.FileInfo, startNs, endNs int64, bucketNs int64) bool {
	if fi.RowCount <= 0 || fi.MinTimeNs <= 0 || fi.MaxTimeNs <= 0 {
		return false
	}
	if fi.MinTimeNs < startNs || fi.MaxTimeNs > endNs {
		return false
	}
	if bucketNs <= 0 {
		return true
	}
	return fi.MinTimeNs/bucketNs == fi.MaxTimeNs/bucketNs
}

// answerViaScan is the oracle: every file is read for real.
func answerViaScan(t *testing.T, s *Storage, queryStr string, files []fakeFile, startNs, endNs int64) []string {
	t.Helper()
	return runPipes(t, queryStr, func(wb logstorage.WriteDataBlockFunc) {
		for _, f := range files {
			s.emitTrueRows(f, startNs, endNs, wb)
		}
	})
}

// TestManifestFastPath_ExactnessMatrix is the core guarantee: for every
// combination of file size, window coverage and query shape, answering part of
// the query from manifest metadata must return EXACTLY what a full scan
// returns.
func TestManifestFastPath_ExactnessMatrix(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)

	// File spans, relative to testBase, paired with the window they are
	// queried under. Sizes stay at oracle-affordable row counts; the
	// million-row cells are covered by TestManifestFastPath_ExactAboveOneMillion.
	// 0 is a manifest entry with no row count yet (not enriched from the footer):
	// it must be read, never answered as an empty file.
	rowCounts := []int64{0, 1, 2, 9_999, 10_000, 10_001}
	coverage := []struct {
		name           string
		minOff, maxOff int64
		startOff       int64
		endOff         int64
	}{
		{"file fully inside window", 10 * hour, 10*hour + 30*int64(time.Minute), 0, 24 * hour},
		{"file straddling window start", -1 * hour, 30 * int64(time.Minute), 0, 24 * hour},
		{"file straddling window end", 23 * hour, 25 * hour, 0, 24 * hour},
		{"file exactly on window bounds", 0, 24 * hour, 0, 24 * hour},
		{"file outside window", 48 * hour, 49 * hour, 0, 24 * hour},
		{"file spanning two hours", 10*hour + 30*int64(time.Minute), 11*hour + 30*int64(time.Minute), 0, 24 * hour},
	}
	queries := []struct {
		q        string
		bucketNs int64
	}{
		{`* | stats count()`, 0},
		{`* | stats by (_time:1h) count()`, hour},
		{`* | stats by (_time:30m) count()`, int64(30 * time.Minute)},
		{`* | stats by (_time:1m) count()`, int64(time.Minute)},
		{`* | stats by (_time:5s) count()`, int64(5 * time.Second)},
		{`* | stats by (_time:1d) count()`, 24 * hour},
		{`* | stats by (_time:1h) count() | sort by (_time)`, hour},
	}

	for _, rc := range rowCounts {
		for _, cov := range coverage {
			f := newFakeFile("f.parquet", rc, testBase+cov.minOff, testBase+cov.maxOff)
			files := []fakeFile{f}
			startNs, endNs := testBase+cov.startOff, testBase+cov.endOff
			for _, qc := range queries {
				name := fmt.Sprintf("rows=%d/%s/%s", rc, cov.name, qc.q)
				t.Run(name, func(t *testing.T) {
					want := answerViaScan(t, s, qc.q, files, startNs, endNs)
					got, served := answerViaFastPath(t, s, qc.q, files, startNs, endNs)
					if len(got) != len(want) {
						t.Fatalf("fast path returned %d rows, scan returned %d\n got: %v\nwant: %v", len(got), len(want), got, want)
					}
					for i := range want {
						if got[i] != want[i] {
							t.Fatalf("row %d: fast path %q, scan %q\n got: %v\nwant: %v", i, got[i], want[i], got, want)
						}
					}
					wantServed := 0
					if wantServedFromMetadata(f.fi, startNs, endNs, qc.bucketNs) {
						wantServed = 1
					}
					if served != wantServed {
						t.Errorf("served %d files from metadata, want %d (file [%d,%d] window [%d,%d] bucket %dns)",
							served, wantServed, f.fi.MinTimeNs, f.fi.MaxTimeNs, startNs, endNs, qc.bucketNs)
					}
				})
			}
		}
	}
}

// TestManifestFastPath_ExactnessMatrix_MultiFile repeats the matrix with a mix
// of files per query, so per-bucket counts must add up across files that are
// served from metadata and files that are scanned.
func TestManifestFastPath_ExactnessMatrix_MultiFile(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	files := []fakeFile{
		newFakeFile("inside-1h.parquet", 500, testBase+1*hour, testBase+1*hour+int64(45*time.Minute)),
		newFakeFile("inside-other-hour.parquet", 700, testBase+3*hour, testBase+3*hour+int64(10*time.Minute)),
		newFakeFile("straddles-hour.parquet", 300, testBase+5*hour+int64(50*time.Minute), testBase+6*hour+int64(10*time.Minute)),
		newFakeFile("straddles-window-start.parquet", 200, testBase-int64(30*time.Minute), testBase+int64(30*time.Minute)),
		newFakeFile("single-row.parquet", 1, testBase+7*hour, testBase+7*hour),
	}
	startNs, endNs := testBase, testBase+24*hour

	for _, q := range []string{
		`* | stats count()`,
		`* | stats by (_time:1h) count()`,
		`* | stats by (_time:15m) count()`,
		`* | stats by (_time:1d) count()`,
	} {
		t.Run(q, func(t *testing.T) {
			want := answerViaScan(t, s, q, files, startNs, endNs)
			got, served := answerViaFastPath(t, s, q, files, startNs, endNs)
			if strings.Join(got, ";") != strings.Join(want, ";") {
				t.Errorf("fast path != scan\n got: %v\nwant: %v", got, want)
			}
			if served == 0 {
				t.Error("no file was answered from metadata — the comparison proves nothing")
			}
		})
	}
}

// TestManifestFastPath_ExactAboveOneMillion is the regression for the reported
// bug: files above the old maxSyntheticRows = 1_000_000 cap were silently
// under-counted. The oracle here is manifest arithmetic — the file's whole
// span sits inside the window, so count() is exactly RowCount.
func TestManifestFastPath_ExactAboveOneMillion(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)

	for _, rowCount := range []int64{999_999, 1_000_000, 1_000_001, 1_500_000, 5_000_000} {
		t.Run(strconv.FormatInt(rowCount, 10), func(t *testing.T) {
			fi := manifest.FileInfo{
				Key:       "big.parquet",
				RowCount:  rowCount,
				MinTimeNs: testBase + 10*hour,
				MaxTimeNs: testBase + 10*hour + int64(30*time.Minute),
			}
			rows := runPipes(t, `* | stats count()`, func(wb logstorage.WriteDataBlockFunc) {
				remaining := s.manifestFastPath(context.Background(), []manifest.FileInfo{fi}, testBase, testBase+24*hour, countOnlyPlan, wb)
				if len(remaining) != 0 {
					t.Fatalf("file was not served from metadata: %v", remaining)
				}
			})
			want := []string{"count(*)=" + strconv.FormatInt(rowCount, 10)}
			if len(rows) != 1 || rows[0] != want[0] {
				t.Fatalf("count() = %v, want %v", rows, want)
			}
		})
	}
}

// TestManifestFastPath_ExactAboveOneMillion_Bucketed repeats it for a
// histogram query whose file sits inside one bucket.
func TestManifestFastPath_ExactAboveOneMillion_Bucketed(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	const rowCount = int64(1_500_000)

	fi := manifest.FileInfo{
		Key:       "big.parquet",
		RowCount:  rowCount,
		MinTimeNs: testBase + 10*hour + int64(time.Minute),
		MaxTimeNs: testBase + 10*hour + int64(59*time.Minute),
	}
	q := `* | stats by (_time:1h) count()`
	plan := planMetadataOnly(mustParseQuery(t, q))
	rows := runPipes(t, q, func(wb logstorage.WriteDataBlockFunc) {
		remaining := s.manifestFastPath(context.Background(), []manifest.FileInfo{fi}, testBase, testBase+24*hour, plan, wb)
		if len(remaining) != 0 {
			t.Fatalf("file inside a single 1h bucket was not served from metadata: %v", remaining)
		}
	})
	if len(rows) != 1 {
		t.Fatalf("expected exactly one bucket, got %v", rows)
	}
	if !strings.HasSuffix(rows[0], "count(*)="+strconv.FormatInt(rowCount, 10)) {
		t.Errorf("bucket row = %q, want count(*)=%d", rows[0], rowCount)
	}
}

// ---------------------------------------------------------------------------
// Property test
// ---------------------------------------------------------------------------

// TestManifestFastPath_PropertyRandomManifests generates random files, windows
// and bucket steps, and requires the fast-path answer to equal the scan answer
// every time.
func TestManifestFastPath_PropertyRandomManifests(t *testing.T) {
	s := testStorage()
	rng := rand.New(rand.NewSource(20260913))
	steps := []string{"", "5s", "1m", "15m", "1h", "1d"}
	day := int64(24 * time.Hour)

	for iter := 0; iter < 200; iter++ {
		nFiles := 1 + rng.Intn(5)
		files := make([]fakeFile, nFiles)
		for i := range files {
			minOff := rng.Int63n(day)
			span := rng.Int63n(int64(3 * time.Hour))
			rowCount := int64(1 + rng.Intn(400))
			files[i] = newFakeFile(fmt.Sprintf("f%d.parquet", i), rowCount, testBase+minOff, testBase+minOff+span)
		}
		startOff := rng.Int63n(day / 2)
		endOff := startOff + rng.Int63n(day)
		startNs, endNs := testBase+startOff, testBase+endOff

		step := steps[rng.Intn(len(steps))]
		q := `* | stats count()`
		if step != "" {
			q = `* | stats by (_time:` + step + `) count()`
		}

		want := answerViaScan(t, s, q, files, startNs, endNs)
		got, _ := answerViaFastPath(t, s, q, files, startNs, endNs)
		if strings.Join(got, ";") != strings.Join(want, ";") {
			t.Fatalf("iteration %d: fast path != scan\nquery: %s\nwindow: [%d,%d]\nfiles: %+v\n got: %v\nwant: %v",
				iter, q, startNs, endNs, fileInfos(files), got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Safeguards: fallback decisions, metrics, allocation ceiling
// ---------------------------------------------------------------------------

// TestManifestFastPath_SubBucketFileFallsBack asserts the fallback is actually
// TAKEN — the file is handed back for a real read — when a bucket boundary
// falls inside it, and that the decision is visible in the metrics.
func TestManifestFastPath_SubBucketFileFallsBack(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)

	served := manifest.FileInfo{Key: "one-bucket", RowCount: 10, MinTimeNs: testBase + hour, MaxTimeNs: testBase + hour + int64(30*time.Minute)}
	straddles := manifest.FileInfo{Key: "two-buckets", RowCount: 10, MinTimeNs: testBase + hour + int64(50*time.Minute), MaxTimeNs: testBase + 2*hour + int64(10*time.Minute)}

	plan := planMetadataOnly(mustParseQuery(t, `* | stats by (_time:1h) count()`))

	beforeServed := metrics.MetadataOnlyFiles.Get()
	beforeFallback := metrics.MetadataOnlyFallbackFiles.Get()

	var emittedRows int
	remaining := s.manifestFastPath(context.Background(), []manifest.FileInfo{served, straddles}, testBase, testBase+24*hour, plan,
		func(_ uint, db *logstorage.DataBlock) { emittedRows += db.RowsCount() })

	if len(remaining) != 1 || remaining[0].Key != "two-buckets" {
		t.Fatalf("remaining = %+v, want only the bucket-straddling file", remaining)
	}
	if emittedRows != 10 {
		t.Errorf("emitted %d rows, want 10 (only the single-bucket file)", emittedRows)
	}
	if got := metrics.MetadataOnlyFiles.Get() - beforeServed; got != 1 {
		t.Errorf("MetadataOnlyFiles delta = %d, want 1", got)
	}
	if got := metrics.MetadataOnlyFallbackFiles.Get() - beforeFallback; got != 1 {
		t.Errorf("MetadataOnlyFallbackFiles delta = %d, want 1", got)
	}
}

// TestManifestFastPath_IneligibleQueryServesNothing locks the hard guard: when
// the query needs real row values, NO file may be answered from metadata,
// whatever its time span.
func TestManifestFastPath_IneligibleQueryServesNothing(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	files := []manifest.FileInfo{
		{Key: "a", RowCount: 10, MinTimeNs: testBase + hour, MaxTimeNs: testBase + hour + 1},
		{Key: "b", RowCount: 20, MinTimeNs: testBase + 2*hour, MaxTimeNs: testBase + 2*hour + 1},
	}
	for _, q := range []string{`*`, `* | sort by (_time)`, `* | stats by (_time) count()`, `* | stats by (service.name) count()`, `* | fields _msg`} {
		t.Run(q, func(t *testing.T) {
			plan := planMetadataOnly(mustParseQuery(t, q))
			emitted := 0
			remaining := s.manifestFastPath(context.Background(), files, testBase, testBase+24*hour, plan,
				func(_ uint, db *logstorage.DataBlock) { emitted += db.RowsCount() })
			if emitted != 0 {
				t.Errorf("emitted %d rows for a query that needs real values", emitted)
			}
			if len(remaining) != len(files) {
				t.Errorf("remaining = %d files, want all %d deferred to a real read", len(remaining), len(files))
			}
		})
	}
}

// TestManifestFastPath_ImplausibleRowCountIsRead locks the corruption
// safeguard: a manifest entry whose RowCount cannot be a row count is READ,
// not answered with a truncated number.
func TestManifestFastPath_ImplausibleRowCountIsRead(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{Key: "corrupt", RowCount: maxPlausibleRowCount + 1, MinTimeNs: testBase, MaxTimeNs: testBase + 1}
	emitted := 0
	remaining := s.manifestFastPath(context.Background(), []manifest.FileInfo{fi}, testBase-1, testBase+2, countOnlyPlan,
		func(_ uint, db *logstorage.DataBlock) { emitted += db.RowsCount() })
	if emitted != 0 {
		t.Errorf("emitted %d rows for an implausible row count", emitted)
	}
	if len(remaining) != 1 {
		t.Errorf("remaining = %d, want the file handed back for a real read", len(remaining))
	}
}

// TestStreamConstTimeBlocks_ConstantColumn locks the representation the whole
// optimisation rests on: one column, one distinct value, so VL stores it once
// and pipeStats answers from the row count.
func TestStreamConstTimeBlocks_ConstantColumn(t *testing.T) {
	s := testStorage()
	fi := manifest.FileInfo{RowCount: 25_000, MinTimeNs: testBase, MaxTimeNs: testBase + int64(time.Hour)}

	name := s.timestampFieldName()
	wantValue := s.registry.FormatField(name, fi.MinTimeNs)
	s.streamConstTimeBlocks(context.Background(), fi, func(_ uint, db *logstorage.DataBlock) {
		cols := db.GetColumns(false)
		if len(cols) != 1 || cols[0].Name != name {
			t.Fatalf("columns = %+v, want exactly one %q column", cols, name)
		}
		for i, v := range cols[0].Values {
			if v != wantValue {
				t.Fatalf("value %d = %q, want the constant %q", i, v, wantValue)
			}
		}
	})
}

// TestStreamConstTimeBlocks_AllocationCeiling is the guard against the O(rows)
// regression coming back: the number of allocations must not grow with
// RowCount. The old implementation allocated one formatted string per row.
func TestStreamConstTimeBlocks_AllocationCeiling(t *testing.T) {
	s := testStorage()
	drop := func(_ uint, _ *logstorage.DataBlock) {}

	small := manifest.FileInfo{RowCount: 10_000, MinTimeNs: testBase, MaxTimeNs: testBase + int64(time.Hour)}
	large := manifest.FileInfo{RowCount: 2_000_000, MinTimeNs: testBase, MaxTimeNs: testBase + int64(time.Hour)}

	ctx := context.Background()
	smallAllocs := testing.AllocsPerRun(3, func() { s.streamConstTimeBlocks(ctx, small, drop) })
	largeAllocs := testing.AllocsPerRun(3, func() { s.streamConstTimeBlocks(ctx, large, drop) })

	if largeAllocs > smallAllocs {
		t.Errorf("allocations grew with RowCount: %.0f for 10k rows, %.0f for 2M rows (200x the rows must not cost more allocations)",
			smallAllocs, largeAllocs)
	}
	if largeAllocs > 8 {
		t.Errorf("allocations per file = %.0f, want a small constant (<= 8)", largeAllocs)
	}
}

// TestManifestFastPath_ExactAtMaxRowsBudget locks how a truncated answer leaves
// the fast path. The query's budget cancels the context mid-answer (preFilter ->
// cancel) and streamConstTimeBlocks stops emitting; the fast path must then
// behave exactly like this module's scan branch rather than returning nil as if
// the short count were the whole answer.
//
// The traces module deliberately treats a MAX-ROWS cancellation as a truncation
// rather than an error — its Jaeger/Tempo search handlers use that limit as a
// result cap — so this test asserts that contract, and its sibling below asserts
// that any OTHER cancellation does propagate. The logs module has no such caller
// and returns the error unconditionally; see the same test there.
func TestManifestFastPath_ExactAtMaxRowsBudget(t *testing.T) {
	const maxRows = int64(5_000)

	s := testStorage()
	s.cfg.Query.MaxRows = maxRows

	start := time.Unix(0, testBase)
	partition := "dt=" + start.UTC().Format("2006-01-02") + "/hour=" + start.UTC().Format("15")
	s.manifest.AddFile(partition, manifest.FileInfo{
		Key:       "over-budget.parquet",
		Size:      64 * 1024 * 1024,
		RowCount:  maxRows * 100,
		MinTimeNs: testBase + int64(time.Minute),
		MaxTimeNs: testBase + int64(30*time.Minute),
	})

	q := mustParseQueryWithTime(t, "* | stats count()", testBase, testBase+int64(time.Hour))
	ctx := storage.WithTimestampOnlyHint(context.Background())

	var rows int64
	err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
		rows += int64(db.RowsCount())
	})
	if err != nil {
		t.Fatalf("RunQuery: %v — a max-rows truncation is this module's documented cap, not an error", err)
	}
	if rows > maxRows+syntheticChunkSize {
		t.Errorf("emitted %d rows, expected the max-rows cap to stop emission near %d", rows, maxRows)
	}
}

// TestManifestFastPath_CallerCancellationPropagates is the other half: a
// cancellation that is NOT the max-rows cap must reach the caller as an error
// instead of a silently short count.
func TestManifestFastPath_CallerCancellationPropagates(t *testing.T) {
	s := testStorage()
	s.cfg.Query.MaxRows = 0 // no cap, so only the caller can cancel

	start := time.Unix(0, testBase)
	partition := "dt=" + start.UTC().Format("2006-01-02") + "/hour=" + start.UTC().Format("15")
	s.manifest.AddFile(partition, manifest.FileInfo{
		Key:       "big.parquet",
		Size:      64 * 1024 * 1024,
		RowCount:  500_000,
		MinTimeNs: testBase + int64(time.Minute),
		MaxTimeNs: testBase + int64(30*time.Minute),
	})

	q := mustParseQueryWithTime(t, "* | stats count()", testBase, testBase+int64(time.Hour))
	ctx, cancel := context.WithCancel(storage.WithTimestampOnlyHint(context.Background()))

	var blocks int
	err := s.RunQuery(ctx, nil, q, func(_ uint, _ *logstorage.DataBlock) {
		blocks++
		if blocks == 2 {
			cancel()
		}
	})
	cancel()

	if err == nil {
		t.Fatal("RunQuery returned nil after the caller cancelled mid-answer; a short count must be an error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// ---------------------------------------------------------------------------
// hits: the exact query shape VictoriaLogs builds for /select/logsql/hits
// ---------------------------------------------------------------------------

// hitsQuery builds the query ProcessHitsRequest runs, through VL's own
// AddCountByTimePipe, so these tests track the real handler rather than a
// hand-written approximation of it.
func hitsQuery(t *testing.T, step time.Duration, fields []string) *logstorage.Query {
	t.Helper()
	q := mustParseQuery(t, "*")
	q.AddCountByTimePipe(int64(step), 0, fields)
	return q
}

// TestPlanMetadataOnly_HitsQueries walks /select/logsql/hits. Without `field=`
// it is `stats by (_time:step) count() hits | sort by (_time)` — eligible, one
// bucket. With `field=service.name` it groups by that column too, and a
// metadata-only block has no such column, so it is refused at EVERY step —
// including a step wider than any file, where every file sits inside one bucket
// of _time but its per-service split is still unknown to the manifest.
// (The HTTP layer refuses it earlier as well: requestNeedsFieldData withholds the
// timestamp-only hint whenever `field=` is present; see
// TestWrapVLTimestampOnly_FieldParamSkipsHint.)
func TestPlanMetadataOnly_HitsQueries(t *testing.T) {
	for _, step := range []time.Duration{5 * time.Second, time.Minute, 5 * time.Minute, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		t.Run("no_fields/"+step.String(), func(t *testing.T) {
			q := hitsQuery(t, step, nil)
			plan := planMetadataOnly(q)
			if !plan.eligible || len(plan.buckets) != 1 {
				t.Fatalf("plan = %+v, want eligible with one bucket for %s", plan, q)
			}
			if plan.buckets[0].Size != int64(step) {
				t.Errorf("bucket size = %d, want %d", plan.buckets[0].Size, int64(step))
			}
		})
		for _, fields := range [][]string{{"service.name"}, {"level"}, {"service.name", "level"}} {
			t.Run(fmt.Sprintf("fields=%v/%s", fields, step), func(t *testing.T) {
				q := hitsQuery(t, step, fields)
				if plan := planMetadataOnly(q); plan.eligible {
					t.Errorf("hits grouped by %v classified as metadata-answerable: %s", fields, q)
				}
			})
		}
	}
}

// TestManifestFastPath_ExactnessHitsWithFields runs the hits shapes through the
// oracle: with fields the fast path must answer NO file at any step (and the
// answer must still equal the scan); without fields it answers exactly the files
// the containment rule allows.
func TestManifestFastPath_ExactnessHitsWithFields(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	files := []fakeFile{
		newFakeFile("short.parquet", 600, testBase+2*hour+int64(5*time.Minute), testBase+2*hour+int64(9*time.Minute)),
		newFakeFile("one-hour.parquet", 900, testBase+4*hour+int64(time.Minute), testBase+4*hour+int64(58*time.Minute)),
		newFakeFile("three-hours.parquet", 1200, testBase+6*hour+int64(40*time.Minute), testBase+9*hour+int64(20*time.Minute)),
	}
	startNs, endNs := testBase, testBase+24*hour

	for _, step := range []time.Duration{5 * time.Minute, time.Hour, 24 * time.Hour, 7 * 24 * time.Hour} {
		for _, fields := range [][]string{nil, {fakeServiceField}} {
			qs := hitsQuery(t, step, fields).String()
			t.Run(fmt.Sprintf("step=%s/fields=%v", step, fields), func(t *testing.T) {
				want := answerViaScan(t, s, qs, files, startNs, endNs)
				got, served := answerViaFastPath(t, s, qs, files, startNs, endNs)
				if strings.Join(got, ";") != strings.Join(want, ";") {
					t.Fatalf("fast path != scan for %s\n got: %v\nwant: %v", qs, got, want)
				}
				wantServed := 0
				if fields == nil {
					for _, f := range files {
						// _time:1w is VL's calendar week (Monday-aligned), which the
						// plain epoch truncation cannot restate; every file here
						// sits inside the same week anyway.
						bucket := int64(step)
						if step == 7*24*time.Hour {
							bucket = 0
						}
						if wantServedFromMetadata(f.fi, startNs, endNs, bucket) {
							wantServed++
						}
					}
				}
				if served != wantServed {
					t.Errorf("served %d files from metadata, want %d (%s)", served, wantServed, qs)
				}
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Row filters: _stream selectors and friends never reach the fast path
// ---------------------------------------------------------------------------

// TestManifestFastPath_RowFiltersRefused covers every filter form that is
// folded into the query's top-level filter rather than a pipe — `_stream`
// selectors in both spellings, `_stream_id`, a word, a field — so the pipe
// classifier alone would call them eligible. RunQuery's `filter == nil` gate is
// what refuses them, because metadata cannot evaluate a row filter: serving the
// file would count rows the selector excludes.
func TestManifestFastPath_RowFiltersRefused(t *testing.T) {
	for _, qs := range []string{
		`{service.name="api"} | stats count()`,
		`_stream:{service.name="api"} | stats count()`,
		`{service.name="api"} | stats by (_time:1h) count()`,
		`_stream_id:0000007b000001c850d9950ea6196b1a4812081265faa1c7 | stats count()`,
		`error | stats count()`,
		`service.name:="api" | stats count()`,
		`* | filter service.name:="api" | stats count()`,
	} {
		t.Run(qs, func(t *testing.T) {
			q := mustParseQueryWithTime(t, qs, testBase, testBase+int64(time.Hour))
			if parseFilterFromQuery(q) == nil {
				t.Fatalf("no row filter reported for %s — RunQuery would serve it from metadata", qs)
			}
		})
	}
	// Control: the time filter VL always prepends is NOT a row filter, or the
	// fast path could never fire at all.
	if f := parseFilterFromQuery(mustParseQueryWithTime(t, `* | stats count()`, testBase, testBase+int64(time.Hour))); f != nil {
		t.Errorf("a bare time window was reported as a row filter: %v", f)
	}
}

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

// TestManifestFastPath_ServesOnlyTheFilesItIsGiven locks the tenant contract at
// the fast path's own boundary. manifestFastPath is a pure function of the file
// set passed in: it never consults the manifest, so it cannot count another
// tenant's files unless the caller hands them over. Choosing the right set is
// the caller's job — manifest.GetFilesForRangeTenant is what a tenant-scoped
// RunQuery passes — and this test proves that given tenant A's set, tenant B's
// rows are never counted, and that an empty set counts nothing even while the
// manifest holds files for the window.
func TestManifestFastPath_ServesOnlyTheFilesItIsGiven(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	start := time.Unix(0, testBase+10*hour).UTC()
	partition := "dt=" + start.Format("2006-01-02") + "/hour=" + start.Format("15")

	tenantA := manifest.FileInfo{Key: "1/0/logs/" + partition + "/a.parquet", Size: 1024, RowCount: 1_234, MinTimeNs: testBase + 10*hour + 1, MaxTimeNs: testBase + 10*hour + int64(20*time.Minute)}
	tenantB := manifest.FileInfo{Key: "2/0/logs/" + partition + "/b.parquet", Size: 1024, RowCount: 98_765, MinTimeNs: testBase + 10*hour + 1, MaxTimeNs: testBase + 10*hour + int64(20*time.Minute)}
	s.manifest.AddFile(partition, tenantA)
	s.manifest.AddFile(partition, tenantB)

	windowStart, windowEnd := testBase+10*hour, testBase+11*hour

	scoped := s.manifest.GetFilesForRangeTenant(windowStart, windowEnd, "1", "0")
	if len(scoped) != 1 || scoped[0].Key != tenantA.Key {
		t.Fatalf("fixture: tenant-scoped lookup returned %+v, want only tenant A's file", scoped)
	}

	var rows int64
	remaining := s.manifestFastPath(context.Background(), scoped, windowStart, windowEnd, countOnlyPlan,
		func(_ uint, db *logstorage.DataBlock) { rows += int64(db.RowsCount()) })
	if len(remaining) != 0 {
		t.Errorf("remaining = %+v, want tenant A's file served", remaining)
	}
	if rows != tenantA.RowCount {
		t.Errorf("counted %d rows, want exactly tenant A's %d (tenant B holds %d in the same window)", rows, tenantA.RowCount, tenantB.RowCount)
	}

	rows = 0
	s.manifestFastPath(context.Background(), nil, windowStart, windowEnd, countOnlyPlan,
		func(_ uint, db *logstorage.DataBlock) { rows += int64(db.RowsCount()) })
	if rows != 0 {
		t.Errorf("an empty file set counted %d rows; the fast path must not look anything up on its own", rows)
	}
}

// TestRunQuery_FastPathStaysInsideTheQueriedTenant goes one step further than
// the unit test above, because this module's RunQuery already scopes its file
// set (manifest.GetFilesForRangeTenant for a single tenant): with two tenants'
// files in the same window, a count for tenant 1/0 answered entirely from
// metadata must count exactly tenant 1/0's rows.
func TestRunQuery_FastPathStaysInsideTheQueriedTenant(t *testing.T) {
	s := testStorage()
	hour := int64(time.Hour)
	start := time.Unix(0, testBase+10*hour).UTC()
	partition := "dt=" + start.Format("2006-01-02") + "/hour=" + start.Format("15")

	tenantA := manifest.FileInfo{Key: "1/0/traces/" + partition + "/a.parquet", Size: 1024, RowCount: 1_234, MinTimeNs: testBase + 10*hour + 1, MaxTimeNs: testBase + 10*hour + int64(20*time.Minute)}
	tenantB := manifest.FileInfo{Key: "2/0/traces/" + partition + "/b.parquet", Size: 1024, RowCount: 98_765, MinTimeNs: testBase + 10*hour + 1, MaxTimeNs: testBase + 10*hour + int64(20*time.Minute)}
	s.manifest.AddFile(partition, tenantA)
	s.manifest.AddFile(partition, tenantB)

	q := mustParseQueryWithTime(t, "* | stats count()", testBase+10*hour, testBase+11*hour)
	ctx := storage.WithTimestampOnlyHint(context.Background())

	var rows int64
	tenant := []logstorage.TenantID{{AccountID: 1, ProjectID: 0}}
	if err := s.RunQuery(ctx, tenant, q, func(_ uint, db *logstorage.DataBlock) { rows += int64(db.RowsCount()) }); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if rows != tenantA.RowCount {
		t.Errorf("tenant 1/0 counted %d rows, want exactly %d (tenant 2/0 holds %d in the same window)", rows, tenantA.RowCount, tenantB.RowCount)
	}
}
