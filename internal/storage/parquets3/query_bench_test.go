package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// legacySyntheticBlocks reproduces the manifest fast path as it was BEFORE
// this change: one formatted timestamp per row, evenly spaced across the
// file's time span, chunked at syntheticChunkSize and capped at 1M rows per
// file (the cap that under-counted large files). It exists only so the
// benchmarks below can report a before/after on the same machine.
func legacySyntheticBlocks(s *Storage, fi manifest.FileInfo, emit func(*logstorage.DataBlock)) {
	const legacyMaxSyntheticRows = 1_000_000
	total := int(fi.RowCount)
	if total <= 0 {
		return
	}
	if total > legacyMaxSyntheticRows {
		total = legacyMaxSyntheticRows
	}
	name := s.timestampFieldName()
	for offset := 0; offset < total; offset += syntheticChunkSize {
		chunk := syntheticChunkSize
		if offset+chunk > total {
			chunk = total - offset
		}
		values := make([]string, chunk)
		if total == 1 {
			values[0] = s.registry.FormatField(name, fi.MinTimeNs)
		} else {
			step := (fi.MaxTimeNs - fi.MinTimeNs) / int64(total-1)
			if step == 0 {
				step = 1
			}
			for i := range values {
				ts := fi.MinTimeNs + int64(offset+i)*step
				if ts > fi.MaxTimeNs {
					ts = fi.MaxTimeNs
				}
				values[i] = s.registry.FormatField(name, ts)
			}
		}
		db := &logstorage.DataBlock{}
		db.SetColumns([]logstorage.BlockColumn{{Name: name, Values: values}})
		emit(db)
	}
}

// benchFiles builds a manifest of n files of rowCount rows each, every file
// contained in its own hour so a `stats by (_time:1h)` query can serve all of
// them from metadata.
func benchFiles(n int, rowCount int64) []manifest.FileInfo {
	const base = int64(1767225600_000000000) // 2026-01-01T00:00:00Z
	files := make([]manifest.FileInfo, n)
	for i := range files {
		start := base + int64(i)*int64(time.Hour)
		files[i] = manifest.FileInfo{
			Key:       fmt.Sprintf("dt=2026-01-01/part-%04d.parquet", i),
			Size:      rowCount * 64,
			RowCount:  rowCount,
			MinTimeNs: start + int64(time.Minute),
			MaxTimeNs: start + int64(59*time.Minute),
		}
	}
	return files
}

// BenchmarkManifestFastPath_Emit measures block production for a whole query's
// worth of files, before (one formatted string per row) and after (one
// constant column per file). The two shapes are the ones the S3 layout
// actually produces: many small files right after flush, and a few
// target-sized files after compaction.
func BenchmarkManifestFastPath_Emit(b *testing.B) {
	s := &Storage{registry: schema.NewRegistry(schema.LogsProfile)}
	ctx := context.Background()
	drop := func(_ uint, _ *logstorage.DataBlock) {}
	dropDB := func(_ *logstorage.DataBlock) {}

	shapes := []struct {
		name     string
		files    int
		rowCount int64
	}{
		{"124files_x_2k_rows", 124, 2_000},
		{"4files_x_2M_rows", 4, 2_000_000},
	}
	for _, sh := range shapes {
		files := benchFiles(sh.files, sh.rowCount)
		b.Run(sh.name+"/before", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, fi := range files {
					legacySyntheticBlocks(s, fi, dropDB)
				}
			}
		})
		b.Run(sh.name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, fi := range files {
					s.streamConstTimeBlocks(ctx, fi, drop)
				}
			}
		})
	}
}

// BenchmarkManifestFastPath_CountQuery measures the whole answer: blocks are
// produced AND pushed through VL's pipe machinery for `* | stats count()`,
// which is what a `/select/logsql/stats_query` costs once every file is
// covered by the manifest (zero S3 requests).
func BenchmarkManifestFastPath_CountQuery(b *testing.B) {
	s := &Storage{registry: schema.NewRegistry(schema.LogsProfile)}
	ctx := context.Background()

	run := func(b *testing.B, files []manifest.FileInfo, legacy bool) {
		q, err := logstorage.ParseQuery(`* | stats count()`)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{{}}, q, false, nil)
			err := logstorage.RunQueryExternal(qctx,
				func(wb logstorage.WriteDataBlockFunc) error {
					for _, fi := range files {
						if legacy {
							legacySyntheticBlocks(s, fi, func(db *logstorage.DataBlock) { wb(0, db) })
						} else {
							s.streamConstTimeBlocks(ctx, fi, wb)
						}
					}
					return nil
				},
				func(_ uint, _ *logstorage.DataBlock) {})
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	shapes := []struct {
		name     string
		files    int
		rowCount int64
	}{
		{"124files_x_2k_rows", 124, 2_000},
		{"4files_x_2M_rows", 4, 2_000_000},
	}
	for _, sh := range shapes {
		files := benchFiles(sh.files, sh.rowCount)
		b.Run(sh.name+"/before", func(b *testing.B) { run(b, files, true) })
		b.Run(sh.name+"/after", func(b *testing.B) { run(b, files, false) })
	}
}

// BenchmarkTokenBloomExtraction measures token extraction performance from
// query strings, exercised for every query in the hot path.
func BenchmarkTokenBloomExtraction(b *testing.B) {
	queries := map[string]string{
		"simple":    `error timeout`,
		"field":     `service.name:="api-gateway" AND trace_id:="abc123"`,
		"body":      `_msg:"connection timeout to database host"`,
		"mixed":     `service.name:="api-gw" error connection refused`,
		"complex":   `_msg:"kubernetes pod crashloop" AND severity_text:="error" | stats count() by service.name`,
		"free_text": `nginx 502 bad gateway upstream`,
		"empty":     `*`,
		"negated":   `NOT service.name:="internal" AND trace_id:="xyz789"`,
	}

	for name, q := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				extractSearchTokens(q)
			}
		})
	}
}

// BenchmarkTokenBloomCheck measures the bloom filter check performance
// for token bloom skip decisions on row groups.
func BenchmarkTokenBloomCheck(b *testing.B) {
	// Build a token bloom from realistic body content
	bodies := make([]string, 1000)
	for i := range bodies {
		bodies[i] = "kubernetes pod api-gateway-7b8c9d-xkq2v in namespace production reported connection timeout to database host db-primary.internal after 5000ms"
	}
	key, value := buildTokenBloomMetadata(bodies, 0)
	metadata := map[string]string{key: string(value)}

	searchTokens := []string{"connection", "timeout", "database"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenBloomSkip(metadata, 0, searchTokens)
	}
}

// BenchmarkTokenBloomCheck_Miss measures bloom filter check when the token
// is definitely absent (the skip path).
func BenchmarkTokenBloomCheck_Miss(b *testing.B) {
	bodies := make([]string, 1000)
	for i := range bodies {
		bodies[i] = "kubernetes pod api-gateway reported healthy status"
	}
	key, value := buildTokenBloomMetadata(bodies, 0)
	metadata := map[string]string{key: string(value)}

	// These tokens are not in the bodies
	searchTokens := []string{"segfault", "coredump", "oomkill"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenBloomSkip(metadata, 0, searchTokens)
	}
}

// BenchmarkProjectionColumns measures column projection performance,
// exercising queryColumns which determines which Parquet columns to read.
func BenchmarkProjectionColumns(b *testing.B) {
	reg := schema.NewRegistry(schema.LogsProfile)

	queries := map[string]struct {
		query      string
		pipeFields []string
	}{
		"wildcard": {
			query:      "*",
			pipeFields: nil,
		},
		"exact_match": {
			query:      `service.name:="api-gateway"`,
			pipeFields: nil,
		},
		"with_pipe_fields": {
			query:      `service.name:="api-gateway" | stats count() by service.name`,
			pipeFields: []string{"service.name"},
		},
		"multi_field": {
			query:      `service.name:="api-gw" AND trace_id:="abc123" AND severity_text:="error"`,
			pipeFields: nil,
		},
		"free_text": {
			query:      `"connection timeout"`,
			pipeFields: nil,
		},
		"complex_pipe": {
			query:      `* | stats count() by service.name, severity_text | sort by count desc | limit 10`,
			pipeFields: []string{"service.name", "severity_text"},
		},
	}

	for name, tc := range queries {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryColumns(tc.query, reg, tc.pipeFields)
			}
		})
	}
}

// BenchmarkReadRowGroupColumnar_Projected measures columnar row group reading
// with column projection (the fast path for narrow queries).
func BenchmarkReadRowGroupColumnar_Projected(b *testing.B) {
	f, _ := writeTestParquetFile(b, 10000)
	reg := schema.NewRegistry(schema.LogsProfile)
	rg := f.RowGroups()[0]
	startNs := int64(0)
	endNs := int64(1 << 62)

	// Narrow projection: only 2 columns
	narrowCols := map[string]bool{
		"timestamp_unix_nano": true,
		"service.name":        true,
	}

	b.Run("narrow_2cols", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			readRowGroupColumnar(f, rg, narrowCols, reg, startNs, endNs, nil, nil)
		}
	})

	// Medium projection: 5 columns
	mediumCols := map[string]bool{
		"timestamp_unix_nano": true,
		"body":                true,
		"severity_text":       true,
		"service.name":        true,
		"trace_id":            true,
	}

	b.Run("medium_5cols", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			readRowGroupColumnar(f, rg, mediumCols, reg, startNs, endNs, nil, nil)
		}
	})

	// All columns
	allCols := allLeafColumns(f)
	b.Run("all_cols", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			readRowGroupColumnar(f, rg, allCols, reg, startNs, endNs, nil, nil)
		}
	})
}

// BenchmarkSortFilesByCacheAffinity measures the cache-aware sorting used
// to prioritize files with cached footers.
func BenchmarkSortFilesByCacheAffinity(b *testing.B) {
	files := make([]manifest.FileInfo, 500)
	cachedKeys := make(map[string]bool)
	for i := range files {
		key := "dt=2026-01-01/hour=00/file_" + string(rune('A'+i%26)) + ".parquet"
		files[i] = manifest.FileInfo{Key: key, Size: int64(i * 1024)}
		if i%3 == 0 {
			cachedKeys[key] = true
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Make a copy since sort is in-place
		cp := make([]manifest.FileInfo, len(files))
		copy(cp, files)
		sortFilesByCacheAffinity(cp, cachedKeys)
	}
}

// BenchmarkExtractExactMatch_MultiField measures extractExactMatch across
// different query patterns used in the hot query path.
func BenchmarkExtractExactMatch_MultiField(b *testing.B) {
	query := `service.name:="api-gateway" AND trace_id:="abc123def456" AND severity_text:="error"`

	fields := []string{"service.name", "trace_id", "severity_text", "k8s.namespace.name"}
	for _, f := range fields {
		b.Run(f, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				extractExactMatch(query, f)
			}
		})
	}
}

// BenchmarkWriteAndReadParquet_RoundTrip measures the full write-then-read
// cycle that the query path exercises when opening files.
func BenchmarkWriteAndReadParquet_RoundTrip(b *testing.B) {
	rows := make([]schema.LogRow, 5000)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: int64(1716393600000000000 + i*1000000),
			Body:              "test log message body content here for round trip benchmark",
			SeverityText:      "INFO",
			SeverityNumber:    int32(9),
			ServiceName:       "api-gateway",
			K8sNamespaceName:  "production",
			K8sPodName:        "api-gateway-7b8c9d-xkq2v",
			TraceID:           "abc123def456",
			SpanID:            "span789",
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[schema.LogRow](&buf)
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		data := buf.Bytes()
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		_ = f.RowGroups()
	}
}

// straddlingFixture is a set of Parquet objects spanning spanHours hours each,
// starting at an UNALIGNED offset, so every file straddles a bucket boundary at
// any step finer than its span and none of them can be answered from metadata.
type straddlingFixture struct {
	data    map[string][]byte
	infos   []manifest.FileInfo
	parts   []string
	rows    int64
	startNs int64
	endNs   int64
}

var (
	straddlingFixturesMu sync.Mutex
	straddlingFixtures   = map[string]*straddlingFixture{}
)

// newStraddlingFixture builds (once per process — CI runs benchmarks with
// -count=2, and generating the Parquet bodies is not what is being measured)
// the fixture for a shape.
func newStraddlingFixture(b *testing.B, n, rowCount, spanHours int) *straddlingFixture {
	b.Helper()
	id := fmt.Sprintf("%d/%d/%d", n, rowCount, spanHours)
	straddlingFixturesMu.Lock()
	defer straddlingFixturesMu.Unlock()
	if fx, ok := straddlingFixtures[id]; ok {
		return fx
	}

	// 07:23:17 — deliberately not on an hour, 5-minute or 1-minute boundary.
	base := time.Date(2026, 1, 1, 7, 23, 17, 0, time.UTC)
	span := time.Duration(spanHours) * time.Hour
	step := span / time.Duration(rowCount)

	fx := &straddlingFixture{data: map[string][]byte{}}
	for i := 0; i < n; i++ {
		fileStart := base.Add(time.Duration(i) * span)
		rows := make([]logRow, rowCount)
		for j := range rows {
			rows[j] = logRow{
				TimestampUnixNano: fileStart.Add(time.Duration(j) * step).UnixNano(),
				Body:              "benchmark log line for the straddling-file fallback measurement",
				SeverityText:      "INFO",
				ServiceName:       "api-gateway",
			}
		}
		key := fmt.Sprintf("logs/dt=%s/hour=%02d/straddle-%03d.parquet", fileStart.Format("2006-01-02"), fileStart.Hour(), i)
		fx.data[key] = writeParquetToBytes(b, rows)
		fx.infos = append(fx.infos, manifest.FileInfo{
			Key:       key,
			Size:      int64(len(fx.data[key])),
			RowCount:  int64(rowCount),
			MinTimeNs: rows[0].TimestampUnixNano,
			MaxTimeNs: rows[rowCount-1].TimestampUnixNano,
		})
		fx.parts = append(fx.parts, fmt.Sprintf("dt=%s/hour=%02d", fileStart.Format("2006-01-02"), fileStart.Hour()))
		fx.rows += int64(rowCount)
	}
	fx.startNs = base.Add(-time.Hour).UnixNano()
	fx.endNs = base.Add(time.Duration(n)*span + time.Hour).UnixNano()
	straddlingFixtures[id] = fx
	return fx
}

// storage returns a Storage with EMPTY caches over the fixture's objects. The
// S3 read-ahead and coalescing settings are put back to the production
// defaults: testStorageWithS3 shrinks them to exercise the ranged-read code
// paths, which would misprice the GETs and bytes this benchmark reports.
func (fx *straddlingFixture) storage(b *testing.B, mock *mockS3Server) *Storage {
	s := testStorageWithS3(b, mock.url())
	defaults := config.Default()
	s.cfg.S3.ReadAheadBytes = defaults.S3.ReadAheadBytes
	s.cfg.S3.CoalesceGapBytes = defaults.S3.CoalesceGapBytes
	for i, fi := range fx.infos {
		s.manifest.AddFile(fx.parts[i], fi)
	}
	return s
}

// BenchmarkManifestFastPath_StraddlingFallback prices the correctness trade this
// change makes. A histogram step FINER than the files' time spans means no file
// can be answered from metadata — every one straddles a bucket boundary — so
// each is read for real. The comparison is the same files under a step COARSER
// than the whole window, where the metadata path applies.
//
// "cold" builds a Storage with empty caches for every iteration (first touch:
// footer + column fetch); "warm" reuses one Storage after a warm-up query. Every
// timed response is validated — the rows handed to the pipes must add up to the
// fixture's exact row count, otherwise the iteration fails instead of counting.
//
// This is the bound to quote when lakehouse_metadata_only_fallback_files_total
// rises: the cost of reading the timestamp column of every file in the window.
func BenchmarkManifestFastPath_StraddlingFallback(b *testing.B) {
	shapes := []struct {
		name      string
		files     int
		rowCount  int
		spanHours int
	}{
		{"8files_x_50k_rows_2h_span", 8, 50_000, 2},
		{"4files_x_200k_rows_3h_span", 4, 200_000, 3},
	}
	for _, sh := range shapes {
		fx := newStraddlingFixture(b, sh.files, sh.rowCount, sh.spanHours)
		mock := newMockS3Server()
		for k, v := range fx.data {
			mock.putFile(k, v)
		}

		hinted := storage.WithTimestampOnlyHint(context.Background())
		runValidatedCtx := func(b *testing.B, ctx context.Context, s *Storage, q *logstorage.Query) {
			var rows atomic.Int64
			if err := s.RunQuery(ctx, nil, q, func(_ uint, db *logstorage.DataBlock) {
				rows.Add(int64(db.RowsCount()))
			}); err != nil {
				b.Fatal(err)
			}
			if got := rows.Load(); got != fx.rows {
				b.Fatalf("invalid response: %d rows reached the pipes, want exactly %d", got, fx.rows)
			}
		}
		runValidated := func(b *testing.B, s *Storage, q *logstorage.Query) { runValidatedCtx(b, hinted, s, q) }
		query := func(b *testing.B, qs string) *logstorage.Query {
			q, err := logstorage.ParseQuery(qs)
			if err != nil {
				b.Fatal(err)
			}
			q.AddTimeFilter(fx.startNs, fx.endNs)
			return q
		}
		report := func(b *testing.B, iters int, gets0, bytes0 int64, fallback0, served0 uint64) {
			if iters == 0 {
				return
			}
			n := float64(iters)
			b.ReportMetric(float64(mock.gets.Load()-gets0)/n, "s3_gets/op")
			b.ReportMetric(float64(mock.bytesServed.Load()-bytes0)/n, "s3_bytes/op")
			b.ReportMetric(float64(metrics.MetadataOnlyFallbackFiles.Get()-fallback0)/n, "files_read/op")
			b.ReportMetric(float64(metrics.MetadataOnlyFiles.Get()-served0)/n, "files_from_metadata/op")
			var objectBytes int64
			for _, fi := range fx.infos {
				objectBytes += fi.Size
			}
			b.ReportMetric(float64(objectBytes), "window_object_bytes")
		}

		// _time:5m is finer than every file's span: every file straddles, every
		// file is read. Cold = empty caches per query.
		b.Run(sh.name+"/step_5m_every_file_read/cold", func(b *testing.B) {
			q := query(b, `* | stats by (_time:5m) count()`)
			gets0, bytes0 := mock.gets.Load(), mock.bytesServed.Load()
			fallback0, served0 := metrics.MetadataOnlyFallbackFiles.Get(), metrics.MetadataOnlyFiles.Get()
			b.ReportAllocs()
			iters := 0
			for b.Loop() {
				b.StopTimer()
				s := fx.storage(b, mock)
				b.StartTimer()
				runValidated(b, s, q)
				iters++
			}
			report(b, iters, gets0, bytes0, fallback0, served0)
		})

		// Same, warm: footers and column chunks already fetched once.
		b.Run(sh.name+"/step_5m_every_file_read/warm", func(b *testing.B) {
			q := query(b, `* | stats by (_time:5m) count()`)
			s := fx.storage(b, mock)
			runValidated(b, s, q)
			gets0, bytes0 := mock.gets.Load(), mock.bytesServed.Load()
			fallback0, served0 := metrics.MetadataOnlyFallbackFiles.Get(), metrics.MetadataOnlyFiles.Get()
			b.ReportAllocs()
			b.ResetTimer()
			iters := 0
			for b.Loop() {
				runValidated(b, s, q)
				iters++
			}
			report(b, iters, gets0, bytes0, fallback0, served0)
		})

		// The same query WITHOUT the timestamp-only hint is an ordinary scan of
		// the window. Its cost is the yardstick for the fallback above: if the
		// two match, the correctness gate adds nothing beyond the read itself.
		// Measured on the first shape only, to keep CI's benchmark job short.
		if sh.name == shapes[0].name {
			b.Run(sh.name+"/step_5m_plain_scan_no_hint/warm", func(b *testing.B) {
				q := query(b, `* | stats by (_time:5m) count()`)
				s := fx.storage(b, mock)
				runValidatedCtx(b, context.Background(), s, q)
				gets0, bytes0 := mock.gets.Load(), mock.bytesServed.Load()
				fallback0, served0 := metrics.MetadataOnlyFallbackFiles.Get(), metrics.MetadataOnlyFiles.Get()
				b.ReportAllocs()
				b.ResetTimer()
				iters := 0
				for b.Loop() {
					runValidatedCtx(b, context.Background(), s, q)
					iters++
				}
				report(b, iters, gets0, bytes0, fallback0, served0)
			})
		}

		// _time:1d is coarser than the whole window: metadata answers every
		// file. No cache is involved, so cold and warm are the same number.
		b.Run(sh.name+"/step_1d_metadata_only", func(b *testing.B) {
			q := query(b, `* | stats by (_time:1d) count()`)
			s := fx.storage(b, mock)
			gets0, bytes0 := mock.gets.Load(), mock.bytesServed.Load()
			fallback0, served0 := metrics.MetadataOnlyFallbackFiles.Get(), metrics.MetadataOnlyFiles.Get()
			b.ReportAllocs()
			iters := 0
			for b.Loop() {
				runValidated(b, s, q)
				iters++
			}
			report(b, iters, gets0, bytes0, fallback0, served0)
		})
		mock.close()
	}
}
