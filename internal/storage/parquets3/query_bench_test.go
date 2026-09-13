package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
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
			readRowGroupColumnar(f, rg, narrowCols, reg, startNs, endNs, nil)
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
			readRowGroupColumnar(f, rg, mediumCols, reg, startNs, endNs, nil)
		}
	})

	// All columns
	allCols := allLeafColumns(f)
	b.Run("all_cols", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			readRowGroupColumnar(f, rg, allCols, reg, startNs, endNs, nil)
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
