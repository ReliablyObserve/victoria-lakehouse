package parquets3

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BenchmarkManifestFastPath measures synthetic block creation from manifest
// metadata, exercising syntheticManifestBlock which avoids all S3 I/O.
func BenchmarkManifestFastPath(b *testing.B) {
	s := &Storage{
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	fi := manifest.FileInfo{
		Key:       "dt=2026-01-01/hour=00/file.parquet",
		Size:      10 * 1024 * 1024,
		RowCount:  50000,
		MinTimeNs: 1700000000000000000,
		MaxTimeNs: 1700003600000000000,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db := s.syntheticManifestBlock(fi)
		if db == nil {
			b.Fatal("syntheticManifestBlock returned nil")
		}
	}
}

// BenchmarkManifestFastPath_LargeRowCount measures synthetic block creation
// with a large row count to stress the timestamp generation loop.
func BenchmarkManifestFastPath_LargeRowCount(b *testing.B) {
	s := &Storage{
		registry: schema.NewRegistry(schema.LogsProfile),
	}

	fi := manifest.FileInfo{
		Key:       "dt=2026-01-01/hour=00/large.parquet",
		Size:      100 * 1024 * 1024,
		RowCount:  1_000_000,
		MinTimeNs: 1700000000000000000,
		MaxTimeNs: 1700003600000000000,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.syntheticManifestBlock(fi)
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
