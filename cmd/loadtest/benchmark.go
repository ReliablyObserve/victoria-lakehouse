package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// BenchmarkConfig holds S3 connection settings for optional upload tests.
type BenchmarkConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
}

func runBenchmarks(cfg BenchmarkConfig) []BenchmarkResult {
	var results []BenchmarkResult

	fileSizes := []struct {
		name string
		rows int
	}{
		{"1MB", 500},
		{"5MB", 2500},
		{"10MB", 5000},
		{"50MB", 25000},
		{"100MB", 50000},
	}

	rowGroupSizes := []int{1000, 5000, 10000, 50000}
	compressionLevels := []int{1, 3, 9, 19}

	log.Println("Starting benchmark suite...")
	for _, fs := range fileSizes {
		for _, rgs := range rowGroupSizes {
			if rgs > fs.rows {
				continue
			}
			for _, cl := range compressionLevels {
				result := benchmarkSingle(cfg, fs.name, fs.rows, rgs, cl)
				results = append(results, result)
				log.Printf("  %s rg=%d zstd=%d → %d bytes (ratio %.2f) write=%.0fms read=%.0fms",
					fs.name, rgs, cl, result.FileSizeBytes, result.Ratio, result.WriteTimeMs, result.ReadTimeMs)
			}
		}
	}

	return results
}

func benchmarkSingle(_ BenchmarkConfig, sizeName string, rowCount, rowGroupSize, compressionLevel int) BenchmarkResult {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	rows := benchGenerateLogRows(rng, rowCount)
	rawSize := benchEstimateRawSize(rows)

	writeStart := time.Now()
	data, err := benchWriteParquet(rows, rowGroupSize, compressionLevel)
	writeTime := time.Since(writeStart)
	if err != nil {
		log.Printf("ERROR writing parquet: %v", err)
		return BenchmarkResult{FileSize: sizeName, RowGroupSize: rowGroupSize, CompressionLvl: compressionLevel}
	}

	readStart := time.Now()
	_ = benchReadParquet(data)
	readTime := time.Since(readStart)

	ratio := float64(rawSize) / float64(len(data))
	if len(data) == 0 {
		ratio = 0
	}

	return BenchmarkResult{
		FileSize:       sizeName,
		RowGroupSize:   rowGroupSize,
		CompressionLvl: compressionLevel,
		WriteTimeMs:    float64(writeTime.Microseconds()) / 1000.0,
		ReadTimeMs:     float64(readTime.Microseconds()) / 1000.0,
		FileSizeBytes:  int64(len(data)),
		RawSizeBytes:   rawSize,
		Ratio:          ratio,
		RowCount:       rowCount,
	}
}

func benchGenerateLogRows(rng *rand.Rand, count int) []schema.LogRow {
	now := time.Now()
	rows := make([]schema.LogRow, count)
	services := []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	namespaces := []string{"production", "staging"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	levelNums := map[string]int32{"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}
	messages := []string{
		"request completed successfully",
		"processing incoming request from client",
		"database query executed in 12ms",
		"cache miss for key user:1234",
		"connection established to upstream service",
		"rate limit threshold approaching",
		"failed to parse request body: unexpected EOF",
		"authentication token validated",
		"retry attempt 2/3 for downstream call",
		"graceful shutdown initiated",
	}

	for i := range rows {
		ts := now.Add(-time.Duration(rng.Intn(48*3600)) * time.Second)
		svc := services[rng.Intn(len(services))]
		lvl := levels[rng.Intn(len(levels))]
		msg := messages[rng.Intn(len(messages))]

		rows[i] = schema.LogRow{
			TimestampUnixNano: ts.UnixNano(),
			Body:              fmt.Sprintf("[%s] %s svc=%s req=%x", lvl, msg, svc, rng.Int63()),
			SeverityText:      lvl,
			SeverityNumber:    levelNums[lvl],
			ServiceName:       svc,
			K8sNamespaceName:  namespaces[rng.Intn(len(namespaces))],
			K8sPodName:        fmt.Sprintf("%s-%x", svc, rng.Int31()),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-pool-%c-%d", 'a'+rune(rng.Intn(2)), 1+rng.Intn(4)),
			DeployEnv:         "production",
			CloudRegion:       "us-east-1",
			HostName:          fmt.Sprintf("ip-10-0-%d-%d", rng.Intn(4), rng.Intn(256)),
			TraceID:           fmt.Sprintf("%032x", rng.Int63()),
			SpanID:            fmt.Sprintf("%016x", rng.Int63()),
			Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
			StreamID:          fmt.Sprintf("%016x", rng.Int63()),
			ScopeName:         "benchmark",
		}
	}
	return rows
}

func benchEstimateRawSize(rows []schema.LogRow) int64 {
	var total int64
	for _, r := range rows {
		total += 8 + int64(len(r.Body)) + int64(len(r.SeverityText)) + 4
		total += int64(len(r.ServiceName) + len(r.K8sNamespaceName) + len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName) + len(r.K8sNodeName) + len(r.DeployEnv))
		total += int64(len(r.CloudRegion) + len(r.HostName) + len(r.TraceID) + len(r.SpanID))
		total += int64(len(r.Stream) + len(r.StreamID) + len(r.ScopeName))
	}
	return total
}

func benchWriteParquet(rows []schema.LogRow, rowGroupSize, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: benchZstdLevel(compressionLevel)}
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func benchZstdLevel(level int) zstd.Level {
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

func benchReadParquet(data []byte) []schema.LogRow {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()
	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, _ := reader.Read(rows)
	return rows[:total]
}
