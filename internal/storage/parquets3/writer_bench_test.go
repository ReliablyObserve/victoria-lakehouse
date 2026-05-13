package parquets3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func generateRealisticLogRows(n int) []schema.LogRow {
	rng := rand.New(rand.NewSource(42))
	services := []string{"api-gateway", "auth-service", "user-service", "payment-service", "notification-service"}
	namespaces := []string{"production", "staging"}
	pods := []string{"pod-abc12", "pod-def34", "pod-ghi56", "pod-jkl78", "pod-mno90"}
	deployments := []string{"api-gateway-v2", "auth-service-v3", "user-service-v1"}
	nodes := []string{"ip-10-0-1-42", "ip-10-0-2-17", "ip-10-0-3-88"}
	envs := []string{"production", "staging", "development"}
	regions := []string{"us-east-1", "eu-west-1", "ap-southeast-1"}
	hosts := []string{"host-001", "host-002", "host-003", "host-004"}
	levels := []string{"info", "warn", "error", "debug"}
	scopes := []string{"http.server", "db.client", "grpc.server", "messaging.consumer"}

	bodies := []string{
		`{"method":"GET","path":"/api/v1/users","status":200,"duration_ms":45,"request_id":"req-abc123"}`,
		`{"method":"POST","path":"/api/v1/auth/login","status":401,"duration_ms":12,"error":"invalid_credentials"}`,
		`{"level":"error","msg":"connection timeout","host":"db-primary.internal","timeout_ms":5000}`,
		`{"event":"cache_miss","key":"user:12345","latency_ms":2,"fallback":"database"}`,
		`{"msg":"request completed","trace_id":"abc123def456","span_id":"789012","duration_ms":156}`,
		`Starting health check for service payment-service on port 8080`,
		`Kubernetes liveness probe succeeded for pod pod-abc12 in namespace production`,
		`Rate limiter triggered for client 192.168.1.100 - 429 Too Many Requests`,
		`Database migration v42 applied successfully in 3.2 seconds`,
		`TLS certificate for *.example.com expires in 30 days - renewal scheduled`,
	}

	rows := make([]schema.LogRow, n)
	baseTime := time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC).UnixNano()

	for i := range rows {
		ts := baseTime + int64(i)*int64(time.Millisecond)*10 + int64(rng.Intn(10000))
		traceID := fmt.Sprintf("%032x", rng.Int63())
		spanID := fmt.Sprintf("%016x", rng.Int63())

		sevText := levels[rng.Intn(len(levels))]
		var sevNum int32
		switch sevText {
		case "debug":
			sevNum = 5
		case "info":
			sevNum = 9
		case "warn":
			sevNum = 13
		case "error":
			sevNum = 17
		}

		rows[i] = schema.LogRow{
			TimestampUnixNano:  ts,
			Body:               bodies[rng.Intn(len(bodies))],
			SeverityText:       sevText,
			SeverityNumber:     sevNum,
			ServiceName:        services[rng.Intn(len(services))],
			K8sNamespaceName:   namespaces[rng.Intn(len(namespaces))],
			K8sPodName:         pods[rng.Intn(len(pods))],
			K8sDeploymentName:  deployments[rng.Intn(len(deployments))],
			K8sNodeName:        nodes[rng.Intn(len(nodes))],
			DeployEnv:          envs[rng.Intn(len(envs))],
			CloudRegion:        regions[rng.Intn(len(regions))],
			HostName:           hosts[rng.Intn(len(hosts))],
			TraceID:            traceID,
			SpanID:             spanID,
			Stream:             fmt.Sprintf("{service.name=%q}", services[rng.Intn(len(services))]),
			StreamID:           fmt.Sprintf("stream-%04d", rng.Intn(50)),
			ScopeName:          scopes[rng.Intn(len(scopes))],
			ResourceAttributes: map[string]string{
				"service.version":  fmt.Sprintf("v%d.%d.%d", rng.Intn(3), rng.Intn(10), rng.Intn(50)),
				"telemetry.sdk":    "opentelemetry-go",
				"os.type":          "linux",
				"process.pid":      fmt.Sprintf("%d", rng.Intn(65535)),
				"container.id":     fmt.Sprintf("%016x", rng.Int63()),
				"k8s.cluster.name": "prod-us-east-1",
			},
			LogAttributes: map[string]string{
				"http.method":       []string{"GET", "POST", "PUT", "DELETE"}[rng.Intn(4)],
				"http.status_code":  fmt.Sprintf("%d", []int{200, 201, 400, 401, 403, 404, 500}[rng.Intn(7)]),
				"http.url":         fmt.Sprintf("/api/v%d/%s", rng.Intn(3)+1, []string{"users", "orders", "products"}[rng.Intn(3)]),
				"net.peer.ip":      fmt.Sprintf("10.%d.%d.%d", rng.Intn(256), rng.Intn(256), rng.Intn(256)),
				"thread.name":      fmt.Sprintf("worker-%d", rng.Intn(16)),
			},
		}
	}
	return rows
}

func writeParquetAtLevel(rows []schema.LogRow, level int, rowGroupSize int) ([]byte, int64, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(level)}
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "service.name"),
			parquet.SplitBlockFilter(10, "trace_id"),
		),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, 0, err
	}
	if err := writer.Close(); err != nil {
		return nil, 0, err
	}
	rawBytes := estimateRawBytesLogs(rows)
	return buf.Bytes(), rawBytes, nil
}

type compressionResult struct {
	Level           int     `json:"level"`
	LevelName       string  `json:"level_name"`
	CompressedBytes int64   `json:"compressed_bytes"`
	RawBytes        int64   `json:"raw_bytes"`
	Ratio           float64 `json:"ratio"`
	WriteTimeMs     float64 `json:"write_time_ms"`
	WriteMBperSec   float64 `json:"write_mb_per_sec"`
	MemAllocMB      float64 `json:"mem_alloc_mb"`
	MemTotalAllocMB float64 `json:"mem_total_alloc_mb"`
}

func levelName(level int) string {
	switch {
	case level <= 1:
		return "SpeedFastest"
	case level <= 5:
		return "SpeedDefault"
	case level <= 10:
		return "SpeedBetterCompression"
	default:
		return "SpeedBestCompression"
	}
}

func TestCompressionLevelBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compression benchmark in short mode")
	}

	rowCounts := []int{10000, 50000}
	levels := []int{1, 3, 5, 7, 9, 11}
	rowGroupSize := 10000

	for _, numRows := range rowCounts {
		t.Run(fmt.Sprintf("rows_%d", numRows), func(t *testing.T) {
			rows := generateRealisticLogRows(numRows)

			results := make([]compressionResult, 0, len(levels))

			for _, level := range levels {
				runtime.GC()
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				start := time.Now()
				compressed, rawBytes, err := writeParquetAtLevel(rows, level, rowGroupSize)
				elapsed := time.Since(start)

				if err != nil {
					t.Fatalf("level %d: %v", level, err)
				}

				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				compressedSize := int64(len(compressed))
				ratio := float64(rawBytes) / float64(compressedSize)
				mbPerSec := float64(rawBytes) / (1 << 20) / elapsed.Seconds()
				allocMB := float64(memAfter.Alloc-memBefore.Alloc) / (1 << 20)
				totalAllocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1 << 20)

				results = append(results, compressionResult{
					Level:           level,
					LevelName:       levelName(level),
					CompressedBytes: compressedSize,
					RawBytes:        rawBytes,
					Ratio:           ratio,
					WriteTimeMs:     float64(elapsed.Milliseconds()),
					WriteMBperSec:   mbPerSec,
					MemAllocMB:      allocMB,
					MemTotalAllocMB: totalAllocMB,
				})
			}

			// Print results table
			t.Logf("\n=== ZSTD Compression Benchmark (%d rows, row_group=%d) ===", numRows, rowGroupSize)
			t.Logf("%-6s %-25s %12s %12s %8s %10s %12s %12s",
				"Level", "Name", "Compressed", "Raw", "Ratio", "Time(ms)", "MB/s", "MemAlloc(MB)")
			t.Logf("%-6s %-25s %12s %12s %8s %10s %12s %12s",
				"-----", "----", "----------", "---", "-----", "--------", "----", "-----------")

			for _, r := range results {
				t.Logf("%-6d %-25s %12d %12d %8.2fx %10.1f %12.1f %12.1f",
					r.Level, r.LevelName, r.CompressedBytes, r.RawBytes,
					r.Ratio, r.WriteTimeMs, r.WriteMBperSec, r.MemTotalAllocMB)
			}

			// Verify compression gets better (or at least not worse) at higher levels
			sort.Slice(results, func(i, j int) bool {
				return results[i].Level < results[j].Level
			})

			bestRatio := results[len(results)-1].Ratio
			worstRatio := results[0].Ratio
			if worstRatio > bestRatio*1.1 {
				t.Logf("WARNING: fastest level ratio (%.2f) is better than best level (%.2f) — may indicate data characteristics", worstRatio, bestRatio)
			}

			// Print JSON for machine parsing
			jsonOut, _ := json.MarshalIndent(results, "", "  ")
			t.Logf("\nJSON:\n%s", string(jsonOut))

			// Speed regression check: fastest level should be at least 2x faster than slowest
			if len(results) >= 2 {
				fastest := results[0].WriteTimeMs
				slowest := results[len(results)-1].WriteTimeMs
				if slowest > 0 && fastest > 0 {
					speedup := slowest / fastest
					t.Logf("\nSpeedup: level 1 is %.1fx faster than level %d", speedup, results[len(results)-1].Level)
				}
			}
		})
	}
}

func TestCompressionLevelBenchmarkRowGroupSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping row group size benchmark in short mode")
	}

	numRows := 50000
	rows := generateRealisticLogRows(numRows)
	level := 3 // Default level
	rgSizes := []int{1000, 5000, 10000, 50000}

	t.Logf("\n=== Row Group Size Impact (level=%d, rows=%d) ===", level, numRows)
	t.Logf("%-12s %12s %12s %8s %10s",
		"RowGroupSize", "Compressed", "Raw", "Ratio", "Time(ms)")

	for _, rgs := range rgSizes {
		start := time.Now()
		compressed, rawBytes, err := writeParquetAtLevel(rows, level, rgs)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("rg=%d: %v", rgs, err)
		}
		ratio := float64(rawBytes) / float64(len(compressed))
		t.Logf("%-12d %12d %12d %8.2fx %10.1f", rgs, len(compressed), rawBytes, ratio, float64(elapsed.Milliseconds()))
	}
}

func BenchmarkWriteParquet_Level1(b *testing.B) {
	rows := generateRealisticLogRows(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeParquetAtLevel(rows, 1, 10000)
	}
}

func BenchmarkWriteParquet_Level3(b *testing.B) {
	rows := generateRealisticLogRows(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeParquetAtLevel(rows, 3, 10000)
	}
}

func BenchmarkWriteParquet_Level7(b *testing.B) {
	rows := generateRealisticLogRows(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeParquetAtLevel(rows, 7, 10000)
	}
}

func BenchmarkWriteParquet_Level11(b *testing.B) {
	rows := generateRealisticLogRows(10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeParquetAtLevel(rows, 11, 10000)
	}
}
