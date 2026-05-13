package parquets3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	realLogsDir   = "/tmp/real-bench/logs"
	realTracesDir = "/tmp/real-bench/traces"
)

type realDataBenchResult struct {
	Signal          string  `json:"signal"`
	Level           int     `json:"level"`
	LevelName       string  `json:"level_name"`
	SourceFiles     int     `json:"source_files"`
	TotalRows       int     `json:"total_rows"`
	OrigCompressed  int64   `json:"orig_compressed_bytes"`
	RawBytes        int64   `json:"raw_bytes"`
	NewCompressed   int64   `json:"new_compressed_bytes"`
	Ratio           float64 `json:"ratio"`
	WriteTimeMs     float64 `json:"write_time_ms"`
	WriteMBperSec   float64 `json:"write_mb_per_sec"`
	WriteMemAllocMB float64 `json:"write_mem_alloc_mb"`
	WriteCPUTimeMs  float64 `json:"write_cpu_time_ms"`
	ReadTimeMs      float64 `json:"read_time_ms"`
	ReadMBperSec    float64 `json:"read_mb_per_sec"`
	ReadMemAllocMB  float64 `json:"read_mem_alloc_mb"`
	ReadCPUTimeMs   float64 `json:"read_cpu_time_ms"`
	ReadRows        int     `json:"read_rows"`
}

func loadRealLogRows(t *testing.T, dir string, maxFiles int) ([]schema.LogRow, int64) {
	t.Helper()
	var allRows []schema.LogRow
	var origSize int64

	files := findParquetFiles(t, dir, maxFiles)
	if len(files) == 0 {
		t.Skipf("no parquet files found in %s — run mc cp to download real data first", dir)
	}

	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		origSize += int64(len(data))

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}

		r := parquet.NewGenericReader[schema.LogRow](f)
		buf := make([]schema.LogRow, r.NumRows())
		n, err := r.Read(buf)
		if err != nil && n == 0 {
			t.Fatalf("read rows from %s: %v", path, err)
		}
		allRows = append(allRows, buf[:n]...)
		_ = r.Close()
	}

	return allRows, origSize
}

func loadRealTraceRows(t *testing.T, dir string, maxFiles int) ([]schema.TraceRow, int64) {
	t.Helper()
	var allRows []schema.TraceRow
	var origSize int64

	files := findParquetFiles(t, dir, maxFiles)
	if len(files) == 0 {
		t.Skipf("no parquet files found in %s — run mc cp to download real data first", dir)
	}

	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		origSize += int64(len(data))

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}

		r := parquet.NewGenericReader[schema.TraceRow](f)
		buf := make([]schema.TraceRow, r.NumRows())
		n, err := r.Read(buf)
		if err != nil && n == 0 {
			t.Fatalf("read rows from %s: %v", path, err)
		}
		allRows = append(allRows, buf[:n]...)
		_ = r.Close()
	}

	return allRows, origSize
}

func findParquetFiles(t *testing.T, dir string, maxFiles int) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && filepath.Ext(path) == ".parquet" {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if maxFiles > 0 && len(files) > maxFiles {
		files = files[:maxFiles]
	}
	return files
}

func writeLogRowsAtLevel(rows []schema.LogRow, level int, rowGroupSize int) ([]byte, int64, error) {
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
	return buf.Bytes(), estimateRawBytesLogs(rows), nil
}

func writeTraceRowsAtLevel(rows []schema.TraceRow, level int, rowGroupSize int) ([]byte, int64, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(level)}
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf,
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
	return buf.Bytes(), estimateRawBytesTraces(rows), nil
}

func readLogRows(data []byte) (int, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, err
	}
	r := parquet.NewGenericReader[schema.LogRow](f)
	defer func() { _ = r.Close() }()

	total := 0
	buf := make([]schema.LogRow, 1000)
	for {
		n, err := r.Read(buf)
		total += n
		if n == 0 || err != nil {
			break
		}
	}
	return total, nil
}

func readTraceRows(data []byte) (int, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, err
	}
	r := parquet.NewGenericReader[schema.TraceRow](f)
	defer func() { _ = r.Close() }()

	total := 0
	buf := make([]schema.TraceRow, 1000)
	for {
		n, err := r.Read(buf)
		total += n
		if n == 0 || err != nil {
			break
		}
	}
	return total, nil
}

func TestRealDataCompressionBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real data benchmark in short mode")
	}

	levels := []int{1, 3, 5, 7, 9, 11}
	rowGroupSize := 10000
	maxFiles := 0 // 0 = all files

	t.Run("logs", func(t *testing.T) {
		rows, origSize := loadRealLogRows(t, realLogsDir, maxFiles)
		t.Logf("Loaded %d real log rows from %s (original compressed: %.2f MB)",
			len(rows), realLogsDir, float64(origSize)/(1<<20))

		results := make([]realDataBenchResult, 0, len(levels))

		for _, level := range levels {
			runtime.GC()
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			cpuBefore := time.Now()
			compressed, rawBytes, err := writeLogRowsAtLevel(rows, level, rowGroupSize)
			writeElapsed := time.Since(cpuBefore)
			if err != nil {
				t.Fatalf("write level %d: %v", level, err)
			}

			var memAfterWrite runtime.MemStats
			runtime.ReadMemStats(&memAfterWrite)
			writeAllocMB := float64(memAfterWrite.TotalAlloc-memBefore.TotalAlloc) / (1 << 20)

			runtime.GC()
			runtime.GC()
			var memBeforeRead runtime.MemStats
			runtime.ReadMemStats(&memBeforeRead)

			readStart := time.Now()
			readN, err := readLogRows(compressed)
			readElapsed := time.Since(readStart)
			if err != nil {
				t.Fatalf("read level %d: %v", level, err)
			}

			var memAfterRead runtime.MemStats
			runtime.ReadMemStats(&memAfterRead)
			readAllocMB := float64(memAfterRead.TotalAlloc-memBeforeRead.TotalAlloc) / (1 << 20)

			compSize := int64(len(compressed))
			ratio := float64(rawBytes) / float64(compSize)
			writeMBps := float64(rawBytes) / (1 << 20) / writeElapsed.Seconds()
			readMBps := float64(compSize) / (1 << 20) / readElapsed.Seconds()

			results = append(results, realDataBenchResult{
				Signal:          "logs",
				Level:           level,
				LevelName:       levelName(level),
				SourceFiles:     maxFiles,
				TotalRows:       len(rows),
				OrigCompressed:  origSize,
				RawBytes:        rawBytes,
				NewCompressed:   compSize,
				Ratio:           ratio,
				WriteTimeMs:     float64(writeElapsed.Milliseconds()),
				WriteMBperSec:   writeMBps,
				WriteMemAllocMB: writeAllocMB,
				ReadTimeMs:      float64(readElapsed.Milliseconds()),
				ReadMBperSec:    readMBps,
				ReadMemAllocMB:  readAllocMB,
				ReadRows:        readN,
			})
		}

		printResults(t, "LOGS", results)
	})

	t.Run("traces", func(t *testing.T) {
		rows, origSize := loadRealTraceRows(t, realTracesDir, maxFiles)
		t.Logf("Loaded %d real trace rows from %s (original compressed: %.2f MB)",
			len(rows), realTracesDir, float64(origSize)/(1<<20))

		results := make([]realDataBenchResult, 0, len(levels))

		for _, level := range levels {
			runtime.GC()
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			cpuBefore := time.Now()
			compressed, rawBytes, err := writeTraceRowsAtLevel(rows, level, rowGroupSize)
			writeElapsed := time.Since(cpuBefore)
			if err != nil {
				t.Fatalf("write level %d: %v", level, err)
			}

			var memAfterWrite runtime.MemStats
			runtime.ReadMemStats(&memAfterWrite)
			writeAllocMB := float64(memAfterWrite.TotalAlloc-memBefore.TotalAlloc) / (1 << 20)

			runtime.GC()
			runtime.GC()
			var memBeforeRead runtime.MemStats
			runtime.ReadMemStats(&memBeforeRead)

			readStart := time.Now()
			readN, err := readTraceRows(compressed)
			readElapsed := time.Since(readStart)
			if err != nil {
				t.Fatalf("read level %d: %v", level, err)
			}

			var memAfterRead runtime.MemStats
			runtime.ReadMemStats(&memAfterRead)
			readAllocMB := float64(memAfterRead.TotalAlloc-memBeforeRead.TotalAlloc) / (1 << 20)

			compSize := int64(len(compressed))
			ratio := float64(rawBytes) / float64(compSize)
			writeMBps := float64(rawBytes) / (1 << 20) / writeElapsed.Seconds()
			readMBps := float64(compSize) / (1 << 20) / readElapsed.Seconds()

			results = append(results, realDataBenchResult{
				Signal:          "traces",
				Level:           level,
				LevelName:       levelName(level),
				SourceFiles:     maxFiles,
				TotalRows:       len(rows),
				OrigCompressed:  origSize,
				RawBytes:        rawBytes,
				NewCompressed:   compSize,
				Ratio:           ratio,
				WriteTimeMs:     float64(writeElapsed.Milliseconds()),
				WriteMBperSec:   writeMBps,
				WriteMemAllocMB: writeAllocMB,
				ReadTimeMs:      float64(readElapsed.Milliseconds()),
				ReadMBperSec:    readMBps,
				ReadMemAllocMB:  readAllocMB,
				ReadRows:        readN,
			})
		}

		printResults(t, "TRACES", results)
	})
}

func printResults(t *testing.T, signal string, results []realDataBenchResult) {
	t.Helper()

	if len(results) == 0 {
		return
	}

	t.Logf("\n=== REAL DATA ZSTD Compression Benchmark — %s (%d rows from E2E compose) ===",
		signal, results[0].TotalRows)
	t.Logf("Original on-disk: %.2f MB (already ZSTD compressed at default level)",
		float64(results[0].OrigCompressed)/(1<<20))
	t.Logf("Estimated raw: %.2f MB\n", float64(results[0].RawBytes)/(1<<20))

	t.Logf("%-6s %-25s %12s %8s | %10s %10s %10s | %10s %10s %10s",
		"Level", "Name", "Compressed", "Ratio",
		"Wr ms", "Wr MB/s", "Wr Mem MB",
		"Rd ms", "Rd MB/s", "Rd Mem MB")
	t.Logf("%-6s %-25s %12s %8s | %10s %10s %10s | %10s %10s %10s",
		"-----", "----", "----------", "-----",
		"-----", "-------", "---------",
		"-----", "-------", "---------")

	for _, r := range results {
		t.Logf("%-6d %-25s %10.2f MB %7.2fx | %10.0f %10.1f %10.1f | %10.0f %10.1f %10.1f",
			r.Level, r.LevelName,
			float64(r.NewCompressed)/(1<<20), r.Ratio,
			r.WriteTimeMs, r.WriteMBperSec, r.WriteMemAllocMB,
			r.ReadTimeMs, r.ReadMBperSec, r.ReadMemAllocMB)
	}

	// Print vs-level-3 comparison
	var level3 *realDataBenchResult
	for i := range results {
		if results[i].Level == 3 {
			level3 = &results[i]
			break
		}
	}
	if level3 != nil {
		t.Logf("\n--- vs Level 3 (default) ---")
		t.Logf("%-6s %12s %12s %12s %12s",
			"Level", "Size vs L3", "Write Spd", "Read Spd", "Wr Mem")
		for _, r := range results {
			sizeDelta := float64(r.NewCompressed-level3.NewCompressed) / float64(level3.NewCompressed) * 100
			writeSpd := r.WriteMBperSec / level3.WriteMBperSec * 100
			readSpd := r.ReadMBperSec / level3.ReadMBperSec * 100
			memDelta := r.WriteMemAllocMB / level3.WriteMemAllocMB * 100
			t.Logf("%-6d %+11.1f%% %11.0f%% %11.0f%% %11.0f%%",
				r.Level, sizeDelta, writeSpd, readSpd, memDelta)
		}
	}

	// Write vs read tradeoff summary
	t.Logf("\n--- Write/Read Tradeoff Summary ---")
	sort.Slice(results, func(i, j int) bool { return results[i].Level < results[j].Level })
	fastest := results[0]
	slowest := results[len(results)-1]
	t.Logf("Write speed range: %.0f MB/s (level %d) → %.0f MB/s (level %d) = %.1fx difference",
		fastest.WriteMBperSec, fastest.Level, slowest.WriteMBperSec, slowest.Level,
		fastest.WriteMBperSec/slowest.WriteMBperSec)
	t.Logf("Read speed range:  %.0f MB/s (level %d) → %.0f MB/s (level %d) = %.1fx difference",
		fastest.ReadMBperSec, fastest.Level, slowest.ReadMBperSec, slowest.Level,
		fastest.ReadMBperSec/slowest.ReadMBperSec)
	t.Logf("Compression range: %.2fx (level %d) → %.2fx (level %d)",
		fastest.Ratio, fastest.Level, slowest.Ratio, slowest.Level)

	// JSON output for machine parsing
	jsonOut, _ := json.MarshalIndent(results, "", "  ")
	t.Logf("\nJSON:\n%s", string(jsonOut))
}

func TestRealDataGoBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real data Go benchmark in short mode")
	}

	t.Run("logs_write", func(t *testing.T) {
		rows, _ := loadRealLogRows(t, realLogsDir, 100)
		t.Logf("Loaded %d log rows for Go benchmark", len(rows))

		levels := []int{1, 3, 7, 11}
		for _, level := range levels {
			t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
				const iterations = 3
				var totalWrite, totalRead time.Duration
				var compSize int64

				for i := 0; i < iterations; i++ {
					runtime.GC()
					start := time.Now()
					data, _, err := writeLogRowsAtLevel(rows, level, 10000)
					totalWrite += time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
					compSize = int64(len(data))

					runtime.GC()
					start = time.Now()
					_, err = readLogRows(data)
					totalRead += time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
				}

				avgWrite := totalWrite / iterations
				avgRead := totalRead / iterations
				t.Logf("Level %d: write=%v read=%v compressed=%.2f MB (%d rows, avg of %d runs)",
					level, avgWrite, avgRead, float64(compSize)/(1<<20), len(rows), iterations)
			})
		}
	})

	t.Run("traces_write", func(t *testing.T) {
		rows, _ := loadRealTraceRows(t, realTracesDir, 100)
		t.Logf("Loaded %d trace rows for Go benchmark", len(rows))

		levels := []int{1, 3, 7, 11}
		for _, level := range levels {
			t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
				const iterations = 3
				var totalWrite, totalRead time.Duration
				var compSize int64

				for i := 0; i < iterations; i++ {
					runtime.GC()
					start := time.Now()
					data, _, err := writeTraceRowsAtLevel(rows, level, 10000)
					totalWrite += time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
					compSize = int64(len(data))

					runtime.GC()
					start = time.Now()
					_, err = readTraceRows(data)
					totalRead += time.Since(start)
					if err != nil {
						t.Fatal(err)
					}
				}

				avgWrite := totalWrite / iterations
				avgRead := totalRead / iterations
				t.Logf("Level %d: write=%v read=%v compressed=%.2f MB (%d rows, avg of %d runs)",
					level, avgWrite, avgRead, float64(compSize)/(1<<20), len(rows), iterations)
			})
		}
	})
}
