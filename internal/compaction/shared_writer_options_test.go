package compaction

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The compaction writers add nothing to the shared writer and take nothing away:
// their output is the shared writer's output for the level, row-group size,
// attestation and slot binding they were given. A caller that drifts (another
// level, another row-group size, a dropped resolver, a flipped attestation) is
// caught here, per signal.
func TestWriteCompactedIsTheSharedWriterWithTheGivenOptions(t *testing.T) {
	saved := activeSlotResolver
	t.Cleanup(func() { activeSlotResolver = saved })
	resolver := schema.NewSlotResolver([]schema.SlotAttr{{Name: "tenant.tier", Bloom: true}})
	logs := make([]schema.LogRow, 450)
	spans := make([]schema.TraceRow, 450)
	for i := range logs {
		logs[i] = schema.LogRow{TimestampUnixNano: 1_000 + int64(i), Body: fmt.Sprintf("request %d served", i), ServiceName: "svc", TraceID: fmt.Sprintf("%032x", i)}
		schema.SetLogSlot(&logs[i], "ded_s01", fmt.Sprintf("tier-%d", i%3))
		spans[i] = schema.TraceRow{TimestampUnixNano: 1_000 + int64(i), TraceID: fmt.Sprintf("%032x", i/3), SpanID: fmt.Sprintf("%016x", i), SpanName: "op", Body: fmt.Sprintf("event %d", i)}
	}
	for _, slots := range []*schema.SlotResolver{nil, resolver} {
		activeSlotResolver = slots
		for _, level := range []int{1, 7, 11} {
			for _, hex := range []bool{false, true} {
				o := parquetwriter.Options{RowGroupSize: 100, CompressionLevel: level, Slots: slots, TraceIDHex: hex}
				name := fmt.Sprintf("slots=%v/level=%d/hex=%v", slots != nil, level, hex)

				got, err := writeCompactedLogs(logs, o.RowGroupSize, level, hex)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				want, err := parquetwriter.WriteLogs(logs, o)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if !bytes.Equal(got, want.Data) {
					t.Errorf("logs %s: the compacted object is not the shared writer's output for the same options", name)
				}

				got, err = writeCompactedTraces(spans, o.RowGroupSize, level, hex)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				wantT, err := parquetwriter.WriteTraces(spans, o)
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				if !bytes.Equal(got, wantT.Data) {
					t.Errorf("traces %s: the compacted object is not the shared writer's output for the same options", name)
				}
			}
		}
	}
}

// Through the real Compactor and the DEFAULT compaction schedule (zstd 7 and
// 10,000-row groups for an L1 output, zstd 11 and 20,000 for L2), the output is
// the shared writer's output for the level and row-group size the schedule names.
// The level is not recorded in the footer, so a call site that drifts from the
// schedule (another level, another size) is caught only by comparing the bytes.
func TestCompactUsesTheDefaultScheduleThroughTheSharedWriter(t *testing.T) {
	cfg := config.Default().Compaction
	for _, mode := range []config.Mode{config.ModeLogs, config.ModeTraces} {
		for _, c := range []struct {
			sourceLevel, rows int
		}{{0, 12000}, {1, 25000}} {
			outLevel := c.sourceLevel + 1
			wantLevel, wantRG := cfg.CompressionLevelForOutput(outLevel), cfg.RowGroupSizeForOutput(outLevel)
			if (outLevel == 1 && (wantLevel != 7 || wantRG != 10000)) || (outLevel == 2 && (wantLevel != 11 || wantRG != 20000)) {
				t.Fatalf("the default schedule changed (L%d: zstd %d, %d rows); update this test with the schedule", outLevel, wantLevel, wantRG)
			}
			t.Run(fmt.Sprintf("%s/L%d", mode, outLevel), func(t *testing.T) {
				pool := newMockPool()
				m := manifest.New("test-bucket", "data/")
				const partition = "dt=2026-05-04/hour=10"
				var files []manifest.FileInfo
				var inputs [][]byte
				for i := 0; i < 2; i++ {
					var data []byte
					if mode == config.ModeLogs {
						rows := make([]schema.LogRow, 0, c.rows/2)
						for j := i; j < c.rows; j += 2 {
							rows = append(rows, schema.LogRow{TimestampUnixNano: 1_000 + int64(j), Body: fmt.Sprintf("request %d served", j), ServiceName: "svc", SeverityText: "INFO", TraceID: fmt.Sprintf("%032x", j)})
						}
						data = makeTestParquet(t, rows)
					} else {
						rows := make([]schema.TraceRow, 0, c.rows/2)
						for j := i; j < c.rows; j += 2 {
							rows = append(rows, schema.TraceRow{TimestampUnixNano: 1_000 + int64(j), TraceID: fmt.Sprintf("%032x", j/3), SpanID: fmt.Sprintf("%016x", j), SpanName: "op", ServiceName: "svc"})
						}
						data = makeTestTraceParquet(t, rows)
					}
					key := fmt.Sprintf("data/%s/batch-%d.parquet", partition, i)
					pool.put(key, data)
					fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: int64(c.rows / 2), SchemaFingerprint: "fp", CompactionLevel: c.sourceLevel}
					m.AddFile(partition, fi)
					files = append(files, fi)
					inputs = append(inputs, data)
				}
				comp := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Prefix: "data/", Mode: mode, RowGroupSize: 777, CompressionLevel: 1, CompactionConfig: cfg})
				res, err := comp.Compact(context.Background(), partition, files, c.sourceLevel)
				if err != nil {
					t.Fatal(err)
				}
				if len(res.OutputFiles) != 1 {
					t.Fatalf("%d outputs", len(res.OutputFiles))
				}
				got := pool.get(res.OutputFile)

				var want []byte
				if mode == config.ModeLogs {
					rows, err := readLogRows(got)
					if err != nil || len(rows) != c.rows {
						t.Fatalf("read back %d rows (%v), want %d", len(rows), err, c.rows)
					}
					want, err = writeCompactedLogs(rows, wantRG, wantLevel, schema.CompactedTraceIDHex(inputs, schema.LogRowsTraceIDHex(rows)))
					if err != nil {
						t.Fatal(err)
					}
				} else {
					rows, err := readTraceRows(got)
					if err != nil || len(rows) != c.rows {
						t.Fatalf("read back %d rows (%v), want %d", len(rows), err, c.rows)
					}
					want, err = writeCompactedTraces(rows, wantRG, wantLevel, schema.CompactedTraceIDHex(inputs, schema.TraceRowsTraceIDHex(rows)))
					if err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(got, want) {
					t.Errorf("the compacted object (%d bytes) is not the shared writer's output at zstd %d and %d rows per row group (%d bytes): the call site drifted from the schedule", len(got), wantLevel, wantRG, len(want))
				}
			})
		}
	}
}
