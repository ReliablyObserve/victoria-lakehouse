package compaction

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetfooter"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestCompaction_RecordsFooterTail: the manifest entry of a compacted object
// carries the bytes at the end of the object a footer read needs (page-index
// stripe and footer), measured on the object written, so a cold read of it is one
// exact request. Both signals.
func TestCompaction_RecordsFooterTail(t *testing.T) {
	for _, mode := range []config.Mode{config.ModeLogs, config.ModeTraces} {
		t.Run(string(mode), func(t *testing.T) {
			pool := newMockPool()
			m := manifest.New("test-bucket", "")
			partition := "dt=2026-07-01/hour=03"
			var files []manifest.FileInfo
			var keys []string
			ts := int64(1000)
			for i := 0; i < 2; i++ {
				key := fmt.Sprintf("%s/src-%d.parquet", partition, i)
				var data []byte
				const n = 250
				if mode == config.ModeLogs {
					var rows []schema.LogRow
					for j := 0; j < n; j++ {
						ts++
						rows = append(rows, schema.LogRow{TimestampUnixNano: ts, Body: fmt.Sprint("body ", ts), ServiceName: "svc", TraceID: hexA})
					}
					data = writeAttestedLogs(t, rows, attestYes)
				} else {
					var rows []schema.TraceRow
					for j := 0; j < n; j++ {
						ts++
						rows = append(rows, schema.TraceRow{TimestampUnixNano: ts, SpanID: fmt.Sprint(ts), ServiceName: "svc", TraceID: hexA})
					}
					data = writeAttestedTraces(t, rows, attestYes)
				}
				pool.put(key, data)
				fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: n, MinTimeNs: 1000, MaxTimeNs: ts, TraceIDHex: true}
				m.AddFile(partition, fi)
				files = append(files, fi)
				keys = append(keys, key)
			}
			markListed(t, m, keys)
			c := NewCompactor(CompactorConfig{Pool: pool, Manifest: m, Mode: mode, RowGroupSize: 100})
			res, err := c.Compact(context.Background(), partition, files, 0)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			out := pool.get(res.OutputFile)
			fi, ok := m.GetFileByKey(res.OutputFile)
			if !ok {
				t.Fatalf("output %s not in the manifest", res.OutputFile)
			}
			want := parquetfooter.Inspect(out)
			if want.Tail <= 0 || fi.FooterTail != want.Tail {
				t.Fatalf("manifest FooterTail=%d, the output's footer says %d", fi.FooterTail, want.Tail)
			}
			if fi.BloomBytes != want.BloomBytes || fi.BloomBytes == 0 {
				t.Fatalf("manifest BloomBytes=%d, the output's footer says %d (the single footer parse must still feed it)", fi.BloomBytes, want.BloomBytes)
			}
			if len(fi.ColumnBytes) == 0 || !reflect.DeepEqual(fi.ColumnBytes, want.ColumnBytes) {
				t.Fatalf("manifest ColumnBytes=%v, the footer says %v", fi.ColumnBytes, want.ColumnBytes)
			}
			if got := stripeTail(t, out); got != fi.FooterTail {
				t.Fatalf("recorded %d, but the object's page-index stripe starts %d bytes from the end", fi.FooterTail, got)
			}
		})
	}
}

// stripeTail is the distance from the end of data to its first page-index section,
// measured with no help from parquetfooter.
func stripeTail(t *testing.T, data []byte) int32 {
	t.Helper()
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	first := int64(len(data))
	for _, rg := range pf.Metadata().RowGroups {
		for _, c := range rg.Columns {
			for _, o := range []int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if o > 0 && o < first {
					first = o
				}
			}
		}
	}
	return int32(int64(len(data)) - first)
}
