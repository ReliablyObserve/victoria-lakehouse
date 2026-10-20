package compaction

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// attestation is how a compaction input's footer states lh.trace_id_hex.
type attestation int

const (
	attestYes    attestation = iota // "1"
	attestNo                        // "0"
	attestAbsent                    // no key: a file written before the key existed
)

const hexA = "0af7651916cd43dd8448eb211c80319c"
const hexB = "4bf92f3577b34da6a3ce929d0e0bf736"

func writeAttestedLogs(t *testing.T, rows []schema.LogRow, a attestation) []byte {
	t.Helper()
	if a == attestAbsent {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[schema.LogRow](&buf)
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	data, err := writeCompactedLogs(rows, 100, 1, a == attestYes)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeAttestedTraces(t *testing.T, rows []schema.TraceRow, a attestation) []byte {
	t.Helper()
	if a == attestAbsent {
		var buf bytes.Buffer
		w := parquet.NewGenericWriter[schema.TraceRow](&buf)
		if _, err := w.Write(rows); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	data, err := writeCompactedTraces(rows, 100, 1, a == attestYes)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func outputKV(t *testing.T, data []byte) string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	v, ok := f.Lookup(schema.TraceIDHexMetaKey)
	if !ok || (v != "0" && v != "1") {
		t.Fatalf("output footer %s = %q (present=%v), want ASCII 0 or 1", schema.TraceIDHexMetaKey, v, ok)
	}
	return v
}

type hexInput struct {
	ids []string
	a   attestation
}

// TestCompaction_TraceIDHexIsANDOfInputs: a compacted file attests
// lowercase-hex trace ids only when every input footer attests it AND every
// merged row is hex. An input without the key, or with "0", makes the output
// unattested even when its rows happen to be hex; the manifest entry records
// exactly what the footer says. Both signals.
func TestCompaction_TraceIDHexIsANDOfInputs(t *testing.T) {
	cases := []struct {
		name   string
		inputs []hexInput
		want   bool
	}{
		{"all_attested", []hexInput{{[]string{hexA}, attestYes}, {[]string{hexB, ""}, attestYes}}, true},
		{"one_absent_hex_rows", []hexInput{{[]string{hexA}, attestYes}, {[]string{hexB}, attestAbsent}}, false},
		{"one_declined_hex_rows", []hexInput{{[]string{hexA}, attestYes}, {[]string{hexB}, attestNo}}, false},
		{"one_non_hex", []hexInput{{[]string{hexA}, attestYes}, {[]string{"abc-def-ghi"}, attestNo}}, false},
		// A footer that claims "1" over non-hex rows (a writer bug) never
		// passes on: the merged rows decide too.
		{"forged_input_bit", []hexInput{{[]string{hexA}, attestYes}, {[]string{"4bf92f35-77b3-4da6-a3ce-929d0e0bf736"}, attestYes}}, false},
		{"uppercase", []hexInput{{[]string{hexA}, attestYes}, {[]string{"ABCDEF"}, attestYes}}, false},
		{"no_trace_ids", []hexInput{{[]string{""}, attestYes}, {[]string{""}, attestYes}}, true},
	}
	for _, mode := range []config.Mode{config.ModeLogs, config.ModeTraces} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/%s", mode, tc.name), func(t *testing.T) {
				pool := newMockPool()
				m := manifest.New("test-bucket", "")
				partition := "dt=2026-07-01/hour=03"
				var files []manifest.FileInfo
				var keys []string
				ts := int64(1000)
				for i, in := range tc.inputs {
					key := fmt.Sprintf("%s/src-%d.parquet", partition, i)
					var data []byte
					var n int
					if mode == config.ModeLogs {
						var rows []schema.LogRow
						for _, id := range in.ids {
							ts++
							rows = append(rows, schema.LogRow{TimestampUnixNano: ts, Body: "b", ServiceName: "svc", TraceID: id})
						}
						data, n = writeAttestedLogs(t, rows, in.a), len(rows)
					} else {
						var rows []schema.TraceRow
						for _, id := range in.ids {
							ts++
							rows = append(rows, schema.TraceRow{TimestampUnixNano: ts, SpanID: fmt.Sprint(ts), ServiceName: "svc", TraceID: id})
						}
						data, n = writeAttestedTraces(t, rows, in.a), len(rows)
					}
					pool.put(key, data)
					fi := manifest.FileInfo{Key: key, Size: int64(len(data)), RowCount: int64(n), MinTimeNs: 1000, MaxTimeNs: ts, TraceIDHex: in.a == attestYes}
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
				kv := outputKV(t, pool.get(res.OutputFile))
				if (kv == "1") != tc.want {
					t.Errorf("output footer attests %q, want attested=%v", kv, tc.want)
				}
				fi, ok := m.GetFileByKey(res.OutputFile)
				if !ok {
					t.Fatalf("output %s not in the manifest", res.OutputFile)
				}
				if fi.TraceIDHex != tc.want {
					t.Errorf("manifest TraceIDHex=%v, want %v", fi.TraceIDHex, tc.want)
				}
			})
		}
	}
}
