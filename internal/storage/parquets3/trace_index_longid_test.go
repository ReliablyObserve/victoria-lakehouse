package parquets3

import (
	"bytes"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// longIDFooterIndex writes a trace file whose first trace ID is idLen bytes
// long (it sorts before the normal one) and returns the file's `_trace_idx`
// footer value, if any.
func longIDFooterIndex(t *testing.T, idLen int) (string, bool) {
	t.Helper()
	mk := func(tid string, i int64) schema.TraceRow {
		return schema.TraceRow{
			TimestampUnixNano: 1_000 + i, StartTimeUnixNano: schema.Int64Ptr(1_000 + i), DurationNs: schema.Int64Ptr(10),
			TraceID: tid, SpanID: "s1", SpanName: "op", ServiceName: "svc",
		}
	}
	res, err := writeTracesParquet([]schema.TraceRow{mk(strings.Repeat("0", idLen), 0), mk("abcdef0123456789", 1)}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range f.Metadata().KeyValueMetadata {
		if kv.Key == traceIndexMetadataKey {
			return kv.Value, true
		}
	}
	return "", false
}

// TestWriteTracesParquet_LongTraceIDLeavesNoWrappedIndex pins #768 for the
// root module's trace writer: an ID above the uint16 limit leaves the file
// without a `_trace_idx`; the longest representable ID is still indexed.
func TestWriteTracesParquet_LongTraceIDLeavesNoWrappedIndex(t *testing.T) {
	for _, n := range []int{traceindex.MaxTraceIDLen + 1, traceindex.MaxTraceIDLen + 5, 1 << 17} {
		if v, ok := longIDFooterIndex(t, n); ok {
			entries, err := unmarshalTraceIndex([]byte(v))
			t.Fatalf("a %d-byte trace ID produced an index (%d entries, decode err %v)", n, len(entries), err)
		}
	}
	v, ok := longIDFooterIndex(t, traceindex.MaxTraceIDLen)
	if !ok {
		t.Fatal("a 65535-byte trace ID is representable and must be indexed")
	}
	entries, err := unmarshalTraceIndex([]byte(v))
	if err != nil || len(entries) != 2 {
		t.Fatalf("decode: %d entries, err=%v", len(entries), err)
	}
}

// TestTraceIndexFromMetadata_RejectsWrappedLegacyValue: a `_trace_idx` value
// written by an older release with a wrapped length (a 65,541-byte ID then a
// normal one) is read as "no usable index", never as wrong entries.
func TestTraceIndexFromMetadata_RejectsWrappedLegacyValue(t *testing.T) {
	long := strings.Repeat("0", 1<<16+5)
	var b bytes.Buffer
	b.Write([]byte{traceindex.Version, 2, 0, 0, 0})
	put := func(id string, start, end byte) {
		b.Write([]byte{byte(len(id)), byte(len(id) >> 8)}) // uint16 wrap, as the old writer did
		b.WriteString(id)
		b.Write([]byte{1, 0, start, 0, 0, 0, 0, 0, 0, 0, end, 0, 0, 0, 0, 0, 0, 0})
	}
	put(long, 10, 20)
	put("abcdef0123456789", 30, 40)
	if entries, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: b.String()}); ok {
		t.Fatalf("a wrapped legacy index was accepted: %+v", entries)
	}
}
