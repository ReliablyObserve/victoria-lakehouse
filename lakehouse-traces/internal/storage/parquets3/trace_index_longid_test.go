package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// longIDRows returns spans of one trace whose ID is longID bytes long (it
// sorts before the normal ID) plus spans of a normal trace.
func longIDRows(longID int, normal string, base time.Time) []schema.TraceRow {
	mk := func(tid string, i int) schema.TraceRow {
		ts := base.Add(time.Duration(i) * time.Second).UnixNano()
		return schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts),
			TraceID: tid, SpanID: fmt.Sprintf("%016x", i), SpanName: fmt.Sprintf("op%d", i), ServiceName: "alpha",
			Stream: `{resource_attr:service.name="alpha"}`, StreamID: fmt.Sprintf("%048x", 7),
		}
	}
	rows := []schema.TraceRow{mk(strings.Repeat("0", longID), 0)}
	for i := 1; i <= 3; i++ {
		rows = append(rows, mk(normal, i))
	}
	return rows
}

func footerMeta(t *testing.T, data []byte) *format.FileMetaData {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f.Metadata()
}

// TestTraceIndex_LongTraceIDDoesNotHideNormalTrace pins #768: one trace ID of
// 65,536+ bytes in a file used to wrap the uint16 length in the `_trace_idx`
// footer, the index decoded into garbage, and the pre-filter dropped the file
// for the normal trace stored next to it (trace-by-ID silently lost its
// spans). The file must now carry no index (or a correct one), the pre-filter
// must keep it and the footer lookup must not report a wrong answer.
func TestTraceIndex_LongTraceIDDoesNotHideNormalTrace(t *testing.T) {
	const normal = "abcdef0123456789abcdef0123456789"
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	for _, longLen := range []int{65535, 65536, 65536 + 5, 1 << 17} {
		t.Run(fmt.Sprint(longLen), func(t *testing.T) {
			res, err := writeTracesParquet(longIDRows(longLen, normal, base), 10, 3)
			if err != nil {
				t.Fatal(err)
			}
			meta := footerMeta(t, res.Data)
			if !traceIdxKeepFile(meta, map[string]bool{normal: true}) {
				t.Fatalf("the pre-filter drops a file that holds the queried trace (long ID of %d bytes)", longLen)
			}
			e, ok, err := findTraceIDInFooterMeta(meta, normal)
			if err != nil {
				t.Fatal(err)
			}
			hasIdx := false
			for _, kv := range meta.KeyValueMetadata {
				hasIdx = hasIdx || kv.Key == traceIndexMetadataKey
			}
			switch {
			case longLen > traceindex.MaxTraceIDLen && hasIdx:
				t.Fatalf("a %d-byte trace ID must leave the file without an index", longLen)
			case longLen <= traceindex.MaxTraceIDLen && (!hasIdx || !ok):
				t.Fatalf("a %d-byte trace ID is representable: want an index listing the normal trace (hasIdx=%v ok=%v)", longLen, hasIdx, ok)
			case ok && (e.TraceID != normal || e.StartNs > e.EndNs):
				t.Fatalf("lookup returned a wrong entry %+v", e)
			}
		})
	}
}

// TestTraceIndex_LegacyWrappedIndexKeepsFile: a file written by an older
// release carries a wrapped `_trace_idx`; readers must treat it as unusable
// (keep the file, miss in the footer lookup, fall back to the span scan).
func TestTraceIndex_LegacyWrappedIndexKeepsFile(t *testing.T) {
	const normal = "abcdef0123456789abcdef0123456789"
	for _, longLen := range []int{1<<16 + 1, 1<<16 + 5, 1 << 17} {
		wrapped := wrappedLegacyIndex(longLen, normal)
		meta := &format.FileMetaData{KeyValueMetadata: []format.KeyValue{{Key: traceIndexMetadataKey, Value: string(wrapped)}}}
		if !traceIdxKeepFile(meta, map[string]bool{normal: true}) {
			t.Fatalf("long ID %d: a wrapped legacy index dropped the file", longLen)
		}
		if got := traceIdxClassifyFile(meta, map[string]bool{normal: true}); got != "kept_error" {
			t.Fatalf("long ID %d: classified %q, want kept_error", longLen, got)
		}
		if _, ok, _ := findTraceIDInFooterMeta(meta, normal); ok {
			t.Fatalf("long ID %d: the footer lookup answered from a wrapped index", longLen)
		}
	}
}

// wrappedLegacyIndex builds the footer value the pre-#768 writer produced for
// a long ID followed by a normal one: the uint16 length wraps.
func wrappedLegacyIndex(longLen int, normal string) []byte {
	var b bytes.Buffer
	b.Write([]byte{traceindex.Version, 2, 0, 0, 0})
	put := func(id string, idLen int, start, end int64) {
		b.Write([]byte{byte(idLen), byte(idLen >> 8)}) // uint16 wrap
		b.WriteString(id)
		b.Write([]byte{1, 0})
		for _, v := range []int64{start, end} {
			for i := 0; i < 8; i++ {
				b.WriteByte(byte(v >> (8 * i)))
			}
		}
	}
	put(strings.Repeat("0", longLen), longLen, 10, 20)
	put(normal, len(normal), 30, 40)
	return b.Bytes()
}

// TestTraceIndex_LongTraceIDColdTraceByID is the end-to-end form: a file that
// holds a 64 KiB+ trace ID and a normal trace is flushed to S3 and the normal
// trace is read back by ID through the cold-tier query path with all of its
// spans (the old index made the pre-filter drop the file).
func TestTraceIndex_LongTraceIDColdTraceByID(t *testing.T) {
	const normal = "abcdef0123456789abcdef0123456789"
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	res, err := writeTracesParquet(longIDRows(1<<16+5, normal, base), 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/long.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	if got := len(run(context.Background(), fmt.Sprintf(`trace_id:=%s`, normal))); got != 3 {
		t.Fatalf("trace-by-ID returned %d spans of the normal trace, want 3", got)
	}
}
