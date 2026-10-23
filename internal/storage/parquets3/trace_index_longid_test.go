package parquets3

import (
	"bytes"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// longIDPositions are the over-long trace IDs of the position tests, sorted
// before ("0"), between ("m") and after ("z") the short IDs.
func longIDFile(t *testing.T, longLen int) (rows []schema.TraceRow, wantShort map[string][2]int64) {
	t.Helper()
	wantShort = map[string][2]int64{}
	ids := []string{
		strings.Repeat("0", longLen), "a-short", "b-short",
		strings.Repeat("m", longLen), "n-short", "o-short",
		strings.Repeat("z", longLen),
	}
	for i, id := range ids {
		start := int64(1_000 + 10*i)
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: start, StartTimeUnixNano: schema.Int64Ptr(start), DurationNs: schema.Int64Ptr(5),
			TraceID: id, SpanID: "s1", SpanName: "op", ServiceName: "svc",
		})
		if len(id) <= traceindex.MaxTraceIDLen {
			wantShort[id] = [2]int64{start, start + 5}
		}
	}
	return rows, wantShort
}

func footerIndexOf(t *testing.T, data []byte) (string, bool) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
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

// TestWriteTracesParquet_LongTraceIDsAreLeftOutOfTheIndexAndNothingElse pins
// #768 for the root module's trace writer: IDs above the uint16 limit, at the
// first, middle and last position of the file, are left out of `_trace_idx`;
// the index still lists EVERY short ID with its exact bounds, and the file
// never carries a wrapped index.
func TestWriteTracesParquet_LongTraceIDsAreLeftOutOfTheIndexAndNothingElse(t *testing.T) {
	for _, n := range []int{traceindex.MaxTraceIDLen + 1, traceindex.MaxTraceIDLen + 5, 1 << 17} {
		rows, want := longIDFile(t, n)
		entriesBefore := metrics.WriterTraceIdxEntriesTotal.Get()
		res, err := writeTracesParquet(rows, 10, 3)
		if err != nil {
			t.Fatal(err)
		}
		// The entries counter counts the entries listed, not the over-long
		// IDs the index leaves out.
		if d := metrics.WriterTraceIdxEntriesTotal.Get() - entriesBefore; d != uint64(len(want)) {
			t.Fatalf("long IDs of %d bytes: trace_idx_entries_total grew by %d, want the %d listed", n, d, len(want))
		}
		v, ok := footerIndexOf(t, res.Data)
		if !ok {
			t.Fatalf("long IDs of %d bytes: the file has no index although it has short IDs", n)
		}
		entries, err := unmarshalTraceIndex([]byte(v))
		if err != nil {
			t.Fatalf("long IDs of %d bytes: index does not decode: %v", n, err)
		}
		if len(entries) != len(want) {
			t.Fatalf("long IDs of %d bytes: index lists %d IDs, want every one of the %d short IDs", n, len(entries), len(want))
		}
		for _, e := range entries {
			w, in := want[e.TraceID]
			if !in || e.StartNs != w[0] || e.EndNs != w[1] {
				t.Fatalf("long IDs of %d bytes: unexpected entry %q [%d,%d]", n, e.TraceID, e.StartNs, e.EndNs)
			}
		}
	}
	// The longest representable ID is still listed.
	rows, want := longIDFile(t, traceindex.MaxTraceIDLen)
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	v, _ := footerIndexOf(t, res.Data)
	if entries, err := unmarshalTraceIndex([]byte(v)); err != nil || len(entries) != len(want) {
		t.Fatalf("65535-byte IDs: %d entries (err %v), want all %d listed", len(entries), err, len(want))
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
