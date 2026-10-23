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

// mixedRows returns one span per trace ID: three over-long IDs (sorting first,
// in the middle and last) among short ones. spanStart is the first span's
// start; every span lasts 5 ns, and trace i starts at spanStart+10*i.
func mixedRows(longLen int, spanStart int64) (rows []schema.TraceRow, ids []string) {
	ids = []string{
		strings.Repeat("0", longLen), "a-short", "b-short",
		strings.Repeat("m", longLen), "n-short", "o-short",
		strings.Repeat("z", longLen),
	}
	for i, id := range ids {
		st := spanStart + int64(10*i)
		rows = append(rows, schema.TraceRow{
			TimestampUnixNano: st, StartTimeUnixNano: schema.Int64Ptr(st), DurationNs: schema.Int64Ptr(5),
			TraceID: id, SpanID: fmt.Sprintf("%016x", i), SpanName: fmt.Sprintf("op%d", i), ServiceName: "alpha",
			Stream: `{resource_attr:service.name="alpha"}`, StreamID: fmt.Sprintf("%048x", 7),
		})
	}
	return rows, ids
}

func footerMeta(t *testing.T, data []byte) *format.FileMetaData {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f.Metadata()
}

// TestTraceIndex_LongTraceIDsAreLeftOutAndEveryShortIDIsListed pins #768 in
// the traces module's flusher: over-long IDs at the first, middle and last
// position leave the footer index listing EVERY short ID (so the pre-filter
// still prunes), and the pre-filter keeps the file for a queried over-long ID,
// which the index cannot list.
func TestTraceIndex_LongTraceIDsAreLeftOutAndEveryShortIDIsListed(t *testing.T) {
	for _, longLen := range []int{traceindex.MaxTraceIDLen, 65536, 65536 + 5, 1 << 17} {
		t.Run(fmt.Sprint(longLen), func(t *testing.T) {
			rows, ids := mixedRows(longLen, 1_000)
			res, err := writeTracesParquet(rows, 10, 3)
			if err != nil {
				t.Fatal(err)
			}
			meta := footerMeta(t, res.Data)
			var raw string
			for _, kv := range meta.KeyValueMetadata {
				if kv.Key == traceIndexMetadataKey {
					raw = kv.Value
				}
			}
			if raw == "" {
				t.Fatal("the file has no index although it has short IDs")
			}
			entries, err := unmarshalTraceIndex([]byte(raw))
			if err != nil {
				t.Fatalf("index does not decode: %v", err)
			}
			listed := map[string]TraceIndexEntry{}
			for _, e := range entries {
				listed[e.TraceID] = e
			}
			for i, id := range ids {
				e, in := listed[id]
				if traceindex.Indexable(id) != in {
					t.Fatalf("id #%d (%d bytes): listed=%v, want %v", i, len(id), in, traceindex.Indexable(id))
				}
				if in && (e.StartNs != int64(1_000+10*i) || e.EndNs != int64(1_000+10*i+5)) {
					t.Fatalf("id #%d: bounds [%d,%d]", i, e.StartNs, e.EndNs)
				}
				if !traceIdxKeepFile(meta, map[string]bool{id: true}) {
					t.Fatalf("id #%d (%d bytes) is in the file but the pre-filter drops it", i, len(id))
				}
			}
			if traceIdxKeepFile(meta, map[string]bool{"absent-short": true}) {
				t.Fatal("the index lists every short ID, so an absent short ID must still drop the file")
			}
			// A query naming an over-long ID the file does not hold: the index
			// cannot say so, the file is kept (scanned) and classified unindexed.
			absentLong := map[string]bool{strings.Repeat("q", traceindex.MaxTraceIDLen+1): true}
			if !traceIdxKeepFile(meta, absentLong) || traceIdxClassifyFile(meta, absentLong) != "kept_unindexed" {
				t.Fatal("an over-long queried ID must keep the file")
			}
			// trace_id:in(...) with ANY over-long id keeps the file.
			mixedQuery := map[string]bool{"absent-short": true, strings.Repeat("q", traceindex.MaxTraceIDLen+1): true}
			if !traceIdxKeepFile(meta, mixedQuery) || traceIdxClassifyFile(meta, mixedQuery) != "kept_unindexed" {
				t.Fatal("a query listing a short and an over-long ID must keep the file")
			}
			// The footer lookup: a short ID resolves, an over-long one is a miss.
			if e, ok, _ := findTraceIDInFooterMeta(meta, "n-short"); !ok || e.StartNs != 1_040 {
				t.Fatalf("short ID lookup: %+v ok=%v", e, ok)
			}
			if _, ok, _ := findTraceIDInFooterMeta(meta, ids[0]); ok == !traceindex.Indexable(ids[0]) {
				t.Fatalf("lookup of a %d-byte ID: ok=%v", len(ids[0]), ok)
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

// TestLookupTraceIndex_SplitTraceKeepsFullBoundWithLongIDInTheFile is the
// reviewer's lock: file A holds trace T plus a 65,541-byte ID that sorts AFTER
// T, file B holds T a minute later. LookupTraceIndex(T) must return the whole
// bound [A..B] (an index-less A would contribute nothing and narrow the
// answer to B), and trace-by-ID must return every span of T.
func TestLookupTraceIndex_SplitTraceKeepsFullBoundWithLongIDInTheFile(t *testing.T) {
	const tid = "abcdef0123456789abcdef0123456789"
	long := strings.Repeat("z", 65541) // sorts after tid
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	span := func(id string, i int, at time.Time) schema.TraceRow {
		ts := at.UnixNano()
		return schema.TraceRow{
			TimestampUnixNano: ts, StartTimeUnixNano: schema.Int64Ptr(ts), DurationNs: schema.Int64Ptr(1_000),
			TraceID: id, SpanID: fmt.Sprintf("%016x", i), SpanName: "op", ServiceName: "alpha",
			Stream: `{resource_attr:service.name="alpha"}`, StreamID: fmt.Sprintf("%048x", 7),
		}
	}
	aStart, bStart := base, base.Add(time.Minute)
	resA, err := writeTracesParquet([]schema.TraceRow{span(tid, 1, aStart), span(tid, 2, aStart.Add(time.Second)), span(long, 3, aStart)}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	resB, err := writeTracesParquet([]schema.TraceRow{span(tid, 4, bStart), span(tid, 5, bStart.Add(time.Second))}, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	dir := fmt.Sprintf("traces/dt=%s/hour=%02d", base.Format("2006-01-02"), base.Hour())
	registerFileInMockS3(t, s, mock, dir+"/a.parquet", resA.Data, aStart)
	registerFileInMockS3(t, s, mock, dir+"/b.parquet", resB.Data, bStart)

	startNs, endNs, found, err := s.LookupTraceIndex(context.Background(), nil, tid)
	if err != nil || !found {
		t.Fatalf("LookupTraceIndex(T): found=%v err=%v", found, err)
	}
	wantStart, wantEnd := aStart.UnixNano(), bStart.Add(time.Second).UnixNano()+1_000
	if startNs != wantStart || endNs != wantEnd {
		t.Fatalf("bound = [%d,%d], want the full [%d,%d] across A and B", startNs, endNs, wantStart, wantEnd)
	}
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	if got := len(run(context.Background(), fmt.Sprintf(`trace_id:=%s`, tid))); got != 4 {
		t.Fatalf("trace-by-ID returned %d spans of T, want 4", got)
	}
}

// TestTraceIndex_LongTraceIDColdTraceByID is the end-to-end form: one file holds
// over-long trace IDs at the first, middle and last position plus short ones.
// Every short trace is read back by ID (through the pre-filter), and so is
// each over-long ID itself (the pre-filter keeps the file, the footer lookup
// misses and the span scan answers) with its exact spans.
func TestTraceIndex_LongTraceIDColdTraceByID(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.cfg.Mode = config.ModeTraces
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	rows, ids := mixedRows(65536+5, base.UnixNano())
	// A second span for the middle short trace, so counts differ per trace.
	extra := rows[4]
	extra.SpanID = "00000000000000ff"
	rows = append(rows, extra)
	res, err := writeTracesParquet(rows, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	registerFileInMockS3(t, s, mock, fmt.Sprintf("traces/dt=%s/hour=%02d/long.parquet", base.Format("2006-01-02"), base.Hour()), res.Data, base)
	run := tracesRunner(t, s, base.Add(-time.Hour).UnixNano(), base.Add(time.Hour).UnixNano())
	for i, id := range ids {
		want := 1
		if i == 4 {
			want = 2
		}
		if got := len(run(context.Background(), fmt.Sprintf(`trace_id:=%s`, id))); got != want {
			t.Fatalf("trace-by-ID of id #%d (%d bytes) returned %d spans, want %d", i, len(id), got, want)
		}
	}
	if got := len(run(context.Background(), fmt.Sprintf(`trace_id:in(%s,%s)`, ids[1], ids[3]))); got != 2 {
		t.Fatalf("trace_id:in(short, long) returned %d spans, want 2", got)
	}
	if got := len(run(context.Background(), `trace_id:=absent-short`)); got != 0 {
		t.Fatalf("absent short id returned %d spans", got)
	}
}
