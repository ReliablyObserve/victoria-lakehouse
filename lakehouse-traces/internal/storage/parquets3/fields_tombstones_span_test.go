package parquets3

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Mirror of internal/storage/parquets3/fields_tombstones_span_test.go: the
// enumeration scans read whole files, so the tombstones applied to a scanned
// file's rows must be those overlapping the file, not only the query window.

type traceSpanFixture struct {
	storage *Storage
	query   *logstorage.Query
}

func newTraceSpanFixture(t *testing.T) *traceSpanFixture {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)

	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	early := now.Add(-10 * time.Second)
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: early.UnixNano(), ServiceName: "old-svc", SpanName: "GET /old", TraceID: "t1", SpanID: "s1", Stream: `{svc="old"}`, StreamID: "stream-old"},
		{TimestampUnixNano: now.UnixNano(), ServiceName: "new-svc", SpanName: "GET /new", TraceID: "t2", SpanID: "s2", Stream: `{svc="new"}`, StreamID: "stream-new"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(early.UnixNano(), now.UnixNano())
	if len(files) != 1 || files[0].MinTimeNs == files[0].MaxTimeNs {
		t.Fatalf("fixture: want one file spanning both spans, got %+v", files)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{ID: "edge", Query: "*", StartNs: early.UnixNano(), EndNs: early.Add(time.Second).UnixNano(), Mode: "hide"})
	s.SetTombstoneStore(store)

	return &traceSpanFixture{
		storage: s,
		query:   mustParseQueryWithTime(t, "*", now.Add(-time.Second).UnixNano(), now.Add(time.Second).UnixNano()),
	}
}

func assertTraceNotEnumerated(t *testing.T, endpoint string, got []logstorage.ValueWithHits, deleted, kept string) {
	t.Helper()
	var sawKept bool
	for _, v := range got {
		if v.Value == deleted {
			t.Fatalf("%s enumerates %q, whose only span is tombstoned: %v", endpoint, deleted, traceValueStrings(got))
		}
		if v.Value == kept {
			sawKept = true
		}
	}
	if !sawKept {
		t.Fatalf("%s lost the untouched value %q: %v", endpoint, kept, traceValueStrings(got))
	}
}

func TestTraceFieldValues_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newTraceSpanFixture(t)
	got, err := f.storage.GetFieldValues(context.Background(), nil, f.query, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	assertTraceNotEnumerated(t, "field_values", got, "old-svc", "new-svc")
}

func TestTraceGetStreams_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newTraceSpanFixture(t)
	got, err := f.storage.GetStreams(context.Background(), nil, f.query, 100)
	if err != nil {
		t.Fatalf("GetStreams: %v", err)
	}
	assertTraceNotEnumerated(t, "streams", got, `{svc="old"}`, `{svc="new"}`)
}

func TestTraceGetStreamIDs_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newTraceSpanFixture(t)
	got, err := f.storage.GetStreamIDs(context.Background(), nil, f.query, 100)
	if err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
	assertTraceNotEnumerated(t, "stream_ids", got, "stream-old", "stream-new")
}

// TestTraceFieldValues_LabelIndexGatedByATombstoneAnywhere: the label index is
// not time-scoped, so any active tombstone can cover a value it would serve.
func TestTraceFieldValues_LabelIndexGatedByATombstoneAnywhere(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)

	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	yesterday := now.Add(-24 * time.Hour)
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: yesterday.UnixNano(), ServiceName: "secret-svc", SpanName: "x", TraceID: "t1", SpanID: "s1"},
	})
	bw.triggerFlush()
	bw.AddTraceRows([]schema.TraceRow{
		{TimestampUnixNano: now.UnixNano(), ServiceName: "web", SpanName: "y", TraceID: "t2", SpanID: "s2"},
	})
	bw.triggerFlush()
	s.labelIndex.Add("service.name", []string{"secret-svc", "web"})

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{ID: "yesterday", Query: `service.name:="secret-svc"`,
		StartNs: yesterday.Add(-time.Minute).UnixNano(), EndNs: yesterday.Add(time.Minute).UnixNano(), Mode: "hide"})
	s.SetTombstoneStore(store)

	q := mustParseQueryWithTime(t, "*", now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	assertTraceNotEnumerated(t, "field_values", got, "secret-svc", "web")
}

func TestTraceFilesTimeSpan(t *testing.T) {
	files := []manifest.FileInfo{
		{Key: "a", MinTimeNs: 100, MaxTimeNs: 200},
		{Key: "b", MinTimeNs: 50, MaxTimeNs: 150},
	}
	if lo, hi := filesTimeSpan(files, 120, 130); lo != 50 || hi != 200 {
		t.Errorf("span = [%d, %d], want [50, 200] (the scanned files' rows)", lo, hi)
	}
	if lo, hi := filesTimeSpan(files, 10, 500); lo != 10 || hi != 500 {
		t.Errorf("span = [%d, %d], want the wider query window [10, 500]", lo, hi)
	}
	unknown := append(files, manifest.FileInfo{Key: "c"})
	if lo, hi := filesTimeSpan(unknown, 120, 130); lo != math.MinInt64 || hi != math.MaxInt64 {
		t.Errorf("a file with unknown bounds could hold rows from any time; span = [%d, %d]", lo, hi)
	}
	if lo, hi := filesTimeSpan(nil, 1, 2); lo != 1 || hi != 2 {
		t.Errorf("no files: span = [%d, %d], want the query window", lo, hi)
	}
}
