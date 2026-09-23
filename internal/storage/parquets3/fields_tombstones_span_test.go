package parquets3

import (
	"context"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The field-enumeration scans (field_values, streams, stream_ids) read every
// row of every file overlapping the query window — including rows of that file
// that lie outside the window. The tombstones applied to those rows used to be
// looked up for the exact window only, so a delete covering a scanned file's
// rows outside the window was never consulted and its value was enumerated.
// These mirror TestFieldNames_TombstoneInsideACountedFileButOutsideTheWindow.

// spanFixture is one flushed file holding an early row (old-svc, stream A) and
// a late row (new-svc, stream B) ten seconds apart, a narrow query window around
// the late row, and a tombstone over the early row only.
type spanFixture struct {
	storage *Storage
	query   *logstorage.Query
}

func newSpanFixture(t *testing.T) *spanFixture {
	t.Helper()
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)

	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	early := now.Add(-10 * time.Second)
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: early.UnixNano(), Body: "early", ServiceName: "old-svc", Stream: `{app="old"}`, StreamID: "stream-old"},
		{TimestampUnixNano: now.UnixNano(), Body: "late", ServiceName: "new-svc", Stream: `{app="new"}`, StreamID: "stream-new"},
	})
	bw.triggerFlush()

	files := s.manifest.GetFilesForRange(early.UnixNano(), now.UnixNano())
	if len(files) != 1 || files[0].MinTimeNs == files[0].MaxTimeNs {
		t.Fatalf("fixture: want one file spanning both rows, got %+v", files)
	}

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "edge", Query: "*", StartNs: early.UnixNano(), EndNs: early.Add(time.Second).UnixNano(), Mode: "hide"})
	s.SetTombstoneStore(store)

	return &spanFixture{
		storage: s,
		query:   mustParseQueryWithTime(t, "*", now.Add(-time.Second).UnixNano(), now.Add(time.Second).UnixNano()),
	}
}

func assertNotEnumerated(t *testing.T, endpoint string, got []logstorage.ValueWithHits, deleted, kept string) {
	t.Helper()
	var sawKept bool
	for _, v := range got {
		if v.Value == deleted {
			t.Fatalf("%s enumerates %q, whose only row is tombstoned: %v", endpoint, deleted, valueStrings(got))
		}
		if v.Value == kept {
			sawKept = true
		}
	}
	if !sawKept {
		t.Fatalf("%s lost the untouched value %q: %v", endpoint, kept, valueStrings(got))
	}
}

func TestFieldValues_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newSpanFixture(t)
	got, err := f.storage.GetFieldValues(context.Background(), nil, f.query, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	assertNotEnumerated(t, "field_values", got, "old-svc", "new-svc")
}

func TestGetStreams_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newSpanFixture(t)
	got, err := f.storage.GetStreams(context.Background(), nil, f.query, 100)
	if err != nil {
		t.Fatalf("GetStreams: %v", err)
	}
	assertNotEnumerated(t, "streams", got, `{app="old"}`, `{app="new"}`)
}

func TestGetStreamIDs_TombstoneInsideAScannedFileButOutsideTheWindow(t *testing.T) {
	f := newSpanFixture(t)
	got, err := f.storage.GetStreamIDs(context.Background(), nil, f.query, 100)
	if err != nil {
		t.Fatalf("GetStreamIDs: %v", err)
	}
	assertNotEnumerated(t, "stream_ids", got, "stream-old", "stream-new")
}

// TestFieldValues_LabelIndexGatedByATombstoneAnywhere: the in-memory label
// index is not time-scoped — it lists every value any file ever carried — so a
// tombstone in ANY hour can cover a value it would serve. With no pmeta catalog
// it is the first fast path field_values tries.
func TestFieldValues_LabelIndexGatedByATombstoneAnywhere(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)

	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	yesterday := now.Add(-24 * time.Hour)
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: yesterday.UnixNano(), Body: "old", ServiceName: "secret-svc"},
	})
	bw.triggerFlush()
	bw.AddLogRows([]schema.LogRow{
		{TimestampUnixNano: now.UnixNano(), Body: "new", ServiceName: "web"},
	})
	bw.triggerFlush()
	s.labelIndex.Add("service.name", []string{"secret-svc", "web"})

	store := delete.NewTombstoneStore()
	store.Add(delete.Tombstone{Tenants: []delete.TenantRef{{}}, ID: "yesterday", Query: `service.name:="secret-svc"`,
		StartNs: yesterday.Add(-time.Minute).UnixNano(), EndNs: yesterday.Add(time.Minute).UnixNano(), Mode: "hide"})
	s.SetTombstoneStore(store)

	// A window that contains both hours: the label index would answer with
	// both values, and the deleted one must not be among them.
	q := mustParseQueryWithTime(t, "*", yesterday.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	got, err := s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	assertNotEnumerated(t, "field_values", got, "secret-svc", "web")

	// A window over today only: nothing deleted is in it, but the time-blind
	// label index would still have answered with yesterday's deleted value.
	q = mustParseQueryWithTime(t, "*", now.Add(-time.Minute).UnixNano(), now.Add(time.Minute).UnixNano())
	got, err = s.GetFieldValues(context.Background(), nil, q, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues: %v", err)
	}
	assertNotEnumerated(t, "field_values (today only)", got, "secret-svc", "web")
}
