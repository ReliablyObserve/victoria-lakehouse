package parquets3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/membuffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// namesToHits reads a field_names answer as name -> hits.
func namesToHits(vs []logstorage.ValueWithHits) map[string]uint64 {
	m := make(map[string]uint64, len(vs))
	for _, v := range vs {
		m[v.Value] = v.Hits
	}
	return m
}

// fnRow is fvcRow of the stream svc-<level> that also carries a trace id.
func fnRow(at time.Duration, level, traceID string) schema.LogRow {
	r := fvcRow(at, level)
	r.TraceID = traceID
	return r
}

func fnWindow(t *testing.T, query string) *logstorage.Query {
	t.Helper()
	return mustParseQueryWithTime(t, query, fvcBase.UnixNano(), fvcBase.Add(time.Hour).UnixNano())
}

// field_names credits a column with every matching row of the stream blocks
// that list it, as upstream's field_names pipe does: trace_id is set on one row
// of the INFO stream, so the INFO stream lists it and its three rows are
// credited; the WARN stream never carries a trace id, lists no such column,
// and its two rows add nothing.
func TestFieldNames_CreditsTheRowsOfEachStreamBlock(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{
		fnRow(1*time.Minute, "INFO", "t1"), fnRow(2*time.Minute, "INFO", ""), fnRow(3*time.Minute, "INFO", ""),
		fnRow(4*time.Minute, "WARN", ""), fnRow(5*time.Minute, "WARN", ""),
	})
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 3 {
		t.Errorf("trace_id hits = %d, want 3 (the rows of the one stream that lists it): %v", m["trace_id"], m)
	}
	for _, name := range []string{"_time", "_msg", "_stream", "level", "service.name"} {
		if m[name] != 5 {
			t.Errorf("%s hits = %d, want 5 (every row): %v", name, m[name], m)
		}
	}
	for _, internal := range []string{"timestamp_unix_nano", "body", "severity_text", "account_id", "project_id", "log.attributes", "resource.attributes"} {
		if _, ok := m[internal]; ok {
			t.Errorf("%q listed: a Parquet column name is not a field of the rows: %v", internal, m)
		}
	}
	// Upstream's order: hits descending, then name.
	for i := 1; i < len(got); i++ {
		a, b := got[i-1], got[i]
		if a.Hits < b.Hits || (a.Hits == b.Hits && a.Value > b.Value) {
			t.Fatalf("answer not in upstream order at %d: %v then %v", i, a, b)
		}
	}
}

// The row filter picks the rows AFTER a block's columns are known: trace_id
// stays listed, credited to the matching rows of the stream that has it (2),
// although neither matching row carries a trace id.
func TestFieldNames_FilterSelectsRowsAfterTheColumnsAreKnown(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{
		fnRow(1*time.Minute, "INFO", "t1"), fnRow(2*time.Minute, "INFO", ""), fnRow(3*time.Minute, "INFO", ""),
		fnRow(4*time.Minute, "WARN", ""),
	})
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "level:=INFO AND -trace_id:t1"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["trace_id"] != 2 || m["_msg"] != 2 {
		t.Errorf("trace_id/_msg hits = %d/%d, want 2/2: %v", m["trace_id"], m["_msg"], m)
	}
	// The time range works the same way: only the rows inside it are credited.
	win := mustParseQueryWithTime(t, "*", fvcBase.Add(90*time.Second).UnixNano(), fvcBase.Add(3*time.Minute+time.Second).UnixNano())
	got, err = s.GetFieldNames(context.Background(), nil, win)
	if err != nil {
		t.Fatal(err)
	}
	if m = namesToHits(got); m["_msg"] != 2 || m["trace_id"] != 2 {
		t.Errorf("windowed _msg/trace_id hits = %d/%d, want 2/2: %v", m["_msg"], m["trace_id"], m)
	}
}

// A window holding no flushed object is answered from the unflushed rows alone
// (#378): they list their fields and credit their rows, as on hot storage.
func TestFieldNames_AnswersAWindowHeldOnlyInTheLocalBuffer(t *testing.T) {
	bs, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i, level := range []string{"INFO", "INFO", "WARN"} {
		lr.MustAdd(logstorage.TenantID{}, now+int64(i), []logstorage.Field{
			{Name: "service.name", Value: "checkout"}, {Name: "level", Value: level}, {Name: "_msg", Value: "event"},
		}, 1)
	}
	bs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	bs.DebugFlush()

	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())
	s.SetLocalBuffer(bs)
	q := mustParseQueryWithTime(t, "*", now-int64(time.Hour), now+int64(time.Hour))
	got, err := s.GetFieldNames(context.Background(), nil, q)
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	for _, name := range []string{"_time", "_msg", "_stream", "_stream_id", "level", "service.name"} {
		if m[name] != 3 {
			t.Errorf("%s hits = %d, want 3: %v", name, m[name], m)
		}
	}
	// The hits equal what a count over the same window says.
	filtered := mustParseQueryWithTime(t, "level:=WARN", now-int64(time.Hour), now+int64(time.Hour))
	got, err = s.GetFieldNames(context.Background(), nil, filtered)
	if err != nil {
		t.Fatal(err)
	}
	if m = namesToHits(got); m["_msg"] != 1 {
		t.Errorf("filtered _msg hits = %d, want 1: %v", m["_msg"], m)
	}
}

// Rows an insert peer still holds are merged as a query merges them: the object
// flushed from the segment the peer serves is not read twice, and the peer's
// own fields join the list.
func TestFieldNames_MergesPeerBufferAndObjects(t *testing.T) {
	const nonce = "65000000aaaabbbb"
	s := fvcStorageNonce(t, nonce, []schema.LogRow{fvcRow(5*time.Minute, "INFO"), fvcRow(6*time.Minute, "INFO")})
	s.bufferBridge = fvcPeer(t, []schema.LogRow{
		fvcRow(5*time.Minute, "INFO"), fvcRow(6*time.Minute, "INFO"), // in the segment's object too
		fnRow(20*time.Minute, "WARN", "t9"), fvcRow(21*time.Minute, "WARN"),
	}, nonce)
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["_msg"] != 4 {
		t.Errorf("_msg hits = %d, want 4 (the peer's four rows; its segment's object is not read again): %v", m["_msg"], m)
	}
	if m["trace_id"] != 2 {
		t.Errorf("trace_id hits = %d, want 2 (the peer's WARN stream lists it): %v", m["trace_id"], m)
	}
}

// stream_field_names credits a tag with the rows of every matching stream that
// carries it (#461), in upstream's order, over every layer.
func TestGetStreamFieldNames_CreditsTheRowsOfEachStream(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{
		fvcRow(1*time.Minute, "INFO"), fvcRow(2*time.Minute, "INFO"), fvcRow(3*time.Minute, "INFO"), fvcRow(4*time.Minute, "WARN"),
	})
	s.bufferBridge = fvcPeer(t, []schema.LogRow{fvcRow(20*time.Minute, "WARN")})
	got, err := s.GetStreamFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	want := []logstorage.ValueWithHits{{Value: "service.name", Hits: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("stream_field_names = %v, want %v", got, want)
	}
	// A filter narrows the streams, and a window holding none answers nothing.
	got, _ = s.GetStreamFieldNames(context.Background(), nil, fnWindow(t, "level:=INFO"))
	if want = []logstorage.ValueWithHits{{Value: "service.name", Hits: 3}}; !reflect.DeepEqual(got, want) {
		t.Errorf("filtered stream_field_names = %v, want %v", got, want)
	}
	empty := mustParseQueryWithTime(t, "*", fvcBase.Add(-48*time.Hour).UnixNano(), fvcBase.Add(-47*time.Hour).UnixNano())
	if got, _ = s.GetStreamFieldNames(context.Background(), nil, empty); len(got) != 0 {
		t.Errorf("empty window answered %v", got)
	}
}

// The answer does not depend on how the rows of a stream are spread over
// objects: one stream held in two objects (a flush each) credits a field one
// of its rows carries with every row of the stream, as one block does.
func TestFieldNames_DoNotDependOnTheObjectLayout(t *testing.T) {
	first := []schema.LogRow{fnRow(1*time.Minute, "INFO", "t1"), fnRow(2*time.Minute, "INFO", "")}
	second := []schema.LogRow{fnRow(3*time.Minute, "INFO", ""), fnRow(4*time.Minute, "INFO", "")}
	one := fvcStorage(t, append(append([]schema.LogRow{}, first...), second...))
	two := fvcStorage(t, first, second)
	a, err := one.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := two.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("one object: %v\ntwo objects: %v", a, b)
	}
	if m := namesToHits(b); m["trace_id"] != 4 {
		t.Errorf("trace_id hits = %d, want 4", m["trace_id"])
	}
}

// The rows an insert peer holds are a block of their own: a field only they
// carry credits the peer's rows of the stream, not the rows of the same stream
// in objects (an unmerged in-memory part on hot storage).
func TestFieldNames_BufferRowsAreTheirOwnBlock(t *testing.T) {
	s := fvcStorage(t, []schema.LogRow{fvcRow(1*time.Minute, "INFO"), fvcRow(2*time.Minute, "INFO")})
	s.bufferBridge = fvcPeer(t, []schema.LogRow{fnRow(20*time.Minute, "INFO", "t7"), fvcRow(21*time.Minute, "INFO")})
	got, err := s.GetFieldNames(context.Background(), nil, fnWindow(t, "*"))
	if err != nil {
		t.Fatal(err)
	}
	m := namesToHits(got)
	if m["_msg"] != 4 || m["trace_id"] != 2 {
		t.Errorf("_msg/trace_id hits = %d/%d, want 4/2: %v", m["_msg"], m["trace_id"], m)
	}
	for name := range m {
		if len(name) > 0 && name[0] == 0 {
			t.Errorf("internal marker column %q leaked into the answer", name)
		}
	}
}
