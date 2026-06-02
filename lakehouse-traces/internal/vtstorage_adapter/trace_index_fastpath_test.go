package vtstorageadapter

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// fastpathStore implements both storage.Storage (we satisfy only what
// Adapter.RunQuery actually touches via assertion) and traceIndexLookup.
type fastpathStore struct {
	noopStore
	got      string
	startNs  int64
	endNs    int64
	found    bool
	err      error
	runQuery func(*logstorage.Query) // records fallback path invocation
}

func (s *fastpathStore) LookupTraceIndex(_ context.Context, traceID string) (int64, int64, bool, error) {
	s.got = traceID
	return s.startNs, s.endNs, s.found, s.err
}

func (s *fastpathStore) RunQuery(_ context.Context, _ []logstorage.TenantID, q *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	if s.runQuery != nil {
		s.runQuery(q)
	}
	return nil
}

// noopStore satisfies storage.Storage with default returns so test
// fixtures only override the methods they care about.
type noopStore struct{}

func (noopStore) RunQuery(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error {
	return nil
}
func (noopStore) GetFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) GetFieldValues(context.Context, []logstorage.TenantID, *logstorage.Query, string, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) GetStreamFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) GetStreamFieldValues(context.Context, []logstorage.TenantID, *logstorage.Query, string, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) GetStreams(context.Context, []logstorage.TenantID, *logstorage.Query, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) GetStreamIDs(context.Context, []logstorage.TenantID, *logstorage.Query, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (noopStore) HasDataForRange(int64, int64) bool { return true }
func (noopStore) Close() error                      { return nil }

var _ storage.Storage = noopStore{}

// makeTraceIndexQuery builds the literal query VT issues against the
// trace-by-ID path so the test exercises the real detection logic, not
// a hand-crafted approximation.
func makeTraceIndexQuery(t *testing.T, traceID string, bucket uint64) *logstorage.Query {
	t.Helper()
	qStr := `{` + otelpb.TraceIDIndexStreamName + `="` + strconv.FormatUint(bucket, 10) + `"} AND ` +
		otelpb.TraceIDIndexFieldName + `:="` + traceID + `" | stats min(_time) _time, ` +
		`min(` + otelpb.TraceIDIndexStartTimeFieldName + `) ` + otelpb.TraceIDIndexStartTimeFieldName + `, ` +
		`max(` + otelpb.TraceIDIndexEndTimeFieldName + `) ` + otelpb.TraceIDIndexEndTimeFieldName
	q, err := logstorage.ParseQueryAtTimestamp(qStr, 1)
	if err != nil {
		t.Fatalf("ParseQueryAtTimestamp(%q): %v", qStr, err)
	}
	return q
}

func TestAdapter_TraceIndexFastpath_Hit(t *testing.T) {
	store := &fastpathStore{startNs: 1_000_000_000, endNs: 2_500_000_000, found: true}
	a := &Adapter{store: store}

	var emitted *logstorage.DataBlock
	wb := func(_ uint, db *logstorage.DataBlock) { emitted = db }

	q := makeTraceIndexQuery(t, "abc123", 42)
	if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	if store.got != "abc123" {
		t.Errorf("LookupTraceIndex called with %q, want %q", store.got, "abc123")
	}
	if emitted == nil {
		t.Fatal("expected synthetic DataBlock, got nil")
	}
	cols := emitted.GetColumns(false)
	if len(cols) != 3 {
		t.Fatalf("emitted block has %d columns, want 3", len(cols))
	}
	byName := map[string]string{}
	for _, c := range cols {
		if len(c.Values) != 1 {
			t.Errorf("column %q: %d values, want 1", c.Name, len(c.Values))
		}
		byName[c.Name] = c.Values[0]
	}
	if byName[otelpb.TraceIDIndexStartTimeFieldName] != "1000000000" {
		t.Errorf("start_time = %q, want 1000000000", byName[otelpb.TraceIDIndexStartTimeFieldName])
	}
	if byName[otelpb.TraceIDIndexEndTimeFieldName] != "2500000000" {
		t.Errorf("end_time = %q, want 2500000000", byName[otelpb.TraceIDIndexEndTimeFieldName])
	}
	if _, ok := byName["_time"]; !ok {
		t.Error("emitted block missing _time column expected by VT tempo handler")
	}
}

func TestAdapter_TraceIndexFastpath_MissFallsThrough(t *testing.T) {
	var fallbackQuery *logstorage.Query
	store := &fastpathStore{
		found:    false,
		runQuery: func(q *logstorage.Query) { fallbackQuery = q },
	}
	a := &Adapter{store: store}

	wb := func(uint, *logstorage.DataBlock) {}

	q := makeTraceIndexQuery(t, "cold-trace", 9)
	if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if fallbackQuery == nil {
		t.Fatal("expected fallback to store.RunQuery on lookup miss")
	}
	// The fallback must be the rewritten span-scan query
	// (rewriteTraceIndexQuery output), not the original VT index query.
	got := fallbackQuery.String()
	if got == "" {
		t.Errorf("fallback query is empty")
	}
	// rewriteTraceIndexQuery's output filters on trace_id, not trace_id_idx.
	if !contains(got, "trace_id") {
		t.Errorf("fallback query %q does not filter on trace_id (expected span-scan rewrite)", got)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && indexOf(s, sub) >= 0
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestAdapter_TraceIndexFastpath_ErrorIsNotFatal(t *testing.T) {
	// An error from LookupTraceIndex must not bubble up — it should
	// behave like a miss so the span-scan fallback still runs.
	store := &fastpathStore{
		err: errors.New("footer fetch failed"),
		runQuery: func(*logstorage.Query) {
			// fallback ran — that's what we want.
		},
	}
	a := &Adapter{store: store}

	wb := func(uint, *logstorage.DataBlock) {}

	q := makeTraceIndexQuery(t, "x", 1)
	if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
		t.Errorf("RunQuery should swallow lookup errors, got %v", err)
	}
}

func TestAdapter_TraceIndexFastpath_NonIndexQueryUntouched(t *testing.T) {
	// A regular logs query must NOT touch LookupTraceIndex.
	store := &fastpathStore{
		startNs: 0, endNs: 0, found: true, // would lie if asked
		runQuery: func(*logstorage.Query) {},
	}
	a := &Adapter{store: store}

	wb := func(uint, *logstorage.DataBlock) {}

	q, err := logstorage.ParseQueryAtTimestamp(`service.name:="api-gateway"`, 1)
	if err != nil {
		t.Fatalf("ParseQueryAtTimestamp: %v", err)
	}
	if err := a.RunQuery(&logstorage.QueryContext{Context: context.Background(), Query: q}, wb); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	if store.got != "" {
		t.Errorf("LookupTraceIndex was called with %q for a non-index query", store.got)
	}
}

func TestTraceIndexLookupTraceID_DetectsVTShape(t *testing.T) {
	q := makeTraceIndexQuery(t, "deadbeef", 7)
	got, ok := traceIndexLookupTraceID(q)
	if !ok {
		t.Fatal("expected detection ok=true on canonical VT trace-by-ID query")
	}
	if got != "deadbeef" {
		t.Errorf("traceID = %q, want %q", got, "deadbeef")
	}
}

func TestTraceIndexLookupTraceID_IgnoresPlainSpanQuery(t *testing.T) {
	q, _ := logstorage.ParseQueryAtTimestamp(`trace_id:="abc"`, 1)
	if _, ok := traceIndexLookupTraceID(q); ok {
		t.Error("plain trace_id query must not be classified as VT index lookup")
	}
}
