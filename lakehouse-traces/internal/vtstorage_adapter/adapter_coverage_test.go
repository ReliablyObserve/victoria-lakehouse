package vtstorageadapter

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// TestInit_InstallsExternalStorage verifies the full wiring: Init must
// register the adapter as VT's external storage so that VT's dispatch
// functions (vtstorage.RunQuery / vtstorage.GetTenantIDs) route to the
// lakehouse store instead of VT's local storage.
func TestInit_InstallsExternalStorage(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: true}
	Init(mock)
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })

	q, err := logstorage.ParseQueryAtTimestamp("*", 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := newTestQctx(context.Background(), nil, q)
	if err := vtstorage.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("vtstorage.RunQuery error: %v", err)
	}
	if !mock.runQueryCalled {
		t.Fatal("Init did not install the adapter: vtstorage.RunQuery never reached the lakehouse store")
	}

	// Without a tenant lister, the legacy single-zero-tenant answer must hold.
	tenants, err := vtstorage.GetTenantIDs(context.Background(), 100, 200)
	if err != nil {
		t.Fatalf("vtstorage.GetTenantIDs error: %v", err)
	}
	if len(tenants) != 1 || tenants[0].AccountID != 0 || tenants[0].ProjectID != 0 {
		t.Fatalf("expected legacy [{0,0}] tenant answer, got %v", tenants)
	}
}

// TestInit_WithTenantLister verifies the option plumbing end-to-end:
// the lister wired at Init time must drive vtstorage.GetTenantIDs.
func TestInit_WithTenantLister(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: true}
	var gotStart, gotEnd int64
	lister := func(startNs, endNs int64) []logstorage.TenantID {
		gotStart, gotEnd = startNs, endNs
		return []logstorage.TenantID{{AccountID: 7, ProjectID: 9}}
	}
	Init(mock, WithTenantLister(lister))
	t.Cleanup(func() { vtstorage.SetExternalStorage(nil) })

	tenants, err := vtstorage.GetTenantIDs(context.Background(), 1111, 2222)
	if err != nil {
		t.Fatalf("vtstorage.GetTenantIDs error: %v", err)
	}
	if len(tenants) != 1 || tenants[0].AccountID != 7 || tenants[0].ProjectID != 9 {
		t.Fatalf("expected lister tenants [{7,9}], got %v", tenants)
	}
	if gotStart != 1111 || gotEnd != 2222 {
		t.Fatalf("lister received wrong range: start=%d end=%d", gotStart, gotEnd)
	}
}

// TestGetTenantIDs_ListerEmptyFallsBackToZeroTenant: a wired lister that
// returns no tenants must NOT suppress the legacy answer — VT background
// tasks (servicegraph) still need exactly one tenant to iterate.
func TestGetTenantIDs_ListerEmptyFallsBackToZeroTenant(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: true}
	a := &Adapter{store: mock, tenantLister: func(_, _ int64) []logstorage.TenantID { return nil }}

	tenants, err := a.GetTenantIDs(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tenants) != 1 || tenants[0].AccountID != 0 || tenants[0].ProjectID != 0 {
		t.Fatalf("expected fallback [{0,0}], got %v", tenants)
	}
}

// TestGetTenantIDs_NoDataShortCircuitsLister: when the store owns no data
// in the range, the lister must never run — nil means "nothing here".
func TestGetTenantIDs_NoDataShortCircuitsLister(t *testing.T) {
	mock := &mockStorage{returnHasDataForRange: false}
	listerCalled := false
	a := &Adapter{store: mock, tenantLister: func(_, _ int64) []logstorage.TenantID {
		listerCalled = true
		return []logstorage.TenantID{{AccountID: 1}}
	}}

	tenants, err := a.GetTenantIDs(context.Background(), 10, 20)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenants != nil {
		t.Fatalf("expected nil tenants when no data, got %v", tenants)
	}
	if listerCalled {
		t.Fatal("lister must not be consulted when HasDataForRange is false")
	}
}

func TestFilterValuesBySubstring(t *testing.T) {
	in := []logstorage.ValueWithHits{
		{Value: "http.status_code", Hits: 10},
		{Value: "service.name", Hits: 5},
		{Value: "status", Hits: 2},
	}

	t.Run("empty filter is identity (same slice, not a copy)", func(t *testing.T) {
		got := filterValuesBySubstring(in, "")
		if len(got) != 3 {
			t.Fatalf("expected all 3 values, got %v", got)
		}
		if &got[0] != &in[0] {
			t.Error("empty filter should return the input slice unchanged")
		}
	})

	t.Run("substring narrows", func(t *testing.T) {
		got := filterValuesBySubstring(in, "status")
		if len(got) != 2 {
			t.Fatalf("expected 2 matches for %q, got %v", "status", got)
		}
		if got[0].Value != "http.status_code" || got[1].Value != "status" {
			t.Errorf("wrong matches: %v", got)
		}
	})

	t.Run("no match yields empty non-nil slice", func(t *testing.T) {
		got := filterValuesBySubstring(in, "zzz")
		if len(got) != 0 {
			t.Fatalf("expected no matches, got %v", got)
		}
		if got == nil {
			t.Error("expected empty (non-nil) slice for no matches")
		}
	})

	t.Run("filter is substring not prefix", func(t *testing.T) {
		got := filterValuesBySubstring(in, "name")
		if len(got) != 1 || got[0].Value != "service.name" {
			t.Fatalf("expected service.name only, got %v", got)
		}
	})
}

// TestGetFieldFamilies_ApplySubstringFilter verifies the adapter applies the
// VT v0.9.2 substring filter itself (the shared storage interface does not
// carry it).
func TestGetFieldFamilies_ApplySubstringFilter(t *testing.T) {
	values := []logstorage.ValueWithHits{
		{Value: "alpha", Hits: 1},
		{Value: "beta", Hits: 2},
		{Value: "alphabet", Hits: 3},
	}

	t.Run("GetFieldNames", func(t *testing.T) {
		a := newTestAdapter(&mockStorage{returnValues: values})
		got, err := a.GetFieldNames(newTestQctx(context.Background(), nil, nil), "alpha")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 || got[0].Value != "alpha" || got[1].Value != "alphabet" {
			t.Fatalf("filter not applied: %v", got)
		}
	})

	t.Run("GetFieldValues", func(t *testing.T) {
		a := newTestAdapter(&mockStorage{returnValues: values})
		got, err := a.GetFieldValues(newTestQctx(context.Background(), nil, nil), "f", "beta", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Value != "beta" {
			t.Fatalf("filter not applied: %v", got)
		}
	})

	t.Run("GetStreamFieldNames", func(t *testing.T) {
		a := newTestAdapter(&mockStorage{returnValues: values})
		got, err := a.GetStreamFieldNames(newTestQctx(context.Background(), nil, nil), "bet")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("filter not applied: %v", got)
		}
	})

	t.Run("GetStreamFieldValues", func(t *testing.T) {
		a := newTestAdapter(&mockStorage{returnValues: values})
		got, err := a.GetStreamFieldValues(newTestQctx(context.Background(), nil, nil), "f", "alphabet", 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 || got[0].Value != "alphabet" {
			t.Fatalf("filter not applied: %v", got)
		}
	})
}

// TestGetFieldFamilies_ErrorPropagation: storage errors must surface as-is
// and the substring filter must never run on a nil result.
func TestGetFieldFamilies_ErrorPropagation(t *testing.T) {
	wantErr := errors.New("s3 listing failed")

	cases := []struct {
		name string
		call func(a *Adapter) ([]logstorage.ValueWithHits, error)
	}{
		{"GetFieldNames", func(a *Adapter) ([]logstorage.ValueWithHits, error) {
			return a.GetFieldNames(newTestQctx(context.Background(), nil, nil), "x")
		}},
		{"GetFieldValues", func(a *Adapter) ([]logstorage.ValueWithHits, error) {
			return a.GetFieldValues(newTestQctx(context.Background(), nil, nil), "f", "x", 1)
		}},
		{"GetStreamFieldNames", func(a *Adapter) ([]logstorage.ValueWithHits, error) {
			return a.GetStreamFieldNames(newTestQctx(context.Background(), nil, nil), "x")
		}},
		{"GetStreamFieldValues", func(a *Adapter) ([]logstorage.ValueWithHits, error) {
			return a.GetStreamFieldValues(newTestQctx(context.Background(), nil, nil), "f", "x", 1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAdapter(&mockStorage{returnErr: wantErr})
			got, err := tc.call(a)
			if !errors.Is(err, wantErr) {
				t.Fatalf("expected %v, got %v", wantErr, err)
			}
			if got != nil {
				t.Fatalf("expected nil result on error, got %v", got)
			}
		})
	}
}

// allFieldsCapturingStorage records whether the all-fields projection
// hint was present on the context handed to RunQuery.
type allFieldsCapturingStorage struct {
	mockStorage
	sawAllFieldsHint bool
}

func (s *allFieldsCapturingStorage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	s.sawAllFieldsHint = storage.IsAllFields(ctx)
	return s.mockStorage.RunQuery(ctx, tenantIDs, q, writeBlock)
}

// TestRunQuery_FieldEnumeratingPipeSetsAllFieldsHint: queries with
// field-enumerating pipes (field_names / facets) must reach the storage
// with the all-fields context hint so projection narrowing is bypassed.
// Without the hint, /api/v2/search/tags reports a truncated tag schema.
func TestRunQuery_FieldEnumeratingPipeSetsAllFieldsHint(t *testing.T) {
	for _, queryStr := range []string{
		`* | field_names`,
		`* | facets 10`,
	} {
		t.Run(queryStr, func(t *testing.T) {
			mock := &allFieldsCapturingStorage{}
			a := &Adapter{store: mock}

			q, err := logstorage.ParseQueryAtTimestamp(queryStr, 1000000000)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if !logstorage.QueryNeedsAllFields(q) {
				t.Fatalf("precondition: %q should need all fields", queryStr)
			}
			qctx := newTestQctx(context.Background(), nil, q)
			if err := a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
				t.Fatalf("RunQuery error: %v", err)
			}
			if !mock.runQueryCalled {
				t.Fatal("storage RunQuery not reached")
			}
			if !mock.sawAllFieldsHint {
				t.Fatal("storage did not receive the all-fields hint context for a field-enumerating pipe")
			}
		})
	}
}

// TestRunQuery_NonEnumeratingQueryHasNoAllFieldsHint is the negative
// control: ordinary queries must NOT carry the hint, or every query
// would lose projection narrowing.
func TestRunQuery_NonEnumeratingQueryHasNoAllFieldsHint(t *testing.T) {
	mock := &allFieldsCapturingStorage{}
	a := &Adapter{store: mock}

	q, err := logstorage.ParseQueryAtTimestamp(`foo:="bar"`, 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := newTestQctx(context.Background(), nil, q)
	if err := a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}
	if mock.sawAllFieldsHint {
		t.Fatal("plain query must not carry the all-fields hint")
	}
}

// TestRunQuery_StripPathWithoutPipes covers the direct (non-subquery)
// dispatch after stripping the trace_id_idx_stream selector: a search
// query with the selector but no pipes goes straight to store.RunQuery
// with the rewritten query.
func TestRunQuery_StripPathWithoutPipes(t *testing.T) {
	mock := &mockStorage{}
	a := newTestAdapter(mock)

	qStr := `{trace_id_idx_stream="abc"} AND service.name:="api-gateway"`
	q, err := logstorage.ParseQueryAtTimestamp(qStr, 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	qctx := newTestQctx(context.Background(), nil, q)
	if err := a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {}); err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}
	if !mock.runQueryCalled {
		t.Fatal("expected store.RunQuery to be called")
	}
	if mock.lastQuery == nil {
		t.Fatal("expected a non-nil rewritten query")
	}
	gotStr := mock.lastQuery.String()
	if strings.Contains(gotStr, "trace_id_idx_stream") {
		t.Fatalf("stream selector not stripped: %s", gotStr)
	}
	if !strings.Contains(gotStr, "api-gateway") {
		t.Fatalf("original filter lost in rewrite: %s", gotStr)
	}
	if logstorage.QueryHasPipes(mock.lastQuery) {
		t.Fatalf("pipe-less query gained pipes: %s", gotStr)
	}
}

func TestStripTraceIndexStream(t *testing.T) {
	t.Run("selector-only query becomes match-all", func(t *testing.T) {
		q, err := logstorage.ParseQueryAtTimestamp(`{trace_id_idx_stream="42"}`, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		rewritten, ok := stripTraceIndexStream(q)
		if !ok {
			t.Fatal("expected strip to trigger")
		}
		if strings.Contains(rewritten.String(), "trace_id_idx_stream") {
			t.Fatalf("selector survived strip: %s", rewritten.String())
		}
	})

	t.Run("no selector mention is a no-op", func(t *testing.T) {
		q, err := logstorage.ParseQueryAtTimestamp(`foo:="bar"`, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if _, ok := stripTraceIndexStream(q); ok {
			t.Fatal("strip must not trigger without the selector")
		}
	})

	t.Run("mention outside a stream selector is a no-op", func(t *testing.T) {
		// The marker substring appears as a field filter, not as a
		// `{trace_id_idx_stream=...}` stream selector — the cleaner finds
		// nothing to remove and strip must report false.
		q, err := logstorage.ParseQueryAtTimestamp(`trace_id_idx_stream:="42"`, 1000000000)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		if _, ok := stripTraceIndexStream(q); ok {
			t.Fatal("strip must not trigger when the selector form is absent")
		}
	})
}

func TestStripIndexStreamSelector_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "unclosed selector left intact",
			in:   `{trace_id_idx_stream="42" AND foo:bar`,
			want: `{trace_id_idx_stream="42" AND foo:bar`,
		},
		{
			name: "selector-only collapses to match-all",
			in:   `{trace_id_idx_stream="42"}`,
			want: `*`,
		},
		{
			name: "leading AND cleaned after strip",
			in:   `{trace_id_idx_stream="42"} AND foo:bar`,
			want: `foo:bar`,
		},
		{
			name: "multiple selectors all removed",
			in:   `{trace_id_idx_stream="1"} foo:bar {trace_id_idx_stream="2"}`,
			want: `foo:bar`,
		},
		{
			name: "empty input becomes match-all",
			in:   ``,
			want: `*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripIndexStreamSelector(tt.in); got != tt.want {
				t.Errorf("stripIndexStreamSelector(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestRewriteTraceIndexQuery_EmptyTraceID(t *testing.T) {
	// trace_id_idx:="" carries the marker but yields no trace ID — the
	// rewrite must decline rather than emit trace_id:="" (which would
	// scan for empty-string trace IDs).
	q, err := logstorage.ParseQueryAtTimestamp(`{trace_id_idx_stream="42"} AND trace_id_idx:=""`, 1000000000)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if _, ok := rewriteTraceIndexQuery(q); ok {
		t.Fatal("rewrite must decline when the trace ID is empty")
	}
}

func TestExtractTraceIDFromIndexQuery_AdversarialInputs(t *testing.T) {
	tests := []struct {
		name     string
		queryStr string
		want     string
	}{
		{
			name:     "marker at end of string (nothing after :=)",
			queryStr: `trace_id_idx:=`,
			want:     "",
		},
		{
			name:     "unterminated quote",
			queryStr: `trace_id_idx:="abc123`,
			want:     "",
		},
		{
			name:     "empty quoted value",
			queryStr: `trace_id_idx:=""`,
			want:     "",
		},
		{
			name:     "unquoted value terminated by pipe",
			queryStr: `trace_id_idx:=deadbeef|stats count()`,
			want:     "deadbeef",
		},
		{
			name:     "unquoted value terminated by close paren",
			queryStr: `(trace_id_idx:=deadbeef)`,
			want:     "deadbeef",
		},
		{
			name:     "unquoted value runs to end of string",
			queryStr: `trace_id_idx:=deadbeef`,
			want:     "deadbeef",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractTraceIDFromIndexQuery(tt.queryStr); got != tt.want {
				t.Errorf("extractTraceIDFromIndexQuery(%q) = %q, want %q", tt.queryStr, got, tt.want)
			}
		})
	}
}
