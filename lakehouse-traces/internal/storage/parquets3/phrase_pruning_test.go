package parquets3

import (
	"context"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A quoted phrase filter (`field:"v"`) is NOT an exact match. Upstream
// (lib/logstorage/filter_phrase.go, matchPhrase) matches a row when the field
// CONTAINS the phrase on token boundaries. The cold tier used to read the
// quoted form as equality and prune every bloom layer on the exact value
// (#319). VictoriaTraces fetches a trace's spans with the phrase form
// `trace_id:"X"`, so trace_id keeps exact-value pruning only because its
// stored values are proven single hex tokens (see phrase_exact.go).

type phraseRow struct {
	traceID string
	span    string
	service string
	id      string
}

var phraseFixtureRows = []phraseRow{
	{"abcdef0123456789abcdef0123456789", "GET /api/v1/users", "frontend", "r0"},
	{"abcdef", "GET /api", "frontend-v2", "r1"},
	{"abcdef01", "POST /api/v1/orders", "orders", "r2"},
	{"ffff0000ffff0000ffff0000ffff0000", "GET /health", "frontend", "r3"},
	{"ABCDEF", "DELETE /api/v1/users/7", "frontend-v2", "r4"},
	{"0123456789abcdef0123456789abcdef", "db query", "db-primary", "r5"},
	{"1234abcd", "GET /x", "api-gw-v2", "r6"},
}

var phraseQueries = []string{
	// VT's trace-by-ID span fetch form and the exact forms.
	`trace_id:"abcdef0123456789abcdef0123456789"`,
	`trace_id:="abcdef0123456789abcdef0123456789"`,
	`trace_id:abcdef0123456789abcdef0123456789`,
	`trace_id:"ffff0000ffff0000ffff0000ffff0000"`,
	`trace_id:"deadbeefdeadbeefdeadbeefdeadbeef"`,
	`trace_id:in("abcdef","ABCDEF","deadbeef")`,
	// A phrase that is a prefix of a longer id is not on a token boundary.
	`trace_id:"abcdef"`,
	`trace_id:abcdef`,
	`trace_id:"abcdef01"`,
	`trace_id:"abc"`,
	`trace_id:"ABCDEF"`,
	`trace_id:"abc-def"`,
	// Other columns: substring-on-boundary phrases must still match.
	`span.name:"GET /api"`,
	`span.name:"/api/v1"`,
	`span.name:"users"`,
	`span.name:="GET /api"`,
	`service.name:"frontend"`,
	`service.name:="frontend"`,
	`service.name:"api-gw"`,
	`service.name:"gw-v2"`,
	`service.name:="api-gw"`,
	`trace_id:"abcdef" service.name:"frontend"`,
	`trace_id:"abcdef" span.name:"GET /api"`,
	`trace_id:"abcdef0123456789abcdef0123456789" | fields span_id`,
}

func phraseOracle(t *testing.T, query string) []string {
	t.Helper()
	f := parseFilterFromQueryStr(query)
	if f == nil {
		t.Fatalf("no filter parsed from %q", query)
	}
	var want []string
	for _, r := range phraseFixtureRows {
		fields := []logstorage.Field{
			{Name: "trace_id", Value: r.traceID},
			{Name: "span.name", Value: r.span},
			{Name: "service.name", Value: r.service},
		}
		if f.MatchRow(fields) {
			want = append(want, r.id)
		}
	}
	sort.Strings(want)
	return want
}

func newPhraseStorage(t *testing.T, pmeta bool) (*Storage, func(), time.Time) {
	t.Helper()
	mock := newMockS3Server()
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeTraces)
	if pmeta {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	}
	now := time.Now().Truncate(time.Second)
	// One flush per row: each row sits in its own file with its own footer
	// bloom, row-group SBBF, pmeta file bloom and _trace_idx.
	for i, r := range phraseFixtureRows {
		ts := now.Add(time.Duration(i) * time.Millisecond).UnixNano()
		bw.AddTraceRows([]schema.TraceRow{{
			TimestampUnixNano: ts,
			StartTimeUnixNano: ts,
			DurationNs:        1000,
			TraceID:           r.traceID,
			SpanID:            r.id,
			SpanName:          r.span,
			ServiceName:       r.service,
		}})
		bw.triggerFlush()
	}
	return s, mock.close, now
}

func runPhraseQuery(t *testing.T, s *Storage, query string, now time.Time) []string {
	t.Helper()
	q := mustParseQueryWithTime(t, query, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	var mu sync.Mutex
	var rowsOut []map[string]string
	if err := s.RunQuery(context.Background(), nil, q, func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		rowsOut = append(rowsOut, blockRowFields([]*logstorage.DataBlock{db})...)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("RunQuery(%q): %v", query, err)
	}
	var got []string
	for _, row := range rowsOut {
		got = append(got, row["span_id"])
	}
	sort.Strings(got)
	return got
}

// TestPhrasePruning_ColdMatchesHotMatcher: every phrase shape returns what
// VictoriaTraces' own MatchRow returns, through every pruning layer (footer
// bloom, row-group SBBF, pmeta/file bloom, _trace_idx, token bloom, pushdown).
func TestPhrasePruning_ColdMatchesHotMatcher(t *testing.T) {
	for _, pmeta := range []bool{true, false} {
		name := "pmeta_off"
		if pmeta {
			name = "pmeta_on"
		}
		t.Run(name, func(t *testing.T) {
			s, closeFn, now := newPhraseStorage(t, pmeta)
			defer closeFn()
			for _, query := range phraseQueries {
				want := phraseOracle(t, query)
				got := runPhraseQuery(t, s, query, now)
				if !slices.Equal(got, want) {
					t.Errorf("%s: cold=%v hot(MatchRow)=%v", query, got, want)
				}
			}
		})
	}
}

// TestPhrasePruning_ExtractionRules pins the layer inputs: a phrase is an
// exact value only on trace_id and only when it is a plain ASCII token; every
// other phrase yields no exact value for any layer.
func TestPhrasePruning_ExtractionRules(t *testing.T) {
	type c struct {
		query, col string
		want       []string
	}
	for _, tc := range []c{
		// proven single-token domain: usable as an exact value
		{`trace_id:"abcdef0123456789abcdef0123456789"`, "trace_id", []string{"abcdef0123456789abcdef0123456789"}},
		{`trace_id:abcdef`, "trace_id", []string{"abcdef"}},
		{`trace_id:="abc-def"`, "trace_id", []string{"abc-def"}},
		// not a plain token: never exact
		{`trace_id:"abc-def"`, "trace_id", nil},
		{`trace_id:"abc def"`, "trace_id", nil},
		{`trace_id:""`, "trace_id", nil},
		// other columns: a phrase is never exact
		{`span.name:"GET /api"`, "span.name", nil},
		{`service.name:"frontend"`, "service.name", nil},
		{`service.name:frontend`, "service.name", nil},
		// exact and in() stay exact
		{`service.name:="frontend"`, "service.name", []string{"frontend"}},
		{`span.name:="GET /api"`, "span.name", []string{"GET /api"}},
		{`trace_id:in("a","b")`, "trace_id", []string{"a", "b"}},
	} {
		got := extractFilterValuesAST(tc.query, tc.col)
		if !slices.Equal(got, tc.want) {
			t.Errorf("extractFilterValuesAST(%q, %q) = %v, want %v", tc.query, tc.col, got, tc.want)
		}
	}
	s := testStorage()
	for _, query := range []string{`span.name:"GET /api"`, `service.name:"frontend"`, `trace_id:"abc-def"`} {
		if bc := s.buildBloomChecks(query); len(bc) != 0 {
			t.Errorf("buildBloomChecks(%q) = %v: bloom would prune on a non-exact phrase", query, bc)
		}
		if pdf := buildPushDownFilter(query, s.registry); pdf != nil {
			t.Errorf("buildPushDownFilter(%q) = %+v: pushdown would prune on a phrase", query, pdf.Checks)
		}
	}
	if bc := s.buildBloomChecks(`trace_id:"abcdef0123456789abcdef0123456789"`); len(bc) != 1 {
		t.Errorf("trace-by-ID phrase must keep the footer/row-group bloom check, got %v", bc)
	}
}

// TestPhrasePruning_TraceByIDStillPrunes: the VT form keeps pruning at every
// layer (file bloom, _trace_idx), so trace-by-ID does not turn into a scan.
func TestPhrasePruning_TraceByIDStillPrunes(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, true)
	defer closeFn()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) != len(phraseFixtureRows) {
		t.Fatalf("want %d files, got %d", len(phraseFixtureRows), len(files))
	}
	ctx := context.Background()
	for _, query := range []string{
		`trace_id:"deadbeefdeadbeefdeadbeefdeadbeef"`,
		`trace_id:="deadbeefdeadbeefdeadbeefdeadbeef"`,
	} {
		for _, fi := range files {
			if !s.checkFileBloom(ctx, fi, query) {
				t.Errorf("%s: pmeta file bloom did not prune %s", query, fi.Key)
			}
		}
	}
	// the file holding the trace is never pruned, in either form
	for _, query := range []string{
		`trace_id:"abcdef0123456789abcdef0123456789"`,
		`trace_id:="abcdef0123456789abcdef0123456789"`,
	} {
		kept := 0
		for _, fi := range files {
			if !s.checkFileBloom(ctx, fi, query) {
				kept++
			}
		}
		if kept < 1 {
			t.Errorf("%s: every file pruned, including the one holding the trace", query)
		}
	}
	// _trace_idx narrowing agrees for the phrase form
	tids := extractFilterValuesAST(`trace_id:"abcdef0123456789abcdef0123456789"`, "trace_id")
	if len(tids) != 1 {
		t.Fatalf("phrase trace id not extracted for _trace_idx: %v", tids)
	}
	got := s.filterFilesByTraceIdx(ctx, files, tids)
	if len(got) != 1 {
		t.Errorf("_trace_idx kept %d files for a one-file trace, want 1", len(got))
	}
}
