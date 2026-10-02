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
// CONTAINS the phrase on token boundaries, so `trace_id:"abc-def"` matches the
// stored value `abc-def-ghi`. The cold tier used to read the quoted form as
// equality and prune every bloom layer on the exact value `abc-def`, returning
// 0 rows where hot VictoriaLogs returns the row (#319).

type phraseRow struct {
	traceID string
	service string
	body    string
}

// phraseFixtureRows: values chosen so each phrase shape below is a substring
// of some stored value, on a token boundary or not.
var phraseFixtureRows = []phraseRow{
	{"abc-def-ghi", "api-gw-v2", "r0"},
	{"abc-def", "api-gw", "r1"},
	{"uvw-abc-def-ghi", "db-primary", "r2"},
	{"xyz", "api-gw-v2", "r3"},
	{"abcdef", "cache", "r4"},
	{"0123456789abcdef0123456789abcdef", "worker", "r5"},
	{"fedcba9876543210fedcba9876543210", "worker", "r6"},
}

// phraseQueries covers: a phrase that is a substring of a longer value, a
// phrase at word boundaries, a phrase that is NOT on a token boundary, the
// full-value phrase, the unquoted phrase, exact, in(), and AND combinations.
var phraseQueries = []string{
	`trace_id:"abc-def"`,
	`trace_id:"def-ghi"`,
	`trace_id:"ghi"`,
	`trace_id:ghi`,
	`trace_id:"abc-d"`,
	`trace_id:"abc-def-ghi"`,
	`trace_id:abc`,
	`trace_id:"abc"`,
	`trace_id:"0123456789abcdef0123456789abcdef"`,
	`trace_id:="0123456789abcdef0123456789abcdef"`,
	`trace_id:=abc-def`,
	`trace_id:="abc-def"`,
	`trace_id:in("abc-def","xyz")`,
	`service.name:"api-gw"`,
	`service.name:"gw-v2"`,
	`service.name:="api-gw"`,
	`trace_id:"abc-def" service.name:"api-gw"`,
	`trace_id:"abc-def" service.name:="api-gw"`,
	`trace_id:"abc-def" | stats count() n`,
}

func phraseOracle(t *testing.T, query string, rows []phraseRow) []string {
	t.Helper()
	f := parseFilterFromQueryStr(query)
	if f == nil {
		t.Fatalf("no filter parsed from %q", query)
	}
	var want []string
	for _, r := range rows {
		fields := []logstorage.Field{
			{Name: "trace_id", Value: r.traceID},
			{Name: "service.name", Value: r.service},
			{Name: "_msg", Value: r.body},
		}
		if f.MatchRow(fields) {
			want = append(want, r.body)
		}
	}
	sort.Strings(want)
	return want
}

func newPhraseStorage(t *testing.T, pmeta bool) (*Storage, func(), time.Time) {
	t.Helper()
	mock := newMockS3Server()
	s := testStorageWithS3(t, mock.url())
	var bw *BatchWriter
	if pmeta {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw = NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
		bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	} else {
		bw = NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	}
	now := time.Now().Truncate(time.Second)
	// One flush per row so every row sits in its own file with its own
	// footer bloom, row-group SBBF and pmeta file bloom.
	for i, r := range phraseFixtureRows {
		bw.AddLogRows([]schema.LogRow{{
			TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              r.body,
			ServiceName:       r.service,
			TraceID:           r.traceID,
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
		if b, ok := row["_msg"]; ok {
			got = append(got, b)
		} else if _, ok := row["trace_id"]; ok {
			// RunQuery hands back the projected rows; the stats pipe runs
			// above it. The row count is what the pipe would count.
			got = append(got, "row")
		}
	}
	sort.Strings(got)
	return got
}

// TestPhrasePruning_ColdMatchesHotMatcher: for every phrase shape the cold
// answer equals what VictoriaLogs' own MatchRow returns, on a fresh file per
// row, with pmeta on (file bloom facet) and off (per-file bloom), through the
// footer bloom and row-group SBBF layers.
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
				want := phraseOracle(t, query, phraseFixtureRows)
				got := runPhraseQuery(t, s, query, now)
				if query == `trace_id:"abc-def" | stats count() n` {
					want = make([]string, len(phraseOracle(t, `trace_id:"abc-def"`, phraseFixtureRows)))
					for i := range want {
						want[i] = "row"
					}
				}
				if !slices.Equal(got, want) {
					t.Errorf("%s: cold=%v hot(MatchRow)=%v", query, got, want)
				}
			}
		})
	}
}

// TestPhrasePruning_NoExactExtraction pins the layer inputs directly: no
// bloom, pushdown or file-label layer may receive the phrase value as an
// exact value, and the exact forms must still be extracted.
func TestPhrasePruning_NoExactExtraction(t *testing.T) {
	for _, query := range []string{
		`trace_id:"abc-def"`, `trace_id:abc`, `service.name:"api-gw"`,
		`trace_id:"abc-def" service.name:"api-gw"`,
	} {
		for _, col := range []string{"trace_id", "service.name"} {
			if got := extractFilterValuesAST(query, col); len(got) != 0 {
				t.Errorf("extractFilterValuesAST(%q, %q) = %v: a phrase is not an exact value", query, col, got)
			}
			if got := extractFilterValues(query, col); len(got) != 0 {
				t.Errorf("extractFilterValues(%q, %q) = %v: a phrase is not an exact value", query, col, got)
			}
		}
	}
	s := testStorage()
	for _, query := range []string{`trace_id:"abc-def"`, `service.name:"api-gw"`} {
		if bc := s.buildBloomChecks(query); len(bc) != 0 {
			t.Errorf("buildBloomChecks(%q) = %v: footer/row-group bloom would prune on the phrase", query, bc)
		}
		if pdf := buildPushDownFilter(query, s.registry); pdf != nil {
			t.Errorf("buildPushDownFilter(%q) = %+v: pushdown would prune on the phrase", query, pdf.Checks)
		}
	}
	for _, c := range []struct {
		query, col, want string
	}{
		{`trace_id:="abc-def"`, "trace_id", "abc-def"},
		{`trace_id:=abc-def`, "trace_id", "abc-def"},
		{`service.name:="api-gw"`, "service.name", "api-gw"},
	} {
		got := extractFilterValuesAST(c.query, c.col)
		if len(got) != 1 || got[0] != c.want {
			t.Errorf("extractFilterValuesAST(%q, %q) = %v, want [%s]", c.query, c.col, got, c.want)
		}
	}
	if got := extractFilterValuesAST(`trace_id:in("abc-def","xyz")`, "trace_id"); len(got) != 2 {
		t.Errorf("in() must stay an exact set, got %v", got)
	}
}

// TestPhrasePruning_TokenBloomKeepsPhraseTokens: the token bloom prunes a
// phrase only by "every phrase token is present" (sound on token boundaries).
func TestPhrasePruning_TokenBloomKeepsPhraseTokens(t *testing.T) {
	got := extractSearchTokens(`_msg:"connection refused"`)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "connection" || got[1] != "refused" {
		t.Errorf("phrase tokens = %v, want [connection refused]", got)
	}
}

// TestPhrasePruning_PmetaFileBloomKeepsPhraseFiles pins the pmeta file-bloom
// layer directly: a phrase that is a substring of a stored value must not
// exclude that file, while the exact forms still prune.
func TestPhrasePruning_PmetaFileBloomKeepsPhraseFiles(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, true)
	defer closeFn()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) != len(phraseFixtureRows) {
		t.Fatalf("want %d files, got %d", len(phraseFixtureRows), len(files))
	}
	ctx := context.Background()
	for _, query := range []string{`trace_id:"abc-def"`, `trace_id:"def-ghi"`, `trace_id:"ghi"`} {
		for _, fi := range files {
			if s.checkFileBloom(ctx, fi, query) {
				t.Errorf("%s: pmeta file bloom pruned %s on a phrase", query, fi.Key)
			}
		}
	}
	for _, fi := range files {
		if !s.checkFileBloom(ctx, fi, `trace_id:="no-such-trace-xyz"`) {
			t.Errorf("exact absent value no longer pruned for %s", fi.Key)
		}
	}
}
