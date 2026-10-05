package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A quoted phrase filter (`field:"v"`) is NOT an exact match. Upstream
// (lib/logstorage/filter_phrase.go, matchPhrase) matches a row when the field
// CONTAINS the phrase on token boundaries, so `trace_id:"abc-def"` matches the
// stored value `abc-def-ghi`. The cold tier used to read the quoted form as
// equality and prune every bloom layer on the exact value `abc-def`, returning
// 0 rows where hot VictoriaLogs returns the row (#319). A phrase is now pruned
// by its tokens only, except that a full-hex `trace_id:"X"` prunes as the value
// X in a file whose writer attests that every trace_id in it is lowercase hex
// (lh.trace_id_hex=1). Logs carry whatever the shipper sent (UUIDs, base64),
// so any file with a non-hex id is unattested.

type phraseRow struct {
	traceID string
	service string
	text    string
	id      string // unique; the row's _msg starts with it
}

func (r phraseRow) body() string { return r.id + " " + r.text }

// phraseFixtureFiles: each inner slice is flushed as one Parquet object.
// attested says what its footer must attest.
var phraseFixtureFiles = []struct {
	rows     []phraseRow
	attested bool
}{
	{[]phraseRow{{"abcdef0123456789abcdef0123456789", "frontend", "GET /api/v1/users", "r0"}}, true},
	{[]phraseRow{{"abcdef", "frontend-v2", "GET /api", "r1"}}, true},
	{[]phraseRow{{"abcdef01", "orders", "POST /api/v1/orders", "r2"}}, true},
	{[]phraseRow{{"ffff0000ffff0000ffff0000ffff0000", "frontend", "GET /health", "r3"}}, true},
	{[]phraseRow{{"ABCDEF", "frontend-v2", "DELETE /api/v1/users/7", "r4"}}, false},
	{[]phraseRow{{"0123456789abcdef0123456789abcdef", "db-primary", "db query", "r5"}}, true},
	{[]phraseRow{{"1234abcd", "api-gw-v2", "GET /x", "r6"}}, true},
	{[]phraseRow{{"fedcba9876543210fedcba9876543210", "worker", "job done", "r7"}}, true},
	// The review's ids and the original #319 fixture: shipper-supplied ids.
	{[]phraseRow{{"abc-def-ghi", "review", "GET /review", "r8"}}, false},
	{[]phraseRow{{"abc-def", "api-gw", "GET /review2", "r9"}}, false},
	{[]phraseRow{{"uvw-abc-def-ghi", "db-primary", "GET /review3", "r10"}}, false},
	{[]phraseRow{{"xyz", "cache", "GET /review4", "r11"}}, false},
	{[]phraseRow{{"4bf92f35-77b3-4da6-a3ce-929d0e0bf736", "review", "GET /uuid", "r12"}}, false},
	// One non-hex id makes the whole file unattested, hex rows included.
	{[]phraseRow{{"abc", "mixed", "GET /mixed", "r13"}, {"zz-abc", "mixed", "GET /mixed2", "r14"}}, false},
	// A base64 id.
	{[]phraseRow{{"S/kvNXezTaajzpKdDvc2Aw==", "review", "GET /b64", "r15"}}, false},
}

var phraseQueries = []string{
	// Trace-by-ID style forms and the exact forms.
	`trace_id:"abcdef0123456789abcdef0123456789"`,
	`trace_id:="abcdef0123456789abcdef0123456789"`,
	`trace_id:abcdef0123456789abcdef0123456789`,
	`trace_id:"ffff0000ffff0000ffff0000ffff0000"`,
	`trace_id:"deadbeefdeadbeefdeadbeefdeadbeef"`,
	`trace_id:in("abcdef","ABCDEF","deadbeef")`,
	`trace_id:in("abc-def","xyz")`,
	// A phrase that is a prefix of a longer id is not on a token boundary.
	`trace_id:"abcdef"`,
	`trace_id:abcdef`,
	`trace_id:"abcdef01"`,
	`trace_id:"ABCDEF"`,
	`trace_id:"abc-def"`,
	`trace_id:"def-ghi"`,
	`trace_id:"abc-d"`,
	`trace_id:"abc-def-ghi"`,
	// The review's six shapes: a phrase inside a non-hex id.
	`trace_id:"abc"`,
	`trace_id:abc`,
	`trace_id:"ghi"`,
	`trace_id:"4bf92f35"`,
	`trace_id:4bf92f35`,
	`trace_id:"929d0e0bf736"`,
	`trace_id:"77b3-4da6"`,
	`trace_id:"kvNXezTaajzpKdDvc2Aw"`,
	`trace_id:=abc`,
	`trace_id:=abc-def`,
	`trace_id:="abc-def"`,
	// Exact prefix: `field:="v"*` is a prefix, never the exact value v.
	`trace_id:="abc"*`,
	`trace_id:=abc*`,
	`trace_id:="4bf92f35"*`,
	`trace_id:="abcdef01"*`,
	`service.name:="api"*`,
	`service.name:="front"*`,
	// Phrase prefix (#298).
	`trace_id:"abcdef01"*`,
	`trace_id:abcdef01*`,
	`trace_id:"4bf92f35"*`,
	`trace_id:"abc-def"*`,
	`trace_id:abcdef*`,
	// OR / NOT around a phrase: never a value to prune by.
	`trace_id:"abc" OR service.name:="cache"`,
	`trace_id:"abcdef" OR trace_id:"4bf92f35"`,
	`NOT trace_id:"abc"`,
	`-trace_id:"abcdef"`,
	`NOT (trace_id:"abc" service.name:="x")`,
	`NOT (trace_id:"abc" service.name:="mixed")`,
	`service.name:="review" NOT trace_id:"ghi"`,
	`(trace_id:"abc" OR trace_id:"ghi") service.name:="review"`,
	// Other columns and _msg word phrases.
	`service.name:"api-gw"`,
	`service.name:"gw-v2"`,
	`service.name:"frontend"`,
	`service.name:="frontend"`,
	`service.name:="api-gw"`,
	`_msg:"GET /api"`,
	`"GET /api"`,
	`"/api/v1"`,
	`users`,
	`GET`,
	`trace_id:"abcdef" service.name:"frontend"`,
	`trace_id:"abcdef" "GET /api"`,
	`trace_id:"abc" service.name:="mixed"`,
	`trace_id:"abcdef0123456789abcdef0123456789" | fields _msg`,
}

// phraseIfQueries: `stats ... if (...)` shapes; inner is the if filter.
var phraseIfQueries = []struct{ q, inner string }{
	{`* | stats count() if (trace_id:"abc") n`, `trace_id:"abc"`},
	{`* | stats count() if (trace_id:"4bf92f35") n`, `trace_id:"4bf92f35"`},
	{`* | stats count() if (NOT trace_id:"abcdef") n`, `NOT trace_id:"abcdef"`},
	{`* | stats count() if (trace_id:"ghi" OR trace_id:"abcdef") n`, `trace_id:"ghi" OR trace_id:"abcdef"`},
	{`* | stats by (service.name) count() if (trace_id:"abc") n`, `trace_id:"abc"`},
}

func allPhraseRows() []phraseRow {
	var out []phraseRow
	for _, f := range phraseFixtureFiles {
		out = append(out, f.rows...)
	}
	return out
}

func phraseOracle(t *testing.T, query string) []string {
	t.Helper()
	f := parseFilterFromQueryStr(query)
	if f == nil {
		t.Fatalf("no filter parsed from %q", query)
	}
	var want []string
	for _, r := range allPhraseRows() {
		fields := []logstorage.Field{
			{Name: "trace_id", Value: r.traceID},
			{Name: "service.name", Value: r.service},
			{Name: "_msg", Value: r.body()},
		}
		if f.MatchRow(fields) {
			want = append(want, r.body())
		}
	}
	sort.Strings(want)
	return want
}

func newPhraseStorage(t *testing.T, pmeta bool) (*Storage, func(), time.Time) {
	t.Helper()
	mock := newMockS3Server()
	s := testStorageWithS3(t, mock.url())
	bw := NewBatchWriter(&s.cfg.Insert, s.pool, s.manifest, "logs/", config.ModeLogs)
	if pmeta {
		s.cfg.Pmeta = config.PmetaConfig{Enabled: true}
		s.catalog = newCatalogStore(s.cfg.Pmeta, "logs/")
		bw.catalogObserver = &catalogObserver{store: s.catalog, pool: s.pool}
	}
	now := time.Now().Truncate(time.Second)
	// One flush per fixture file: each sits in its own object with its own
	// footer bloom, row-group SBBF, pmeta file bloom and lh.trace_id_hex
	// attestation.
	i := 0
	for _, file := range phraseFixtureFiles {
		var rows []schema.LogRow
		for _, r := range file.rows {
			rows = append(rows, schema.LogRow{
				TimestampUnixNano: now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
				Body:              r.body(),
				ServiceName:       r.service,
				TraceID:           r.traceID,
			})
			i++
		}
		bw.stageLogRows(rows)
		bw.flushStagedNow()
	}
	return s, mock.close, now
}

func phraseRunner(t *testing.T, s *Storage, now time.Time) func(query string) []map[string]string {
	t.Helper()
	run := reviewRunner(t, s, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	return func(query string) []map[string]string { return run(context.Background(), query) }
}

func msgs(rows []map[string]string) []string {
	var got []string
	for _, row := range rows {
		got = append(got, row["_msg"])
	}
	sort.Strings(got)
	return got
}

// TestPhrasePruning_ColdMatchesHotMatcher: every phrase shape returns what
// VictoriaLogs' own MatchRow returns, through every pruning layer (footer
// bloom, row-group SBBF, pmeta/file bloom, token bloom, pushdown), on attested
// and unattested files, as rows and as a count.
func TestPhrasePruning_ColdMatchesHotMatcher(t *testing.T) {
	for _, pmeta := range []bool{true, false} {
		name := "pmeta_off"
		if pmeta {
			name = "pmeta_on"
		}
		t.Run(name, func(t *testing.T) {
			s, closeFn, now := newPhraseStorage(t, pmeta)
			defer closeFn()
			run := phraseRunner(t, s, now)
			for _, query := range phraseQueries {
				want := phraseOracle(t, query)
				if query == `trace_id:"abcdef0123456789abcdef0123456789" | fields _msg` {
					want = phraseOracle(t, `trace_id:"abcdef0123456789abcdef0123456789"`)
				}
				if got := msgs(run(query)); !slices.Equal(got, want) {
					t.Errorf("%s: cold=%v hot(MatchRow)=%v", query, got, want)
				}
				if got := sumN(run(query + ` | stats count() n`)); got != len(want) {
					t.Errorf("%s | stats count(): cold=%d hot(MatchRow)=%d", query, got, len(want))
				}
			}
			for _, c := range phraseIfQueries {
				want := len(phraseOracle(t, c.inner))
				if got := sumN(run(c.q)); got != want {
					t.Errorf("%s: cold=%d hot(MatchRow)=%d", c.q, got, want)
				}
			}
		})
	}
}

// TestPhrasePruning_AttestationRecorded: every flushed object's footer carries
// lh.trace_id_hex as ASCII "1" or "0", and the manifest entry and the pmeta
// file-meta facet carry the same bit.
func TestPhrasePruning_AttestationRecorded(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, true)
	defer closeFn()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	if len(files) != len(phraseFixtureFiles) {
		t.Fatalf("want %d files, got %d", len(phraseFixtureFiles), len(files))
	}
	byFirstRow := map[string]bool{}
	for _, f := range phraseFixtureFiles {
		byFirstRow[f.rows[0].body()] = f.attested
	}
	for _, fi := range files {
		data, err := s.pool.Download(context.Background(), fi.Key)
		if err != nil {
			t.Fatal(err)
		}
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		kv, ok := pf.Lookup(schema.TraceIDHexMetaKey)
		if !ok || (kv != "0" && kv != "1") {
			t.Fatalf("%s: footer %s = %q (present=%v), want ASCII 0 or 1", fi.Key, schema.TraceIDHexMetaKey, kv, ok)
		}
		reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
		rows := make([]schema.LogRow, reader.NumRows())
		n, _ := reader.Read(rows)
		_ = reader.Close()
		if n == 0 {
			t.Fatalf("%s: no rows read", fi.Key)
		}
		want, known := byFirstRow[rows[0].Body]
		if !known {
			// Rows of one file are time-ordered; find any known body.
			for _, r := range rows[:n] {
				if w, ok := byFirstRow[r.Body]; ok {
					want, known = w, true
					break
				}
			}
		}
		if !known {
			t.Fatalf("%s: unknown first body %q", fi.Key, rows[0].Body)
		}
		if (kv == "1") != want {
			t.Errorf("%s (first row %s): footer attests %q, want attested=%v", fi.Key, rows[0].Body, kv, want)
		}
		if fi.TraceIDHex != want {
			t.Errorf("%s: manifest TraceIDHex=%v, want %v", fi.Key, fi.TraceIDHex, want)
		}
		fm, ok := s.catalog.FileMeta(manifest.ExtractTenantPartition(fi.Key), fi.Key)
		if !ok || fm.TraceIDHex != want {
			t.Errorf("%s: pmeta file-meta TraceIDHex=%v (found=%v), want %v", fi.Key, fm.TraceIDHex, ok, want)
		}
	}
}

// TestPhrasePruning_AttestedFilesPrune: on attested files a full-hex phrase
// prunes at every layer (pmeta file bloom, the multi-file bloom pre-filter and
// the row-group SBBF), so trace-by-ID does not turn into a scan; on unattested
// files it prunes nowhere.
func TestPhrasePruning_AttestedFilesPrune(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, true)
	defer closeFn()
	// bloomFilterFiles is a no-op without a bloom cache; the pmeta facet answers
	// first, so the legacy loader is never consulted.
	s.bloomCache = bloomindex.NewBloomCache(1024*1024, func(context.Context, string) (*bloomindex.Index, error) {
		return nil, nil
	})
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	ctx := context.Background()
	var attested, other []string
	for _, fi := range files {
		if fi.TraceIDHex {
			attested = append(attested, fi.Key)
		} else {
			other = append(other, fi.Key)
		}
	}
	wantAttested := 0
	for _, f := range phraseFixtureFiles {
		if f.attested {
			wantAttested++
		}
	}
	if len(attested) != wantAttested || len(other) != len(phraseFixtureFiles)-wantAttested || wantAttested < 5 {
		t.Fatalf("fixture: %d attested / %d other files, want %d / %d", len(attested), len(other), wantAttested, len(phraseFixtureFiles)-wantAttested)
	}
	keys := func(fs []manifest.FileInfo) []string {
		var out []string
		for _, fi := range fs {
			out = append(out, fi.Key)
		}
		sort.Strings(out)
		return out
	}
	sort.Strings(other)

	// A bloom may false-positive (tiny per-file blooms measured about 6%), so
	// absent ids are sampled: an unattested file is never pruned on the phrase,
	// an attested one is pruned on all but a few samples, and the exact form
	// prunes every file the same way.
	const samples = 24
	var attKept, exactKept int
	for i := 0; i < samples; i++ {
		id := fmt.Sprintf("%032x", 0xfeed0000+i)
		absent := `trace_id:"` + id + `"`
		for _, fi := range files {
			got := s.checkFileBloom(ctx, fi, absent)
			if !fi.TraceIDHex && got {
				t.Errorf("checkFileBloom(%s, unattested) pruned a phrase", fi.Key)
			}
			if fi.TraceIDHex && !got {
				attKept++
			}
			// the phrase prunes an attested file exactly as the exact form does
			if fi.TraceIDHex && got != s.checkFileBloom(ctx, fi, `trace_id:="`+id+`"`) {
				t.Errorf("%s: attested phrase and exact form disagree", fi.Key)
			}
		}
		kept := keys(s.bloomFilterFiles(ctx, slices.Clone(files), absent))
		for _, k := range other {
			if !slices.Contains(kept, k) {
				t.Errorf("bloom pre-filter pruned unattested file %s on a phrase", k)
			}
		}
		exactKept += len(s.bloomFilterFiles(ctx, slices.Clone(files), `trace_id:="`+id+`"`))
	}
	if attKept*5 > samples*len(attested) {
		t.Errorf("attested files kept %d of %d (absent phrase): the reader is not pruning by the attestation", attKept, samples*len(attested))
	}
	if exactKept*5 > samples*len(files) {
		t.Errorf("exact absent id kept %d of %d file checks", exactKept, samples*len(files))
	}
	if got := keys(s.bloomFilterFiles(ctx, slices.Clone(files), `trace_id:"deadbeefdeadbeefdeadbeefdeadbeef"`)); len(got) >= len(files) {
		t.Errorf("absent hex phrase pruned nothing: kept %d of %d", len(got), len(files))
	}

	// The file holding the trace is kept in both forms.
	present := `trace_id:"abcdef0123456789abcdef0123456789"`
	want := append([]string{}, other...)
	for _, fi := range files {
		if fi.TraceIDHex && !s.checkFileBloom(ctx, fi, `trace_id:="abcdef0123456789abcdef0123456789"`) {
			want = append(want, fi.Key)
		}
	}
	sort.Strings(want)
	if n := len(want) - len(other); n < 1 || n > 3 {
		t.Fatalf("exact form kept %d attested files, want the holder (plus at most a couple of bloom false positives)", n)
	}
	if got := keys(s.bloomFilterFiles(ctx, slices.Clone(files), present)); !slices.Equal(got, want) {
		t.Errorf("phrase pre-filter kept %v, want %v", got, want)
	}

	// Row-group SBBF: the file's own footer decides.
	if bc := s.buildBloomChecksFor(present, true); len(bc) != 1 {
		t.Errorf("attested file: %d row-group checks for the phrase, want 1", len(bc))
	}
	if bc := s.buildBloomChecksFor(present, false); len(bc) != 0 {
		t.Errorf("unattested file: %d row-group checks for the phrase, want 0", len(bc))
	}
	if bc := s.buildBloomChecksFor(`trace_id:"abc-def"`, true); len(bc) != 0 {
		t.Errorf("a non-hex phrase is never a value, got %d checks", len(bc))
	}
	if bc := s.buildBloomChecksFor(`service.name:"frontend"`, true); len(bc) != 0 {
		t.Errorf("a phrase on another column is never a value, got %d checks", len(bc))
	}
}

// TestPhrasePruning_ExtractionRules pins the layer inputs: a phrase is never an
// exact value; a full lowercase-hex trace_id phrase under the root AND is the
// only one offered to attested files.
func TestPhrasePruning_ExtractionRules(t *testing.T) {
	type c struct {
		query, col string
		want       []string
	}
	for _, tc := range []c{
		{`trace_id:"abcdef0123456789abcdef0123456789"`, "trace_id", nil},
		{`trace_id:abcdef`, "trace_id", nil},
		{`trace_id:"abc-def"`, "trace_id", nil},
		{`trace_id:""`, "trace_id", nil},
		{`k8s.pod.name:"GET /api"`, "k8s.pod.name", nil},
		{`service.name:"frontend"`, "service.name", nil},
		{`service.name:frontend`, "service.name", nil},
		// exact prefix is not an exact value, quoted or not
		{`trace_id:="abc"*`, "trace_id", nil},
		{`trace_id:=abc*`, "trace_id", nil},
		{`service.name:="api-gw"*`, "service.name", nil},
		// exact and in() stay exact
		{`trace_id:="abc-def"`, "trace_id", []string{"abc-def"}},
		{`service.name:="frontend"`, "service.name", []string{"frontend"}},
		{`k8s.pod.name:="GET /api"`, "k8s.pod.name", []string{"GET /api"}},
		{`trace_id:in("a","b")`, "trace_id", []string{"a", "b"}},
	} {
		if got := extractFilterValuesAST(tc.query, tc.col); !slices.Equal(got, tc.want) {
			t.Errorf("extractFilterValuesAST(%q, %q) = %v, want %v", tc.query, tc.col, got, tc.want)
		}
	}
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`trace_id:"abcdef0123456789abcdef0123456789"`, []string{"abcdef0123456789abcdef0123456789"}},
		{`trace_id:abcdef`, []string{"abcdef"}},
		{`trace_id:"abc" service.name:="x"`, []string{"abc"}},
		{`trace_id:"abc" | fields _msg`, []string{"abc"}},
		{`trace_id:"ABCDEF"`, nil},
		{`trace_id:"abc-def"`, nil},
		{`trace_id:"abcg"`, nil},
		{`trace_id:""`, nil},
		{`trace_id:"abc"*`, nil},
		{`trace_id:="abc"`, nil},
		{`span_id:"abc"`, nil},
		{`NOT trace_id:"abc"`, nil},
		{`-trace_id:"abc"`, nil},
		{`trace_id:"abc" OR service.name:="x"`, nil},
		{`(trace_id:"abc" OR trace_id:"def") service.name:="x"`, nil},
		{`NOT (trace_id:"abc" service.name:="x")`, nil},
		{`* | stats count() if (trace_id:"abc") n`, nil},
	} {
		if got := extractHexTraceIDPhrasesAST(tc.query); !slices.Equal(got, tc.want) {
			t.Errorf("extractHexTraceIDPhrasesAST(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
	s := testStorage()
	for _, query := range []string{`k8s.pod.name:"GET /api"`, `service.name:"frontend"`, `trace_id:"abc-def"`, `trace_id:"abcdef0123456789abcdef0123456789"`} {
		if bc := s.buildBloomChecks(query); len(bc) != 0 {
			t.Errorf("buildBloomChecks(%q) = %v: bloom would prune on a phrase in an unattested file", query, bc)
		}
		if pdf := buildPushDownFilter(query, s.registry); pdf != nil {
			t.Errorf("buildPushDownFilter(%q) = %+v: pushdown would prune on a phrase", query, pdf.Checks)
		}
	}
}

// TestPhrasePruning_ExactMatchStringFallback pins the text extractor the AST
// path falls back to: an exact prefix, a phrase and a glued suffix are not
// exact values; escaped quotes are unescaped.
func TestPhrasePruning_ExactMatchStringFallback(t *testing.T) {
	for _, c := range []struct{ query, want string }{
		{`trace_id:="abc"`, "abc"},
		{`trace_id:="abc" service.name:="x"`, "abc"},
		{`trace_id:="abc")`, "abc"},
		{`trace_id:="abc"|fields a`, "abc"},
		{`trace_id:="a\"b"`, `a"b`},
		{`trace_id:=abc`, "abc"},
		{`trace_id:="abc"*`, ""},
		{`trace_id:="a\"b"*`, ""},
		{`trace_id:=abc*`, ""},
		{`trace_id:"abc"`, ""},
		{`trace_id:abc`, ""},
		{`trace_id:="abc`, ""},
	} {
		if got := extractExactMatch(c.query, "trace_id"); got != c.want {
			t.Errorf("extractExactMatch(%q) = %q, want %q", c.query, got, c.want)
		}
	}
}

// TestPhrasePruning_AttestedPruneValues: the helper appends phrases only for
// the trace_id column of an attested file.
func TestPhrasePruning_AttestedPruneValues(t *testing.T) {
	q := `trace_id:"abcdef" service.name:="x"`
	trace := schema.FieldMapping{ParquetColumn: "trace_id", InternalName: "trace_id"}
	svc := schema.FieldMapping{ParquetColumn: "service.name", InternalName: "service.name"}
	if got := attestedPruneValues(nil, q, trace, true); !slices.Equal(got, []string{"abcdef"}) {
		t.Errorf("attested trace_id: %v", got)
	}
	if got := attestedPruneValues(nil, q, trace, false); got != nil {
		t.Errorf("unattested trace_id: %v", got)
	}
	if got := attestedPruneValues([]string{"x"}, q, svc, true); !slices.Equal(got, []string{"x"}) {
		t.Errorf("service.name: %v", got)
	}
}

// TestPhrasePruning_FooterLearnedAttestation: a manifest entry that lacks the
// bit (learned from a listing) gets it from the object's footer, and only when
// the footer attests it. Both enrichment paths: the cached-footer one in
// storage.go and the query-time one in storage_query.go.
func TestPhrasePruning_FooterLearnedAttestation(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, false)
	defer closeFn()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	orig := s.manifest
	defer func() { s.manifest = orig }()
	for name, enrich := range map[string]func(manifest.FileInfo, *parquet.File){
		"enrichFromParquetFile":    func(fi manifest.FileInfo, pf *parquet.File) { s.enrichFromParquetFile(fi, pf) },
		"enrichManifestFromFooter": func(fi manifest.FileInfo, pf *parquet.File) { s.enrichManifestFromFooter(fi, pf) },
	} {
		t.Run(name, func(t *testing.T) {
			m := manifest.New("test-bucket", "logs/")
			s.manifest = m
			for _, fi := range files {
				m.AddFile(manifest.ExtractTenantPartition(fi.Key), manifest.FileInfo{Key: fi.Key, Size: fi.Size})
			}
			for _, fi := range files {
				data, err := s.pool.Download(context.Background(), fi.Key)
				if err != nil {
					t.Fatal(err)
				}
				pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Fatal(err)
				}
				enrich(manifest.FileInfo{Key: fi.Key}, pf)
			}
			attested := 0
			for _, fi := range m.GetFilesForRange(0, 1<<62) {
				var want bool
				for _, o := range files {
					if o.Key == fi.Key {
						want = o.TraceIDHex
					}
				}
				if want {
					attested++
				}
				if fi.TraceIDHex != want {
					t.Errorf("%s: learned TraceIDHex=%v, want %v", fi.Key, fi.TraceIDHex, want)
				}
			}
			if attested == 0 {
				t.Error("no attested file learned; the fixture proves nothing")
			}
		})
	}
}

func TestPhrasePruning_SidecarCarriesAttestation(t *testing.T) {
	for _, want := range []bool{true, false} {
		fm := manifest.FileInfoToMeta(manifest.FileInfo{Key: "k", RowCount: 1, TraceIDHex: want})
		var fi manifest.FileInfo
		fm.ApplyTo(&fi)
		if fi.TraceIDHex != want || fm.TraceIDHex != want {
			t.Errorf("sidecar round trip: meta=%v applied=%v want %v", fm.TraceIDHex, fi.TraceIDHex, want)
		}
	}
}

// TestPhrasePruning_PushdownExactPrefix: the pushdown reads the exact prefix
// `field:="v"*` as a prefix check on v, never as the exact value v; the exact
// form stays exact.
func TestPhrasePruning_PushdownExactPrefix(t *testing.T) {
	s := testStorage()
	pdf := buildPushDownFilter(`service.name:="api"*`, s.registry)
	if pdf == nil || len(pdf.Checks) != 1 || pdf.Checks[0].Op != PushDownPrefix || pdf.Checks[0].Value != "api" {
		t.Fatalf("exact prefix pushdown = %+v, want one prefix check on \"api\"", pdf)
	}
	pdf = buildPushDownFilter(`service.name:="api"`, s.registry)
	if pdf == nil || len(pdf.Checks) != 1 || pdf.Checks[0].Op != PushDownExact || pdf.Checks[0].Value != "api" {
		t.Fatalf("exact pushdown = %+v, want one exact check on \"api\"", pdf)
	}
}
