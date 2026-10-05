package parquets3

import (
	"bytes"
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A quoted phrase filter (`field:"v"`) is NOT an exact match. Upstream
// (lib/logstorage/filter_phrase.go, matchPhrase) matches a row when the field
// CONTAINS the phrase on token boundaries. The cold tier used to read the
// quoted form as equality and prune every bloom layer on the exact value
// (#319). A phrase is now pruned by its tokens only, except that a full-hex
// `trace_id:"X"` (VictoriaTraces' trace-by-ID form) prunes as the value X in a
// file whose writer attests that every trace_id in it is lowercase hex
// (lh.trace_id_hex=1). VictoriaTraces stores ids as sent on its OTLP/HTTP JSON
// and native paths, so files holding such ids carry lh.trace_id_hex=0.

type phraseRow struct {
	traceID string
	span    string
	service string
	id      string
}

// phraseFixtureFiles: each inner slice is flushed as one Parquet object.
// attested says what its footer must attest.
var phraseFixtureFiles = []struct {
	rows     []phraseRow
	attested bool
}{
	{[]phraseRow{{"abcdef0123456789abcdef0123456789", "GET /api/v1/users", "frontend", "r0"}}, true},
	{[]phraseRow{{"abcdef", "GET /api", "frontend-v2", "r1"}}, true},
	{[]phraseRow{{"abcdef01", "POST /api/v1/orders", "orders", "r2"}}, true},
	{[]phraseRow{{"ffff0000ffff0000ffff0000ffff0000", "GET /health", "frontend", "r3"}}, true},
	{[]phraseRow{{"ABCDEF", "DELETE /api/v1/users/7", "frontend-v2", "r4"}}, false},
	{[]phraseRow{{"0123456789abcdef0123456789abcdef", "db query", "db-primary", "r5"}}, true},
	{[]phraseRow{{"1234abcd", "GET /x", "api-gw-v2", "r6"}}, true},
	// The review's ids: as stored by VictoriaTraces from OTLP/HTTP JSON or
	// /insert/native, which keep the id as sent.
	{[]phraseRow{{"abc-def-ghi", "GET /review", "review", "r7"}}, false},
	{[]phraseRow{{"4bf92f35-77b3-4da6-a3ce-929d0e0bf736", "GET /uuid", "review", "r8"}}, false},
	// One non-hex id makes the whole file unattested, hex rows included.
	{[]phraseRow{{"abc", "GET /mixed", "mixed", "r9"}, {"zz-abc", "GET /mixed2", "mixed", "r10"}}, false},
	// A base64 id (OTLP/HTTP JSON clients that base64-encode bytes).
	{[]phraseRow{{"S/kvNXezTaajzpKdDvc2Aw==", "GET /b64", "review", "r11"}}, false},
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
	`trace_id:"ABCDEF"`,
	`trace_id:"abc-def"`,
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
	// Exact prefix: `field:="v"*` is a prefix, never the exact value v.
	`trace_id:="abc"*`,
	`trace_id:=abc*`,
	`trace_id:="4bf92f35"*`,
	`trace_id:="abcdef01"*`,
	`"resource_attr:service.name":="front"*`,
	`name:="GET /api"*`,
	// Phrase prefix (#298).
	`trace_id:"abcdef01"*`,
	`trace_id:abcdef01*`,
	`trace_id:"4bf92f35"*`,
	`trace_id:"abc-def"*`,
	// OR / NOT around a phrase: never a value to prune by.
	`trace_id:"abc" OR "resource_attr:service.name":="orders"`,
	`trace_id:"abcdef" OR trace_id:"4bf92f35"`,
	`NOT trace_id:"abc"`,
	`-trace_id:"abcdef"`,
	`NOT (trace_id:"abc" "resource_attr:service.name":="mixed")`,
	`"resource_attr:service.name":="review" NOT trace_id:"ghi"`,
	`(trace_id:"abc" OR trace_id:"ghi") "resource_attr:service.name":="review"`,
	// Other columns: substring-on-boundary phrases must still match.
	`name:"GET /api"`,
	`name:"/api/v1"`,
	`name:"users"`,
	`name:="GET /api"`,
	`"resource_attr:service.name":"frontend"`,
	`"resource_attr:service.name":="frontend"`,
	`"resource_attr:service.name":"api-gw"`,
	`"resource_attr:service.name":"gw-v2"`,
	`"resource_attr:service.name":="api-gw"`,
	`trace_id:"abcdef" "resource_attr:service.name":"frontend"`,
	`trace_id:"abcdef" name:"GET /api"`,
	`trace_id:"abc" "resource_attr:service.name":="mixed"`,
	`trace_id:"abcdef0123456789abcdef0123456789" | fields span_id`,
}

// phraseIfQueries: `stats ... if (...)` shapes; inner is the if filter.
var phraseIfQueries = []struct{ q, inner string }{
	{`* | stats count() if (trace_id:"abc") n`, `trace_id:"abc"`},
	{`* | stats count() if (trace_id:"4bf92f35") n`, `trace_id:"4bf92f35"`},
	{`* | stats count() if (NOT trace_id:"abcdef") n`, `NOT trace_id:"abcdef"`},
	{`* | stats count() if (trace_id:"ghi" OR trace_id:"abcdef") n`, `trace_id:"ghi" OR trace_id:"abcdef"`},
	{`* | stats by ("resource_attr:service.name") count() if (trace_id:"abc") n`, `trace_id:"abc"`},
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
			{Name: "name", Value: r.span},
			{Name: "resource_attr:service.name", Value: r.service},
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
	// One flush per fixture file: each sits in its own object with its own
	// footer bloom, row-group SBBF, pmeta file bloom, _trace_idx and
	// lh.trace_id_hex attestation.
	i := 0
	for _, file := range phraseFixtureFiles {
		var rows []schema.TraceRow
		for _, r := range file.rows {
			ts := now.Add(time.Duration(i) * time.Millisecond).UnixNano()
			i++
			rows = append(rows, schema.TraceRow{
				TimestampUnixNano: ts,
				StartTimeUnixNano: ts,
				DurationNs:        1000,
				TraceID:           r.traceID,
				SpanID:            r.id,
				SpanName:          r.span,
				ServiceName:       r.service,
			})
		}
		bw.stageTraceRows(rows)
		bw.flushStagedNow()
	}
	return s, mock.close, now
}

func phraseRunner(t *testing.T, s *Storage, now time.Time) func(query string) []map[string]string {
	t.Helper()
	run := tracesRunner(t, s, now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	return func(query string) []map[string]string { return run(context.Background(), query) }
}

func spanIDs(rows []map[string]string) []string {
	var got []string
	for _, row := range rows {
		got = append(got, row["span_id"])
	}
	sort.Strings(got)
	return got
}

// TestPhrasePruning_ColdMatchesHotMatcher: every phrase shape returns what
// VictoriaTraces' own MatchRow returns, through every pruning layer (footer
// bloom, row-group SBBF, pmeta/file bloom, _trace_idx, token bloom, pushdown),
// on attested and unattested files, as rows and as a count.
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
				if got := spanIDs(run(query)); !slices.Equal(got, want) {
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
		byFirstRow[f.rows[0].id] = f.attested
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
		reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
		rows := make([]schema.TraceRow, reader.NumRows())
		n, _ := reader.Read(rows)
		_ = reader.Close()
		want := byFirstRow[rows[0].SpanID]
		if n == 0 || (kv == "1") != want {
			t.Errorf("%s (first span %s): footer attests %q, want attested=%v", fi.Key, rows[0].SpanID, kv, want)
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
// prunes at every layer (pmeta file bloom, the multi-file bloom pre-filter,
// _trace_idx and the row-group SBBF), so trace-by-ID does not turn into a scan;
// on unattested files it prunes nowhere.
func TestPhrasePruning_AttestedFilesPrune(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, true)
	defer closeFn()
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
	if len(attested) != 6 || len(other) != 5 {
		t.Fatalf("fixture: %d attested / %d other files, want 6 / 5", len(attested), len(other))
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

	absent := `trace_id:"deadbeefdeadbeefdeadbeefdeadbeef"`
	for _, fi := range files {
		if got := s.checkFileBloom(ctx, fi, absent); got != fi.TraceIDHex {
			t.Errorf("checkFileBloom(%s, attested=%v) pruned=%v, want %v", fi.Key, fi.TraceIDHex, got, fi.TraceIDHex)
		}
	}
	if got := keys(s.filterFilesByBloomIndex(slices.Clone(files), absent)); !slices.Equal(got, other) {
		t.Errorf("bloom pre-filter kept %v, want only the unattested files %v", got, other)
	}
	if got := keys(s.filterFilesByTraceIdxAttested(ctx, slices.Clone(files), nil, extractHexTraceIDPhrasesAST(absent))); !slices.Equal(got, other) {
		t.Errorf("_trace_idx kept %v, want only the unattested files %v", got, other)
	}
	if got := keys(s.filterFilesByTraceIdxAttested(ctx, slices.Clone(files), extractFilterValuesAST(absent, "trace_id"), nil)); len(got) != len(files) {
		t.Errorf("_trace_idx with no exact id kept %d files, want all %d", len(got), len(files))
	}
	// The exact form prunes every file, attested or not.
	if got := s.filterFilesByBloomIndex(slices.Clone(files), `trace_id:="deadbeefdeadbeefdeadbeefdeadbeef"`); len(got) != 0 {
		t.Errorf("exact absent id kept %v", keys(got))
	}

	// The file holding the trace is kept in both forms.
	present := `trace_id:"abcdef0123456789abcdef0123456789"`
	want := append([]string{}, other...)
	for _, fi := range files {
		if fi.TraceIDHex && s.checkFileBloom(ctx, fi, `trace_id:="abcdef0123456789abcdef0123456789"`) == false {
			want = append(want, fi.Key)
		}
	}
	sort.Strings(want)
	if len(want) != len(other)+1 {
		t.Fatalf("exact form kept %d attested files, want 1", len(want)-len(other))
	}
	if got := keys(s.filterFilesByBloomIndex(slices.Clone(files), present)); !slices.Equal(got, want) {
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
		{`span.name:"GET /api"`, "span.name", nil},
		{`service.name:"frontend"`, "service.name", nil},
		{`service.name:frontend`, "service.name", nil},
		// exact prefix is not an exact value, quoted or not
		{`trace_id:="abc"*`, "trace_id", nil},
		{`trace_id:=abc*`, "trace_id", nil},
		{`service.name:="api-gw"*`, "service.name", nil},
		// exact and in() stay exact
		{`trace_id:="abc-def"`, "trace_id", []string{"abc-def"}},
		{`service.name:="frontend"`, "service.name", []string{"frontend"}},
		{`span.name:="GET /api"`, "span.name", []string{"GET /api"}},
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
		{`trace_id:"abc" | fields span_id`, []string{"abc"}},
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
	for _, query := range []string{`span.name:"GET /api"`, `service.name:"frontend"`, `trace_id:"abc-def"`, `trace_id:"abcdef0123456789abcdef0123456789"`} {
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
// the footer attests it.
func TestPhrasePruning_FooterLearnedAttestation(t *testing.T) {
	s, closeFn, now := newPhraseStorage(t, false)
	defer closeFn()
	files := s.manifest.GetFilesForRange(now.Add(-time.Hour).UnixNano(), now.Add(time.Hour).UnixNano())
	m := manifest.New("test-bucket", "logs/")
	orig := s.manifest
	s.manifest = m
	defer func() { s.manifest = orig }()
	for _, fi := range files {
		bare := manifest.FileInfo{Key: fi.Key, Size: fi.Size}
		m.AddFile(partitionOfKey(fi.Key), bare)
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
		s.enrichFromParquetFile(manifest.FileInfo{Key: fi.Key}, pf)
	}
	for _, fi := range m.GetFilesForRange(0, 1<<62) {
		var want bool
		for _, orig := range files {
			if orig.Key == fi.Key {
				want = orig.TraceIDHex
			}
		}
		if fi.TraceIDHex != want {
			t.Errorf("%s: learned TraceIDHex=%v, want %v", fi.Key, fi.TraceIDHex, want)
		}
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
