package registry

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestRows_LoadAndCounts enforces the coverage rule: coverage is measured by
// DISTINCT upstream keys per kind (one real inventory item can be exercised
// by more than one row — e.g. an edge case or a differ variant — without
// inflating the count), not by raw row counts. Thresholds match the real
// upstream inventory (pipes 48, filters 33, stats 24, traceql 9), and no row
// may cite one of the 7 phantom names that were removed from the original
// candidate lists because they are not real upstream inventory items.
func TestRows_LoadAndCounts(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}

	distinctUpstream := func(kind Kind, surface Surface) int {
		seen := map[string]bool{}
		for _, r := range reg.Rows {
			if r.Kind == kind && r.Surface == surface && r.Upstream != nil {
				seen[r.Upstream.Key()] = true
			}
		}
		return len(seen)
	}
	if n := distinctUpstream(KindPipe, SurfaceVL); n < 48 {
		t.Fatalf("want >= 48 distinct pipe upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindFilter, SurfaceVL); n < 33 {
		t.Fatalf("want >= 33 distinct filter upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindStats, SurfaceVL); n < 24 {
		t.Fatalf("want >= 24 distinct stats upstream keys, got %d", n)
	}
	if n := distinctUpstream(KindTraceQL, SurfaceVT); n < 9 {
		t.Fatalf("want >= 9 distinct traceql upstream keys, got %d", n)
	}

	phantom := map[string]bool{
		"pipe:pack": true, "pipe:unpack": true, "pipe:update": true, "pipe:sort_topk": true,
		"stats:json_values_sorted": true, "stats:json_values_topk": true,
		"filter:generic": true,
	}
	for _, r := range reg.Rows {
		if r.Upstream != nil && phantom[r.Upstream.Key()] {
			t.Fatalf("row %s cites phantom upstream item %s (not in the real inventory)", r.ID, r.Upstream.Key())
		}
	}

	if reg.ByID["vl.select.tail.unsupported"] == nil || reg.ByID["vl.select.tail.unsupported"].Expect != ExpectUnsupported {
		t.Fatal("tail row must be declared unsupported")
	}
	if reg.ByID["vt.tempo.metrics_instant.absent"] == nil {
		t.Fatal("metrics/instant must be declared absent (upstream lacks it too)")
	}
	if reg.ByID["lh.stats.overview.schema"] == nil {
		t.Fatal("LH stats overview row missing")
	}
}

// templateReplacer substitutes the row-template placeholders with dummy
// values so a query string is syntactically complete enough to parse. Values
// are chosen only to be well-formed (a valid RFC3339 timestamp, a valid hex
// id of the right length) — not to be semantically meaningful.
var templateReplacer = strings.NewReplacer(
	"{{seed.start}}", "2026-01-01T00:00:00Z",
	"{{seed.cold_end}}", "2026-01-02T00:00:00Z",
	"{{seed.trace_id}}", strings.Repeat("0123456789abcdef", 2), // 32 hex chars
	// _stream_id is TenantID.marshalString (one uint64, 16 hex chars) followed
	// by u128.marshalString (two uint64, 32 hex chars) = 48 hex chars total
	// (see deps/VictoriaLogs/lib/logstorage/{tenant_id,u128,stream_id}.go) —
	// not the 64 a naive guess would produce; a 64-char value fails
	// filterStreamID's unmarshal and defeats the point of this test.
	"{{seed.stream_id}}", strings.Repeat("0123456789abcdef", 3),
	"{{tenant.account}}", "0",
	"{{tenant.project}}", "0",
)

// TestRows_QueriesParseWithUpstream enforces the reuse-upstream-code rule:
// every row that sends a LogsQL query to a /select/logsql/* endpoint
// must parse with the real vendored VL parser (github.com/VictoriaMetrics/
// VictoriaLogs/lib/logstorage, vendored under deps/ at VL_VERSION_LOGS and
// reachable from the root module via the go.mod replace directive) — not just
// look plausible. The row's rendered query must also round-trip: re-parsing
// Query.String() must succeed.
func TestRows_QueriesParseWithUpstream(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}
	pinnedVL := pinnedVLVersion(t)
	checked, skippedAhead := 0, 0
	for _, r := range reg.Rows {
		if r.Request == nil || !strings.HasPrefix(r.Request.Path, "/select/logsql/") {
			continue
		}
		raw, ok := r.Request.Params["query"]
		if !ok || raw == "" {
			continue
		}
		// Rows deliberately exercising invalid-query error handling (expect
		// both tiers to 400 the same way) are validated at runtime (a later
		// phase), not by the real parser here.
		if r.Compare != nil && r.Compare.Type == "error" {
			continue
		}
		// A row whose `since` is NEWER than the pin exercises syntax the
		// vendored parser does not have yet by design — that's the entire
		// point of the since/differ annotation, and it can never parse against
		// today's deps/VictoriaLogs checkout. A row whose `since` the pin has
		// already reached is checked like any other: after the VL 1.52.0 bump
		// that includes coalesce (since 1.51.0) and json_array_concat (since
		// 1.52.0), which previously escaped the parser entirely.
		if sinceNewerThanPin(r.Since["vl"], pinnedVL) {
			skippedAhead++
			continue
		}
		checked++
		q := templateReplacer.Replace(raw)
		parsed, err := logstorage.ParseQuery(q)
		if err != nil {
			t.Errorf("row %s: query %q failed to parse with the real VL parser: %v", r.ID, q, err)
			continue
		}
		if _, err := logstorage.ParseQuery(parsed.String()); err != nil {
			t.Errorf("row %s: round-trip parse of %q failed: %v", r.ID, parsed.String(), err)
		}
	}
	if checked == 0 {
		t.Fatal("no /select/logsql/* rows with a query param were checked — the test is not exercising anything")
	}
	t.Logf("parsed %d LogsQL row queries against vendored VictoriaLogs %s (%d skipped: since is newer than the pin)", checked, pinnedVL, skippedAhead)
}

// pinnedVLVersion reads VL_VERSION_LOGS from the Makefile — the single source
// of truth for which VictoriaLogs the vendored parser under deps/ actually is.
func pinnedVLVersion(t *testing.T) string {
	t.Helper()
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	v := inventory.DefaultDirs(root).VLVersion
	if v == "" {
		t.Fatal("VL_VERSION_LOGS not readable from the Makefile — without the pin this test cannot tell an ahead-of-pin row from a broken one")
	}
	return v
}

// sinceNewerThanPin reports whether a row's `since` version is strictly newer
// than the pinned one. Both are dotted integers with an optional leading "v".
// An unparseable or absent `since` is never "ahead": the row gets checked.
func sinceNewerThanPin(since, pin string) bool {
	if since == "" {
		return false
	}
	part := func(s string, i int) int {
		fields := strings.Split(strings.TrimPrefix(s, "v"), ".")
		if i >= len(fields) {
			return 0
		}
		n, err := strconv.Atoi(fields[i])
		if err != nil {
			return 0
		}
		return n
	}
	for i := 0; i < 3; i++ {
		a, b := part(since, i), part(pin, i)
		if a != b {
			return a > b
		}
	}
	return false
}

// tempoKnownStageFuncs are the pipe-stage keywords the traces module accepts
// after "| " in a TraceQL query (its own parser test, run in the traces
// module, is the source of truth for the full grammar — this is a
// structural sanity check only, since the root module cannot import
// VictoriaTraces).
var tempoKnownStageFuncs = map[string]bool{
	"rate": true, "count_over_time": true, "min_over_time": true, "max_over_time": true,
	"avg_over_time": true, "sum_over_time": true, "quantile_over_time": true,
	"histogram_over_time": true, "compare": true, "by": true, "select": true,
	// "count" is a real TraceQL spanset-pipeline keyword (e.g. `| count() > 1`
	// to filter traces by matched-span count) — a search-query construct, not
	// a metrics function, so it wasn't in the original metrics-function list.
	"count": true,
}

// TestRows_TempoQueriesStructural is a structural-only check (balanced
// brackets, known pipe-stage function names) for TraceQL query strings sent
// to /select/tempo/*. The exact TraceQL grammar lives in VictoriaTraces,
// which the root module (LogsQL/VL only) cannot import; real TraceQL
// parsing of these queries runs as part of the traces module's own test
// suite later.
func TestRows_TempoQueriesStructural(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, r := range reg.Rows {
		if r.Request == nil || !strings.HasPrefix(r.Request.Path, "/select/tempo/") {
			continue
		}
		raw, ok := r.Request.Params["q"]
		if !ok || raw == "" {
			continue
		}
		checked++
		q := templateReplacer.Replace(raw)
		if err := checkBalancedBrackets(q); err != nil {
			t.Errorf("row %s: TraceQL query %q: %v", r.ID, q, err)
		}
		for _, stage := range splitPipeStages(q)[1:] {
			fn := leadingIdentifier(strings.TrimSpace(stage))
			if fn == "" || !tempoKnownStageFuncs[fn] {
				t.Errorf("row %s: TraceQL query %q: pipe stage %q does not start with a known function", r.ID, q, stage)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no /select/tempo/* rows with a q param were checked — the test is not exercising anything")
	}
}

// splitPipeStages splits a TraceQL query on "| " pipe-stage boundaries,
// like strings.Split(q, "| "), except it ignores any "| " that falls inside
// a quoted string literal ("..." or `...`) — a naive strings.Split would
// mistake a literal pipe character inside an attribute value (e.g.
// span.name="a| b") for a stage boundary and misreport the row that
// contains it as having an unknown/malformed pipe stage.
func splitPipeStages(q string) []string {
	var out []string
	var quote byte
	start := 0
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '"' || c == '`':
			quote = c
		case c == '|' && i+1 < len(q) && q[i+1] == ' ':
			out = append(out, q[start:i])
			i++ // also skip the space consumed by the "| " delimiter
			start = i + 1
		}
	}
	return append(out, q[start:])
}

// checkBalancedBrackets verifies every '(' / '{' has a matching close, in order.
func checkBalancedBrackets(q string) error {
	var stack []byte
	open := map[byte]byte{')': '(', '}': '{'}
	for i := 0; i < len(q); i++ {
		switch c := q[i]; c {
		case '(', '{':
			stack = append(stack, c)
		case ')', '}':
			if len(stack) == 0 || stack[len(stack)-1] != open[c] {
				return fmt.Errorf("unbalanced %q at byte offset %d", c, i)
			}
			stack = stack[:len(stack)-1]
		}
	}
	if len(stack) != 0 {
		return fmt.Errorf("%d unclosed bracket(s)", len(stack))
	}
	return nil
}

// leadingIdentifier returns the leading [A-Za-z0-9_]+ run of s.
func leadingIdentifier(s string) string {
	end := 0
	for end < len(s) {
		c := s[end]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			break
		}
		end++
	}
	return s[:end]
}

// TestSplitPipeStages_QuoteAware proves splitPipeStages does not mistake a
// literal "| " inside a quoted string (double-quoted or backtick) for a
// pipe-stage boundary, and still splits normally outside of quotes.
func TestSplitPipeStages_QuoteAware(t *testing.T) {
	cases := []struct {
		name string
		q    string
		want []string
	}{
		{
			name: "no quotes, matches strings.Split",
			q:    `{resource_attr:service.name="checkout"} | count() > 1`,
			want: []string{`{resource_attr:service.name="checkout"} `, `count() > 1`},
		},
		{
			name: "double-quoted literal pipe is not a boundary",
			q:    `{span_attr:message="a| b"} | count() > 1`,
			want: []string{`{span_attr:message="a| b"} `, `count() > 1`},
		},
		{
			name: "backtick-quoted literal pipe is not a boundary",
			q:    "{span_attr:message=`a| b`} | count() > 1",
			want: []string{"{span_attr:message=`a| b`} ", "count() > 1"},
		},
		{
			name: "multiple real stages after a quoted pipe",
			q:    `{span_attr:message="a| b"} | by(resource_attr:service.name) | count() > 1`,
			want: []string{`{span_attr:message="a| b"} `, `by(resource_attr:service.name) `, `count() > 1`},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := splitPipeStages(c.q)
			if len(got) != len(c.want) {
				t.Fatalf("splitPipeStages(%q) = %v, want %v", c.q, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("splitPipeStages(%q)[%d] = %q, want %q", c.q, i, got[i], c.want[i])
				}
			}
		})
	}
}
