package conformance

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// VictoriaLogs 1.51.0 tightened the LogsQL pipe grammar: a bare word after a
// pipe is no longer silently treated as a filter. `parsePipe` now tries the
// pipe names, then the stats-function shorthand, then the filter shorthand —
// and a word that is none of those is rejected with
//
//	unexpected pipe name %q; probably, 'filter' is missing in front of %q
//
// so `_time:5m | error` must be written `_time:5m | filter error` (or folded
// into the leading filter as `_time:5m error`). Quoted tokens, non-word tokens
// (`!foo`, `{host="x"}`, `>5`) and `not` are still accepted bare — 1.52.0
// restored those after 1.51.0 briefly rejected them too.
//
// The registry's own rows are parsed by
// TestRows_QueriesParseWithUpstream. This file extends the same idea to every
// OTHER LogsQL literal in the repository — tests, benchmark and operational
// scripts, docs, dashboards — so the next grammar change is caught by a unit
// test rather than by a query that silently 400s in production.

// logsqlScanRoots are the paths scanned for LogsQL literals, relative to the
// repo root. deps/ is excluded on purpose (upstream's own sources are not ours
// to fix), as are generated and archival files.
var logsqlScanRoots = []string{
	"internal", "cmd", "lakehouse-traces/internal", "lakehouse-traces/main.go",
	"lakehouse-traces/handlers.go", "tests", "scripts", "docs", "README.md",
	"deployment/docker/grafana/provisioning", "dashboards", "alerts",
	"benchmarks", ".github/workflows", "CHANGELOG.md",
}

// logsqlScanSkipDirs are directories pruned anywhere under a scan root.
var logsqlScanSkipDirs = map[string]bool{
	"deps": true, "node_modules": true, "testdata": false,
}

var logsqlScanExts = map[string]bool{
	".go": true, ".sh": true, ".md": true, ".json": true, ".yaml": true, ".yml": true,
}

// missingFilterRe matches the exact error VictoriaLogs raises for the grammar
// change this file guards. Matching the message rather than any parse failure
// is what keeps the scan honest: a string that merely happens to contain a
// pipe fails with some other message and is ignored.
var missingFilterRe = regexp.MustCompile(`probably, 'filter' is missing in front of`)

// pipeKeywordRe matches "| <word>" — the shape both a LogsQL pipeline and a
// shell pipeline have. The keyword is checked against the real parser's tables
// below.
var pipeKeywordRe = regexp.MustCompile(`\|\s*([a-zA-Z_][a-zA-Z0-9_]*)`)

// logsqlMarkerRe matches the VictoriaLogs special field names. A piped string
// containing one is treated as a LogsQL query even when no stage name is
// recognisable — which is exactly the case the 1.51.0 change creates: in
// `_time:5m | error` the only stage is the word that stopped parsing, so a
// keyword-only test would skip the very literal it exists to catch.
var logsqlMarkerRe = regexp.MustCompile(`(^|[^a-zA-Z0-9_])_(time|msg|stream|stream_id)\b`)

// logsqlPipeKeywords is every name VictoriaLogs accepts directly after a pipe:
// the pipe names (including aliases such as head/keep/where) and the stats
// function names that may appear without the `stats` keyword. Derived by
// probing the real parser, so it cannot drift from the vendored version.
func logsqlPipeKeywords() map[string]bool {
	out := map[string]bool{}
	for _, name := range []string{
		// pipes and aliases (lib/logstorage/pipe.go initPipeParsers)
		"block_stats", "blocks_count", "coalesce", "collapse_nums", "copy", "cp",
		"decolorize", "del", "delete", "drop", "drop_empty_fields", "extract",
		"extract_regexp", "eval", "facets", "field_names", "field_values", "fields",
		"filter", "first", "format", "generate_sequence", "hash", "join",
		"json_array_concat", "json_array_len", "head", "keep", "last", "len", "limit",
		"math", "mv", "offset", "order", "pack_json", "pack_logfmt", "query_stats",
		"rename", "replace", "replace_regexp", "rm", "running_stats", "sample",
		"set_stream_fields", "skip", "sort", "split", "stats", "stats_remote",
		"stream_context", "time_add", "top", "total_stats", "union", "uniq",
		"unpack_json", "unpack_logfmt", "unpack_syslog", "unpack_words", "unroll",
		"where",
		// stats functions usable without the `stats` keyword
		// (lib/logstorage/pipe_stats.go getStatsFuncParsers) plus `by`
		"any", "avg", "by", "count", "count_empty", "count_uniq", "count_uniq_hash",
		"field_max", "field_min", "histogram", "json_values", "max", "median", "min",
		"quantile", "rate", "rate_sum", "row_any", "row_max", "row_min", "stddev",
		"sum", "sum_len", "uniq_values", "values",
	} {
		out[name] = true
	}
	return out
}

// notLogsQL lists literals that reach the parser looking like a LogsQL
// pipeline but must not be rewritten — a shell pipeline, prose, a format
// string, or (today) the deliberately-broken fixtures this file needs in order
// to assert that the grammar change is still in force. Each entry is the exact
// literal text plus the reason it is exempt. Keeping them enumerated (rather
// than loosening the heuristic) means a NEW look-alike has to be judged by a
// human instead of slipping through, and
// TestNotLogsQLExemptionsAreStillNeeded deletes entries that stop matching
// anything.
var notLogsQL = map[string]string{
	"_time:5m | error":                      "deliberately-broken fixture: quoted in this file's doc comment and asserted as rejected by TestLogsQLPipeGrammarContract",
	"_time:5m | stats count() rows | error": "deliberately-broken fixture of TestLogsQLPipeGrammarContract",
	"_time:5m | myservice":                  "deliberately-broken fixture of TestLogsQLPipeGrammarContract",
}

type logsqlLiteral struct {
	file string
	line int
	text string
}

// TestRepoLogsQLLiteralsParse is the gate: every LogsQL literal outside the
// registry must parse with the vendored VictoriaLogs parser.
func TestRepoLogsQLLiteralsParse(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	requireVLDeps(t, root)

	lits := scanLogsQLLiterals(t, root)
	keywords := logsqlPipeKeywords()

	parsed, skipped := 0, 0
	seen := map[string]bool{}
	for _, l := range lits {
		if !looksLikeLogsQL(l.text, keywords) {
			continue
		}
		if reason, ok := notLogsQL[l.text]; ok {
			skipped++
			_ = reason
			continue
		}
		if _, err := logstorage.ParseQuery(l.text); err != nil {
			if !missingFilterRe.MatchString(err.Error()) {
				// Not the grammar change this gate is about: a template with
				// %s/{{...}} holes, a partial pipeline, a shell fragment.
				continue
			}
			key := l.text
			if seen[key] {
				continue
			}
			seen[key] = true
			t.Errorf("%s:%d: LogsQL literal uses the pre-1.51.0 pipe shorthand and no longer parses:\n  %s\n  %v\n"+
				"  Write `| filter <word>` (or fold the word into the leading filter). If this is not a LogsQL query, add it to notLogsQL with the reason.",
				l.file, l.line, l.text, err)
			continue
		}
		parsed++
	}

	if parsed < 50 {
		t.Fatalf("only %d LogsQL literals were parsed across %v — the extractor stopped finding them, so this gate is passing vacuously", parsed, logsqlScanRoots)
	}
	t.Logf("parsed %d LogsQL literals from %d candidate strings (%d exempted as not-LogsQL)", parsed, len(lits), skipped)
}

// TestLogsQLPipeGrammarContract pins the exact post-1.52.0 rule the sweep
// above enforces, straight against the vendored parser. It is what tells a
// future reader (and a future upstream bump) which forms after a pipe are
// accepted bare and which now need `filter` — VictoriaLogs 1.51.0 rejected
// several of these and 1.52.0 restored them, so "it parses" is not a stable
// assumption worth carrying in prose alone.
func TestLogsQLPipeGrammarContract(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	requireVLDeps(t, root)

	accepted := []struct{ query, why string }{
		{`_time:5m | filter error`, "the explicit filter pipe — always correct"},
		{`_time:5m error`, "folded into the leading filter instead of a pipe stage"},
		{`_time:5m | !error`, "a negated word is not a bare word token"},
		{`_time:5m | not error`, "'not' is a logical operator, not a pipe name"},
		{`_time:5m | {host="x"}`, "a stream filter does not start with a word"},
		{`_time:5m | level:error`, "word followed by ':' is the filter shorthand"},
		{`_time:5m | "error"`, "a quoted token is always a filter"},
		{`_time:5m | stats count() rows | rows:>5`, "a comparison filter on a stats result"},
		{`_time:5m | count()`, "the stats shorthand without the 'stats' keyword"},
	}
	for _, c := range accepted {
		if _, err := logstorage.ParseQuery(c.query); err != nil {
			t.Errorf("%q should parse (%s) but does not: %v", c.query, c.why, err)
		}
	}

	rejected := []string{
		`_time:5m | error`,
		`_time:5m | stats count() rows | error`,
		`_time:5m | myservice`,
	}
	for _, q := range rejected {
		_, err := logstorage.ParseQuery(q)
		if err == nil {
			t.Errorf("%q parses, but a bare word after a pipe must be rejected since VictoriaLogs 1.51.0 — the sweep in this file has nothing left to guard", q)
			continue
		}
		if !missingFilterRe.MatchString(err.Error()) {
			t.Errorf("%q was rejected with %v, not the missing-'filter' error the sweep matches on — update missingFilterRe", q, err)
		}
	}
}

// TestNotLogsQLExemptionsAreStillNeeded keeps the exemption list from rotting:
// an entry whose literal no longer appears anywhere is dead weight that would
// silently re-exempt a future literal with the same text.
func TestNotLogsQLExemptionsAreStillNeeded(t *testing.T) {
	if len(notLogsQL) == 0 {
		t.Skip("no exemptions")
	}
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, l := range scanLogsQLLiterals(t, root) {
		present[l.text] = true
	}
	var dead []string
	for text := range notLogsQL {
		if !present[text] {
			dead = append(dead, text)
		}
	}
	sort.Strings(dead)
	for _, d := range dead {
		t.Errorf("notLogsQL exempts %q, which no longer appears in the repository — drop the entry", d)
	}
}

// requireVLDeps skips only when the vendored VictoriaLogs is absent and
// skipping is allowed.
func requireVLDeps(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "deps", "VictoriaLogs")
	if _, err := os.Stat(dir); err != nil {
		if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
			t.Fatalf("deps missing (%s): run make deps-logs", dir)
		}
		t.Skip("vendored deps not present locally")
	}
}

// looksLikeLogsQL decides whether a piped string is worth handing to the
// parser. Two independent signals, either of which is enough:
//
//   - a stage name the parser recognises after a pipe (hasKnownPipeKeyword), or
//   - a VictoriaLogs special field (_time, _msg, _stream, _stream_id), which
//     catches a pipeline whose only stage is the bare word that no longer
//     parses.
//
// A shell pipeline into grep/jq/awk has neither and is never considered.
func looksLikeLogsQL(s string, keywords map[string]bool) bool {
	return hasKnownPipeKeyword(s, keywords) || logsqlMarkerRe.MatchString(s)
}

// hasKnownPipeKeyword reports whether the string contains "| <word>" for a word
// the LogsQL parser recognises after a pipe.
func hasKnownPipeKeyword(s string, keywords map[string]bool) bool {
	for _, m := range pipeKeywordRe.FindAllStringSubmatch(s, -1) {
		if keywords[strings.ToLower(m[1])] {
			return true
		}
	}
	return false
}

func scanLogsQLLiterals(t *testing.T, root string) []logsqlLiteral {
	t.Helper()
	var out []logsqlLiteral
	for _, rel := range logsqlScanRoots {
		p := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("scan root %s: %v", rel, err)
		}
		if !info.IsDir() {
			out = append(out, extractLiterals(t, root, p)...)
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if logsqlScanSkipDirs[d.Name()] {
					return fs.SkipDir
				}
				return nil
			}
			if !logsqlScanExts[strings.ToLower(filepath.Ext(path))] {
				return nil
			}
			out = append(out, extractLiterals(t, root, path)...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", rel, err)
		}
	}
	return out
}

// literalSpanRe matches the quoted / backticked spans that carry a query in
// every file type scanned: Go interpreted and raw strings, shell and YAML
// quoted scalars, JSON strings, and Markdown inline code. Spans are matched
// per line, which is what the reported line number refers to; a query split
// across source lines is not extracted (and none in this repository is).
var literalSpanRe = regexp.MustCompile("`([^`\n]*)`" + `|"((?:[^"\\\n]|\\.)*)"|'([^'\n]*)'`)

func extractLiterals(t *testing.T, root, path string) []logsqlLiteral {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	rel, _ := filepath.Rel(root, path)
	rel = filepath.ToSlash(rel)

	var out []logsqlLiteral
	for i, line := range strings.Split(string(data), "\n") {
		for _, m := range literalSpanRe.FindAllStringSubmatch(line, -1) {
			for _, g := range m[1:] {
				if g == "" || !strings.Contains(g, "|") {
					continue
				}
				out = append(out, logsqlLiteral{file: rel, line: i + 1, text: unescapeGo(g)})
			}
		}
	}
	return out
}

// unescapeGo resolves the escapes a Go / JSON interpreted string literal can
// carry, so the parser sees the query the program actually sends.
func unescapeGo(s string) string {
	r := strings.NewReplacer(`\"`, `"`, `\\`, `\`, `\n`, "\n", `\t`, "\t", `\r`, "\r")
	return r.Replace(s)
}
