package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func phraseMatches(t testing.TB, phrase, value string) bool {
	t.Helper()
	f := parseFilterFromQueryStr(`trace_id:"` + phrase + `"`)
	if f == nil {
		t.Fatalf("no filter parsed for phrase %q", phrase)
	}
	return f.MatchRow([]logstorage.Field{{Name: "trace_id", Value: value}})
}

// Logs has no column with a provable single-token value domain, so a phrase is
// never an exact value. trace_id in particular carries what the shipper sent
// (UUIDs contain '-'), and upstream matches a phrase inside a longer value.

func TestPhraseExact_NotEquivalentOnLogsColumns(t *testing.T) {
	if !phraseMatches(t, "4bf92f35", "4bf92f35-77b3-4da6-a3ce-929d0e0bf736") {
		t.Fatal("expected upstream to match phrase 4bf92f35 inside a UUID")
	}
	for _, c := range []string{"trace_id", "span_id", "service.name", "_msg", "body", "k8s.pod.name"} {
		if phraseIsExactForField(c, "abc") {
			t.Errorf("logs: phraseIsExactForField(%q, abc) = true; no logs column is proven single-token", c)
		}
	}
	if len(phraseExactColumns) != 0 {
		t.Errorf("phraseExactColumns = %v, want empty for logs", phraseExactColumns)
	}
}

// TestPhraseExact_TokenPhraseStillMatchesLongerValue: the failure mode the
// exact-value prune used to have, against upstream's matcher.
func TestPhraseExact_TokenPhraseStillMatchesLongerValue(t *testing.T) {
	for _, tc := range [][2]string{{"abc-def", "abc-def-ghi"}, {"def-ghi", "abc-def-ghi"}, {"ghi", "abc-def-ghi"}} {
		if !phraseMatches(t, tc[0], tc[1]) {
			t.Errorf("upstream does not match phrase %q in %q", tc[0], tc[1])
		}
	}
}

// FuzzPhraseExactLogs: logs never treats any phrase as exact.
func FuzzPhraseExactLogs(f *testing.F) {
	f.Add("trace_id", "abc")
	f.Add("service.name", "")
	f.Fuzz(func(t *testing.T, field, phrase string) {
		if phraseIsExactForField(field, phrase) {
			t.Fatalf("logs: phraseIsExactForField(%q, %q) = true", field, phrase)
		}
	})
}
