package parquets3

import (
	"testing"
	"unicode"

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

// allStrings returns every string over alphabet with length 1..maxLen.
func allStrings(alphabet []string, maxLen int) []string {
	var out []string
	cur := []string{""}
	for l := 1; l <= maxLen; l++ {
		var next []string
		for _, c := range cur {
			for _, a := range alphabet {
				next = append(next, c+a)
			}
		}
		out = append(out, next...)
		cur = next
	}
	return out
}

// TestPhraseExact_EquivalentToExactOnSingleTokenDomain is the proof behind
// phraseExactColumns, run against upstream's own matcher: for every value V made
// only of token runes (the superset of VictoriaTraces' hex trace ids, including
// uppercase, '_' and a non-ASCII letter) and every ASCII token phrase P,
// `trace_id:"P"` matches V exactly when V == P. So exact-value pruning cannot
// lose a row.
func TestPhraseExact_EquivalentToExactOnSingleTokenDomain(t *testing.T) {
	domain := allStrings([]string{"0", "1", "a", "F", "_", "\u00e9"}, 4)
	phrases := allStrings([]string{"0", "1", "a", "F", "_"}, 3)
	checked := 0
	for _, p := range phrases {
		if !phraseIsExactForField("trace_id", p) {
			t.Fatalf("phraseIsExactForField(trace_id, %q) = false for an ASCII token", p)
		}
		for _, v := range domain {
			if got, want := phraseMatches(t, p, v), v == p; got != want {
				t.Fatalf("phrase %q vs value %q: upstream match=%v, equality=%v", p, v, got, want)
			}
			checked++
		}
	}
	t.Logf("%d (phrase, value) pairs agree with upstream matchPhrase", checked)
}

// TestPhraseExact_NotEquivalentOutsideDomain is the negative control: once
// values may contain a non-token rune ('-' in a UUID), a phrase matches longer
// values and exact-value pruning would lose rows. This is why only proven
// columns are listed.
func TestPhraseExact_NotEquivalentOutsideDomain(t *testing.T) {
	if !phraseMatches(t, "abc", "abc-def") {
		t.Fatal("expected upstream to match phrase abc inside abc-def")
	}
	if phraseIsExactForField("service.name", "abc") || phraseIsExactForField("span.name", "abc") ||
		phraseIsExactForField("body", "abc") || phraseIsExactForField("_msg", "abc") {
		t.Fatal("only proven single-token columns may be exact")
	}
}

func TestPhraseIsExactForField_Rules(t *testing.T) {
	for _, c := range []struct {
		field, phrase string
		want          bool
	}{
		{"trace_id", "abcdef0123456789", true},
		{"trace_id", "ABC_def", true},
		{"trace_id", "", false},
		{"trace_id", "abc-def", false},
		{"trace_id", "abc def", false},
		{"trace_id", "caf\u00e9", false},
		{"span_id", "abcdef", false},
		{"service.name", "abc", false},
	} {
		if got := phraseIsExactForField(c.field, c.phrase); got != c.want {
			t.Errorf("phraseIsExactForField(%q, %q) = %v, want %v", c.field, c.phrase, got, c.want)
		}
	}
	if _, ok := phraseExactColumns["trace_id"]; !ok || len(phraseExactColumns) != 1 {
		t.Errorf("phraseExactColumns = %v, want exactly trace_id", phraseExactColumns)
	}
}

// FuzzPhraseExact: whenever phraseIsExactForField accepts a phrase and the
// stored value is a single token, upstream's matcher agrees with equality.
func FuzzPhraseExact(f *testing.F) {
	for _, s := range [][2]string{{"abc", "abc"}, {"abc", "abcd"}, {"a", "a_b"}, {"0f", "00f"}} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, phrase, value string) {
		if !phraseIsExactForField("trace_id", phrase) {
			return
		}
		for _, r := range value {
			if !isUpstreamTokenRune(r) {
				return // outside the proven domain
			}
		}
		if value == "" {
			return
		}
		if got, want := phraseMatches(t, phrase, value), value == phrase; got != want {
			t.Fatalf("phrase %q value %q: upstream=%v equality=%v", phrase, value, got, want)
		}
	})
}

// isUpstreamTokenRune mirrors upstream isTokenRune (tokenizer.go) for the fuzz
// domain filter; TestPhraseExact_EquivalentToExactOnSingleTokenDomain checks
// the same property against the real matcher.
func isUpstreamTokenRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
