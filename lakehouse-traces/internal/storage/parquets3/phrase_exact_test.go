package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func phraseMatches(t testing.TB, phrase, value string) bool {
	t.Helper()
	f := parseFilterFromQueryStr(`trace_id:"` + phrase + `"`)
	if f == nil {
		t.Fatalf("no filter parsed for phrase %q", phrase)
	}
	return f.MatchRow([]logstorage.Field{{Name: "trace_id", Value: value}})
}

// allStrings returns every string over alphabet with length 0..maxLen.
func allStrings(alphabet []string, maxLen int) []string {
	out := []string{""}
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

// TestPhraseExact_EquivalentToExactOnAttestedDomain is the proof behind the
// lh.trace_id_hex exception, run against upstream's own matcher: in a file
// whose every trace_id is lowercase hex (empty included), a phrase that is a
// full lowercase-hex token matches a value exactly when the value equals it.
// So the exact-value blooms and _trace_idx may prune that phrase there.
func TestPhraseExact_EquivalentToExactOnAttestedDomain(t *testing.T) {
	alphabet := []string{"0", "9", "a", "f"}
	domain := allStrings(alphabet, 5)
	checked := 0
	for _, p := range allStrings(alphabet, 4) {
		if !schema.IsLowerHexToken(p) {
			continue // the empty phrase is never offered
		}
		for _, v := range domain {
			if !schema.IsLowerHex(v) {
				t.Fatalf("domain value %q is not lowercase hex", v)
			}
			if got, want := phraseMatches(t, p, v), v == p; got != want {
				t.Fatalf("phrase %q vs value %q: upstream match=%v, equality=%v", p, v, got, want)
			}
			checked++
		}
	}
	t.Logf("%d (phrase, value) pairs agree with upstream matchPhrase", checked)
}

// TestPhraseExact_NotEquivalentOutsideDomain is the negative control: the
// review's stored ids are not lowercase hex, and upstream matches the review's
// phrases inside them. A file holding any such id must not attest.
func TestPhraseExact_NotEquivalentOutsideDomain(t *testing.T) {
	uuid := "4bf92f35-77b3-4da6-a3ce-929d0e0bf736"
	for _, c := range [][2]string{
		{"abc", "abc-def-ghi"}, {"ghi", "abc-def-ghi"}, {"abc-def", "abc-def-ghi"},
		{"4bf92f35", uuid}, {"929d0e0bf736", uuid}, {"77b3-4da6", uuid},
		{"kvNXezTaajzpKdDvc2Aw", "S/kvNXezTaajzpKdDvc2Aw=="},
	} {
		if !phraseMatches(t, c[0], c[1]) {
			t.Errorf("upstream does not match phrase %q in %q", c[0], c[1])
		}
		if schema.IsLowerHex(c[1]) {
			t.Errorf("%q counted as lowercase hex", c[1])
		}
	}
	// Uppercase hex is outside the domain too: ABCDEF is one token but a
	// lowercase phrase cannot equal it and the writer must not attest it.
	if schema.IsLowerHex("ABCDEF") || schema.IsLowerHexToken("ABC") {
		t.Error("uppercase hex must not be attested or offered")
	}
}

// FuzzPhraseExact: on the attested domain upstream's matcher agrees with
// equality for every phrase the reader may offer as a value.
func FuzzPhraseExact(f *testing.F) {
	for _, s := range [][2]string{{"abc", "abc"}, {"abc", "abcd"}, {"a", "0a"}, {"0f", "00f"}, {"f", ""}} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, phrase, value string) {
		if !schema.IsLowerHexToken(phrase) || !schema.IsLowerHex(value) {
			return // outside what an attested file holds or the reader offers
		}
		if got, want := phraseMatches(t, phrase, value), value == phrase; got != want {
			t.Fatalf("phrase %q value %q: upstream=%v equality=%v", phrase, value, got, want)
		}
	})
}
