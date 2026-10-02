package parquets3

// phraseExactColumns lists the query fields whose stored values are PROVEN to
// be a single upstream token, so a phrase filter on them can be pruned by an
// exact-value bloom.
//
// Why that is sound. Upstream matches `field:"P"` with matchPhrase
// (lib/logstorage/filter_phrase.go): the field value V must contain P, and when
// P starts (ends) with a token rune the rune before (after) the match must not
// be a token rune; token runes are letters, digits and '_' (isTokenRune in
// lib/logstorage/tokenizer.go). If every stored V consists only of token runes
// (a single token), no rune can precede or follow a match, so V contains P on
// token boundaries only when V == P. For such a column `field:"P"` is
// equivalent to `field:=P` for an ASCII token phrase P, and the exact-value
// bloom, the file-level bloom and the {_trace_idx} footer index may prune it.
// TestPhraseExact_EquivalentToExactOnSingleTokenDomain checks the equivalence
// against upstream's own matcher over the whole domain.
//
// Columns proven here:
//   - none for logs: no logs column has a value domain the writer can
//     guarantee. trace_id in particular carries whatever the shipper sent (for
//     example UUIDs such as 4bf92f35-77b3-4da6-a3ce-929d0e0bf736, whose '-'
//     separates several tokens), so a phrase can match a longer value and the
//     logs binary never prunes a phrase by an exact value.
//
// A column belongs here only with a proof of its value domain at ingest. A
// phrase on any other column, and any phrase that is not a plain ASCII token,
// is never pruned by an exact value; the token bloom (all phrase tokens must be
// present) is the only phrase pruning allowed there.
var phraseExactColumns = map[string]struct{}{}

// phraseIsExactForField reports whether the phrase filter `fieldName:"phrase"`
// may be treated as the exact value `fieldName:=phrase` for pruning.
func phraseIsExactForField(fieldName, phrase string) bool {
	if phrase == "" {
		return false // an empty phrase matches only an empty/missing value
	}
	if _, ok := phraseExactColumns[fieldName]; !ok {
		return false
	}
	for i := 0; i < len(phrase); i++ {
		c := phrase[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		default:
			return false
		}
	}
	return true
}
