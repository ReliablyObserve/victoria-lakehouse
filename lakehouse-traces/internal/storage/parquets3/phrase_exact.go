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
//   - trace_id (traces): VictoriaTraces writes it only as
//     hex.AppendEncode(<span trace-id bytes>)
//     (deps/VictoriaTraces/app/vtinsert/opentelemetry/pb.go: traceID =
//     fb.formatHex(traceIDBytes); fmt_buffer.go: hex.AppendEncode), so every
//     stored value matches [0-9a-f]* and is a single upstream token.
//
// A column belongs here only with a proof of its value domain at ingest. A
// phrase on any other column, and any phrase that is not a plain ASCII token,
// is never pruned by an exact value; the token bloom (all phrase tokens must be
// present) is the only phrase pruning allowed there.
var phraseExactColumns = map[string]struct{}{"trace_id": {}}

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
