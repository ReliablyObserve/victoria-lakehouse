package parquets3

import (
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/parquetwriter"
)

// The token bloom and its footer key-value codec live in internal/parquetwriter,
// the one writer of Lakehouse Parquet objects: the flush writer and the compactor
// build the blooms there, this package only reads them (tokenBloomSkip). The
// aliases keep this package's call sites unchanged.

// TokenBloom is the per-row-group body token bloom stored in the Parquet footer.
type TokenBloom = parquetwriter.TokenBloom

// NewTokenBloom creates a bloom filter sized for expectedTokens at fpr.
func NewTokenBloom(expectedTokens int, fpr float64) *TokenBloom {
	return parquetwriter.NewTokenBloom(expectedTokens, fpr)
}

// tokenize splits a string into deduplicated, lowercased tokens (>= 2 chars).
func tokenize(s string) []string { return parquetwriter.Tokenize(s) }

// buildTokenBloomMetadata creates the footer key-value pair of a row group's
// body token bloom.
func buildTokenBloomMetadata(bodies []string, rgIndex int) (key string, value []byte) {
	return parquetwriter.BuildTokenBloomMetadata(bodies, rgIndex)
}

// tokenBloomSkip returns true if ANY search token is definitely absent from
// the row group (i.e., safe to skip this row group).
func tokenBloomSkip(metadata map[string]string, rgIndex int, searchTokens []string) bool {
	if len(searchTokens) == 0 {
		return false
	}

	key := parquetwriter.TokenBloomKey(rgIndex)
	raw, ok := metadata[key]
	if !ok {
		return false
	}

	var bloom TokenBloom
	if err := bloom.UnmarshalBinary([]byte(raw)); err != nil {
		return false
	}

	for _, tok := range searchTokens {
		if !bloom.Test(tok) {
			return true
		}
	}
	return false
}

// extractSearchTokens uses native filter guarantees before applying the physical
// bloom tokenizer. Parse failures conservatively disable bloom pruning.
func extractSearchTokens(queryStr string) []string {
	q, err := logstorage.ParseQuery(queryStr)
	if err != nil {
		return nil
	}
	return searchTokensFromQuery(q)
}

func searchTokensFromQuery(q *logstorage.Query) []string {
	seen := make(map[string]bool)
	var tokens []string
	for _, nativeToken := range logstorage.QueryRequiredMessageTokens(q) {
		// Native phrase matching can match invalid bytes inside a UTF-8 rune.
		// Rune tokenization would turn that byte suffix into a different word,
		// which is not guaranteed to exist in the persisted body bloom.
		if !utf8.ValidString(nativeToken) {
			continue
		}
		for _, token := range tokenize(nativeToken) {
			if !seen[token] {
				seen[token] = true
				tokens = append(tokens, token)
			}
		}
	}
	return tokens
}
func stripPipeOutsideQuotes(s string) string {
	inQuote := false
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			inQuote = !inQuote
		}
		if !inQuote && i+3 <= len(s) && s[i:i+3] == " | " {
			return s[:i]
		}
	}
	return s
}
