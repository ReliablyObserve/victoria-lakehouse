package parquets3

import (
	"fmt"
	"unicode/utf8"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tokenbloom"
)

// TokenBloom is the per-row-group body token bloom (see internal/tokenbloom, the
// one codec the writers of every path and this reader share).
type TokenBloom = tokenbloom.TokenBloom

// NewTokenBloom creates a bloom filter sized for expectedTokens at the given
// false positive rate.
func NewTokenBloom(expectedTokens int, fpr float64) *TokenBloom {
	return tokenbloom.New(expectedTokens, fpr)
}

// tokenize splits a string into deduplicated, lowercased tokens.
func tokenize(s string) []string { return tokenbloom.Tokenize(s) }

// buildTokenBloomMetadata creates the footer key-value pair of a row group's
// body token bloom.
func buildTokenBloomMetadata(bodies []string, rgIndex int) (key string, value []byte) {
	return tokenbloom.BuildMetadata(bodies, rgIndex)
}

func tokenBloomSkip(metadata map[string]string, rgIndex int, searchTokens []string) bool {
	if len(searchTokens) == 0 {
		return false
	}

	key := fmt.Sprintf(tokenbloom.KeyFormat, rgIndex)
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
