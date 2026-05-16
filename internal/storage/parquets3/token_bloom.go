package parquets3

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"unicode"
)

// TokenBloom is a minimal bloom filter for full-text token searches.
// It uses double hashing (FNV-1a based) to simulate k hash functions.
type TokenBloom struct {
	bits []uint64
	k    int // number of hash functions
}

// NewTokenBloom creates a bloom filter sized for expectedTokens at the given
// false positive rate. For 10K tokens at 1% FPR, this produces ~1KB.
func NewTokenBloom(expectedTokens int, fpr float64) *TokenBloom {
	if expectedTokens <= 0 {
		expectedTokens = 1
	}
	if fpr <= 0 || fpr >= 1 {
		fpr = 0.01
	}

	// Optimal number of bits: m = -n*ln(p) / (ln(2))^2
	m := int(math.Ceil(-float64(expectedTokens) * math.Log(fpr) / (math.Ln2 * math.Ln2)))
	// Round up to multiple of 64
	words := (m + 63) / 64

	// Optimal number of hash functions: k = (m/n) * ln(2)
	k := int(math.Ceil(float64(m) / float64(expectedTokens) * math.Ln2))
	if k < 1 {
		k = 1
	}

	return &TokenBloom{
		bits: make([]uint64, words),
		k:    k,
	}
}

// Add inserts a token into the bloom filter.
func (b *TokenBloom) Add(token string) {
	h1, h2 := doubleHash(token)
	m := uint64(len(b.bits)) * 64
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % m
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

// Test returns true if the token might be in the set, false if definitely absent.
func (b *TokenBloom) Test(token string) bool {
	if len(b.bits) == 0 {
		return false
	}
	h1, h2 := doubleHash(token)
	m := uint64(len(b.bits)) * 64
	for i := 0; i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary encodes the bloom filter to a compact binary format.
// Format: [k:uint16][numWords:uint32][bits...]
func (b *TokenBloom) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 2+4+len(b.bits)*8)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(b.k))
	binary.LittleEndian.PutUint32(buf[2:6], uint32(len(b.bits)))
	for i, w := range b.bits {
		binary.LittleEndian.PutUint64(buf[6+i*8:6+(i+1)*8], w)
	}
	return buf, nil
}

// UnmarshalBinary decodes a bloom filter from binary format.
func (b *TokenBloom) UnmarshalBinary(data []byte) error {
	if len(data) < 6 {
		return fmt.Errorf("token bloom data too short: %d bytes", len(data))
	}
	b.k = int(binary.LittleEndian.Uint16(data[0:2]))
	numWords := int(binary.LittleEndian.Uint32(data[2:6]))
	if len(data) < 6+numWords*8 {
		return fmt.Errorf("token bloom data truncated: need %d bytes, have %d", 6+numWords*8, len(data))
	}
	b.bits = make([]uint64, numWords)
	for i := range b.bits {
		b.bits[i] = binary.LittleEndian.Uint64(data[6+i*8 : 6+(i+1)*8])
	}
	return nil
}

// doubleHash computes two independent hash values for double hashing.
// Uses FNV-1a as h1 and a murmur-inspired mix as h2.
func doubleHash(s string) (uint64, uint64) {
	h := fnv.New64a()
	h.Write([]byte(s))
	h1 := h.Sum64()

	// Murmur-style finalizer for h2 (independent from h1)
	h2 := h1 ^ 0x517cc1b727220a95
	h2 = (h2 ^ (h2 >> 33)) * 0xff51afd7ed558ccd
	h2 = (h2 ^ (h2 >> 33)) * 0xc4ceb9fe1a85ec53
	h2 = h2 ^ (h2 >> 33)
	// Ensure h2 is odd so it's coprime with m (better distribution)
	h2 |= 1

	return h1, h2
}

// tokenize splits a string into deduplicated, lowercased tokens.
// Splits on non-alphanumeric characters, keeps tokens >= 2 chars.
func tokenize(s string) []string {
	words := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	seen := make(map[string]struct{}, len(words))
	result := make([]string, 0, len(words))
	for _, w := range words {
		w = strings.ToLower(w)
		if len(w) < 2 {
			continue
		}
		if _, ok := seen[w]; ok {
			continue
		}
		seen[w] = struct{}{}
		result = append(result, w)
	}
	return result
}

// buildTokenBloomMetadata creates a metadata key-value pair for a row group's
// body field token bloom filter.
// Key format: _bloom_body_rg_{N}
// Value: marshaled TokenBloom bytes (base64-encoded for Parquet string metadata).
func buildTokenBloomMetadata(bodies []string, rgIndex int) (key string, value []byte) {
	// Collect all tokens from all body strings
	allTokens := make(map[string]struct{})
	for _, body := range bodies {
		for _, tok := range tokenize(body) {
			allTokens[tok] = struct{}{}
		}
	}

	expectedTokens := len(allTokens)
	if expectedTokens == 0 {
		expectedTokens = 1
	}

	bloom := NewTokenBloom(expectedTokens, 0.01)
	for tok := range allTokens {
		bloom.Add(tok)
	}

	data, _ := bloom.MarshalBinary()
	key = fmt.Sprintf("_bloom_body_rg_%d", rgIndex)
	return key, data
}

// tokenBloomSkip returns true if ANY search token is definitely absent from
// the row group (i.e., safe to skip this row group).
func tokenBloomSkip(metadata map[string]string, rgIndex int, searchTokens []string) bool {
	if len(searchTokens) == 0 {
		return false
	}

	key := fmt.Sprintf("_bloom_body_rg_%d", rgIndex)
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

// extractSearchTokens extracts body/message content search terms from a LogsQL
// query string. In VL's format:
// - bare words search the _msg field
// - field:value patterns search specific fields
// - body/_msg field searches are tokenized for bloom checking
func extractSearchTokens(queryStr string) []string {
	if queryStr == "" {
		return nil
	}

	var tokens []string

	// Look for _msg:"value" or _msg:value or body:"value" patterns
	for _, fieldName := range []string{"_msg", "body", "message"} {
		// Quoted exact match: field:"value"
		for _, prefix := range []string{fieldName + `:"`, fieldName + `:"`} {
			idx := 0
			for idx < len(queryStr) {
				pos := strings.Index(queryStr[idx:], prefix)
				if pos < 0 {
					break
				}
				start := idx + pos + len(prefix)
				end := strings.Index(queryStr[start:], `"`)
				if end < 0 {
					break
				}
				tokens = append(tokens, tokenize(queryStr[start:start+end])...)
				idx = start + end + 1
			}
		}

		// Unquoted: field:value (terminated by space or end)
		unquotedPrefix := fieldName + ":"
		idx := 0
		for idx < len(queryStr) {
			pos := strings.Index(queryStr[idx:], unquotedPrefix)
			if pos < 0 {
				break
			}
			start := idx + pos + len(unquotedPrefix)
			// Skip if it's a quoted variant (already handled above)
			if start < len(queryStr) && queryStr[start] == '"' {
				idx = start + 1
				continue
			}
			end := strings.IndexByte(queryStr[start:], ' ')
			var val string
			if end < 0 {
				val = queryStr[start:]
			} else {
				val = queryStr[start : start+end]
			}
			tokens = append(tokens, tokenize(val)...)
			if end < 0 {
				break
			}
			idx = start + end + 1
		}
	}

	// Also extract bare words (terms not part of field:value patterns) that
	// implicitly search _msg. A bare word is a space-delimited token that
	// doesn't contain ':' (simplified heuristic).
	parts := strings.Fields(queryStr)
	for _, p := range parts {
		if strings.Contains(p, ":") {
			continue
		}
		// Skip LogsQL operators/keywords
		if isLogsQLKeyword(p) {
			continue
		}
		tokens = append(tokens, tokenize(p)...)
	}

	// Deduplicate
	seen := make(map[string]struct{}, len(tokens))
	deduped := make([]string, 0, len(tokens))
	for _, t := range tokens {
		if _, ok := seen[t]; !ok {
			seen[t] = struct{}{}
			deduped = append(deduped, t)
		}
	}
	return deduped
}

// isLogsQLKeyword returns true for LogsQL reserved words that shouldn't be
// treated as search tokens.
func isLogsQLKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "and", "or", "not", "in", "by", "with", "limit", "offset",
		"asc", "desc", "pipe", "|", "*", "_time", "_stream":
		return true
	}
	return false
}

// Integration TODOs:
//
// Write side: In the Parquet writer (writer.go), after writing each row group,
// call buildTokenBloomMetadata with the body column values and store in
// file-level key-value metadata.
//
// Read side: In storage_query.go's row group loop, after bloom filter check,
// add token bloom check:
//
//   searchTokens := extractSearchTokens(queryStr)
//   fileMetadata := parseFileMetadata(f)
//   // ... in row group loop:
//   if tokenBloomSkip(fileMetadata, rgIdx, searchTokens) {
//       metrics.ParquetRowGroupsSkipped.Inc("token_bloom")
//       continue
//   }
