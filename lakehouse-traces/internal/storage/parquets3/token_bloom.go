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

func doubleHash(s string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	h1 := h.Sum64()

	h2 := h1 ^ 0x517cc1b727220a95
	h2 = (h2 ^ (h2 >> 33)) * 0xff51afd7ed558ccd
	h2 = (h2 ^ (h2 >> 33)) * 0xc4ceb9fe1a85ec53
	h2 = h2 ^ (h2 >> 33)
	h2 |= 1

	return h1, h2
}

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

func buildTokenBloomMetadata(bodies []string, rgIndex int) (key string, value []byte) {
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

func extractSearchTokens(queryStr string) []string {
	if queryStr == "" {
		return nil
	}

	if idx := strings.Index(queryStr, " | "); idx >= 0 {
		queryStr = queryStr[:idx]
	}

	var tokens []string

	for _, fieldName := range []string{"_msg", "body", "message"} {
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

		unquotedPrefix := fieldName + ":"
		idx := 0
		for idx < len(queryStr) {
			pos := strings.Index(queryStr[idx:], unquotedPrefix)
			if pos < 0 {
				break
			}
			start := idx + pos + len(unquotedPrefix)
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

	parts := strings.Fields(queryStr)
	for _, p := range parts {
		if strings.Contains(p, ":") {
			continue
		}
		if isLogsQLKeyword(p) {
			continue
		}
		tokens = append(tokens, tokenize(p)...)
	}

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

func isLogsQLKeyword(s string) bool {
	switch strings.ToLower(s) {
	case "and", "or", "not", "in", "by", "with", "limit", "offset",
		"asc", "desc", "pipe", "|", "*", "_time", "_stream":
		return true
	}
	return false
}
