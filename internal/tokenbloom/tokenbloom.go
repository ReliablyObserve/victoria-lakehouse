// Package tokenbloom is the per-row-group body token bloom the writers embed in
// the Parquet footer key-value metadata (key _bloom_body_rg_<N>), and the one
// codec both the writers (flush, compaction, delete rewrite) and the readers
// (row-group pruning on a body word search) use.
package tokenbloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"unicode"

	"github.com/parquet-go/parquet-go"
)

// TokenBloom is a minimal bloom filter for full-text token searches.
// It uses double hashing (FNV-1a based) to simulate k hash functions.
type TokenBloom struct {
	bits []uint64
	k    int // number of hash functions
}

// New creates a bloom filter sized for expectedTokens at the given
// false positive rate. For 10K tokens at 1% FPR, this produces ~1KB.
func New(expectedTokens int, fpr float64) *TokenBloom {
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
	_, _ = h.Write([]byte(s))
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

// Tokenize splits a string into deduplicated, lowercased tokens.
// Splits on non-alphanumeric characters, keeps tokens >= 2 chars.
func Tokenize(s string) []string {
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

// BuildMetadata creates a metadata key-value pair for a row group's
// body field token bloom filter.
// Key format: _bloom_body_rg_{N}
// Value: marshaled TokenBloom bytes (base64-encoded for Parquet string metadata).
func BuildMetadata(bodies []string, rgIndex int) (key string, value []byte) {
	// Collect all tokens from all body strings
	allTokens := make(map[string]struct{})
	for _, body := range bodies {
		for _, tok := range Tokenize(body) {
			allTokens[tok] = struct{}{}
		}
	}

	expectedTokens := len(allTokens)
	if expectedTokens == 0 {
		expectedTokens = 1
	}

	bloom := New(expectedTokens, 0.01)
	for tok := range allTokens {
		bloom.Add(tok)
	}

	data, _ := bloom.MarshalBinary()
	key = fmt.Sprintf(KeyFormat, rgIndex)
	return key, data
}

// K is the number of hash functions of the filter.
func (b *TokenBloom) K() int { return b.k }

// Words is the number of 64-bit words of the filter.
func (b *TokenBloom) Words() int { return len(b.bits) }

// KeyFormat is the footer key of row group N's body token bloom.
const KeyFormat = "_bloom_body_rg_%d"

// RowGroupOptions are the footer key-values (one per row group that has a non-empty
// body) for n rows written in row groups of rowGroupSize: row group i holds rows
// [i*rowGroupSize, (i+1)*rowGroupSize), the split parquet-go makes with
// MaxRowsPerRowGroup(rowGroupSize). body returns the body of row i.
func RowGroupOptions(n, rowGroupSize int, body func(i int) string) []parquet.WriterOption {
	if rowGroupSize <= 0 {
		return nil
	}
	var opts []parquet.WriterOption
	for rg := 0; rg*rowGroupSize < n; rg++ {
		start, end := rg*rowGroupSize, (rg+1)*rowGroupSize
		if end > n {
			end = n
		}
		bodies := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			if b := body(i); b != "" {
				bodies = append(bodies, b)
			}
		}
		if len(bodies) > 0 {
			key, value := BuildMetadata(bodies, rg)
			opts = append(opts, parquet.KeyValueMetadata(key, string(value)))
		}
	}
	return opts
}
