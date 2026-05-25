package parquets3

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestTokenBloomAddTest(t *testing.T) {
	bloom := NewTokenBloom(100, 0.01)

	// Add some tokens
	tokens := []string{"hello", "world", "error", "timeout", "connection"}
	for _, tok := range tokens {
		bloom.Add(tok)
	}

	// All added tokens must be found
	for _, tok := range tokens {
		if !bloom.Test(tok) {
			t.Errorf("token %q was added but Test returned false", tok)
		}
	}

	// Tokens not added should (likely) not be found
	absent := []string{"missing", "absent", "nonexistent"}
	for _, tok := range absent {
		// This could be a false positive, but with low FPR it's very unlikely
		if bloom.Test(tok) {
			t.Logf("possible false positive for %q (acceptable if rare)", tok)
		}
	}
}

func TestTokenBloomFalsePositiveRate(t *testing.T) {
	bloom := NewTokenBloom(1000, 0.01)

	// Add 1000 tokens
	for i := 0; i < 1000; i++ {
		bloom.Add(fmt.Sprintf("present_%d", i))
	}

	// Test 1000 absent tokens
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		if bloom.Test(fmt.Sprintf("absent_%d", i)) {
			falsePositives++
		}
	}

	// FPR should be < 2% (spec says 1% target, allow 2x margin)
	fpr := float64(falsePositives) / 1000.0
	if fpr >= 0.02 {
		t.Errorf("false positive rate too high: %.3f (expected < 0.02)", fpr)
	}
	t.Logf("false positive rate: %.3f (%d/1000)", fpr, falsePositives)
}

func TestTokenBloomMarshalUnmarshal(t *testing.T) {
	bloom := NewTokenBloom(500, 0.01)

	tokens := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, tok := range tokens {
		bloom.Add(tok)
	}

	data, err := bloom.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	var restored TokenBloom
	if err := restored.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Restored bloom must behave identically
	if restored.k != bloom.k {
		t.Errorf("k mismatch: got %d, want %d", restored.k, bloom.k)
	}
	if len(restored.bits) != len(bloom.bits) {
		t.Errorf("bits length mismatch: got %d, want %d", len(restored.bits), len(bloom.bits))
	}

	for _, tok := range tokens {
		if !restored.Test(tok) {
			t.Errorf("token %q not found after unmarshal", tok)
		}
	}
}

func TestTokenBloomMarshalBinarySize(t *testing.T) {
	// 10K tokens at 1% FPR should be ~1KB (within 2KB)
	bloom := NewTokenBloom(10000, 0.01)
	data, err := bloom.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	// Optimal m = -10000 * ln(0.01) / (ln2)^2 ~ 95851 bits ~ 11982 bytes
	// Plus 6 bytes header. Should be under 12KB.
	// The spec says "~1KB" for 10K tokens which seems to mean the filter is compact.
	// Actually m ~ 95851 bits ~ 11.7KB, so let's verify it's reasonable.
	t.Logf("bloom size for 10K tokens at 1%% FPR: %d bytes", len(data))
	if len(data) > 15000 {
		t.Errorf("bloom too large: %d bytes (expected < 15000)", len(data))
	}
}

func TestTokenBloomUnmarshalErrors(t *testing.T) {
	var bloom TokenBloom

	// Too short
	err := bloom.UnmarshalBinary([]byte{0, 1, 2})
	if err == nil {
		t.Error("expected error for short data")
	}

	// Truncated data
	err = bloom.UnmarshalBinary([]byte{7, 0, 10, 0, 0, 0}) // claims 10 words but no data
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestTokenBloomEmptyFilter(t *testing.T) {
	bloom := NewTokenBloom(0, 0.01)
	// Should not panic
	bloom.Add("test")
	if !bloom.Test("test") {
		t.Error("token added to empty-initialized bloom not found")
	}
}

func TestTokenize(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect []string
	}{
		{
			name:   "basic split",
			input:  "hello world",
			expect: []string{"hello", "world"},
		},
		{
			name:   "punctuation split",
			input:  "error:timeout at connection.reset()",
			expect: []string{"error", "timeout", "at", "connection", "reset"},
		},
		{
			name:   "lowercase",
			input:  "HTTP Error TIMEOUT",
			expect: []string{"http", "error", "timeout"},
		},
		{
			name:   "deduplication",
			input:  "error error ERROR Error",
			expect: []string{"error"},
		},
		{
			name:   "min length 2",
			input:  "a b cd ef g hi",
			expect: []string{"cd", "ef", "hi"},
		},
		{
			name:   "empty string",
			input:  "",
			expect: []string{},
		},
		{
			name:   "single char",
			input:  "x",
			expect: []string{},
		},
		{
			name:   "all punctuation",
			input:  "!@#$%^&*()",
			expect: []string{},
		},
		{
			name:   "mixed alphanumeric",
			input:  "log2023-error_code_42",
			expect: []string{"log2023", "error", "code", "42"},
		},
		{
			name:   "unicode letters",
			input:  "cafe resume naive",
			expect: []string{"cafe", "resume", "naive"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tokenize(tc.input)
			if len(tc.expect) == 0 {
				if len(got) != 0 {
					t.Errorf("expected empty, got %v", got)
				}
				return
			}
			if len(got) != len(tc.expect) {
				t.Errorf("length mismatch: got %v, want %v", got, tc.expect)
				return
			}
			for i := range tc.expect {
				if got[i] != tc.expect[i] {
					t.Errorf("token[%d]: got %q, want %q", i, got[i], tc.expect[i])
				}
			}
		})
	}
}

func TestExtractSearchTokens(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect []string
	}{
		{
			name:   "empty query",
			query:  "",
			expect: nil,
		},
		{
			name:   "bare word",
			query:  "timeout",
			expect: []string{"timeout"},
		},
		{
			name:   "bare words multiple",
			query:  "connection timeout",
			expect: []string{"connection", "timeout"},
		},
		{
			name:   "msg field quoted",
			query:  `_msg:"connection refused"`,
			expect: []string{"connection", "refused"},
		},
		{
			name:   "msg field unquoted",
			query:  `_msg:timeout`,
			expect: []string{"timeout"},
		},
		{
			name:   "body field",
			query:  `body:"internal server error"`,
			expect: []string{"internal", "server", "error"},
		},
		{
			name:   "skip keywords",
			query:  "error and timeout",
			expect: []string{"error", "timeout"},
		},
		{
			name:   "skip non-body fields",
			query:  `service:"api-gw" error`,
			expect: []string{"error"},
		},
		{
			name:   "mixed query",
			query:  `_msg:"disk full" and critical`,
			expect: []string{"disk", "full", "critical"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSearchTokens(tc.query)
			if tc.expect == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if len(got) != len(tc.expect) {
				t.Errorf("length mismatch: got %v, want %v", got, tc.expect)
				return
			}
			// Check all expected tokens are present (order may vary due to dedup)
			gotSet := make(map[string]bool, len(got))
			for _, g := range got {
				gotSet[g] = true
			}
			for _, e := range tc.expect {
				if !gotSet[e] {
					t.Errorf("expected token %q not found in result %v", e, got)
				}
			}
		})
	}
}

func TestTokenBloomSkip(t *testing.T) {
	// Build metadata for a row group
	bodies := []string{
		"connection timeout on database",
		"retry attempt failed for service",
		"disk space running low on node-3",
	}
	key, value := buildTokenBloomMetadata(bodies, 0)

	if key != "_bloom_body_rg_0" {
		t.Errorf("unexpected key: %s", key)
	}

	metadata := map[string]string{
		key: string(value),
	}

	// Search for present tokens - should NOT skip
	if tokenBloomSkip(metadata, 0, []string{"connection", "timeout"}) {
		t.Error("should not skip: all tokens present")
	}

	// Search for absent token - SHOULD skip
	if !tokenBloomSkip(metadata, 0, []string{"nonexistent_xyz_token"}) {
		t.Error("should skip: token definitely absent")
	}

	// Search for mix of present and absent - SHOULD skip (any absent means skip)
	if !tokenBloomSkip(metadata, 0, []string{"connection", "nonexistent_xyz_token"}) {
		t.Error("should skip: one token definitely absent")
	}

	// Empty search tokens - should NOT skip
	if tokenBloomSkip(metadata, 0, nil) {
		t.Error("should not skip with nil tokens")
	}
	if tokenBloomSkip(metadata, 0, []string{}) {
		t.Error("should not skip with empty tokens")
	}

	// Missing row group index - should NOT skip (no metadata)
	if tokenBloomSkip(metadata, 99, []string{"nonexistent_xyz_token"}) {
		t.Error("should not skip when metadata key missing")
	}
}

func TestBuildTokenBloomMetadata(t *testing.T) {
	bodies := []string{
		"hello world from the logging system",
		"error occurred in module auth",
	}

	key, value := buildTokenBloomMetadata(bodies, 5)

	// Verify key format
	if key != "_bloom_body_rg_5" {
		t.Errorf("wrong key format: %s", key)
	}

	// Verify value can be unmarshaled
	var bloom TokenBloom
	if err := bloom.UnmarshalBinary(value); err != nil {
		t.Fatalf("cannot unmarshal metadata value: %v", err)
	}

	// Verify tokens from bodies are present
	expectedTokens := []string{"hello", "world", "from", "the", "logging", "system",
		"error", "occurred", "in", "module", "auth"}
	for _, tok := range expectedTokens {
		if !bloom.Test(tok) {
			t.Errorf("expected token %q not found in bloom", tok)
		}
	}
}

func TestBuildTokenBloomMetadataEmpty(t *testing.T) {
	key, value := buildTokenBloomMetadata(nil, 0)
	if key != "_bloom_body_rg_0" {
		t.Errorf("wrong key: %s", key)
	}
	// Should still produce valid (empty) bloom
	var bloom TokenBloom
	if err := bloom.UnmarshalBinary(value); err != nil {
		t.Errorf("cannot unmarshal empty bloom: %v", err)
	}
}

func TestTokenBloomLargeScale(t *testing.T) {
	// Test with a larger token set to verify scaling behavior
	bloom := NewTokenBloom(5000, 0.01)
	rng := rand.New(rand.NewSource(42))

	added := make(map[string]bool, 5000)
	for i := 0; i < 5000; i++ {
		tok := fmt.Sprintf("token_%d_%d", i, rng.Intn(100000))
		bloom.Add(tok)
		added[tok] = true
	}

	// All added tokens must be found
	for tok := range added {
		if !bloom.Test(tok) {
			t.Fatalf("added token %q not found", tok)
		}
	}

	// Check FPR on absent tokens
	fp := 0
	tests := 10000
	for i := 0; i < tests; i++ {
		tok := fmt.Sprintf("missing_%d_%d", i, rng.Intn(1000000))
		if bloom.Test(tok) {
			fp++
		}
	}
	fpr := float64(fp) / float64(tests)
	t.Logf("large scale FPR: %.4f (%d/%d)", fpr, fp, tests)
	if fpr > 0.02 {
		t.Errorf("FPR too high for large scale: %.4f", fpr)
	}
}
