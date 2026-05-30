package vlstorage

import (
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/cespare/xxhash/v2"
)

// TestComputeStreamID_FormatMatchesVL verifies that the helper output
// is a 48-character lowercase hex string (16 tenant + 16 hi + 16 lo)
// — exactly the shape VL produces from sid.marshalString.
func TestComputeStreamID_FormatMatchesVL(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 0, ProjectID: 0}
	canonical := "fake canonical bytes"

	got := computeStreamID(tenant, canonical)
	if len(got) != 48 {
		t.Errorf("len = %d, want 48", len(got))
	}
	for i, c := range got {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Errorf("char[%d] = %q, want lowercase hex", i, c)
		}
	}
}

// TestComputeStreamID_Empty returns empty for empty canonical.
func TestComputeStreamID_Empty(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
	if got := computeStreamID(tenant, ""); got != "" {
		t.Errorf("empty canonical should yield empty stream_id; got %q", got)
	}
}

// TestComputeStreamID_Deterministic two calls with same input return
// the same string.
func TestComputeStreamID_Deterministic(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 7, ProjectID: 42}
	canonical := "stream-tag-bytes-here"

	a := computeStreamID(tenant, canonical)
	b := computeStreamID(tenant, canonical)
	if a != b {
		t.Errorf("non-deterministic: %q vs %q", a, b)
	}
}

// TestComputeStreamID_TenantSensitive different tenants produce
// different outputs (matching VL's behavior — tenant ID is the first
// 16 hex chars).
func TestComputeStreamID_TenantSensitive(t *testing.T) {
	canonical := "same-canonical"
	t1 := computeStreamID(logstorage.TenantID{AccountID: 1, ProjectID: 1}, canonical)
	t2 := computeStreamID(logstorage.TenantID{AccountID: 2, ProjectID: 1}, canonical)
	if t1 == t2 {
		t.Error("different tenants should produce different stream_ids")
	}
	// Last 32 chars (the u128 hash part) must match — same canonical
	// data hashes to the same value regardless of tenant.
	if t1[16:] != t2[16:] {
		t.Errorf("hash portion should match: t1=%q t2=%q", t1, t2)
	}
}

// TestComputeStreamID_CanonicalSensitive different canonical strings
// produce different hashes.
func TestComputeStreamID_CanonicalSensitive(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 0, ProjectID: 0}
	a := computeStreamID(tenant, "stream-a")
	b := computeStreamID(tenant, "stream-b")
	if a == b {
		t.Error("different canonical inputs should produce different stream_ids")
	}
	// Tenant prefix should match (both are tenant 0/0).
	if a[:16] != b[:16] {
		t.Errorf("tenant prefix should match: a=%q b=%q", a, b)
	}
}

// TestComputeStreamID_LooksLikeVLOutput verifies the output is
// lowercase hex. VL's marshalUint64Hex uses lowercase, so any client
// that string-compares stream_id values from LH against VL-generated
// ones gets a match.
func TestComputeStreamID_LooksLikeVLOutput(t *testing.T) {
	tenant := logstorage.TenantID{AccountID: 0, ProjectID: 0}
	got := computeStreamID(tenant, "x")
	if strings.ToLower(got) != got {
		t.Errorf("expected lowercase output, got %q", got)
	}
}

// TestComputeStreamID_MatchesVLAlgorithm regression-locks the stream-id
// algorithm by re-computing the expected value using the SAME primitive
// (xxhash64 with the "magic!" suffix) that VL's logstorage.hash128
// uses internally, then comparing byte-for-byte to computeStreamID.
//
// This is the only way to catch a future drift between LH and VL's
// hash without making logstorage.hash128 public. If anyone "optimizes"
// computeStreamID to use a different hash, drops the magic suffix,
// changes the byte order, or alters the tenant encoding, this test
// fails immediately.
//
// To verify the test is a true lock: temporarily edit
// stream_id.go to remove the `_, _ = h.Write([]byte("magic!"))` line
// or change the tenant encoding — this test MUST fail. If it still
// passes, the test isn't pinning the contract.
func TestComputeStreamID_MatchesVLAlgorithm(t *testing.T) {
	cases := []struct {
		name            string
		tenant          logstorage.TenantID
		streamCanonical string
	}{
		{"tenant zero / simple stream", logstorage.TenantID{AccountID: 0, ProjectID: 0}, "{a=\"b\"}"},
		{"tenant 1/2 / nested stream", logstorage.TenantID{AccountID: 1, ProjectID: 2}, "{service.name=\"api\",pod=\"x-1\"}"},
		{"large tenant / long canonical", logstorage.TenantID{AccountID: 0x7fff_ffff, ProjectID: 0x1234_5678}, strings.Repeat("k=\"v\",", 32)},
		{"unicode canonical", logstorage.TenantID{AccountID: 7, ProjectID: 42}, "{lbl=\"ñ→★\"}"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Re-implement VL's hash128 inline using the same primitives
			// VL itself uses (xxhash + "magic!" suffix) and the same
			// 48-char layout (tenant + hi + lo, all lowercase hex).
			h := xxhash.New()
			_, _ = h.Write([]byte(tc.streamCanonical))
			hi := h.Sum64()
			_, _ = h.Write([]byte("magic!"))
			lo := h.Sum64()

			tenant := uint64(tc.tenant.AccountID)<<32 | uint64(tc.tenant.ProjectID)
			expected := hex64(tenant) + hex64(hi) + hex64(lo)

			got := computeStreamID(tc.tenant, tc.streamCanonical)
			if got != expected {
				t.Errorf("computeStreamID drift\n  got      %q\n  expected %q\n  tenant=%+v canonical=%q",
					got, expected, tc.tenant, tc.streamCanonical)
			}
		})
	}
}

// hex64 returns the 16-character lowercase hex encoding of n,
// matching VL's marshalUint64Hex byte order (most-significant first).
func hex64(n uint64) string {
	const hexChars = "0123456789abcdef"
	var buf [16]byte
	for i := 15; i >= 0; i-- {
		buf[i] = hexChars[n&0xf]
		n >>= 4
	}
	return string(buf[:])
}
