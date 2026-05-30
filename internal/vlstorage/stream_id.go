package vlstorage

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/cespare/xxhash/v2"
)

// computeStreamID returns the hex-encoded `_stream_id` value VL would
// compute for the same (tenantID, streamTagsCanonical) pair.
//
// Mirrors VL's stream ID algorithm exactly so /select/logsql/stream_ids
// and the `_stream_id` field on every queried row return the same
// values as VL would for the equivalent insert. Required by the
// 100% VL API compatibility rule (feedback_vl_vt_upstream): the
// `_stream_id` column on cold-tier rows was empty because LH's insert
// path bypasses VL's internal streamID-computing pipeline; this helper
// recovers it without modifying VL upstream.
//
// VL's algorithm (deps/VictoriaLogs/lib/logstorage):
//   - hash128.go: hi = xxhash64(canonical); lo = xxhash64(canonical + "magic!")
//   - u128.go:    marshalString = hex(hi) + hex(lo) (16 hex chars each)
//   - tenant_id.go: marshalString = hex(AccountID<<32 | ProjectID) (16 hex chars)
//   - stream_id.go: marshalString = tenantID.marshalString + u128.marshalString
//
// Output is 48 lowercase hex chars (16 tenant + 16 hi + 16 lo).
// Returns "" when streamTagsCanonical is empty (rows without a stream).
func computeStreamID(tenantID logstorage.TenantID, streamTagsCanonical string) string {
	if streamTagsCanonical == "" {
		return ""
	}

	h := xxhash.New()
	_, _ = h.Write([]byte(streamTagsCanonical))
	hi := h.Sum64()
	_, _ = h.Write([]byte("magic!"))
	lo := h.Sum64()

	tenant := uint64(tenantID.AccountID)<<32 | uint64(tenantID.ProjectID)

	var buf [48]byte
	formatHexUint64(buf[0:16], tenant)
	formatHexUint64(buf[16:32], hi)
	formatHexUint64(buf[32:48], lo)
	return string(buf[:])
}

// formatHexUint64 writes the 16-character lowercase hex
// representation of n into dst[:16]. dst must have at least 16 bytes.
// Matches VL's marshalUint64Hex byte order (most-significant byte first).
func formatHexUint64(dst []byte, n uint64) {
	const hexChars = "0123456789abcdef"
	for i := 15; i >= 0; i-- {
		dst[i] = hexChars[n&0xf]
		n >>= 4
	}
}
