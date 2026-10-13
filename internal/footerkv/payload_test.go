package footerkv

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	mrand "math/rand"
	"sort"
)

// indexLikePayload is n entries in the layout of the trace index: a 16-bit ID
// length, the hex ID, a 16-bit partition and two 64-bit nanosecond times.
func indexLikePayload(n int) []byte {
	rng := mrand.New(mrand.NewSource(1))
	b := make([]byte, 5, 5+n*52)
	b[0] = 1
	binary.LittleEndian.PutUint32(b[1:], uint32(n))
	now := int64(1_760_000_000_000_000_000)
	// traceindex.Compute emits the entries sorted by trace ID, which is what makes
	// the shared hex prefixes compress; a benchmark on unsorted IDs would overstate
	// the cost of this footer value.
	ids := make([]string, n)
	for i := range ids {
		var id [16]byte
		_, _ = rand.Read(id[:])
		ids[i] = hex.EncodeToString(id[:])
	}
	sort.Strings(ids)
	for i := 0; i < n; i++ {
		b = binary.LittleEndian.AppendUint16(b, 32)
		b = append(b, ids[i]...)
		b = binary.LittleEndian.AppendUint16(b, uint16(rng.Intn(1024)))
		start := now - int64(rng.Intn(3600))*1_000_000_000 - int64(rng.Intn(1_000_000_000))
		b = binary.LittleEndian.AppendUint64(b, uint64(start))
		b = binary.LittleEndian.AppendUint64(b, uint64(start+1_000_000+int64(rng.Intn(2_000_000_000))))
	}
	return b
}
