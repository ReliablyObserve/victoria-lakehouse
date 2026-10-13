// Package footerkv is the single codec for Lakehouse binary payloads stored in
// the Parquet footer key_value_metadata.
//
// The Parquet thrift KeyValue.value is a string, and Arrow-rs based readers
// (Polars, DataFusion, InfluxDB 3, Databend) validate it as UTF-8 and refuse the
// whole file otherwise. Every Lakehouse payload that is binary is therefore
// stored as standard base64 (RFC 4648 section 4, padding on), which is pure
// ASCII. Two forms exist, one per entry, and they are not interchangeable:
//
//   - Encode/Decode: plain base64. The `_bloom_body_rg_<N>` token blooms use it;
//     a bloom filter's bits are close to random, so compressing them gains nothing.
//   - EncodeCompressed/DecodeCompressed: base64 of a zstd frame. The `_trace_idx`
//     trace-ID index uses it; the index is hex trace IDs and nanosecond times,
//     which compress to well under their raw size, so the footer ends up smaller
//     than the former raw-bytes encoding (and a third of plain base64).
//
// There is deliberately no fallback to an earlier encoding: a value that does
// not decode in the form its entry uses (not base64, not zstd, over the size
// bound, truncated) reads as absent, exactly like a missing entry, and callers
// then keep the file or row group, the conservative answer.
package footerkv

import (
	"encoding/base64"
	"sync/atomic"

	"github.com/klauspost/compress/zstd"
)

// MaxDecodedBytes bounds what DecodeCompressed will allocate for one value (a
// decompression-bomb guard: the footer itself is capped at 64 MiB by the footer
// reader, so a value cannot legitimately decode to more than that either). It is
// the same bound the field-presence footer facet uses.
const MaxDecodedBytes = 64 << 20

// maxEncodedBytes is the longest string DecodeCompressed looks at: the base64 of
// an incompressible payload of MaxDecodedBytes plus the zstd frame overhead.
const maxEncodedBytes = (MaxDecodedBytes + MaxDecodedBytes/8 + 1<<10) / 3 * 4

// Encode returns the footer-KV string for a binary payload: plain base64.
func Encode(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

// Decode returns the binary payload of a plain-base64 footer-KV string. ok is
// false when s is not valid standard padded base64 (including the former raw
// binary encoding, truncation and corruption); the caller must then treat the
// entry as absent. It never panics.
func Decode(s string) (raw []byte, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	return raw, true
}

var (
	encoder atomic.Pointer[zstd.Encoder]
	decoder atomic.Pointer[zstd.Decoder]
)

// level is the zstd level of EncodeCompressed.
var level = zstd.SpeedDefault

func getEncoder() *zstd.Encoder {
	if e := encoder.Load(); e != nil {
		return e
	}
	e, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(level), zstd.WithEncoderConcurrency(1))
	if !encoder.CompareAndSwap(nil, e) {
		return encoder.Load()
	}
	return e
}

// getDecoder returns the shared decoder. Its concurrency is left at the library
// default so that parallel footer reads (the trace-ID index lookup fans out over
// many files) are not serialised on one decoder.
func getDecoder() *zstd.Decoder {
	if d := decoder.Load(); d != nil {
		return d
	}
	d, _ := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxDecodedBytes))
	if !decoder.CompareAndSwap(nil, d) {
		return decoder.Load()
	}
	return d
}

// EncodeCompressed returns the footer-KV string for a binary payload that
// compresses: base64 of one zstd frame.
func EncodeCompressed(raw []byte) string {
	return base64.StdEncoding.EncodeToString(getEncoder().EncodeAll(raw, nil))
}

// DecodeCompressed returns the payload of an EncodeCompressed string. ok is
// false when s is not standard padded base64, not one valid zstd frame, longer
// than the bound, or decodes to more than MaxDecodedBytes. It never panics and
// never allocates more than MaxDecodedBytes.
func DecodeCompressed(s string) (raw []byte, ok bool) {
	if len(s) > maxEncodedBytes {
		return nil, false
	}
	z, ok := Decode(s)
	if !ok || len(z) == 0 {
		// Zero bytes hold no frame; the decoder would call that an empty payload.
		return nil, false
	}
	raw, err := getDecoder().DecodeAll(z, nil)
	if err != nil {
		return nil, false
	}
	return raw, true
}
