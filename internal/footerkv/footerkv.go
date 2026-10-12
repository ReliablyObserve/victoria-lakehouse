// Package footerkv is the single codec for Lakehouse binary payloads stored in
// the Parquet footer key_value_metadata.
//
// The Parquet thrift KeyValue.value is a string, and Arrow-rs based readers
// (Polars, DataFusion, InfluxDB 3, Databend) validate it as UTF-8 and refuse the
// whole file otherwise. Every Lakehouse payload that is binary (the `_trace_idx`
// trace-ID index and the `_bloom_body_rg_<N>` token blooms) is therefore stored
// as standard base64 (RFC 4648 section 4, padding on), which is pure ASCII.
//
// There is deliberately no fallback to the former raw-bytes encoding: a value
// that is not valid base64 reads as absent, exactly like a missing entry (callers
// then keep the file or row group, the conservative answer).
package footerkv

import "encoding/base64"

// Encode returns the footer-KV string for a binary payload.
func Encode(raw []byte) string {
	return base64.StdEncoding.EncodeToString(raw)
}

// Decode returns the binary payload of a footer-KV string. ok is false when s is
// not valid standard padded base64 (including the former raw binary encoding,
// truncation and corruption); the caller must then treat the entry as absent.
// It never panics.
func Decode(s string) (raw []byte, ok bool) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, false
	}
	return raw, true
}
