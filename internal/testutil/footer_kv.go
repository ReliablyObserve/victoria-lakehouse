package testutil

import (
	"bytes"
	"testing"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
)

// RequireFooterKVUTF8 opens a Parquet object and fails the test unless every
// footer key_value_metadata key and value is valid UTF-8. The thrift
// KeyValue.value is a string, and Arrow-rs readers (Polars, DataFusion,
// InfluxDB 3, Databend) validate it and refuse the whole file otherwise
// (issue #340). Asserting it over EVERY entry, instead of the entries known
// today, is what makes a future footer KV that stores raw bytes fail a test
// rather than an external reader. It returns the entries for further checks.
func RequireFooterKVUTF8(t testing.TB, data []byte) map[string]string {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	out := map[string]string{}
	md := f.Metadata()
	if md == nil {
		return out
	}
	for _, kv := range md.KeyValueMetadata {
		if !utf8.ValidString(kv.Key) {
			t.Errorf("footer KV key %q is not valid UTF-8; Arrow-rs readers refuse the file", kv.Key)
		}
		if !utf8.ValidString(kv.Value) {
			t.Errorf("footer KV %q value (%d bytes) is not valid UTF-8; Arrow-rs readers (Polars, DataFusion) refuse the file", kv.Key, len(kv.Value))
		}
		out[kv.Key] = kv.Value
	}
	return out
}
