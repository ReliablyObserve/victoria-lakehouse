package schema

import (
	"bytes"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// TraceIDHexMetaKey is the Parquet footer key/value entry in which a writer
// attests that EVERY trace_id value it wrote into the file is plain lowercase
// hex ([0-9a-f]*; an empty value counts, it stands for "no trace id"). The
// value is the ASCII byte "1" when the file attests it and "0" when it does not.
// A file without the key (written before the key existed) attests nothing.
//
// Readers use it for one thing: a phrase filter `trace_id:"P"` whose phrase is a
// full lowercase-hex token may be pruned by the exact-value blooms of an
// attested file. Upstream matches a phrase on token boundaries (matchPhrase in
// lib/logstorage/filter_phrase.go); a value made only of token runes has no
// boundary inside it, so it contains P on boundaries only when it IS P. Outside
// the attested domain (a UUID, an id as sent over OTLP/HTTP JSON, anything sent
// to /insert/native) the phrase can match a longer value and only the token
// bloom may prune it.
//
// The entry is ASCII, so the footer stays readable by every Parquet reader.
const TraceIDHexMetaKey = "lh.trace_id_hex"

// IsLowerHex reports whether s consists only of the bytes 0-9 and a-f. The
// empty string qualifies (a row without a trace id).
func IsLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// IsLowerHexToken reports whether a phrase may be pruned as an exact value in a
// file that attests TraceIDHexMetaKey: a non-empty lowercase-hex string. An
// empty phrase is excluded (upstream matches it against empty values only).
func IsLowerHexToken(s string) bool {
	return s != "" && IsLowerHex(s)
}

// LogRowsTraceIDHex reports whether every row's trace_id is lowercase hex.
func LogRowsTraceIDHex(rows []LogRow) bool {
	for i := range rows {
		if !IsLowerHex(rows[i].TraceID) {
			return false
		}
	}
	return true
}

// TraceRowsTraceIDHex reports whether every span's trace_id is lowercase hex.
func TraceRowsTraceIDHex(rows []TraceRow) bool {
	for i := range rows {
		if !IsLowerHex(rows[i].TraceID) {
			return false
		}
	}
	return true
}

// TraceIDHexOption is the writer option that records the attestation.
func TraceIDHexOption(attested bool) parquet.WriterOption {
	v := "0"
	if attested {
		v = "1"
	}
	return parquet.KeyValueMetadata(TraceIDHexMetaKey, v)
}

// FooterTraceIDHex reports whether a file's footer attests lowercase-hex
// trace ids. Only the exact value "1" attests; a missing key, "0" or anything
// else does not.
func FooterTraceIDHex(md *format.FileMetaData) bool {
	if md == nil {
		return false
	}
	for _, kv := range md.KeyValueMetadata {
		if kv.Key == TraceIDHexMetaKey {
			return kv.Value == "1"
		}
	}
	return false
}

// FileTraceIDHex reports whether the Parquet file in data attests
// lowercase-hex trace ids. A file that cannot be opened attests nothing.
func FileTraceIDHex(data []byte) bool {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return false
	}
	return FooterTraceIDHex(f.Metadata())
}

// CompactedTraceIDHex is the attestation of a file merged from inputs: every
// input file attests it (AND), and every row written attests it. The first
// term keeps a file whose writer never attested (written before the key
// existed) from passing an attestation on; the second keeps the output true to
// its own rows, whatever happened to them in the merge.
func CompactedTraceIDHex(inputs [][]byte, rowsHex bool) bool {
	if !rowsHex {
		return false
	}
	for _, data := range inputs {
		if !FileTraceIDHex(data) {
			return false
		}
	}
	return true
}
