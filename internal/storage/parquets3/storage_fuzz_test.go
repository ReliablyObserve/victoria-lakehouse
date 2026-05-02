package parquets3

import (
	"testing"
)

func FuzzExtractExactMatch(f *testing.F) {
	f.Add(`service.name:="api-gw"`, "service.name")
	f.Add(`trace_id:="abc123"`, "trace_id")
	f.Add(`service.name:"api-gw"`, "service.name")
	f.Add(`no match here`, "service.name")
	f.Add(``, "")
	f.Add(`field:="value" AND other:="x"`, "field")
	f.Add(`field:="value" AND other:="x"`, "other")
	f.Add(`field:="unclosed`, "field")
	f.Add(`field:=""`, "field")
	f.Add(`a:="b" c:="d" e:="f"`, "c")
	f.Add("\x00\x01:=\"val\"", "\x00\x01")
	f.Add(`field:="val with \"quotes\""`, "field")

	f.Fuzz(func(t *testing.T, query, fieldName string) {
		result := extractExactMatch(query, fieldName)
		_ = result
	})
}

func FuzzIsPrintable(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x01, 0x02, 0x03})
	f.Add([]byte{'\t', '\n', '\r'})
	f.Add([]byte{0x20, 0x7e})
	f.Add([]byte{0x1f})
	f.Add([]byte("日本語"))
	f.Add([]byte{0xff, 0xfe})

	f.Fuzz(func(t *testing.T, b []byte) {
		result := isPrintable(b)
		_ = result
	})
}

