package parquets3

import (
	"testing"
)

func TestExtractExactMatch_TableDriven(t *testing.T) {
	tests := []struct {
		name  string
		query string
		field string
		want  string
	}{
		{"exact with :=", `service.name:="api-gw"`, "service.name", "api-gw"},
		{"exact with :", `service.name:"api-gw"`, "service.name", "api-gw"},
		{"no match", "no match here", "service.name", ""},
		{"empty query", "", "service.name", ""},
		{"empty field matches prefix", `field:="val"`, "", "val"},
		{"both empty", "", "", ""},
		{"multiple fields", `a:="x" AND b:="y"`, "b", "y"},
		{"unclosed quote :=", `field:="unclosed`, "field", ""},
		{"unclosed quote :", `field:"unclosed`, "field", ""},
		{"empty value :=", `field:=""`, "field", ""},
		{"empty value :", `field:""`, "field", ""},
		{"field at start", `trace_id:="abc123"`, "trace_id", "abc123"},
		{"field not present", `other:="val"`, "missing", ""},
		{"prefix collision", `service.name_extra:="val"`, "service.name", ""},
		{"value with spaces", `field:="hello world"`, "field", "hello world"},
		{"value with special chars", `field:="a=b:c/d"`, "field", "a=b:c/d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractExactMatch(tt.query, tt.field)
			if got != tt.want {
				t.Errorf("extractExactMatch(%q, %q) = %q, want %q", tt.query, tt.field, got, tt.want)
			}
		})
	}
}

func TestIsPrintable_TableDriven(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  bool
	}{
		{"empty", []byte{}, true},
		{"ascii text", []byte("hello world"), true},
		{"tab", []byte{'\t'}, true},
		{"newline", []byte{'\n'}, true},
		{"carriage return", []byte{'\r'}, true},
		{"null byte", []byte{0x00}, false},
		{"bell", []byte{0x07}, false},
		{"control char 0x01", []byte{0x01}, false},
		{"control char 0x1f", []byte{0x1f}, false},
		{"space (0x20)", []byte{0x20}, true},
		{"mixed printable and control", []byte("hello\x01world"), false},
		{"utf8 multibyte", []byte("日本語"), true},
		{"high bytes", []byte{0xff, 0xfe}, true},
		{"mixed tabs and text", []byte("col1\tcol2\ncol3"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isPrintable(tt.input)
			if got != tt.want {
				t.Errorf("isPrintable(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

