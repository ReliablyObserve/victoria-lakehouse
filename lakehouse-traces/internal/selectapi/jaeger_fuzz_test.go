package selectapi

import (
	"testing"
)

func FuzzParseTimestampNanos(f *testing.F) {
	f.Add("1714650000000000000")
	f.Add("0")
	f.Add("-1000000000")
	f.Add("2024-05-02T14:00:00.123456789Z")
	f.Add("2024-05-02T14:00:00Z")
	f.Add("2024-05-02T16:00:00+02:00")
	f.Add("")
	f.Add("not-a-timestamp")
	f.Add("2024-05-02")
	f.Add("1714650000.123")
	f.Add("9223372036854775807")
	f.Add("-9223372036854775808")
	f.Add("99999999999999999999999")

	f.Fuzz(func(t *testing.T, input string) {
		ns, ok := parseTimestampNanos(input)
		_ = ns
		_ = ok
	})
}

func FuzzSpanKindName(f *testing.F) {
	f.Add("0")
	f.Add("1")
	f.Add("2")
	f.Add("3")
	f.Add("4")
	f.Add("5")
	f.Add("")
	f.Add("999")
	f.Add("internal")

	f.Fuzz(func(t *testing.T, code string) {
		result := spanKindName(code)
		// Unknown codes return the code itself (passthrough)
		if code != "" && result == "" {
			t.Errorf("spanKindName(%q) returned empty string for non-empty input", code)
		}
	})
}
