package footerkv

import (
	"bytes"
	"testing"
	"unicode/utf8"
)

func TestRoundTrip(t *testing.T) {
	for _, raw := range [][]byte{
		{},
		{0x01},
		{0x01, 0x05, 0x00, 0x00, 0x00, 0x20, 0x00},
		bytes.Repeat([]byte{0xff, 0xfe, 0x80}, 1000),
	} {
		enc := Encode(raw)
		if !utf8.ValidString(enc) {
			t.Fatalf("Encode(%x) is not valid UTF-8", raw)
		}
		for i := 0; i < len(enc); i++ {
			if enc[i] >= 0x80 || enc[i] < 0x20 {
				t.Fatalf("Encode(%x) has non-ASCII byte %#x", raw, enc[i])
			}
		}
		got, ok := Decode(enc)
		if !ok || !bytes.Equal(got, raw) {
			t.Fatalf("Decode(Encode(%x)) = %x, %v", raw, got, ok)
		}
	}
}

// Anything that is not standard padded base64 reads as absent: the former raw
// binary encoding, truncation, a flipped byte, URL-safe alphabet, no padding.
func TestDecodeRejects(t *testing.T) {
	good := Encode(bytes.Repeat([]byte{0xab, 0xcd, 0xef}, 40))
	for name, in := range map[string]string{
		"raw trace index":  "\x01\x05\x00\x00\x00 \x006532437b",
		"raw high bytes":   "\xff\xfe\xfd",
		"truncated":        good[:len(good)-3],
		"bad char":         good[:10] + "*" + good[11:],
		"url-safe alpha":   "_-8=",
		"stripped padding": Encode([]byte{1, 2})[:3],
	} {
		if raw, ok := Decode(in); ok {
			t.Errorf("%s: Decode(%q) = %x, true; want false", name, in, raw)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add("")
	f.Add("AQID")
	f.Add("\x01\x05\x00\x00\x00")
	f.Add(Encode([]byte{1, 2, 3, 4}))
	f.Fuzz(func(t *testing.T, s string) {
		raw, ok := Decode(s) // must never panic
		if ok {
			// Round trip: an accepted value re-encodes to a UTF-8 string that decodes to the same bytes.
			enc := Encode(raw)
			if !utf8.ValidString(enc) {
				t.Fatalf("re-encode not UTF-8: %q", enc)
			}
			back, ok2 := Decode(enc)
			if !ok2 || !bytes.Equal(back, raw) {
				t.Fatalf("round trip mismatch for %q", s)
			}
		}
	})
}
