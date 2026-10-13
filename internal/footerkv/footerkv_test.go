package footerkv

import (
	"bytes"
	crand "crypto/rand"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
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

func TestCompressedRoundTrip(t *testing.T) {
	for _, raw := range [][]byte{
		{},
		{0x01},
		bytes.Repeat([]byte{0xff, 0xfe, 0x80}, 1000),
		indexLikePayload(500),
	} {
		enc := EncodeCompressed(raw)
		for i := 0; i < len(enc); i++ {
			if enc[i] >= 0x80 || enc[i] < 0x20 {
				t.Fatalf("EncodeCompressed has non-ASCII byte %#x", enc[i])
			}
		}
		got, ok := DecodeCompressed(enc)
		if !ok || !bytes.Equal(got, raw) {
			t.Fatalf("round trip of %d bytes: ok=%v got %d bytes", len(raw), ok, len(got))
		}
	}
}

// A hex-ID heavy payload (the trace index) is smaller compressed than raw, which
// is why the trace index is stored this way and the incompressible bloom is not.
func TestCompressedIsSmallerForIndexLikePayloadsAndNotForBloomBits(t *testing.T) {
	idx := indexLikePayload(2000)
	if got := len(EncodeCompressed(idx)); got >= len(idx) {
		t.Errorf("compressed index %d bytes, raw %d: compression no longer pays", got, len(idx))
	}
	noise := make([]byte, 64<<10)
	_, _ = crand.Read(noise)
	if got := len(EncodeCompressed(noise)); got <= len(Encode(noise))-len(Encode(noise))/50 {
		t.Errorf("random bytes compressed to %d vs %d plain: the bloom would pay for zstd after all", got, len(Encode(noise)))
	}
}

func TestDecodeCompressedRejects(t *testing.T) {
	raw := indexLikePayload(300)
	good := EncodeCompressed(raw)
	zframe, _ := Decode(good)
	for name, in := range map[string]string{
		"plain base64 of the payload": Encode(raw),
		"raw payload":                 string(raw),
		"raw zstd frame (not base64)": string(zframe),
		"truncated base64":            good[:len(good)-4],
		"truncated frame":             Encode(zframe[:len(zframe)/2]),
		"flipped byte":                flip(good, len(good)/2),
		"junk":                        "!!not base64!!",
		"empty":                       "",
	} {
		if got, ok := DecodeCompressed(in); ok {
			t.Errorf("%s: DecodeCompressed accepted %d bytes", name, len(got))
		}
	}
}

func flip(s string, i int) string {
	b := []byte(s)
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	return string(b)
}

// A decompression bomb: a frame a few hundred bytes long that expands past the
// bound must read as absent, and must not make the process allocate the expansion.
func TestDecodeCompressedBombIsRefusedWithoutAllocatingIt(t *testing.T) {
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	bomb := enc.EncodeAll(make([]byte, MaxDecodedBytes+(1<<20)), nil)
	if len(bomb) > 64<<10 {
		t.Fatalf("test bomb is %d bytes, expected a tiny frame", len(bomb))
	}
	val := Encode(bomb)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	if raw, ok := DecodeCompressed(val); ok {
		t.Fatalf("bomb accepted: %d bytes", len(raw))
	}
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > MaxDecodedBytes/2 {
		t.Errorf("refusing the bomb allocated %d bytes", grown)
	}
	// A frame right at the bound is fine.
	ok := enc.EncodeAll(make([]byte, 1<<20), nil)
	if raw, good := DecodeCompressed(Encode(ok)); !good || len(raw) != 1<<20 {
		t.Errorf("1 MiB zeros: ok=%v len=%d", good, len(raw))
	}
	// An encoded string over the length bound is refused before it is decoded.
	if _, ok := DecodeCompressed(strings.Repeat("A", maxEncodedBytes+4)); ok {
		t.Error("over-long value accepted")
	}
}

func FuzzDecodeCompressed(f *testing.F) {
	f.Add("")
	f.Add(EncodeCompressed(indexLikePayload(20)))
	f.Add(Encode([]byte{0x28, 0xb5, 0x2f, 0xfd, 0x00}))
	bomb, _ := zstd.NewWriter(nil)
	f.Add(Encode(bomb.EncodeAll(make([]byte, 1<<20), nil)))
	f.Fuzz(func(t *testing.T, s string) {
		raw, ok := DecodeCompressed(s) // never panics, never over the bound
		if ok && len(raw) > MaxDecodedBytes {
			t.Fatalf("decoded %d bytes", len(raw))
		}
	})
}

// The shared encoder and decoder are created on first use; goroutines racing
// there all end up with a working one.
func TestCodecFirstUseRace(t *testing.T) {
	encoder.Store(nil)
	decoder.Store(nil)
	raw := indexLikePayload(50)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, ok := DecodeCompressed(EncodeCompressed(raw))
			if !ok || !bytes.Equal(got, raw) {
				t.Error("round trip failed under first-use contention")
			}
		}()
	}
	wg.Wait()
}
