package parquets3

import (
	"testing"
	"time"
)

// TestParseFooterFromBytes_RejectsAbsurdFooterLength pins the traces twin
// of the fix for the logs module's FuzzParseFooterBytes hang (nightly on
// main since 2026-09-08). ParseFooterFromBytes is duplicated between the
// logs and traces modules; see the logs module's
// internal/storage/parquets3/footer_cache.go and
// footer_regression_test.go for the full root-cause writeup.
//
// Root cause: parquet-go's OpenFile independently re-parses the trailing
// 8 bytes of the footer buffer for a footer length and, when that length
// exceeds what it already has in hand, allocates a buffer sized directly
// from it (make([]byte, footerSize) in parquet-go@v0.30.1's file.go) —
// footerSize is a raw little-endian uint32 fully controlled by whoever
// produced the bytes, up to ~4 GiB. ParseFooterFromBytes now validates
// that length against the actual buffer size (and a sane maximum) before
// ever calling into parquet-go.
func TestParseFooterFromBytes_RejectsAbsurdFooterLength(t *testing.T) {
	footer := make([]byte, 64)
	// Declare a footer length of 0xFFFFFFF0 (~4 GiB) in the little-endian
	// 4-byte length field, followed by the "PAR1" magic.
	footer[len(footer)-8] = 0xF0
	footer[len(footer)-7] = 0xFF
	footer[len(footer)-6] = 0xFF
	footer[len(footer)-5] = 0xFF
	copy(footer[len(footer)-4:], []byte("PAR1"))

	start := time.Now()
	cached, file, err := ParseFooterFromBytes("absurd.parquet", footer, int64(len(footer)))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error for absurd footer length, got success")
	}
	if cached != nil || file != nil {
		t.Fatalf("expected nil results on error, got cached=%v file=%v", cached, file)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("rejecting an absurd footer length took %s, want microseconds (no allocation)", elapsed)
	}
}

// TestParseFooterFromBytes_RecoversFromDecoderPanic pins the traces twin
// of the recover-from-panic fix: a crafted footer whose thrift-decoded
// schema carries a negative (or otherwise absurd) SchemaElement.NumChildren
// makes parquet-go panic ("makeslice: len out of range" from
// make([]*Column, numChildren) in parquet-go@v0.30.1's column.go) instead
// of returning an error. footerBytes/fileSize below are the decoded form
// of the minimized crasher recorded in the logs module at
// testdata/fuzz/FuzzParseFooterBytes/2a42c33e12fefdfc (this module has no
// fuzz target for this function, so the raw bytes are reproduced here
// rather than via a shared corpus).
func TestParseFooterFromBytes_RecoversFromDecoderPanic(t *testing.T) {
	data := []byte("00000000\x150\x19<H\v00000000000\x1510\x150\x15\x800\x150\x18\x020011,,801000\x150%0\x18\x03000x0,8000\x160\x19\x1c\x19,&0\x1c\x150\x19\x150\x19\x18\x0200\x150\x160\x16\x940\x16\x940&0<60(\b00000000\x18\b0000000001,1180080011111180800&0\x1c\x150\x19\x150\x19\x18\x03000\x150\x160\x16\xe80\x16\xe80&\x9c0<60(\x0500000\x18\x0500000001\x1501\f080,1080011111111110\x16\xfc0\x160X07000000001700000000C0m0000000000000000C07000000008\x0000\b\x01\x00\x00PAR1")

	// Same decode as the logs module's footer_fuzz_test.go
	// decodeFuzzInput: first 8 bytes big-endian fileSize, rest is
	// footerBytes.
	var fileSize int64
	for i := 0; i < 8; i++ {
		fileSize = (fileSize << 8) | int64(data[i])
	}
	footerBytes := data[8:]

	// A panic here fails the test regardless of the recover in
	// ParseFooterFromBytes; the assertions below pin the converted-error
	// contract specifically.
	cached, file, err := ParseFooterFromBytes("crasher.parquet", footerBytes, fileSize)
	if err == nil {
		t.Fatalf("expected error recovering from decoder panic, got success")
	}
	if cached != nil || file != nil {
		t.Fatalf("expected nil results on error, got cached=%v file=%v", cached, file)
	}
}
