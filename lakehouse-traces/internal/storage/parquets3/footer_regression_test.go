package parquets3

import (
	"runtime"
	"strings"
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
// Root cause: when the declared footer length (the little-endian uint32
// in the trailing 8-byte suffix, independently re-parsed by parquet-go's
// own OpenFile) exceeds what the caller's footerBytes buffer actually
// holds, parquet-go issues a single read for the whole declared length —
// and footerReaderAt.ReadAt zero-fills whatever part of that falls in the
// synthetic "gap" region byte-by-byte. With a large fileSize the gap can
// span most of the file, so this is a multi-second, multi-GiB-touching
// loop, not (only) a bare allocation. This test uses fileSize = 1<<40
// deliberately: at a small fileSize (e.g. len(footer)) the unpatched code
// already errors fast because parquet-go's subsequent read falls straight
// off the end of the buffer (EOF), without ever exercising the gap-fill
// loop that actually hangs.
func TestParseFooterFromBytes_RejectsAbsurdFooterLength(t *testing.T) {
	footer := make([]byte, 64)
	// Declare a footer length of 0xFFFFFFF0 (~4 GiB) in the little-endian
	// 4-byte length field, followed by the "PAR1" magic.
	footer[len(footer)-8] = 0xF0
	footer[len(footer)-7] = 0xFF
	footer[len(footer)-6] = 0xFF
	footer[len(footer)-5] = 0xFF
	copy(footer[len(footer)-4:], []byte("PAR1"))

	const hugeFileSize = int64(1) << 40 // 1 TiB: large enough that the gap
	// region can absorb the whole declared length, which is what makes
	// the pre-fix gap-fill loop actually slow.

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	start := time.Now()
	cached, file, err := ParseFooterFromBytes("absurd.parquet", footer, hugeFileSize)
	elapsed := time.Since(start)

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	allocDelta := after.TotalAlloc - before.TotalAlloc

	if err == nil {
		t.Fatalf("expected error for absurd footer length, got success")
	}
	if !strings.Contains(err.Error(), "declared footer length") {
		t.Fatalf("expected error to mention declared footer length, got: %v", err)
	}
	if cached != nil || file != nil {
		t.Fatalf("expected nil results on error, got cached=%v file=%v", cached, file)
	}
	if elapsed > 50*time.Millisecond {
		t.Fatalf("rejecting an absurd footer length took %s, want microseconds (no gap-fill loop)", elapsed)
	}
	if allocDelta > 1<<20 {
		t.Fatalf("rejecting an absurd footer length allocated %d bytes, want < 1 MiB (no gap-fill buffer)", allocDelta)
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
