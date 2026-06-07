package parquets3

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// fuzzTestRow is a tiny row schema used to synthesize a real parquet footer
// for the seed corpus. We don't reuse testRowWide here to keep this file
// independent of the rest of the test file's setup.
type fuzzTestRow struct {
	TS  int64  `parquet:"ts"`
	Msg string `parquet:"msg"`
}

// buildSeedFooterBytes synthesizes a real parquet file in memory and returns
// (footerSlice, fileSize). footerSlice covers footerLen + 8 trailing bytes —
// the same shape the prefetcher feeds to ParseFooterFromBytes.
func buildSeedFooterBytes(tb testing.TB) ([]byte, int64) {
	tb.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[fuzzTestRow](&buf)
	rows := make([]fuzzTestRow, 32)
	for i := range rows {
		rows[i] = fuzzTestRow{TS: int64(1000 + i), Msg: "hello"}
	}
	if _, err := w.Write(rows); err != nil {
		tb.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}
	data := buf.Bytes()
	fileSize := int64(len(data))
	footerLen, err := FooterLength(data[len(data)-8:])
	if err != nil {
		tb.Fatalf("read footer len: %v", err)
	}
	footerSlice := data[len(data)-(footerLen+8):]
	out := make([]byte, len(footerSlice))
	copy(out, footerSlice)
	return out, fileSize
}

// fuzzInput packs (footerBytes, fileSize) into a single []byte so we can use
// the simple f.Add([]byte) corpus shape that matches bloomindex's style.
// Encoding: first 8 bytes big-endian fileSize, rest is footerBytes.
// Decoding tolerates any input length (returns 0 fileSize on short input).
func encodeFuzzInput(footer []byte, fileSize int64) []byte {
	out := make([]byte, 8+len(footer))
	for i := 0; i < 8; i++ {
		out[i] = byte(fileSize >> (56 - i*8))
	}
	copy(out[8:], footer)
	return out
}

func decodeFuzzInput(data []byte) ([]byte, int64) {
	if len(data) < 8 {
		return data, 0
	}
	var fileSize int64
	for i := 0; i < 8; i++ {
		fileSize = (fileSize << 8) | int64(data[i])
	}
	return data[8:], fileSize
}

func FuzzParseFooterBytes(f *testing.F) {
	footer, fileSize := buildSeedFooterBytes(f)

	// Valid baseline.
	f.Add(encodeFuzzInput(footer, fileSize))

	// Truncated valid footer at fixed offsets.
	if len(footer) > 1 {
		f.Add(encodeFuzzInput(footer[:len(footer)-1], fileSize))
	}
	if len(footer) > 4 {
		f.Add(encodeFuzzInput(footer[:len(footer)-4], fileSize))
	}
	if len(footer) > 16 {
		f.Add(encodeFuzzInput(footer[:len(footer)-16], fileSize))
	}
	if len(footer) > 1 {
		f.Add(encodeFuzzInput(footer[:len(footer)/2], fileSize))
	}

	// Magic-looking-but-not-thrift: "PAR1" magic at the tail, but with
	// random bytes claiming a thrift footer in front.
	fakeMagic := make([]byte, 64)
	for i := range fakeMagic {
		fakeMagic[i] = 0xAB
	}
	// Footer-length field (little-endian) declares 52 bytes of footer.
	fakeMagic[len(fakeMagic)-8] = 52
	fakeMagic[len(fakeMagic)-7] = 0
	fakeMagic[len(fakeMagic)-6] = 0
	fakeMagic[len(fakeMagic)-5] = 0
	// PAR1 magic at the end.
	copy(fakeMagic[len(fakeMagic)-4:], []byte("PAR1"))
	f.Add(encodeFuzzInput(fakeMagic, int64(1024)))

	// All-zero 1 KiB buffer.
	zeros := make([]byte, 1024)
	f.Add(encodeFuzzInput(zeros, 1024))

	// 1 MiB random buffer.
	rng := rand.New(rand.NewSource(0xC0FFEE))
	big := make([]byte, 1<<20)
	_, _ = rng.Read(big)
	f.Add(encodeFuzzInput(big, int64(len(big))))

	// Degenerate sizes.
	f.Add(encodeFuzzInput([]byte{}, 0))
	f.Add(encodeFuzzInput([]byte{}, -1))
	f.Add(encodeFuzzInput([]byte("PAR1"), 4))
	f.Add(encodeFuzzInput([]byte("PAR1PAR1"), 8))

	f.Fuzz(func(t *testing.T, data []byte) {
		footerBytes, fileSize := decodeFuzzInput(data)

		// Must never panic. Error is fine.
		cached, file, err := ParseFooterFromBytes("fuzz.parquet", footerBytes, fileSize)
		if err != nil {
			if cached != nil || file != nil {
				t.Fatalf("error path returned non-nil result: cached=%v file=%v", cached, file)
			}
			return
		}

		// Success path: the contract is that the caller can read
		// KeyValueMetadata without panic — that's the trace-idx access path.
		if file == nil {
			t.Fatalf("nil file on success")
		}
		md := file.Metadata()
		if md == nil {
			// parquet-go always returns non-nil; tolerate but skip the kv probe.
			return
		}
		// Walk KeyValueMetadata — this is the exact access pattern used
		// by trace_index lookup of _trace_idx.
		for _, kv := range md.KeyValueMetadata {
			_ = kv.Key
			_ = kv.Value
		}
		// Also exercise row-group and column-chunk traversal since callers
		// iterate these without nil checks.
		for _, rg := range file.RowGroups() {
			_ = rg.NumRows()
			for _, cc := range rg.ColumnChunks() {
				_ = cc.Column()
			}
		}
	})
}

// FuzzFooterLength exercises the tiny 8-byte length parser. It already
// guards on len < 8 but a fuzz pass costs nothing and matches the
// "panic-safe against arbitrary bytes" project policy.
func FuzzFooterLength(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 'P', 'A', 'R', '1'})
	f.Add([]byte{255, 255, 255, 255, 'P', 'A', 'R', '1'})
	f.Add([]byte{1, 0, 0, 0, 'X', 'X', 'X', 'X'})
	f.Add(make([]byte, 7))
	f.Add(make([]byte, 8))
	f.Add(make([]byte, 32))

	f.Fuzz(func(t *testing.T, tail []byte) {
		// Must not panic.
		_, _ = FooterLength(tail)
	})
}
