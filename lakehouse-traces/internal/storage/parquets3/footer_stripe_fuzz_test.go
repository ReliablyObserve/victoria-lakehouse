package parquets3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// FuzzCacheFooterFromTail drives the stripe-extracting cache writer with
// arbitrary tails: it must never panic, and on success the entry must satisfy
// the overlay's safety invariant — the kept tail ends exactly at the object
// size — and an overlay built from it must read exactly the bytes the tail
// holds.
//
// Input: 8 bytes big-endian fileSize, 8 bytes big-endian tail offset delta
// (fileSize - len(tail) is derived, the delta perturbs it), then the tail.
func FuzzCacheFooterFromTail(f *testing.F) {
	obj := logsObject(f, 3000, 1000)
	size := int64(len(obj))
	ft := footerTotal(obj)
	ps := objectStripeStart(f, obj)
	enc := func(tail []byte, fileSize, delta int64) []byte {
		out := make([]byte, 16, 16+len(tail))
		for i := 0; i < 8; i++ {
			out[i] = byte(fileSize >> (56 - 8*i))
			out[8+i] = byte(delta >> (56 - 8*i))
		}
		return append(out, tail...)
	}
	f.Add(enc(obj[size-int64(ft):], size, 0))
	f.Add(enc(obj[ps:], size, 0))
	f.Add(enc(obj[ps-100:], size, 0))
	f.Add(enc(obj[size-int64(ft):], size, 1)) // off by one: must be rejected
	f.Add(enc(obj[size-int64(ft):], size+5, 0))
	f.Add(enc(obj[:64], size, 0))
	f.Add(enc(nil, 0, 0))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 16 {
			return
		}
		var fileSize, delta int64
		for i := 0; i < 8; i++ {
			fileSize = fileSize<<8 | int64(data[i])
			delta = delta<<8 | int64(data[8+i])
		}
		tail := data[16:]
		tailOff := fileSize - int64(len(tail)) + delta%3 // mostly consistent, sometimes off
		cf, file, err := cacheFooterFromTail(context.Background(), nil, "fuzz.parquet", tail, tailOff, fileSize)
		if err != nil {
			if cf != nil || file != nil {
				t.Fatalf("error path returned a result: cf=%v file=%v", cf != nil, file != nil)
			}
			return
		}
		kept, off := cf.Tail()
		if off+int64(len(kept)) != fileSize {
			t.Fatalf("kept tail [%d,+%d) does not end at the object size %d", off, len(kept), fileSize)
		}
		if off < tailOff {
			t.Fatalf("kept tail starts at %d before the offered region %d without a fetch", off, tailOff)
		}
		if !bytes.Equal(kept, tail[off-tailOff:]) {
			t.Fatal("kept tail differs from the offered bytes")
		}
		_ = cf.HasPageIndex()
		if cf.Weight() <= int64(len(kept)) {
			t.Fatalf("weight %d does not exceed the raw tail %d", cf.Weight(), len(kept))
		}
		ov := s3reader.NewOverlayReaderAt(zeroReader{size: fileSize}, fileSize, kept)
		if ov == nil {
			t.Fatal("a valid entry must build an overlay")
		}
		got := make([]byte, len(kept))
		n, rerr := ov.ReadAt(got, off)
		if n != len(kept) || (rerr != nil && rerr != io.EOF) || !bytes.Equal(got, kept) {
			t.Fatalf("overlay read of the whole tail: n=%d err=%v", n, rerr)
		}
	})
}

type zeroReader struct{ size int64 }

func (z zeroReader) Size() int64 { return z.size }
func (z zeroReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= z.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if off+n > z.size {
		n = z.size - off
	}
	for i := int64(0); i < n; i++ {
		p[i] = 0
	}
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}
