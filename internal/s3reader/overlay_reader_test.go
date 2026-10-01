package s3reader

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

// countingObject is an in-memory object that counts the reads (and bytes)
// that reach it — the stand-in for "S3".
type countingObject struct {
	data  []byte
	reads atomic.Int64
	bytes atomic.Int64
	mu    sync.Mutex
	log   [][2]int64 // off, len
}

func (c *countingObject) Size() int64 { return int64(len(c.data)) }

func (c *countingObject) ReadAt(p []byte, off int64) (int, error) {
	c.reads.Add(1)
	c.mu.Lock()
	c.log = append(c.log, [2]int64{off, int64(len(p))})
	c.mu.Unlock()
	if off >= int64(len(c.data)) {
		return 0, io.EOF
	}
	n := copy(p, c.data[off:])
	c.bytes.Add(int64(n))
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func newObject(n int, seed int64) *countingObject {
	d := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(d)
	return &countingObject{data: d}
}

func TestOverlay_RefusesMismatchedTail(t *testing.T) {
	obj := newObject(1000, 1)
	if NewOverlayReaderAt(obj, 1000, nil) != nil {
		t.Fatal("empty tail must not build an overlay")
	}
	if NewOverlayReaderAt(obj, 0, []byte{1}) != nil {
		t.Fatal("zero size must not build an overlay")
	}
	if NewOverlayReaderAt(obj, 10, make([]byte, 11)) != nil {
		t.Fatal("tail longer than the object must not build an overlay")
	}
	if NewOverlayReaderAt(nil, 10, []byte{1}) != nil {
		t.Fatal("nil inner must not build an overlay")
	}
	if ov := NewOverlayReaderAt(obj, 1000, obj.data[900:]); ov == nil || ov.TailOffset() != 900 || ov.Size() != 1000 {
		t.Fatalf("valid overlay not built: %+v", ov)
	}
}

// Section boundaries: reads entirely before, exactly at, straddling and
// entirely inside the cached tail, and past EOF, all byte-equal to the object.
func TestOverlay_SectionBoundaries(t *testing.T) {
	const size, tailLen = 4096, 700
	obj := newObject(size, 2)
	tailOff := int64(size - tailLen)
	ov := NewOverlayReaderAt(obj, size, append([]byte(nil), obj.data[tailOff:]...))

	cases := []struct {
		name         string
		off          int64
		n            int
		wantInnerGet bool
		wantEOF      bool
	}{
		{"before tail", 0, 100, true, false},
		{"ends exactly at tail start", tailOff - 50, 50, true, false},
		{"starts exactly at tail start", tailOff, 100, false, false},
		{"straddles tail start", tailOff - 30, 80, true, false},
		{"inside tail", tailOff + 10, 50, false, false},
		{"ends exactly at EOF", size - 64, 64, false, false},
		{"runs past EOF", size - 10, 64, false, true},
		{"straddle and past EOF", tailOff - 20, tailLen + 100, true, true},
		{"starts at EOF", size, 8, false, true},
		{"starts past EOF", size + 100, 8, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := obj.reads.Load()
			p := make([]byte, c.n)
			n, err := ov.ReadAt(p, c.off)
			if c.wantEOF {
				if err != io.EOF {
					t.Fatalf("want io.EOF, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := 0
			if c.off < size {
				want = copy(make([]byte, c.n), obj.data[c.off:])
			}
			if n != want {
				t.Fatalf("n = %d, want %d", n, want)
			}
			if n > 0 && !bytes.Equal(p[:n], obj.data[c.off:c.off+int64(n)]) {
				t.Fatal("overlay returned different bytes than the object")
			}
			got := obj.reads.Load() - before
			if c.wantInnerGet && got != 1 {
				t.Fatalf("inner reads = %d, want 1", got)
			}
			if !c.wantInnerGet && got != 0 {
				t.Fatalf("inner reads = %d, want 0 (served from memory)", got)
			}
		})
	}
}

// A straddling read only asks the wrapped reader for the part BEFORE the
// cached tail — never the cached part again, and never synthesised bytes.
func TestOverlay_StraddleFetchesOnlyTheFront(t *testing.T) {
	obj := newObject(1000, 3)
	ov := NewOverlayReaderAt(obj, 1000, append([]byte(nil), obj.data[800:]...))
	p := make([]byte, 100)
	if _, err := ov.ReadAt(p, 750); err != nil {
		t.Fatal(err)
	}
	if len(obj.log) != 1 || obj.log[0] != [2]int64{750, 50} {
		t.Fatalf("inner read log = %v, want exactly [750,+50]", obj.log)
	}
	if !bytes.Equal(p, obj.data[750:850]) {
		t.Fatal("straddling read returned wrong bytes")
	}
}

// Random ranges against the object: the overlay must be indistinguishable
// from reading the object directly (the property the safety rule rests on).
func TestOverlay_RandomRangesByteEqualToObject(t *testing.T) {
	const size = 50_000
	obj := newObject(size, 4)
	for _, tailLen := range []int{1, 8, 777, 25_000, size} {
		ov := NewOverlayReaderAt(obj, size, append([]byte(nil), obj.data[size-tailLen:]...))
		rnd := rand.New(rand.NewSource(int64(tailLen)))
		for i := 0; i < 3000; i++ {
			off := rnd.Int63n(size + 200)
			n := rnd.Intn(5000)
			got := make([]byte, n)
			want := make([]byte, n)
			gn, gerr := ov.ReadAt(got, off)
			wn, werr := obj.ReadAt(want, off)
			if gn != wn || (gerr == nil) != (werr == nil) || !bytes.Equal(got[:gn], want[:wn]) {
				t.Fatalf("tail=%d off=%d n=%d: overlay (%d,%v) != object (%d,%v)", tailLen, off, n, gn, gerr, wn, werr)
			}
		}
	}
}

func TestOverlay_NegativeOffsetAndEmptyRead(t *testing.T) {
	obj := newObject(100, 5)
	ov := NewOverlayReaderAt(obj, 100, obj.data[50:])
	if _, err := ov.ReadAt(make([]byte, 4), -1); err == nil {
		t.Fatal("negative offset must error")
	}
	if n, err := ov.ReadAt(nil, 10); n != 0 || err != nil {
		t.Fatalf("empty read = (%d,%v)", n, err)
	}
}

func TestOverlay_InnerErrorPropagates(t *testing.T) {
	boom := errors.New("boom")
	ov := NewOverlayReaderAt(failingReader{size: 100, err: boom}, 100, make([]byte, 20))
	if _, err := ov.ReadAt(make([]byte, 10), 0); !errors.Is(err, boom) {
		t.Fatalf("front read error = %v, want boom", err)
	}
	if _, err := ov.ReadAt(make([]byte, 40), 60); !errors.Is(err, boom) {
		t.Fatalf("straddling read error = %v, want boom", err)
	}
	if n, err := ov.ReadAt(make([]byte, 10), 85); n != 10 || err != nil {
		t.Fatalf("read inside the tail must not touch the failing inner: (%d,%v)", n, err)
	}
}

type failingReader struct {
	size int64
	err  error
}

func (f failingReader) Size() int64                       { return f.size }
func (f failingReader) ReadAt([]byte, int64) (int, error) { return 0, f.err }

func TestOverlay_ConcurrentReads(t *testing.T) {
	const size = 20_000
	obj := newObject(size, 6)
	ov := NewOverlayReaderAt(obj, size, append([]byte(nil), obj.data[size-5000:]...))
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(g)))
			for i := 0; i < 500; i++ {
				off := rnd.Int63n(size)
				p := make([]byte, 1+rnd.Intn(3000))
				n, _ := ov.ReadAt(p, off)
				if !bytes.Equal(p[:n], obj.data[off:off+int64(n)]) {
					t.Errorf("goroutine %d: wrong bytes at %d", g, off)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if ov.ServedBytes() == 0 || ov.ServedReads() == 0 {
		t.Fatal("expected some reads to be served from memory")
	}
}

// The manifest size says 1000 bytes but the object behind the key is shorter
// (truncated or replaced). A read that straddles the cached tail must fail
// with the inner short read: it must never return bytes of the cached tail
// after a gap of unread (zero) bytes.
func TestOverlay_StraddleOverShortInnerIsAnError(t *testing.T) {
	const size, tailLen, innerLen = 1000, 200, 700 // tail starts at 800; the object ends at 700
	full := newObject(size, 5)
	inner := &countingObject{data: append([]byte(nil), full.data[:innerLen]...)}
	ov := NewOverlayReaderAt(inner, size, append([]byte(nil), full.data[size-tailLen:]...))

	p := make([]byte, 300) // [750, 1050): front [750, 800) is past the short object
	n, err := ov.ReadAt(p, 750)
	if err == nil {
		t.Fatalf("straddling read over a short object returned no error (n=%d)", n)
	}
	if n > 0 {
		t.Fatalf("straddling read over a short object returned %d bytes, want 0 (object ends at %d, read starts at 750)", n, innerLen)
	}

	// Front partly available: 50 bytes exist ([700-50, 700)) then EOF.
	p = make([]byte, 300)
	n, err = ov.ReadAt(p, 650) // front [650, 800): only [650, 700) exists
	if err == nil {
		t.Fatalf("partial front read returned no error (n=%d)", n)
	}
	if n > innerLen-650 {
		t.Fatalf("partial front read returned %d bytes, more than the %d the object has", n, innerLen-650)
	}
	if !bytes.Equal(p[:n], full.data[650:650+n]) {
		t.Fatal("partial front read returned wrong bytes")
	}
}

// FuzzOverlayReadAt: for any object, tail length, offset and length, the
// overlay returns exactly what the object returns.
func FuzzOverlayReadAt(f *testing.F) {
	f.Add(uint16(1000), uint16(200), uint16(0), uint16(300))
	f.Add(uint16(1000), uint16(200), uint16(900), uint16(300))
	f.Add(uint16(1000), uint16(1000), uint16(0), uint16(2000))
	f.Add(uint16(10), uint16(1), uint16(9), uint16(5))
	f.Add(uint16(500), uint16(100), uint16(450), uint16(10))
	f.Fuzz(func(t *testing.T, size, tailLen, off, n uint16) {
		if size == 0 || tailLen == 0 || tailLen > size {
			return
		}
		obj := newObject(int(size), int64(size)*31+int64(tailLen))
		ov := NewOverlayReaderAt(obj, int64(size), append([]byte(nil), obj.data[int(size)-int(tailLen):]...))
		got, want := make([]byte, n), make([]byte, n)
		gn, gerr := ov.ReadAt(got, int64(off))
		wn, werr := obj.ReadAt(want, int64(off))
		if gn != wn || (gerr == nil) != (werr == nil) || !bytes.Equal(got[:gn], want[:wn]) {
			t.Fatalf("size=%d tail=%d off=%d n=%d: overlay (%d,%v) != object (%d,%v)", size, tailLen, off, n, gn, gerr, wn, werr)
		}
	})
}
