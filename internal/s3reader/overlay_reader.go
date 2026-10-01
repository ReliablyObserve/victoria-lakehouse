package s3reader

import (
	"errors"
	"io"
	"sync/atomic"
)

// OverlayReaderAt serves the tail of one immutable object (the Parquet footer
// and, when it was cached with it, the page-index stripe) from memory and
// forwards every other read to the wrapped reader. A cached file therefore
// opens with ZERO S3 round trips: parquet-go's footer read and every lazy
// ColumnIndex()/OffsetIndex() read land inside the cached tail, and only data
// ranges reach S3.
//
// Safety rules (the overlay must never change what a read returns):
//   - The cached tail must end exactly at the object size the caller passes
//     (NewOverlayReaderAt refuses otherwise). Object keys are immutable, so key
//     and size together identify the bytes; a size mismatch means the cache
//     entry belongs to another version of the key and no overlay is built.
//   - Only bytes the cache really holds are served. Bytes before the tail
//     always come from the wrapped reader — never zero-filled or synthesised
//     (a zero-filled bloom section once produced false negatives, #172). A
//     read that straddles the start of the tail is split: the front part is a
//     real read, the rest is copied from memory.
//
// It is safe for concurrent use; the tail is read-only.
type OverlayReaderAt struct {
	inner   ReaderAtSizer
	tail    []byte
	tailOff int64
	size    int64

	memBytes atomic.Int64 // bytes served from memory
	memReads atomic.Int64 // ReadAt calls fully or partly served from memory
}

// NewOverlayReaderAt returns an overlay over inner for an object of the given
// size whose last len(tail) bytes are tail. It returns nil when the tail does
// not end exactly at size (stale or foreign cache entry) or is empty.
func NewOverlayReaderAt(inner ReaderAtSizer, size int64, tail []byte) *OverlayReaderAt {
	if inner == nil || len(tail) == 0 || size <= 0 || int64(len(tail)) > size {
		return nil
	}
	return &OverlayReaderAt{inner: inner, tail: tail, tailOff: size - int64(len(tail)), size: size}
}

// Size returns the object size.
func (o *OverlayReaderAt) Size() int64 { return o.size }

// TailOffset is the object offset where the cached tail starts.
func (o *OverlayReaderAt) TailOffset() int64 { return o.tailOff }

// ServedBytes is the number of bytes served from memory so far.
func (o *OverlayReaderAt) ServedBytes() int64 { return o.memBytes.Load() }

// ServedReads is the number of reads served (at least partly) from memory.
func (o *OverlayReaderAt) ServedReads() int64 { return o.memReads.Load() }

var errOverlayNegativeOffset = errors.New("s3reader: negative offset")

// ReadAt implements io.ReaderAt.
func (o *OverlayReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errOverlayNegativeOffset
	}
	if off >= o.size {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	want := int64(len(p))
	short := false
	if off+want > o.size {
		want = o.size - off
		short = true
	}
	end := off + want

	var n int
	switch {
	case off >= o.tailOff:
		n = copy(p[:want], o.tail[off-o.tailOff:])
		o.memBytes.Add(int64(n))
		o.memReads.Add(1)
	case end <= o.tailOff:
		return o.inner.ReadAt(p, off)
	default:
		// Straddles the start of the tail: real read for the front, memory
		// for the rest.
		front := o.tailOff - off
		fn, err := o.inner.ReadAt(p[:front], off)
		if err != nil && (err != io.EOF || int64(fn) != front) {
			return fn, err
		}
		rest := copy(p[front:want], o.tail)
		o.memBytes.Add(int64(rest))
		o.memReads.Add(1)
		n = fn + rest
	}
	if short || int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}
