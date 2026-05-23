package s3reader

import (
	"io"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ReaderAtSizer combines io.ReaderAt with a Size method,
// matching the interface already implemented by S3ReaderAt.
type ReaderAtSizer interface {
	io.ReaderAt
	Size() int64
}

// BufferedS3ReaderAt wraps a ReaderAtSizer with a read-ahead buffer.
// Sequential reads within the prefetch window are served from the buffer,
// reducing the number of underlying S3 GetObject calls.
type BufferedS3ReaderAt struct {
	inner    ReaderAtSizer
	fileSize int64
	prefetch int64

	mu       sync.Mutex
	buf      []byte
	bufStart int64
	bufEnd   int64
}

// NewBufferedReaderAt creates a BufferedS3ReaderAt wrapping inner.
// prefetch controls the read-ahead window size in bytes (default 2MB if <= 0).
func NewBufferedReaderAt(inner ReaderAtSizer, fileSize int64, prefetch int64) *BufferedS3ReaderAt {
	if prefetch <= 0 {
		prefetch = 2 * 1024 * 1024
	}
	const maxPrefetch = 64 * 1024 * 1024 // 64MB safety cap
	if prefetch > maxPrefetch {
		prefetch = maxPrefetch
	}
	return &BufferedS3ReaderAt{
		inner:    inner,
		fileSize: fileSize,
		prefetch: prefetch,
		bufStart: -1,
		bufEnd:   -1,
	}
}

// ReadAt reads len(p) bytes from the underlying source at byte offset off.
// It returns the number of bytes read and any error encountered.
// Reads that fall within the current buffer are served without an underlying call.
func (b *BufferedS3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= b.fileSize {
		return 0, io.EOF
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	reqEnd := off + int64(len(p))

	// Buffer hit: requested range is fully contained in the buffer.
	if b.bufStart >= 0 && off >= b.bufStart && reqEnd <= b.bufEnd {
		metrics.S3BufferHits.Inc()
		n := copy(p, b.buf[off-b.bufStart:reqEnd-b.bufStart])
		return n, nil
	}

	// Buffer miss: fetch a new window starting at off.
	// Use max(prefetch, len(p)) so a single fetch always covers the request.
	metrics.S3BufferMisses.Inc()
	fetchSize := b.prefetch
	if int64(len(p)) > fetchSize {
		fetchSize = int64(len(p))
	}
	fetchEnd := off + fetchSize
	if fetchEnd > b.fileSize {
		fetchEnd = b.fileSize
	}

	fetchBuf := make([]byte, fetchEnd-off)
	n, err := b.inner.ReadAt(fetchBuf, off)
	if err != nil && err != io.EOF {
		return 0, err
	}

	b.buf = fetchBuf[:n]
	b.bufStart = off
	b.bufEnd = off + int64(n)

	copyEnd := reqEnd
	if copyEnd > b.bufEnd {
		copyEnd = b.bufEnd
	}
	copied := copy(p, b.buf[:copyEnd-off])
	// Only return io.EOF when we actually hit the end of the file.
	// Returning io.EOF for a mid-file short read violates io.ReaderAt's contract.
	if b.bufEnd < b.fileSize {
		return copied, nil
	}
	if copyEnd < reqEnd {
		return copied, io.EOF
	}
	return copied, nil
}

// Size returns the total file size.
func (b *BufferedS3ReaderAt) Size() int64 {
	return b.fileSize
}
