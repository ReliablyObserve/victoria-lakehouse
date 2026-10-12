package parquetfooter

import (
	"context"
	"errors"
	"fmt"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

const (
	// MaxHint bounds a recorded tail length a reader will trust: a larger one is
	// ignored (the prefetch window is used) rather than read, so a corrupt value
	// cannot turn a footer probe into a download of the object.
	MaxHint = 128 << 20

	// lookBehindDefault is how far before an oversize footer the prefix read
	// reaches, to pick up the page-index stripe in the same request when the
	// object's tail length is not known. Measured stripe sizes are 3.4 KB (logs,
	// flush-sized objects) and 6.4 KB (traces, compacted objects); 32 KiB leaves
	// room for wide row-group counts. A larger stripe costs one more ranged read
	// where the footer cache entry is built.
	lookBehindDefault = 32 << 10
)

// LookBehind is the look-behind of the prefix read (see lookBehindDefault). A
// variable only so tests can exercise a stripe the look-behind misses.
var LookBehind int64 = lookBehindDefault

// Downloader is a ranged object read: the bytes [offset, offset+length) of key.
// kind labels the request for the S3 metrics ("footer").
type Downloader func(ctx context.Context, kind, key string, offset, length int64) ([]byte, error)

// ErrOverBudget is returned by Read when the footer is larger than the budget the
// caller was willing to spend on it. The object is not malformed: the caller just
// declined to read a footer this large (see Read).
var ErrOverBudget = errors.New("footer larger than the read budget")

// ErrNotParquet marks a trailer that is not a Parquet trailer, or a footer that
// cannot fit in the object; the bytes read are not usable as a footer.
var ErrNotParquet = errors.New("not a parquet footer")

// Tail is the end of an object: Data holds the bytes [Off, Off+len(Data)), and
// Off+len(Data) is the object size. It always holds the whole footer and its
// 8-byte trailer, and, when the stripe was in reach, the page-index stripe.
type Tail struct {
	Data []byte
	Off  int64
}

// Read fetches the end of an object so that the whole footer is in memory.
//
// window is the number of bytes a first read asks for when nothing is known
// about the object (the per-signal prefetch tail, already clamped by size);
// hint is the tail length recorded when the object was written (Info.Tail), 0
// when unknown. With a valid hint the first read is exactly that many bytes: the
// footer and the page-index stripe in one request, no more and no fewer.
//
// The first read ends with the trailer, which gives the footer length. If the
// footer did not fit (no hint, a footer larger than the window, or a hint that
// is stale), a second read fetches only the part before the first one: the
// missing head of the footer plus LookBehind bytes for the stripe. The bytes
// already read are not read again.
//
// budget, when positive, is the most the caller will spend on one footer (the
// batch prefetch passes max(64 KiB, object size / 8): an object that is mostly
// footer is better read by whatever query needs it than probed ahead of time).
// A recorded length above it returns ErrOverBudget without any request; an
// unrecorded footer found to be above it after the first request returns
// ErrOverBudget without the second. 0 means no limit (the open path, which needs
// the footer).
//
// Each request counts under lakehouse_s3_gets_by_phase_total{phase="footer"}.
func Read(ctx context.Context, dl Downloader, key string, size, window, hint, budget int64) (Tail, error) {
	if size < TrailerLen {
		return Tail{}, fmt.Errorf("%w: object %s is %d bytes", ErrNotParquet, key, size)
	}
	want := window
	if hint >= TrailerLen && hint <= size && hint <= MaxHint {
		if budget > 0 && hint > budget {
			return Tail{}, fmt.Errorf("%w: recorded tail %d of %s, budget %d", ErrOverBudget, hint, key, budget)
		}
		want = hint
	}
	want = min(max(want, TrailerLen), size)
	off := size - want

	metrics.S3GetsByPhase.Inc("footer")
	b, err := dl(ctx, "footer", key, off, want)
	if err != nil {
		return Tail{}, fmt.Errorf("download footer range: %w", err)
	}
	if int64(len(b)) != want {
		return Tail{}, fmt.Errorf("footer tail short: got %d, want %d", len(b), want)
	}
	footerLen, err := Length(b[len(b)-TrailerLen:])
	if err != nil {
		return Tail{}, fmt.Errorf("%w: %v", ErrNotParquet, err)
	}
	need := int64(footerLen) + TrailerLen
	if need <= want {
		return Tail{Data: b, Off: off}, nil
	}
	if need > size {
		return Tail{}, fmt.Errorf("%w: footer length %d exceeds object %s (%d bytes)", ErrNotParquet, footerLen, key, size)
	}
	if budget > 0 && need > budget {
		return Tail{}, fmt.Errorf("%w: footer of %s is %d bytes, budget %d", ErrOverBudget, key, need, budget)
	}

	start := max(size-need-LookBehind, 0)
	metrics.S3GetsByPhase.Inc("footer")
	head, err := dl(ctx, "footer", key, start, off-start)
	if err != nil {
		return Tail{}, fmt.Errorf("download oversize footer range: %w", err)
	}
	if int64(len(head)) != off-start {
		return Tail{}, fmt.Errorf("oversize footer fetch short: got %d, want %d", len(head), off-start)
	}
	data := make([]byte, size-start)
	copy(data, head)
	copy(data[len(head):], b)
	return Tail{Data: data, Off: start}, nil
}
