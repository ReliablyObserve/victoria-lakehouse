package s3reader

import (
	"context"
	"fmt"

	"golang.org/x/sync/singleflight"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Metadata-GET singleflight (Quickwit pattern): concurrent queries that need
// the same small metadata object (parquet footer tail, per-file .bloom
// sidecar, partition pmeta/_bloom.bin bundle) currently each issue their own
// GET. These helpers collapse the duplicates onto one in-flight GET per
// object key; every other caller waits for and shares the result.
//
// The in-flight GET runs on a context detached from the initiating caller
// (context.WithoutCancel) so one cancelled query cannot poison the shared
// result for the callers still waiting on it. Each waiter still honors its
// OWN context: a cancelled waiter unblocks immediately with ctx.Err() while
// the flight completes for the others. The underlying transport timeouts
// (ResponseHeaderTimeout + retryS3's bounded retries) bound the detached GET.
//
// Callers MUST treat the returned []byte as read-only — it is shared across
// every caller of the same flight. All current call sites only parse
// (bloomindex.Unmarshal, ParseFooterFromBytes), never mutate.

// DownloadDedup is Download with singleflight dedup, keyed by object key.
// kind labels the dedupe metric (footer | bloom | pmeta_bundle).
func (p *ClientPool) DownloadDedup(ctx context.Context, kind, key string) ([]byte, error) {
	return p.dedup(ctx, kind, key, func(c context.Context) ([]byte, error) {
		return p.Download(c, key)
	})
}

// DownloadRangeDedup is DownloadRange with singleflight dedup, keyed by
// object key + range (different ranges of the same object stay independent).
func (p *ClientPool) DownloadRangeDedup(ctx context.Context, kind, key string, offset, length int64) ([]byte, error) {
	sfKey := fmt.Sprintf("%s\x00%d:%d", key, offset, length)
	return p.dedup(ctx, kind, sfKey, func(c context.Context) ([]byte, error) {
		return p.DownloadRange(c, key, offset, length)
	})
}

func (p *ClientPool) dedup(ctx context.Context, kind, sfKey string, fetch func(context.Context) ([]byte, error)) ([]byte, error) {
	ch := p.sf.DoChan(sfKey, func() (any, error) {
		return fetch(context.WithoutCancel(ctx))
	})
	select {
	case res := <-ch:
		if res.Shared {
			metrics.S3MetaSingleflightDedup.Inc(kind)
		}
		if res.Err != nil {
			return nil, res.Err
		}
		data, _ := res.Val.([]byte)
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// sfGroup is embedded in ClientPool; declared here so the singleflight
// dependency stays local to the dedup helpers.
type sfGroup = singleflight.Group
