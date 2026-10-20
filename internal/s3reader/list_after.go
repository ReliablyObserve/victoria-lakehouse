package s3reader

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ListAfter lists the keys under prefix that sort after startAfter (empty: all
// of them), page by page, handing each to fn with its LastModified time. The
// first request carries StartAfter; later pages carry only the continuation
// token. A page that reports no more results ends the listing, whatever it
// held. Each page is one ListObjectsV2 request.
func (p *ClientPool) ListAfter(ctx context.Context, prefix, startAfter string, fn func(key string, lastModified time.Time) error) error {
	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(p.resolveBucket(prefix)),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		in.StartAfter = aws.String(startAfter)
	}
	for {
		metrics.S3RequestsTotal.Inc("ListObjectsV2")
		start := time.Now()
		var page *s3.ListObjectsV2Output
		err := retryS3(ctx, 3, func() error {
			var listErr error
			page, listErr = p.client.ListObjectsV2(ctx, in)
			return listErr
		})
		metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.S3ErrorsTotal.Inc("ListObjectsV2")
			return fmt.Errorf("s3 ListObjectsV2 %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			if o.Key == nil {
				continue
			}
			var mod time.Time
			if o.LastModified != nil {
				mod = *o.LastModified
			}
			if err := fn(*o.Key, mod); err != nil {
				return err
			}
		}
		if page.IsTruncated == nil || !*page.IsTruncated {
			return nil
		}
		next := aws.ToString(page.NextContinuationToken)
		if next == "" || next == aws.ToString(in.ContinuationToken) {
			// A page that says there is more but names no way to it (no token, an
			// empty one, or the one it was just asked with) is not a complete
			// listing: ending here would let the caller act on a partial one, and
			// asking again would never end.
			return fmt.Errorf("s3 ListObjectsV2 %s: truncated page without a new continuation token", prefix)
		}
		in = &s3.ListObjectsV2Input{
			Bucket:            in.Bucket,
			Prefix:            in.Prefix,
			ContinuationToken: page.NextContinuationToken,
		}
	}
}
