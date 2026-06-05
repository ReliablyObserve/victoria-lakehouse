package s3reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type S3ReaderAt struct {
	client *s3.Client
	bucket string
	key    string
	size   int64
	ctx    context.Context
}

// retryS3 retries fn on transient S3 errors (503 SlowDown,
// ServiceUnavailable, InternalError, RequestTimeout, connection
// reset, i/o timeout). Backoff schedule is exponential with FULL
// JITTER per Marc Brooker's "Exponential Backoff and Jitter":
//
//	sleep = rand[0, min(cap, base * 2^attempt))
//
// Full jitter (vs equal jitter or none) is the right default when
// multiple peers retry the same throttled call — without jitter,
// all 6 peers' retries land at the exact same offset and keep
// hitting the rate limit. With full jitter, the retry cloud
// spreads uniformly across the backoff window so the rate limit
// recovers between waves. At 5 s cap, 5 retries → max ~155 s
// total wait worst-case, mean ~77 s.
//
// Metrics: every retry attempt increments S3ThrottleTotal; every
// successful retry (i.e. fn returned nil on attempt > 0) is
// observable as the gap between throttle count and final error
// count.
func retryS3(ctx context.Context, maxRetries int, fn func() error) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isRetryable(lastErr) {
			return lastErr
		}
		if i < maxRetries {
			metrics.S3ThrottleTotal.Inc()
			cap := 5 * time.Second
			base := time.Duration(1<<uint(i)) * 100 * time.Millisecond
			if base > cap {
				base = cap
			}
			// Full jitter: pick anywhere in [0, base). Use a
			// fresh random source each call rather than the
			// shared mrand global — concurrent goroutines hitting
			// the same source would serialize on the mutex,
			// defeating the spread we want from jitter.
			// #nosec G404 -- non-crypto jitter for retry backoff
			jitter := time.Duration(rand.Int63n(int64(base) + 1))
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return lastErr
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "SlowDown", "ServiceUnavailable", "InternalError", "RequestTimeout":
			return true
		}
	}
	return strings.Contains(err.Error(), "connection reset") ||
		strings.Contains(err.Error(), "i/o timeout")
}

// BucketRouterFunc returns the S3 bucket that holds the object at
// the given key. Empty string = use the pool's default bucket.
// Installed on a single pool to route per-call reads (and the
// occasional non-tenant-aware write) to a tenant's bucket when
// bucket isolation is configured. The writer continues to choose
// buckets explicitly via TenantBucketFunc on flush.
type BucketRouterFunc func(key string) string

type ClientPool struct {
	client *s3.Client
	bucket string
	router BucketRouterFunc
}

// SetBucketRouter installs a router consulted by every method that
// takes an S3 key. Safe to call at any time; reads are not
// synchronized with writes (the field is set once at startup and
// only read thereafter).
func (p *ClientPool) SetBucketRouter(f BucketRouterFunc) {
	p.router = f
}

// resolveBucket returns the bucket to use for an operation on key,
// consulting the router when one is installed and falling back to
// the pool's default bucket otherwise.
func (p *ClientPool) resolveBucket(key string) string {
	if p.router != nil {
		if b := p.router(key); b != "" {
			return b
		}
	}
	return p.bucket
}

// WithBucket returns a shallow clone that talks to a different bucket
// using the same underlying *s3.Client and HTTP transport. Used by
// the per-tenant bucket router so each tenant's writes/reads land in
// the bucket configured via TenantOverride.S3.Bucket (or a global
// BucketTemplate expansion) without rebuilding the s3.Client.
//
// The original pool is unchanged. Cheap because s3.Client is
// goroutine-safe and bucket-agnostic; only the per-call Bucket
// parameter differs.
func (p *ClientPool) WithBucket(bucket string) *ClientPool {
	if bucket == "" || bucket == p.bucket {
		return p
	}
	return &ClientPool{client: p.client, bucket: bucket}
}

func NewClientPool(ctx context.Context, cfg *config.S3Config) (*ClientPool, error) {
	maxConns := cfg.MaxConnections
	if maxConns <= 0 {
		maxConns = 128
	}

	transport := &http.Transport{
		MaxIdleConnsPerHost:   maxConns,
		MaxIdleConns:          maxConns * 2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true, // Parquet files are already ZSTD-compressed
	}

	httpClient := &http.Client{Transport: transport}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = cfg.ForcePathStyle
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &ClientPool{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

func (p *ClientPool) NewReaderAt(ctx context.Context, key string, size int64) *S3ReaderAt {
	// Resolve the bucket up-front so any per-read router decision
	// captures the same answer for the lifetime of this reader.
	return &S3ReaderAt{
		client: p.client,
		bucket: p.resolveBucket(key),
		key:    key,
		size:   size,
		ctx:    ctx,
	}
}

func (r *S3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	end := off + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", off, end)

	start := time.Now()
	metrics.S3RequestsTotal.Inc("GetObject")

	var out *s3.GetObjectOutput
	err := retryS3(r.ctx, 3, func() error {
		var getErr error
		out, getErr = r.client.GetObject(r.ctx, &s3.GetObjectInput{
			Bucket: aws.String(r.bucket),
			Key:    aws.String(r.key),
			Range:  aws.String(rangeHeader),
		})
		return getErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("GetObject")
		return 0, fmt.Errorf("s3 GetObject range %s: %w", rangeHeader, err)
	}
	defer func() { _ = out.Body.Close() }()

	n, err := io.ReadFull(out.Body, p[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	metrics.S3BytesReadTotal.Add(n)
	return n, err
}

func (r *S3ReaderAt) Size() int64 {
	return r.size
}

func (p *ClientPool) S3Client() *s3.Client {
	return p.client
}

func (p *ClientPool) Upload(ctx context.Context, key string, data []byte) error {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("PutObject")

	err := retryS3(ctx, 3, func() error {
		_, putErr := p.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(p.resolveBucket(key)),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/octet-stream"),
		})
		return putErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("PutObject")
		return fmt.Errorf("s3 PutObject %s: %w", key, err)
	}
	return nil
}

func (p *ClientPool) Bucket() string {
	return p.bucket
}

// Copy issues an S3 server-side copy from (sourceBucket, sourceKey)
// to (destBucket, destKey). Used by the tenant bucket migration tool
// to retroactively move existing Parquet objects when a tenant's
// S3.Bucket override is added after the first writes. Server-side
// copy avoids the latency + bandwidth cost of a download/upload
// round-trip through the LH process.
func (p *ClientPool) Copy(ctx context.Context, sourceBucket, sourceKey, destBucket, destKey string) error {
	if sourceBucket == "" {
		sourceBucket = p.bucket
	}
	if destBucket == "" {
		destBucket = p.bucket
	}
	start := time.Now()
	metrics.S3RequestsTotal.Inc("CopyObject")
	err := retryS3(ctx, 3, func() error {
		_, copyErr := p.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(destBucket),
			Key:        aws.String(destKey),
			CopySource: aws.String(url.PathEscape(sourceBucket + "/" + sourceKey)),
		})
		return copyErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("CopyObject")
		return fmt.Errorf("s3 CopyObject %s/%s -> %s/%s: %w",
			sourceBucket, sourceKey, destBucket, destKey, err)
	}
	return nil
}

func (p *ClientPool) Download(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("GetObject")

	var out *s3.GetObjectOutput
	err := retryS3(ctx, 3, func() error {
		var getErr error
		out, getErr = p.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.resolveBucket(key)),
			Key:    aws.String(key),
		})
		return getErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("GetObject")
		return nil, fmt.Errorf("s3 GetObject %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 body %s: %w", key, err)
	}
	metrics.S3BytesReadTotal.Add(len(data))
	return data, nil
}

func (p *ClientPool) DownloadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("GetObject")
	metrics.S3RangeReadsTotal.Inc()

	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)

	var out *s3.GetObjectOutput
	err := retryS3(ctx, 3, func() error {
		var getErr error
		out, getErr = p.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.resolveBucket(key)),
			Key:    aws.String(key),
			Range:  aws.String(rangeHeader),
		})
		return getErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("GetObject")
		return nil, fmt.Errorf("s3 GetObject range %s key=%s: %w", rangeHeader, key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 body range %s key=%s: %w", rangeHeader, key, err)
	}
	metrics.S3BytesReadTotal.Add(len(data))
	metrics.S3RangeBytesRead.Add(len(data))
	return data, nil
}

func (p *ClientPool) Delete(ctx context.Context, key string) error {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("DeleteObject")

	err := retryS3(ctx, 3, func() error {
		_, delErr := p.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(p.resolveBucket(key)),
			Key:    aws.String(key),
		})
		return delErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.S3ErrorsTotal.Inc("DeleteObject")
		return fmt.Errorf("s3 DeleteObject %s: %w", key, err)
	}
	return nil
}

func (p *ClientPool) Exists(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("HeadObject")

	err := retryS3(ctx, 3, func() error {
		_, headErr := p.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(p.resolveBucket(key)),
			Key:    aws.String(key),
		})
		return headErr
	})
	metrics.S3RequestDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		var nsk *types.NotFound
		if errors.As(err, &nsk) {
			return false, nil
		}
		var nf *types.NoSuchKey
		if errors.As(err, &nf) {
			return false, nil
		}
		metrics.S3ErrorsTotal.Inc("HeadObject")
		return false, fmt.Errorf("s3 HeadObject %s: %w", key, err)
	}
	return true, nil
}
