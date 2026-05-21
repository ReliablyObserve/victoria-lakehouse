package s3reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
			backoff := time.Duration(1<<uint(i)) * 100 * time.Millisecond
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			select {
			case <-time.After(backoff):
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

type ClientPool struct {
	client *s3.Client
	bucket string
}

func NewClientPool(ctx context.Context, cfg *config.S3Config) (*ClientPool, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
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
	return &S3ReaderAt{
		client: p.client,
		bucket: p.bucket,
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
			Bucket:      aws.String(p.bucket),
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

func (p *ClientPool) Download(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	metrics.S3RequestsTotal.Inc("GetObject")

	var out *s3.GetObjectOutput
	err := retryS3(ctx, 3, func() error {
		var getErr error
		out, getErr = p.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(p.bucket),
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
			Bucket: aws.String(p.bucket),
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
			Bucket: aws.String(p.bucket),
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
			Bucket: aws.String(p.bucket),
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
