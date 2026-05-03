package s3reader

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

type S3ReaderAt struct {
	client *s3.Client
	bucket string
	key    string
	size   int64
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

func (p *ClientPool) NewReaderAt(key string, size int64) *S3ReaderAt {
	return &S3ReaderAt{
		client: p.client,
		bucket: p.bucket,
		key:    key,
		size:   size,
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

	out, err := r.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 GetObject range %s: %w", rangeHeader, err)
	}
	defer func() { _ = out.Body.Close() }()

	n, err := io.ReadFull(out.Body, p[:end-off+1])
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func (r *S3ReaderAt) Size() int64 {
	return r.size
}

func (p *ClientPool) S3Client() *s3.Client {
	return p.client
}

func (p *ClientPool) Download(ctx context.Context, key string) ([]byte, error) {
	out, err := p.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(p.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3 body %s: %w", key, err)
	}
	return data, nil
}
