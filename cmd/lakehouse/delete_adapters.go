package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// s3PoolAdapter wraps a ClientPool to satisfy the delete.S3Pool interface
// by adding List support using the underlying S3 client.
type s3PoolAdapter struct {
	pool *s3reader.ClientPool
}

func (a *s3PoolAdapter) Upload(ctx context.Context, key string, data []byte) error {
	return a.pool.Upload(ctx, key, data)
}

func (a *s3PoolAdapter) Download(ctx context.Context, key string) ([]byte, error) {
	return a.pool.Download(ctx, key)
}

func (a *s3PoolAdapter) Delete(ctx context.Context, key string) error {
	return a.pool.Delete(ctx, key)
}

func (a *s3PoolAdapter) List(ctx context.Context, prefix string) ([]string, error) {
	client := a.pool.S3Client()
	bucket := a.pool.Bucket()

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys, nil
}

// manifestQuerierAdapter wraps a *manifest.Manifest to satisfy delete.ManifestQuerier
// by converting manifest.FileInfo to delete.FileInfo.
type manifestQuerierAdapter struct {
	m *manifest.Manifest
}

func (a *manifestQuerierAdapter) GetFilesForRange(startNs, endNs int64) []delete.FileInfo {
	mFiles := a.m.GetFilesForRange(startNs, endNs)
	result := make([]delete.FileInfo, len(mFiles))
	for i, f := range mFiles {
		result[i] = delete.FileInfo{
			Key:       f.Key,
			Size:      f.Size,
			MinTimeNs: f.MinTimeNs,
			MaxTimeNs: f.MaxTimeNs,
		}
	}
	return result
}
