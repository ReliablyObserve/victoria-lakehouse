package parquets3

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestShouldSkipByFooter_NoFilter(t *testing.T) {
	// No pushdown filter (empty query) -- should never skip
	skip, err := shouldSkipByFooter(context.Background(), nil, manifest.FileInfo{
		Key: "test.parquet", Size: 10000,
	}, "", schema.NewRegistry(schema.TracesProfile), nil)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Error("should not skip when no filter applies")
	}
}

func TestShouldSkipByFooter_NilPool(t *testing.T) {
	skip, err := shouldSkipByFooter(context.Background(), nil, manifest.FileInfo{
		Key: "test.parquet", Size: 100000,
	}, "service.name:=\"api-gateway\"", schema.NewRegistry(schema.TracesProfile), nil)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Error("should not skip when pool is nil")
	}
}

func TestShouldSkipByFooter_SmallFile(t *testing.T) {
	skip, err := shouldSkipByFooter(context.Background(), &s3reader.ClientPool{}, manifest.FileInfo{
		Key: "test.parquet", Size: 1000, // < 32KB
	}, "service.name:=\"api-gateway\"", schema.NewRegistry(schema.TracesProfile), nil)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Error("should not skip small files")
	}
}

func TestShouldSkipByFooter_CachedFooter(t *testing.T) {
	fc := NewFooterCache(10)
	fc.Put("test.parquet", &CachedFooter{FileSize: 100000})

	skip, err := shouldSkipByFooter(context.Background(), &s3reader.ClientPool{}, manifest.FileInfo{
		Key: "test.parquet", Size: 100000,
	}, "service.name:=\"api-gateway\"", schema.NewRegistry(schema.TracesProfile), fc)
	if err != nil {
		t.Fatal(err)
	}
	if skip {
		t.Error("should not skip when footer is already cached")
	}
}
