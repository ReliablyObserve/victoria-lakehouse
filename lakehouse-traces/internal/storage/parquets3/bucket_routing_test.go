package parquets3

import (
	"context"
	"sync"
	"testing"
)

// fakeUploader records every Upload call so we can assert which
// bucket+key combinations were emitted by the writer's per-tenant
// flush path without spinning up real S3.
type fakeUploader struct {
	bucket string
	mu     sync.Mutex
	puts   []string // bucket/key
}

func (f *fakeUploader) Upload(_ context.Context, key string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts = append(f.puts, f.bucket+"|"+key)
	return nil
}

func TestBatchWriter_BucketForTenant_NoResolver_ReturnsDefault(t *testing.T) {
	// With no tenantBucket/tenantPool installed, bucketForTenant
	// returns the writer's default pool unchanged ("" sentinel).
	w := &BatchWriter{}
	bucket, _ := w.bucketForTenant(1, 1)
	if bucket != "" {
		t.Errorf("no-resolver bucket = %q, want empty (default)", bucket)
	}
}

func TestBatchWriter_BucketForTenant_RoutesViaResolvers(t *testing.T) {
	defaultUploader := &fakeUploader{bucket: "default"}
	acmeUploader := &fakeUploader{bucket: "acme-bucket"}

	w := &BatchWriter{}
	w.SetTenantBucket(func(a, p uint32) string {
		if a == 1002 && p == 0 {
			return "acme-bucket"
		}
		return ""
	})
	w.SetTenantPool(func(bucket string) PoolWriter {
		if bucket == "acme-bucket" {
			return acmeUploader
		}
		return defaultUploader
	})

	bucket, uploader := w.bucketForTenant(1002, 0)
	if bucket != "acme-bucket" {
		t.Errorf("acme bucket = %q, want acme-bucket", bucket)
	}
	if uploader != acmeUploader {
		t.Error("acme tenant must route through acmeUploader")
	}

	// Untenanted (0:0) → bucketForTenant returns empty sentinel and
	// the writer's default pool (here nil — we never set one).
	bucket, _ = w.bucketForTenant(0, 0)
	if bucket != "" {
		t.Errorf("default bucket sentinel = %q, want empty", bucket)
	}
}
