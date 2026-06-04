package tenant

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

type fakeManifest struct {
	mu    sync.Mutex
	files map[string][]manifest.FileInfo
}

func (f *fakeManifest) AllFiles() map[string][]manifest.FileInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	snap := make(map[string][]manifest.FileInfo, len(f.files))
	for k, v := range f.files {
		cp := make([]manifest.FileInfo, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

func (f *fakeManifest) SetFileBucket(key, bucket string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, files := range f.files {
		for i := range files {
			if files[i].Key == key {
				files[i].Bucket = bucket
				return
			}
		}
	}
}

type fakeS3 struct {
	mu      sync.Mutex
	copies  []string
	deletes []string
	copyErr error
	delErr  error
}

func (f *fakeS3) Copy(_ context.Context, sourceBucket, sourceKey, destBucket, destKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.copyErr != nil {
		return f.copyErr
	}
	f.copies = append(f.copies, sourceBucket+"/"+sourceKey+"|"+destBucket+"/"+destKey)
	return nil
}

func (f *fakeS3) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delErr != nil {
		return f.delErr
	}
	f.deletes = append(f.deletes, key)
	return nil
}

func TestMigrator_MigrateTenant_MovesMatchingFiles(t *testing.T) {
	mf := &fakeManifest{files: map[string][]manifest.FileInfo{
		"dt=2026-06-03/hour=09": {
			{Key: "1002/0/logs/dt=2026-06-03/hour=09/a.parquet", Size: 100},
			{Key: "1002/0/logs/dt=2026-06-03/hour=09/b.parquet", Size: 200},
			// Other tenant — must be ignored.
			{Key: "999/9/logs/dt=2026-06-03/hour=09/c.parquet", Size: 300},
		},
		"dt=2026-06-02/hour=10": {
			// Already in target bucket — should be skipped, not re-copied.
			{Key: "1002/0/logs/dt=2026-06-02/hour=10/d.parquet", Bucket: "acme-bucket", Size: 50},
		},
	}}
	s3 := &fakeS3{}
	mig := NewMigrator(mf, s3, "default-bucket")

	res := mig.MigrateTenant(context.Background(), 1002, 0, "acme-bucket")

	if res.FilesScanned != 3 {
		t.Errorf("scanned = %d, want 3 (3 acme files; other tenant ignored entirely)", res.FilesScanned)
	}
	if res.FilesMoved != 2 {
		t.Errorf("moved = %d, want 2", res.FilesMoved)
	}
	if res.FilesSkipped != 1 {
		t.Errorf("skipped = %d, want 1 (file already in target)", res.FilesSkipped)
	}
	if res.BytesMoved != 300 {
		t.Errorf("bytes_moved = %d, want 300 (100+200)", res.BytesMoved)
	}
	if len(s3.copies) != 2 {
		t.Errorf("copies = %d, want 2", len(s3.copies))
	}
	if len(s3.deletes) != 2 {
		t.Errorf("deletes = %d, want 2", len(s3.deletes))
	}

	// Manifest should now reflect the new bucket for moved files only.
	for _, files := range mf.AllFiles() {
		for _, fi := range files {
			if fi.Key == "999/9/logs/dt=2026-06-03/hour=09/c.parquet" {
				if fi.Bucket != "" {
					t.Errorf("other-tenant file mutated: bucket=%q", fi.Bucket)
				}
			} else if fi.Bucket != "acme-bucket" {
				t.Errorf("file %s bucket=%q, want acme-bucket", fi.Key, fi.Bucket)
			}
		}
	}
}

func TestMigrator_TargetRequired(t *testing.T) {
	mf := &fakeManifest{}
	mig := NewMigrator(mf, &fakeS3{}, "default")
	res := mig.MigrateTenant(context.Background(), 1, 1, "")
	if len(res.Errors) == 0 {
		t.Error("missing target_bucket must surface error")
	}
}

func TestMigrator_CopyFailure_LeavesManifestUntouched(t *testing.T) {
	mf := &fakeManifest{files: map[string][]manifest.FileInfo{
		"p": {{Key: "5/5/logs/a.parquet"}},
	}}
	s3 := &fakeS3{copyErr: errors.New("simulated S3 5xx")}
	mig := NewMigrator(mf, s3, "default")
	res := mig.MigrateTenant(context.Background(), 5, 5, "target")
	if res.FilesMoved != 0 {
		t.Errorf("moved = %d, want 0 on copy failure", res.FilesMoved)
	}
	if res.FilesErrored != 1 {
		t.Errorf("errored = %d, want 1", res.FilesErrored)
	}
	// Manifest still points at the original bucket (empty).
	if mf.files["p"][0].Bucket != "" {
		t.Errorf("manifest bucket = %q, want empty (copy failed)", mf.files["p"][0].Bucket)
	}
	// Delete must NOT have fired.
	if len(s3.deletes) != 0 {
		t.Errorf("deletes = %d, want 0 when copy failed", len(s3.deletes))
	}
}

func TestMigrator_DeleteFailure_KeepsBytesAsOrphan(t *testing.T) {
	mf := &fakeManifest{files: map[string][]manifest.FileInfo{
		"p": {{Key: "5/5/logs/a.parquet", Size: 99}},
	}}
	s3 := &fakeS3{delErr: errors.New("delete throttled")}
	mig := NewMigrator(mf, s3, "default")
	res := mig.MigrateTenant(context.Background(), 5, 5, "target")
	if res.FilesMoved != 1 {
		t.Errorf("moved = %d, want 1 (copy succeeded; delete failure must not unwind)", res.FilesMoved)
	}
	if len(res.Errors) != 1 {
		t.Errorf("errors = %d, want 1 surface", len(res.Errors))
	}
	if mf.files["p"][0].Bucket != "target" {
		t.Errorf("manifest bucket = %q, want target (flip happens before delete)", mf.files["p"][0].Bucket)
	}
}

func TestParseTenantKeyFromString(t *testing.T) {
	a, p, err := ParseTenantKeyFromString("1002:0")
	if err != nil || a != 1002 || p != 0 {
		t.Errorf("good input: got (%d,%d,%v), want (1002,0,nil)", a, p, err)
	}
	// Bare account form mirrors upstream VL/VT (project defaults to 0).
	a, p, err = ParseTenantKeyFromString("42")
	if err != nil || a != 42 || p != 0 {
		t.Errorf("bare account: got (%d,%d,%v), want (42,0,nil)", a, p, err)
	}
	if _, _, err := ParseTenantKeyFromString("abc:0"); err == nil {
		t.Error("non-numeric account must error")
	}
	if _, _, err := ParseTenantKeyFromString("1:abc"); err == nil {
		t.Error("non-numeric project must error")
	}
	if _, _, err := ParseTenantKeyFromString(""); err == nil {
		t.Error("empty key must error")
	}
}
