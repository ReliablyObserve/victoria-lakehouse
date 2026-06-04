package tenant

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// MigrationManifest is the subset of manifest.Manifest the migrator
// touches. Kept as an interface so tests don't need a real manifest.
type MigrationManifest interface {
	AllFiles() map[string][]manifest.FileInfo
	SetFileBucket(key, bucket string)
}

// MigrationS3 is the subset of s3reader.ClientPool needed by the
// migrator: server-side copy + delete. Source bucket is read from
// FileInfo.Bucket (falling back to defaultBucket) so the migrator
// can flip ownership from either default or another tenant bucket.
type MigrationS3 interface {
	Copy(ctx context.Context, sourceBucket, sourceKey, destBucket, destKey string) error
	Delete(ctx context.Context, key string) error
}

// MigrationResult is the per-call summary returned by MigrateTenant.
type MigrationResult struct {
	AccountID    uint32   `json:"account_id"`
	ProjectID    uint32   `json:"project_id"`
	TargetBucket string   `json:"target_bucket"`
	FilesScanned int      `json:"files_scanned"`
	FilesMoved   int      `json:"files_moved"`
	FilesSkipped int      `json:"files_skipped"`
	BytesMoved   int64    `json:"bytes_moved"`
	FilesErrored int      `json:"files_errored"`
	Errors       []string `json:"errors,omitempty"`
}

// Migrator retroactively rebases a tenant's existing Parquet objects
// onto a new bucket. The flow per file is:
//
//  1. S3 server-side copy old_bucket/key -> target_bucket/key (no
//     bytes flow through the LH process)
//  2. Flip manifest.FileInfo.Bucket so subsequent reads route
//     correctly via the pool's BucketRouter
//  3. Delete the old object
//
// Step (3) is best-effort: a failed delete leaves the bytes
// orphaned in the old bucket but the manifest already points at the
// new copy, so reads stay correct. Operators can sweep orphans via
// the source bucket's lifecycle policy.
type Migrator struct {
	manifest      MigrationManifest
	pool          MigrationS3
	defaultBucket string
}

// NewMigrator wires the migrator against the running manifest +
// default-pool. The defaultBucket is used as the source for files
// whose FileInfo.Bucket is empty (the common pre-isolation case).
func NewMigrator(m MigrationManifest, pool MigrationS3, defaultBucket string) *Migrator {
	return &Migrator{manifest: m, pool: pool, defaultBucket: defaultBucket}
}

// MigrateTenant walks the manifest for files belonging to the given
// tenant (matched by the {account}/{project}/ prefix in the S3 key),
// then for each one: copies to target, flips manifest, deletes source.
//
// Files already in target are counted as Skipped, not re-migrated.
// Files whose key doesn't match the tenant prefix are silently
// skipped — the matcher is the same prefix-based parser the bucket
// router uses, so behavior is consistent across read and migration
// paths.
func (m *Migrator) MigrateTenant(ctx context.Context, accountID, projectID uint32, targetBucket string) MigrationResult {
	result := MigrationResult{
		AccountID:    accountID,
		ProjectID:    projectID,
		TargetBucket: targetBucket,
	}
	if targetBucket == "" {
		result.Errors = append(result.Errors, "target_bucket required")
		return result
	}

	prefix := fmt.Sprintf("%d/%d/", accountID, projectID)
	for _, files := range m.manifest.AllFiles() {
		for _, fi := range files {
			if !strings.HasPrefix(fi.Key, prefix) {
				continue
			}
			result.FilesScanned++

			sourceBucket := fi.Bucket
			if sourceBucket == "" {
				sourceBucket = m.defaultBucket
			}
			if sourceBucket == targetBucket {
				result.FilesSkipped++
				continue
			}

			if err := m.pool.Copy(ctx, sourceBucket, fi.Key, targetBucket, fi.Key); err != nil {
				result.FilesErrored++
				result.Errors = append(result.Errors,
					fmt.Sprintf("copy %s/%s: %v", sourceBucket, fi.Key, err))
				continue
			}

			// Flip ownership BEFORE deleting source so a crash here
			// leaves bytes in both buckets (safe) rather than only
			// in the source while the manifest points at the target.
			m.manifest.SetFileBucket(fi.Key, targetBucket)

			if err := m.pool.Delete(ctx, fi.Key); err != nil {
				// Manifest already updated; orphan is recoverable
				// via the source bucket's lifecycle policy. Surface
				// the error for visibility but don't unwind.
				result.Errors = append(result.Errors,
					fmt.Sprintf("delete-source %s/%s: %v (manifest already flipped to %s)",
						sourceBucket, fi.Key, err, targetBucket))
			}

			result.FilesMoved++
			result.BytesMoved += fi.Size
		}
	}
	return result
}

// ParseTenantKeyFromString accepts either "account:project" or a bare
// "account" form. The bare form mirrors upstream VL/VT's
// ParseTenantID, where a missing project segment is treated as 0.
// Used by the admin endpoint's request parser.
func ParseTenantKeyFromString(s string) (uint32, uint32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, 0, fmt.Errorf("empty tenant key")
	}
	a, p, hasProject := strings.Cut(s, ":")
	acc, err := strconv.ParseUint(strings.TrimSpace(a), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("account %q: %w", a, err)
	}
	if !hasProject {
		return uint32(acc), 0, nil
	}
	proj, err := strconv.ParseUint(strings.TrimSpace(p), 10, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("project %q: %w", p, err)
	}
	return uint32(acc), uint32(proj), nil
}
