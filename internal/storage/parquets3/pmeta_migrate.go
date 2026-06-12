package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// pmetaTenantIsolatedMarkerSuffix is appended to the AutoPrefix to form the
// idempotency marker key. Its presence means the one-time legacy global pmeta
// bundle cleanup has already run, so CleanupLegacyGlobalBundles becomes a no-op.
const pmetaTenantIsolatedMarkerSuffix = "_meta/pmeta-tenant-isolated.marker"

// CleanupLegacyGlobalBundles deletes the orphaned OLD-format pmeta bundle
// objects left behind by the move from a global dt=/hour= partition to a
// tenant-scoped <account>/<project>/<signal>/dt=/hour= partition.
//
// Before the change, a partition's bundle lived at:
//
//	<autoPrefix><pure dt=/hour=>/_pmeta.bundle   (+ sibling /_bloom.bin)
//
// where the pure partition is manifest.ExtractPartition(fileKey), e.g.
// "dt=2026-06-09/hour=10". After the change, the bundle lives at:
//
//	<autoPrefix><account>/<project>/<signal>/dt=/hour=/_pmeta.bundle
//
// (manifest.ExtractTenantPartition(fileKey)). The new tenant bundles are
// rebuilt from files by the warm self-heal, so deleting the old globals causes
// no data loss.
//
// Safety: this only ever deletes keys of the exact form
//
//	autoPrefix + <ExtractPartition result> + suffix
//
// ExtractPartition returns a pure "dt=.../hour=HH" with NO tenant segment, so
// these keys have no <account>/<project>/<signal>/ prefix and cannot collide
// with the tenant-format keys (which always carry that prefix). There is no
// LIST/glob — only the exact computed keys are deleted, and "not found" is
// ignored so the cleanup is idempotent.
//
// It runs at most once per prefix: an idempotency marker is written on success
// and checked on entry. Returns the number of objects successfully deleted.
func (s *Storage) CleanupLegacyGlobalBundles(ctx context.Context, autoPrefix string) (deleted int) {
	pool := s.Pool()
	if pool == nil {
		return 0
	}

	// Gate: if the marker already exists, the cleanup has run before.
	markerKey := autoPrefix + pmetaTenantIsolatedMarkerSuffix
	if _, err := pool.Download(ctx, markerKey); err == nil {
		return 0
	}

	// Manifest-driven set of distinct OLD (pure dt=/hour=) partitions. No S3 LIST.
	oldPartitions := make(map[string]struct{})
	for _, files := range s.Manifest().AllFiles() {
		for _, fi := range files {
			p := manifest.ExtractPartition(fi.Key)
			if p == "" {
				continue
			}
			oldPartitions[p] = struct{}{}
		}
	}

	// Delete the exact old bundle objects for each distinct pure partition.
	for p := range oldPartitions {
		bundleKey := autoPrefix + p + "/_pmeta.bundle"
		bloomKey := autoPrefix + p + "/_bloom.bin"
		if err := pool.Delete(ctx, bundleKey); err == nil {
			deleted++
		}
		if err := pool.Delete(ctx, bloomKey); err == nil {
			deleted++
		}
	}

	// Write the marker so this never runs again for this prefix.
	if err := pool.Upload(ctx, markerKey, []byte("v2")); err != nil {
		logger.Warnf("legacy pmeta bundle cleanup: failed to write marker %s: %s", markerKey, err)
	}

	logger.Infof("legacy pmeta bundle cleanup: deleted %d old global bundle objects", deleted)
	return deleted
}
