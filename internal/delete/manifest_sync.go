package delete

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ManifestUpdater is the slice of *manifest.Manifest the rewrite scheduler
// needs to publish a rewritten object. Declared as an interface so the delete
// package depends on behaviour rather than on the concrete manifest, and so the
// fault-injection tests can fail any single step of the hand-off.
type ManifestUpdater interface {
	// GetFileByKey returns the entry being superseded, so the replacement can
	// inherit the fields a rewrite does not change (bucket, schema
	// fingerprint, compaction level, storage class).
	GetFileByKey(key string) (manifest.FileInfo, bool)
	// PartitionForKey returns the partition that owns a key. The replacement
	// must be filed under the SAME partition, not one re-derived from the key
	// string.
	PartitionForKey(key string) (string, bool)
	// ReplaceFile swaps oldKey's entry for fi in one critical section.
	ReplaceFile(partition string, oldKey string, fi manifest.FileInfo) bool
	// RemoveFile drops an entry — the RowsKept == 0 case, where the rewrite
	// leaves no replacement object at all.
	RemoveFile(partition string, key string)
	// HasKey is the post-condition check: after a publish the new key must be
	// present and the old one absent.
	HasKey(key string) bool
}

// publishRewrite moves a rewritten object's registration into the manifest.
//
// This is the step whose absence was the whole bug. Without it the manifest
// kept pointing at a key the rewriter had already deleted, which fails in two
// opposite and equally wrong directions depending on whether the tombstone
// survives:
//
//   - tombstone present: the 404 recovery path SKIPS the missing key, so the
//     rows the delete was supposed to KEEP disappear from every query;
//   - tombstone lost (it only reached disk on a clean shutdown): the same path
//     synthesises RowCount blocks from the stale manifest entry, so the rows
//     the delete was supposed to REMOVE reappear in counts and hit totals.
//
// Meanwhile the replacement object was in nobody's manifest, so the orphan
// sweep deleted it once it passed the age gate — taking the kept rows with it.
//
// On success result.Published is set, which is what licenses Rewriter.Commit to
// delete the superseded object.
func publishRewrite(m ManifestUpdater, result *RewriteResult) error {
	if m == nil {
		return fmt.Errorf("no manifest wired into the rewrite scheduler")
	}
	if result == nil || result.RowsRemoved == 0 {
		return nil
	}

	partition, known := m.PartitionForKey(result.OldKey)
	if !known {
		// The superseded object was not in the manifest. That is already an
		// inconsistency (it was listed as a tombstone's affected key), but the
		// replacement must still end up managed or the orphan sweep will
		// reclaim it. File it under the partition its key encodes.
		partition = extractPartition(result.OldKey)
		metrics.DeleteStartupInconsistencies.Inc("rewrite_source_unmanifested")
		logger.Warnf("rewrite source not in manifest; key=%s, filing replacement under partition=%s",
			result.OldKey, partition)
	}

	if result.RowsKept == 0 {
		// Nothing survived the filter: no replacement object exists, so the
		// entry simply goes away.
		m.RemoveFile(partition, result.OldKey)
		if m.HasKey(result.OldKey) {
			metrics.DeleteRewriteManifestErrors.Inc()
			return fmt.Errorf("manifest still holds %s after removal", result.OldKey)
		}
		result.Published = true
		metrics.DeleteRewriteManifestUpdated.Inc()
		return nil
	}

	old, _ := m.GetFileByKey(result.OldKey)
	fi := rewrittenFileInfo(old, result)

	m.ReplaceFile(partition, result.OldKey, fi)

	// Post-condition, checked rather than assumed: the manifest must now serve
	// the kept rows from the new key and must no longer point at the old one.
	if !m.HasKey(fi.Key) || m.HasKey(result.OldKey) {
		metrics.DeleteRewriteManifestErrors.Inc()
		return fmt.Errorf("manifest swap did not take effect: new=%s present=%v, old=%s present=%v",
			fi.Key, m.HasKey(fi.Key), result.OldKey, m.HasKey(result.OldKey))
	}

	result.Published = true
	metrics.DeleteRewriteManifestUpdated.Inc()
	logger.Infof("rewrite published; old=%s, new=%s, rows_kept=%d, rows_removed=%d, partition=%s",
		result.OldKey, fi.Key, result.RowsKept, result.RowsRemoved, partition)
	return nil
}

// rewrittenFileInfo builds the replacement manifest entry.
//
// Each field is either RECOMPUTED from the kept rows or INHERITED from the
// superseded entry, and the split is deliberate:
//
//   - RowCount, MinTimeNs/MaxTimeNs, RawBytes, Size, BloomBytes, ColumnBytes
//     and LabelAggregates are recomputed. These are the fields query paths
//     answer from without opening the file; inheriting any of them would keep
//     serving the deleted rows from metadata. RowCount in particular feeds the
//     manifest fast path and the 404 recovery's synthetic blocks.
//   - Labels and ColumnStats are inherited. A rewrite only ever REMOVES rows,
//     so the old file's label set and column min/max are a superset of the new
//     file's. A superset is the safe direction for both: it can only make a
//     pruning decision more conservative (scan a file that turns out to have
//     nothing), never skip a file that does hold matching rows.
//   - Bucket, SchemaFingerprint, CompactionLevel and the storage-class fields
//     describe where and how the object lives, which the rewrite preserves.
func rewrittenFileInfo(old manifest.FileInfo, result *RewriteResult) manifest.FileInfo {
	return manifest.FileInfo{
		Key:               result.NewKey,
		Bucket:            old.Bucket,
		Size:              result.BytesAfter,
		RowCount:          result.RowsKept,
		MinTimeNs:         result.MinTimeNs,
		MaxTimeNs:         result.MaxTimeNs,
		RawBytes:          result.RawBytes,
		BloomBytes:        result.BloomBytes,
		SchemaFingerprint: old.SchemaFingerprint,
		CompactionLevel:   old.CompactionLevel,
		Labels:            old.Labels,
		ColumnStats:       old.ColumnStats,
		LabelAggregates:   result.LabelAggregates,
		ColumnBytes:       result.ColumnBytes,
		StorageClass:      old.StorageClass,
		ClassCheckedAt:    old.ClassCheckedAt,
		ClassSource:       old.ClassSource,
		CreatedAt:         time.Now(),
	}
}
