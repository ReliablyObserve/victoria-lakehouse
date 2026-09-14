package delete

import (
	"errors"
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
	// ReplaceFile swaps oldKey's entry for fi in one critical section, and
	// only if oldKey is still present.
	ReplaceFile(partition string, oldKey string, fi manifest.FileInfo) bool
	// RemoveFileIfPresent drops an entry if it is still there — the
	// RowsKept == 0 case, where the rewrite leaves no replacement at all.
	RemoveFileIfPresent(partition string, key string) bool
	// HasKey is the post-condition check: after a publish the new key must be
	// present and the old one absent.
	HasKey(key string) bool
	// GetFilesForRange lists the files that may hold a tombstone's rows. A
	// tombstone's AffectedKeys is a snapshot from when the delete was issued;
	// compaction and late flushes change the file set under it, so the
	// scheduler re-reads it before retiring a tombstone.
	GetFilesForRange(startNs, endNs int64) []manifest.FileInfo

	// ClaimPending reserves the replacement key before it is uploaded — no
	// refresh adopts it, and no other writer can take the same key;
	// ReleasePending gives the claim back when nothing was written and
	// AbandonPending retires an upload that will never be published.
	// Retire / UnretireIfReplacedBy / ConfirmDeleted / LookupRetired let an
	// interrupted rewrite be finished or undone after a restart without the
	// refresh re-adopting the object it let go of. Hold / Release keep another
	// publish from superseding a replacement whose swap is not durable yet.
	// Listed reports whether the manifest has seen a bucket listing in this
	// process, without which an absent key says nothing about the object.
	// ExpectInListing / AwaitingListing cover the same blind spot for one key
	// after a listing has run: an undone rewrite's source is back to being the
	// live copy of its rows, but its entry only returns with the next refresh.
	// See manifest/retired.go.
	ClaimPending(key string) bool
	ReleasePending(key string)
	AbandonPending(key string)
	Retire(key, by string, reclaim bool) bool
	UnretireIfReplacedBy(key, replacement string) bool
	ConfirmDeleted(key string)
	LookupRetired(key string) (manifest.RetiredKey, bool)
	Hold(key string)
	Release(key string)
	Listed() bool
	ExpectInListing(key string)
	AwaitingListing(key string) bool
}

// removedByRewrite is the "replaced by" a source records when a rewrite removed
// every one of its rows and so wrote no replacement. It lets an interrupted
// rewrite of that kind be undone without disturbing a retirement some other
// component made.
const removedByRewrite = "rewrite:all-rows-removed"

// replacedBy is the retirement "by" a rewrite of source uses.
func replacedBy(newKey string) string {
	if newKey == "" {
		return removedByRewrite
	}
	return newKey
}

// errSourceSuperseded reports that a rewrite's source left the manifest between
// the read and the publish — a concurrent compaction merged it. The rewrite is
// discarded: publishing it would put the kept rows in the manifest twice (the
// rewrite and the compacted output) and bring the deleted rows back through the
// compacted copy. The tombstone then follows the rows to that compacted output.
var errSourceSuperseded = errors.New("rewrite source was superseded before publish")

// errReplacementKeyTaken reports that the key chosen for a replacement is
// already a file the manifest serves. The replacement must NOT be deleted in
// that case: the object under that key may be the other file.
var errReplacementKeyTaken = errors.New("replacement key already belongs to a registered file")

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
// The publish is conditional on the source still being registered (see
// errSourceSuperseded). On success result.Published is set, which is what
// licenses Rewriter.Commit to delete the superseded object. The returned entry
// is the replacement that was registered, or nil when the rewrite left no
// replacement (every row removed).
func publishRewrite(m ManifestUpdater, result *RewriteResult) (*manifest.FileInfo, error) {
	if m == nil {
		return nil, fmt.Errorf("no manifest wired into the rewrite scheduler")
	}
	if result == nil || result.RowsRemoved == 0 {
		return nil, nil
	}

	partition, known := m.PartitionForKey(result.OldKey)
	if !known {
		metrics.DeleteRewriteSuperseded.Inc()
		return nil, errSourceSuperseded
	}

	if result.RowsKept == 0 {
		// Nothing survived the filter: no replacement object exists, so the
		// entry simply goes away — if it is still ours to remove.
		if !m.RemoveFileIfPresent(partition, result.OldKey) {
			return nil, refusedPublish(m, result.OldKey)
		}
		// Record the removal as this rewrite's, so a restart that has to undo it
		// can tell it from a retirement someone else made.
		m.Retire(result.OldKey, removedByRewrite, true)
		result.Published = true
		metrics.DeleteRewriteManifestUpdated.Inc()
		return nil, nil
	}

	old, _ := m.GetFileByKey(result.OldKey)
	fi := rewrittenFileInfo(old, result)

	if !m.ReplaceFile(partition, result.OldKey, fi) {
		if m.HasKey(result.NewKey) {
			// The replacement key belongs to a file this manifest already
			// serves. The claim taken before the upload makes this
			// unreachable within one process; refuse loudly rather than
			// delete an object that is not ours.
			metrics.DeleteRewriteKeyCollisions.Inc()
			return nil, fmt.Errorf("%w: %s", errReplacementKeyTaken, result.NewKey)
		}
		return nil, refusedPublish(m, result.OldKey)
	}

	// Post-condition, checked rather than assumed: a manifest that reports a
	// successful swap must no longer list the old key — otherwise committing
	// would delete an object the manifest still points at. The NEW key is
	// deliberately not re-checked: once the swap has landed, a concurrent
	// compaction may already have merged it, which is a legitimate outcome, not
	// a failed publish.
	if m.HasKey(result.OldKey) {
		metrics.DeleteRewriteManifestErrors.Inc()
		return nil, fmt.Errorf("manifest reported the swap but still lists %s", result.OldKey)
	}

	result.Published = true
	metrics.DeleteRewriteManifestUpdated.Inc()
	logger.Infof("rewrite published; old=%s, new=%s, rows_kept=%d, rows_removed=%d, partition=%s",
		result.OldKey, fi.Key, result.RowsKept, result.RowsRemoved, partition)
	return &fi, nil
}

// refusedPublish classifies a publish the manifest refused. It is "superseded"
// ONLY when the source is verifiably gone: that is the case where the rows now
// live in a compacted output and the tombstone may follow them there. If the
// source is still registered, the refusal is a failure, and treating it as
// superseded would mark the key reaped while the source still holds the rows —
// which the tombstone's retirement would then un-hide.
func refusedPublish(m ManifestUpdater, oldKey string) error {
	if m.HasKey(oldKey) {
		metrics.DeleteRewriteManifestErrors.Inc()
		return fmt.Errorf("manifest refused the publish while %s is still registered", oldKey)
	}
	metrics.DeleteRewriteSuperseded.Inc()
	return errSourceSuperseded
}

// rewrittenFileInfo builds the replacement manifest entry.
//
// Each field is either RECOMPUTED from the kept rows or INHERITED from the
// superseded entry, and the split is deliberate:
//
//   - RowCount, MinTimeNs/MaxTimeNs, RawBytes, Size, BloomBytes, ColumnBytes,
//     LabelAggregates and Labels are recomputed. These are the fields query
//     paths answer from without opening the file; inheriting any of them would
//     keep serving the deleted rows from metadata. RowCount feeds the manifest
//     fast path and the 404 recovery's synthetic blocks; Labels feed both the
//     inverted index and the pmeta field catalog, which field_values serves
//     verbatim, so a superset inherited from the old file would put deleted
//     values straight back into every dropdown. Recomputed labels are exactly
//     what the flush writer records for the same rows, so pruning over them
//     behaves like it does for any freshly flushed file.
//   - ColumnStats are inherited. A rewrite only ever REMOVES rows, so the old
//     file's column min/max are a superset of the new file's. A superset is
//     the safe direction for range pruning — it can only scan a file that
//     turns out to have nothing, never skip one that holds matching rows — and
//     unlike labels, column stats are never served as values.
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
		Labels:            result.Labels,
		ColumnStats:       old.ColumnStats,
		LabelAggregates:   result.LabelAggregates,
		ColumnBytes:       result.ColumnBytes,
		StorageClass:      old.StorageClass,
		ClassCheckedAt:    old.ClassCheckedAt,
		ClassSource:       old.ClassSource,
		CreatedAt:         time.Now(),
	}
}
