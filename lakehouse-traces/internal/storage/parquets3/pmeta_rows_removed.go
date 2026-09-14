package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

// PmetaOnRewritten folds a published delete rewrite into the pmeta facets. It
// is PmetaOnCompacted (the replacement's file-meta and bloom entries in, the
// superseded file's out) plus the one thing a compaction never needs: the field
// catalog's value sets are rebuilt, because a rewrite REMOVES rows and the
// catalog is a union that only ever grows. Without the rebuild, a value carried
// only by the deleted rows stays enumerable in field_values once the tombstone
// that currently hides it retires.
//
// Wired as the rewrite scheduler's OnPublished hook in both binaries. Mirror of
// internal/storage/parquets3/pmeta_rows_removed.go.
func (s *Storage) PmetaOnRewritten(added []manifest.FileInfo, removed []string, blooms map[string]map[string][]string) {
	s.PmetaOnCompacted(added, removed, blooms)
	keys := append([]string(nil), removed...)
	for _, fi := range added {
		keys = append(keys, fi.Key)
	}
	s.PmetaRebuildCatalogValues(keys)
}

// PmetaRebuildCatalogValues re-derives the enumerable field values of every
// partition the given keys belong to, from the files that partition holds NOW.
// Also the tombstone-retirement hook: a compaction that dropped tombstoned rows
// shrinks the catalog's true union too, and retirement is the moment the query
// filter stops hiding what the catalog still lists.
//
// A partition with a file whose manifest entry carries no labels is skipped and
// counted: replaying it would silently drop that file's values and make the
// catalog serve a PARTIAL list as authoritative, which is worse than the stale
// superset it would replace.
func (s *Storage) PmetaRebuildCatalogValues(keys []string) {
	if s.catalog == nil || len(keys) == 0 {
		return
	}
	parts := make(map[string]bool, len(keys))
	for _, k := range keys {
		parts[manifest.ExtractTenantPartition(k)] = true
	}

	byPart := make(map[string][]manifest.FileInfo, len(parts))
	for _, files := range s.manifest.AllFiles() {
		for _, fi := range files {
			if tp := manifest.ExtractTenantPartition(fi.Key); parts[tp] {
				byPart[tp] = append(byPart[tp], fi)
			}
		}
	}

	for tp := range parts {
		files := byPart[tp]
		contributions := make([]pmeta.FileContribution, 0, len(files))
		complete := true
		for _, fi := range files {
			if fi.Labels == nil && fi.RowCount > 0 {
				complete = false
				break
			}
			var truncated []string
			for field, vals := range fi.Labels {
				if len(vals) >= maxLabelsPerField {
					truncated = append(truncated, field)
				}
			}
			contributions = append(contributions, pmeta.FileContribution{
				Partition:       tp,
				FileKey:         fi.Key,
				RowCount:        fi.RowCount,
				Labels:          fi.Labels,
				TruncatedFields: truncated,
			})
		}
		if !complete {
			metrics.DeleteCatalogRebuilds.Inc("skipped_unlabeled_file")
			continue
		}
		if s.catalog.RebuildFieldCatalogValues(tp, contributions) {
			metrics.DeleteCatalogRebuilds.Inc("rebuilt")
		}
	}
}
