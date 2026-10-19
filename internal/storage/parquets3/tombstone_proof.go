package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Per-file tombstone gating.
//
// A tombstone used to switch the metadata fast paths off for the whole query
// window. It now only does so for the files it can match: a file whose own
// metadata proves that no row of it can match any tombstone acting on it (the
// time range, a complete label set, the pmeta bloom; delete.FileProof) is read
// as if there were no tombstone at all. The proof uses memory only, never an S3
// read, and anything it cannot decide counts as "may match", which is the
// behaviour before the proof existed.
//
// Twin of lakehouse-traces/internal/storage/parquets3/tombstone_proof.go — keep
// the two identical.

// maxProofPairs bounds the (file, tombstone) pairs one query proves; above it
// every file is read with its tombstones.
var maxProofPairs = 1 << 20

// tombstoneProofs aliases delete.Proofs, as tombstone aliases delete.Tombstone.
type tombstoneProofs = delete.Proofs

// compileProofs compiles tss for one request.
func (s *Storage) compileProofs(tss []tombstone) tombstoneProofs {
	return delete.CompileProofs(tss, func(field string) (string, bool, bool) {
		m := s.registry.ResolveToParquet(field)
		if m == nil || m.Origin != schema.OriginPromoted {
			return "", false, false
		}
		return m.ParquetColumn, m.HasBloom, true
	})
}

// anyMayMatch reports whether any of tss, the tombstones acting on fi, may
// match a row of it. The pmeta bloom facet answers the bloom question alone:
// the per-file `.bloom` object is never fetched, and a facet that cannot answer
// leaves the question open.
func (s *Storage) anyMayMatch(proofs tombstoneProofs, fi *manifest.FileInfo, tss []tombstone) bool {
	if len(tss) == 0 {
		return false
	}
	var bloom func(column, value string) (bool, bool)
	if s.catalog != nil {
		partition, keys := manifest.ExtractTenantPartition(fi.Key), []string{fi.Key}
		bloom = func(column, value string) (bool, bool) {
			matched, ok := s.catalog.BloomMayContain(partition, keys, column, value)
			return len(matched) > 0, ok
		}
	}
	// A compaction output carries the UNION of its inputs' label sets, which is
	// partial when an input had none (a file known only from a listing), so its
	// label set proves nothing; its bloom is built from the merged rows.
	labels := fi.Labels
	if fi.CompactionLevel > 0 {
		labels = nil
	}
	return proofs.AnyMayMatch(tss, fi.MinTimeNs, fi.MaxTimeNs, labels, maxLabelsPerField, bloom)
}

// cleanLane is how the files proven free of every tombstone acting on them are
// read: with no tombstone at all, so they keep the metadata fast paths and the
// plain read context.
type cleanLane struct {
	files []manifest.FileInfo
	keys  map[string]struct{}
	ctx   context.Context
	write logstorage.WriteDataBlockFunc
}

// newCleanLane splits files by the proof into the lane (clean files) and the
// rest, which are read with their tombstones as before. ctx and write are the
// lane's read context and write function, both built with no tombstone.
func (s *Storage) newCleanLane(files []manifest.FileInfo, sink *tombstoneSink, ctx context.Context, write logstorage.WriteDataBlockFunc) (*cleanLane, []manifest.FileInfo) {
	lane := &cleanLane{keys: map[string]struct{}{}, ctx: ctx, write: write}
	if len(files)*len(sink.tss) > maxProofPairs {
		return lane, files
	}
	proofs := s.compileProofs(sink.tss)
	var rest []manifest.FileInfo
	for i := range files {
		if s.anyMayMatch(proofs, &files[i], sink.forFile(files[i].Key)) {
			rest = append(rest, files[i])
			continue
		}
		lane.files = append(lane.files, files[i])
		lane.keys[files[i].Key] = struct{}{}
	}
	return lane, rest
}

// pick is the read context, tombstone flag and write function for the file at
// key: the lane's when the file is clean, otherwise the query's own. A nil lane
// (no tombstone overlaps the query) has no clean file.
func (l *cleanLane) pick(key string, ctx context.Context, hasTombstones bool, sink *tombstoneSink) (context.Context, bool, logstorage.WriteDataBlockFunc) {
	if l != nil {
		if _, ok := l.keys[key]; ok {
			return l.ctx, false, l.write
		}
	}
	return ctx, hasTombstones, sink.forKey(key)
}
