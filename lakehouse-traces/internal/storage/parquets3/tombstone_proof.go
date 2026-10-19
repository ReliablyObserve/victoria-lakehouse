package parquets3

import (
	"context"
	"path"
	"strings"

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
// Twin of internal/storage/parquets3/tombstone_proof.go — keep
// the two identical.

// maxProofPairs bounds the (file, tombstone) pairs one query proves; above it
// every file is read with its tombstones.
var maxProofPairs = 1 << 20

// tombstoneProofs are the compiled proofs of one request and the bloom they ask.
type tombstoneProofs struct {
	delete.Proofs
	bloom delete.Bloom
}

// compileProofs compiles tss for one request. The pmeta bloom facet answers the
// bloom question alone: the per-file `.bloom` object is never fetched, and a
// facet that cannot answer leaves the question open.
func (s *Storage) compileProofs(tss []tombstone) tombstoneProofs {
	p := tombstoneProofs{Proofs: delete.CompileProofs(tss, func(field string) (string, bool, bool) {
		m := s.registry.ResolveToParquet(field)
		if m == nil || m.Origin != schema.OriginPromoted {
			return "", false, false
		}
		return m.ParquetColumn, m.HasBloom, true
	})}
	if s.catalog != nil {
		p.bloom = func(key, column, value string) (bool, bool) {
			matched, ok := s.catalog.BloomMayContain(manifest.ExtractTenantPartition(key), []string{key}, column, value)
			return len(matched) > 0, ok
		}
	}
	return p
}

// anyMayMatch reports whether any of tss, the tombstones acting on fi, may
// match a row of it.
func (p tombstoneProofs) anyMayMatch(fi *manifest.FileInfo, tss []tombstone) bool {
	if len(tss) == 0 {
		return false
	}
	// A compaction output's label set is the UNION of its inputs', which is
	// partial when an input had none (a file known only from a listing): it
	// proves nothing (#621). Its bloom is built from the merged rows and does.
	// Only the pod that compacted knows the level; a manifest built from a LIST
	// and pmeta has level 0 plus the union labels, so the key is checked too
	// (every compaction output is "compacted-L<level>-<id>.parquet").
	labels := fi.Labels
	if fi.CompactionLevel > 0 || strings.HasPrefix(path.Base(fi.Key), "compacted-") {
		labels = nil
	}
	return p.AnyMayMatch(tss, fi.Key, fi.MinTimeNs, fi.MaxTimeNs, labels, maxLabelsPerField, p.bloom)
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

// newCleanLane splits files by the proof, in place, into the lane (clean files,
// first) and the rest, which are read with their tombstones as before. ctx and
// write are the lane's read context and write function, both built with no
// tombstone. files is the query's own slice: it is reordered, not copied.
func (s *Storage) newCleanLane(files []manifest.FileInfo, sink *tombstoneSink, ctx context.Context, write logstorage.WriteDataBlockFunc) (*cleanLane, []manifest.FileInfo) {
	lane := &cleanLane{ctx: ctx, write: write}
	if len(files)*len(sink.tss) > maxProofPairs {
		return lane, files
	}
	proofs, clean := s.compileProofs(sink.tss), 0
	for i := range files {
		if !proofs.anyMayMatch(&files[i], sink.forFile(files[i].Key)) {
			files[clean], files[i] = files[i], files[clean]
			clean++
		}
	}
	lane.files = files[:clean] // rest follows it in the same array: append(lane.files, rest...) copies onto itself
	return lane, files[clean:]
}

// markClean records which files are still to be read once the fast paths have
// answered what they could: the lane's, from then on, not the query's.
func (l *cleanLane) markClean(files []manifest.FileInfo) {
	if l == nil {
		return
	}
	l.keys = make(map[string]struct{}, len(files))
	for i := range files {
		l.keys[files[i].Key] = struct{}{}
	}
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

// splitCleanFiles returns what RunQuery's metadata fast paths run over: the
// files, the read context and the write function they use, the files that keep
// their tombstones, and the clean lane (nil without a tombstone, when the fast
// paths run over every file with the query's own context and write function).
func (s *Storage) splitCleanFiles(hasTombstones bool, files []manifest.FileInfo, sink *tombstoneSink, ctx context.Context, write logstorage.WriteDataBlockFunc,
	baseCtx context.Context, q *logstorage.Query, neededFields []string, filter *logstorage.Filter, queryStr string, plan metadataOnlyPlan,
	writeWith func([]tombstone) logstorage.WriteDataBlockFunc) ([]manifest.FileInfo, context.Context, logstorage.WriteDataBlockFunc, []manifest.FileInfo, *cleanLane) {
	if !hasTombstones {
		return files, ctx, write, nil, nil
	}
	cleanCtx := withMetadataOnlyPlan(withReadContext(baseCtx, q, neededFields, filter, queryStr, nil), plan)
	lane, rest := s.newCleanLane(files, sink, cleanCtx, writeWith(nil))
	return lane.files, lane.ctx, lane.write, rest, lane
}
