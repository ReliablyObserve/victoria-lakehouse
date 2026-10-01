// Twin of internal/storage/parquets3/bounds_resolve.go — keep the two in step.

package parquets3

import (
	"context"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// boundsResolveConcurrency caps the footer reads one watermark computation runs
// at once. The objects involved are the handful a pod learned only by listing
// (its own final flush before a restart, or a peer's flushes), so this is a
// ceiling, not a typical fan-out.
const boundsResolveConcurrency = 8

// withExactBounds returns files with every listing-inferred time range replaced
// by the object's real one. The buffer watermark is the newest MaxTimeNs among
// the selected objects; an inferred MaxTimeNs is the END OF THE PARTITION HOUR,
// which would hide every buffered row of the rest of the hour. The bounds are
// resolved before the object takes part in a watermark, from (in order) what the
// manifest has learned meanwhile, the pmeta file-meta facet (RAM), and the
// object's own Parquet footer.
//
// An object whose bounds cannot be resolved keeps BoundsInferred, and the
// watermark ignores it (see bufferWatermarksFor). Objects that already have
// exact bounds cost nothing here. The caller's slice is not modified.
func (s *Storage) withExactBounds(ctx context.Context, files []manifest.FileInfo) []manifest.FileInfo {
	var todo []int
	for i := range files {
		if files[i].BoundsInferred {
			todo = append(todo, i)
		}
	}
	if len(todo) == 0 {
		return files
	}
	out := append([]manifest.FileInfo(nil), files...)

	workers := boundsResolveConcurrency
	if workers > len(todo) {
		workers = len(todo)
	}
	idxCh := make(chan int, len(todo))
	for _, i := range todo {
		idxCh <- i
	}
	close(idxCh)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idxCh {
				out[i] = s.resolveFileBounds(ctx, out[i])
			}
		}()
	}
	wg.Wait()
	return out
}

// resolveFileBounds makes one object's bounds exact if any source can say what
// they are, and returns the manifest's resulting entry (still BoundsInferred
// when nothing could).
func (s *Storage) resolveFileBounds(ctx context.Context, fi manifest.FileInfo) manifest.FileInfo {
	if s.manifest == nil {
		return fi
	}
	current := func() manifest.FileInfo {
		if cur, ok := s.manifest.GetFileByKey(fi.Key); ok {
			return cur
		}
		return fi
	}
	// The manifest may have been enriched since the caller took its copy (a
	// scan that opened the object, a warmup pass).
	if cur := current(); !cur.BoundsInferred {
		return cur
	}
	// The pmeta file-meta facet: exact bounds from RAM, no S3 read.
	if s.catalog != nil {
		if fm, ok := (catalogFileMetaProvider{store: s.catalog}).FileMeta("", fi.Key); ok && fm.MinTimeNs > 0 {
			s.manifest.EnrichFileMetadata(fi.Key, fm.RowCount, fm.MinTimeNs, fm.MaxTimeNs)
			if cur := current(); !cur.BoundsInferred {
				return cur
			}
		}
	}
	// The object's own footer and page index.
	if s.pool != nil || s.smartCache != nil {
		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err == nil && len(data) > 0 {
			if _, pf, perr := ParseFooterFromData(fi.Key, data); perr == nil {
				s.enrichFromParquetFile(current(), pf)
			}
		} else if err != nil && ctx.Err() == nil {
			logger.Warnf("buffer watermark: cannot read %s to resolve its time bounds: %s", fi.Key, err)
		}
	}
	return current()
}

// recentInferredWindow and recentInferredMaxFiles bound the startup pass that
// resolves inferred bounds (enrichRecentInferredBounds): the buffer only ever
// holds recent rows, so older objects wait for the lazy path.
const (
	recentInferredWindow   = 6 * time.Hour
	recentInferredMaxFiles = 512
)

// enrichRecentInferredBounds resolves the inferred bounds of the recent
// objects at startup, so the first query after a restart does not pay for it.
// It returns how many objects now have exact bounds.
func (s *Storage) enrichRecentInferredBounds(ctx context.Context) int {
	if s.manifest == nil || s.pool == nil {
		return 0
	}
	now := time.Now()
	startNs := now.Add(-recentInferredWindow).UnixNano()
	var todo []manifest.FileInfo
	for _, fi := range s.manifest.GetFilesForRange(startNs, now.UnixNano()) {
		if fi.BoundsInferred {
			todo = append(todo, fi)
		}
	}
	if len(todo) == 0 {
		return 0
	}
	if len(todo) > recentInferredMaxFiles {
		todo = todo[:recentInferredMaxFiles]
	}
	s.enrichSmallFiles(ctx, todo)
	resolved := 0
	for _, fi := range todo {
		if cur, ok := s.manifest.GetFileByKey(fi.Key); ok && !cur.BoundsInferred {
			resolved++
		}
	}
	return resolved
}
