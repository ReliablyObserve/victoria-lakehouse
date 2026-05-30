package parquets3

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// OwnershipChecker determines whether a given cache key is owned by the
// local node. In a distributed setup the cache ring assigns each file to
// exactly one pod; only that pod should warm the file into its local cache.
type OwnershipChecker interface {
	IsLocal(key string) bool
}

// filterOwnedFiles returns only the files owned by this node according to
// the checker. If checker is nil (single-node / no partitioning) all files
// are returned unchanged.
func filterOwnedFiles(files []manifest.FileInfo, checker OwnershipChecker) []manifest.FileInfo {
	if checker == nil {
		return files // no partitioning, warm all
	}
	var owned []manifest.FileInfo
	for _, f := range files {
		if checker.IsLocal(f.Key) {
			owned = append(owned, f)
		}
	}
	return owned
}

func (s *Storage) WarmupCache(ctx context.Context) {
	partitionsBack := s.cfg.Cache.WarmupPartitions
	if partitionsBack <= 0 {
		partitionsBack = 6
	}
	maxFiles := s.cfg.Cache.WarmupMaxFiles
	if maxFiles <= 0 {
		maxFiles = 500
	}
	concurrency := s.cfg.Cache.WarmupConcurrency
	if concurrency <= 0 {
		concurrency = 16
	}

	now := time.Now()
	end := now.UnixNano()
	start := now.Add(-time.Duration(partitionsBack) * time.Hour).UnixNano()

	files := s.manifest.GetFilesForRange(start, end)
	if len(files) == 0 {
		logger.Infof("warmup: no files in range [-%dh, now]", partitionsBack)
		return
	}

	// Sort by recency (newest first) so most recent data is warmed first
	sort.Slice(files, func(i, j int) bool {
		return files[i].Key > files[j].Key
	})

	if len(files) > maxFiles {
		files = files[:maxFiles]
	}

	// Filter to only files owned by this node in a distributed setup.
	var checker OwnershipChecker
	if s.smartCache != nil {
		checker = s.smartCache
	}
	beforeFilter := len(files)
	files = filterOwnedFiles(files, checker)
	if beforeFilter != len(files) {
		logger.Infof("warmup: filtered %d -> %d files by ownership", beforeFilter, len(files))
	}

	logger.Infof("warmup: starting cache warmup; files=%d partitions_back=%d concurrency=%d",
		len(files), partitionsBack, concurrency)

	warmupStart := time.Now()
	var warmed atomic.Int64
	var errors atomic.Int64
	var bytesLoaded atomic.Int64

	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}
				// Reserve the process-wide file-resident budget BEFORE
				// downloading. Without this, the background warmup pool
				// runs unbounded against the same memory headroom the
				// foreground query pool depends on — on a 7-day wildcard
				// the two fanouts collide and OOM-kill the container
				// (per heap-diff: warmup held 131 MiB concurrently with
				// query holding 607 MiB via the same getFileData path).
				rel, fbErr := acquireFileBudget(ctx, fi.Size)
				if fbErr != nil {
					return
				}
				data, err := s.getFileData(ctx, fi.Key, fi.Size)
				if err != nil {
					rel()
					errors.Add(1)
					continue
				}
				warmed.Add(1)
				bytesLoaded.Add(int64(len(data)))

				if s.footerCache != nil {
					cached, _, parseErr := ParseFooterFromData(fi.Key, data)
					if parseErr == nil {
						s.footerCache.Put(fi.Key, cached)
					}
				}
				rel()

				n := warmed.Load()
				if n%100 == 0 {
					logger.Infof("warmup: progress %d/%d files, %.1f MB loaded",
						n, len(files), float64(bytesLoaded.Load())/(1024*1024))
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(warmupStart)
	logger.Infof("warmup: complete; files=%d errors=%d bytes=%.1fMB elapsed=%s",
		warmed.Load(), errors.Load(),
		float64(bytesLoaded.Load())/(1024*1024), elapsed)

	metrics.PrefetchTasksTotal.Add("warmup", int(warmed.Load()))
	metrics.PrefetchBytesTotal.Add(int(bytesLoaded.Load()))
}
