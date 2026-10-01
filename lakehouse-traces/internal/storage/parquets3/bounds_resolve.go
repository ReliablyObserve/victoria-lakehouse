// Twin of internal/storage/parquets3/bounds_resolve.go - keep the two in step.

package parquets3

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Resolving the exact time range of an object the manifest only knows from the
// S3 listing.
//
// The buffer watermark is the newest MaxTimeNs among the objects a query
// selected. An object learned from the listing has no recorded range; the
// manifest infers the partition hour for it (FileInfo.BoundsInferred), whose
// end would hide every buffered row of the rest of the hour. The exact range
// comes from, in order: the manifest itself (something enriched it meanwhile),
// the pmeta file-meta facet (RAM), and the `_time` column-chunk statistics in
// the object's Parquet footer, read with ONE ranged GET of the object's tail -
// never the whole object and never its page index.
//
// The rule that governs every shortcut here: a count must never be doubled.
// An object that cannot be resolved therefore keeps its INFERRED MaxTimeNs in
// the watermark (the buffer is hidden up to the end of its hour until it
// resolves) instead of dropping out of it, and a failed read is not repeated on
// every query: it is retried with back-off.

const (
	// boundsResolveConcurrency caps the footer reads one watermark computation
	// runs at once.
	boundsResolveConcurrency = 8
	// boundsResolveTimeout is the budget of ONE watermark computation. A
	// query never waits longer than this on object-store reads that main does
	// not make at all; whatever is unresolved by then keeps its inferred end.
	boundsResolveTimeout = 2 * time.Second
	// A failed resolution of an object is not retried before boundsBackoffMin,
	// doubling per consecutive failure up to boundsBackoffMax.
	boundsBackoffMin = 5 * time.Second
	boundsBackoffMax = 5 * time.Minute
)

// nowFn is the clock the buffer-retention floor reads; tests replace it.
var nowFn = time.Now

type boundsRetry struct {
	until time.Time
	fails int
}

// boundsResolver is the per-Storage state of the resolution: the negative
// cache (back-off per object key) and the in-flight reads (one footer read per
// key at a time). The zero value is ready to use.
type boundsResolver struct {
	mu       sync.Mutex
	retry    map[string]boundsRetry
	inflight map[string]chan struct{}
}

// backedOff reports whether key's last resolution failed recently enough that
// it must not be tried again yet.
func (r *boundsResolver) backedOff(key string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return now.Before(r.retry[key].until)
}

func (r *boundsResolver) failed(key string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retry == nil {
		r.retry = make(map[string]boundsRetry)
	}
	st := r.retry[key]
	st.fails++
	d := boundsBackoffMin << (st.fails - 1)
	if d > boundsBackoffMax || d <= 0 {
		d = boundsBackoffMax
	}
	st.until = now.Add(d)
	r.retry[key] = st
}

func (r *boundsResolver) succeeded(key string) {
	r.mu.Lock()
	delete(r.retry, key)
	r.mu.Unlock()
}

// begin returns (nil, true) when the caller owns the read of key, or the
// channel to wait on when another caller is already reading it.
func (r *boundsResolver) begin(key string) (wait chan struct{}, owner bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch, ok := r.inflight[key]; ok {
		return ch, false
	}
	if r.inflight == nil {
		r.inflight = make(map[string]chan struct{})
	}
	r.inflight[key] = make(chan struct{})
	return nil, true
}

func (r *boundsResolver) end(key string) {
	r.mu.Lock()
	ch := r.inflight[key]
	delete(r.inflight, key)
	r.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// watermarkFloor is the oldest timestamp the buffer can still hold a row for:
// the later of the query's start and now - buffer retention. An object whose
// inferred end is below it cannot change what the buffer serves.
func (s *Storage) watermarkFloor(startNs int64) int64 {
	floor := startNs
	if s.cfg != nil && s.cfg.Insert.BufferRetention > 0 {
		if r := nowFn().Add(-s.cfg.Insert.BufferRetention).UnixNano(); r > floor {
			floor = r
		}
	}
	return floor
}

// withExactBounds returns files with the inferred bounds of every listed
// object replaced by exact ones where a source has them. Objects it cannot
// resolve within boundsResolveTimeout (or that are backed off) are returned
// unchanged, still BoundsInferred. The caller's slice is not modified.
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

	ctx, cancel := context.WithTimeout(ctx, boundsResolveTimeout)
	defer cancel()

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
	// The manifest may have been enriched since the caller took its copy.
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
	if ctx.Err() != nil || (s.pool == nil && s.footerCache == nil) {
		return current()
	}
	now := time.Now()
	if s.inferredBounds.backedOff(fi.Key, now) {
		return current()
	}
	wait, owner := s.inferredBounds.begin(fi.Key)
	if !owner {
		select {
		case <-wait:
		case <-ctx.Done():
		}
		return current()
	}
	defer s.inferredBounds.end(fi.Key)

	rows, minNs, maxNs, err := s.readFooterTimeBounds(ctx, fi)
	switch {
	case err == nil:
		s.manifest.EnrichFileMetadata(fi.Key, rows, minNs, maxNs)
		if cur := current(); !cur.BoundsInferred {
			s.inferredBounds.succeeded(fi.Key)
			return cur
		}
		// A range that is exactly the partition hour is not evidence either.
		s.inferredBounds.failed(fi.Key, now)
	case ctx.Err() == context.Canceled:
		// The caller went away: not the object's fault. (A deadline that ran
		// out DURING the read is: a slow object is backed off like a failing one.)
	default:
		s.inferredBounds.failed(fi.Key, now)
		logger.Warnf("buffer watermark: cannot read the footer of %s to resolve its time bounds (retrying with back-off): %s", fi.Key, err)
	}
	return current()
}

// readFooterTimeBounds reads the object's footer - from the footer cache, or
// with one ranged GET of its tail - and returns its row count and the min/max
// of the timestamp column from the column-chunk statistics. It never reads
// data pages or the page index.
func (s *Storage) readFooterTimeBounds(ctx context.Context, fi manifest.FileInfo) (rows, minNs, maxNs int64, err error) {
	var pf *parquet.File
	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached != nil {
			pf = cached.File
		}
	}
	if pf == nil {
		if s.pool == nil {
			return 0, 0, 0, errNoFooter
		}
		size := fi.Size
		if size <= 0 {
			return 0, 0, 0, errNoFooter
		}
		prefetch := s.footerPrefetchBytes()
		offset := size - footerPrefetchTail(prefetch, size)
		if offset < 0 {
			offset = 0
		}
		metrics.S3GetsByPhase.Inc("footer")
		tail, derr := s.pool.DownloadRange(ctx, fi.Key, offset, size-offset)
		if derr != nil {
			return 0, 0, 0, derr
		}
		if len(tail) < 8 {
			return 0, 0, 0, errNoFooter
		}
		footerLen, ferr := FooterLength(tail[len(tail)-8:])
		if ferr != nil {
			return 0, 0, 0, ferr
		}
		if footerLen+8 > len(tail) {
			return 0, 0, 0, errNoFooter // larger than one tail read: leave it unresolved
		}
		cached, parsed, perr := ParseFooterFromBytes(fi.Key, tail[len(tail)-(footerLen+8):], size)
		if perr != nil {
			return 0, 0, 0, perr
		}
		if s.footerCache != nil {
			s.footerCache.Put(fi.Key, cached)
		}
		pf = parsed
	}
	return footerTimeBounds(pf, findColumnIndex(pf.Root(), s.registry.TimestampColumn()))
}

type boundsError string

func (e boundsError) Error() string { return string(e) }

const errNoFooter = boundsError("no usable footer")

// footerTimeBounds returns the row count and the min/max of leaf column tsIdx
// over every row group, from the column-chunk statistics in the footer. A row
// group without usable int64 statistics makes the whole answer unavailable: a
// partial range is worse than none.
func footerTimeBounds(pf *parquet.File, tsIdx int) (rows, minNs, maxNs int64, err error) {
	md := pf.Metadata()
	if md == nil || tsIdx < 0 {
		return 0, 0, 0, errNoFooter
	}
	have := false
	for i := range md.RowGroups {
		rg := &md.RowGroups[i]
		if rg.NumRows == 0 {
			continue
		}
		if tsIdx >= len(rg.Columns) {
			return 0, 0, 0, errNoFooter
		}
		st := rg.Columns[tsIdx].MetaData.Statistics
		mn, mx := st.MinValue, st.MaxValue
		if len(mn) != 8 || len(mx) != 8 {
			mn, mx = st.Min, st.Max // the deprecated fields older writers fill
		}
		if len(mn) != 8 || len(mx) != 8 {
			return 0, 0, 0, errNoFooter
		}
		lo, hi := int64(binary.LittleEndian.Uint64(mn)), int64(binary.LittleEndian.Uint64(mx))
		if !have || lo < minNs {
			minNs = lo
		}
		if !have || hi > maxNs {
			maxNs = hi
		}
		have = true
		rows += rg.NumRows
	}
	if !have || minNs <= 0 || maxNs < minNs {
		return 0, 0, 0, errNoFooter
	}
	return rows, minNs, maxNs, nil
}

// recentInferredWindow and recentInferredMaxFiles bound the startup pass that
// resolves inferred bounds (enrichRecentInferredBounds): the buffer only ever
// holds recent rows, so older objects wait for the lazy path.
const (
	recentInferredWindow   = 6 * time.Hour
	recentInferredMaxFiles = 512
	// recentInferredBudget bounds the whole startup pass.
	recentInferredBudget = 30 * time.Second
)

// enrichRecentInferredBounds resolves the inferred bounds of the recent
// objects at startup (ranged footer reads only), so the first query after a
// restart does not pay for it. It returns how many objects now have exact
// bounds.
func (s *Storage) enrichRecentInferredBounds(ctx context.Context) int {
	if s.manifest == nil {
		return 0
	}
	now := nowFn()
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
	ctx, cancel := context.WithTimeout(ctx, recentInferredBudget)
	defer cancel()
	taskCh := make(chan manifest.FileInfo, len(todo))
	for _, fi := range todo {
		taskCh <- fi
	}
	close(taskCh)
	var wg sync.WaitGroup
	for w := 0; w < boundsResolveConcurrency && w < len(todo); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}
				s.resolveFileBounds(ctx, fi)
			}
		}()
	}
	wg.Wait()
	resolved := 0
	for _, fi := range todo {
		if cur, ok := s.manifest.GetFileByKey(fi.Key); ok && !cur.BoundsInferred {
			resolved++
		}
	}
	return resolved
}
