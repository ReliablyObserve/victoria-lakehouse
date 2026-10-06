package parquets3

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// CachedFooter is one footer-cache entry: the parsed footer plus the raw tail
// bytes behind it.
//
// tail holds the object's last len(tail) bytes: always the footer, and — when
// the cache writer could see it — the page-index stripe (ColumnIndex and
// OffsetIndex sections) that sits just before the footer. The parsed File
// reads its lazy metadata through those same bytes, and the overlay reader
// serves them to the query-time open, so a cached file opens with no S3 round
// trip and a ColumnIndex()/OffsetIndex() call never touches S3 either.
// Entries are immutable once cached.
type CachedFooter struct {
	File       *parquet.File
	FileSize   int64
	footerSize int
	chunks     int // column chunks over all row groups (what the weight model scales by)

	tail    []byte
	tailOff int64
}

// Resident-size model of one cache entry, fitted to measured heap per entry
// (TestCachedFooterWeightCalibration re-measures it on every run with 1, 8 and
// 40 row groups, fresh and with every page index decoded, and fails when the
// model is below the heap or above 1.5x of it):
//
//	resident ~ len(tail) + retainedMetadataFactor*footerSize
//	         + chunkGraphBytes*columnChunks + cachedFooterOverhead
//
// len(tail) is the raw footer + page-index stripe the entry owns;
// retainedMetadataFactor is the decoded thrift metadata (KV strings, row-group
// and column structs) per footer byte; columnChunks is row groups x columns:
// parquet-go keeps a column-chunk graph per chunk (about 22 KiB per row group
// of the 48-column logs schema) and, once a query decoded the page index,
// memoizes the column and offset index of every chunk on the cached file (a
// further 17-37 KiB per row group), so chunkGraphBytes covers the
// index-decoded state: the entry grows after it was charged; cachedFooterOverhead
// is the fixed per-file part (column tree of the parsed *parquet.File, list
// element, map slot).
const (
	retainedMetadataFactor = 2.25
	chunkGraphBytes        = 1088
	cachedFooterOverhead   = 26 << 10
)

// Weight is the number of resident bytes the entry is charged against the
// cache budget. An entry that kept the whole object (the whole-file fallback)
// is charged the object size.
func (c *CachedFooter) Weight() int64 {
	if c == nil {
		return 0
	}
	if len(c.tail) == 0 {
		// Fallback entry whose handle retains the whole object.
		if c.FileSize > 0 {
			return c.FileSize + cachedFooterOverhead
		}
		return cachedFooterOverhead
	}
	return int64(len(c.tail)) + int64(retainedMetadataFactor*float64(c.footerSize)) +
		chunkGraphBytes*int64(c.chunks) + cachedFooterOverhead
}

// Tail returns the cached tail bytes and the object offset they start at.
// The slice is shared and read-only. The tail is nil when the entry holds
// none.
func (c *CachedFooter) Tail() ([]byte, int64) {
	if c == nil {
		return nil, 0
	}
	return c.tail, c.tailOff
}

// HasPageIndex reports whether the cached tail covers the whole page-index
// stripe, i.e. every ColumnIndex()/OffsetIndex() read is served from memory.
// A file without a page index counts as covered.
func (c *CachedFooter) HasPageIndex() bool {
	if c == nil || len(c.tail) == 0 || c.File == nil {
		return false
	}
	start := pageIndexStripeStart(c.File)
	return start < 0 || start >= c.tailOff
}

// needsPageIndex reports whether the entry reads its page index through a
// cached tail that does not cover it: a ColumnIndex()/OffsetIndex() on its file
// fails (errOutsideCachedRange). An entry holding the whole object, or a file
// without a page index, does not.
func (c *CachedFooter) needsPageIndex() bool {
	return c != nil && c.File != nil && len(c.tail) > 0 && !c.HasPageIndex()
}

// pageIndexStripeStart returns the smallest ColumnIndex/OffsetIndex offset in
// the footer metadata, or -1 when the file has no page index.
func pageIndexStripeStart(f *parquet.File) int64 {
	md := f.Metadata()
	if md == nil {
		return -1
	}
	start := int64(-1)
	for i := range md.RowGroups {
		for j := range md.RowGroups[i].Columns {
			c := &md.RowGroups[i].Columns[j]
			for _, o := range [...]int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if o > 0 && (start < 0 || o < start) {
					start = o
				}
			}
		}
	}
	return start
}

type footerEntry struct {
	key    string
	footer *CachedFooter
	weight int64
	elem   *list.Element
}

// FooterCache is an LRU of parsed footers bounded by RESIDENT BYTES, not by
// entry count. A count bound hides the real cost: a footer with token blooms
// or a trace index runs to hundreds of KB, so "10,000 entries" ranged from
// ~50 MB to several GB depending on the data. Eviction runs until the new
// entry fits; an entry larger than the whole budget is not cached.
type FooterCache struct {
	mu       sync.RWMutex
	items    map[string]*footerEntry
	lru      *list.List
	maxBytes int64
	bytes    int64
}

// NewFooterCache returns a cache holding at most maxBytes of footers;
// maxBytes <= 0 selects the auto budget (defaultFooterMaxBytes: a share of
// memory.Allowed(), clamped per signal).
func NewFooterCache(maxBytes int64) *FooterCache {
	if maxBytes <= 0 {
		maxBytes = defaultFooterMaxBytes()
	}
	return &FooterCache{
		items:    make(map[string]*footerEntry),
		lru:      list.New(),
		maxBytes: maxBytes,
	}
}

// newConfiguredFooterCache builds the footer cache from cache.footer_max_bytes
// (an explicit value is used as given; 0 = auto, see autoFooterMaxBytes) and
// logs the resolved budget once at startup.
func newConfiguredFooterCache(cfg *config.Config) *FooterCache {
	configured := int64(cfg.Cache.FooterMaxBytes)
	fc := NewFooterCache(configured)
	if configured > 0 {
		logger.Infof("footer cache budget: %d MiB (cache.footer_max_bytes)", fc.MaxBytes()>>20)
	} else {
		logger.Infof("footer cache budget: %d MiB (auto: %d%% of the %d MiB the process may use for caches, clamped to [%d MiB, %d MiB]; set cache.footer_max_bytes to override)",
			fc.MaxBytes()>>20, footerBudgetPercent, int64(memory.Allowed())>>20, footerBudgetMin>>20, int64(footerBudgetMax)>>20)
	}
	return fc
}

func (fc *FooterCache) Get(key string) (*CachedFooter, bool) {
	fc.mu.Lock()
	entry, ok := fc.items[key]
	if !ok {
		fc.mu.Unlock()
		return nil, false
	}
	fc.lru.MoveToFront(entry.elem)
	footer := entry.footer
	fc.mu.Unlock()
	metrics.FooterCacheHits.Inc()
	return footer, true
}

// GetFor is Get for a caller that knows the object's current size (the
// manifest's). An entry cached for another size belongs to another version of
// the key: it is removed and reported as a miss, so a stale schema never
// reaches the file open.
func (fc *FooterCache) GetFor(key string, size int64) (*CachedFooter, bool) {
	fc.mu.Lock()
	entry, ok := fc.items[key]
	if !ok {
		fc.mu.Unlock()
		return nil, false
	}
	if entry.footer.FileSize != size {
		fc.removeLocked(entry)
		fc.publishLocked()
		fc.mu.Unlock()
		return nil, false
	}
	fc.lru.MoveToFront(entry.elem)
	footer := entry.footer
	fc.mu.Unlock()
	metrics.FooterCacheHits.Inc()
	return footer, true
}

// Put caches footer under key, evicting least-recently-used entries until it
// fits the byte budget. A footer heavier than the whole budget is dropped
// (and any older entry for the key removed).
func (fc *FooterCache) Put(key string, footer *CachedFooter) {
	w := footer.Weight()
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if entry, ok := fc.items[key]; ok {
		fc.removeLocked(entry)
	}
	if w > fc.maxBytes {
		fc.publishLocked()
		return
	}
	for fc.bytes+w > fc.maxBytes {
		back := fc.lru.Back()
		if back == nil {
			break
		}
		fc.removeLocked(back.Value.(*footerEntry))
		metrics.FooterCacheEvictions.Inc()
	}
	entry := &footerEntry{key: key, footer: footer, weight: w}
	entry.elem = fc.lru.PushFront(entry)
	fc.items[key] = entry
	fc.bytes += w
	fc.publishLocked()
}

// PutIfFits caches footer only when it fits the free budget without evicting
// anything; it reports whether it did. For a bulk fetch whose entries are
// consumed by the caller directly, so what the cache keeps is a bonus that must
// not push out entries other work still needs.
func (fc *FooterCache) PutIfFits(key string, footer *CachedFooter) bool {
	w := footer.Weight()
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if entry, ok := fc.items[key]; ok {
		fc.removeLocked(entry)
	}
	if fc.bytes+w > fc.maxBytes {
		fc.publishLocked()
		return false
	}
	entry := &footerEntry{key: key, footer: footer, weight: w}
	entry.elem = fc.lru.PushFront(entry)
	fc.items[key] = entry
	fc.bytes += w
	fc.publishLocked()
	return true
}

func (fc *FooterCache) removeLocked(e *footerEntry) {
	fc.lru.Remove(e.elem)
	delete(fc.items, e.key)
	fc.bytes -= e.weight
}

func (fc *FooterCache) publishLocked() {
	metrics.FooterCacheEntries.Set(int64(len(fc.items)))
	metrics.FooterCacheBytes.Set(fc.bytes)
}

// Bytes returns the resident bytes currently charged to the cache.
func (fc *FooterCache) Bytes() int64 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.bytes
}

// AvgEntryBytes is the mean resident charge of a cached footer (0 when empty).
func (fc *FooterCache) AvgEntryBytes() int64 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	if len(fc.items) == 0 {
		return 0
	}
	return fc.bytes / int64(len(fc.items))
}

// MaxBytes returns the byte budget.
func (fc *FooterCache) MaxBytes() int64 {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.maxBytes
}

// Has returns true if the key is present in the cache without modifying LRU
// order or incrementing hit metrics. Used for cache-affinity sorting.
func (fc *FooterCache) Has(key string) bool {
	fc.mu.RLock()
	_, ok := fc.items[key]
	fc.mu.RUnlock()
	return ok
}

func (fc *FooterCache) Len() int {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return len(fc.items)
}

// Keys returns the keys currently in the cache in most-recently-used
// order (front of the LRU first). Used at shutdown to persist a
// snapshot of cached file keys so the next process can asynchronously
// re-prefetch their footers from S3 — the cold-start version of the
// "instant after restart" experience.
//
// The slice is a fresh copy; callers may modify it. No effect on LRU
// order or hit metrics.
func (fc *FooterCache) Keys() []string {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	out := make([]string, 0, fc.lru.Len())
	for e := fc.lru.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*footerEntry).key)
	}
	return out
}

func (fc *FooterCache) Remove(key string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if entry, ok := fc.items[key]; ok {
		fc.removeLocked(entry)
		fc.publishLocked()
	}
}

// ParseFooterFromData opens a downloaded object and returns two things: the
// cache entry for its metadata, and a handle over the downloaded bytes for the
// caller's own reads (row groups, page index, column data).
//
// The cache entry keeps ONLY a copy of the footer. A CachedFooter holding a
// handle over the object body keeps that body alive for as long as the entry
// stays in the footer cache, and the cache is bounded by item count, not bytes:
// 10,000 entries of multi-MiB objects is gigabytes of retained heap. Callers
// that need to read data must use the returned *parquet.File, never the
// entry's — the entry's handle fails a read of the column-data region with
// errOutsideCachedRange, the same as an entry built by the footer prefetch.
func ParseFooterFromData(key string, data []byte) (*CachedFooter, *parquet.File, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("open parquet file %s: %w", key, err)
	}
	if tail, terr := footerTailOfObject(data, f); terr == nil {
		if cached, _, ferr := ParseFooterFromBytes(key, tail, int64(len(data))); ferr == nil {
			return cached, f, nil
		}
	}
	// The object parsed but its footer did not re-parse on its own (a shape the
	// footer prefetch would also fail on). Cache the handle we have rather than
	// failing the read; it retains the object, which is why it is the fallback.
	metrics.FooterParseRejected.Inc("footer_copy_failed")
	return &CachedFooter{
		File:     f,
		FileSize: int64(len(data)),
	}, f, nil
}

// parseObjectFor is ParseFooterFromData for a caller that holds the object's
// bytes and may already have its footer cached. A cached entry for exactly this
// object (same key and size, immutable) that already holds the page index
// needs no replacement, so the second footer parse, the tail copy and the Put
// are skipped and fresh is false: the caller keeps the entry it has. That work
// was ~220 us and 346 KB of allocations per file per query on the facets
// whole-object path, on top of parquet.OpenFile's own parse of the same
// footer. Without a usable entry it is exactly ParseFooterFromData and fresh
// is true (the caller Puts the returned entry).
func parseObjectFor(fc *FooterCache, key string, data []byte) (cached *CachedFooter, f *parquet.File, fresh bool, err error) {
	if fc != nil {
		if have, ok := fc.GetFor(key, int64(len(data))); ok && have.File != nil && (len(have.tail) == 0 || have.HasPageIndex()) {
			f, err = parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				return nil, nil, false, fmt.Errorf("open parquet file %s: %w", key, err)
			}
			return have, f, false, nil
		}
	}
	cached, f, err = ParseFooterFromData(key, data)
	return cached, f, true, err
}

// footerTailOfObject returns a COPY of the object's metadata tail: the page
// index (column + offset index, written right before the footer) and the footer
// itself. The copy matters — a sub-slice of the object keeps the whole object
// alive — and so does including the page index: per-page null counts and
// min/max bounds live there, and field_names' hit counts and the manifest
// enrichment's time bounds read them through the cached handle.
func footerTailOfObject(data []byte, f *parquet.File) ([]byte, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("object is %d bytes, too short for a parquet footer", len(data))
	}
	footerLen, err := FooterLength(data[len(data)-8:])
	if err != nil {
		return nil, err
	}
	total := footerLen + 8
	if total > len(data) {
		return nil, fmt.Errorf("declared footer length %d exceeds the object (%d bytes)", footerLen, len(data))
	}
	start := int64(len(data) - total)
	if md := f.Metadata(); md != nil {
		for i := range md.RowGroups {
			for j := range md.RowGroups[i].Columns {
				c := &md.RowGroups[i].Columns[j]
				for _, off := range [...]int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
					if off > 0 && off < start {
						start = off
					}
				}
			}
		}
	}
	if start < 0 || start > int64(len(data)) {
		start = int64(len(data) - total)
	}
	return bytes.Clone(data[start:]), nil
}

// maxParquetFooterBytes is a policy cap on the trusted parquet footer
// size, not the hang guard (the buffer bound below is). Trace L2 files
// carry a `_trace_idx` footer KV that reaches ~11 MB at 512 MB objects and
// scales up to ~39 MB at traceindex.maxEntries (1<<20 entries x ~37 bytes
// each); 64 MiB leaves headroom over that plus row-group metadata for
// wide schemas, while remaining far below the ~4 GiB an
// attacker/corruption-controlled uint32 length field could otherwise
// claim.
const maxParquetFooterBytes = 64 * 1024 * 1024

// ParseFooterFromBytes parses just the parquet footer from raw footer bytes.
// footerBytes should contain the last N bytes of the file including the
// 4-byte footer length and 4-byte magic number. Uses a synthetic ReaderAt
// that serves "PAR1" at offset 0 and the footer at the file tail, so
// parquet-go's magic validation succeeds without downloading the full file.
//
// The trailing 8 bytes of footerBytes declare a footer length that
// parquet-go's own OpenFile independently re-parses from those same bytes.
// When that declared length exceeds what OpenFile already read
// optimistically, it issues a *single* read for the whole declared
// length (parquet-go's file.go) — and our own footerReaderAt.ReadAt
// used to zero-fill whatever part of that request fell in the synthetic
// "gap" region (the fictional column-data span between the magic bytes and
// the real footer) byte-by-byte; with a large fileSize that turned a declared
// length outrunning the footer buffer into a multi-second, multi-GiB loop
// (the resource blow-up that hung FuzzParseFooterBytes). The reader now fails
// such a read with errOutsideCachedRange instead, so there is no gap loop; the
// bound on the declared length against the buffer we actually hold is kept,
// as is the maxParquetFooterBytes cap and the fileSize comparison below
// (defense-in-depth policy limits).
func ParseFooterFromBytes(key string, footerBytes []byte, fileSize int64) (cachedFooter *CachedFooter, file *parquet.File, err error) {
	cachedFooter, file, _, err = parseFooterBytes(key, footerBytes, fileSize)
	return cachedFooter, file, err
}

// parseFooterBytes is ParseFooterFromBytes that also returns the synthetic
// reader behind the parsed file, so cacheFooterFromTail can re-point it at an
// owned copy of the bytes before the entry is published.
func parseFooterBytes(key string, footerBytes []byte, fileSize int64) (cachedFooter *CachedFooter, file *parquet.File, rd *footerReaderAt, err error) {
	if fileSize <= 0 {
		metrics.FooterParseRejected.Inc("invalid_file_size")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: invalid file size %d", key, fileSize)
	}
	if int64(len(footerBytes)) > fileSize {
		metrics.FooterParseRejected.Inc("footer_exceeds_file_size")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: footer bytes (%d) exceed file size (%d)", key, len(footerBytes), fileSize)
	}
	if len(footerBytes) < 8 {
		metrics.FooterParseRejected.Inc("too_short")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: need at least 8 bytes, got %d", key, len(footerBytes))
	}
	// FooterLength decodes the declared length and validates the "PAR1"
	// magic in one step (see below).
	declaredLenInt, lenErr := FooterLength(footerBytes[len(footerBytes)-8:])
	if lenErr != nil {
		metrics.FooterParseRejected.Inc("bad_magic")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: %w", key, lenErr)
	}
	declaredLen := int64(declaredLenInt)
	// The real hang guard: parquet-go can never be made to read (and our
	// footerReaderAt can never be made to gap-fill) more than the buffer
	// we already hold.
	if declaredLen+8 > int64(len(footerBytes)) {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_buffer")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds available buffer (%d bytes)", key, declaredLen, len(footerBytes)-8)
	}
	// Implied by the buffer bound above (footerBytes is already known to
	// fit within fileSize) but checked explicitly so this invariant holds
	// even if the buffer-bound check above is ever loosened or reordered.
	if declaredLen > fileSize {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_file_size")
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds file size (%d)", key, declaredLen, fileSize)
	}
	if declaredLen > maxParquetFooterBytes {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_cap")
		logger.Warnf("footer parse: declared footer length %d for %s exceeds policy cap %d bytes — rejecting (a legitimate file this large needs the cap raised)", declaredLen, key, maxParquetFooterBytes)
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds max %d", key, declaredLen, maxParquetFooterBytes)
	}

	// parquet-go's thrift-decoded metadata carries other untrusted integer
	// fields we don't independently validate (e.g. SchemaElement.NumChildren
	// feeding a raw `make([]*Column, numChildren)` in column.go) — a crafted
	// negative or absurd value there panics inside parquet-go rather than
	// returning an error. Recover so a malformed/adversarial footer can
	// never crash the caller; the fuzz harness's contract ("must never
	// panic, error is fine") holds even for bugs in the vendored decoder.
	defer func() {
		if rec := recover(); rec != nil {
			metrics.FooterParseRejected.Inc("decoder_panic")
			cachedFooter, file, rd = nil, nil, nil
			err = fmt.Errorf("parse parquet footer %s: panic in parquet-go decoder: %v", key, rec)
		}
	}()

	r := &footerReaderAt{
		footer:   footerBytes,
		fileSize: fileSize,
	}
	f, err := parquet.OpenFile(r, fileSize, &parquet.FileConfig{
		SkipPageIndex:    true,
		SkipBloomFilters: true,
		SkipMagicBytes:   true,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse parquet footer %s: %w", key, err)
	}
	chunks := 0
	if md := f.Metadata(); md != nil {
		for i := range md.RowGroups {
			chunks += len(md.RowGroups[i].Columns)
		}
	}
	return &CachedFooter{
		File:       f,
		FileSize:   fileSize,
		footerSize: int(declaredLen) + 8,
		chunks:     chunks,
		tail:       footerBytes,
		tailOff:    fileSize - int64(len(footerBytes)),
	}, f, r, nil
}

// errOutsideCachedRange is what footerReaderAt returns for a read that is not
// wholly inside the bytes the cache entry holds (the head magic and the cached
// tail). A caller that sees it knows the answer is not in memory; it must fetch
// the bytes or treat the value as unknown, never read zeros as data. The
// zero-filling reader this replaces decoded a page index it did not have as an
// EMPTY one, and parquet-go memoized that on the shared cached file: every
// later ColumnIndex() on it reported "no pages" until the entry was evicted.
var errOutsideCachedRange = errors.New("read outside the cached footer ranges")

// footerReaderAt serves a minimal virtual parquet file: "PAR1" magic at
// offset 0 and the cached tail (footer, plus the page-index stripe when the
// writer of the entry could see it) at the end of the file. A read that is not
// wholly inside one of those two ranges fails with errOutsideCachedRange.
type footerReaderAt struct {
	footer   []byte
	fileSize int64
}

func (r *footerReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.fileSize {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	end := off + int64(len(p))
	if end > r.fileSize {
		end = r.fileSize
	}
	footerStart := r.fileSize - int64(len(r.footer))
	switch {
	case off >= footerStart:
		// A read running past the end of the object is short with no error,
		// as it always was; parquet-go sizes its reads to the file.
		return copy(p, r.footer[off-footerStart:]), nil
	case end <= 4:
		return copy(p, "PAR1"[off:end]), nil
	}
	return 0, fmt.Errorf("%w: [%d,%d) of %d (cached tail starts at %d)", errOutsideCachedRange, off, end, r.fileSize, footerStart)
}

// FooterLength reads the parquet footer length from the last 8 bytes of a file.
// Returns the footer length (excluding the 8-byte suffix).
func FooterLength(tail8 []byte) (int, error) {
	if len(tail8) < 8 {
		return 0, fmt.Errorf("need 8 bytes, got %d", len(tail8))
	}
	magic := string(tail8[4:8])
	if magic != "PAR1" {
		return 0, fmt.Errorf("not a parquet file (magic=%q)", magic)
	}
	footerLen := int(binary.LittleEndian.Uint32(tail8[0:4]))
	return footerLen, nil
}
