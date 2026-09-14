package parquets3

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type CachedFooter struct {
	File       *parquet.File
	FileSize   int64
	footerSize int
}

type footerEntry struct {
	key    string
	footer *CachedFooter
	elem   *list.Element
}

type FooterCache struct {
	mu       sync.RWMutex
	items    map[string]*footerEntry
	lru      *list.List
	maxItems int
}

func NewFooterCache(maxItems int) *FooterCache {
	if maxItems <= 0 {
		maxItems = 10000
	}
	return &FooterCache{
		items:    make(map[string]*footerEntry, maxItems),
		lru:      list.New(),
		maxItems: maxItems,
	}
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

func (fc *FooterCache) Put(key string, footer *CachedFooter) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if entry, ok := fc.items[key]; ok {
		entry.footer = footer
		fc.lru.MoveToFront(entry.elem)
		return
	}

	for fc.lru.Len() >= fc.maxItems {
		back := fc.lru.Back()
		if back == nil {
			break
		}
		evicted := back.Value.(*footerEntry)
		fc.lru.Remove(back)
		delete(fc.items, evicted.key)
		metrics.FooterCacheEvictions.Inc()
	}

	entry := &footerEntry{key: key, footer: footer}
	entry.elem = fc.lru.PushFront(entry)
	fc.items[key] = entry
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
		fc.lru.Remove(entry.elem)
		delete(fc.items, key)
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
// entry's — the entry's handle reads the column-data region as zeros, the same
// as an entry built by the footer prefetch.
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
// zero-fills whatever part of that request falls in the synthetic "gap"
// region (the fictional column-data span between the magic bytes and the
// real footer) byte-by-byte, a manual loop the compiler doesn't
// vectorize. With a large fileSize, that gap can span most of the file,
// so a declared length that outruns the actual footer buffer turns one
// call into a multi-second, multi-GiB-touching loop — that's the
// resource blow-up that hung FuzzParseFooterBytes, not the bare
// allocation (a large make() alone is typically a cheap virtual-memory
// reservation). Bounding the declared length against the buffer we
// actually hold — before ever calling into parquet-go — is what prevents
// it: every read parquet-go can then issue lands entirely inside the real
// footer bytes (a cheap copy()), never the gap loop, regardless of how
// large fileSize is. The additional maxParquetFooterBytes cap and the
// fileSize comparison below are defense-in-depth policy limits, not this
// hang guard.
func ParseFooterFromBytes(key string, footerBytes []byte, fileSize int64) (cachedFooter *CachedFooter, file *parquet.File, err error) {
	if fileSize <= 0 {
		metrics.FooterParseRejected.Inc("invalid_file_size")
		return nil, nil, fmt.Errorf("parse parquet footer %s: invalid file size %d", key, fileSize)
	}
	if int64(len(footerBytes)) > fileSize {
		metrics.FooterParseRejected.Inc("footer_exceeds_file_size")
		return nil, nil, fmt.Errorf("parse parquet footer %s: footer bytes (%d) exceed file size (%d)", key, len(footerBytes), fileSize)
	}
	if len(footerBytes) < 8 {
		metrics.FooterParseRejected.Inc("too_short")
		return nil, nil, fmt.Errorf("parse parquet footer %s: need at least 8 bytes, got %d", key, len(footerBytes))
	}
	// FooterLength decodes the declared length and validates the "PAR1"
	// magic in one step (see below).
	declaredLenInt, lenErr := FooterLength(footerBytes[len(footerBytes)-8:])
	if lenErr != nil {
		metrics.FooterParseRejected.Inc("bad_magic")
		return nil, nil, fmt.Errorf("parse parquet footer %s: %w", key, lenErr)
	}
	declaredLen := int64(declaredLenInt)
	// The real hang guard: parquet-go can never be made to read (and our
	// footerReaderAt can never be made to gap-fill) more than the buffer
	// we already hold.
	if declaredLen+8 > int64(len(footerBytes)) {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_buffer")
		return nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds available buffer (%d bytes)", key, declaredLen, len(footerBytes)-8)
	}
	// Implied by the buffer bound above (footerBytes is already known to
	// fit within fileSize) but checked explicitly so this invariant holds
	// even if the buffer-bound check above is ever loosened or reordered.
	if declaredLen > fileSize {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_file_size")
		return nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds file size (%d)", key, declaredLen, fileSize)
	}
	if declaredLen > maxParquetFooterBytes {
		metrics.FooterParseRejected.Inc("declared_length_exceeds_cap")
		logger.Warnf("footer parse: declared footer length %d for %s exceeds policy cap %d bytes — rejecting (a legitimate file this large needs the cap raised)", declaredLen, key, maxParquetFooterBytes)
		return nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds max %d", key, declaredLen, maxParquetFooterBytes)
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
			cachedFooter, file = nil, nil
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
		return nil, nil, fmt.Errorf("parse parquet footer %s: %w", key, err)
	}
	return &CachedFooter{
		File:       f,
		FileSize:   fileSize,
		footerSize: len(footerBytes),
	}, f, nil
}

// footerReaderAt serves a minimal virtual parquet file: "PAR1" magic at
// offset 0 and the real footer bytes at the file tail. Requests for bytes
// in the gap (column data region) return zeros — those offsets are never
// read during metadata-only parsing.
type footerReaderAt struct {
	footer   []byte
	fileSize int64
}

func (r *footerReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= r.fileSize {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && off+int64(n) < r.fileSize {
		pos := off + int64(n)
		if pos < 4 {
			magic := []byte("PAR1")
			end := int64(4)
			if end > r.fileSize {
				end = r.fileSize
			}
			copied := copy(p[n:], magic[pos:end])
			n += copied
			continue
		}

		footerStart := r.fileSize - int64(len(r.footer))
		if pos >= footerStart {
			idx := pos - footerStart
			copied := copy(p[n:], r.footer[idx:])
			n += copied
			continue
		}

		gapEnd := footerStart
		if off+int64(len(p)) < gapEnd {
			gapEnd = off + int64(len(p))
		}
		gapBytes := int(gapEnd - pos)
		for i := 0; i < gapBytes && n < len(p); i++ {
			p[n] = 0
			n++
		}
	}

	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
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
