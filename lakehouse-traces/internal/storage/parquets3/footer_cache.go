package parquets3

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

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

// Resize updates the cache cap. Used by the auto-tune ticker called
// after RefreshFromS3 so the cache size tracks the manifest's file
// count instead of being pinned at the startup value.
//
// Safe for concurrent use. Returns the number of evictions performed.
func (fc *FooterCache) Resize(newMax int) int {
	if newMax <= 0 {
		return 0
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if newMax == fc.maxItems {
		return 0
	}
	fc.maxItems = newMax
	evicted := 0
	for fc.lru.Len() > newMax {
		back := fc.lru.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*footerEntry)
		fc.lru.Remove(back)
		delete(fc.items, entry.key)
		metrics.FooterCacheEvictions.Inc()
		evicted++
	}
	return evicted
}

// MaxItems returns the current cap. Exposed for the /lakehouse stats
// API so operators can verify auto-tuning is taking effect.
func (fc *FooterCache) MaxItems() int {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.maxItems
}

func (fc *FooterCache) Remove(key string) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	if entry, ok := fc.items[key]; ok {
		fc.lru.Remove(entry.elem)
		delete(fc.items, key)
	}
}

// ParseFooterFromData creates a CachedFooter by parsing only the parquet metadata
// from the end of a full file's data. This avoids re-parsing on subsequent accesses.
func ParseFooterFromData(key string, data []byte) (*CachedFooter, *parquet.File, error) {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("open parquet file %s: %w", key, err)
	}
	return &CachedFooter{
		File:     f,
		FileSize: int64(len(data)),
	}, f, nil
}

// maxParquetFooterBytes bounds the trusted parquet footer size. Measured
// footers in this project top out at 467-519KB (traces L2 files with an
// embedded trace index — see footer_prefetch.go); 16 MiB leaves >30x
// headroom for legitimately wide schemas while staying far below the
// ~4 GiB an attacker/corruption-controlled uint32 length field could
// otherwise claim.
const maxParquetFooterBytes = 16 * 1024 * 1024

// ParseFooterFromBytes parses just the parquet footer from raw footer bytes.
// footerBytes should contain the last N bytes of the file including the
// 4-byte footer length and 4-byte magic number. Uses a synthetic ReaderAt
// that serves "PAR1" at offset 0 and the footer at the file tail, so
// parquet-go's magic validation succeeds without downloading the full file.
//
// The trailing 8 bytes of footerBytes declare a footer length that
// parquet-go's own OpenFile independently re-parses from those same bytes
// and uses to size an allocation (make([]byte, footerSize) in
// parquet-go's file.go) — that length is a raw little-endian uint32
// (0..~4 GiB) fully controlled by whoever produced footerBytes. Validate
// it here — bounded against the buffer we actually hold and a sane
// maximum — before ever calling into parquet-go, so a malformed or
// malicious length can never drive a multi-GiB allocation. Without this,
// footer bytes whose embedded length vastly exceeds the buffer make
// parquet-go allocate up to ~4 GiB per call; repeated across fuzz/query
// executions that is exactly the resource blow-up that hung the logs
// module's FuzzParseFooterBytes (this twin fixes the same shared bug).
func ParseFooterFromBytes(key string, footerBytes []byte, fileSize int64) (cachedFooter *CachedFooter, file *parquet.File, err error) {
	if fileSize <= 0 {
		return nil, nil, fmt.Errorf("parse parquet footer %s: invalid file size %d", key, fileSize)
	}
	if int64(len(footerBytes)) > fileSize {
		return nil, nil, fmt.Errorf("parse parquet footer %s: footer bytes (%d) exceed file size (%d)", key, len(footerBytes), fileSize)
	}
	if len(footerBytes) < 8 {
		return nil, nil, fmt.Errorf("parse parquet footer %s: need at least 8 bytes, got %d", key, len(footerBytes))
	}
	declaredLen := int64(binary.LittleEndian.Uint32(footerBytes[len(footerBytes)-8 : len(footerBytes)-4]))
	if declaredLen+8 > int64(len(footerBytes)) {
		return nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds available buffer (%d bytes)", key, declaredLen, len(footerBytes)-8)
	}
	if declaredLen > maxParquetFooterBytes {
		return nil, nil, fmt.Errorf("parse parquet footer %s: declared footer length %d exceeds max %d", key, declaredLen, maxParquetFooterBytes)
	}

	// parquet-go's thrift-decoded metadata carries other untrusted integer
	// fields we don't independently validate (e.g. SchemaElement.NumChildren
	// feeding a raw `make([]*Column, numChildren)` in column.go) — a crafted
	// negative or absurd value there panics inside parquet-go rather than
	// returning an error. Recover so a malformed/adversarial footer can
	// never crash the caller.
	defer func() {
		if rec := recover(); rec != nil {
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
