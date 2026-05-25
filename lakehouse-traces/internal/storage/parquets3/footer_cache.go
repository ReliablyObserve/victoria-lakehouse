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
	fc.mu.RLock()
	entry, ok := fc.items[key]
	fc.mu.RUnlock()
	if !ok {
		return nil, false
	}
	fc.mu.Lock()
	fc.lru.MoveToFront(entry.elem)
	fc.mu.Unlock()
	metrics.FooterCacheHits.Inc()
	return entry.footer, true
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

// ParseFooterFromBytes parses just the parquet footer from raw footer bytes.
// footerBytes should contain the last N bytes of the file including the
// 4-byte footer length and 4-byte magic number. Uses a synthetic ReaderAt
// that serves "PAR1" at offset 0 and the footer at the file tail, so
// parquet-go's magic validation succeeds without downloading the full file.
func ParseFooterFromBytes(key string, footerBytes []byte, fileSize int64) (*CachedFooter, *parquet.File, error) {
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
