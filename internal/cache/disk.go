package cache

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	vlfs "github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/resourcebounds"
)

type diskEntry struct {
	key  string
	path string
	size int64
	// boundRelease releases the K8s-style smart-cache-disk Bound slot
	// reserved when this entry was admitted. Nil for entries admitted
	// before SetBound was called or for entries that arrived via paths
	// where the disk bound is intentionally bypassed (no current
	// callers — kept for symmetry with cache/lru.go).
	boundRelease func()
}

type DiskCache struct {
	mu        sync.Mutex
	dir       string
	items     map[string]*list.Element
	order     *list.List
	curSize   int64
	maxSize   int64
	watermark float64
	hits      uint64
	misses    uint64
	evictions uint64
	// bound is the K8s-style smart-cache-disk admission gate, set via
	// SetBound at storage construction. See cache/lru.go SetBound docs
	// — same semantics: TryAcquire on Put, Release on evict/Delete/Clear,
	// nil bound preserves pre-bound behaviour exactly.
	bound    *resourcebounds.Bound
	rejected uint64
}

// writeFileAtomic wraps vlfs.MustWriteAtomic (which panics) to return an error
// for graceful cache degradation.
func writeFileAtomic(path string, data []byte) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("atomic write failed: %v", r)
			}
		}()
		vlfs.MustWriteAtomic(path, data, true)
	}()
	return err
}

// mkdirIfNotExist wraps vlfs.MustMkdirIfNotExist (which panics) to return an error.
func mkdirIfNotExist(path string) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("mkdir failed: %v", r)
			}
		}()
		vlfs.MustMkdirIfNotExist(path)
	}()
	return err
}

func NewDiskCache(dir string, maxSize int64, watermark float64) (*DiskCache, error) {
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	if watermark <= 0 || watermark > 1 {
		watermark = 0.8
	}
	return &DiskCache{
		dir:       dir,
		items:     make(map[string]*list.Element),
		order:     list.New(),
		maxSize:   maxSize,
		watermark: watermark,
	}, nil
}

func (d *DiskCache) keyToPath(key string) string {
	safe := strings.NewReplacer("/", "_", ":", "_", "=", "_").Replace(key)
	return filepath.Join(d.dir, safe)
}

func (d *DiskCache) Get(key string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		if _, err := os.Stat(de.path); err == nil {
			d.order.MoveToFront(el)
			d.hits++
			return de.path, true
		}
		d.order.Remove(el)
		delete(d.items, key)
		d.curSize -= de.size
	}
	d.misses++
	return "", false
}

// SetBound attaches a K8s-style smart-cache-disk admission gate. See
// cache/lru.go SetBound docs — same semantics. Call once at storage
// construction; not thread-safe with concurrent Puts.
func (d *DiskCache) SetBound(b *resourcebounds.Bound) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bound = b
}

// RejectedByBound returns the cumulative count of Put / PutFromPath
// calls rejected by the smart-cache-disk bound. See cache/lru.go
// RejectedByBound for the contract.
func (d *DiskCache) RejectedByBound() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.rejected
}

func (d *DiskCache) Put(key string, data []byte) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	size := int64(len(data))

	// K8s-style smart-cache-disk admission. Same shape as cache/lru.go:
	// TryAcquire BEFORE any disk I/O; rejection skips the write (caller
	// receives ErrBoundFull, treats it as a soft cache miss). Updates
	// release the old slot first, then try-acquire the new size — if
	// the new acquire fails, the old entry remains intact.
	var newRelease func()
	if d.bound != nil {
		if el, ok := d.items[key]; ok {
			oldEntry := el.Value.(*diskEntry)
			if oldEntry.boundRelease != nil {
				oldEntry.boundRelease()
				oldEntry.boundRelease = nil
			}
		}
		rel, err := d.bound.TryAcquire(size)
		if err != nil {
			d.rejected++
			return "", err
		}
		newRelease = rel
	}

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		if err := writeFileAtomic(de.path, data); err != nil {
			if newRelease != nil {
				newRelease()
			}
			return "", err
		}
		d.curSize = d.curSize - de.size + size
		de.size = size
		de.boundRelease = newRelease
		d.order.MoveToFront(el)
		return de.path, nil
	}

	path := d.keyToPath(key)
	if err := writeFileAtomic(path, data); err != nil {
		if newRelease != nil {
			newRelease()
		}
		return "", err
	}

	de := &diskEntry{key: key, path: path, size: size, boundRelease: newRelease}
	el := d.order.PushFront(de)
	d.items[key] = el
	d.curSize += size

	d.evictIfNeeded()

	return path, nil
}

func (d *DiskCache) PutFromPath(key string, srcPath string) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	size := info.Size()
	dstPath := filepath.Clean(d.keyToPath(key))
	absDir, _ := filepath.Abs(d.dir)
	absDst, _ := filepath.Abs(dstPath)
	if !strings.HasPrefix(absDst, absDir+string(filepath.Separator)) {
		return fmt.Errorf("path traversal detected: %s escapes cache dir %s", dstPath, d.dir)
	}

	// Smart-cache-disk admission (mirrors Put). On update, release the
	// existing entry's slot first; if the new TryAcquire fails, no
	// state changes (caller observes ErrBoundFull).
	var newRelease func()
	if d.bound != nil {
		if el, ok := d.items[key]; ok {
			oldEntry := el.Value.(*diskEntry)
			if oldEntry.boundRelease != nil {
				oldEntry.boundRelease()
				oldEntry.boundRelease = nil
			}
		}
		rel, terr := d.bound.TryAcquire(size)
		if terr != nil {
			d.rejected++
			return terr
		}
		newRelease = rel
	}

	data, err := os.ReadFile(srcPath) // #nosec G304 -- srcPath is caller-controlled internal path
	if err != nil {
		if newRelease != nil {
			newRelease()
		}
		return err
	}
	if err := writeFileAtomic(dstPath, data); err != nil {
		if newRelease != nil {
			newRelease()
		}
		return err
	}

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		d.curSize = d.curSize - de.size + size
		de.size = size
		de.path = dstPath
		de.boundRelease = newRelease
		d.order.MoveToFront(el)
	} else {
		de := &diskEntry{key: key, path: dstPath, size: size, boundRelease: newRelease}
		el := d.order.PushFront(de)
		d.items[key] = el
		d.curSize += size
	}

	d.evictIfNeeded()
	return nil
}

func (d *DiskCache) evictIfNeeded() {
	threshold := int64(float64(d.maxSize) * d.watermark)
	for d.curSize > threshold && d.order.Len() > 0 {
		el := d.order.Back()
		if el == nil {
			break
		}
		de := el.Value.(*diskEntry)
		_ = os.Remove(de.path)
		d.order.Remove(el)
		delete(d.items, de.key)
		d.curSize -= de.size
		if de.boundRelease != nil {
			de.boundRelease()
			de.boundRelease = nil
		}
		d.evictions++
	}
}

func (d *DiskCache) Delete(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		_ = os.Remove(de.path)
		d.order.Remove(el)
		delete(d.items, de.key)
		d.curSize -= de.size
		if de.boundRelease != nil {
			de.boundRelease()
			de.boundRelease = nil
		}
	}
}

func (d *DiskCache) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.order.Len()
}

func (d *DiskCache) Size() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.curSize
}

func (d *DiskCache) Stats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return Stats{
		Entries:   d.order.Len(),
		Size:      d.curSize,
		MaxSize:   d.maxSize,
		Hits:      d.hits,
		Misses:    d.misses,
		Evictions: d.evictions,
	}
}

func (d *DiskCache) Clear() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Release all bound slots before dropping references — without this
	// the bound's outstanding counters would leak by the size of the
	// cache on every Clear, eventually exhausting the limit and
	// silently rejecting all future Puts.
	for _, el := range d.items {
		de := el.Value.(*diskEntry)
		_ = os.Remove(de.path)
		if de.boundRelease != nil {
			de.boundRelease()
			de.boundRelease = nil
		}
	}
	d.items = make(map[string]*list.Element)
	d.order.Init()
	d.curSize = 0
	return nil
}

func (d *DiskCache) Dir() string {
	return d.dir
}
