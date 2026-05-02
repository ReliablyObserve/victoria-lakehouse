package cache

import (
	"container/list"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type diskEntry struct {
	key  string
	path string
	size int64
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
}

func NewDiskCache(dir string, maxSize int64, watermark float64) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
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

func (d *DiskCache) Put(key string, data []byte) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	size := int64(len(data))

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		if err := os.WriteFile(de.path, data, 0o640); err != nil {
			return "", err
		}
		d.curSize = d.curSize - de.size + size
		de.size = size
		d.order.MoveToFront(el)
		return de.path, nil
	}

	path := d.keyToPath(key)
	if err := os.WriteFile(path, data, 0o640); err != nil {
		return "", err
	}

	de := &diskEntry{key: key, path: path, size: size}
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
	dstPath := d.keyToPath(key)

	data, err := os.ReadFile(srcPath)
	if err != nil {
		return err
	}
	if err := os.WriteFile(dstPath, data, 0o640); err != nil {
		return err
	}

	if el, ok := d.items[key]; ok {
		de := el.Value.(*diskEntry)
		d.curSize = d.curSize - de.size + size
		de.size = size
		de.path = dstPath
		d.order.MoveToFront(el)
	} else {
		de := &diskEntry{key: key, path: dstPath, size: size}
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

	for _, el := range d.items {
		de := el.Value.(*diskEntry)
		_ = os.Remove(de.path)
	}
	d.items = make(map[string]*list.Element)
	d.order.Init()
	d.curSize = 0
	return nil
}

func (d *DiskCache) Dir() string {
	return d.dir
}
