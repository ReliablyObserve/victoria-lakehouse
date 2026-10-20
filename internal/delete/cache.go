package delete

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// bodyCache is the optional local copy of the tombstone objects a pod has read.
//
// Every tombstone object is written once and never changes, so a cached body
// needs no validation against the bucket: it only saves a GET. The cache is read
// at start (to put the known tombstones in force at once, and to skip the GETs of
// the first listing) and written whenever an object is read. Nothing in it is
// ever written back to the bucket, and nothing in it removes a tombstone: an
// entry the bucket no longer lists is HEAD-checked and only a 404 forgets it
// (protocol.go). A pod that was down for any length of time therefore cannot
// bring back a tombstone that was un-deleted meanwhile: the un-delete's fate key
// is listed and read at start, and it wins.
//
// A file holds the key on its first line and the body after it.
type bodyCache struct {
	dir string
	mu  sync.Mutex
	// failed is set after the first write error so a read-only or full volume is
	// logged once, not on every object.
	failed bool
}

func newBodyCache(dir string) *bodyCache {
	if dir == "" {
		return nil
	}
	return &bodyCache{dir: dir}
}

func (c *bodyCache) file(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(c.dir, hex.EncodeToString(sum[:16])+".tomb")
}

// get returns the cached body of key.
func (c *bodyCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	raw, err := os.ReadFile(c.file(key))
	if err != nil {
		return nil, false
	}
	k, body, ok := splitCacheFile(raw)
	if !ok || k != key {
		return nil, false
	}
	return body, true
}

func splitCacheFile(raw []byte) (key string, body []byte, ok bool) {
	i := bytes.IndexByte(raw, '\n')
	if i < 0 {
		return "", nil, false
	}
	return string(raw[:i]), raw[i+1:], true
}

// put stores the body of key. A failure is logged once and otherwise ignored:
// the cache is an optimisation.
func (c *bodyCache) put(key string, body []byte) {
	if c == nil {
		return
	}
	if err := os.MkdirAll(c.dir, 0o750); err == nil {
		tmp := c.file(key) + ".tmp"
		data := append(append([]byte(key), '\n'), body...)
		if err = os.WriteFile(tmp, data, 0o600); err == nil {
			if err = os.Rename(tmp, c.file(key)); err == nil {
				return
			}
		}
		_ = os.Remove(tmp)
		c.warn(err)
		return
	} else {
		c.warn(err)
	}
}

func (c *bodyCache) warn(err error) {
	c.mu.Lock()
	first := !c.failed
	c.failed = true
	c.mu.Unlock()
	if first {
		logger.Warnf("tombstone cache %s is not writable: %s (the cache is optional; tombstones are read from the bucket)", c.dir, err)
	}
}

// remove deletes the cached body of key.
func (c *bodyCache) remove(key string) {
	if c == nil {
		return
	}
	_ = os.Remove(c.file(key))
}

// each calls fn with the key and body of every cached object.
func (c *bodyCache) each(fn func(key string, body []byte)) {
	if c == nil {
		return
	}
	names, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	for _, de := range names {
		if de.IsDir() || !strings.HasSuffix(de.Name(), ".tomb") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(c.dir, de.Name()))
		if err != nil {
			continue
		}
		if key, body, ok := splitCacheFile(raw); ok {
			fn(key, body)
		}
	}
}
