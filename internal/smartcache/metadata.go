package smartcache

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type EntryMeta struct {
	CreatedAt         time.Time            `json:"created_at"`
	LastAccess        time.Time            `json:"last_access"`
	AccessCount       int                  `json:"access_count"`
	AccessWindowStart time.Time            `json:"access_window_start"`
	PinnedBy          map[string]time.Time `json:"pinned_by,omitempty"`
	Signal            string               `json:"signal"`
	TraceIDs          []string             `json:"trace_ids,omitempty"`
	Size              int64                `json:"size"`
}

func (e *EntryMeta) IsPinned() bool {
	now := time.Now()
	for _, expiry := range e.PinnedBy {
		if now.Before(expiry) {
			return true
		}
	}
	return false
}

type MetadataMap struct {
	mu    sync.RWMutex
	items map[string]EntryMeta
}

func NewMetadataMap() *MetadataMap {
	return &MetadataMap{
		items: make(map[string]EntryMeta),
	}
}

func (m *MetadataMap) Set(key string, meta EntryMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = meta
}

func (m *MetadataMap) Get(key string) (EntryMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.items[key]
	return meta, ok
}

func (m *MetadataMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

func (m *MetadataMap) RecordAccess(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	meta.LastAccess = time.Now()
	meta.AccessCount++
	m.items[key] = meta
}

func (m *MetadataMap) Pin(key, queryID string, grace time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	if meta.PinnedBy == nil {
		meta.PinnedBy = make(map[string]time.Time)
	}
	meta.PinnedBy[queryID] = time.Now().Add(grace)
	m.items[key] = meta
}

func (m *MetadataMap) Unpin(key, queryID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	delete(meta.PinnedBy, queryID)
	m.items[key] = meta
}

func (m *MetadataMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

func (m *MetadataMap) TotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, meta := range m.items {
		total += meta.Size
	}
	return total
}

func (m *MetadataMap) All() map[string]EntryMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make(map[string]EntryMeta, len(m.items))
	for k, v := range m.items {
		cp[k] = deepCopyEntry(v)
	}
	return cp
}

func deepCopyEntry(e EntryMeta) EntryMeta {
	if e.PinnedBy != nil {
		pinCopy := make(map[string]time.Time, len(e.PinnedBy))
		for k, v := range e.PinnedBy {
			pinCopy[k] = v
		}
		e.PinnedBy = pinCopy
	}
	if e.TraceIDs != nil {
		tidCopy := make([]string, len(e.TraceIDs))
		copy(tidCopy, e.TraceIDs)
		e.TraceIDs = tidCopy
	}
	return e
}

func (m *MetadataMap) Reconcile(diskFiles map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.items {
		if _, exists := diskFiles[key]; !exists {
			delete(m.items, key)
		}
	}

	now := time.Now()
	for key, size := range diskFiles {
		if _, exists := m.items[key]; !exists {
			m.items[key] = EntryMeta{
				CreatedAt:         now,
				LastAccess:        now,
				AccessWindowStart: now,
				Size:              size,
			}
		}
	}
}

func (m *MetadataMap) SaveSnapshot(path string) error {
	m.mu.RLock()
	cp := make(map[string]EntryMeta, len(m.items))
	for k, v := range m.items {
		cp[k] = deepCopyEntry(v)
	}
	m.mu.RUnlock()

	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (m *MetadataMap) LoadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var items map[string]EntryMeta
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = items
	return nil
}
