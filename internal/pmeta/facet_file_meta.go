package pmeta

import (
	"encoding/json"
	"io"
	"sync"
)

// fileMetaEntry mirrors manifest.FileMeta (the _file_metadata.json per-file
// payload) exactly — identical short json keys — so this facet is a byte-for-byte
// dual-write of that sidecar, which the parity gate checks before the sidecar is
// ever retired.
type fileMetaEntry struct {
	RowCount          int64               `json:"rc"`
	MinTimeNs         int64               `json:"mn"`
	MaxTimeNs         int64               `json:"mx"`
	RawBytes          int64               `json:"rb,omitempty"`
	SchemaFingerprint string              `json:"sf,omitempty"`
	Labels            map[string][]string `json:"lb,omitempty"`
}

// fileMetaFacet folds _file_metadata.json into the unified pmeta bundle: per-file
// metadata for a partition, keyed by file key.
type fileMetaFacet struct {
	partition string
	mu        sync.RWMutex
	byKey     map[string]fileMetaEntry
}

// NewFileMetaFactory returns a FacetFactory for per-file metadata.
func NewFileMetaFactory() FacetFactory {
	return func(partition string) Facet {
		return &fileMetaFacet{partition: partition, byKey: map[string]fileMetaEntry{}}
	}
}

func (f *fileMetaFacet) Kind() FacetKind { return FacetFileMeta }

func (f *fileMetaFacet) Merge(c FileContribution) {
	if c.FileKey == "" {
		return
	}
	f.mu.Lock()
	f.byKey[c.FileKey] = fileMetaEntry{
		RowCount:          c.RowCount,
		MinTimeNs:         c.MinTimeNs,
		MaxTimeNs:         c.MaxTimeNs,
		RawBytes:          c.RawBytes,
		SchemaFingerprint: c.SchemaFingerprint,
		Labels:            c.Labels,
	}
	f.mu.Unlock()
}

// Encode/Decode use JSON (the same shape as the sidecar). Go's json.Marshal sorts
// map keys, so the payload is deterministic for the bundle CRC.
func (f *fileMetaFacet) Encode(w io.Writer) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.NewEncoder(w).Encode(f.byKey)
}

func (f *fileMetaFacet) Decode(r io.Reader) error {
	m := map[string]fileMetaEntry{}
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return err
	}
	f.mu.Lock()
	if f.byKey == nil {
		f.byKey = m
	} else {
		for k, v := range m { // Decode-as-Merge (self-heal path)
			f.byKey[k] = v
		}
	}
	f.mu.Unlock()
	return nil
}

func (f *fileMetaFacet) EstimateBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var n int64
	for k, v := range f.byKey {
		n += int64(len(k)) + 48 + int64(len(v.SchemaFingerprint))
		for fk, vals := range v.Labels {
			n += int64(len(fk))
			for _, s := range vals {
				n += int64(len(s))
			}
		}
	}
	return n
}

// absorbFacet merges a decoded file-meta facet into this live one (warm-merge):
// keys absent from the live facet are copied; live entries win on conflict
// (a live flush is at least as fresh as the persisted bundle).
func (f *fileMetaFacet) absorbFacet(other Facet) {
	of, ok := other.(*fileMetaFacet)
	if !ok {
		return
	}
	of.mu.RLock()
	entries := make(map[string]fileMetaEntry, len(of.byKey))
	for k, v := range of.byKey {
		entries[k] = v
	}
	of.mu.RUnlock()
	f.mu.Lock()
	for k, v := range entries {
		if _, exists := f.byKey[k]; !exists {
			f.byKey[k] = v
		}
	}
	f.mu.Unlock()
}

// removeFiles drops per-file entries (compaction/retention hook).
func (f *fileMetaFacet) removeFiles(keys []string) {
	f.mu.Lock()
	for _, k := range keys {
		delete(f.byKey, k)
	}
	f.mu.Unlock()
}

func (f *fileMetaFacet) fileMeta(key string) (fileMetaEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.byKey[key]
	return e, ok
}

// FileMetaView is the exported per-file metadata read from FacetFileMeta.
type FileMetaView struct {
	RowCount          int64
	MinTimeNs         int64
	MaxTimeNs         int64
	RawBytes          int64
	SchemaFingerprint string
	Labels            map[string][]string
}

// FileMeta returns a file's metadata from its partition's FacetFileMeta, if loaded.
func (s *Store) FileMeta(partition, fileKey string) (FileMetaView, bool) {
	fc, ok := s.Get(partition, FacetFileMeta)
	if !ok {
		return FileMetaView{}, false
	}
	fm, ok := fc.(*fileMetaFacet)
	if !ok {
		return FileMetaView{}, false
	}
	e, ok := fm.fileMeta(fileKey)
	if !ok {
		return FileMetaView{}, false
	}
	return FileMetaView(e), true
}
