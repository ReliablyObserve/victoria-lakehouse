package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LabelInfo struct {
	Name        string         `json:"name"`
	Cardinality int            `json:"cardinality"`
	Values      []string       `json:"values,omitempty"`
	ValueCounts map[string]int `json:"value_counts,omitempty"`
	SeenInFiles int            `json:"seen_in_files"`
	PerTenant   map[string]int `json:"per_tenant,omitempty"`
}

type LabelIndex struct {
	mu     sync.RWMutex
	labels map[string]*LabelInfo
}

func NewLabelIndex() *LabelIndex {
	return &LabelIndex{labels: make(map[string]*LabelInfo)}
}

func (idx *LabelIndex) Add(name string, values []string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.addLocked(name, values, nil)
}

// AddWithValueCounts is like Add but also merges sampled per-value frequency
// counts. This produces differentiated storage estimates in the breakdown API.
func (idx *LabelIndex) AddWithValueCounts(name string, values []string, counts map[string]int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.addLocked(name, values, counts)
}

func (idx *LabelIndex) addLocked(name string, values []string, counts map[string]int) {
	// When counts is provided it is authoritative: it comes from a real
	// data-page scan (sampleValueFrequency), whereas values may include
	// entries from a footer-only or column-index path that returns
	// truncated BYTE_ARRAY prefixes (e.g. "notification-ser" alongside
	// "notification-service"). To prevent truncated values from leaking
	// into li.Values we ignore the values slice when counts has entries.
	effectiveValues := values
	if len(counts) > 0 {
		effectiveValues = make([]string, 0, len(counts))
		for v := range counts {
			effectiveValues = append(effectiveValues, v)
		}
	}

	if li, ok := idx.labels[name]; ok {
		li.SeenInFiles++
		existing := make(map[string]bool, len(li.Values))
		for _, v := range li.Values {
			existing[v] = true
		}
		for _, v := range effectiveValues {
			if !existing[v] && len(li.Values) < 10000 {
				li.Values = append(li.Values, v)
				existing[v] = true
			}
		}
		if li.ValueCounts == nil {
			li.ValueCounts = make(map[string]int)
		}
		if len(counts) > 0 {
			for v, c := range counts {
				li.ValueCounts[v] += c
			}
		} else {
			for _, v := range values {
				li.ValueCounts[v]++
			}
		}
		li.Cardinality = len(li.Values)
	} else {
		capped := effectiveValues
		if len(capped) > 10000 {
			capped = capped[:10000]
		}
		vc := make(map[string]int, len(capped))
		if len(counts) > 0 {
			for v, c := range counts {
				vc[v] = c
			}
		} else {
			for _, v := range capped {
				vc[v]++
			}
		}
		idx.labels[name] = &LabelInfo{
			Name:        name,
			Cardinality: len(capped),
			Values:      capped,
			ValueCounts: vc,
			SeenInFiles: 1,
		}
	}
}

func (idx *LabelIndex) GetFieldNames() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	names := make([]string, 0, len(idx.labels))
	for name := range idx.labels {
		names = append(names, name)
	}
	return names
}

func (idx *LabelIndex) GetFieldValues(name string, limit uint64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	li, ok := idx.labels[name]
	if !ok {
		return nil
	}
	if limit > 0 && limit < uint64(len(li.Values)) {
		return li.Values[:limit]
	}
	return li.Values
}

func (idx *LabelIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.labels)
}

// AddWithTenant is like Add but also tracks per-tenant unique value count.
// The per-tenant cardinality reflects the number of unique values the tenant
// has been seen with. Each call updates the count to be at least the number
// of unique values in the passed slice.
func (idx *LabelIndex) AddWithTenant(name string, values []string, tenant string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.addLocked(name, values, nil)
	li := idx.labels[name]

	// Track per-tenant cardinality
	if tenant != "" {
		if li.PerTenant == nil {
			li.PerTenant = make(map[string]int)
		}
		// Count unique values in the input slice
		unique := make(map[string]struct{}, len(values))
		for _, v := range values {
			unique[v] = struct{}{}
		}
		count := len(unique)
		if count > li.PerTenant[tenant] {
			li.PerTenant[tenant] = count
		}
	}
}

// GetLabelInfo returns the LabelInfo for the given field name, or nil if not found.
func (idx *LabelIndex) GetLabelInfo(name string) *LabelInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	li, ok := idx.labels[name]
	if !ok {
		return nil
	}
	return li
}

// GetAllLabelInfo returns all label infos in the index.
func (idx *LabelIndex) GetAllLabelInfo() []*LabelInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]*LabelInfo, 0, len(idx.labels))
	for _, li := range idx.labels {
		result = append(result, li)
	}
	return result
}

type ManifestState struct {
	Files      map[string][]FileInfoState `json:"files"`
	MinTime    time.Time                  `json:"min_time"`
	MaxTime    time.Time                  `json:"max_time"`
	TotalFiles int                        `json:"total_files"`
	TotalBytes int64                      `json:"total_bytes"`
	SavedAt    time.Time                  `json:"saved_at"`
}

type FileInfoState struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

type FileMetaEntry struct {
	Key               string              `json:"k"`
	RowCount          int64               `json:"rc,omitempty"`
	MinTimeNs         int64               `json:"mn,omitempty"`
	MaxTimeNs         int64               `json:"mx,omitempty"`
	RawBytes          int64               `json:"rb,omitempty"`
	SchemaFingerprint string              `json:"sf,omitempty"`
	Labels            map[string][]string `json:"lb,omitempty"`
}

type FileMetadataCache struct {
	Entries []FileMetaEntry `json:"entries"`
	SavedAt time.Time       `json:"saved_at"`
}

type Persister struct {
	dir string
}

func NewPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create persist dir: %w", err)
	}
	return &Persister{dir: dir}, nil
}

func (p *Persister) SaveManifest(state *ManifestState) error {
	state.SavedAt = time.Now()
	return p.writeJSON("manifest.json", state)
}

func (p *Persister) LoadManifest() (*ManifestState, error) {
	var state ManifestState
	if err := p.readJSON("manifest.json", &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (p *Persister) SaveLabelIndex(idx *LabelIndex) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	data := struct {
		Labels  map[string]*LabelInfo `json:"labels"`
		SavedAt time.Time             `json:"saved_at"`
	}{
		Labels:  idx.labels,
		SavedAt: time.Now(),
	}
	return p.writeJSON("label-index.json", data)
}

// MarshalLabelIndex returns the JSON encoding of idx, suitable for
// uploading to S3 so a new pod (or a pod that lost its local disk
// volume) can recover the cluster's accumulated label index without
// having to re-scan every parquet file. Mirrors the byte-for-byte
// layout SaveLabelIndex writes to disk; UnmarshalLabelIndex applies the
// same drift-reconciliation pass on the way back in.
func MarshalLabelIndex(idx *LabelIndex) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	data := struct {
		Labels  map[string]*LabelInfo `json:"labels"`
		SavedAt time.Time             `json:"saved_at"`
	}{
		Labels:  idx.labels,
		SavedAt: time.Now(),
	}
	return json.Marshal(data)
}

// UnmarshalLabelIndex parses a JSON-encoded label index produced by
// MarshalLabelIndex. Applies the same sanitation LoadLabelIndex does so
// truncated BYTE_ARRAY prefix values from older pods don't leak into
// the freshly loaded index.
func UnmarshalLabelIndex(buf []byte) (*LabelIndex, error) {
	var data struct {
		Labels map[string]*LabelInfo `json:"labels"`
	}
	if err := json.Unmarshal(buf, &data); err != nil {
		return nil, err
	}
	if data.Labels == nil {
		data.Labels = make(map[string]*LabelInfo)
	}
	for _, li := range data.Labels {
		if li == nil || len(li.ValueCounts) == 0 || len(li.Values) == 0 {
			continue
		}
		kept := li.Values[:0]
		for _, v := range li.Values {
			if _, ok := li.ValueCounts[v]; ok {
				kept = append(kept, v)
			}
		}
		li.Values = kept
		li.Cardinality = len(kept)
	}
	return &LabelIndex{labels: data.Labels}, nil
}

// MergeFrom adds all labels from src into idx. Used when recovering a
// label index from S3 alongside whatever was already loaded from local
// disk — we want the union, not just the most recent write.
func (idx *LabelIndex) MergeFrom(src *LabelIndex) {
	if src == nil {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	src.mu.RLock()
	defer src.mu.RUnlock()
	for name, srcInfo := range src.labels {
		if srcInfo == nil {
			continue
		}
		existing, ok := idx.labels[name]
		if !ok {
			// Copy by value so a subsequent mutation on src doesn't
			// alias into idx (and vice versa).
			cp := *srcInfo
			if srcInfo.Values != nil {
				cp.Values = append([]string(nil), srcInfo.Values...)
			}
			if srcInfo.ValueCounts != nil {
				cp.ValueCounts = make(map[string]int, len(srcInfo.ValueCounts))
				for k, v := range srcInfo.ValueCounts {
					cp.ValueCounts[k] = v
				}
			}
			if srcInfo.PerTenant != nil {
				cp.PerTenant = make(map[string]int, len(srcInfo.PerTenant))
				for k, v := range srcInfo.PerTenant {
					cp.PerTenant[k] = v
				}
			}
			idx.labels[name] = &cp
			continue
		}
		// Merge values: bounded by 10K per label (same cap as Add).
		existingVals := make(map[string]bool, len(existing.Values))
		for _, v := range existing.Values {
			existingVals[v] = true
		}
		for _, v := range srcInfo.Values {
			if !existingVals[v] && len(existing.Values) < 10000 {
				existing.Values = append(existing.Values, v)
				existingVals[v] = true
			}
		}
		existing.Cardinality = len(existing.Values)
		if srcInfo.ValueCounts != nil {
			if existing.ValueCounts == nil {
				existing.ValueCounts = make(map[string]int, len(srcInfo.ValueCounts))
			}
			for v, c := range srcInfo.ValueCounts {
				existing.ValueCounts[v] += c
			}
		}
		if srcInfo.SeenInFiles > existing.SeenInFiles {
			existing.SeenInFiles = srcInfo.SeenInFiles
		}
		if srcInfo.PerTenant != nil {
			if existing.PerTenant == nil {
				existing.PerTenant = make(map[string]int, len(srcInfo.PerTenant))
			}
			for k, v := range srcInfo.PerTenant {
				existing.PerTenant[k] += v
			}
		}
	}
}

func (p *Persister) LoadLabelIndex() (*LabelIndex, error) {
	var data struct {
		Labels map[string]*LabelInfo `json:"labels"`
	}
	if err := p.readJSON("label-index.json", &data); err != nil {
		return nil, err
	}
	if data.Labels == nil {
		data.Labels = make(map[string]*LabelInfo)
	}
	// Reconcile any historical drift between li.Values and li.ValueCounts.
	// ValueCounts is always built from real data-page scans, so when both
	// are present we trust ValueCounts and drop any Values entry that
	// isn't accounted for there (typically a truncated BYTE_ARRAY prefix
	// from a pre-fix run, e.g. "notification-ser" alongside the full
	// "notification-service"). One-shot sanitization at load time.
	for _, li := range data.Labels {
		if li == nil || len(li.ValueCounts) == 0 || len(li.Values) == 0 {
			continue
		}
		kept := li.Values[:0]
		for _, v := range li.Values {
			if _, ok := li.ValueCounts[v]; ok {
				kept = append(kept, v)
			}
		}
		li.Values = kept
		li.Cardinality = len(kept)
	}
	return &LabelIndex{labels: data.Labels}, nil
}

func (p *Persister) writeJSON(filename string, v any) error {
	path := filepath.Join(p.dir, filename)
	tmp := path + ".tmp"

	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", filename, err)
	}

	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", filename, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", filename, err)
	}
	return nil
}

func (p *Persister) readJSON(filename string, v any) error {
	path := filepath.Join(p.dir, filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func (p *Persister) SaveFileMetadata(cache *FileMetadataCache) error {
	cache.SavedAt = time.Now()
	return p.writeJSON("file-metadata.json", cache)
}

func (p *Persister) LoadFileMetadata() (*FileMetadataCache, error) {
	var cache FileMetadataCache
	if err := p.readJSON("file-metadata.json", &cache); err != nil {
		return nil, err
	}
	return &cache, nil
}

func (p *Persister) Dir() string {
	return p.dir
}
