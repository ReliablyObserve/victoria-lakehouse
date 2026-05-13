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

	if li, ok := idx.labels[name]; ok {
		li.SeenInFiles++
		existing := make(map[string]bool, len(li.Values))
		for _, v := range li.Values {
			existing[v] = true
		}
		for _, v := range values {
			if !existing[v] && len(li.Values) < 10000 {
				li.Values = append(li.Values, v)
				existing[v] = true
			}
		}
		li.Cardinality = len(li.Values)
	} else {
		capped := values
		if len(capped) > 10000 {
			capped = capped[:10000]
		}
		idx.labels[name] = &LabelInfo{
			Name:        name,
			Cardinality: len(capped),
			Values:      capped,
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

	li, ok := idx.labels[name]
	if ok {
		li.SeenInFiles++
		existing := make(map[string]bool, len(li.Values))
		for _, v := range li.Values {
			existing[v] = true
		}
		for _, v := range values {
			if !existing[v] && len(li.Values) < 10000 {
				li.Values = append(li.Values, v)
				existing[v] = true
			}
		}
		li.Cardinality = len(li.Values)
	} else {
		capped := values
		if len(capped) > 10000 {
			capped = capped[:10000]
		}
		li = &LabelInfo{
			Name:        name,
			Cardinality: len(capped),
			Values:      capped,
			SeenInFiles: 1,
		}
		idx.labels[name] = li
	}

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

func (p *Persister) Dir() string {
	return p.dir
}
