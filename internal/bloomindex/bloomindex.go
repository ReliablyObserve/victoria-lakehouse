package bloomindex

import (
	"encoding/binary"
	"errors"
	"hash"
	"hash/fnv"
	"math"
	"sync"
)

// Index is a multi-column file-level bloom index. Each file key maps to
// a set of per-column bloom filters, enabling any exact-match query to
// skip files that definitely don't contain the target value.
type Index struct {
	mu      sync.RWMutex
	entries map[string]map[string]*Filter // key → column → filter
}

func New() *Index {
	return &Index{
		entries: make(map[string]map[string]*Filter),
	}
}

// Add registers a bloom filter for the given file key and column.
func (idx *Index) Add(key, column string, f *Filter) {
	idx.mu.Lock()
	cols, ok := idx.entries[key]
	if !ok {
		cols = make(map[string]*Filter)
		idx.entries[key] = cols
	}
	cols[column] = f
	idx.mu.Unlock()
}

// AddColumns registers multiple column bloom filters for a file key at once.
func (idx *Index) AddColumns(key string, columns map[string]*Filter) {
	if len(columns) == 0 {
		return
	}
	idx.mu.Lock()
	cols, ok := idx.entries[key]
	if !ok {
		cols = make(map[string]*Filter, len(columns))
		idx.entries[key] = cols
	}
	for col, f := range columns {
		cols[col] = f
	}
	idx.mu.Unlock()
}

// MayContain returns the list of file keys that might contain the given
// value in the specified column. Files not in the index or without a
// filter for the column are always included (conservative).
func (idx *Index) MayContain(keys []string, column, value string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string
	for _, key := range keys {
		cols, ok := idx.entries[key]
		if !ok {
			result = append(result, key)
			continue
		}
		f, ok := cols[column]
		if !ok {
			result = append(result, key)
			continue
		}
		if f.MayContain(value) {
			result = append(result, key)
		}
	}
	return result
}

// MayContainAll checks multiple column/value pairs. A file is included only
// if ALL conditions might be satisfied (AND semantics).
func (idx *Index) MayContainAll(keys []string, checks []ColumnCheck) []string {
	if len(checks) == 0 {
		return keys
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string
	for _, key := range keys {
		cols, ok := idx.entries[key]
		if !ok {
			result = append(result, key)
			continue
		}
		keep := true
		for _, check := range checks {
			f, ok := cols[check.Column]
			if !ok {
				continue // no filter for this column — can't exclude
			}
			if !f.MayContain(check.Value) {
				keep = false
				break
			}
		}
		if keep {
			result = append(result, key)
		}
	}
	return result
}

// ColumnCheck represents a column/value pair for multi-column filtering.
type ColumnCheck struct {
	Column string
	Value  string
}

// Has returns true if the index contains any bloom filter for the given key.
func (idx *Index) Has(key string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.entries[key]
	return ok
}

// Len returns the number of file entries in the index.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// Filter is a space-efficient bloom filter.
type Filter struct {
	bits    []byte
	numHash uint8
}

// NewFilter creates a bloom filter sized for n items with the given
// false positive rate.
func NewFilter(n int, fpRate float64) *Filter {
	if n <= 0 {
		n = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	m := optimalBits(n, fpRate)
	k := optimalHashes(m, n)
	return &Filter{
		bits:    make([]byte, (m+7)/8),
		numHash: uint8(k),
	}
}

// Add inserts a value into the bloom filter.
func (f *Filter) Add(value string) {
	h := fnvHash(value)
	for i := uint8(0); i < f.numHash; i++ {
		pos := bloomHash(h, uint32(i), uint32(len(f.bits)*8))
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the value might be in the set.
func (f *Filter) MayContain(value string) bool {
	h := fnvHash(value)
	for i := uint8(0); i < f.numHash; i++ {
		pos := bloomHash(h, uint32(i), uint32(len(f.bits)*8))
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Size returns the size in bytes of the bloom filter.
func (f *Filter) Size() int {
	return len(f.bits) + 1 // bits + numHash byte
}

// Marshal serializes the filter to bytes.
func (f *Filter) Marshal() []byte {
	out := make([]byte, 1+len(f.bits))
	out[0] = f.numHash
	copy(out[1:], f.bits)
	return out
}

// UnmarshalFilter deserializes a filter from bytes.
func UnmarshalFilter(data []byte) (*Filter, error) {
	if len(data) < 2 {
		return nil, errors.New("bloom filter data too short")
	}
	return &Filter{
		numHash: data[0],
		bits:    append([]byte(nil), data[1:]...),
	}, nil
}

const indexVersion = 2

// Marshal serializes the entire multi-column index to bytes.
// Format v2: [version=2][entry_count]
//
//	per entry: [key_len][key][col_count]
//	  per col: [col_name_len][col_name][filter_len][filter_data]
func (idx *Index) Marshal() []byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	size := 1 + 4 // version + entry count
	for key, cols := range idx.entries {
		size += 2 + len(key) + 2 // key_len + key + col_count
		for col, f := range cols {
			size += 1 + len(col) + 4 + f.Size() // col_name_len + col_name + filter_len + filter
		}
	}

	buf := make([]byte, 0, size)
	buf = append(buf, indexVersion)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(idx.entries)))

	for key, cols := range idx.entries {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(cols)))
		for col, f := range cols {
			buf = append(buf, byte(len(col)))
			buf = append(buf, col...)
			fData := f.Marshal()
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(fData)))
			buf = append(buf, fData...)
		}
	}
	return buf
}

// Unmarshal deserializes an index from bytes. Supports v1 (single-column)
// and v2 (multi-column) formats.
func Unmarshal(data []byte) (*Index, error) {
	if len(data) < 5 {
		return nil, errors.New("bloom index data too short")
	}
	switch data[0] {
	case 1:
		return unmarshalV1(data)
	case 2:
		return unmarshalV2(data)
	default:
		return nil, errors.New("unsupported bloom index version")
	}
}

func unmarshalV1(data []byte) (*Index, error) {
	count := binary.LittleEndian.Uint32(data[1:5])
	idx := &Index{entries: make(map[string]map[string]*Filter, count)}
	pos := 5

	for i := uint32(0); i < count; i++ {
		if pos+2 > len(data) {
			return nil, errors.New("truncated bloom index")
		}
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if pos+keyLen > len(data) {
			return nil, errors.New("truncated bloom index key")
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		if pos+4 > len(data) {
			return nil, errors.New("truncated bloom index filter length")
		}
		fLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4

		if pos+fLen > len(data) {
			return nil, errors.New("truncated bloom index filter data")
		}
		f, err := UnmarshalFilter(data[pos : pos+fLen])
		if err != nil {
			return nil, err
		}
		// V1 stored single filter as "trace_id" column
		idx.entries[key] = map[string]*Filter{"trace_id": f}
		pos += fLen
	}
	return idx, nil
}

func unmarshalV2(data []byte) (*Index, error) {
	count := binary.LittleEndian.Uint32(data[1:5])
	idx := &Index{entries: make(map[string]map[string]*Filter, count)}
	pos := 5

	for i := uint32(0); i < count; i++ {
		if pos+2 > len(data) {
			return nil, errors.New("truncated bloom index")
		}
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if pos+keyLen > len(data) {
			return nil, errors.New("truncated bloom index key")
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		if pos+2 > len(data) {
			return nil, errors.New("truncated bloom index col count")
		}
		colCount := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		cols := make(map[string]*Filter, colCount)
		for c := 0; c < colCount; c++ {
			if pos+1 > len(data) {
				return nil, errors.New("truncated bloom index col name")
			}
			colNameLen := int(data[pos])
			pos++

			if pos+colNameLen > len(data) {
				return nil, errors.New("truncated bloom index col name data")
			}
			colName := string(data[pos : pos+colNameLen])
			pos += colNameLen

			if pos+4 > len(data) {
				return nil, errors.New("truncated bloom index filter length")
			}
			fLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4

			if pos+fLen > len(data) {
				return nil, errors.New("truncated bloom index filter data")
			}
			f, err := UnmarshalFilter(data[pos : pos+fLen])
			if err != nil {
				return nil, err
			}
			cols[colName] = f
			pos += fLen
		}
		idx.entries[key] = cols
	}
	return idx, nil
}

// MergeFrom adds all entries from other into this index, overwriting on conflict.
func (idx *Index) MergeFrom(other *Index) {
	if other == nil {
		return
	}
	other.mu.RLock()
	defer other.mu.RUnlock()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for k, cols := range other.entries {
		existing, ok := idx.entries[k]
		if !ok {
			idx.entries[k] = cols
			continue
		}
		for col, f := range cols {
			existing[col] = f
		}
	}
}

func optimalBits(n int, fpRate float64) int {
	m := -float64(n) * math.Log(fpRate) / (math.Log(2) * math.Log(2))
	return int(math.Ceil(m))
}

func optimalHashes(m, n int) int {
	k := float64(m) / float64(n) * math.Log(2)
	return int(math.Max(1, math.Round(k)))
}

var hashPool = sync.Pool{
	New: func() any { return fnv.New64a() },
}

func fnvHash(s string) uint64 {
	h := hashPool.Get().(hash.Hash64)
	h.Reset()
	_, _ = h.Write([]byte(s))
	v := h.Sum64()
	hashPool.Put(h)
	return v
}

// Double hashing: h(i) = h1 + i*h2
func bloomHash(h uint64, i, m uint32) uint32 {
	h1 := uint32(h)
	h2 := uint32(h >> 32)
	return (h1 + i*h2) % m
}
