package bloomindex

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// allocDelta reports the bytes allocated while f runs (TotalAlloc is
// process-wide, so ceilings below are generous).
func allocDelta(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// crafted returns a bloom index value of the given version whose header
// declares count entries and which carries payload zero bytes after it.
func crafted(version byte, count uint32, payload int) []byte {
	b := make([]byte, 5+payload)
	b[0] = version
	binary.LittleEndian.PutUint32(b[1:5], count)
	return b
}

// marshalV1 is the v1 (single-column) writer. Marshal only ever wrote v2 (see
// git history: indexVersion = 2 since the first commit), so v1 values exist
// only as hand-built shapes; this mirrors the layout unmarshalV1 documents:
// [1][count] then per entry [key_len:2][key][filter_len:4][filter].
func marshalV1(entries map[string]*Filter) []byte {
	buf := []byte{1}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))
	for key, f := range entries {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(key)))
		buf = append(buf, key...)
		fd := f.Marshal()
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(fd)))
		buf = append(buf, fd...)
	}
	return buf
}

// TestUnmarshal_DeclaredCountBoundedByPayload pins issue #763. A bloom index
// value is untrusted bucket data: a 5-byte value declaring 1,000,000 entries
// used to preallocate ~56 MB (v1 56.0 MB, v2 55.9 MB measured) before failing.
// It must be refused without allocating in proportion to the declared count.
func TestUnmarshal_DeclaredCountBoundedByPayload(t *testing.T) {
	const ceiling = 4 << 20 // 4 MiB (assumed generous: TotalAlloc is process-wide)
	cases := map[string][]byte{
		"v1 5 bytes count 1M":          {1, 0x40, 0x42, 0x0F, 0x00},
		"v2 5 bytes count 1M":          {2, 0x40, 0x42, 0x0F, 0x00},
		"v1 count at cap":              crafted(1, maxIndexEntries, 0),
		"v2 count at cap":              crafted(2, maxIndexEntries, 0),
		"v1 count 1":                   crafted(1, 1, 0),
		"v2 count 1":                   crafted(2, 1, 0),
		"v1 count exceeds fit":         crafted(1, 1000, 8*1000-1),
		"v2 count exceeds fit":         crafted(2, 1000, 4*1000-1),
		"v1 count > cap":               crafted(1, maxIndexEntries+1, 0),
		"v2 count 2^32-1":              crafted(2, 0xFFFFFFFF, 0),
		"v2 one entry col count 65535": append(crafted(2, 1, 0), 0, 0, 0xFF, 0xFF), // key_len 0, col_count 65535, no columns
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			var err error
			a := allocDelta(func() { _, err = Unmarshal(data) })
			if err == nil {
				t.Fatal("expected an error")
			}
			if a > ceiling {
				t.Fatalf("allocated %d bytes for a %d-byte value (ceiling %d)", a, len(data), ceiling)
			}
		})
	}
}

// TestUnmarshal_BoundIsExact: the bound is count x (smallest entry) against the
// bytes after the 5-byte header, not an approximation. The smallest v1 entry is
// key_len 2 + empty key + filter_len 4 + a 2-byte filter (UnmarshalFilter
// rejects anything shorter) = 8 bytes; the smallest v2 entry is key_len 2 +
// col_count 2 = 4 bytes (a file with no columns); the smallest v2 column is
// name_len 1 + empty name + filter_len 4 + 2-byte filter = 7 bytes.
func TestUnmarshal_BoundIsExact(t *testing.T) {
	t.Run("v1", func(t *testing.T) {
		const n = 1000
		// n minimum-size entries: empty key, 2-byte filter {numHash, 1 bit-byte}.
		fit := crafted(1, n, 0)
		for i := 0; i < n; i++ {
			fit = binary.LittleEndian.AppendUint16(fit, 0)
			fit = binary.LittleEndian.AppendUint32(fit, 2)
			fit = append(fit, 1, 0)
		}
		if len(fit) != 5+8*n {
			t.Fatalf("unexpected size %d", len(fit))
		}
		got, err := Unmarshal(fit)
		if err != nil || got.Len() != 1 { // all keys are "" -> one map entry; the rest overwrite
			t.Fatalf("exact fit: len %v err %v", got, err)
		}
		_, err = Unmarshal(fit[:len(fit)-1])
		if err == nil {
			t.Fatal("one byte short accepted")
		}
		// And the pure count bound, independent of the content check.
		_, err = Unmarshal(crafted(1, n, 8*n-1))
		if err == nil || !strings.Contains(err.Error(), "exceeds payload") {
			t.Fatalf("expected the payload-bound error, got %v", err)
		}
	})
	t.Run("v2 entries", func(t *testing.T) {
		const n = 1000
		fit := crafted(2, n, 0)
		for i := 0; i < n; i++ {
			fit = binary.LittleEndian.AppendUint16(fit, 0) // empty key
			fit = binary.LittleEndian.AppendUint16(fit, 0) // no columns
		}
		if len(fit) != 5+4*n {
			t.Fatalf("unexpected size %d", len(fit))
		}
		got, err := Unmarshal(fit)
		if err != nil || got.Len() != 1 {
			t.Fatalf("exact fit: len %v err %v", got, err)
		}
		_, err = Unmarshal(crafted(2, n, 4*n-1))
		if err == nil || !strings.Contains(err.Error(), "exceeds payload") {
			t.Fatalf("expected the payload-bound error, got %v", err)
		}
	})
	t.Run("v2 columns", func(t *testing.T) {
		const c = 500
		// One entry, empty key, c minimum-size columns (all with the empty name,
		// so they collapse to one map element, but every byte is consumed).
		fit := crafted(2, 1, 0)
		fit = binary.LittleEndian.AppendUint16(fit, 0)
		fit = binary.LittleEndian.AppendUint16(fit, c)
		for i := 0; i < c; i++ {
			fit = append(fit, 0)
			fit = binary.LittleEndian.AppendUint32(fit, 2)
			fit = append(fit, 1, 0)
		}
		if len(fit) != 5+4+7*c {
			t.Fatalf("unexpected size %d", len(fit))
		}
		got, err := Unmarshal(fit)
		if err != nil || got.Len() != 1 {
			t.Fatalf("exact fit: len %v err %v", got, err)
		}
		short := crafted(2, 1, 0)
		short = binary.LittleEndian.AppendUint16(short, 0)
		short = binary.LittleEndian.AppendUint16(short, c)
		short = append(short, make([]byte, 7*c-1)...)
		_, err = Unmarshal(short)
		if err == nil || !strings.Contains(err.Error(), "exceeds payload") {
			t.Fatalf("expected the payload-bound error, got %v", err)
		}
	})
}

// TestUnmarshal_CapStillApplies: the pre-existing cap is kept; an index whose
// payload really holds more than maxIndexEntries entries is still refused by
// the cap (the byte bound alone would let it through).
func TestUnmarshal_CapStillApplies(t *testing.T) {
	for _, v := range []byte{1, 2} {
		n := uint32(maxIndexEntries + 1)
		_, err := Unmarshal(crafted(v, n, int(n)*8))
		if err == nil || !strings.Contains(err.Error(), "exceeds max") {
			t.Fatalf("v%d: expected the cap error, got %v", v, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Differential test against the pre-change decoder.

// legacyUnmarshalV1/V2 are the decoders as they were on main before #763
// (verbatim apart from the names); the new decoders must agree with them on
// every input: same accept/reject outcome and, when accepted, an equal index.
func legacyUnmarshal(data []byte) (*Index, error) {
	if len(data) < 5 {
		return nil, errors.New("bloom index data too short")
	}
	switch data[0] {
	case 1:
		return legacyUnmarshalV1(data)
	case 2:
		return legacyUnmarshalV2(data)
	default:
		return nil, errors.New("unsupported bloom index version")
	}
}

func legacyUnmarshalV1(data []byte) (*Index, error) {
	count := binary.LittleEndian.Uint32(data[1:5])
	if count > maxIndexEntries {
		return nil, fmt.Errorf("bloom index entry count %d exceeds max %d", count, maxIndexEntries)
	}
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
		if fLen < 0 || fLen > len(data)-pos {
			return nil, errors.New("invalid bloom index filter length")
		}
		f, err := UnmarshalFilter(data[pos : pos+fLen])
		if err != nil {
			return nil, err
		}
		idx.entries[key] = map[string]*Filter{"trace_id": f}
		pos += fLen
	}
	return idx, nil
}

func legacyUnmarshalV2(data []byte) (*Index, error) {
	count := binary.LittleEndian.Uint32(data[1:5])
	if count > maxIndexEntries {
		return nil, fmt.Errorf("bloom index entry count %d exceeds max %d", count, maxIndexEntries)
	}
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
			if fLen < 0 || fLen > len(data)-pos {
				return nil, errors.New("invalid bloom index filter length")
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

// diffCheck asserts the new and legacy decoders agree on data; it returns
// whether the input was accepted.
func diffCheck(t *testing.T, data []byte) bool {
	t.Helper()
	want, werr := legacyUnmarshal(data)
	got, gerr := Unmarshal(data)
	if (werr == nil) != (gerr == nil) {
		t.Fatalf("divergence on %d-byte input %x: legacy err=%v new err=%v", len(data), truncHex(data), werr, gerr)
	}
	if werr == nil && !reflect.DeepEqual(want.entries, got.entries) {
		t.Fatalf("divergence on %d-byte input %x: decoded indexes differ", len(data), truncHex(data))
	}
	return werr == nil
}

func truncHex(b []byte) []byte {
	if len(b) > 64 {
		return b[:64]
	}
	return b
}

func randStr(r *rand.Rand, max int) string {
	n := r.Intn(max + 1)
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + r.Intn(26))
	}
	return string(b)
}

// randIndex builds an index through the real writer API over random and
// extreme shapes: empty keys, entries with no columns, empty and 255-byte
// column names, many columns, filters from the smallest the sizing math can
// make up to a few KB.
func randIndex(r *rand.Rand) *Index {
	idx := New()
	nKeys := r.Intn(40)
	for k := 0; k < nKeys; k++ {
		key := randStr(r, 300)
		if r.Intn(8) == 0 {
			key = ""
		}
		nCols := r.Intn(6)
		switch r.Intn(10) {
		case 0:
			nCols = 0
		case 1:
			nCols = 100 + r.Intn(200)
		}
		cols := make(map[string]*Filter, nCols)
		for c := 0; c < nCols; c++ {
			name := randStr(r, 40)
			switch r.Intn(10) {
			case 0:
				name = ""
			case 1:
				name = strings.Repeat("c", 255)
			}
			n := 1 + r.Intn(50)
			fp := []float64{0.01, 0.1, 0.5, 0.99, 0}[r.Intn(5)]
			f := NewFilter(n, fp)
			for v := 0; v < n; v++ {
				f.Add(randStr(r, 20))
			}
			cols[name] = f
		}
		if len(cols) == 0 {
			// an entry with no columns is a writer shape too (AddColumns skips it,
			// but Add/MergeFrom paths and old releases could leave one).
			idx.entries[key] = cols
			continue
		}
		idx.AddColumns(key, cols)
	}
	return idx
}

func mutate(r *rand.Rand, in []byte) []byte {
	out := append([]byte(nil), in...)
	if len(out) == 0 {
		return out
	}
	switch r.Intn(5) {
	case 0: // flip a byte
		out[r.Intn(len(out))] = byte(r.Intn(256))
	case 1: // truncate
		out = out[:r.Intn(len(out))]
	case 2: // extend with garbage
		out = append(out, make([]byte, r.Intn(32))...)
	case 3: // rewrite the header count (small values keep the legacy allocation tiny)
		if len(out) >= 5 {
			binary.LittleEndian.PutUint32(out[1:5], uint32(r.Intn(5000)))
		}
	case 4: // rewrite a random 2-byte field
		if len(out) >= 2 {
			p := r.Intn(len(out) - 1)
			binary.LittleEndian.PutUint16(out[p:], uint16(r.Intn(70000)))
		}
	}
	return out
}

// TestUnmarshal_DifferentialAgainstLegacy: the byte bound never rejects
// anything the pre-change decoder accepted. Inputs are produced by the real
// writer (Index.Marshal, v2; marshalV1 for the decoder-only v1 shape) over
// random and extreme indexes, every proper prefix of each (truncation), and
// random byte/count/length mutations. 0 divergences required.
func TestUnmarshal_DifferentialAgainstLegacy(t *testing.T) {
	iters := 400
	if testing.Short() {
		iters = 120
	}
	r := rand.New(rand.NewSource(763))
	var accepted, rejected int
	count := func(ok bool) {
		if ok {
			accepted++
		} else {
			rejected++
		}
	}
	for i := 0; i < iters; i++ {
		idx := randIndex(r)
		v2 := idx.Marshal()
		count(diffCheck(t, v2))
		// the full writer round-trip stays exact
		back, err := Unmarshal(v2)
		if err != nil || !reflect.DeepEqual(back.entries, idx.entries) {
			t.Fatalf("round trip lost data: %v", err)
		}

		// v1 from the same keys: one filter per key.
		v1src := make(map[string]*Filter)
		for k, cols := range idx.entries {
			for _, f := range cols {
				v1src[k] = f
				break
			}
		}
		v1 := marshalV1(v1src)
		count(diffCheck(t, v1))

		// every proper prefix of small encodings, a random sample of larger ones
		for _, enc := range [][]byte{v1, v2} {
			if len(enc) <= 600 {
				for n := 0; n < len(enc); n++ {
					count(diffCheck(t, enc[:n]))
				}
			} else {
				for s := 0; s < 40; s++ {
					count(diffCheck(t, enc[:r.Intn(len(enc))]))
				}
			}
			for s := 0; s < 40; s++ {
				count(diffCheck(t, mutate(r, enc)))
			}
		}
	}
	// Pure noise behind a valid-looking header.
	for i := 0; i < iters; i++ {
		noise := make([]byte, r.Intn(200))
		r.Read(noise)
		for _, v := range []byte{1, 2} {
			b := append([]byte{v, 0, 0, 0, 0}, noise...)
			binary.LittleEndian.PutUint32(b[1:5], uint32(r.Intn(300)))
			count(diffCheck(t, b))
		}
	}
	// Extremes: the empty index and a single maximal key.
	count(diffCheck(t, New().Marshal()))
	big := New()
	big.AddColumns(strings.Repeat("k", 65535), map[string]*Filter{"c": filterWith("x")})
	count(diffCheck(t, big.Marshal()))
	count(diffCheck(t, marshalV1(map[string]*Filter{})))
	if accepted == 0 || rejected == 0 {
		t.Fatalf("degenerate corpus: accepted=%d rejected=%d", accepted, rejected)
	}
	t.Logf("differential: %d inputs accepted, %d rejected, 0 divergences", accepted, rejected)
}

// ---------------------------------------------------------------------------
// Benchmarks: valid decode must not regress.

func benchIndex(files, cols int) *Index {
	idx := New()
	for i := 0; i < files; i++ {
		m := make(map[string]*Filter, cols)
		for c := 0; c < cols; c++ {
			f := NewFilter(200, 0.01)
			for v := 0; v < 200; v++ {
				f.Add(fmt.Sprintf("v-%d-%d-%d", i, c, v))
			}
			m[fmt.Sprintf("col%d", c)] = f
		}
		idx.AddColumns(fmt.Sprintf("tenant/dt=2026-10-23/part-%06d.parquet", i), m)
	}
	return idx
}

// BenchmarkUnmarshalValid decodes realistic valid v2 indexes to show the bound
// adds no measurable cost on valid input.
func BenchmarkUnmarshalValid(b *testing.B) {
	for _, tc := range []struct{ files, cols int }{{200, 4}, {5000, 2}} {
		raw := benchIndex(tc.files, tc.cols).Marshal()
		b.Run(fmt.Sprintf("v2/files=%d/cols=%d", tc.files, tc.cols), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for i := 0; i < b.N; i++ {
				if _, err := Unmarshal(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	v1src := make(map[string]*Filter)
	for i := 0; i < 5000; i++ {
		v1src[fmt.Sprintf("tenant/dt=2026-10-23/part-%06d.parquet", i)] = filterWith("a", "b", "c")
	}
	raw1 := marshalV1(v1src)
	b.Run("v1/files=5000", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(raw1)))
		for i := 0; i < b.N; i++ {
			if _, err := Unmarshal(raw1); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestUnmarshal_DeclaredColumnCountBounded: a v2 entry declaring 65535 columns
// sized a ~3.5 MB map before the first column was read. The per-entry column count
// is bounded by the bytes that follow, as the entry count is.
func TestUnmarshal_DeclaredColumnCountBounded(t *testing.T) {
	const ceiling = 256 << 10 // assumed generous: the unbounded decoder allocated ~3.5 MB (measured 3.5 MB, below the 4 MiB ceiling of the count test)
	data := append(crafted(2, 1, 0), 0, 0, 0xFF, 0xFF)
	var err error
	a := allocDelta(func() { _, err = Unmarshal(data) })
	if err == nil {
		t.Fatal("expected an error")
	}
	if a > ceiling {
		t.Fatalf("allocated %d bytes for a %d-byte value (ceiling %d)", a, len(data), ceiling)
	}
}
