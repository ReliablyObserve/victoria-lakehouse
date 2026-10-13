package tracesidx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// ReadKind labels a ranged read so the cache tiers behind a Source can apply a
// policy per kind (footers are pinned for the hot window, index slices and pages
// are LRU).
type ReadKind uint8

const (
	KindFooter ReadKind = iota
	KindIndex
	KindPage
	KindBloom
)

func (k ReadKind) String() string {
	switch k {
	case KindFooter:
		return "footer"
	case KindIndex:
		return "index"
	case KindPage:
		return "page"
	case KindBloom:
		return "bloom"
	}
	return "unknown"
}

// Source is the ranged reader of one idx object. In production it is backed by the
// cache tiers (memory, disk, peers, then S3); in tests by a counting byte slice.
type Source interface {
	// Key identifies the object (the S3 key).
	Key() string
	Size() int64
	// ReadRange returns exactly n bytes at off.
	ReadRange(ctx context.Context, kind ReadKind, off, n int64) ([]byte, error)
}

// Want selects the columns a lookup returns. trace_id_idx is always read.
type Want uint8

const (
	WantTime Want = 1 << iota
	WantStream
	WantStart
	WantEnd
	WantDuration

	WantAll = WantTime | WantStream | WantStart | WantEnd | WantDuration
)

// Limits on what a (possibly hostile) object may make a lookup read or allocate.
const (
	footerWindow     = 64 << 10 // the first tail read: footer and, usually, nothing more
	maxFooterBytes   = 16 << 20
	maxIndexBytes    = 4 << 20 // one column or offset index slice
	maxPageBytes     = 16 << 20
	maxRowGroups     = 1 << 20
	indexCoalesceGap = 4 << 10
)

// ErrNotIdxObject is returned for an object that is not a trace_id_idx table.
var ErrNotIdxObject = errors.New("tracesidx: not a trace_id_idx object")

// LookupStats counts what one lookup read.
type LookupStats struct {
	Reads      int
	Bytes      int64
	RowGroups  int // row groups whose id range holds the key
	PagesRead  int
	FooterRead bool
}

type objectEntry struct {
	file  *parquet.File
	size  int64
	meta  *format.FileMetaData
	cols  map[string]int
	idMin [][]byte // per row group: min/max trace_id_idx, the fences
	idMax [][]byte
}

// Locator finds the rows of one trace id in an idx object by reading only the
// footer (once, then cached here), the id column's index slices of the one row
// group that can hold the id, and the data pages that can hold it. It never opens
// the object with the page index on: that reads the page index of every row group
// (measured 69 KB / 701 KB / 3.5 MB at 1 M / 10 M / 50 M rows).
type Locator struct {
	mu    sync.Mutex
	max   int
	order []string
	files map[string]*objectEntry
}

// NewLocator returns a Locator that keeps up to maxObjects parsed footers.
func NewLocator(maxObjects int) *Locator {
	if maxObjects < 1 {
		maxObjects = 256
	}
	return &Locator{max: maxObjects, files: make(map[string]*objectEntry)}
}

// Fences returns the object's [min, max] trace id, from the footer.
func (l *Locator) Fences(ctx context.Context, src Source) (minID, maxID string, err error) {
	e, _, err := l.open(ctx, src)
	if err != nil {
		return "", "", err
	}
	if len(e.idMin) == 0 {
		return "", "", nil
	}
	return string(e.idMin[0]), string(e.idMax[len(e.idMax)-1]), nil
}

func (l *Locator) cached(key string, size int64) *objectEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.files[key]; ok && e.size == size {
		return e
	}
	return nil
}

func (l *Locator) store(key string, e *objectEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.files[key]; !ok {
		l.order = append(l.order, key)
		for len(l.order) > l.max {
			delete(l.files, l.order[0])
			l.order = l.order[1:]
		}
	}
	l.files[key] = e
}

// Forget drops an object's cached footer (the object was replaced or removed).
func (l *Locator) Forget(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.files, key)
	l.order = slices.DeleteFunc(l.order, func(k string) bool { return k == key })
}

// open returns the parsed footer of src, reading it when it is not cached.
func (l *Locator) open(ctx context.Context, src Source) (*objectEntry, *LookupStats, error) {
	st := &LookupStats{}
	size := src.Size()
	if e := l.cached(src.Key(), size); e != nil {
		return e, st, nil
	}
	if size < 12 {
		return nil, st, fmt.Errorf("%w: %d bytes", ErrNotIdxObject, size)
	}
	n := min(size, int64(footerWindow))
	tail, err := src.ReadRange(ctx, KindFooter, size-n, n)
	if err != nil {
		return nil, st, fmt.Errorf("tracesidx: read footer of %s: %w", src.Key(), err)
	}
	st.Reads++
	st.Bytes += int64(len(tail))
	st.FooterRead = true
	if int64(len(tail)) != n || string(tail[len(tail)-4:]) != "PAR1" {
		return nil, st, fmt.Errorf("%w: bad trailer", ErrNotIdxObject)
	}
	footerLen := int64(uint32(tail[len(tail)-8]) | uint32(tail[len(tail)-7])<<8 | uint32(tail[len(tail)-6])<<16 | uint32(tail[len(tail)-5])<<24)
	if footerLen <= 0 || footerLen > maxFooterBytes || footerLen+8 > size {
		return nil, st, fmt.Errorf("%w: footer length %d", ErrNotIdxObject, footerLen)
	}
	if footerLen+8 > n {
		more := footerLen + 8
		tail, err = src.ReadRange(ctx, KindFooter, size-more, more)
		if err != nil {
			return nil, st, fmt.Errorf("tracesidx: read footer of %s: %w", src.Key(), err)
		}
		st.Reads++
		st.Bytes += int64(len(tail))
		n = more
	}
	ov := &overlay{size: size}
	ov.add(size-n, tail)
	f, err := parquet.OpenFile(ov, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true), parquet.SkipMagicBytes(true), parquet.ReadBufferSize(int(min(n, 1<<20))))
	if err != nil {
		return nil, st, fmt.Errorf("%w: %v", ErrNotIdxObject, err)
	}
	e, err := newEntry(f, size)
	if err != nil {
		return nil, st, err
	}
	l.store(src.Key(), e)
	return e, st, nil
}

func newEntry(f *parquet.File, size int64) (*objectEntry, error) {
	meta := f.Metadata()
	if len(meta.RowGroups) > maxRowGroups {
		return nil, fmt.Errorf("%w: %d row groups", ErrNotIdxObject, len(meta.RowGroups))
	}
	ok := false
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key == FormatKey && len(kv.Value) > 0 && kv.Value[0] == '1' {
			ok = true
		}
	}
	if !ok {
		return nil, fmt.Errorf("%w: no %s entry", ErrNotIdxObject, FormatKey)
	}
	e := &objectEntry{file: f, size: size, meta: meta, cols: map[string]int{}}
	for i, c := range f.Schema().Columns() {
		e.cols[c[0]] = i
	}
	for _, name := range []string{TimeField, StreamField, IDField, StartField, EndField, DurationField} {
		if _, ok := e.cols[name]; !ok {
			return nil, fmt.Errorf("%w: column %s missing", ErrNotIdxObject, name)
		}
	}
	idCol := e.cols[IDField]
	for i := range meta.RowGroups {
		cols := meta.RowGroups[i].Columns
		if len(cols) != len(e.cols) {
			return nil, fmt.Errorf("%w: row group %d has %d columns", ErrNotIdxObject, i, len(cols))
		}
		s := cols[idCol].MetaData.Statistics
		e.idMin = append(e.idMin, slices.Clone(s.MinValue))
		e.idMax = append(e.idMax, slices.Clone(s.MaxValue))
	}
	return e, nil
}

// Lookup returns the rows whose trace_id_idx is id, with the columns want. The
// rows come back with Stream, Start, ... zero for columns not in want.
func (l *Locator) Lookup(ctx context.Context, src Source, id string, want Want) ([]Row, LookupStats, error) {
	e, st, err := l.open(ctx, src)
	if err != nil {
		return nil, *st, err
	}
	var rows []Row
	for rg := range e.meta.RowGroups {
		if len(e.idMin[rg]) == 0 || id < string(e.idMin[rg]) || id > string(e.idMax[rg]) {
			continue
		}
		st.RowGroups++
		got, err := l.lookupGroup(ctx, src, e, rg, id, want, st)
		if err != nil {
			return nil, *st, err
		}
		rows = append(rows, got...)
	}
	return rows, *st, nil
}

type pageRef struct {
	off, size, first int64
}

// pagesOf decodes an offset index slice (already validated) into page references.
func pagesOf(oi parquet.OffsetIndex, fileSize int64) ([]pageRef, error) {
	out := make([]pageRef, 0, oi.NumPages())
	for i := 0; i < oi.NumPages(); i++ {
		p := pageRef{off: oi.Offset(i), size: oi.CompressedPageSize(i), first: oi.FirstRowIndex(i)}
		if p.off < 0 || p.size <= 0 || p.size > maxPageBytes || p.off+p.size > fileSize || p.first < 0 {
			return nil, fmt.Errorf("tracesidx: bad page location %+v", p)
		}
		if i > 0 && p.first <= out[i-1].first {
			return nil, fmt.Errorf("tracesidx: page rows not increasing")
		}
		out = append(out, p)
	}
	return out, nil
}

func (l *Locator) lookupGroup(ctx context.Context, src Source, e *objectEntry, rg int, id string, want Want, st *LookupStats) ([]Row, error) {
	group := e.file.RowGroups()[rg]
	chunks := group.ColumnChunks()
	idChunk, ok := chunks[e.cols[IDField]].(*parquet.FileColumnChunk)
	if !ok {
		return nil, fmt.Errorf("tracesidx: unexpected column chunk type")
	}
	ov := &overlay{size: e.size}
	cc := func(col string) *format.ColumnChunk { return &e.meta.RowGroups[rg].Columns[e.cols[col]] }

	// 1. In one round: the id column's column index, and the offset indexes of the id
	// column and of every wanted column (the writer lays a row group's offset indexes
	// out back to back, so these coalesce into one read).
	idc := cc(IDField)
	cols := []struct {
		w    Want
		name string
	}{{WantTime, TimeField}, {WantStream, StreamField}, {WantStart, StartField}, {WantEnd, EndField}, {WantDuration, DurationField}}
	idxReqs := []rng{
		{idc.ColumnIndexOffset, int64(idc.ColumnIndexLength)},
		{idc.OffsetIndexOffset, int64(idc.OffsetIndexLength)},
	}
	for _, c := range cols {
		if want&c.w != 0 {
			m := cc(c.name)
			idxReqs = append(idxReqs, rng{m.OffsetIndexOffset, int64(m.OffsetIndexLength)})
		}
	}
	if err := l.fetch(ctx, src, ov, st, KindIndex, indexCoalesceGap, idxReqs); err != nil {
		return nil, err
	}
	ci, err := idChunk.ColumnIndexFrom(ov)
	if err != nil {
		return nil, fmt.Errorf("tracesidx: id column index: %w", err)
	}
	oi, err := idChunk.OffsetIndexFrom(ov)
	if err != nil {
		return nil, fmt.Errorf("tracesidx: id offset index: %w", err)
	}
	pages, err := pagesOf(oi, e.size)
	if err != nil {
		return nil, err
	}
	if ci.NumPages() != len(pages) {
		return nil, fmt.Errorf("tracesidx: column and offset index disagree (%d vs %d pages)", ci.NumPages(), len(pages))
	}
	// 2. The pages whose [min, max] holds the id.
	idv := []byte(id)
	var cand []int
	for p := range pages {
		if bytes.Compare(ci.MinValue(p).ByteArray(), idv) <= 0 && bytes.Compare(idv, ci.MaxValue(p).ByteArray()) <= 0 {
			cand = append(cand, p)
		}
	}
	if len(cand) == 0 {
		return nil, nil
	}
	reqs := make([]rng, len(cand))
	for i, p := range cand {
		reqs[i] = rng{pages[p].off, pages[p].size}
	}
	if err := l.fetch(ctx, src, ov, st, KindPage, 0, reqs); err != nil {
		return nil, err
	}
	st.PagesRead += len(cand)
	var matchRows []int64
	for _, p := range cand {
		vals, err := readPage(idChunk, ov, pages[p].first, pageRows(pages, p, group.NumRows()))
		if err != nil {
			return nil, err
		}
		for i, v := range vals {
			if string(v.ByteArray()) == id {
				matchRows = append(matchRows, pages[p].first+int64(i))
			}
		}
	}
	if len(matchRows) == 0 {
		return nil, nil
	}
	rows := make([]Row, len(matchRows))
	for i := range rows {
		rows[i].TraceID = id
	}
	// 3. The other wanted columns, at the matching rows.
	type colPlan struct {
		name  string
		chunk *parquet.FileColumnChunk
		pages []pageRef
	}
	var plans []colPlan
	for _, c := range cols {
		if want&c.w == 0 {
			continue
		}
		ch, ok := chunks[e.cols[c.name]].(*parquet.FileColumnChunk)
		if !ok {
			return nil, fmt.Errorf("tracesidx: unexpected column chunk type")
		}
		plans = append(plans, colPlan{name: c.name, chunk: ch})
	}
	var pageReqs []rng
	type need struct{ plan, page int }
	seen := map[need]bool{}
	for i := range plans {
		oi, err := plans[i].chunk.OffsetIndexFrom(ov)
		if err != nil {
			return nil, fmt.Errorf("tracesidx: %s offset index: %w", plans[i].name, err)
		}
		pg, err := pagesOf(oi, e.size)
		if err != nil {
			return nil, err
		}
		plans[i].pages = pg
		for _, r := range matchRows {
			p := pageAt(pg, r)
			if p < 0 {
				return nil, fmt.Errorf("tracesidx: row %d outside the pages of %s", r, plans[i].name)
			}
			if !seen[need{i, p}] {
				seen[need{i, p}] = true
				pageReqs = append(pageReqs, rng{pg[p].off, pg[p].size})
			}
		}
	}
	if err := l.fetch(ctx, src, ov, st, KindPage, 0, pageReqs); err != nil {
		return nil, err
	}
	st.PagesRead += len(pageReqs)
	for i := range plans {
		for ri, r := range matchRows {
			p := pageAt(plans[i].pages, r)
			vals, err := readPage(plans[i].chunk, ov, r, 1)
			if err != nil || len(vals) != 1 {
				return nil, fmt.Errorf("tracesidx: read %s row %d (page %d): %v", plans[i].name, r, p, err)
			}
			switch plans[i].name {
			case TimeField:
				rows[ri].Time = vals[0].Int64()
			case StreamField:
				rows[ri].Stream = vals[0].Int32()
			case StartField:
				rows[ri].Start = vals[0].Int64()
			case EndField:
				rows[ri].End = vals[0].Int64()
			case DurationField:
				rows[ri].Duration = vals[0].Int64()
			}
		}
	}
	return rows, nil
}

func pageAt(pages []pageRef, row int64) int {
	i := sort.Search(len(pages), func(i int) bool { return pages[i].first > row }) - 1
	return i
}

func pageRows(pages []pageRef, p int, groupRows int64) int64 {
	if p+1 < len(pages) {
		return pages[p+1].first - pages[p].first
	}
	return groupRows - pages[p].first
}

// readPage reads n values of a column chunk starting at row, from pages already in ov.
func readPage(chunk *parquet.FileColumnChunk, ov *overlay, row, n int64) ([]parquet.Value, error) {
	pg := chunk.PagesFrom(ov)
	defer pg.Close()
	if err := pg.SeekToRow(row); err != nil {
		return nil, fmt.Errorf("tracesidx: seek to row %d: %w", row, err)
	}
	page, err := pg.ReadPage()
	if err != nil {
		return nil, fmt.Errorf("tracesidx: read page at row %d: %w", row, err)
	}
	defer parquet.Release(page)
	if page.NumRows() < n {
		n = page.NumRows()
	}
	vals := make([]parquet.Value, n)
	r := page.Values()
	got := 0
	for got < len(vals) {
		k, err := r.ReadValues(vals[got:])
		got += k
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	vals = vals[:got]
	for i := range vals {
		vals[i] = vals[i].Clone()
	}
	return vals, nil
}

type rng struct{ off, n int64 }

// fetch reads the ranges (coalescing neighbours closer than gap) concurrently and
// adds them to ov. Every slice an object's footer points at is bounds-checked, and
// a column or offset index slice is structurally validated, before the parquet
// library decodes it, so a hostile object cannot make a lookup allocate or read more
// than the limits above.
func (l *Locator) fetch(ctx context.Context, src Source, ov *overlay, st *LookupStats, kind ReadKind, gap int64, reqs []rng) error {
	if len(reqs) == 0 {
		return nil
	}
	limit := int64(maxPageBytes)
	if kind == KindIndex {
		limit = maxIndexBytes
	}
	for _, r := range reqs {
		if r.off <= 0 || r.n <= 0 || r.n > limit || r.off+r.n > src.Size() {
			return fmt.Errorf("tracesidx: range %d+%d outside the object or over the %s limit", r.off, r.n, kind)
		}
	}
	sorted := slices.Clone(reqs)
	slices.SortFunc(sorted, func(a, b rng) int { return int(a.off - b.off) })
	merged := []rng{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		if r.off <= last.off+last.n+gap {
			if end := r.off + r.n; end > last.off+last.n {
				last.n = end - last.off
			}
			continue
		}
		merged = append(merged, r)
	}
	out := make([][]byte, len(merged))
	errs := make([]error, len(merged))
	var wg sync.WaitGroup
	for i, m := range merged {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i], errs[i] = src.ReadRange(ctx, kind, m.off, m.n)
		}()
	}
	wg.Wait()
	for i, m := range merged {
		if errs[i] != nil {
			return fmt.Errorf("tracesidx: read %s %d+%d: %w", kind, m.off, m.n, errs[i])
		}
		if int64(len(out[i])) != m.n {
			return fmt.Errorf("tracesidx: short %s read %d of %d", kind, len(out[i]), m.n)
		}
		st.Reads++
		st.Bytes += m.n
		ov.add(m.off, out[i])
	}
	if kind == KindIndex {
		for _, r := range reqs {
			b, _ := ov.slice(r.off, r.n)
			if err := ValidateThrift(b); err != nil {
				return fmt.Errorf("tracesidx: index slice %d+%d: %w", r.off, r.n, err)
			}
		}
	}
	return nil
}

// overlay is a sparse view of an object: the windows a lookup has fetched. A read
// outside them is an error (a lookup that wandered off its plan), never a silent GET.
type overlay struct {
	size int64
	mu   sync.Mutex
	win  []window
}

type window struct {
	off  int64
	data []byte
}

func (o *overlay) add(off int64, data []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.win = append(o.win, window{off, data})
}

func (o *overlay) slice(off, n int64) ([]byte, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range o.win {
		if off >= w.off && off+n <= w.off+int64(len(w.data)) {
			return w.data[off-w.off : off-w.off+n], true
		}
	}
	return nil, false
}

// ReadAt serves a read from one window; a read that starts inside a window and runs
// past its end returns what the window has and io.EOF (a page reader asks for its
// buffer size, not the page size).
func (o *overlay) ReadAt(p []byte, off int64) (int, error) {
	if off >= o.size {
		return 0, io.EOF
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, w := range o.win {
		if off >= w.off && off < w.off+int64(len(w.data)) {
			n := copy(p, w.data[off-w.off:])
			if n < len(p) {
				return n, io.EOF
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("tracesidx: unplanned read of %d bytes at %d", len(p), off)
}
