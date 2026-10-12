// Package fieldpresence records, per object, which VictoriaLogs/VictoriaTraces
// field names each stream carries, in the object's Parquet footer key-value
// metadata under lakehouse.field_presence.
//
// A hot block lists a field only if some row of the block has a value for it,
// and field_names credits the field with the block's rows. The facet holds the
// same information for an object: per (stream, UTC day), the rows and the set of
// field names with a non-empty value on at least one row. It is computed from
// the rows a writer holds (flush, compaction, delete rewrite) by the read path's
// own row-to-field conversion, so the names are the names a field_names read of
// the object would list; the package only stores and decodes them.
//
// The value is ASCII ("v1:" plus base64 of a zstd-compressed binary payload), so
// the footer stays valid UTF-8 (#340) and readers that do not know the key ignore
// it. An object whose facet would pass MaxKVBytes carries no facet and the marker
// OverCapKey instead, so a reader knows to use the rows.
package fieldpresence

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const (
	// KVKey is the footer key-value key holding the facet.
	KVKey = "lakehouse.field_presence"
	// OverCapKey marks an object whose facet exceeded MaxKVBytes and was left out.
	OverCapKey = "lakehouse.field_presence_over_cap"
	// Prefix is the format version of the value; a reader that does not know it
	// treats the object as having no facet.
	Prefix = "v1:"
	// NamingVersion identifies the set of names the conversion emits for a row
	// (see the golden test next to the converters). A facet written under another
	// naming version is not trusted by a reader.
	NamingVersion = 1
	// MaxKVBytes caps the footer value.
	MaxKVBytes = 1 << 20

	// A builder that has seen more distinct names or stream-days than these gives
	// up and reports the object as over the cap: such a facet is far above
	// MaxKVBytes, and the writer's memory stays bounded whatever the data.

	// maxPayload bounds a decoded payload (a decompression-bomb guard).
	maxPayload = 64 << 20
	// NoDay is the day of rows whose _time could not be formatted.
	NoDay = int32(-1 << 31)
)

// Limits of a Builder (variables so tests can lower them).
var (
	maxNames      = 1 << 20
	maxStreamDays = 1 << 20
)

// Stream key kinds, in the order the counter prefers them: the row's _stream_id,
// else its _stream, else none.
const (
	KindID24   byte = 'i' // _stream_id of 48 lowercase hex digits (8-byte tenant + 16-byte hash), stored as 24 bytes
	KindIDText byte = 'I' // any other _stream_id, stored as text
	KindStream byte = 's' // _stream text (rows without a _stream_id)
	KindNone   byte = 'n' // rows with neither
)

// ErrNotTrusted reports a value this build does not read: another format
// version or another naming version.
var ErrNotTrusted = errors.New("field presence: unsupported version")

// Entry is one stream's rows on one UTC day and the set of its field names.
type Entry struct {
	Day  int32 // days since 1970-01-01 UTC, NoDay when unknown
	Kind byte
	Key  string // raw 24 bytes for KindID24, else the text
	Rows uint64
	Set  uint32 // index into Facet.Sets
}

// Facet is a decoded facet. Names are sorted; every Set lists indexes into
// Names in ascending order; Entries are sorted by (Day, Kind, Key).
type Facet struct {
	Naming  uint32
	Names   []string
	Sets    [][]uint32
	Entries []Entry
}

// NamesOf returns the field names of an entry.
func (f *Facet) NamesOf(e Entry) []string {
	if int(e.Set) >= len(f.Sets) {
		return nil
	}
	out := make([]string, 0, len(f.Sets[e.Set]))
	for _, id := range f.Sets[e.Set] {
		out = append(out, f.Names[id])
	}
	return out
}

// StreamKey is the stream key as the field_names counter derives it from the
// _stream_id and _stream values of a row.
func StreamKey(streamID, stream string) (kind byte, key string) {
	switch {
	case streamID != "":
		if len(streamID) == 48 {
			var raw [24]byte
			if n, err := hex.Decode(raw[:], []byte(streamID)); err == nil && n == 24 && hex.EncodeToString(raw[:]) == streamID {
				return KindID24, string(raw[:])
			}
		}
		return KindIDText, streamID
	case stream != "":
		return KindStream, stream
	}
	return KindNone, ""
}

// DayOf is the UTC day (days since the epoch) of a "YYYY-MM-DD..." timestamp, as
// the counter keys a block on the first ten characters of its _time.
func DayOf(formattedTime string) int32 {
	if len(formattedTime) < 10 {
		return NoDay
	}
	t, err := time.Parse("2006-01-02", formattedTime[:10])
	if err != nil {
		return NoDay
	}
	return int32(t.Unix() / 86400)
}

type streamDay struct {
	day    int32
	id     string // the row's _stream_id
	stream string // its _stream, kept only when it has no _stream_id
}

type acc struct {
	rows uint64
	cols []uint64 // bit set over builder name ids
}

// Builder accumulates a facet. It is not safe for concurrent use.
type Builder struct {
	ids     map[string]int
	names   []string
	streams map[streamDay]*acc
	last    *acc
	lastKey streamDay
	// lastBase is the base ids last added to last (AddRowIDs).
	lastBase []int
	// over is set when a limit was hit: nothing more is accumulated.
	over bool
}

// NewBuilder returns an empty builder.
func NewBuilder() *Builder {
	return &Builder{ids: make(map[string]int), streams: make(map[streamDay]*acc)}
}

// NameID is the builder's id for a field name (registered on first use), or -1
// when the builder has given up (too many distinct names); AddRow ignores -1.
func (b *Builder) NameID(name string) int {
	id, ok := b.ids[name]
	if !ok {
		if len(b.names) >= maxNames {
			b.over = true
			return -1
		}
		name = strings.Clone(name)
		id = len(b.names)
		b.ids[name] = id
		b.names = append(b.names, name)
	}
	return id
}

// AddRow credits one row to its stream-day and unions the field names (by
// NameID) it carries into the stream-day's set. The stream is the row's
// _stream_id, else its _stream, else none, as the field_names counter keys it.
func (b *Builder) AddRow(day int32, streamID, stream string, ids []int) {
	if b.over {
		return
	}
	if streamID != "" {
		stream = ""
	}
	sd := streamDay{day, streamID, stream}
	a := b.last
	if a == nil || sd != b.lastKey {
		a = b.streams[sd]
		if a == nil {
			if len(b.streams) >= maxStreamDays {
				b.over = true
				return
			}
			a = &acc{}
			sd.id, sd.stream = strings.Clone(sd.id), strings.Clone(sd.stream)
			b.streams[sd] = a
		}
		b.last, b.lastKey, b.lastBase = a, sd, nil
	}
	a.rows++
	a.set(ids)
}

// Add is AddRow for a row given by field names, crediting rows rows.
func (b *Builder) Add(day int32, streamID, stream string, rows uint64, names []string) {
	ids := make([]int, len(names))
	for i, n := range names {
		ids[i] = b.NameID(n)
	}
	b.AddRow(day, streamID, stream, ids)
	if b.last != nil && !b.over {
		b.last.rows += rows - 1
	}
}

// Streams is the number of stream-days accumulated.
func (b *Builder) Streams() int { return len(b.streams) }

// Facet returns the accumulated facet in canonical order.
func (b *Builder) Facet() *Facet {
	f := &Facet{Naming: NamingVersion}
	order := make([]int, len(b.names))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return b.names[order[i]] < b.names[order[j]] })
	newID := make([]uint32, len(b.names))
	f.Names = make([]string, len(order))
	for pos, old := range order {
		newID[old] = uint32(pos)
		f.Names[pos] = b.names[old]
	}
	setIdx := make(map[string]uint32)
	f.Entries = make([]Entry, 0, len(b.streams))
	var ids []uint32
	var keyBuf []byte
	for sd, a := range b.streams {
		kind, key := StreamKey(sd.id, sd.stream)
		ids = ids[:0]
		for w, word := range a.cols {
			for ; word != 0; word &= word - 1 {
				ids = append(ids, newID[w*64+bits.TrailingZeros64(word)])
			}
		}
		slices.Sort(ids)
		keyBuf = keyBuf[:0]
		for _, id := range ids {
			keyBuf = binary.AppendUvarint(keyBuf, uint64(id))
		}
		si, ok := setIdx[string(keyBuf)]
		if !ok {
			si = uint32(len(f.Sets))
			setIdx[string(keyBuf)] = si
			f.Sets = append(f.Sets, slices.Clone(ids))
		}
		f.Entries = append(f.Entries, Entry{Day: sd.day, Kind: kind, Key: key, Rows: a.rows, Set: si})
	}
	sort.Slice(f.Entries, func(i, j int) bool { return entryLess(f.Entries[i], f.Entries[j]) })
	// Set tables are numbered by first use in entry order, so the encoding does
	// not depend on map iteration order.
	renum := make([]int64, len(f.Sets))
	for i := range renum {
		renum[i] = -1
	}
	var sets [][]uint32
	for i := range f.Entries {
		s := f.Entries[i].Set
		if renum[s] < 0 {
			renum[s] = int64(len(sets))
			sets = append(sets, f.Sets[s])
		}
		f.Entries[i].Set = uint32(renum[s])
	}
	f.Sets = sets
	return f
}

func entryLess(a, b Entry) bool {
	if a.Day != b.Day {
		return a.Day < b.Day
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.Key < b.Key
}

// Result is a facet ready to be written to a footer.
type Result struct {
	Value   string // the KV value, empty when OverCap
	OverCap bool
	Streams int
}

// Finish encodes the accumulated facet.
func (b *Builder) Finish() Result {
	if b.over {
		return Result{OverCap: true, Streams: len(b.streams)}
	}
	return b.Facet().Encode()
}

// Encode returns the footer value of the facet, or the over-cap marker.
func (f *Facet) Encode() Result {
	payload := f.marshal()
	enc := encoder()
	z := enc.EncodeAll(payload, make([]byte, 0, len(payload)/3+16))
	n := len(Prefix) + base64.StdEncoding.EncodedLen(len(z))
	if n > MaxKVBytes {
		return Result{OverCap: true, Streams: len(f.Entries)}
	}
	var sb strings.Builder
	sb.Grow(n)
	sb.WriteString(Prefix)
	sb.WriteString(base64.StdEncoding.EncodeToString(z))
	return Result{Value: sb.String(), Streams: len(f.Entries)}
}

// Options are the footer key-values of the result: the facet, or the marker.
func (r Result) Options() []parquet.WriterOption {
	if r.OverCap {
		return []parquet.WriterOption{parquet.KeyValueMetadata(OverCapKey, "1")}
	}
	if r.Value == "" {
		return nil
	}
	return []parquet.WriterOption{parquet.KeyValueMetadata(KVKey, r.Value)}
}

var (
	encPool atomic.Pointer[zstd.Encoder]
	decPool atomic.Pointer[zstd.Decoder]
)

func encoder() *zstd.Encoder {
	if e := encPool.Load(); e != nil {
		return e
	}
	e, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	if !encPool.CompareAndSwap(nil, e) {
		return encPool.Load()
	}
	return e
}

func decoder() *zstd.Decoder {
	if d := decPool.Load(); d != nil {
		return d
	}
	d, _ := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxPayload), zstd.WithDecoderConcurrency(1))
	if !decPool.CompareAndSwap(nil, d) {
		return decPool.Load()
	}
	return d
}

// idPrefix is the longest common prefix of the KindID24 keys.
func (f *Facet) idPrefix() string {
	prefix, first := "", true
	for _, e := range f.Entries {
		if e.Kind != KindID24 {
			continue
		}
		if first {
			prefix, first = e.Key, false
			continue
		}
		n := 0
		for n < len(prefix) && n < len(e.Key) && prefix[n] == e.Key[n] {
			n++
		}
		prefix = prefix[:n]
	}
	return prefix
}

func (f *Facet) marshal() []byte {
	b := make([]byte, 0, 64+len(f.Entries)*28)
	b = binary.AppendUvarint(b, uint64(f.Naming))
	b = binary.AppendUvarint(b, uint64(len(f.Names)))
	for _, n := range f.Names {
		b = binary.AppendUvarint(b, uint64(len(n)))
		b = append(b, n...)
	}
	b = binary.AppendUvarint(b, uint64(len(f.Sets)))
	for _, s := range f.Sets {
		b = binary.AppendUvarint(b, uint64(len(s)))
		prev := uint32(0)
		for _, id := range s {
			b = binary.AppendUvarint(b, uint64(id-prev))
			prev = id
		}
	}
	// Every id of an object shares its tenant: the longest common prefix of the
	// 24-byte ids is stored once.
	prefix := f.idPrefix()
	b = binary.AppendUvarint(b, uint64(len(prefix)))
	b = append(b, prefix...)
	b = binary.AppendUvarint(b, uint64(len(f.Entries)))
	prevDay := int64(0)
	for i, e := range f.Entries {
		if i == 0 {
			b = binary.AppendVarint(b, int64(e.Day))
		} else {
			b = binary.AppendUvarint(b, uint64(int64(e.Day)-prevDay))
		}
		prevDay = int64(e.Day)
		b = append(b, e.Kind)
		if e.Kind == KindID24 {
			b = append(b, e.Key[len(prefix):]...)
		} else {
			b = binary.AppendUvarint(b, uint64(len(e.Key)))
			b = append(b, e.Key...)
		}
		b = binary.AppendUvarint(b, e.Rows)
		b = binary.AppendUvarint(b, uint64(e.Set))
	}
	return b
}

// Decode parses a footer value. It never panics on hostile input; a value of
// another format or naming version returns ErrNotTrusted.
func Decode(value string) (*Facet, error) {
	if !strings.HasPrefix(value, Prefix) {
		return nil, fmt.Errorf("%w: missing %q prefix", ErrNotTrusted, Prefix)
	}
	if len(value) > MaxKVBytes {
		return nil, errors.New("field presence: value over the cap")
	}
	z, err := base64.StdEncoding.DecodeString(value[len(Prefix):])
	if err != nil {
		return nil, fmt.Errorf("field presence: base64: %w", err)
	}
	payload, err := decoder().DecodeAll(z, nil)
	if err != nil {
		return nil, fmt.Errorf("field presence: zstd: %w", err)
	}
	return unmarshal(payload)
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errors.New("field presence: truncated varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errors.New("field presence: truncated varint")
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.err = errors.New("field presence: truncated payload")
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// count reads a length that must be satisfiable by the bytes left (each element
// takes at least min bytes), so a hostile count cannot size an allocation.
func (r *reader) count(min uint64) int {
	n := r.uvarint()
	if r.err != nil {
		return 0
	}
	if n > uint64(len(r.b))/min {
		r.err = errors.New("field presence: count exceeds payload")
		return 0
	}
	return int(n)
}

func unmarshal(payload []byte) (*Facet, error) {
	r := &reader{b: payload}
	f := &Facet{}
	naming := r.uvarint()
	if r.err == nil && naming != NamingVersion {
		return nil, fmt.Errorf("%w: naming %d", ErrNotTrusted, naming)
	}
	f.Naming = uint32(naming)
	nNames := r.count(1)
	f.Names = make([]string, 0, nNames)
	for i := 0; i < nNames && r.err == nil; i++ {
		l := r.uvarint()
		f.Names = append(f.Names, string(r.bytes(l)))
	}
	nSets := r.count(1)
	f.Sets = make([][]uint32, 0, nSets)
	for i := 0; i < nSets && r.err == nil; i++ {
		n := r.count(1)
		set := make([]uint32, 0, n)
		prev := uint64(0)
		for j := 0; j < n && r.err == nil; j++ {
			prev += r.uvarint()
			if prev >= uint64(len(f.Names)) {
				r.err = errors.New("field presence: name id out of range")
				break
			}
			set = append(set, uint32(prev))
		}
		f.Sets = append(f.Sets, set)
	}
	prefix := string(r.bytes(r.uvarint()))
	if r.err == nil && len(prefix) > 24 {
		r.err = errors.New("field presence: id prefix too long")
	}
	nEntries := r.count(3)
	f.Entries = make([]Entry, 0, nEntries)
	prevDay := int64(0)
	for i := 0; i < nEntries && r.err == nil; i++ {
		var e Entry
		if i == 0 {
			prevDay = r.varint()
		} else {
			prevDay += int64(r.uvarint())
		}
		if prevDay < -1<<31 || prevDay > 1<<31-1 {
			r.err = errors.New("field presence: day out of range")
			break
		}
		e.Day = int32(prevDay)
		kind := r.bytes(1)
		if r.err != nil {
			break
		}
		e.Kind = kind[0]
		switch e.Kind {
		case KindID24:
			e.Key = prefix + string(r.bytes(uint64(24-len(prefix))))
		case KindIDText, KindStream, KindNone:
			e.Key = string(r.bytes(r.uvarint()))
		default:
			r.err = fmt.Errorf("field presence: unknown stream kind %d", e.Kind)
		}
		e.Rows = r.uvarint()
		set := r.uvarint()
		if r.err == nil && set >= uint64(len(f.Sets)) {
			r.err = errors.New("field presence: set id out of range")
		}
		e.Set = uint32(set)
		f.Entries = append(f.Entries, e)
	}
	if r.err != nil {
		return nil, r.err
	}
	if len(r.b) != 0 {
		return nil, errors.New("field presence: trailing bytes")
	}
	return f, nil
}

// Equal reports whether two facets describe the same stream-days, rows and
// field-name sets (independent of set numbering).
func Equal(a, b *Facet) bool {
	if len(a.Entries) != len(b.Entries) {
		return false
	}
	for i := range a.Entries {
		ea, eb := a.Entries[i], b.Entries[i]
		if ea.Day != eb.Day || ea.Kind != eb.Kind || ea.Key != eb.Key || ea.Rows != eb.Rows {
			return false
		}
		if !slices.Equal(a.NamesOf(ea), b.NamesOf(eb)) {
			return false
		}
	}
	return true
}

// FromFooter reads the facet out of a Parquet footer's key-values. present is
// false when the footer has neither the facet nor the marker; overCap is true
// when the writer left the facet out for its size.
func FromFooter(kvs []format.KeyValue) (f *Facet, overCap bool, err error) {
	for _, kv := range kvs {
		switch kv.Key {
		case OverCapKey:
			overCap = true
		case KVKey:
			f, err = Decode(kv.Value)
		}
	}
	return f, overCap, err
}

// ---- writer hooks ---------------------------------------------------------

// Computers produce the facet builder of a batch of rows. Each binary installs
// the ones that use its own read path's row-to-field conversion (the root and
// the traces module name fields differently), so the shared compactor and
// delete rewriter, which cannot import either storage package, call through
// them. slots is the Tier-2 slot binding being written into the same footer.
type Computers struct {
	Logs   func(rows []schema.LogRow, slots schema.SlotMapping) *Builder
	Traces func(rows []schema.TraceRow, slots schema.SlotMapping) *Builder
}

var computers atomic.Pointer[Computers]

// SetComputers installs the row-to-facet functions (called once at startup by
// each binary, before any writer runs).
func SetComputers(c Computers) { computers.Store(&c) }

// LogsOptions are the footer key-values for rows being written as a logs object:
// none when no computer is installed (unit tests of the shared writers).
func LogsOptions(rows []schema.LogRow, slots schema.SlotMapping) []parquet.WriterOption {
	c := computers.Load()
	if c == nil || c.Logs == nil || len(rows) == 0 {
		return nil
	}
	return observe(c.Logs(rows, slots), "logs")
}

// TracesOptions is LogsOptions for spans.
func TracesOptions(rows []schema.TraceRow, slots schema.SlotMapping) []parquet.WriterOption {
	c := computers.Load()
	if c == nil || c.Traces == nil || len(rows) == 0 {
		return nil
	}
	return observe(c.Traces(rows, slots), "traces")
}

// Observe encodes a builder and records the metrics. Exported for the flush
// writers, which hold the builder already.
func Observe(b *Builder, signal string) []parquet.WriterOption { return observe(b, signal) }

func observe(b *Builder, signal string) []parquet.WriterOption {
	start := time.Now()
	res := b.Finish()
	metrics.FieldPresenceBuildDuration.Observe(time.Since(start).Seconds())
	if res.OverCap {
		metrics.FieldPresenceOverCap.Inc(signal)
	} else {
		metrics.FieldPresenceBytes.Observe(signal, float64(len(res.Value)))
	}
	return res.Options()
}

// AddRowIDs is AddRow for a row whose names are the union of base (the same
// slice for every row of one pattern) and extra. The union of a base already
// added to the stream-day is skipped.
func (b *Builder) AddRowIDs(day int32, streamID, stream string, base, extra []int) {
	if b.over {
		return
	}
	if streamID != "" {
		stream = ""
	}
	sd := streamDay{day, streamID, stream}
	a := b.last
	if a == nil || sd != b.lastKey {
		a = b.streams[sd]
		if a == nil {
			if len(b.streams) >= maxStreamDays {
				b.over = true
				return
			}
			a = &acc{}
			sd.id, sd.stream = strings.Clone(sd.id), strings.Clone(sd.stream)
			b.streams[sd] = a
		}
		b.last, b.lastKey, b.lastBase = a, sd, nil
	}
	a.rows++
	if len(base) > 0 && (len(b.lastBase) != len(base) || &b.lastBase[0] != &base[0]) {
		a.set(base)
		b.lastBase = base
	}
	a.set(extra)
}

func (a *acc) set(ids []int) {
	for _, id := range ids {
		if id < 0 {
			continue
		}
		for id/64 >= len(a.cols) {
			a.cols = append(a.cols, 0)
		}
		a.cols[id/64] |= 1 << (id % 64)
	}
}
