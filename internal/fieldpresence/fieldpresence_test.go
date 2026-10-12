package fieldpresence

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// randomBuilder fills a builder with random stream-days, rows and name sets.
func randomBuilder(rng *rand.Rand, streams, names int) *Builder {
	b := NewBuilder()
	for i := 0; i < streams*3; i++ {
		s := rng.Intn(streams)
		var id, stream string
		switch rng.Intn(4) {
		case 0:
			stream = fmt.Sprintf(`{s="%d"}`, s)
		case 1:
		case 2:
			id = fmt.Sprintf("text-id-%d", s)
		default:
			id = fmt.Sprintf("%048x", uint64(s)*2654435761+1)
		}
		var ns []string
		for j := 0; j < 1+rng.Intn(8); j++ {
			ns = append(ns, fmt.Sprintf("field.%d", rng.Intn(names)))
		}
		b.Add(int32(20000+rng.Intn(3)), id, stream, uint64(1+rng.Intn(1000)), ns)
	}
	return b
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		f := randomBuilder(rng, 1+rng.Intn(200), 1+rng.Intn(60)).Facet()
		res := f.Encode()
		if res.OverCap || res.Value == "" {
			t.Fatalf("seed %d: unexpected over-cap", seed)
		}
		got, err := Decode(res.Value)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !Equal(f, got) {
			t.Fatalf("seed %d: decoded facet differs", seed)
		}
		if !utf8.ValidString(res.Value) || !strings.HasPrefix(res.Value, Prefix) {
			t.Fatalf("seed %d: value is not an ASCII v1 string", seed)
		}
		for _, r := range res.Value {
			if r > 127 {
				t.Fatalf("seed %d: non-ASCII byte in the value", seed)
			}
		}
	}
}

// The footer value is byte-reproducible: insertion order and map iteration order
// do not change it (the writers' byte-reproducibility tests depend on it).
func TestEncodingIsDeterministic(t *testing.T) {
	type row struct {
		day        int32
		id, stream string
		names      []string
	}
	rng := rand.New(rand.NewSource(7))
	var rows []row
	for i := 0; i < 300; i++ {
		rows = append(rows, row{int32(20000 + rng.Intn(2)), fmt.Sprintf("%048x", rng.Intn(20)), "", []string{fmt.Sprintf("n%d", rng.Intn(30)), fmt.Sprintf("n%d", rng.Intn(30))}})
	}
	enc := func(order []int) string {
		b := NewBuilder()
		for _, i := range order {
			b.Add(rows[i].day, rows[i].id, rows[i].stream, 1, rows[i].names)
		}
		return b.Finish().Value
	}
	order := rng.Perm(len(rows))
	want := enc(order)
	for i := 0; i < 5; i++ {
		rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
		if got := enc(order); got != want {
			t.Fatal("the encoding depends on the order rows were added in")
		}
	}
}

func TestStreamKey(t *testing.T) {
	hex48 := strings.Repeat("ab", 24)
	for _, tc := range []struct {
		id, stream string
		kind       byte
		keyLen     int
	}{
		{hex48, "ignored", KindID24, 24},
		{strings.ToUpper(hex48), "", KindIDText, 48}, // not the lowercase form: kept verbatim
		{"short-id", "{a=b}", KindIDText, 8},
		{"", "{a=b}", KindStream, 5},
		{"", "", KindNone, 0},
		{strings.Repeat("zz", 24), "", KindIDText, 48},
	} {
		kind, key := StreamKey(tc.id, tc.stream)
		if kind != tc.kind || len(key) != tc.keyLen {
			t.Errorf("StreamKey(%q,%q) = %c/%d, want %c/%d", tc.id, tc.stream, kind, len(key), tc.kind, tc.keyLen)
		}
	}
	if _, key := StreamKey(hex48, ""); hex.EncodeToString([]byte(key)) != hex48 {
		t.Error("a 48-digit id is not stored as its 24 raw bytes")
	}
}

func TestDayOf(t *testing.T) {
	if got := DayOf("1970-01-02T00:00:00Z"); got != 1 {
		t.Errorf("DayOf = %d, want 1", got)
	}
	if got := DayOf("2026-07-01T23:59:59.999999999Z"); got != 20635 {
		t.Errorf("DayOf = %d, want 20635", got)
	}
	for _, bad := range []string{"", "2026-07", "not-a-date!!"} {
		if got := DayOf(bad); got != NoDay {
			t.Errorf("DayOf(%q) = %d, want NoDay", bad, got)
		}
	}
}

// A row is credited to the stream-day of its _stream_id, else its _stream; a
// row with an id never splits over the streams that happen to share its _stream.
func TestBuilderKeysRowsLikeTheCounter(t *testing.T) {
	b := NewBuilder()
	id := strings.Repeat("0f", 24)
	b.Add(1, id, `{a="1"}`, 1, []string{"x"})
	b.Add(1, id, `{a="2"}`, 1, []string{"y"}) // same id, other _stream: the same stream
	b.Add(1, "", `{a="1"}`, 1, []string{"z"}) // no id: keyed by _stream
	b.Add(2, id, "", 1, []string{"x"})        // another day: another block
	f := b.Facet()
	if len(f.Entries) != 3 {
		t.Fatalf("%d stream-days, want 3: %+v", len(f.Entries), f.Entries)
	}
	for _, e := range f.Entries {
		if e.Kind == KindID24 && e.Day == 1 {
			if e.Rows != 2 || strings.Join(f.NamesOf(e), ",") != "x,y" {
				t.Errorf("id stream-day = %d rows %v, want 2 rows x,y", e.Rows, f.NamesOf(e))
			}
		}
	}
}

func TestOverCapStoresNoFacetButTheMarker(t *testing.T) {
	b := NewBuilder()
	rng := rand.New(rand.NewSource(3))
	// Enough random 24-byte ids that the (incompressible) keys pass the cap.
	for i := 0; MaxKVBytes > 0 && i < 60000; i++ {
		b.Add(1, fmt.Sprintf("%016x%032x", rng.Uint64(), rng.Uint64())[:48], "", 1, []string{"a"})
	}
	res := b.Finish()
	if !res.OverCap || res.Value != "" {
		t.Fatalf("a facet of %d streams must be over the cap (value %d bytes)", res.Streams, len(res.Value))
	}
	opts := res.Options()
	if len(opts) != 1 {
		t.Fatalf("over-cap result has %d options, want the marker alone", len(opts))
	}
	if !footerHas(t, opts, OverCapKey, "1") || footerHas(t, opts, KVKey, "") {
		t.Fatal("over-cap object must carry the marker and no facet")
	}
}

// footerHas writes a tiny Parquet file with the options and checks a footer key.
func footerHas(t *testing.T, opts []parquet.WriterOption, key, want string) bool {
	t.Helper()
	data := writeWithOptions(t, opts)
	f, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range f.Metadata().KeyValueMetadata {
		if kv.Key == key {
			return want == "" || kv.Value == want
		}
	}
	return false
}

func writeWithOptions(t *testing.T, opts []parquet.WriterOption) []byte {
	t.Helper()
	var sb strings.Builder
	w := parquet.NewGenericWriter[schema.LogRow](&sb, opts...)
	if _, err := w.Write([]schema.LogRow{{TimestampUnixNano: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return []byte(sb.String())
}

func TestFromFooterReadsTheFacetAlone(t *testing.T) {
	b := NewBuilder()
	b.Add(5, "", `{s="1"}`, 3, []string{"_msg", "level"})
	data := writeWithOptions(t, b.Finish().Options())
	pf, err := parquet.OpenFile(strings.NewReader(string(data)), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	f, overCap, err := FromFooter(pf.Metadata().KeyValueMetadata)
	if err != nil || overCap || f == nil || len(f.Entries) != 1 || f.Entries[0].Rows != 3 {
		t.Fatalf("FromFooter = %+v overCap=%v err=%v", f, overCap, err)
	}
}

func TestDecodeRejectsWhatItDoesNotKnow(t *testing.T) {
	good := NewBuilder()
	good.Add(1, "", "s", 1, []string{"a"})
	val := good.Finish().Value
	for name, v := range map[string]string{
		"no prefix":     val[len(Prefix):],
		"other version": "v2:" + val[len(Prefix):],
		"bad base64":    Prefix + "!!!!",
		"bad zstd":      Prefix + base64.StdEncoding.EncodeToString([]byte("not zstd")),
		"empty":         "",
		"prefix only":   Prefix,
	} {
		if f, err := Decode(v); err == nil || f != nil {
			t.Errorf("%s: Decode accepted %q", name, v)
		}
	}
	// Another naming version is not trusted.
	f := good.Facet()
	f.Naming = NamingVersion + 1
	if _, err := Decode(f.Encode().Value); err == nil {
		t.Error("a facet of another naming version was accepted")
	}
}

// Hostile payloads (counts that exceed the bytes left, ids out of range, trailing
// bytes) are errors, never panics or large allocations.
func TestUnmarshalHostilePayloads(t *testing.T) {
	good := NewBuilder()
	good.Add(1, strings.Repeat("ab", 24), "", 2, []string{"a", "b"})
	payload := good.Facet().marshal()
	for i := 0; i < len(payload); i++ {
		_, _ = unmarshal(payload[:i]) // truncated
	}
	rng := rand.New(rand.NewSource(11))
	for i := 0; i < 5000; i++ {
		mut := append([]byte(nil), payload...)
		for k := 0; k < 1+rng.Intn(4); k++ {
			mut[rng.Intn(len(mut))] = byte(rng.Intn(256))
		}
		_, _ = unmarshal(mut)
	}
	if _, err := unmarshal(append(append([]byte(nil), payload...), 0)); err == nil {
		t.Error("trailing bytes accepted")
	}
	if _, err := unmarshal([]byte{NamingVersion, 0xff, 0xff, 0xff, 0xff, 0x0f}); err == nil {
		t.Error("a name count larger than the payload was accepted")
	}
}

func FuzzDecode(f *testing.F) {
	b := NewBuilder()
	b.Add(1, strings.Repeat("0a", 24), "", 4, []string{"_msg", "level", "service.name"})
	b.Add(2, "", `{s="x"}`, 1, []string{"_msg"})
	f.Add(b.Finish().Value)
	f.Add(Prefix)
	f.Add("v1:AAAA")
	f.Fuzz(func(t *testing.T, value string) {
		facet, err := Decode(value)
		if err != nil {
			return
		}
		// Whatever decodes re-encodes to a value that decodes to the same facet.
		again, err := Decode(facet.Encode().Value)
		if err != nil || !Equal(facet, again) {
			t.Fatalf("decoded facet does not round trip: %v", err)
		}
	})
}

func FuzzUnmarshalPayload(f *testing.F) {
	b := NewBuilder()
	b.Add(1, strings.Repeat("0a", 24), "", 4, []string{"a", "b"})
	f.Add(b.Facet().marshal())
	f.Fuzz(func(t *testing.T, payload []byte) {
		if facet, err := unmarshal(payload); err == nil {
			// A decoded facet is internally consistent.
			for _, e := range facet.Entries {
				if int(e.Set) >= len(facet.Sets) {
					t.Fatal("set out of range")
				}
				for _, id := range facet.Sets[e.Set] {
					if int(id) >= len(facet.Names) {
						t.Fatal("name out of range")
					}
				}
			}
		}
	})
}

// FuzzEncodeRoundTrip builds a facet from fuzzed rows: it always encodes to a
// valid ASCII value and decodes back to the same facet.
func FuzzEncodeRoundTrip(f *testing.F) {
	f.Add([]byte("seed"), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, n uint8) {
		b := NewBuilder()
		for i := 0; i < int(n); i++ {
			name := string(data[i%(len(data)+1):])
			if len(name) > 40 {
				name = name[:40]
			}
			b.Add(int32(i%3), string(data), "", uint64(i+1), []string{name, "const"})
		}
		res := b.Finish()
		if res.OverCap {
			return
		}
		for _, r := range res.Value {
			if r > 127 {
				t.Fatal("non-ASCII value")
			}
		}
		got, err := Decode(res.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !Equal(b.Facet(), got) {
			t.Fatal("round trip differs")
		}
	})
}

// TestPatternCoversEveryStringColumn fails when a string column is added to a row
// type and not to its pattern function: the names of a row are computed once per
// pattern, so a column the pattern does not read could hide a field.
func TestPatternCoversEveryStringColumn(t *testing.T) {
	check := func(name string, typ reflect.Type, zero any, set func(v reflect.Value, i int), pattern func(any) uint64) {
		t.Helper()
		base := pattern(zero)
		seen := map[uint64]string{}
		for i := 0; i < typ.NumField(); i++ {
			sf := typ.Field(i)
			if sf.Type.Kind() != reflect.String {
				continue
			}
			v := reflect.New(typ).Elem()
			v.Field(i).SetString("x")
			p := pattern(v.Addr().Interface())
			if p == base {
				t.Errorf("%s.%s is not read by its pattern function", name, sf.Name)
			}
			if prev, dup := seen[p&^base]; dup {
				t.Errorf("%s.%s and %s share a pattern bit", name, sf.Name, prev)
			}
			seen[p&^base] = sf.Name
		}
	}
	check("LogRow", reflect.TypeOf(schema.LogRow{}), &schema.LogRow{}, nil, func(r any) uint64 { return LogPattern(r.(*schema.LogRow)) })
	check("TraceRow", reflect.TypeOf(schema.TraceRow{}), &schema.TraceRow{}, nil, func(r any) uint64 { return TracePattern(r.(*schema.TraceRow)) })
}

func TestComputersHookIsOptional(t *testing.T) {
	if opts := LogsOptions([]schema.LogRow{{TimestampUnixNano: 1}}, nil); len(opts) != 0 {
		t.Fatalf("no computer installed, got %d options", len(opts))
	}
	called := 0
	SetComputers(Computers{Logs: func(rows []schema.LogRow, _ schema.SlotMapping) *Builder {
		called++
		b := NewBuilder()
		b.Add(1, "", "s", uint64(len(rows)), []string{"a"})
		return b
	}})
	t.Cleanup(func() { SetComputers(Computers{}) })
	if opts := LogsOptions([]schema.LogRow{{TimestampUnixNano: 1}}, nil); len(opts) != 1 || called != 1 {
		t.Fatalf("installed computer: %d options, %d calls", len(opts), called)
	}
	if opts := LogsOptions(nil, nil); len(opts) != 0 {
		t.Fatal("no rows must write no facet")
	}
	if opts := TracesOptions([]schema.TraceRow{{TimestampUnixNano: 1}}, nil); len(opts) != 0 {
		t.Fatal("no traces computer installed")
	}
}

// A builder that sees more distinct names or stream-days than its limits gives
// up: the object is reported over the cap and nothing more is kept in memory.
func TestBuilderGivesUpPastItsLimits(t *testing.T) {
	defer func(n, s int) { maxNames, maxStreamDays = n, s }(maxNames, maxStreamDays)
	maxNames, maxStreamDays = 50, 40
	b := NewBuilder()
	for i := 0; i < 100; i++ {
		b.Add(1, "", fmt.Sprintf("s%d", i), 1, []string{"a"})
	}
	if res := b.Finish(); !res.OverCap || res.Value != "" {
		t.Fatalf("stream-day limit: %+v", res)
	}
	b = NewBuilder()
	for i := 0; i < 100; i++ {
		b.Add(1, "", "s", 1, []string{fmt.Sprintf("name-%d", i)})
	}
	if res := b.Finish(); !res.OverCap {
		t.Fatalf("name limit: %+v", res)
	}
	b = NewBuilder()
	b.Add(1, "", "s", 1, []string{"a"})
	if res := b.Finish(); res.OverCap || res.Value == "" {
		t.Fatal("a small facet must not be over the cap")
	}
}
