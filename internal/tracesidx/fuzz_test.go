package tracesidx

import (
	"context"
	"math/rand"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func allocated() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.TotalAlloc
}

// A slice that passes ValidateThrift cannot make the library allocate more than a small
// multiple of its length when it decodes it as a column or offset index: the declared
// list sizes are bounded by the bytes that follow. A slice that does not pass is never
// decoded.
func FuzzValidateThrift(f *testing.F) {
	rows := randRows(rand.New(rand.NewSource(1)), 3000)
	data, _, _ := Marshal(rows, 3)
	loc := NewLocator(1)
	e, _, _ := loc.open(context.Background(), newCountSource("seed", data))
	for _, g := range e.meta.RowGroups {
		c := g.Columns[e.cols[IDField]]
		f.Add(data[c.ColumnIndexOffset : c.ColumnIndexOffset+int64(c.ColumnIndexLength)])
		f.Add(data[c.OffsetIndexOffset : c.OffsetIndexOffset+int64(c.OffsetIndexLength)])
	}
	f.Add([]byte{0x19, 0xf6, 0xff, 0xff, 0xff, 0x0f}) // a list that declares 2^31 elements and has none
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 1<<16 || ValidateThrift(b) != nil {
			return
		}
		before := allocated()
		var ci format.ColumnIndex
		var oi format.OffsetIndex
		_ = thrift.Unmarshal(&thrift.CompactProtocol{}, b, &ci)
		_ = thrift.Unmarshal(&thrift.CompactProtocol{}, b, &oi)
		if d := allocated() - before; d > uint64(64*len(b))+1<<20 {
			t.Fatalf("a validated %d-byte slice made the decoder allocate %d bytes", len(b), d)
		}
	})
}

// A hostile object (a valid one with bytes changed) makes a lookup return an error or
// rows, never panic, never read outside the object and never allocate without bound.
func FuzzLocatorHostileObject(f *testing.F) {
	rows := randRows(rand.New(rand.NewSource(2)), 2000)
	data, _, _ := Marshal(append([]Row(nil), rows...), 3)
	f.Add(data, rows[10].TraceID, uint16(0), byte(0))
	f.Add(data, rows[10].TraceID, uint16(len(data)-20), byte(0xff)) // inside the footer
	f.Add(data, rows[10].TraceID, uint16(len(data)/2), byte(1))
	f.Fuzz(func(t *testing.T, base []byte, id string, pos uint16, val byte) {
		if len(base) < 64 || len(base) > 1<<20 {
			return
		}
		obj := append([]byte(nil), base...)
		obj[int(pos)%len(obj)] ^= val
		obj[len(obj)-1-int(pos)%32%len(obj)] ^= val // and near the tail, where the footer is
		src := newCountSource("hostile", obj)
		before := allocated()
		got, st, err := NewLocator(1).Lookup(context.Background(), src, id, WantAll)
		if d := allocated() - before; d > 256<<20 {
			t.Fatalf("a lookup allocated %d bytes", d)
		}
		if err == nil {
			for _, r := range got {
				if r.TraceID != id {
					t.Fatalf("returned a row of another id: %q", r.TraceID)
				}
			}
		}
		if st.Bytes > 64<<20 {
			t.Fatalf("a lookup read %d bytes", st.Bytes)
		}
	})
}
