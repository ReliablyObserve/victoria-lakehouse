package parquets3

import (
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go"
)

// Thrift compact-protocol type ids (the footer is FileMetaData encoded with
// the compact protocol; see the parquet-format spec).
const (
	tcStop      = 0
	tcBoolTrue  = 1
	tcBoolFalse = 2
	tcByte      = 3
	tcI16       = 4
	tcI32       = 5
	tcI64       = 6
	tcDouble    = 7
	tcBinary    = 8
	tcList      = 9
	tcSet       = 10
	tcMap       = 11
	tcStruct    = 12
)

const (
	// footerThriftMaxDepth bounds the Thrift nesting the guard (and with it
	// the decoder) will follow, counting lists, sets, maps and structs alike;
	// real footers nest about 6 levels deep. A value at nesting level 32 is
	// accepted and one at 33 is rejected.
	footerThriftMaxDepth = 32

	// footerThriftStructElemBytes is the fewest bytes a struct occupies as
	// a list element in any parquet.thrift list (SchemaElement, RowGroup,
	// ColumnChunk, KeyValue, ColumnOrder, PageLocation each carry a required
	// field or a union member: at least a field header, a value and the stop
	// byte). A scalar or string element is at least one byte.
	footerThriftStructElemBytes = 3

	// FileMetaData field ids (parquet.thrift) the guard reads.
	fileMetaDataSchema    = 2 // list<SchemaElement>
	fileMetaDataRowGroups = 4 // list<RowGroup>

	// SchemaElement field ids (parquet.thrift).
	schemaElementType        = 1 // i32, "not set if the current element is a non-leaf node"
	schemaElementNumChildren = 5 // i32
)

var errFooterThriftTruncated = errors.New("truncated thrift footer")

// checkFooterThriftBounds rejects a thrift-compact encoded parquet footer
// whose declared sizes cannot be true, before parquet-go allocates from them.
//
// Why: parquet-go v0.32.0 sizes its allocations from what the footer DECLARES,
// before it reads or validates the bytes behind it, in three places:
//
//  1. A Thrift list is allocated with reflect.MakeSlice(size, size) (thrift
//     decode.go) and the size may be any value up to MaxInt32. An 850-byte
//     footer declaring a ColumnChunk list of 2^31 entries (560 bytes each)
//     makes parquet.OpenFile allocate tens of GiB (measured: 26 GiB for one
//     850-byte input, 150 GiB for a 34 KB tail), which ends the process with
//     a Go runtime fatal error and no panic to recover.
//  2. columnLoader.reserve (column.go) allocates one chunk pointer per
//     (leaf column x row group) and one path string per schema element and
//     ancestor, so a small footer with many columns and many row groups, or a
//     deep schema, costs quadratically.
//  3. The Thrift decoder recurses with the nesting of the footer.
//
// Footers are untrusted bytes read from the bucket (MAIN RULE 7), so each is
// bounded here against the bytes that exist and against the file format:
//
//   - every list or set element occupies at least one distinct byte of the
//     footer (a struct element at least footerThriftStructElemBytes), so the
//     declared elements over the whole footer sum to at most its length;
//   - nesting of any kind (lists, sets, maps, structs) is at most
//     footerThriftMaxDepth;
//   - every row group carries one ColumnChunk per leaf column (parquet.thrift:
//     RowGroup.columns follows the schema's leaves; parquet-go rejects any
//     other count) and a ColumnChunk is at least 3 bytes (required file_offset
//     and the stop byte), so leaves x row groups x 3 is at most the footer
//     length. A leaf is a SchemaElement with field 1 (type) set;
//   - the schema nests at most parquet.MaxColumnDepth levels, the limit
//     parquet-go itself enforces, computed the way columnLoader.reserve walks
//     it (from num_children).
//
// Valid files are never rejected: each bound is a true lower bound of the
// encoding or a limit parquet-go enforces anyway. What stays on parquet-go:
// the path strings of a schema that is deep but within 255 levels (at most 256
// strings of 16 bytes for each schema element), see the PR measurements.
func checkFooterThriftBounds(meta []byte) error {
	g := footerGuard{buf: meta, budget: len(meta)}
	if err := g.structBody(0, g.fileMetaDataField); err != nil {
		return err
	}
	if g.leaves*g.rowGroups*footerThriftStructElemBytes > uint64(len(meta)) {
		return fmt.Errorf("thrift footer declares %d leaf columns in %d row groups, which %d bytes cannot hold", g.leaves, g.rowGroups, len(meta))
	}
	return nil
}

type footerGuard struct {
	buf    []byte
	pos    int
	budget int // bytes still available to declared collection elements

	leaves    uint64  // SchemaElements with the type field set
	rowGroups uint64  // elements of FileMetaData.row_groups
	open      []int32 // children still open at each schema ancestor, as in columnLoader.reserve
}

func (g *footerGuard) remaining() int { return len(g.buf) - g.pos }

func (g *footerGuard) byte() (byte, error) {
	if g.pos >= len(g.buf) {
		return 0, errFooterThriftTruncated
	}
	b := g.buf[g.pos]
	g.pos++
	return b, nil
}

func (g *footerGuard) uvarint() (uint64, error) {
	var x uint64
	for shift := uint(0); shift < 70; shift += 7 {
		b, err := g.byte()
		if err != nil {
			return 0, err
		}
		x |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return x, nil
		}
	}
	return 0, errors.New("thrift footer varint overflow")
}

// zigzag32 reads a compact-protocol i32.
func (g *footerGuard) zigzag32() (int32, error) {
	u, err := g.uvarint()
	if err != nil {
		return 0, err
	}
	return int32(u>>1) ^ -int32(u&1), nil
}

func (g *footerGuard) skip(n uint64) error {
	if n > uint64(g.remaining()) {
		return errFooterThriftTruncated
	}
	g.pos += int(n)
	return nil
}

// fieldHook is called for each struct field after its header was read, before
// its value is. It returns true when it consumed the value itself.
type fieldHook func(id int, typ byte, depth int) (bool, error)

// structBody walks one struct up to and including its stop byte. depth is the
// nesting level of the struct itself.
func (g *footerGuard) structBody(depth int, hook fieldHook) error {
	id := 0
	for {
		b, err := g.byte()
		if err != nil {
			return err
		}
		if b == tcStop {
			return nil
		}
		if b>>4 == 0 { // field id follows as a zigzag varint
			u, err := g.zigzag32()
			if err != nil {
				return err
			}
			id = int(u)
		} else {
			id += int(b >> 4)
		}
		typ := b & 0x0F
		if hook != nil {
			handled, err := hook(id, typ, depth+1)
			if err != nil {
				return err
			}
			if handled {
				continue
			}
		}
		if err := g.value(typ, depth+1); err != nil {
			return err
		}
	}
}

// value walks one field value of the given compact type at nesting level
// depth. Booleans inside a struct carry their value in the field type and have
// no payload.
func (g *footerGuard) value(typ byte, depth int) error {
	if depth > footerThriftMaxDepth {
		return errors.New("thrift footer nesting too deep")
	}
	switch typ {
	case tcBoolTrue, tcBoolFalse:
		return nil
	case tcByte:
		return g.skip(1)
	case tcI16, tcI32, tcI64:
		_, err := g.uvarint()
		return err
	case tcDouble:
		return g.skip(8)
	case tcBinary:
		n, err := g.uvarint()
		if err != nil {
			return err
		}
		return g.skip(n)
	case tcStruct:
		return g.structBody(depth, nil)
	case tcList, tcSet:
		_, err := g.list(depth, nil)
		return err
	case tcMap:
		return g.mapValue(depth)
	}
	return fmt.Errorf("thrift footer: unknown type %d", typ)
}

// element walks one collection element; unlike a struct field, a bool element
// is a byte of its own.
func (g *footerGuard) element(typ byte, depth int) error {
	if typ == tcBoolTrue || typ == tcBoolFalse {
		return g.skip(1)
	}
	return g.value(typ, depth)
}

// charge takes n elements of at least minBytes each out of the footer-wide
// budget, rejecting a declared size the remaining budget cannot hold. The
// division keeps the comparison free of integer overflow for any n.
func (g *footerGuard) charge(n uint64, minBytes int) error {
	if n > uint64(g.budget/minBytes) {
		return fmt.Errorf("thrift footer declares %d collection elements but only %d bytes of budget remain", n, g.budget)
	}
	g.budget -= int(n) * minBytes
	return nil
}

// list walks a list or set value at nesting level depth and returns its
// declared size. elem, when set, walks the elements of a struct list.
func (g *footerGuard) list(depth int, elem func(depth int) error) (uint64, error) {
	b, err := g.byte()
	if err != nil {
		return 0, err
	}
	n := uint64(b >> 4)
	if n == 0xF {
		if n, err = g.uvarint(); err != nil {
			return 0, err
		}
	}
	typ := b & 0x0F
	minBytes := 1
	if typ == tcStruct {
		minBytes = footerThriftStructElemBytes
	}
	if err := g.charge(n, minBytes); err != nil {
		return 0, err
	}
	for i := uint64(0); i < n; i++ {
		if elem != nil && typ == tcStruct {
			err = elem(depth + 1)
		} else {
			err = g.element(typ, depth+1)
		}
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

// mapValue walks a map. parquet.thrift has none and parquet-go allocates
// nothing for one it skips, so there is no allocation to bound: the walk itself
// fails on a size the bytes cannot hold.
func (g *footerGuard) mapValue(depth int) error {
	n, err := g.uvarint()
	if err != nil || n == 0 {
		return err
	}
	kv, err := g.byte()
	if err != nil {
		return err
	}
	for i := uint64(0); i < n; i++ {
		if err := g.element(kv>>4, depth+1); err != nil {
			return err
		}
		if err := g.element(kv&0x0F, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// fileMetaDataField reads the two FileMetaData lists whose size drives
// parquet-go's allocation beyond the list itself.
func (g *footerGuard) fileMetaDataField(id int, typ byte, depth int) (bool, error) {
	if typ != tcList {
		return false, nil
	}
	if depth > footerThriftMaxDepth {
		return false, errors.New("thrift footer nesting too deep")
	}
	switch id {
	case fileMetaDataSchema:
		_, err := g.list(depth, g.schemaElement)
		return true, err
	case fileMetaDataRowGroups:
		n, err := g.list(depth, nil)
		if n > g.rowGroups {
			g.rowGroups = n
		}
		return true, err
	}
	return false, nil
}

// schemaElement walks one SchemaElement (nesting level depth), counting leaves
// and tracking the schema's nesting the way parquet-go's columnLoader.reserve
// does, so a schema deeper than parquet-go accepts is rejected before it is
// allocated for.
func (g *footerGuard) schemaElement(depth int) error {
	var leaf bool
	var children int32
	err := g.structBody(depth, func(id int, typ byte, _ int) (bool, error) {
		switch {
		case id == schemaElementType && typ == tcI32:
			leaf = true
		case id == schemaElementNumChildren && typ == tcI32:
			n, err := g.zigzag32()
			children = n
			return true, err
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	if len(g.open) > parquet.MaxColumnDepth {
		return fmt.Errorf("thrift footer schema nests %d levels, more than the %d parquet-go supports", len(g.open), parquet.MaxColumnDepth)
	}
	if leaf {
		g.leaves++
	}
	if children > 0 {
		g.open = append(g.open, children)
		return nil
	}
	// A leaf closes itself and any ancestors whose last child it was.
	for len(g.open) > 0 {
		g.open[len(g.open)-1]--
		if g.open[len(g.open)-1] > 0 {
			break
		}
		g.open = g.open[:len(g.open)-1]
	}
	return nil
}
