package parquets3

import (
	"errors"
	"fmt"
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
	// footerThriftMaxDepth bounds the nesting the guard (and with it the
	// decoder) will follow; real footers nest about 6 levels deep.
	footerThriftMaxDepth = 32

	// footerThriftStructElemBytes is the fewest bytes a struct occupies as
	// a list element in any parquet.thrift list (SchemaElement, RowGroup,
	// ColumnChunk, KeyValue, ColumnOrder, PageLocation each carry a required
	// field or a union member: at least a field header, a value and the stop
	// byte). A scalar or string element is at least one byte.
	footerThriftStructElemBytes = 3
)

var errFooterThriftTruncated = errors.New("truncated thrift footer")

// checkFooterThriftBounds rejects a thrift-compact encoded parquet footer whose
// declared collection sizes cannot fit in the bytes that hold them.
//
// Why: parquet-go's thrift decoder allocates a list with
// reflect.MakeSlice(size, size) from the size the footer DECLARES, before it
// reads a single element, and the size may be any value up to MaxInt32. An
// 850-byte footer that declares a ColumnChunk list of 2^31 entries (560 bytes
// each) makes parquet.OpenFile allocate tens of GiB (measured: 26 GiB of
// allocation for one 850-byte input, 146 GiB for a 100 KB one), which kills
// the process on a normal host with no panic to recover. Footers are
// untrusted bytes read from the bucket (MAIN RULE 7), so the size is bounded
// here against the bytes that exist.
//
// Every list or set element occupies at least one distinct byte of the footer
// (a struct element, footerThriftStructElemBytes), so the sum of declared
// elements over the whole footer is at most its length. Allocation is
// therefore linear in the footer length (at most about 190x, the largest
// parquet-go element is a 560-byte ColumnChunk), not in an attacker-chosen
// 32-bit number. Valid files are never rejected: the budget is the true lower
// bound of the encoding.
func checkFooterThriftBounds(meta []byte) error {
	g := footerGuard{buf: meta, budget: len(meta)}
	return g.structBody(0)
}

type footerGuard struct {
	buf    []byte
	pos    int
	budget int // bytes still available to declared collection elements
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

func (g *footerGuard) skip(n uint64) error {
	if n > uint64(g.remaining()) {
		return errFooterThriftTruncated
	}
	g.pos += int(n)
	return nil
}

// structBody walks one struct up to and including its stop byte.
func (g *footerGuard) structBody(depth int) error {
	if depth > footerThriftMaxDepth {
		return errors.New("thrift footer nesting too deep")
	}
	for {
		b, err := g.byte()
		if err != nil {
			return err
		}
		if b == tcStop {
			return nil
		}
		if b>>4 == 0 { // field id follows as a zigzag varint
			if _, err := g.uvarint(); err != nil {
				return err
			}
		}
		if err := g.value(b&0x0F, depth); err != nil {
			return err
		}
	}
}

// value walks one field value of the given compact type. Booleans inside a
// struct carry their value in the field type and have no payload.
func (g *footerGuard) value(typ byte, depth int) error {
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
		return g.structBody(depth + 1)
	case tcList, tcSet:
		return g.list(depth)
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

func (g *footerGuard) list(depth int) error {
	b, err := g.byte()
	if err != nil {
		return err
	}
	n := uint64(b >> 4)
	if n == 0xF {
		if n, err = g.uvarint(); err != nil {
			return err
		}
	}
	typ := b & 0x0F
	minBytes := 1
	if typ == tcStruct {
		minBytes = footerThriftStructElemBytes
	}
	if err := g.charge(n, minBytes); err != nil {
		return err
	}
	for i := uint64(0); i < n; i++ {
		if err := g.element(typ, depth+1); err != nil {
			return err
		}
	}
	return nil
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
