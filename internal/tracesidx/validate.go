package tracesidx

import (
	"errors"
	"fmt"
)

// ValidateThrift checks that b is one well-formed Thrift compact-protocol struct
// (a Parquet ColumnIndex or OffsetIndex slice) whose every declared length fits in
// the bytes that follow it. The parquet library sizes a slice from the count a list
// header declares, so a few hostile bytes could otherwise ask it for gigabytes; after
// this check the largest allocation is proportional to len(b). It allocates nothing
// and takes time linear in len(b).
func ValidateThrift(b []byte) error {
	v := validator{b: b}
	if err := v.structure(0); err != nil {
		return err
	}
	if v.pos != len(b) {
		return fmt.Errorf("trailing %d bytes after the struct", len(b)-v.pos)
	}
	return nil
}

const maxThriftDepth = 8

var errShort = errors.New("truncated thrift data")

type validator struct {
	b   []byte
	pos int
}

func (v *validator) byte() (byte, error) {
	if v.pos >= len(v.b) {
		return 0, errShort
	}
	c := v.b[v.pos]
	v.pos++
	return c, nil
}

func (v *validator) varint() (uint64, error) {
	var x uint64
	for shift := uint(0); shift < 70; shift += 7 {
		c, err := v.byte()
		if err != nil {
			return 0, err
		}
		x |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return x, nil
		}
	}
	return 0, errors.New("varint too long")
}

func (v *validator) skipBytes(n uint64) error {
	if n > uint64(len(v.b)-v.pos) {
		return errShort
	}
	v.pos += int(n)
	return nil
}

func (v *validator) structure(depth int) error {
	if depth > maxThriftDepth {
		return errors.New("thrift nesting too deep")
	}
	for {
		h, err := v.byte()
		if err != nil {
			return err
		}
		if h == 0 {
			return nil
		}
		typ := h & 0x0f
		if h>>4 == 0 { // the field id follows as a zigzag varint
			if _, err := v.varint(); err != nil {
				return err
			}
		}
		if err := v.value(typ, depth); err != nil {
			return err
		}
	}
}

// value skips one value of a compact-protocol type; typ 1 and 2 (booleans) carry
// their value in the field header and take no bytes.
func (v *validator) value(typ byte, depth int) error {
	switch typ {
	case 1, 2:
		return nil
	case 3:
		return v.skipBytes(1)
	case 4, 5, 6:
		_, err := v.varint()
		return err
	case 7:
		return v.skipBytes(8)
	case 8:
		n, err := v.varint()
		if err != nil {
			return err
		}
		return v.skipBytes(n)
	case 9, 10:
		h, err := v.byte()
		if err != nil {
			return err
		}
		n := uint64(h >> 4)
		if n == 15 {
			if n, err = v.varint(); err != nil {
				return err
			}
		}
		et := h & 0x0f
		// Every element takes at least one byte (a boolean element is one byte),
		// so a count larger than the bytes left is a lie.
		if n > uint64(len(v.b)-v.pos) {
			return errShort
		}
		for i := uint64(0); i < n; i++ {
			if et == 1 || et == 2 {
				if err := v.skipBytes(1); err != nil {
					return err
				}
				continue
			}
			if err := v.value(et, depth+1); err != nil {
				return err
			}
		}
		return nil
	case 11:
		n, err := v.varint()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		if n > uint64(len(v.b)-v.pos) {
			return errShort
		}
		kv, err := v.byte()
		if err != nil {
			return err
		}
		for i := uint64(0); i < n; i++ {
			for _, t := range [2]byte{kv >> 4, kv & 0x0f} {
				if t == 1 || t == 2 {
					if err := v.skipBytes(1); err != nil {
						return err
					}
					continue
				}
				if err := v.value(t, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	case 12:
		return v.structure(depth + 1)
	}
	return fmt.Errorf("unknown thrift type %d", typ)
}
