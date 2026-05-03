package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

const (
	ProtocolVersion = 4

	columnTypeConst   = byte(0)
	columnTypeRegular = byte(1)
)

func MarshalDataBlock(db *storage.DataBlock) []byte {
	size := 8
	for _, col := range db.Columns {
		size += 4 + len(col.Name) + 1
		if allSame(col.Values) {
			if len(col.Values) > 0 {
				size += 4 + len(col.Values[0])
			} else {
				size += 4
			}
		} else {
			for _, v := range col.Values {
				size += 4 + len(v)
			}
		}
	}

	buf := make([]byte, 0, size)
	buf = appendUint32(buf, uint32(db.RowsCount))
	buf = appendUint32(buf, uint32(len(db.Columns)))

	for _, col := range db.Columns {
		buf = appendString(buf, col.Name)
		if allSame(col.Values) {
			buf = append(buf, columnTypeConst)
			if len(col.Values) > 0 {
				buf = appendString(buf, col.Values[0])
			} else {
				buf = appendString(buf, "")
			}
		} else {
			buf = append(buf, columnTypeRegular)
			for _, v := range col.Values {
				buf = appendString(buf, v)
			}
		}
	}

	return buf
}

func UnmarshalDataBlock(data []byte) (*storage.DataBlock, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	rowsCount := int(binary.BigEndian.Uint32(data[0:4]))
	colsCount := int(binary.BigEndian.Uint32(data[4:8]))
	pos := 8

	if colsCount > 10000 || rowsCount > 10000000 {
		return nil, fmt.Errorf("suspicious block size: rows=%d cols=%d", rowsCount, colsCount)
	}

	columns := make([]storage.BlockColumn, colsCount)
	for i := 0; i < colsCount; i++ {
		name, n, err := readString(data, pos)
		if err != nil {
			return nil, fmt.Errorf("read column %d name: %w", i, err)
		}
		pos += n

		if pos >= len(data) {
			return nil, fmt.Errorf("truncated at column %d type", i)
		}
		colType := data[pos]
		pos++

		var values []string
		switch colType {
		case columnTypeConst:
			val, n, err := readString(data, pos)
			if err != nil {
				return nil, fmt.Errorf("read column %d const value: %w", i, err)
			}
			pos += n
			values = make([]string, rowsCount)
			for j := range values {
				values[j] = val
			}
		case columnTypeRegular:
			values = make([]string, rowsCount)
			for j := 0; j < rowsCount; j++ {
				val, n, err := readString(data, pos)
				if err != nil {
					return nil, fmt.Errorf("read column %d row %d: %w", i, j, err)
				}
				pos += n
				values[j] = val
			}
		default:
			return nil, fmt.Errorf("unknown column type %d for column %d", colType, i)
		}

		columns[i] = storage.BlockColumn{Name: name, Values: values}
	}

	return &storage.DataBlock{RowsCount: rowsCount, Columns: columns}, nil
}

func MarshalValueWithHits(vals []storage.ValueWithHits) []byte {
	size := 4
	for _, v := range vals {
		size += 4 + len(v.Value) + 8
	}
	buf := make([]byte, 0, size)
	buf = appendUint32(buf, uint32(len(vals)))
	for _, v := range vals {
		buf = appendString(buf, v.Value)
		buf = appendUint64(buf, v.Hits)
	}
	return buf
}

func UnmarshalValueWithHits(data []byte) ([]storage.ValueWithHits, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	count := int(binary.BigEndian.Uint32(data[0:4]))
	if count > 10000000 {
		return nil, fmt.Errorf("suspicious count: %d", count)
	}

	pos := 4
	vals := make([]storage.ValueWithHits, count)
	for i := 0; i < count; i++ {
		val, n, err := readString(data, pos)
		if err != nil {
			return nil, fmt.Errorf("read value %d: %w", i, err)
		}
		pos += n
		if pos+8 > len(data) {
			return nil, fmt.Errorf("truncated at value %d hits", i)
		}
		hits := binary.BigEndian.Uint64(data[pos : pos+8])
		pos += 8
		vals[i] = storage.ValueWithHits{Value: val, Hits: hits}
	}

	return vals, nil
}

func MarshalTenantIDs(ids []storage.TenantID) []byte {
	buf := make([]byte, 0, 4+len(ids)*8)
	buf = appendUint32(buf, uint32(len(ids)))
	for _, id := range ids {
		buf = appendUint32(buf, id.AccountID)
		buf = appendUint32(buf, id.ProjectID)
	}
	return buf
}

func UnmarshalTenantIDs(data []byte) ([]storage.TenantID, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	count := int(binary.BigEndian.Uint32(data[0:4]))
	if count > 10000000 {
		return nil, fmt.Errorf("suspicious count: %d", count)
	}

	pos := 4
	ids := make([]storage.TenantID, count)
	for i := 0; i < count; i++ {
		if pos+8 > len(data) {
			return nil, fmt.Errorf("truncated at tenant %d", i)
		}
		ids[i] = storage.TenantID{
			AccountID: binary.BigEndian.Uint32(data[pos : pos+4]),
			ProjectID: binary.BigEndian.Uint32(data[pos+4 : pos+8]),
		}
		pos += 8
	}
	return ids, nil
}

func WriteDataBlockStream(w io.Writer, db *storage.DataBlock) error {
	data := MarshalDataBlock(db)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func ReadDataBlockStream(r io.Reader) (*storage.DataBlock, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf)
	if n > math.MaxInt32 {
		return nil, fmt.Errorf("block too large: %d bytes", n)
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return UnmarshalDataBlock(data)
}

func allSame(vals []string) bool {
	if len(vals) <= 1 {
		return true
	}
	first := vals[0]
	for _, v := range vals[1:] {
		if v != first {
			return false
		}
	}
	return true
}

func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint64(buf []byte, v uint64) []byte {
	return append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendString(buf []byte, s string) []byte {
	buf = appendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

func readString(data []byte, pos int) (string, int, error) {
	if pos+4 > len(data) {
		return "", 0, fmt.Errorf("truncated string length at pos %d", pos)
	}
	n := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	if n > 10*1024*1024 {
		return "", 0, fmt.Errorf("string too large: %d bytes", n)
	}
	if pos+4+n > len(data) {
		return "", 0, fmt.Errorf("truncated string data at pos %d, need %d bytes", pos+4, n)
	}
	return string(data[pos+4 : pos+4+n]), 4 + n, nil
}
