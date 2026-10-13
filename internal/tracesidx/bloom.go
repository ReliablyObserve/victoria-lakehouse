package tracesidx

import (
	"encoding/binary"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/bloom"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// addFilters adds the standard split-block bloom filter of trace_id_idx to every row
// group of an object the library wrote without one, and returns the new object.
//
// Each filter holds the row group's DISTINCT ids at BloomBitsPerValue bits each, in a
// power-of-two number of 32-byte blocks (FilterBytes), hashed with xxHash64 of the id
// bytes as the Parquet specification says. The filters are appended after the page
// indexes, each preceded by its Thrift header (numBytes, SPLIT_BLOCK, XXHASH,
// UNCOMPRESSED), and the footer is written again with their offset and length in the
// column metadata. Data pages and page indexes are untouched, so their offsets stay.
// Any reader that understands Parquet bloom filters (DuckDB, pyarrow, ClickHouse,
// Spark, Trino, Polars, DataFusion) reads them.
//
// rows are the sorted rows the object was written from, cut into groups of groupRows.
func addFilters(data []byte, rows []Row, groupRows int) ([]byte, error) {
	if len(data) < 12 || string(data[len(data)-4:]) != "PAR1" {
		return nil, fmt.Errorf("tracesidx: not a parquet object")
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8:]))
	footerStart := len(data) - 8 - footerLen
	if footerLen <= 0 || footerStart < 4 {
		return nil, fmt.Errorf("tracesidx: bad footer length %d", footerLen)
	}
	var meta format.FileMetaData
	if err := thrift.Unmarshal(&thrift.CompactProtocol{}, data[footerStart:footerStart+footerLen], &meta); err != nil {
		return nil, fmt.Errorf("tracesidx: decode footer: %w", err)
	}
	idCol := -1
	for i, c := range schemaColumns() {
		if c == IDField {
			idCol = i
		}
	}
	if idCol < 0 || len(meta.RowGroups) != (len(rows)+groupRows-1)/groupRows {
		return nil, fmt.Errorf("tracesidx: %d row groups for %d rows", len(meta.RowGroups), len(rows))
	}
	out := make([]byte, 0, footerStart+len(meta.RowGroups)*(2*FilterBytes(int64(groupRows))+64)+footerLen+16)
	out = append(out, data[:footerStart]...)
	for g := range meta.RowGroups {
		lo, hi := g*groupRows, min((g+1)*groupRows, len(rows))
		size := FilterBytes(distinctIDs(rows[lo:hi]))
		filter := bloom.MakeSplitBlockFilter(make([]byte, size))
		for i := lo; i < hi; i++ {
			filter.Insert(bloom.XXH64{}.Sum64([]byte(rows[i].TraceID)))
		}
		header := format.BloomFilterHeader{NumBytes: int32(size)}
		header.Algorithm.Value = &format.SplitBlockAlgorithm{}
		header.Hash.Value = &format.XxHash{}
		header.Compression.Value = &format.BloomFilterUncompressed{}
		hdr, err := thrift.Marshal(&thrift.CompactProtocol{}, &header)
		if err != nil {
			return nil, fmt.Errorf("tracesidx: encode bloom header: %w", err)
		}
		md := &meta.RowGroups[g].Columns[idCol].MetaData
		md.BloomFilterOffset = int64(len(out))
		md.BloomFilterLength = int32(len(hdr) + size)
		out = append(out, hdr...)
		out = append(out, filter.Bytes()...)
	}
	footer, err := thrift.Marshal(&thrift.CompactProtocol{}, &meta)
	if err != nil {
		return nil, fmt.Errorf("tracesidx: encode footer: %w", err)
	}
	out = append(out, footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...), nil
}

// schemaColumns returns the leaf column names of the table, in file order.
func schemaColumns() []string {
	var out []string
	for _, c := range parquet.SchemaOf(Row{}).Columns() {
		out = append(out, c[0])
	}
	return out
}
