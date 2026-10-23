package parquets3

// The columnar row-group reader returns the error of a page it cannot read
// instead of handing back the values read so far (issue #503: a damaged or
// unreadable page used to leave a column short or empty, and the read answered 200
// with rows missing). Mirrored in lakehouse-traces/internal/storage/parquets3/row_read_columnar_errors_test.go.

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func columnarErrFixture(t *testing.T) (data []byte, reg *schema.Registry) {
	t.Helper()
	base := time.Date(2026, 5, 10, 14, 30, 0, 0, time.UTC)
	rows := make([]schema.LogRow, 300)
	for i := range rows {
		rows[i] = schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              fmt.Sprintf("row %d", i),
			ServiceName:       "svc",
			LogAttributes:     map[string]string{"k": fmt.Sprintf("v%d", i)},
		}
	}
	res, err := writeLogsParquet(rows, 100, 3)
	if err != nil {
		t.Fatal(err)
	}
	return res.Data, schema.NewRegistry(schema.LogsProfile)
}

func TestColumnarReader_ReportsAPageItCannotRead(t *testing.T) {
	data, reg := columnarErrFixture(t)
	read := func(d []byte, cols ...string) (rows int, err error) {
		f, oerr := parquet.OpenFile(bytes.NewReader(d), int64(len(d)))
		if oerr != nil {
			t.Fatal(oerr)
		}
		want := map[string]bool{}
		for _, c := range cols {
			want[c] = true
		}
		for _, rg := range f.RowGroups() {
			db, rerr := readRowGroupColumnarKeys(f, rg, want, reg, 0, math.MaxInt64, nil, nil, nil)
			if rerr != nil {
				return rows, rerr
			}
			if db != nil {
				rows += db.RowsCount()
			}
		}
		return rows, nil
	}
	cases := []struct {
		name    string
		damaged string   // the column whose pages are damaged
		read    []string // the columns the read asks for
	}{
		{"timestamp column (readInt64Column)", "timestamp_unix_nano", []string{"timestamp_unix_nano"}},
		{"scalar column (readScalarColumnFormatted)", "body", []string{"timestamp_unix_nano", "body"}},
		{"map column (readMapColumnToBlockColsKeys)", "log.attributes.key_value.key", []string{"timestamp_unix_nano", "log.attributes"}},
		{"map value column (readMapColumnToBlockColsKeys)", "log.attributes.key_value.value", []string{"timestamp_unix_nano", "log.attributes"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := read(data, tc.read...)
			if err != nil || rows != 300 {
				t.Fatalf("control: %d rows, err %v; want 300 rows", rows, err)
			}
			if rows, err := read(corruptPages(t, data, 1, tc.damaged), tc.read...); err == nil {
				t.Fatalf("a damaged %s answered %d of 300 rows without error", tc.damaged, rows)
			}
		})
	}
}

const (
	scalarColumn = "body"
	mapColumn    = "log.attributes"
)

// --- an I/O error of the chunk reader, at the page and at the value level ----

var errChunkIO = errors.New("injected chunk read error")

// failingChunk is a column chunk whose pages fail like a read of the object
// that fails half-way (an S3 error after the retries): at the page read, or in
// the values of a page that was read.
type failingChunk struct {
	parquet.ColumnChunk
	atValues bool
}

func (c failingChunk) Pages() parquet.Pages {
	return failingPages{Pages: c.ColumnChunk.Pages(), atValues: c.atValues}
}

type failingPages struct {
	parquet.Pages
	atValues bool
}

func (p failingPages) ReadPage() (parquet.Page, error) {
	pg, err := p.Pages.ReadPage()
	if err != nil {
		return nil, err
	}
	if !p.atValues {
		return nil, errChunkIO
	}
	return failingPage{Page: pg}, nil
}

type failingPage struct{ parquet.Page }

func (p failingPage) Values() parquet.ValueReader { return failingValues{p.Page.Values()} }

type failingValues struct{ parquet.ValueReader }

func (v failingValues) ReadValues(buf []parquet.Value) (int, error) {
	n, _ := v.ValueReader.ReadValues(buf)
	return n, errChunkIO
}

func TestColumnarReaders_ReportAnIOError(t *testing.T) {
	data, reg := columnarErrFixture(t)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	rg := f.RowGroups()[0]
	n := int(rg.NumRows())
	chunk := func(name string) parquet.ColumnChunk {
		for i, path := range f.Schema().Columns() {
			if strings.Join(path, ".") == name {
				return rg.ColumnChunks()[i]
			}
		}
		t.Fatalf("no column %s", name)
		return nil
	}
	mask := make([]bool, n)
	for i := range mask {
		mask[i] = true
	}
	readers := map[string]func(wrap func(parquet.ColumnChunk) parquet.ColumnChunk) error{
		"readInt64Column": func(wrap func(parquet.ColumnChunk) parquet.ColumnChunk) error {
			_, err := readInt64Column(wrap(chunk("timestamp_unix_nano")), n)
			return err
		},
		"readScalarColumnFormatted": func(wrap func(parquet.ColumnChunk) parquet.ColumnChunk) error {
			_, err := readScalarColumnFormatted(wrap(chunk(scalarColumn)), n, mask, n, scalarColumn, reg)
			return err
		},
		"readMapColumnToBlockColsKeys/keys": func(wrap func(parquet.ColumnChunk) parquet.ColumnChunk) error {
			_, err := readMapColumnToBlockColsKeys(wrap(chunk(mapColumn+".key_value.key")), chunk(mapColumn+".key_value.value"), n, mask, n, mapColumn, nil, nil, reg)
			return err
		},
		"readMapColumnToBlockColsKeys/values": func(wrap func(parquet.ColumnChunk) parquet.ColumnChunk) error {
			_, err := readMapColumnToBlockColsKeys(chunk(mapColumn+".key_value.key"), wrap(chunk(mapColumn+".key_value.value")), n, mask, n, mapColumn, nil, nil, reg)
			return err
		},
	}
	same := func(c parquet.ColumnChunk) parquet.ColumnChunk { return c }
	for name, read := range readers {
		t.Run(name, func(t *testing.T) {
			if err := read(same); err != nil {
				t.Fatalf("control: %v", err)
			}
			for _, atValues := range []bool{false, true} {
				err := read(func(c parquet.ColumnChunk) parquet.ColumnChunk {
					return failingChunk{ColumnChunk: c, atValues: atValues}
				})
				if !errors.Is(err, errChunkIO) {
					t.Fatalf("atValues=%v: err = %v, want the chunk's I/O error", atValues, err)
				}
			}
		})
	}
}
