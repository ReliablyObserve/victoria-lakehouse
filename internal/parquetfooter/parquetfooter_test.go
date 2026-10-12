package parquetfooter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type row struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
	Body string `parquet:"body"`
}

// object writes a Parquet object with a page index (parquet-go's default), several
// row groups, and a footer key-value entry of kvBytes bytes (to make the footer
// as big as a test needs).
func object(t testing.TB, rows, rowGroup, kvBytes int) []byte {
	t.Helper()
	var buf bytes.Buffer
	opts := []parquet.WriterOption{parquet.MaxRowsPerRowGroup(int64(rowGroup))}
	if kvBytes > 0 {
		rng := rand.New(rand.NewSource(1))
		v := make([]byte, kvBytes)
		for i := range v { // incompressible-looking but ASCII
			v[i] = byte('a' + rng.Intn(26))
		}
		opts = append(opts, parquet.KeyValueMetadata("test.padding", string(v)))
	}
	w := parquet.NewGenericWriter[row](&buf, opts...)
	data := make([]row, rows)
	for i := range data {
		data[i] = row{ID: int64(i), Name: fmt.Sprintf("n%d", i%17), Body: strings.Repeat("x", 30) + fmt.Sprint(i)}
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// serve is a Downloader over obj that records every request.
type served struct {
	obj   []byte
	calls [][2]int64 // offset, length
	fail  map[int]error
	short map[int]int // call index -> bytes to drop from the answer
}

func (s *served) dl(_ context.Context, kind, key string, off, n int64) ([]byte, error) {
	i := len(s.calls)
	s.calls = append(s.calls, [2]int64{off, n})
	if err := s.fail[i]; err != nil {
		return nil, err
	}
	if kind != "footer" {
		panic("unexpected kind " + kind)
	}
	if off < 0 || n < 0 || off+n > int64(len(s.obj)) {
		return nil, fmt.Errorf("range [%d,+%d) outside object of %d", off, n, len(s.obj))
	}
	out := bytes.Clone(s.obj[off : off+n])
	if d := s.short[i]; d > 0 {
		out = out[:len(out)-d]
	}
	return out, nil
}

func footerLenOf(t testing.TB, obj []byte) int64 {
	t.Helper()
	n, err := Length(obj[len(obj)-TrailerLen:])
	if err != nil {
		t.Fatal(err)
	}
	return int64(n)
}

func TestInspect_TailIsPageIndexStripeThroughEOF(t *testing.T) {
	obj := object(t, 5000, 1000, 0)
	info := Inspect(obj)
	if info.Tail <= 0 {
		t.Fatalf("Tail = %d, want > 0", info.Tail)
	}
	f, err := parquet.OpenFile(bytes.NewReader(obj), int64(len(obj)))
	if err != nil {
		t.Fatal(err)
	}
	// Independent computation: the smallest page-index offset of any column chunk.
	minIdx := int64(len(obj))
	for _, rg := range f.Metadata().RowGroups {
		for _, c := range rg.Columns {
			for _, o := range []int64{c.ColumnIndexOffset, c.OffsetIndexOffset} {
				if o > 0 && o < minIdx {
					minIdx = o
				}
			}
		}
	}
	if want := int64(len(obj)) - minIdx; int64(info.Tail) != want {
		t.Fatalf("Tail = %d, want %d (object %d, first page-index section at %d)", info.Tail, want, len(obj), minIdx)
	}
	if footer := footerLenOf(t, obj) + TrailerLen; int64(info.Tail) <= footer {
		t.Fatalf("Tail %d does not include the page-index stripe in front of the %d-byte footer", info.Tail, footer)
	}
	if len(info.ColumnBytes) != 3 || info.ColumnBytes["id"] <= 0 {
		t.Fatalf("ColumnBytes = %v", info.ColumnBytes)
	}
}

func TestTailLen_Shapes(t *testing.T) {
	rg := func(offsets ...int64) *format.FileMetaData {
		md := &format.FileMetaData{}
		var cols []format.ColumnChunk
		for i := 0; i+1 < len(offsets); i += 2 {
			cols = append(cols, format.ColumnChunk{ColumnIndexOffset: offsets[i], OffsetIndexOffset: offsets[i+1]})
		}
		md.RowGroups = []format.RowGroup{{Columns: cols}}
		return md
	}
	const size, footer = int64(10_000), 700
	cases := []struct {
		name string
		md   *format.FileMetaData
		want int32
	}{
		{"no page index: the footer and trailer", rg(0, 0), footer + TrailerLen},
		{"no row groups", &format.FileMetaData{}, footer + TrailerLen},
		{"stripe before the footer", rg(9000, 9100, 9050, 9200), int32(size - 9000)},
		{"offset index first", rg(9100, 9000), int32(size - 9000)},
		{"an offset inside the footer is ignored", rg(size-100, size-50), footer + TrailerLen},
		{"inconsistent: footer longer than the object", rg(0, 0), 0},
	}
	for _, tc := range cases {
		fl := footer
		if strings.HasPrefix(tc.name, "inconsistent") {
			fl = int(size)
		}
		if got := tailLen(size, fl, tc.md); got != tc.want {
			t.Errorf("%s: tailLen = %d, want %d", tc.name, got, tc.want)
		}
	}
	if StripeStart(nil) != -1 {
		t.Error("StripeStart(nil) must be -1")
	}
	if got := tailLen(MaxHint*2, 1000, rg(1, 2)); got != 0 {
		t.Errorf("a tail past MaxHint must not be recorded, got %d", got)
	}
}

func TestInspect_UnreadableIsZero(t *testing.T) {
	for _, in := range [][]byte{nil, []byte("junk"), []byte("not a parquet file at all"), append(bytes.Repeat([]byte{0}, 20), []byte("PAR1")...)} {
		if got := Inspect(in); got.Tail != 0 || got.BloomBytes != 0 || got.ColumnBytes != nil {
			t.Errorf("Inspect(%q) = %+v, want the zero value", in, got)
		}
	}
}

func TestLength(t *testing.T) {
	if _, err := Length([]byte{1, 2, 3}); err == nil {
		t.Error("a short buffer must fail")
	}
	if _, err := Length([]byte{1, 0, 0, 0, 'N', 'O', 'P', 'E'}); err == nil {
		t.Error("a bad magic must fail")
	}
	if n, err := Length([]byte{9, 9, 9, 9, 1, 0, 0, 0, 'P', 'A', 'R', '1'}); err != nil || n != 1 {
		t.Errorf("Length = %d, %v; want 1 (only the last 8 bytes count)", n, err)
	}
}

func gets() uint64 { return metrics.S3GetsByPhase.Get("footer") }

func TestRead_ExactHintIsOneRequestOfExactlyThatLength(t *testing.T) {
	obj := object(t, 5000, 1000, 40_000)
	info := Inspect(obj)
	s := &served{obj: obj}
	before := gets()
	tail, err := Read(context.Background(), s.dl, "k", int64(len(obj)), 64<<10, int64(info.Tail), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.calls) != 1 || s.calls[0] != [2]int64{int64(len(obj)) - int64(info.Tail), int64(info.Tail)} {
		t.Fatalf("requests = %v, want one of the last %d bytes", s.calls, info.Tail)
	}
	if gets()-before != 1 {
		t.Fatalf("counted %d footer GETs, want 1", gets()-before)
	}
	if !bytes.Equal(tail.Data, obj[tail.Off:]) || tail.Off != int64(len(obj))-int64(info.Tail) {
		t.Fatalf("tail [%d,+%d) is not the end of the object", tail.Off, len(tail.Data))
	}
}

func TestRead_NoHint(t *testing.T) {
	obj := object(t, 5000, 1000, 40_000) // footer ~40 KB
	size := int64(len(obj))
	foot := footerLenOf(t, obj) + TrailerLen
	t.Run("footer inside the window is one request", func(t *testing.T) {
		s := &served{obj: obj}
		tail, err := Read(context.Background(), s.dl, "k", size, 128<<10, 0, 0)
		if err != nil || len(s.calls) != 1 || !bytes.Equal(tail.Data, obj[tail.Off:]) || tail.Off+int64(len(tail.Data)) != size {
			t.Fatalf("calls=%v err=%v", s.calls, err)
		}
	})
	t.Run("footer past the window costs a second request for only the missing head", func(t *testing.T) {
		window := foot / 2
		s := &served{obj: obj}
		tail, err := Read(context.Background(), s.dl, "k", size, window, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.calls) != 2 {
			t.Fatalf("requests = %v, want 2", s.calls)
		}
		first, second := s.calls[0], s.calls[1]
		if first != [2]int64{size - window, window} {
			t.Fatalf("first request %v", first)
		}
		// The second read ends where the first begins: the bytes already read are not read again.
		if second[0]+second[1] != first[0] {
			t.Fatalf("second request %v overlaps or leaves a gap before the first %v", second, first)
		}
		if second[0] != size-foot-LookBehind {
			t.Fatalf("second request starts at %d, want footer start minus the look-behind (%d)", second[0], size-foot-LookBehind)
		}
		if !bytes.Equal(tail.Data, obj[tail.Off:]) || tail.Off != second[0] {
			t.Fatal("joined tail differs from the object")
		}
	})
	t.Run("the look-behind is clamped at the start of the object", func(t *testing.T) {
		small := object(t, 50, 50, 0)
		s := &served{obj: small}
		if _, err := Read(context.Background(), s.dl, "k", int64(len(small)), TrailerLen, 0, 0); err != nil {
			t.Fatal(err)
		}
		if len(s.calls) != 2 || s.calls[1][0] != 0 {
			t.Fatalf("requests %v, want the second to start at 0", s.calls)
		}
	})
}

func TestRead_HintValidation(t *testing.T) {
	obj := object(t, 5000, 1000, 40_000)
	size := int64(len(obj))
	exact := int64(Inspect(obj).Tail)
	foot := footerLenOf(t, obj) + TrailerLen
	window := int64(64 << 10)
	cases := []struct {
		name      string
		hint      int64
		wantFirst int64 // length of the first request
		wantCalls int
	}{
		{"exact", exact, exact, 1},
		{"longer than needed is still correct", exact + 5000, exact + 5000, 1},
		{"the whole object", size, size, 1},
		{"zero is the window", 0, window, 1},
		{"negative is the window", -5, window, 1},
		{"below the trailer is the window", TrailerLen - 1, window, 1},
		{"larger than the object is the window", size + 1, window, 1},
		{"larger than the cap is the window", MaxHint + 1, window, 1},
		{"stale: shorter than the footer", foot - 1000, foot - 1000, 2},
		{"stale: the trailer alone", TrailerLen, TrailerLen, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &served{obj: obj}
			tail, err := Read(context.Background(), s.dl, "k", size, window, tc.hint, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(s.calls) != tc.wantCalls || s.calls[0][1] != tc.wantFirst {
				t.Fatalf("requests %v, want %d with a first of %d bytes", s.calls, tc.wantCalls, tc.wantFirst)
			}
			if !bytes.Equal(tail.Data, obj[tail.Off:]) || int64(len(tail.Data)) < foot {
				t.Fatalf("tail [%d,+%d) is not the end of the object holding the %d-byte footer", tail.Off, len(tail.Data), foot)
			}
			if f, err := parquet.OpenFile(bytes.NewReader(obj), size); err != nil || f == nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRead_Errors(t *testing.T) {
	obj := object(t, 500, 100, 0)
	size := int64(len(obj))
	ctx := context.Background()
	boom := errors.New("boom")

	t.Run("object smaller than a trailer", func(t *testing.T) {
		s := &served{obj: []byte("PAR1")}
		if _, err := Read(ctx, s.dl, "k", 4, 100, 0, 0); !errors.Is(err, ErrNotParquet) || len(s.calls) != 0 {
			t.Fatalf("err=%v calls=%v", err, s.calls)
		}
	})
	t.Run("first request fails", func(t *testing.T) {
		s := &served{obj: obj, fail: map[int]error{0: boom}}
		if _, err := Read(ctx, s.dl, "k", size, 100, 0, 0); !errors.Is(err, boom) || errors.Is(err, ErrNotParquet) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("second request fails", func(t *testing.T) {
		s := &served{obj: object(t, 500, 100, 40_000), fail: map[int]error{1: boom}}
		if _, err := Read(ctx, s.dl, "k", int64(len(s.obj)), 100, 0, 0); !errors.Is(err, boom) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("a short first answer", func(t *testing.T) {
		s := &served{obj: obj, short: map[int]int{0: 3}}
		if _, err := Read(ctx, s.dl, "k", size, 100, 0, 0); err == nil || !strings.Contains(err.Error(), "short") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("a short second answer", func(t *testing.T) {
		big := object(t, 500, 100, 40_000)
		s := &served{obj: big, short: map[int]int{1: 3}}
		if _, err := Read(ctx, s.dl, "k", int64(len(big)), 100, 0, 0); err == nil || !strings.Contains(err.Error(), "short") {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("not a parquet trailer", func(t *testing.T) {
		junk := bytes.Repeat([]byte("x"), 300)
		s := &served{obj: junk}
		if _, err := Read(ctx, s.dl, "k", 300, 100, 0, 0); !errors.Is(err, ErrNotParquet) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("a footer longer than the object", func(t *testing.T) {
		bad := append(bytes.Repeat([]byte{0}, 50), 0xff, 0xff, 0xff, 0x7f, 'P', 'A', 'R', '1')
		s := &served{obj: bad}
		if _, err := Read(ctx, s.dl, "k", int64(len(bad)), 20, 0, 0); !errors.Is(err, ErrNotParquet) {
			t.Fatalf("err=%v", err)
		}
		if len(s.calls) != 1 {
			t.Fatalf("a footer that cannot fit the object must not trigger a second request: %v", s.calls)
		}
	})
}

// FuzzRead: whatever the object bytes, size claim, window and hint, Read does not
// panic, and when it succeeds the tail is the end of the object and holds the
// whole footer its trailer declares.
func FuzzRead(f *testing.F) {
	good := object(f, 300, 100, 2000)
	f.Add(good, int64(len(good)), int64(64), int64(0), int64(0))
	f.Add(good, int64(len(good)), int64(64), int64(Inspect(good).Tail), int64(100))
	f.Add(good, int64(len(good)), int64(8), int64(9), int64(0))
	f.Add([]byte("PAR1"), int64(4), int64(100), int64(0), int64(0))
	f.Add(append(bytes.Repeat([]byte{1}, 40), 0xff, 0xff, 0, 0, 'P', 'A', 'R', '1'), int64(48), int64(20), int64(30), int64(0))
	f.Fuzz(func(t *testing.T, obj []byte, size, window, hint, budget int64) {
		if size < 0 || size > int64(len(obj)) {
			size = int64(len(obj))
		}
		obj = obj[:size]
		s := &served{obj: obj}
		tail, err := Read(context.Background(), s.dl, "k", size, window, hint, budget)
		if err != nil {
			if errors.Is(err, ErrOverBudget) && budget <= 0 {
				t.Fatalf("over budget with no budget: %v", err)
			}
			return
		}
		if tail.Off < 0 || tail.Off+int64(len(tail.Data)) != size || !bytes.Equal(tail.Data, obj[tail.Off:]) {
			t.Fatalf("tail [%d,+%d) is not the end of an object of %d", tail.Off, len(tail.Data), size)
		}
		n, lerr := Length(tail.Data[len(tail.Data)-TrailerLen:])
		if lerr != nil || int64(n)+TrailerLen > int64(len(tail.Data)) {
			t.Fatalf("tail does not hold the footer it declares: %d, %v, have %d", n, lerr, len(tail.Data))
		}
		if len(s.calls) > 2 {
			t.Fatalf("%d requests", len(s.calls))
		}
	})
}

func TestRead_Budget(t *testing.T) {
	obj := object(t, 5000, 1000, 40_000)
	size := int64(len(obj))
	foot := footerLenOf(t, obj) + TrailerLen
	exact := int64(Inspect(obj).Tail)
	ctx := context.Background()
	t.Run("a recorded length over the budget costs no request", func(t *testing.T) {
		s := &served{obj: obj}
		_, err := Read(ctx, s.dl, "k", size, 64<<10, exact, exact-1)
		if !errors.Is(err, ErrOverBudget) || len(s.calls) != 0 {
			t.Fatalf("err=%v calls=%v", err, s.calls)
		}
	})
	t.Run("a recorded length within the budget is read", func(t *testing.T) {
		s := &served{obj: obj}
		if _, err := Read(ctx, s.dl, "k", size, 64<<10, exact, exact); err != nil || len(s.calls) != 1 {
			t.Fatalf("err=%v calls=%v", err, s.calls)
		}
	})
	t.Run("an unrecorded footer over the budget stops after the window", func(t *testing.T) {
		s := &served{obj: obj}
		_, err := Read(ctx, s.dl, "k", size, 1000, 0, foot-1)
		if !errors.Is(err, ErrOverBudget) || len(s.calls) != 1 || s.calls[0][1] != 1000 {
			t.Fatalf("err=%v calls=%v", err, s.calls)
		}
	})
	t.Run("an unrecorded footer within the budget costs the second request", func(t *testing.T) {
		s := &served{obj: obj}
		if _, err := Read(ctx, s.dl, "k", size, 1000, 0, foot); err != nil || len(s.calls) != 2 {
			t.Fatalf("err=%v calls=%v", err, s.calls)
		}
	})
}
