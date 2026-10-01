package parquets3

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Round-3 follow-up tests (#272): partial page indexes, statistics edge cases,
// back-off bookkeeping and the startup window.
//
// Twin of lakehouse-traces/internal/storage/parquets3/restart_watermark_rr3b_test.go.

type rr3Row struct {
	T int64  `parquet:"_time"`
	M string `parquet:"_msg"`
}

// rr3File writes one row group per value (so every group has its own page index).
func rr3File(t *testing.T, vals ...int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[rr3Row](&buf)
	for _, v := range vals {
		if _, err := w.Write([]rr3Row{{v, "x"}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// rr3DropIndex rewrites the footer of data so row group rg has no column index.
func rr3DropIndex(t *testing.T, data []byte, rg int) []byte {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	md := f.Metadata()
	idx := findColumnIndex(f.Root(), "_time")
	md.RowGroups[rg].Columns[idx].ColumnIndexOffset = 0
	md.RowGroups[rg].Columns[idx].ColumnIndexLength = 0
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), md)
	if err != nil {
		t.Fatal(err)
	}
	out := append([]byte(nil), data[:len(data)-8-footerLen]...)
	out = append(out, footer...)
	var l [4]byte
	binary.LittleEndian.PutUint32(l[:], uint32(len(footer)))
	out = append(out, l[:]...)
	return append(out, []byte("PAR1")...)
}

// A row group without a page index may hold the newest row: the range must be
// reported as unknown, not as the range of the groups that have one.
func TestRR3_PartialPageIndexGivesNoBounds(t *testing.T) {
	data := rr3File(t, 100, 900, 500)
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if rows, lo, hi := pageIndexTimeBounds(f, "_time"); rows != 3 || lo != 100 || hi != 900 {
		t.Fatalf("complete index: rows=%d [%d, %d], want 3 [100, 900]", rows, lo, hi)
	}
	partial := rr3DropIndex(t, data, 1) // the group with the newest row
	// Lazy index loading: parquet-go reads each group's index on first use, so a
	// group whose index offset is gone reports an error instead of panicking.
	pf, err := parquet.OpenFile(bytes.NewReader(partial), int64(len(partial)), parquet.SkipPageIndex(true))
	if err != nil {
		t.Fatal(err)
	}
	rows, lo, hi := pageIndexTimeBounds(pf, "_time")
	if rows != 3 {
		t.Errorf("rows = %d, want 3", rows)
	}
	if lo != 0 || hi != 0 {
		t.Errorf("a partial page index reported [%d, %d]: an understated range would let the buffer re-serve the object's rows", lo, hi)
	}
	if rows, lo, hi := pageIndexTimeBounds(f, "nope"); rows != 3 || lo != 0 || hi != 0 {
		t.Errorf("missing column: rows=%d [%d, %d]", rows, lo, hi)
	}
}

// R15 / R16: statistics edge cases.
func TestRR3_FooterStatisticsEdgeCases(t *testing.T) {
	data := rr3File(t, 100, 300)
	open := func() (*parquet.File, int) {
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		return f, findColumnIndex(f.Root(), "_time")
	}
	t.Run("deprecated min/max are used when min_value/max_value are absent", func(t *testing.T) {
		f, idx := open()
		for i := range f.Metadata().RowGroups {
			st := &f.Metadata().RowGroups[i].Columns[idx].MetaData.Statistics
			st.Min, st.Max = st.MinValue, st.MaxValue
			st.MinValue, st.MaxValue = nil, nil
		}
		if rows, lo, hi, err := footerTimeBounds(f, idx); err != nil || rows != 2 || lo != 100 || hi != 300 {
			t.Errorf("rows=%d [%d, %d] err=%v, want 2 [100, 300]", rows, lo, hi, err)
		}
	})
	t.Run("a non-positive minimum is not a time range", func(t *testing.T) {
		f, idx := open()
		var zero [8]byte
		f.Metadata().RowGroups[0].Columns[idx].MetaData.Statistics.MinValue = zero[:]
		if _, lo, hi, err := footerTimeBounds(f, idx); err == nil {
			t.Errorf("min 0 accepted: [%d, %d]", lo, hi)
		}
	})
	t.Run("max below min is rejected", func(t *testing.T) {
		f, idx := open()
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], 50)
		f.Metadata().RowGroups[1].Columns[idx].MetaData.Statistics.MaxValue = b[:]
		f.Metadata().RowGroups[0].Columns[idx].MetaData.Statistics.MaxValue = b[:]
		if _, lo, hi, err := footerTimeBounds(f, idx); err == nil {
			t.Errorf("max < min accepted: [%d, %d]", lo, hi)
		}
	})
}

// R14: the back-off doubles per consecutive failure and is capped.
func TestRR3_BackoffDoublesAndIsCapped(t *testing.T) {
	var r boundsResolver
	base := time.Now()
	want := []time.Duration{boundsBackoffMin, 2 * boundsBackoffMin, 4 * boundsBackoffMin, 8 * boundsBackoffMin}
	for i, w := range want {
		r.failed("k", base, 0)
		r.mu.Lock()
		got := r.retry["k"].until.Sub(base)
		r.mu.Unlock()
		if got != w {
			t.Errorf("failure %d: back-off %v, want %v", i+1, got, w)
		}
	}
	for i := 0; i < 20; i++ {
		r.failed("k", base, 0)
	}
	r.mu.Lock()
	got := r.retry["k"].until.Sub(base)
	r.mu.Unlock()
	if got != boundsBackoffMax {
		t.Errorf("capped back-off = %v, want %v", got, boundsBackoffMax)
	}
}

// R19: a success forgets the back-off entry.
func TestRR3_SuccessForgetsTheBackoffEntry(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	r.s.inferredBounds.mu.Lock()
	r.s.inferredBounds.retry = map[string]boundsRetry{fi.Key: {until: time.Now().Add(-time.Second), fails: 3}} // expired
	r.s.inferredBounds.mu.Unlock()
	cur := r.s.resolveFileBounds(t.Context(), fi)
	if cur.BoundsInferred {
		t.Fatalf("not resolved: %+v", cur)
	}
	r.s.inferredBounds.mu.Lock()
	n := len(r.s.inferredBounds.retry)
	r.s.inferredBounds.mu.Unlock()
	if n != 0 {
		t.Errorf("the back-off table still holds %d entries after a success", n)
	}
}

// R20: a stale copy of an entry the manifest already resolved costs no read.
func TestRR3_StaleCopyOfAResolvedObjectCostsNoRead(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false)
	stale := r.objects()[0]
	r.s.manifest.EnrichFileMetadata(stale.Key, 2, at(rwHour, 10*time.Minute).UnixNano(), at(rwHour, 12*time.Minute).UnixNano())
	mock.reset()
	cur := r.s.resolveFileBounds(t.Context(), stale)
	if cur.BoundsInferred || cur.MaxTimeNs != at(rwHour, 12*time.Minute).UnixNano() {
		t.Errorf("stale copy not replaced by the manifest's exact entry: %+v", cur)
	}
	if full, ranged, _ := mock.gets(); full+ranged != 0 {
		t.Errorf("%d reads for an already resolved object", full+ranged)
	}
}

// R21: callers that waited on another caller's read use the bounds it resolved.
func TestRR3_WaitersUseTheResolvedBounds(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute), at(rwHour, 12*time.Minute))
	r.restart(true, false)
	files := r.objects()
	mock.delay(files[0].Key, 400*time.Millisecond)
	want := at(rwHour, 12*time.Minute).UnixNano()
	var wg sync.WaitGroup
	got := make([]int64, 16)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i] = r.s.bufferWatermarksFor(t.Context(), 0, files)[tenantZero]
		}(i)
	}
	wg.Wait()
	for i, g := range got {
		if g != want {
			t.Errorf("caller %d: watermark %d, want the resolved %d (a waiter fell back to its stale copy)", i, g, want)
		}
	}
	if _, ranged, _ := mock.gets(); ranged != 1 {
		t.Errorf("%d ranged reads, want 1 shared", ranged)
	}
}

// R22: the startup pass covers the whole recent window, not just the last hour.
func TestRR3_StartupPassCoversAFewHoursBack(t *testing.T) {
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour.Add(-4*time.Hour), 10*time.Minute))
	r.restart(true, false)
	rwSetClock(t, rwHour.Add(30*time.Minute))
	if n := r.s.enrichRecentInferredBounds(t.Context()); n != 1 {
		t.Errorf("resolved %d objects, want the one from 4 hours back", n)
	}
}

// A removal that overlaps an in-flight failing read must not leave a back-off
// entry behind.
func TestRR3_RemovalDuringAFailingReadLeavesNoBackoffEntry(t *testing.T) {
	singleAttemptS3(t)
	mock := newRWMock()
	r := newRWRigWith(t, mock.mockS3Server)
	r.ingest("COLD", at(rwHour, 10*time.Minute))
	r.restart(true, false)
	fi := r.objects()[0]
	mock.fail(fi.Key, 404)
	mock.delay(fi.Key, 300*time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.s.bufferWatermarksFor(t.Context(), 0, []manifest.FileInfo{fi})
	}()
	time.Sleep(100 * time.Millisecond)
	r.s.manifest.RemoveFile(manifest.ExtractPartition(fi.Key), fi.Key)
	<-done
	r.s.inferredBounds.mu.Lock()
	n := len(r.s.inferredBounds.retry)
	r.s.inferredBounds.mu.Unlock()
	if n != 0 {
		t.Errorf("%d back-off entries for an object removed while its read was in flight", n)
	}
}

// rwAliasBuffer hands out column values that point into one arena and wipes the
// arena as soon as the callback returns, as the logstore buffer's blocks do.
type rwAliasBuffer struct{ rwBuffer }

func (b *rwAliasBuffer) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	start, end := qctx.Query.GetFilterTimeRange()
	b.mu.Lock()
	var arena []byte
	var spans [][2]int
	for _, r := range b.rows {
		if r.ts >= start && r.ts <= end {
			spans = append(spans, [2]int{len(arena), len(arena) + len(r.level)})
			arena = append(arena, r.level...)
		}
	}
	b.mu.Unlock()
	if len(spans) == 0 {
		return nil
	}
	values := make([]string, len(spans))
	for i, sp := range spans {
		values[i] = unsafe.String(&arena[sp[0]], sp[1]-sp[0])
	}
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: "level", Values: values}})
	writeBlock(0, db)
	for i := range arena {
		arena[i] = 'X' // the engine reuses the block memory
	}
	return nil
}

// The buffered values of field_values must survive the engine reusing its block
// memory: with the keys aliasing it, spans of different names collapsed into one
// wrong name (seen live on the traces binary: all 15 buffered spans counted as
// "repro-buffer-1", sometimes split 10/5).
func TestRR3_BufferedFieldValuesDoNotAliasEngineMemory(t *testing.T) {
	r := newRWRig(t)
	r.ingest("COLD", at(rwHour, 5*time.Minute)) // an object, so the buffer is merged behind a watermark
	r.restart(true, true)
	buf := &rwAliasBuffer{}
	r.s.localBuffer = buf
	buf.add("ALPHA", at(rwHour, 10*time.Minute), at(rwHour, 11*time.Minute))
	buf.add("BETA", at(rwHour, 12*time.Minute))
	from, to := rwWindow()
	hits := r.levelHits(from, to)
	if hits["ALPHA"] != 2 || hits["BETA"] != 1 || hits["COLD"] != 1 || len(hits) != 3 {
		t.Errorf("field_values over buffered rows = %v, want ALPHA=2 BETA=1 COLD=1", hits)
	}
}
