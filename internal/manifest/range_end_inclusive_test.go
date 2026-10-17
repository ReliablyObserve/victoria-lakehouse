package manifest

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// The end of a query's time range is inclusive (the HTTP end argument is exclusive and the
// handler subtracts one nanosecond before the storage sees it), so a partition that starts
// exactly at the end holds rows at the end. The partition walk used to stop at such a
// partition (!p.start.Before(end)), and a row exactly at the end of the range, in the first
// nanosecond of an hour, was never read.

type refFile struct {
	partition string
	fi        FileInfo
	pStart    time.Time
}

// referenceRange is the answer by brute force over every file: a file with recorded bounds
// matches when [min, max] meets [start, end]; one without matches when its hour does.
func referenceRange(files []refFile, startNs, endNs int64) []string {
	var out []string
	for _, f := range files {
		pStartNs, pEndNs := f.pStart.UnixNano(), f.pStart.Add(time.Hour).UnixNano()
		var ok bool
		if f.fi.MinTimeNs != 0 && f.fi.MaxTimeNs != 0 {
			ok = f.fi.MinTimeNs <= endNs && f.fi.MaxTimeNs >= startNs
		} else {
			ok = pStartNs <= endNs && pEndNs > startNs
		}
		if ok {
			out = append(out, f.fi.Key)
		}
	}
	return keysSorted(out)
}

func keysSorted(k []string) []string {
	out := append([]string(nil), k...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func TestGetFilesForRange_AHourStartingAtTheRangeEndIsRead(t *testing.T) {
	m := New("bucket", "")
	h10 := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	h11 := h10.Add(time.Hour)
	m.AddFile(rangeWalkPartition(h10), FileInfo{Key: "a/h10.parquet", Size: 1, RowCount: 1, MinTimeNs: h10.UnixNano() + 5, MaxTimeNs: h10.UnixNano() + 6})
	// the only row of this object is the first nanosecond of the hour
	m.AddFile(rangeWalkPartition(h11), FileInfo{Key: "a/h11.parquet", Size: 1, RowCount: 1, MinTimeNs: h11.UnixNano(), MaxTimeNs: h11.UnixNano()})
	// and an object with no recorded bounds in that hour
	m.AddFile(rangeWalkPartition(h11), FileInfo{Key: "a/h11-nobounds.parquet", Size: 1, RowCount: 1})

	end := h11.UnixNano()
	got := keysOfFiles(m.GetFilesForRange(h10.UnixNano(), end))
	want := []string{"a/h10.parquet", "a/h11-nobounds.parquet", "a/h11.parquet"}
	if !equalKeys(got, want) {
		t.Errorf("GetFilesForRange up to the first nanosecond of the next hour = %v, want %v", got, want)
	}
	// one nanosecond earlier the next hour is out, as it must be.
	got = keysOfFiles(m.GetFilesForRange(h10.UnixNano(), end-1))
	if !equalKeys(got, []string{"a/h10.parquet"}) {
		t.Errorf("GetFilesForRange ending one nanosecond before the next hour = %v, want only the first hour's object", got)
	}
}

// Random manifests queried at the edges of every hour (the boundary and one or two nanoseconds
// either side, and the edges of the files' own bounds) agree with the brute-force answer, for
// the unscoped walk and for every tenant's.
func TestRangeWalk_EdgesAgreeWithBruteForce(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261016))
	tenants := [][2]string{{"0", "0"}, {"1001", "0"}, {"2002", "7"}}
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	for iteration := 0; iteration < 6; iteration++ {
		m := New("test-bucket", "")
		m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
		var files []refFile
		for p := 0; p < 72; p++ {
			if rnd.Intn(3) == 0 {
				continue // sparse
			}
			hour := base.Add(time.Duration(p) * time.Hour)
			part := rangeWalkPartition(hour)
			for _, tn := range tenants {
				for i := 0; i < 1+rnd.Intn(3); i++ {
					fi := FileInfo{Key: fmt.Sprintf("%s/%s/logs/%s/f%d.parquet", tn[0], tn[1], part, i), Size: 10, RowCount: 1}
					switch rnd.Intn(4) {
					case 0: // no bounds recorded
					case 1: // the first nanosecond of the hour
						fi.MinTimeNs, fi.MaxTimeNs = hour.UnixNano(), hour.UnixNano()+int64(rnd.Intn(1000))
					case 2: // the last nanosecond of the hour
						fi.MinTimeNs, fi.MaxTimeNs = hour.UnixNano()+int64(time.Hour)-int64(rnd.Intn(1000))-1, hour.UnixNano()+int64(time.Hour)-1
					default:
						lo := hour.UnixNano() + rnd.Int63n(int64(time.Hour)-1000)
						fi.MinTimeNs, fi.MaxTimeNs = lo, lo+rnd.Int63n(1000)
					}
					m.AddFile(part, fi)
					files = append(files, refFile{part, fi, hour})
				}
			}
		}
		edges := []int64{}
		for p := 0; p < 74; p++ {
			h := base.Add(time.Duration(p) * time.Hour).UnixNano()
			edges = append(edges, h-2, h-1, h, h+1, h+2)
		}
		for _, f := range files {
			edges = append(edges, f.fi.MinTimeNs-1, f.fi.MinTimeNs, f.fi.MinTimeNs+1, f.fi.MaxTimeNs-1, f.fi.MaxTimeNs, f.fi.MaxTimeNs+1)
		}
		for w := 0; w < 600; w++ {
			a, b := edges[rnd.Intn(len(edges))], edges[rnd.Intn(len(edges))]
			if a > b {
				a, b = b, a
			}
			if a <= 0 {
				continue
			}
			got := keysOfFiles(m.GetFilesForRange(a, b))
			want := referenceRange(files, a, b)
			if !equalKeys(got, want) {
				t.Fatalf("iteration %d, window [%d, %d]: GetFilesForRange = %v, want %v", iteration, a, b, got, want)
			}
			for _, tn := range tenants {
				prefix := tn[0] + "/" + tn[1] + "/"
				var mine []refFile
				for _, f := range files {
					if strings.HasPrefix(f.fi.Key, prefix) {
						mine = append(mine, f)
					}
				}
				got := keysOfFiles(m.GetFilesForRangeTenant(a, b, tn[0], tn[1]))
				want := referenceRange(mine, a, b)
				if !equalKeys(got, want) {
					t.Fatalf("iteration %d, tenant %s, window [%d, %d]: GetFilesForRangeTenant = %v, want %v", iteration, prefix, a, b, got, want)
				}
			}
		}
	}
}
