// plansim: OFFLINE planner simulation over REAL L2 parquet footers.
// Replays the planProjectedRanges + coalesceRanges math from
// internal/storage/parquets3/projected_fetch.go and
// internal/s3reader/planned_fetch.go against downloaded files, for four
// planner variants and three workload shapes, and models wall time at
// RTT=100ms with k-way span concurrency.
//
// SCRATCH TOOL — not part of the build, do not commit to main paths.
package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const (
	rttSec   = 0.100                  // 100 ms RTT (live bench shape)
	connBW   = 50e6                   // 50 MB/s per-connection bandwidth assumption
	decodeBW = 100e6                  // compressed-equivalent decode rate (ZSTD ~4x expansion + scan)
	splitB   = int64(rttSec * connBW) // 5MB AnyBlob-style request size for span splitting
	planCapB = 16 << 20               // defaultProjectedFetchMaxBytes
	cfgGapB  = 1 << 20                // CoalesceGapBytes default (1MB)
	openRTTs = 2                      // serial round trips for parquet.OpenFile footer (len read -> footer read)
	openGETs = 2                      // GETs charged per open
	nFiles   = 40                     // live bench file count
	fileWkrs = 8                      // file-worker pool
)

type Range struct{ Off, Len int64 }

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// clampGap mirrors NewPlannedFetchReaderAt's gap clamping.
func clampGap(gap, fileSize int64) int64 {
	if c := max64(64<<10, fileSize/8); gap > c {
		gap = c
	}
	if gap > 16<<20 {
		gap = 16 << 20
	}
	return gap
}

// coalesce mirrors coalesceRanges/mergeRangesWithOverfetch: clamp to file,
// sort, merge ranges within gap; returns merged spans + gap bytes overfetched.
func coalesce(ranges []Range, gap, fileSize int64) (out []Range, overfetch int64) {
	rs := make([]Range, 0, len(ranges))
	for _, r := range ranges {
		off, ln := r.Off, r.Len
		if off < 0 {
			ln += off
			off = 0
		}
		if fileSize > 0 && off+ln > fileSize {
			ln = fileSize - off
		}
		if ln <= 0 || off >= fileSize {
			continue
		}
		rs = append(rs, Range{off, ln})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Off < rs[j].Off })
	for _, r := range rs {
		if n := len(out); n > 0 {
			cur := &out[n-1]
			end := cur.Off + cur.Len
			if r.Off <= end+gap {
				if r.Off > end {
					overfetch += r.Off - end
				}
				if r.Off+r.Len > end {
					cur.Len = r.Off + r.Len - cur.Off
				}
				continue
			}
		}
		out = append(out, r)
	}
	return
}

// planRanges replicates parquets3.planProjectedRanges over footer metadata.
func planRanges(meta *format.FileMetaData, rgIdxs []int, cols map[string]bool) []Range {
	var ranges []Range
	for _, ri := range rgIdxs {
		rg := &meta.RowGroups[ri]
		for ci := range rg.Columns {
			cc := &rg.Columns[ci]
			md := &cc.MetaData
			if len(md.PathInSchema) == 0 || !cols[md.PathInSchema[0]] {
				continue
			}
			start := md.DataPageOffset
			if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < start {
				start = md.DictionaryPageOffset
			}
			if md.TotalCompressedSize > 0 && start >= 0 {
				ranges = append(ranges, Range{start, md.TotalCompressedSize})
			}
			if cc.ColumnIndexOffset > 0 && cc.ColumnIndexLength > 0 {
				ranges = append(ranges, Range{cc.ColumnIndexOffset, int64(cc.ColumnIndexLength)})
			}
			if cc.OffsetIndexOffset > 0 && cc.OffsetIndexLength > 0 {
				ranges = append(ranges, Range{cc.OffsetIndexOffset, int64(cc.OffsetIndexLength)})
			}
		}
	}
	return ranges
}

// fetchWall models the span download as a k-server queue in span order
// (the semaphore in PlannedFetchReaderAt.Fetch): span cost = RTT + len/BW.
func fetchWall(spans []Range, k int) float64 {
	if len(spans) == 0 {
		return 0
	}
	if k > len(spans) {
		k = len(spans)
	}
	free := make([]float64, k)
	var wall float64
	for _, s := range spans {
		mi := 0
		for i := 1; i < k; i++ {
			if free[i] < free[mi] {
				mi = i
			}
		}
		done := free[mi] + rttSec + float64(s.Len)/connBW
		free[mi] = done
		if done > wall {
			wall = done
		}
	}
	return wall
}

type result struct {
	spans  int
	planB  int64
	overB  int64
	wall4  float64 // fetch wall only, k=4
	wall8  float64
	wall16 float64
	capFB  bool
}

// splitSpans cuts spans larger than splitB into splitB-sized sub-GETs
// (AnyBlob request sizing: each request ~ RTT*BW so parallelism, not
// per-request latency, dominates).
func splitSpans(spans []Range) []Range {
	var out []Range
	for _, s := range spans {
		for s.Len > splitB {
			out = append(out, Range{s.Off, splitB})
			s.Off += splitB
			s.Len -= splitB
		}
		out = append(out, s)
	}
	return out
}

type workload struct {
	name   string
	cols   map[string]bool
	allRGs bool // false => every 4th RG (sparse pruned pattern)
}

func main() {
	files := os.Args[1:]
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "usage: plansim <parquet files...>")
		os.Exit(1)
	}

	workloads := []workload{
		{"filtered_count", map[string]bool{"timestamp_unix_nano": true, "service.name": true}, false},
		{"fulltext", map[string]bool{"timestamp_unix_nano": true, "body": true}, true},
		{"count_24h", map[string]bool{"timestamp_unix_nano": true}, true},
	}

	type agg struct {
		res  [5]result
		need int64
		n    int
	}
	aggs := map[string]*agg{}
	for _, w := range workloads {
		aggs[w.name] = &agg{}
	}

	for _, path := range files {
		fh, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		st, _ := fh.Stat()
		size := st.Size()
		pf, err := parquet.OpenFile(fh, size)
		if err != nil {
			panic(err)
		}
		meta := pf.Metadata()

		// --- footer anatomy dump ---
		nRG := len(meta.RowGroups)
		colBytes := map[string]int64{}
		var rows int64
		codec := ""
		for ri := range meta.RowGroups {
			rows += meta.RowGroups[ri].NumRows
			for ci := range meta.RowGroups[ri].Columns {
				md := &meta.RowGroups[ri].Columns[ci].MetaData
				if len(md.PathInSchema) > 0 {
					colBytes[md.PathInSchema[0]] += md.TotalCompressedSize
				}
				if codec == "" {
					codec = md.Codec.String()
				}
			}
		}
		fmt.Printf("FILE %s  size=%.1fMB  rowGroups=%d  rows=%d  codec=%s\n", path, float64(size)/1e6, nRG, rows, codec)
		fmt.Printf("  col bytes: ts=%.2fMB body=%.2fMB service.name=%.2fMB (effective current gap=%.2fMB)\n",
			float64(colBytes["timestamp_unix_nano"])/1e6, float64(colBytes["body"])/1e6,
			float64(colBytes["service.name"])/1e6, float64(clampGap(cfgGapB, size))/1e6)

		for _, w := range workloads {
			var rgIdxs []int
			for i := 0; i < nRG; i++ {
				if w.allRGs || i%4 == 0 {
					rgIdxs = append(rgIdxs, i)
				}
			}
			raw := planRanges(meta, rgIdxs, w.cols)
			var need int64
			for _, r := range raw {
				need += r.Len
			}

			curGap := clampGap(cfgGapB, size)
			a := aggs[w.name]
			a.need += need
			a.n++

			// V1: per-RG plan (verdict's failure shape): coalesce per RG,
			// RGs strictly serialized (each RG fetch completes before next).
			{
				var spans, planB, overB int64
				var w4, w8, w16 float64
				for _, ri := range rgIdxs {
					rr := planRanges(meta, []int{ri}, w.cols)
					sp, ov := coalesce(rr, curGap, size)
					spans += int64(len(sp))
					for _, s := range sp {
						planB += s.Len
					}
					overB += ov
					w4 += fetchWall(sp, 4)
					w8 += fetchWall(sp, 8)
					w16 += fetchWall(sp, 16)
				}
				r := &a.res[0]
				r.spans += int(spans)
				r.planB += planB
				r.overB += overB
				r.wall4 += w4
				r.wall8 += w8
				r.wall16 += w16
				if planB > planCapB {
					r.capFB = true
				}
			}
			// V2: per-FILE plan, current gap (what HEAD's armProjectedPlan does).
			// V3: per-FILE plan, RTT-aware gap* = RTT*connBW = 5MB (16MB cap only).
			for vi, gap := range []int64{curGap, int64(rttSec * connBW)} {
				sp, ov := coalesce(raw, gap, size)
				var planB int64
				for _, s := range sp {
					planB += s.Len
				}
				r := &a.res[1+vi]
				r.spans += len(sp)
				r.planB += planB
				r.overB += ov
				r.wall4 += fetchWall(sp, 4)
				r.wall8 += fetchWall(sp, 8)
				r.wall16 += fetchWall(sp, 16)
				if planB > planCapB {
					r.capFB = true
				}
			}
			// V4: whole-file degenerate: 1 span [0,size), single connection.
			{
				sp := []Range{{0, size}}
				r := &a.res[3]
				r.spans += 1
				r.planB += size
				r.overB += size - need
				r.wall4 += fetchWall(sp, 4)
				r.wall8 += fetchWall(sp, 8)
				r.wall16 += fetchWall(sp, 16)
			}
			// V5: per-FILE gap*=5MB + AnyBlob 5MB span SPLITTING (cap exempt:
			// memory bounded by plan, decoded streaming or budget-charged).
			{
				sp, ov := coalesce(raw, int64(rttSec*connBW), size)
				sp = splitSpans(sp)
				var planB int64
				for _, s := range sp {
					planB += s.Len
				}
				r := &a.res[4]
				r.spans += len(sp)
				r.planB += planB
				r.overB += ov
				r.wall4 += fetchWall(sp, 4)
				r.wall8 += fetchWall(sp, 8)
				r.wall16 += fetchWall(sp, 16)
			}
		}
		fh.Close()
	}

	names := [5]string{"V1 per-RG (live failure)", "V2 per-FILE gap=cur(1MB)", "V3 per-FILE gap*=5MB", "V4 whole-file 1-span", "V5 per-FILE 5MB+split"}
	for _, w := range workloads {
		a := aggs[w.name]
		n := float64(a.n)
		needMB := float64(a.need) / n / 1e6
		fmt.Printf("\n== workload %s ==  exact needed bytes/file = %.2f MB  decode/file = %.0f ms\n",
			w.name, needMB, float64(a.need)/n/decodeBW*1000)
		fmt.Printf("%-26s %9s %10s %8s %8s %8s %8s %11s %11s %11s %6s\n",
			"variant", "GETs/file", "bytes/file", "waste%", "w@k4", "w@k8", "w@k16", "40f/8w k4", "40f/8w k8", "40f/8w k16", "capFB")
		for vi, nm := range names {
			r := a.res[vi]
			spansPF := float64(r.spans) / n
			getsPF := spansPF + openGETs
			bytesPF := float64(r.planB) / n / 1e6
			waste := 0.0
			if r.planB > 0 {
				waste = 100 * float64(r.planB-int64(float64(a.need))) / float64(r.planB)
			}
			open := openRTTs * rttSec
			w4 := open + r.wall4/n
			w8 := open + r.wall8/n
			w16 := open + r.wall16/n
			dec := float64(a.need) / n / decodeBW
			// serial: ceil(40/8)=5 waves of (open+fetch+decode); aggregate-BW
			// floor: total fetched bytes over a 10GbE NIC (1.25 GB/s).
			bwFloor := float64(r.planB) / n * nFiles / 1.25e9
			t4 := maxf(5*(w4+dec), bwFloor)
			t8 := maxf(5*(w8+dec), bwFloor)
			t16 := maxf(5*(w16+dec), bwFloor)
			fb := ""
			if r.capFB {
				fb = "YES"
			}
			fmt.Printf("%-26s %9.1f %9.2fMB %7.1f%% %7.0fms %7.0fms %7.0fms %10.2fs %10.2fs %10.2fs %6s\n",
				nm, getsPF, bytesPF, waste, w4*1000, w8*1000, w16*1000, t4, t8, t16, fb)
		}
		// pipelining upside: decode of file i overlapped with open+fetch of
		// file i+1 (per worker, 5 files each). Shown for V2@k16 and V5@k8.
		open := openRTTs * rttSec
		dec := float64(a.need) / n / decodeBW
		for _, pick := range []struct {
			vi, k int
			nm    string
		}{{1, 16, "V2,k16"}, {4, 8, "V5,k8"}} {
			r := a.res[pick.vi]
			var fw float64
			switch pick.k {
			case 4:
				fw = r.wall4 / n
			case 8:
				fw = r.wall8 / n
			default:
				fw = r.wall16 / n
			}
			f := open + fw
			serial := 5 * (f + dec)
			pipe := f + 4*maxf(f, dec) + dec
			fmt.Printf("  pipelining (%s): serial=%.2fs  fetch/decode-overlapped=%.2fs  (%.0f%% saved)\n",
				pick.nm, serial, pipe, 100*(serial-pipe)/serial)
		}
	}
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
