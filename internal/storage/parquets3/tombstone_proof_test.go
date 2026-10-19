package parquets3

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// proofFile is the file every case below asks about: rows from 1000 to 2000 ns
// of service alpha and beta, level INFO.
func proofFile() manifest.FileInfo {
	return manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/f.parquet",
		MinTimeNs: 1000, MaxTimeNs: 2000,
		Labels: map[string][]string{"service.name": {"alpha", "beta"}, "severity_text": {"INFO"}},
	}
}

// TestTombstoneProof_NeverProves is the table of what the proof may and may not
// conclude. Every "may" row is a doubt that must keep today's behaviour (the
// file is read with the tombstone); the "cannot" rows are the proofs.
func TestTombstoneProof_NeverProves(t *testing.T) {
	mock := newMockS3Server()
	t.Cleanup(mock.close)
	s := testStorageWithS3(t, mock.url())

	over := func(mut func(*manifest.FileInfo)) manifest.FileInfo { fi := proofFile(); mut(&fi); return fi }
	capped := func() map[string][]string {
		vals := make([]string, maxLabelsPerField)
		for i := range vals {
			vals[i] = "svc-" + strconv.Itoa(i)
		}
		return map[string][]string{"service.name": vals}
	}
	lo, hi := int64(-1<<63), int64(1<<63-1)
	cases := []struct {
		name  string
		fi    manifest.FileInfo
		query string
		start int64
		end   int64
		may   bool
	}{
		// the proofs
		{"value no file holds", proofFile(), `service.name:="gone"`, lo, hi, false},
		{"in() of values none holds", proofFile(), `service.name:in("gone","gone2")`, lo, hi, false},
		{"value in an AND with another term", proofFile(), `service.name:="gone" user:="u1"`, lo, hi, false},
		{"internal name resolves to the label column", proofFile(), `level:="ERROR"`, lo, hi, false},
		{"the label column spelling resolves too", proofFile(), `severity_text:="ERROR"`, lo, hi, false},
		{"time: range ends before the file", proofFile(), `user:="u1"`, lo, 999, false},
		{"time: range starts after the file", proofFile(), `user:="u1"`, 2001, hi, false},
		{"empty range (start after end)", proofFile(), `*`, 5, 3, false},
		{"empty range inside the file's span", proofFile(), `service.name:="alpha"`, 1500, 1400, false},
		{"labels prove with unknown time bounds", over(func(f *manifest.FileInfo) { f.MinTimeNs, f.MaxTimeNs = 0, 0 }), `service.name:="gone"`, lo, hi, false},

		// the doubts
		{"value a file holds", proofFile(), `service.name:="alpha"`, lo, hi, true},
		{"in() with one value a file holds", proofFile(), `service.name:in("gone","beta")`, lo, hi, true},
		{"empty value (rows without the field match)", proofFile(), `service.name:=""`, lo, hi, true},
		{"in() with an empty value", proofFile(), `service.name:in("","gone")`, lo, hi, true},
		{"exact prefix", proofFile(), `service.name:="gon"*`, lo, hi, true},
		{"regex", proofFile(), `service.name:~"gon.*"`, lo, hi, true},
		{"negation", proofFile(), `NOT service.name:="gone"`, lo, hi, true},
		{"negation (dash)", proofFile(), `-service.name:="gone"`, lo, hi, true},
		{"OR", proofFile(), `service.name:="gone" OR service.name:="gone2"`, lo, hi, true},
		{"OR with a term", proofFile(), `service.name:="gone" OR user:="u1"`, lo, hi, true},
		{"map attribute", proofFile(), `user:="u1"`, lo, hi, true},
		{"free text", proofFile(), `gone`, lo, hi, true},
		{"in() of a subquery", proofFile(), `service.name:in(error | fields service.name)`, lo, hi, true},
		{"wildcard field name", proofFile(), `service*:="gone"`, lo, hi, true},
		{"match-all", proofFile(), `*`, lo, hi, true},
		{"unparsable query", proofFile(), `service.name:=`, lo, hi, true},
		{"compaction output: its union label set is not trusted", over(func(f *manifest.FileInfo) { f.CompactionLevel = 1 }), `service.name:="gone"`, lo, hi, true},
		{"label set missing", over(func(f *manifest.FileInfo) { f.Labels = nil }), `service.name:="gone"`, lo, hi, true},
		{"label set for another field only", over(func(f *manifest.FileInfo) { f.Labels = map[string][]string{"severity_text": {"INFO"}} }), `service.name:="gone"`, lo, hi, true},
		{"label set at the cap (incomplete)", over(func(f *manifest.FileInfo) { f.Labels = capped() }), `service.name:="gone"`, lo, hi, true},
		{"unknown time bounds", over(func(f *manifest.FileInfo) { f.MinTimeNs, f.MaxTimeNs = 0, 0 }), `user:="u1"`, lo, 999, true},
		{"one unknown time bound", over(func(f *manifest.FileInfo) { f.MinTimeNs = 0 }), `user:="u1"`, lo, 999, true},
		// the boundaries of the inclusive range
		{"range ends at the first row", proofFile(), `user:="u1"`, lo, 1000, true},
		{"range ends one ns before the first row", proofFile(), `user:="u1"`, lo, 999, false},
		{"range starts at the last row", proofFile(), `user:="u1"`, 2000, hi, true},
		{"range starts one ns after the last row", proofFile(), `user:="u1"`, 2001, hi, false},
		{"range inside the file", proofFile(), `user:="u1"`, 1200, 1300, true},
		{"unbounded range", proofFile(), `user:="u1"`, lo, hi, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tss := []tombstone{{ID: "t", Query: c.query, StartNs: c.start, EndNs: c.end}}
			fi := c.fi
			if got := s.anyMayMatch(s.compileProofs(tss), &fi, tss); got != c.may {
				t.Fatalf("anyMayMatch(%q, [%d,%d]) = %v, want %v", c.query, c.start, c.end, got, c.may)
			}
		})
	}
	if g := mock.gets.Load(); g != 0 {
		t.Fatalf("the proof made %d S3 reads, want none", g)
	}
}

// The proof is per tombstone: ONE tombstone that may match keeps the file on
// the row path, however many others cannot.
func TestTombstoneProof_AnyTombstoneThatMayMatchWins(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	fi := proofFile()
	tss := []tombstone{
		{ID: "a", Query: `service.name:="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1},
		{ID: "b", Query: `service.name:="beta"`, StartNs: -1 << 63, EndNs: 1<<63 - 1},
		{ID: "c", Query: `service.name:="gone2"`, StartNs: -1 << 63, EndNs: 1<<63 - 1},
	}
	if !s.anyMayMatch(s.compileProofs(tss), &fi, tss) {
		t.Fatal("a tombstone on beta may match the file, but the proof said clean")
	}
	if s.anyMayMatch(s.compileProofs(tss[:1]), &fi, tss[:1]) || s.anyMayMatch(s.compileProofs(tss[2:]), &fi, tss[2:]) {
		t.Fatal("a tombstone on a value the file does not hold should prove clean")
	}
	// A tombstone the request never compiled may match: unknown means may.
	if !s.anyMayMatch(nil, &fi, tss[:1]) {
		t.Fatal("a tombstone without a compiled proof must count as may-match")
	}
	if s.anyMayMatch(nil, &fi, nil) {
		t.Fatal("no tombstones: nothing may match")
	}
}

// Tombstones with the same ID but different content never share a proof.
func TestTombstoneProof_SameIDDifferentQuery(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	fi := proofFile()
	gone := tombstone{ID: "x", Query: `service.name:="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	alpha := tombstone{ID: "x", Query: `service.name:="alpha"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}
	proofs := s.compileProofs([]tombstone{gone, alpha})
	if s.anyMayMatch(proofs, &fi, []tombstone{gone}) {
		t.Error("gone should prove clean")
	}
	if !s.anyMayMatch(proofs, &fi, []tombstone{alpha}) {
		t.Error("alpha must stay may-match although it shares the ID of a clean tombstone")
	}
}

// The bloom answers from the pmeta facet only: a value no file holds proves clean
// even when the label set cannot (incomplete), a facet that cannot answer proves
// nothing, and the per-file bloom object is never fetched.
func TestTombstoneProof_BloomFacetOnly(t *testing.T) {
	groups := [][]schema.LogRow{{}}
	base := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Hour)
	for i := 0; i < 150; i++ { // more distinct services than a label set holds
		groups[0] = append(groups[0], schema.LogRow{TimestampUnixNano: base.Add(time.Duration(i) * time.Second).UnixNano(), Body: "m", ServiceName: fmt.Sprintf("svc-%03d", i)})
	}
	fx := newGateFx(t, true, groups)
	fi := fx.files[0]
	if len(fi.Labels["service.name"]) < maxLabelsPerField {
		t.Fatalf("fixture: the label set has %d values, want it at the cap", len(fi.Labels["service.name"]))
	}
	getsBefore := fx.mock.gets.Load()
	probe := func(query string) bool {
		tss := []tombstone{{ID: "t", Query: query, StartNs: -1 << 63, EndNs: 1<<63 - 1}}
		return fx.s.anyMayMatch(fx.s.compileProofs(tss), &fi, tss)
	}
	if probe(`service.name:="svc-007"`) != true {
		t.Error("a value the file holds must stay may-match")
	}
	if probe(`service.name:="never-written"`) != false {
		t.Error("the bloom facet should prove a value the file does not hold, with an incomplete label set")
	}
	if probe(`service.name:in("never-written","svc-149")`) != true {
		t.Error("in() with a held value must stay may-match")
	}
	if probe(`service.name:in("never-written","also-never")`) != false {
		t.Error("in() of values the facet excludes should prove clean")
	}
	// A facet that cannot answer: no pmeta, then an empty catalog.
	fx.s.catalog = nil
	if probe(`service.name:="never-written"`) != true {
		t.Error("without pmeta an incomplete label set proves nothing")
	}
	fx.s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	if probe(`service.name:="never-written"`) != true {
		t.Error("an empty bloom facet (usable=false) proves nothing")
	}
	if g := fx.mock.gets.Load() - getsBefore; g != 0 {
		t.Fatalf("the proof made %d S3 reads (a .bloom fetch?), want none", g)
	}
}

// In a multi-tenant request each file is judged against the tombstones of ITS
// tenant only: another tenant's tombstone neither makes the file dirty nor can
// it be proven away by this file's labels.
func TestTombstoneProof_CleanLane_IsPerTenant(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	file := func(key, svc string) manifest.FileInfo {
		return manifest.FileInfo{Key: key + "/logs/dt=2026-06-01/hour=10/f.parquet", MinTimeNs: 1000, MaxTimeNs: 2000,
			Labels: map[string][]string{"service.name": {svc}}}
	}
	fileA, fileB := file("1001/0", "x"), file("2002/7", "y")
	tomb := func(id, query string, acc, proj uint32) tombstone {
		return tombstone{ID: id, Query: query, StartNs: -1 << 63, EndNs: 1<<63 - 1, Tenants: []delete.TenantRef{{AccountID: acc, ProjectID: proj}}}
	}
	scope := resolveTenantScope([]logstorage.TenantID{{AccountID: 1001}, {AccountID: 2002, ProjectID: 7}})
	cases := []struct {
		name           string
		tss            []tombstone
		cleanA, cleanB bool
	}{
		{"1001's tombstone on a value only 2002 holds", []tombstone{tomb("a", `service.name:="y"`, 1001, 0)}, true, true},
		{"1001's tombstone on a value 1001 holds", []tombstone{tomb("a", `service.name:="x"`, 1001, 0)}, false, true},
		{"2002's tombstone on a value 1001 holds", []tombstone{tomb("a", `service.name:="x"`, 2002, 7)}, true, true},
		{"both tenants delete what they hold", []tombstone{tomb("a", `service.name:="x"`, 1001, 0), tomb("b", `service.name:="y"`, 2002, 7)}, false, false},
		{"2002 deletes everything", []tombstone{tomb("a", `*`, 2002, 7)}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sink := newTombstoneSink(scope, c.tss, delete.DefaultKeyTenant, false, func([]tombstone) logstorage.WriteDataBlockFunc { return nil })
			lane, rest := s.newCleanLane([]manifest.FileInfo{fileA, fileB}, sink, nil, nil)
			_, aClean := lane.keys[fileA.Key]
			_, bClean := lane.keys[fileB.Key]
			if aClean != c.cleanA || bClean != c.cleanB {
				t.Fatalf("clean: tenant 1001 file %v, tenant 2002 file %v; want %v and %v (dirty files: %d)", aClean, bClean, c.cleanA, c.cleanB, len(rest))
			}
			if len(lane.files)+len(rest) != 2 {
				t.Fatalf("lane has %d files and rest %d; every file is in exactly one", len(lane.files), len(rest))
			}
		})
	}
	// A single-tenant request applies every scoped tombstone to every file.
	one := newTombstoneSink(resolveTenantScope([]logstorage.TenantID{{AccountID: 1001}}), []tombstone{tomb("a", `service.name:="x"`, 1001, 0)},
		delete.DefaultKeyTenant, false, func([]tombstone) logstorage.WriteDataBlockFunc { return nil })
	if lane, _ := s.newCleanLane([]manifest.FileInfo{fileA}, one, nil, nil); len(lane.files) != 0 {
		t.Fatal("single-tenant: the file holding the deleted value must be dirty")
	}
}

// Above the pair budget nothing is proven: every file keeps today's path.
func TestTombstoneProof_PairBudget(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	saved := maxProofPairs
	t.Cleanup(func() { maxProofPairs = saved })
	fi := proofFile()
	tss := []tombstone{{ID: "t", Query: `service.name:="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}}
	sink := newTombstoneSink(resolveTenantScope(nil), tss, delete.DefaultKeyTenant, false, func([]tombstone) logstorage.WriteDataBlockFunc { return nil })
	maxProofPairs = 1
	if lane, rest := s.newCleanLane([]manifest.FileInfo{fi}, sink, nil, nil); len(lane.files) != 1 || len(rest) != 0 {
		t.Fatalf("one pair is within a budget of 1: lane=%d rest=%d", len(lane.files), len(rest))
	}
	maxProofPairs = 0
	if lane, rest := s.newCleanLane([]manifest.FileInfo{fi}, sink, nil, nil); len(lane.files) != 0 || len(rest) != 1 {
		t.Fatalf("over budget: lane=%d rest=%d, want everything dirty", len(lane.files), len(rest))
	}
}
