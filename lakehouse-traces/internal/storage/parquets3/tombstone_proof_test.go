package parquets3

import (
	"strconv"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// proofFile is the file every case below asks about: spans from 1000 to 2000 ns
// of service alpha and beta, span name GET.
func proofFile() manifest.FileInfo {
	return manifest.FileInfo{
		Key:       "logs/dt=2026-06-01/hour=10/f.parquet",
		MinTimeNs: 1000, MaxTimeNs: 2000,
		Labels: map[string][]string{"service.name": {"alpha", "beta"}, "span.name": {"GET"}},
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
	svc := `"resource_attr:service.name"`
	cases := []struct {
		name  string
		fi    manifest.FileInfo
		query string
		start int64
		end   int64
		may   bool
	}{
		// the proofs
		{"value no file holds", proofFile(), svc + `:="gone"`, lo, hi, false},
		{"in() of values none holds", proofFile(), svc + `:in("gone","gone2")`, lo, hi, false},
		{"value in an AND with another term", proofFile(), svc + `:="gone" span_attr:user:="u1"`, lo, hi, false},
		{"span name resolves to the label column", proofFile(), `name:="DELETE"`, lo, hi, false},
		{"time: range ends before the file", proofFile(), `span_attr:user:="u1"`, lo, 999, false},
		{"time: range starts after the file", proofFile(), `span_attr:user:="u1"`, 2001, hi, false},
		{"empty range (start after end)", proofFile(), `*`, 5, 3, false},
		{"empty range inside the file's span", proofFile(), svc + `:="alpha"`, 1500, 1400, false},
		{"labels prove with unknown time bounds", over(func(f *manifest.FileInfo) { f.MinTimeNs, f.MaxTimeNs = 0, 0 }), svc + `:="gone"`, lo, hi, false},

		// the doubts
		{"value a file holds", proofFile(), svc + `:="alpha"`, lo, hi, true},
		{"span name a file holds", proofFile(), `name:="GET"`, lo, hi, true},
		{"in() with one value a file holds", proofFile(), svc + `:in("gone","beta")`, lo, hi, true},
		{"empty value (spans without the field match)", proofFile(), svc + `:=""`, lo, hi, true},
		{"in() with an empty value", proofFile(), svc + `:in("","gone")`, lo, hi, true},
		{"exact prefix", proofFile(), svc + `:="gon"*`, lo, hi, true},
		{"regex", proofFile(), svc + `:~"gon.*"`, lo, hi, true},
		{"negation", proofFile(), `NOT ` + svc + `:="gone"`, lo, hi, true},
		{"OR", proofFile(), svc + `:="gone" OR ` + svc + `:="gone2"`, lo, hi, true},
		{"span attribute", proofFile(), `span_attr:user:="u1"`, lo, hi, true},
		{"trace id without a bloom (not a label)", proofFile(), `trace_id:="abc"`, lo, hi, true},
		{"trace id phrase", proofFile(), `trace_id:"abc"`, lo, hi, true},
		{"in() of a subquery", proofFile(), svc + `:in(error | fields x)`, lo, hi, true},
		{"match-all", proofFile(), `*`, lo, hi, true},
		{"unparsable query", proofFile(), svc + `:=`, lo, hi, true},
		{"compaction output: its union label set is not trusted", over(func(f *manifest.FileInfo) { f.CompactionLevel = 1 }), svc + `:="gone"`, lo, hi, true},
		{"label set missing", over(func(f *manifest.FileInfo) { f.Labels = nil }), svc + `:="gone"`, lo, hi, true},
		{"label set at the cap (incomplete)", over(func(f *manifest.FileInfo) { f.Labels = capped() }), svc + `:="gone"`, lo, hi, true},
		{"unknown time bounds", over(func(f *manifest.FileInfo) { f.MinTimeNs, f.MaxTimeNs = 0, 0 }), `span_attr:user:="u1"`, lo, 999, true},
		// the boundaries of the inclusive range
		{"range ends at the first span", proofFile(), `span_attr:user:="u1"`, lo, 1000, true},
		{"range ends one ns before the first span", proofFile(), `span_attr:user:="u1"`, lo, 999, false},
		{"range starts at the last span", proofFile(), `span_attr:user:="u1"`, 2000, hi, true},
		{"range starts one ns after the last span", proofFile(), `span_attr:user:="u1"`, 2001, hi, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tss := []tombstone{{ID: "t", Query: c.query, StartNs: c.start, EndNs: c.end}}
			fi := c.fi
			if got := s.compileProofs(tss).anyMayMatch(&fi, tss); got != c.may {
				t.Fatalf("anyMayMatch(%q, [%d,%d]) = %v, want %v", c.query, c.start, c.end, got, c.may)
			}
		})
	}
}

// The bloom facet proves a trace id the file does not hold; the label set
// cannot (trace_id is not a label); a facet that cannot answer proves nothing;
// the per-file bloom object is never fetched.
func TestTombstoneProof_TraceIDBloomFacetOnly(t *testing.T) {
	fx := newGateFx(t, true, gateGroups(gateBase()))
	fi := fx.files[1]
	getsBefore := fx.gets.Load()
	probe := func(query string) bool {
		tss := []tombstone{{ID: "t", Query: query, StartNs: -1 << 63, EndNs: 1<<63 - 1}}
		return fx.s.compileProofs(tss).anyMayMatch(&fi, tss)
	}
	held, other := gateTraceID(1, 2), gateTraceID(0, 2)
	if !probe(`trace_id:="` + held + `"`) {
		t.Error("a trace the file holds must stay may-match")
	}
	if probe(`trace_id:="` + other + `"`) {
		t.Error("a trace only another file holds should prove clean through the bloom facet")
	}
	if probe(`trace_id:in("` + other + `","` + gateTraceID(9, 9) + `")`) {
		t.Error("in() of traces none of which the file holds should prove clean")
	}
	if !probe(`trace_id:in("` + other + `","` + held + `")`) {
		t.Error("in() with a held trace must stay may-match")
	}
	fx.s.catalog = nil
	if !probe(`trace_id:="` + other + `"`) {
		t.Error("without pmeta a trace id proves nothing")
	}
	fx.s.catalog = newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	if !probe(`trace_id:="` + other + `"`) {
		t.Error("an empty bloom facet proves nothing")
	}
	if g := fx.gets.Load() - getsBefore; g != 0 {
		t.Fatalf("the proof made %d S3 reads (a .bloom fetch?), want none", g)
	}
}

// In a multi-tenant request each file is judged against the tombstones of ITS
// tenant only.
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
		{"1001's tombstone on a value only 2002 holds", []tombstone{tomb("a", `"resource_attr:service.name":="y"`, 1001, 0)}, true, true},
		{"1001's tombstone on a value 1001 holds", []tombstone{tomb("a", `"resource_attr:service.name":="x"`, 1001, 0)}, false, true},
		{"2002's tombstone on a value 1001 holds", []tombstone{tomb("a", `"resource_attr:service.name":="x"`, 2002, 7)}, true, true},
		{"both tenants delete what they hold", []tombstone{tomb("a", `"resource_attr:service.name":="x"`, 1001, 0), tomb("b", `"resource_attr:service.name":="y"`, 2002, 7)}, false, false},
		{"2002 deletes everything", []tombstone{tomb("a", `*`, 2002, 7)}, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sink := newTombstoneSink(scope, c.tss, delete.DefaultKeyTenant, false, func([]tombstone) logstorage.WriteDataBlockFunc { return nil })
			lane, rest := s.newCleanLane([]manifest.FileInfo{fileA, fileB}, sink, nil, nil)
			aClean, bClean := laneHas(lane, fileA.Key), laneHas(lane, fileB.Key)
			if aClean != c.cleanA || bClean != c.cleanB {
				t.Fatalf("clean: tenant 1001 file %v, tenant 2002 file %v; want %v and %v (dirty files: %d)", aClean, bClean, c.cleanA, c.cleanB, len(rest))
			}
		})
	}
}

// Above the pair budget nothing is proven: every file keeps today's path.
func TestTombstoneProof_PairBudget(t *testing.T) {
	s := testStorageWithS3(t, "http://127.0.0.1:1")
	saved := maxProofPairs
	t.Cleanup(func() { maxProofPairs = saved })
	fi := proofFile()
	tss := []tombstone{{ID: "t", Query: `"resource_attr:service.name":="gone"`, StartNs: -1 << 63, EndNs: 1<<63 - 1}}
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

// laneHas reports whether the file at key is one of the lane's clean files.
func laneHas(l *cleanLane, key string) bool {
	for i := range l.files {
		if l.files[i].Key == key {
			return true
		}
	}
	return false
}
