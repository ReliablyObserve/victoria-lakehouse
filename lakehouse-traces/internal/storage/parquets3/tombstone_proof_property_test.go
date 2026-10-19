package parquets3

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestProperty_TombstoneProof_GatedEqualsScan is the oracle for per-file
// gating, traces twin: over random files, tombstones (every filter shape the
// proof must refuse, trace_id deletes with and without pmeta, every boundary of
// the time range) and queries, the answer with the proof is byte-identical to
// the answer with every file read with its tombstones (maxProofPairs = 0). The
// count is also checked against the spans themselves, by the predicate the
// rewriter uses, for trace_id tombstones.
func TestProperty_TombstoneProof_GatedEqualsScan(t *testing.T) {
	iters := 40
	if testing.Short() {
		iters = 25
	}
	saved := maxProofPairs
	t.Cleanup(func() { maxProofPairs = saved })

	svcs := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	var engaged uint64
	for it := 0; it < iters; it++ {
		rng := rand.New(rand.NewSource(int64(612 + it)))
		base := time.Now().UTC().Add(-8 * time.Hour).Truncate(time.Hour)
		nFiles := 2 + rng.Intn(3)
		groups := make([][]schema.TraceRow, nFiles)
		hostPool := 4
		if rng.Intn(4) == 0 {
			hostPool = 130
		}
		for g := range groups {
			pick := rng.Perm(len(svcs))[:1+rng.Intn(3)]
			rows := 8 + rng.Intn(8)
			if hostPool > 100 {
				rows = 110
			}
			hourOffset := time.Duration(rng.Intn(3)) * time.Hour
			for i := 0; i < rows; i++ {
				svc := svcs[pick[i%len(pick)]]
				groups[g] = append(groups[g], schema.TraceRow{
					TimestampUnixNano: base.Add(hourOffset + time.Duration(i)*time.Second).UnixNano(),
					TraceID:           gateTraceID(g, i/2),
					SpanID:            fmt.Sprintf("%016x", g*1000+i),
					SpanName:          []string{"GET", "PUT"}[rng.Intn(2)],
					ServiceName:       svc,
					HostName:          fmt.Sprintf("h%d", rng.Intn(hostPool)),
					Stream:            fmt.Sprintf(`{resource_attr:service.name=%q}`, svc),
					StreamID:          fmt.Sprintf("%048x", len(svc)),
					SpanAttributes:    map[string]string{"user": fmt.Sprintf("u%d", rng.Intn(4))},
				})
			}
		}
		fx := newGateFx(t, it%2 == 0, groups)
		if rng.Intn(3) == 0 {
			// A compaction output whose label set is the partial union of its
			// inputs': the file is marked compacted and its sets lose values.
			m := manifest.New("test-bucket", "logs/")
			for i, fi := range fx.files {
				if i == rng.Intn(len(fx.files)) {
					fi.CompactionLevel = 1
					partial := map[string][]string{}
					for k, vs := range fi.Labels {
						partial[k] = vs[:len(vs)/2]
					}
					fi.Labels = partial
				}
				m.AddFile(manifest.ExtractTenantPartition(fi.Key), fi)
			}
			fx.s.manifest = m
		}

		edges := []int64{-1 << 63, 1<<63 - 1}
		for _, fi := range fx.files {
			edges = append(edges, fi.MinTimeNs-1, fi.MinTimeNs, fi.MinTimeNs+1, fi.MaxTimeNs-1, fi.MaxTimeNs, fi.MaxTimeNs+1)
		}
		field := []string{gateSvc, "name", `"resource_attr:host.name"`, "span_attr:user", "trace_id"}
		value := func(f string) string {
			switch f {
			case gateSvc:
				return append(svcs, "gone")[rng.Intn(len(svcs)+1)]
			case "name":
				return []string{"GET", "PUT", "DELETE"}[rng.Intn(3)]
			case `"resource_attr:host.name"`:
				return fmt.Sprintf("h%d", rng.Intn(hostPool+2))
			case "span_attr:user":
				return fmt.Sprintf("u%d", rng.Intn(5))
			}
			return gateTraceID(rng.Intn(nFiles+1), rng.Intn(10))
		}
		var defs []delete.Tombstone
		oracleOK := true
		for k := 1 + rng.Intn(3); k > 0; k-- {
			f := field[rng.Intn(len(field))]
			if f != "trace_id" {
				oracleOK = false // the rewriter names the other fields differently
			}
			v, v2 := value(f), value(f)
			q := []string{
				fmt.Sprintf(`%s:=%q`, f, v),
				fmt.Sprintf(`%s:in(%q,%q)`, f, v, v2),
				fmt.Sprintf(`%s:in("",%q)`, f, v),
				fmt.Sprintf(`%s:=""`, f),
				fmt.Sprintf(`%s:~%q`, f, v+".*"),
				fmt.Sprintf(`NOT %s:=%q`, f, v),
				fmt.Sprintf(`%s:=%q OR %s:=%q`, f, v, f, v2),
				fmt.Sprintf(`%s:=%q name:=%q`, f, v, value("name")),
				fmt.Sprintf(`%s:=%q*`, f, v[:1]),
				`*`,
			}[rng.Intn(10)]
			defs = append(defs, delete.Tombstone{Query: q, StartNs: edges[rng.Intn(len(edges))], EndNs: edges[rng.Intn(len(edges))]})
		}
		fx.tombstone(defs...)

		type shape struct {
			q    string
			hint bool
		}
		for _, sh := range []shape{
			{`* | stats count() n`, true},
			{`* | stats by (_time:1h) count() n`, true},
			{`name:="GET" | stats count() n`, false},
			{`* | stats by (name) count() n`, false},
			{`* | field_names`, false},
			{`* | fields _time, name, trace_id | sort by (_time, name, trace_id)`, false},
		} {
			maxProofPairs = saved
			gated := fx.run(sh.q, sh.hint)
			maxProofPairs = 0
			scan := fx.run(sh.q, sh.hint)
			maxProofPairs = saved
			engaged += gated.meta
			if gated.rows != scan.rows {
				t.Fatalf("iter %d, %s, tombstones %+v:\n gated %s\n scan  %s", it, sh.q, defs, gated.rows, scan.rows)
			}
			if sh.q == `* | stats count() n` && oracleOK {
				var want int64
				for _, g := range groups {
					for i := range g {
						fields := delete.TraceRowFields(&g[i])
						hidden := false
						for d := range defs {
							if defs[d].MatchesFields(fields, g[i].TimestampUnixNano) {
								hidden = true
							}
						}
						if !hidden {
							want++
						}
					}
				}
				if gated.sum != want {
					t.Fatalf("iter %d: count %d, spans say %d; tombstones %+v", it, gated.sum, want, defs)
				}
			}
		}
	}
	if engaged == 0 {
		t.Fatal("no run was answered from metadata: the generator never exercised a proof")
	}
}
