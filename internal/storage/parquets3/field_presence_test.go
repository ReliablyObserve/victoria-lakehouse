package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// facetOfParquet reads the facet out of an object's footer alone.
func facetOfParquet(tb testing.TB, data []byte) (*fieldpresence.Facet, bool) {
	tb.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		tb.Fatal(err)
	}
	facet, overCap, err := fieldpresence.FromFooter(f.Metadata().KeyValueMetadata)
	if err != nil {
		tb.Fatalf("footer facet: %v", err)
	}
	return facet, overCap
}

// presenceHostileKeys are attribute names the cold read path suppresses or
// renames: bookkeeping columns, slot names, and names of scalar columns.
func presenceHostileKeys() []string {
	return []string{
		"account_id", "project_id", "ded_s01", "ded_s02", "level", "body", "_msg", "_time",
		"_stream", "service.name", "severity_number", "timestamp_unix_nano", "trace_id",
	}
}

// genPresenceLogRows builds rows of n random shapes over two UTC days.
func genPresenceLogRows(rng *rand.Rand, n int) []schema.LogRow {
	base := time.Date(2026, 7, 1, 23, 40, 0, 0, time.UTC)
	rows := make([]schema.LogRow, 0, n)
	streams := 1 + rng.Intn(6)
	if rng.Intn(4) == 0 {
		streams = 60 // many streams
	}
	for i := 0; i < n; i++ {
		s := rng.Intn(streams)
		r := schema.LogRow{
			TimestampUnixNano: base.Add(time.Duration(rng.Intn(50)) * time.Minute).Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Body:              pick(rng, "", "msg", "other message"),
			SeverityText:      pick(rng, "", "INFO", "WARN"),
			SeverityNumber:    int32(rng.Intn(3)) * 4,
			ServiceName:       pick(rng, "", "svc-a", "svc-b"),
		}
		switch rng.Intn(5) {
		case 0: // no _stream_id: the counter falls back to _stream
			r.Stream = fmt.Sprintf(`{service.name="s%d"}`, s)
		case 1: // neither
		case 2: // non-hex id
			r.StreamID = fmt.Sprintf("id-%d", s)
			r.Stream = fmt.Sprintf(`{service.name="s%d"}`, s)
		default:
			r.StreamID = fmt.Sprintf("%048x", uint64(s)*7919+1)
			r.Stream = fmt.Sprintf(`{service.name="s%d"}`, s)
		}
		if rng.Intn(3) == 0 {
			r.TraceID = fmt.Sprintf("%032x", rng.Uint64())
		}
		if rng.Intn(4) == 0 {
			r.K8sPodName = "pod-1"
		}
		if rng.Intn(4) == 0 {
			r.ContainerID = "c1"
		}
		if rng.Intn(6) == 0 {
			r.ExceptionType = "E"
		}
		if rng.Intn(5) == 0 {
			r.DedS01 = "slotvalue"
		}
		if rng.Intn(3) == 0 {
			r.LogAttributes = map[string]string{}
			for k := 0; k < 1+rng.Intn(3); k++ {
				key := fmt.Sprintf("attr%d", rng.Intn(5))
				if rng.Intn(4) == 0 {
					key = pickHostile(rng)
				}
				r.LogAttributes[key] = pick(rng, "", "v1", "v2")
			}
		}
		if rng.Intn(4) == 0 {
			r.ResourceAttributes = map[string]string{fmt.Sprintf("res%d", rng.Intn(3)): "r"}
			if rng.Intn(4) == 0 {
				r.ResourceAttributes[pickHostile(rng)] = "x"
			}
		}
		if rng.Intn(8) == 0 {
			r.ScopeAttributes = map[string]string{"scopeattr": "s"}
		}
		rows = append(rows, r)
	}
	return rows
}

func pick(rng *rand.Rand, vs ...string) string { return vs[rng.Intn(len(vs))] }

type streamDayKey struct {
	day  int32
	kind byte
	key  string
}

type presenceCell struct {
	rows  uint64
	names map[string]struct{}
}

// presenceFold folds the footer facets of objects into per stream-day cells: the
// union of the objects' names, credited with the stream-day's rows.
func presenceFold(facets []*fieldpresence.Facet) map[streamDayKey]*presenceCell {
	out := map[streamDayKey]*presenceCell{}
	for _, f := range facets {
		for _, e := range f.Entries {
			k := streamDayKey{e.Day, e.Kind, e.Key}
			c := out[k]
			if c == nil {
				c = &presenceCell{names: map[string]struct{}{}}
				out[k] = c
			}
			c.rows += e.Rows
			for _, n := range f.NamesOf(e) {
				c.names[n] = struct{}{}
			}
		}
	}
	return out
}

// presenceExpected is the answer a field_names read of all the objects must
// give: every name credited with the rows of the stream-days that list it.
func presenceExpected(facets []*fieldpresence.Facet) map[string]uint64 {
	hits := map[string]uint64{}
	for _, c := range presenceFold(facets) {
		for n := range c.names {
			hits[n] += c.rows
		}
	}
	return hits
}

func diffHits(got, want map[string]uint64) string {
	var d []string
	for n, w := range want {
		if g, ok := got[n]; !ok {
			d = append(d, fmt.Sprintf("missing %q (want %d)", n, w))
		} else if g != w {
			d = append(d, fmt.Sprintf("%q hits %d want %d", n, g, w))
		}
	}
	for n, g := range got {
		if _, ok := want[n]; !ok {
			d = append(d, fmt.Sprintf("extra %q (%d)", n, g))
		}
	}
	sort.Strings(d)
	return strings.Join(d, "; ")
}

// The naming-drift guard: the field names a flushed object's footer facet holds
// are, stream-day by stream-day in aggregate, the names and hits the cold read
// path's own field_names lists for the same objects.
func TestFieldPresence_MatchesTheReadPathsFieldNames(t *testing.T) {
	// Tier-2 slot bindings: none, one slot bound, and slots bound to names the
	// read path suppresses (a bookkeeping column) or shares with a scalar field.
	for _, slotAttrs := range [][]string{nil, {"tenant_id"}, {"account_id", "level"}} {
		prev := activeSlotResolver
		var attrs []schema.SlotAttr
		for _, n := range slotAttrs {
			attrs = append(attrs, schema.SlotAttr{Name: n})
		}
		if len(attrs) > 0 {
			SetSlotResolver(schema.NewSlotResolver(attrs))
		} else {
			SetSlotResolver(nil)
		}
		t.Cleanup(func() { SetSlotResolver(prev) })
		for seed := int64(1); seed <= 12; seed++ {
			rng := rand.New(rand.NewSource(seed))
			rows := genPresenceLogRows(rng, 40+rng.Intn(120))
			byHour := map[string][]schema.LogRow{}
			for _, r := range rows {
				p := partitionFromNano(r.TimestampUnixNano)
				byHour[p] = append(byHour[p], r)
			}
			var batches [][]schema.LogRow
			var facets []*fieldpresence.Facet
			for _, b := range byHour {
				res, err := writeLogsParquet(b, 50, 3)
				if err != nil {
					t.Fatal(err)
				}
				f, overCap := facetOfParquet(t, res.Data)
				if f == nil || overCap {
					t.Fatalf("seed %d: written object has no facet (overCap=%v)", seed, overCap)
				}
				facets = append(facets, f)
				batches = append(batches, b)
			}
			s := fvcStorage(t, batches...)
			lo := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano()
			got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, "*", lo, lo+int64(72*time.Hour)))
			if err != nil {
				t.Fatal(err)
			}
			if d := diffHits(namesToHits(got), presenceExpected(facets)); d != "" {
				t.Errorf("seed %d: facet and field_names differ: %s", seed, d)
			}
		}
	}
}

// presenceStreamFilter is the LogsQL filter selecting one stream's rows (only
// streams with a hex _stream_id can be selected by one).
func presenceStreamFilter(k streamDayKey) (string, bool) {
	if k.kind == fieldpresence.KindID24 {
		return fmt.Sprintf(`_stream_id:%x`, k.key), true
	}
	return "", false
}

func TestFieldPresence_MatchesTheReadPathPerStreamDay(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		rng := rand.New(rand.NewSource(seed))
		rows := genPresenceLogRows(rng, 40+rng.Intn(80))
		byHour := map[string][]schema.LogRow{}
		for _, r := range rows {
			p := partitionFromNano(r.TimestampUnixNano)
			byHour[p] = append(byHour[p], r)
		}
		var batches [][]schema.LogRow
		var facets []*fieldpresence.Facet
		for _, b := range byHour {
			res, err := writeLogsParquet(b, 50, 3)
			if err != nil {
				t.Fatal(err)
			}
			f, _ := facetOfParquet(t, res.Data)
			facets = append(facets, f)
			batches = append(batches, b)
		}
		s := fvcStorage(t, batches...)
		for k, cell := range presenceFold(facets) {
			flt, ok := presenceStreamFilter(k)
			if !ok {
				continue
			}
			lo := int64(k.day) * int64(24*time.Hour)
			got, err := s.GetFieldNames(context.Background(), nil, mustParseQueryWithTime(t, flt, lo, lo+int64(24*time.Hour)-1))
			if err != nil {
				t.Fatal(err)
			}
			m := namesToHits(got)
			var gotNames, wantNames []string
			for n := range m {
				gotNames = append(gotNames, n)
			}
			for n := range cell.names {
				wantNames = append(wantNames, n)
			}
			sort.Strings(gotNames)
			sort.Strings(wantNames)
			if strings.Join(gotNames, ",") != strings.Join(wantNames, ",") || m["_time"] != cell.rows {
				t.Errorf("seed %d stream-day %d/%c/%x: read path lists %v with %d rows, facet %v with %d rows", seed, k.day, k.kind, k.key, gotNames, m["_time"], wantNames, cell.rows)
			}
		}
	}
}

func pickHostile(rng *rand.Rand) string {
	k := presenceHostileKeys()
	return k[rng.Intn(len(k))]
}
