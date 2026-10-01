package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// The projection property: for any filter and pipe chain, the answer computed
// from the AST-derived projected columns equals the answer computed from every
// column. A dropped column makes a filter see an absent field or a pipe see an
// empty one, so any under-projection shows up as a difference.

var (
	projFixtureOnce sync.Once
	projFixtureFile *parquet.File
	errProjFixture  error
	projFixtureBase time.Time
)

// projectionFixture returns an in-memory Parquet file of spans whose rows vary
// in every column a generated query can reference.
func projectionFixture(t testing.TB) (*parquet.File, time.Time) {
	t.Helper()
	projFixtureOnce.Do(func() {
		projFixtureBase = time.Date(2026, 5, 10, 14, 0, 0, 0, time.UTC)
		var rows []schema.TraceRow
		i := 0
		for _, svc := range []string{"alpha", "beta", "gamma"} {
			for _, method := range []string{"GET", "POST"} {
				for _, route := range []string{"/cold", "/warm"} {
					for k := 0; k < 4; k++ {
						name := fmt.Sprintf("%s %s", method, route)
						if k == 3 {
							name = "needle-exact"
						}
						rows = append(rows, schema.TraceRow{
							TimestampUnixNano: projFixtureBase.Add(time.Duration(i) * time.Second).UnixNano(),
							StartTimeUnixNano: projFixtureBase.Add(time.Duration(i) * time.Second).UnixNano(),
							TraceID:           fmt.Sprintf("trace-%s-%d", svc, i%5),
							SpanID:            fmt.Sprintf("%016x", i),
							SpanName:          name,
							ServiceName:       svc,
							HostName:          "host-" + svc,
							DurationNs:        int64(1000000 * (1 + i%4)),
							StatusCode:        int32(i % 3),
							HTTPMethod:        method,
							Stream:            fmt.Sprintf(`{resource_attr:service.name=%q}`, svc),
							StreamID:          fmt.Sprintf("%048x", len(svc)),
							SpanAttributes:    map[string]string{"http.route": route, "trace_state": "state-" + route[1:]},
						})
						i++
					}
				}
			}
		}
		res, err := writeTracesParquet(rows, 17, 3) // several row groups
		if err != nil {
			errProjFixture = err
			return
		}
		f, err := parquet.OpenFile(bytes.NewReader(res.Data), int64(len(res.Data)))
		if err != nil {
			errProjFixture = err
			return
		}
		projFixtureFile = f
	})
	if errProjFixture != nil {
		t.Fatalf("projection fixture: %v", errProjFixture)
	}
	return projFixtureFile, projFixtureBase
}

// projectionStorage is a storage configured as the traces binary runs it: the
// traces column registry and traces mode.
func projectionStorage() *Storage {
	s := testStorage()
	s.cfg.Mode = config.ModeTraces
	s.registry = schema.NewRegistry(schema.TracesProfile)
	return s
}

// runProjected evaluates query over the fixture file with the given column
// projection (nil = every column), applying the real row filter and the real
// pipe machinery, and returns the canonical answer.
func runProjectedQuery(t testing.TB, s *Storage, f *parquet.File, base time.Time, query string, project bool) (string, bool) {
	q, err := logstorage.ParseQueryAtTimestamp(query, base.Add(10*time.Minute).UnixNano())
	if err != nil {
		return "", false
	}
	if logstorage.QueryHasFilterSubqueries(q) {
		return "", false
	}
	var cols map[string]bool
	if project {
		cols = neededColumns(s.registry, logstorage.GetQueryNeededFields(q))
	}
	startNs, endNs := q.GetFilterTimeRange()
	filter := parseFilterFromQuery(q)
	aliases := queryParquetNameAliases(q.String(), s.registry, logstorage.GetQueryPipeFields(q))
	search := func(wb logstorage.WriteDataBlockFunc) error {
		for _, rg := range f.RowGroups() {
			err := s.readOneRowGroup(f, rg, startNs, endNs, cols, nil, func(w uint, db *logstorage.DataBlock) {
				if db = filterDataBlock(db, filter); db != nil && db.RowsCount() > 0 {
					wb(w, db)
				}
			}, nil, aliases)
			if err != nil {
				return err
			}
		}
		return nil
	}
	qctx := logstorage.NewQueryContext(context.Background(), &logstorage.QueryStats{}, nil, q, false, nil)
	var mu sync.Mutex
	var out []map[string]string
	// A block is only valid inside the callback (pipes reuse its memory), so
	// copy the rows out here.
	err = logstorage.RunQueryExternal(qctx, search, func(_ uint, db *logstorage.DataBlock) {
		rows := blockRowFields([]*logstorage.DataBlock{db})
		mu.Lock()
		out = append(out, rows...)
		mu.Unlock()
	})
	if err != nil {
		return "", false
	}
	return canonRows(out), true
}

func canonRows(rows []map[string]string) string {
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			fmt.Fprintf(&sb, "%s=%s;", k, r[k])
		}
		lines = append(lines, sb.String())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

var (
	projFilterAtoms = []string{
		`name:="needle-exact"`, `name:GET*`, `name:~"GET /(cold|warm)"`, `"needle-exact"`,
		`trace_id:="trace-alpha-1"`, `trace_id:trace-beta*`, `status_code:=2`, `duration:>2000000`,
		`"resource_attr:service.name":=alpha`, `service.name:=beta`, `"span_attr:http.method":=GET`,
		`"span_attr:http.route":="/cold"`, `trace_state:="state-warm"`, `"resource_attr:host.name":=host-gamma`,
		`_time:30m`, `_time:[2026-05-10T14:00:00Z, 2026-05-10T14:00:30Z]`,
		`{resource_attr:service.name="beta"}`, `_stream:{resource_attr:service.name="gamma"}`, `*`,
	}
	projFields = []string{
		"name", "trace_id", "status_code", "duration", `"resource_attr:service.name"`,
		`"span_attr:http.method"`, `"span_attr:http.route"`, "trace_state", "_stream", "span_id", "c", "m", "who",
	}
)

type projGen struct{ pick func(n int) int }

func (g projGen) atom() string  { return projFilterAtoms[g.pick(len(projFilterAtoms))] }
func (g projGen) field() string { return projFields[g.pick(len(projFields))] }

func (g projGen) filter() string {
	n := 1 + g.pick(3)
	var sb strings.Builder
	for i := 0; i < n; i++ {
		a := g.atom()
		if i > 0 {
			sb.WriteString([]string{" ", " ", " OR ", " AND "}[g.pick(4)])
		}
		switch g.pick(6) {
		case 0:
			sb.WriteString("NOT " + a)
		case 1:
			sb.WriteString("(" + a + ")")
		default:
			sb.WriteString(a)
		}
	}
	return sb.String()
}

func (g projGen) pipe() string {
	f, f2 := g.field(), g.field()
	switch g.pick(18) {
	case 0:
		return `stats count() as n`
	case 1:
		return fmt.Sprintf(`stats by (%s) count() as n`, f)
	case 2:
		return fmt.Sprintf(`stats by (%s, %s) count() as n`, f, f2)
	case 3:
		return fmt.Sprintf(`stats sum(%s) as n`, "duration")
	case 4:
		return fmt.Sprintf(`stats count_uniq(%s) as n`, f)
	case 5:
		return fmt.Sprintf(`uniq by (%s)`, f)
	case 6:
		return fmt.Sprintf(`top 100 (%s)`, f)
	case 7:
		return fmt.Sprintf(`fields %s, %s`, f, f2)
	case 8:
		return `filter ` + g.atom()
	case 9:
		return `extract "<who> <k>" from name`
	case 10:
		return fmt.Sprintf(`format "<%s>-<%s>" as c`, f, f2)
	case 11:
		return `math duration / 1000 as m`
	case 12:
		return `sort by (_time, span_id)`
	case 13:
		return fmt.Sprintf(`rename %s as %s`, f, f2)
	case 14:
		return fmt.Sprintf(`copy %s as c`, f)
	case 15:
		return `unpack_json from name`
	case 16:
		return fmt.Sprintf(`delete %s`, f)
	default:
		return fmt.Sprintf(`stats by (%s) count() if (%s) as n`, f, g.atom())
	}
}

func (g projGen) query() string {
	q := g.filter()
	for i, n := 0, g.pick(4); i < n; i++ {
		q += " | " + g.pipe()
	}
	return q
}

func checkProjectionEquivalence(t testing.TB, s *Storage, f *parquet.File, base time.Time, query string) {
	t.Helper()
	want, ok := runProjectedQuery(t, s, f, base, query, false)
	if !ok {
		return // not a runnable query
	}
	got, ok := runProjectedQuery(t, s, f, base, query, true)
	if !ok {
		t.Fatalf("query %q runs with every column but fails with the projection", query)
	}
	if got != want {
		t.Fatalf("projection changed the answer of %q\n  projected:\n%s\n  all columns:\n%s", query, got, want)
	}
}

// TestProjectionEquivalence_Random checks the property over a fixed-seed sample
// of generated filter + pipe chains, so the regular test run (not only the fuzz
// job) covers it, plus the shapes issue #273 reported.
func TestProjectionEquivalence_Random(t *testing.T) {
	f, base := projectionFixture(t)
	s := projectionStorage()

	for _, q := range []string{
		`name:GET* | stats count() as n`,
		`_time:30m name:="needle-exact" | stats count() as n`,
		`name:="needle-exact" status_code:=2 | stats by ("resource_attr:service.name") count() as n`,
		`{resource_attr:service.name="beta"} _time:30m name:="needle-exact" | stats by (trace_state) count() as n`,
		`trace_state:="state-cold" | stats by ("span_attr:http.route") count() as n`,
	} {
		checkProjectionEquivalence(t, s, f, base, q)
	}

	rng := rand.New(rand.NewSource(273))
	g := projGen{pick: rng.Intn}
	runnable := 0
	for i := 0; i < 250; i++ {
		query := g.query()
		if _, ok := runProjectedQuery(t, s, f, base, query, false); ok {
			runnable++
		}
		checkProjectionEquivalence(t, s, f, base, query)
	}
	if runnable < 120 {
		t.Fatalf("only %d of 250 generated queries were runnable; the generator no longer exercises the property", runnable)
	}
}

// FuzzProjectionEquivalence drives the same generator from fuzz input.
func FuzzProjectionEquivalence(f *testing.F) {
	for _, seed := range [][]byte{
		{0}, {1, 2, 3, 4, 5, 6, 7, 8}, {4, 0, 1, 0, 4, 1, 2, 0}, {9, 9, 9, 9, 9, 9},
		{5, 3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5}, {200, 100, 50, 25, 12, 6, 3, 1},
		[]byte("A&91"), // math + rename over a time-range filter: found block-memory aliasing in the harness
	} {
		f.Add(seed)
	}
	file, base := projectionFixture(f)
	s := projectionStorage()
	f.Fuzz(func(t *testing.T, data []byte) {
		pos := 0
		g := projGen{pick: func(n int) int {
			if len(data) == 0 {
				return 0
			}
			b := int(data[pos%len(data)])
			pos++
			return b % n
		}}
		checkProjectionEquivalence(t, s, file, base, g.query())
	})
}
