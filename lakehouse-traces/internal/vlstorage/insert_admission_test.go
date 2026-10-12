package vlstorage

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// gateKey names a stream of one tenant (by AccountID) for the test gate.
type gateKey struct {
	account uint32
	stream  string
}

// countingGate denies the streams in deny (for every tenant) and in denyFor (for
// one tenant) and records every question it is asked.
type countingGate struct {
	deny      map[string]bool
	denyFor   map[gateKey]bool
	asked     []string
	askedKeys []gateKey
}

func (g *countingGate) AllowStream(account, _ uint32, stream string) bool {
	g.asked = append(g.asked, stream)
	g.askedKeys = append(g.askedKeys, gateKey{account, stream})
	return !g.deny[stream] && !g.denyFor[gateKey{account, stream}]
}

// refusalRecorder replaces the seam through which a refused stream is counted and
// logged, and records the tenant, stream and number of rows of each call.
type refusal struct {
	account, project uint32
	stream           string
	rows             int
}

func recordRefusals(t testing.TB) *[]refusal {
	var got []refusal
	prev := noteCardinalityRefused
	noteCardinalityRefused = func(account, project uint32, stream string, rows int) {
		got = append(got, refusal{account, project, stream, rows})
		prev(account, project, stream, rows)
	}
	t.Cleanup(func() { noteCardinalityRefused = prev })
	return &got
}

func cardinalityDropped() uint64 {
	return metrics.InsertRowsDropped.Get(storage.ReasonCardinalityLimit)
}

// Every row of a stream the tenant's limit refuses is counted under
// lakehouse_insert_rows_dropped_total{reason="cardinality_limit"}, per row and
// not per decision: the request is still answered as a success, so the counter
// is the only trace of rows that are not stored.
func TestVTInsertAdapter_CountsEveryRowRefusedForTheCardinalityLimit(t *testing.T) {
	gate := &countingGate{deny: map[string]bool{`{resource_attr:service.name="over"}`: true}}
	SetCardinalityGate(gate)
	t.Cleanup(func() { SetCardinalityGate(nil) })
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	add := func(app, msg string, ts int64) {
		lr.MustAdd(logstorage.TenantID{AccountID: 1, ProjectID: 2}, ts, []logstorage.Field{{Name: "resource_attr:service.name", Value: app}, {Name: "span_id", Value: msg}, {Name: "trace_id", Value: "t-" + msg}}, 1)
	}
	add("over", "o1", now)
	add("ok", "k1", now)
	add("over", "o2", now+1)
	add("over", "o3", now+2)
	add("ok", "k2", now+3)
	add("over", "before-the-epoch", -1) // refused for its _time, not for the limit

	card0, small0, total0 := cardinalityDropped(), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp), metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got, want := strings.Join(buf.got(), ","), "k1,k2"; got != want {
		t.Errorf("the buffer got %q, want %q", got, want)
	}
	if d := cardinalityDropped() - card0; d != 3 {
		t.Errorf("lakehouse_insert_rows_dropped_total{reason=cardinality_limit} rose by %d, want 3 (one per row, not per decision)", d)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - small0; d != 1 {
		t.Errorf("too_small_timestamp rose by %d, want 1", d)
	}
	if d := metrics.InsertRowsTotal.Get() - total0; d != 2 {
		t.Errorf("lakehouse_insert_rows_total rose by %d, want 2", d)
	}
	if len(gate.asked) != 2 {
		t.Errorf("the gate was asked %d times for two streams: %v", len(gate.asked), gate.asked)
	}
}

// checkAdmission builds a batch from data, runs it through the insert adapter
// with a gate that denies some streams of some tenants, and checks the result
// against the rule written out here independently of the adapter: a row is
// refused when its _time is before the epoch, else when its stream is denied for
// ITS tenant, else it reaches the buffer; the input is not changed; the counters
// move by the number of rows of each kind; every refusal is attributed to the
// tenant and stream of the rows it refused; the gate is asked once per distinct
// (tenant, stream) with a row from the epoch on and never about a row before it.
// Streams are app0..app3 and tenants 1 and 2; data[0] says which streams are
// denied (low nibble: for tenant 1, high nibble: for tenant 2), each further
// byte makes one row (low 2 bits the stream, bit 2 a _time before the epoch, bit
// 3 the smallest time, bit 4 the tenant).
func checkAdmission(t testing.TB, data []byte) {
	if len(data) == 0 || len(data) > 200 {
		return
	}
	denyMask := data[0]
	rows := data[1:]
	gate := &countingGate{denyFor: map[gateKey]bool{}}
	for i := 0; i < 4; i++ {
		if denyMask&(1<<i) != 0 {
			gate.denyFor[gateKey{1, fmt.Sprintf(`{resource_attr:service.name="app%d"}`, i)}] = true
		}
		if denyMask&(1<<(i+4)) != 0 {
			gate.denyFor[gateKey{2, fmt.Sprintf(`{resource_attr:service.name="app%d"}`, i)}] = true
		}
	}
	SetCardinalityGate(gate)
	defer SetCardinalityGate(nil)
	refusals := recordRefusals(t)
	buf := &recordingBuffer{}
	a := &vtInsertAdapter{buf: buf, dir: "/d"}

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	var want []string
	neg, denied := 0, 0
	askedWant := map[gateKey]bool{}
	deniedRows := map[gateKey]int{}
	for i, b := range rows {
		app := fmt.Sprintf("app%d", b&3)
		account := uint32(1 + (b>>4)&1)
		ts := now + int64(i)
		if b&4 != 0 {
			ts = -int64(i) - 1
			if b&8 != 0 {
				ts = math.MinInt64
			}
		}
		msg := fmt.Sprintf("m%d", i)
		lr.MustAdd(logstorage.TenantID{AccountID: account, ProjectID: 2}, ts, []logstorage.Field{{Name: "resource_attr:service.name", Value: app}, {Name: "span_id", Value: msg}, {Name: "trace_id", Value: "t-" + msg}}, 1)
		k := gateKey{account, fmt.Sprintf(`{resource_attr:service.name=%q}`, app)}
		switch {
		case ts < 0:
			neg++
		case gate.denyFor[k]:
			denied++
			deniedRows[k]++
			askedWant[k] = true
		default:
			want = append(want, msg)
			askedWant[k] = true
		}
	}
	card0, small0, total0 := cardinalityDropped(), metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp), metrics.InsertRowsTotal.Get()
	a.MustAddRows(lr)

	if got := strings.Join(buf.got(), ","); got != strings.Join(want, ",") {
		t.Fatalf("the buffer got %q, want %q (denyMask %08b, rows %v)", got, strings.Join(want, ","), denyMask, rows)
	}
	if lr.RowsCount() != len(rows) {
		t.Fatalf("the adapter changed its input: %d rows, want %d", lr.RowsCount(), len(rows))
	}
	if d := cardinalityDropped() - card0; d != uint64(denied) {
		t.Fatalf("cardinality_limit rose by %d, want %d", d, denied)
	}
	if d := metrics.InsertRowsDropped.Get(storage.ReasonTooSmallTimestamp) - small0; d != uint64(neg) {
		t.Fatalf("too_small_timestamp rose by %d, want %d", d, neg)
	}
	if d := metrics.InsertRowsTotal.Get() - total0; d != uint64(len(want)) {
		t.Fatalf("lakehouse_insert_rows_total rose by %d, want %d", d, len(want))
	}
	if len(gate.askedKeys) != len(askedWant) {
		t.Fatalf("the gate was asked %d times (%v), want once for each of the %d (tenant, stream) pairs that have a row from the epoch on", len(gate.askedKeys), gate.askedKeys, len(askedWant))
	}
	attributed := map[gateKey]int{}
	for _, r := range *refusals {
		attributed[gateKey{r.account, r.stream}] += r.rows
		if r.project != 2 {
			t.Fatalf("a refusal is attributed to project %d, want 2", r.project)
		}
	}
	if len(attributed) != len(deniedRows) {
		t.Fatalf("refusals attributed to %v, want %v", attributed, deniedRows)
	}
	for k, n := range deniedRows {
		if attributed[k] != n {
			t.Fatalf("tenant %d stream %s: %d rows attributed, want %d", k.account, k.stream, attributed[k], n)
		}
	}
}

func TestVTInsertAdapter_AdmissionRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	for iter := 0; iter < 500; iter++ {
		data := make([]byte, 1+rng.Intn(60))
		rng.Read(data)
		checkAdmission(t, data)
	}
}

// FuzzAdmittedRows drives the admission and copy loop of the insert adapter
// (admittedRows) with arbitrary batches, gates and times.
func FuzzAdmittedRows(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3})
	f.Add([]byte{0b0101, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{0b1111, 0, 1, 2, 3})
	f.Add([]byte{0, 4, 5, 6, 7, 12, 13})
	f.Add([]byte{0b0010, 4, 1, 1, 5, 9, 13, 2})
	f.Add([]byte{0b0010_0000, 0, 16, 1, 17, 2, 18}) // tenant 2 denied app1: tenant 1's rows of app1 stay
	f.Add([]byte{0b0001_0010, 1, 17, 0, 16})        // each tenant denied a different stream
	f.Fuzz(func(t *testing.T, data []byte) { checkAdmission(t, data) })
}

// Upstream's stream hash is computed from the stream tags alone, without the
// tenant, and one batch can carry several tenants (/internal/insert and the
// multitenant native endpoint). The admission decision is made for a tenant AND
// a stream: tenant 2 at its limit must not refuse the same-named stream of tenant
// 1, which has room, and tenant 1's room must not admit tenant 2's rows. The
// refusal is attributed to the tenant whose limit refused it. Both orders of the
// rows are tried, since the first row of a stream is what the decision is made on.
func TestVTInsertAdapter_SameStreamTwoTenantsOneBatch(t *testing.T) {
	for _, order := range []struct {
		name         string
		firstAccount uint32
	}{{"tenant 2 first", 2}, {"tenant 1 first", 1}} {
		t.Run(order.name, func(t *testing.T) {
			stream := `{resource_attr:service.name="api"}`
			gate := &countingGate{denyFor: map[gateKey]bool{{2, stream}: true}} // tenant 2 is at its limit
			SetCardinalityGate(gate)
			t.Cleanup(func() { SetCardinalityGate(nil) })
			refusals := recordRefusals(t)
			buf := &recordingBuffer{}
			a := &vtInsertAdapter{buf: buf, dir: "/d"}
			now := time.Now().UnixNano()
			lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
			defer logstorage.PutLogRows(lr)
			add := func(account uint32, msg string, ts int64) {
				lr.MustAdd(logstorage.TenantID{AccountID: account, ProjectID: 7}, ts, []logstorage.Field{{Name: "resource_attr:service.name", Value: "api"}, {Name: "span_id", Value: msg}, {Name: "trace_id", Value: "t-" + msg}}, 1)
			}
			second := uint32(3) - order.firstAccount
			add(order.firstAccount, fmt.Sprintf("a%d-1", order.firstAccount), now)
			add(second, fmt.Sprintf("a%d-1", second), now+1)
			add(order.firstAccount, fmt.Sprintf("a%d-2", order.firstAccount), now+2)
			add(second, fmt.Sprintf("a%d-2", second), now+3)

			card0 := cardinalityDropped()
			a.MustAddRows(lr)

			if got, want := strings.Join(buf.got(), ","), "a1-1,a1-2"; got != want {
				t.Errorf("the buffer got %q, want only tenant 1's rows %q (tenant 2 is at its limit)", got, want)
			}
			if d := cardinalityDropped() - card0; d != 2 {
				t.Errorf("cardinality_limit rose by %d, want 2 (tenant 2's two rows)", d)
			}
			if len(*refusals) != 1 || (*refusals)[0] != (refusal{2, 7, stream, 2}) {
				t.Errorf("refusals = %+v, want one: tenant 2:7, stream %s, 2 rows", *refusals, stream)
			}
			if len(gate.askedKeys) != 2 {
				t.Errorf("the gate was asked %d times (%v), want once per tenant", len(gate.askedKeys), gate.askedKeys)
			}
		})
	}
}
