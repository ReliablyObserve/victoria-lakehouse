package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

const testKey = "logs/_meta/tenant-aliases.json"

// fastBackoff keeps retry tests quick.
const fastBackoff = 50 * time.Microsecond

func newTestRegistry(t testing.TB, pool ConditionalPool, rng AutoRange, data func() []uint32) (*TenantResolver, *Registry) {
	t.Helper()
	r := NewResolver(ResolverConfig{AutoRegister: true})
	g, err := NewRegistry(r, RegistryConfig{Pool: pool, Key: testKey, Range: rng, DataTenants: data, Backoff: fastBackoff, MaxAttempts: 64})
	if err != nil {
		t.Fatal(err)
	}
	return r, g
}

func persisted(t testing.TB, pool *MemConditionalPool) []AliasEntry {
	t.Helper()
	data, ok := pool.Get(testKey)
	if !ok {
		return nil
	}
	var out []AliasEntry
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAutoRange_Validate(t *testing.T) {
	cases := []struct {
		name    string
		r       AutoRange
		wantErr bool
	}{
		{"default", DefaultAutoRange(), false},
		{"single id", AutoRange{Min: 5, Max: 5}, false},
		{"min above max", AutoRange{Min: 10, Max: 5}, true},
		{"zero min", AutoRange{Min: 0, Max: 5}, true},
		{"max is the null account", AutoRange{Min: 5, Max: NullAccountID}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.r.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	d := DefaultAutoRange()
	if d.Min != 2147483648 || d.Max != 4294967294 {
		t.Fatalf("default range = %+v", d)
	}
}

func TestAllocate_NeverReturnsAnExistingID(t *testing.T) {
	pool := NewMemConditionalPool()
	rng := AutoRange{Min: 100, Max: 130}
	// Persisted by another pod, and held by int tenants with data.
	seed, _ := json.Marshal([]AliasEntry{{OrgID: "persisted-a", AccountID: 100, Source: SourceAuto}, {OrgID: "persisted-b", AccountID: 101, Source: SourceAuto}})
	pool.Put(testKey, seed)
	data := []uint32{102, 103, 0, 7}
	r, g := newTestRegistry(t, pool, rng, func() []uint32 { return data })
	// A configured alias that (mis)sits inside the range is skipped too.
	if err := r.AddAliasFrom("cfg", TenantID{AccountID: 104}, SourceConfig); err != nil {
		t.Fatal(err)
	}

	taken := map[uint32]string{100: "persisted-a", 101: "persisted-b", 102: "data", 103: "data", 104: "cfg"}
	got := map[uint32]string{}
	for i := 0; i < 20; i++ {
		org := fmt.Sprintf("new-%d", i)
		tid, err := g.Allocate(context.Background(), org)
		if err != nil {
			t.Fatal(err)
		}
		if tid.ProjectID != 0 || !rng.Contains(tid.AccountID) {
			t.Fatalf("%s -> %+v outside range / project != 0", org, tid)
		}
		if owner, clash := taken[tid.AccountID]; clash {
			t.Fatalf("%s got %d which belongs to %s", org, tid.AccountID, owner)
		}
		if other, dup := got[tid.AccountID]; dup {
			t.Fatalf("%s and %s both got %d", org, other, tid.AccountID)
		}
		got[tid.AccountID] = org
	}
	// The persisted aliases are adopted, not overwritten.
	if tid, ok := r.Resolve("persisted-a"); !ok || tid.AccountID != 100 {
		t.Fatalf("persisted-a = %+v %v", tid, ok)
	}
	if name := r.DisplayName(100, 0); name != "persisted-a" {
		t.Fatalf("display name of 100 = %q", name)
	}
	if n := len(persisted(t, pool)); n != 22 {
		t.Fatalf("registry holds %d entries, want 22", n)
	}
}

func TestAllocate_SameOrgIDTwiceIsStable(t *testing.T) {
	_, g := newTestRegistry(t, NewMemConditionalPool(), DefaultAutoRange(), nil)
	a, err := g.Allocate(context.Background(), "same")
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Allocate(context.Background(), "same")
	if err != nil || a != b {
		t.Fatalf("second allocate = %+v, %v; want %+v", b, err, a)
	}
	if a.AccountID != DefaultAutoRegisterMinID {
		t.Fatalf("first ID = %d, want range start", a.AccountID)
	}
}

func TestAllocate_RangeExhausted(t *testing.T) {
	_, g := newTestRegistry(t, NewMemConditionalPool(), AutoRange{Min: 10, Max: 11}, nil)
	for _, org := range []string{"a", "b"} {
		if _, err := g.Allocate(context.Background(), org); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.Allocate(context.Background(), "c"); !errors.Is(err, ErrRangeExhausted) {
		t.Fatalf("err = %v, want ErrRangeExhausted", err)
	}
}

func TestAllocate_ReusesLowestGapOnlyAtTheTop(t *testing.T) {
	pool := NewMemConditionalPool()
	seed, _ := json.Marshal([]AliasEntry{{OrgID: "x", AccountID: 10}, {OrgID: "z", AccountID: 12}})
	pool.Put(testKey, seed)
	_, g := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 13}, nil)
	a, _ := g.Allocate(context.Background(), "p") // above the top: 13
	b, _ := g.Allocate(context.Background(), "q") // top reached: lowest gap, 11
	if a.AccountID != 13 || b.AccountID != 11 {
		t.Fatalf("got %d then %d, want 13 then 11", a.AccountID, b.AccountID)
	}
}

func TestRestart_PersistedAliasesLoadedAndNewAllocationsDoNotCollide(t *testing.T) {
	pool := NewMemConditionalPool()
	rng := AutoRange{Min: 1000, Max: 2000}

	_, g1 := newTestRegistry(t, pool, rng, nil)
	first := map[string]TenantID{}
	for _, org := range []string{"one", "two", "three"} {
		tid, err := g1.Allocate(context.Background(), org)
		if err != nil {
			t.Fatal(err)
		}
		first[org] = tid
	}

	// "Restart": a fresh process has an empty resolver and the old counter is gone.
	r2, g2 := newTestRegistry(t, pool, rng, nil)
	if err := g2.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	for org, want := range first {
		if got, ok := r2.Resolve(org); !ok || got != want {
			t.Fatalf("after restart %s = %+v %v, want %+v", org, got, ok, want)
		}
	}
	used := map[uint32]bool{}
	for _, tid := range first {
		used[tid.AccountID] = true
	}
	for _, org := range []string{"four", "five"} {
		tid, err := g2.Allocate(context.Background(), org)
		if err != nil {
			t.Fatal(err)
		}
		if used[tid.AccountID] {
			t.Fatalf("%s reused %d after restart", org, tid.AccountID)
		}
		used[tid.AccountID] = true
	}
	// Even without a Sync, the allocation itself reads the registry first.
	_, g3 := newTestRegistry(t, pool, rng, nil)
	tid, err := g3.Allocate(context.Background(), "six")
	if err != nil {
		t.Fatal(err)
	}
	if used[tid.AccountID] {
		t.Fatalf("six reused %d without a sync", tid.AccountID)
	}
}

func TestTwoPods_SixteenWayRace_UniqueIDs(t *testing.T) {
	pool := NewMemConditionalPool()
	rng := AutoRange{Min: 5000, Max: 6000}
	const n = 16
	regs := make([]*Registry, n)
	for i := range regs {
		_, regs[i] = newTestRegistry(t, pool, rng, nil)
	}
	before := metrics.TenantAllocConflictsTotal.Get()

	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make([]TenantID, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], errs[i] = regs[i].Allocate(context.Background(), fmt.Sprintf("tenant-%02d", i))
		}(i)
	}
	close(start)
	wg.Wait()

	seen := map[uint32]int{}
	for i := range ids {
		if errs[i] != nil {
			t.Fatalf("pod %d: %v", i, errs[i])
		}
		if j, dup := seen[ids[i].AccountID]; dup {
			t.Fatalf("pods %d and %d both got %d", j, i, ids[i].AccountID)
		}
		seen[ids[i].AccountID] = i
	}
	if got := len(persisted(t, pool)); got != n {
		t.Fatalf("registry holds %d entries, want %d", got, n)
	}
	if metrics.TenantAllocConflictsTotal.Get() == before {
		t.Log("no 412 observed in this run (scheduling); the retry path has its own test")
	}
}

func TestTwoPods_SameOrgIDRaceConverges(t *testing.T) {
	pool := NewMemConditionalPool()
	_, a := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 99}, nil)
	_, b := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 99}, nil)
	var wg sync.WaitGroup
	res := make([]TenantID, 2)
	for i, g := range []*Registry{a, b} {
		wg.Add(1)
		go func(i int, g *Registry) {
			defer wg.Done()
			var err error
			if res[i], err = g.Allocate(context.Background(), "shared-name"); err != nil {
				t.Error(err)
			}
		}(i, g)
	}
	wg.Wait()
	if res[0] != res[1] {
		t.Fatalf("pods disagree on shared-name: %+v vs %+v", res[0], res[1])
	}
	if n := len(persisted(t, pool)); n != 1 {
		t.Fatalf("registry holds %d entries, want 1", n)
	}
}

func TestTwoPods_ManyGoroutinesPerPod(t *testing.T) {
	pool := NewMemConditionalPool()
	rng := AutoRange{Min: 1, Max: 10000}
	_, a := newTestRegistry(t, pool, rng, nil)
	_, b := newTestRegistry(t, pool, rng, nil)
	var wg sync.WaitGroup
	var mu sync.Mutex
	owner := map[uint32]string{}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g := a
			if i%2 == 1 {
				g = b
			}
			org := fmt.Sprintf("org-%d", i)
			tid, err := g.Allocate(context.Background(), org)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if prev, dup := owner[tid.AccountID]; dup {
				t.Errorf("%s and %s share %d", prev, org, tid.AccountID)
			}
			owner[tid.AccountID] = org
		}(i)
	}
	wg.Wait()
	if len(owner) != 16 {
		t.Fatalf("%d distinct IDs, want 16", len(owner))
	}
}

func TestAllocate_RetriesOn412ThenSucceeds(t *testing.T) {
	pool := NewMemConditionalPool()
	var fails atomic.Int32
	pool.BeforeWrite = func(string) error {
		if fails.Add(1) <= 3 {
			return fmt.Errorf("%w: simulated", ErrPreconditionFailed)
		}
		return nil
	}
	_, g := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 20}, nil)
	before := metrics.TenantAllocConflictsTotal.Get()
	tid, err := g.Allocate(context.Background(), "retry-me")
	if err != nil {
		t.Fatal(err)
	}
	if tid.AccountID != 10 {
		t.Fatalf("got %d", tid.AccountID)
	}
	if d := metrics.TenantAllocConflictsTotal.Get() - before; d != 3 {
		t.Fatalf("conflicts counted = %d, want 3", d)
	}
}

func TestAllocate_GivesUpAfterTheBound(t *testing.T) {
	pool := NewMemConditionalPool()
	var writes atomic.Int32
	pool.BeforeWrite = func(string) error {
		writes.Add(1)
		return fmt.Errorf("%w: always", ErrPreconditionFailed)
	}
	r := NewResolver(ResolverConfig{AutoRegister: true})
	g, err := NewRegistry(r, RegistryConfig{Pool: pool, Key: testKey, Range: AutoRange{Min: 10, Max: 20}, MaxAttempts: 4, Backoff: fastBackoff})
	if err != nil {
		t.Fatal(err)
	}
	failed := metrics.TenantAllocFailedTotal.Get()
	_, err = g.Allocate(context.Background(), "doomed")
	if !errors.Is(err, ErrAllocationConflict) {
		t.Fatalf("err = %v, want ErrAllocationConflict", err)
	}
	if writes.Load() != 4 {
		t.Fatalf("attempted %d writes, want exactly the bound 4", writes.Load())
	}
	if _, ok := r.Resolve("doomed"); ok {
		t.Fatal("a failed allocation must not leave the tenant registered")
	}
	if metrics.TenantAllocFailedTotal.Get() != failed+1 {
		t.Fatal("failed allocation not counted")
	}
}

func TestAllocate_S3DownIsUnavailableNotAnID(t *testing.T) {
	pool := NewMemConditionalPool()
	pool.BeforeRead = func(string) error { return errors.New("connection refused") }
	_, g := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 20}, nil)
	if _, err := g.Allocate(context.Background(), "x"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestAllocate_CorruptRegistryIsNeverOverwritten(t *testing.T) {
	pool := NewMemConditionalPool()
	pool.Put(testKey, []byte("not-json["))
	_, g := newTestRegistry(t, pool, AutoRange{Min: 10, Max: 20}, nil)
	if _, err := g.Allocate(context.Background(), "x"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if data, _ := pool.Get(testKey); string(data) != "not-json[" {
		t.Fatalf("corrupt object was overwritten: %q", data)
	}
}

// The live reproduction: acme-corp and staging-team are configured aliases for
// 1001 and 1002, and the persisted file also maps other names onto those IDs.
func TestSync_ConflictingPersistedEntriesAreRefused(t *testing.T) {
	pool := NewMemConditionalPool()
	seed, _ := json.Marshal([]AliasEntry{
		{OrgID: "acme-corp", AccountID: 1001, Source: SourceAuto},
		{OrgID: "new-team-xyz", AccountID: 1001, Source: SourceAuto},
		{OrgID: "staging-team", AccountID: 1002, Source: SourceAuto},
		{OrgID: "does-not-exist-123", AccountID: 1002, Source: SourceAuto},
		{OrgID: "zzz", AccountID: 1003, Source: SourceAuto},
		{OrgID: "dup-a", AccountID: 3000, Source: SourceAuto},
		{OrgID: "dup-b", AccountID: 3000, Source: SourceAuto},
		{OrgID: "two-ids", AccountID: 4000, Source: SourceAuto},
		{OrgID: "two-ids", AccountID: 4001, Source: SourceAuto},
	})
	pool.Put(testKey, seed)
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	for org, id := range map[string]uint32{"acme-corp": 1001, "staging-team": 1002} {
		if err := r.AddAliasFrom(org, TenantID{AccountID: id}, SourceConfig); err != nil {
			t.Fatal(err)
		}
	}
	rejected := metrics.TenantAliasRejectedTotal.Get("registry")
	if err := g.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := metrics.TenantAliasRejectedTotal.Get("registry") - rejected; d < 6 {
		t.Fatalf("rejected counted = %d, want >= 6", d)
	}

	for org, id := range map[string]uint32{"acme-corp": 1001, "staging-team": 1002, "zzz": 1003} {
		if tid, ok := r.Resolve(org); !ok || tid.AccountID != id {
			t.Fatalf("%s = %+v %v, want %d", org, tid, ok, id)
		}
	}
	for _, org := range []string{"new-team-xyz", "does-not-exist-123", "dup-a", "dup-b", "two-ids"} {
		if _, ok := r.Resolve(org); ok {
			t.Fatalf("conflicting entry %q was kept", org)
		}
	}
	if got := r.DisplayName(1001, 0); got != "acme-corp" {
		t.Fatalf("display name of 1001 = %q", got)
	}
	// The object was rewritten without the refused entries.
	var orgs []string
	for _, e := range persisted(t, pool) {
		orgs = append(orgs, e.OrgID)
	}
	sort.Strings(orgs)
	if strings.Join(orgs, ",") != "acme-corp,staging-team,zzz" {
		t.Fatalf("persisted orgs = %v", orgs)
	}
}

func TestSync_MergesConfigAliasesIntoAnEmptyRegistry(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	_ = r.AddAliasFrom("cfg", TenantID{AccountID: 7, ProjectID: 2}, SourceConfig)
	if err := g.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := persisted(t, pool)
	if len(got) != 1 || got[0].OrgID != "cfg" || got[0].ProjectID != 2 {
		t.Fatalf("persisted = %+v", got)
	}
	// Nothing to change: a second sync does not write.
	var writes atomic.Int32
	pool.BeforeWrite = func(string) error { writes.Add(1); return nil }
	if err := g.Sync(context.Background()); err != nil || writes.Load() != 0 {
		t.Fatalf("idle sync: err=%v writes=%d", err, writes.Load())
	}
}

func TestSync_NoRegistryObjectNoConfigWritesNothing(t *testing.T) {
	pool := NewMemConditionalPool()
	_, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	if err := g.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := pool.Get(testKey); ok {
		t.Fatal("an empty sync created the object")
	}
}

func TestSync_AdoptsEntriesOtherPodsRegistered(t *testing.T) {
	pool := NewMemConditionalPool()
	_, a := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	rb, b := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	tid, err := a.Allocate(context.Background(), "from-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rb.Resolve("from-a"); ok {
		t.Fatal("pod b knew the alias before any refresh")
	}
	b.RefreshIfStale(context.Background(), 0)
	if got, ok := rb.Resolve("from-a"); !ok || got != tid {
		t.Fatalf("after refresh: %+v %v", got, ok)
	}
}

func TestAddAlias_ConflictRejectedAndReverseMapIntact(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	if err := r.AddAlias("acme-corp", TenantID{AccountID: 1001}); err != nil {
		t.Fatal(err)
	}
	// Another OrgID on the same ID.
	err := r.AddAlias("new-team-xyz", TenantID{AccountID: 1001})
	if !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("err = %v, want conflict", err)
	}
	var ce *AliasConflictError
	if !errors.As(err, &ce) || ce.ExistingOrgID != "acme-corp" {
		t.Fatalf("conflict detail = %+v", ce)
	}
	// The same OrgID on another ID.
	if err := r.AddAlias("acme-corp", TenantID{AccountID: 1002}); !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("err = %v, want conflict", err)
	}
	// The null account.
	if err := r.AddAlias("nobody", TenantID{AccountID: NullAccountID}); !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("err = %v, want conflict", err)
	}
	if got := r.DisplayName(1001, 0); got != "acme-corp" {
		t.Fatalf("display name of 1001 = %q, reverse map was overwritten", got)
	}
	if got := r.MetricLabel(1001, 0); got != "1001:0" {
		t.Fatalf("metric label = %q", got)
	}
	if _, ok := r.Resolve("new-team-xyz"); ok {
		t.Fatal("rejected alias became resolvable")
	}
	if tid, _ := r.Resolve("acme-corp"); tid.AccountID != 1001 {
		t.Fatalf("acme-corp moved to %+v", tid)
	}
	// An identical re-add is idempotent.
	if err := r.AddAlias("acme-corp", TenantID{AccountID: 1001}); err != nil {
		t.Fatalf("idempotent add: %v", err)
	}
}

func TestSyncHandler_ConflictRejectedCountedAndNeverOverwrites(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAliasFrom("acme-corp", TenantID{AccountID: 1001}, SourceConfig)
	sh := NewSyncHandler(r, "")
	before := metrics.TenantAliasRejectedTotal.Get("sync")
	body, _ := json.Marshal(AliasDelta{NodeID: "peer", Aliases: []AliasEntry{
		{OrgID: "new-team-xyz", AccountID: 1001, Source: SourceAuto},
		{OrgID: "acme-corp", AccountID: 1777, Source: SourceAuto},
		{OrgID: "fine", AccountID: 1500, Source: SourceAuto},
	}})
	rr := httptest.NewRecorder()
	sh.ServeHTTP(rr, httptest.NewRequest("POST", "/internal/tenant/sync", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	if d := metrics.TenantAliasRejectedTotal.Get("sync") - before; d != 2 {
		t.Fatalf("rejected counted = %d, want 2", d)
	}
	if got := r.DisplayName(1001, 0); got != "acme-corp" {
		t.Fatalf("reverse map overwritten: %q", got)
	}
	if _, ok := r.Resolve("fine"); !ok {
		t.Fatal("a non-conflicting entry of the same delta was lost")
	}
}

func TestAdminAPI_ConflictIs409(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	mux := http.NewServeMux()
	NewHandler(r, nil, "").WithRegistry(g).Register(mux)

	post := func(org string, acc uint32) *httptest.ResponseRecorder {
		b, _ := json.Marshal(AliasEntry{OrgID: org, AccountID: acc})
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b)))
		return rr
	}
	if rr := post("acme-corp", 1001); rr.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rr.Code, rr.Body)
	}
	before := metrics.TenantAliasRejectedTotal.Get("admin")
	for name, rr := range map[string]*httptest.ResponseRecorder{
		"same id, other org": post("new-team-xyz", 1001),
		"same org, other id": post("acme-corp", 1002),
		"inside auto range":  post("squatter", DefaultAutoRegisterMinID),
		"the null account":   post("nobody", NullAccountID),
	} {
		if rr.Code != http.StatusConflict {
			t.Fatalf("%s: status %d, want 409 (%s)", name, rr.Code, rr.Body)
		}
	}
	if d := metrics.TenantAliasRejectedTotal.Get("admin") - before; d != 4 {
		t.Fatalf("admin rejections counted = %d, want 4", d)
	}
	if got := r.DisplayName(1001, 0); got != "acme-corp" {
		t.Fatalf("reverse map = %q", got)
	}
	if n := len(persisted(t, pool)); n != 1 {
		t.Fatalf("registry has %d entries, want 1", n)
	}

	// A conflict only visible in the shared registry (another pod's alias).
	other, _ := json.Marshal([]AliasEntry{{OrgID: "acme-corp", AccountID: 1001, Source: SourceAPI}, {OrgID: "podb-alias", AccountID: 1200, Source: SourceAPI}})
	pool.Put(testKey, other)
	if rr := post("late", 1200); rr.Code != http.StatusConflict {
		t.Fatalf("registry-only conflict: %d %s", rr.Code, rr.Body)
	}
}

func TestAdminAPI_DeleteGoesThroughTheRegistry(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	mux := http.NewServeMux()
	NewHandler(r, nil, "").WithRegistry(g).Register(mux)
	b, _ := json.Marshal(AliasEntry{OrgID: "gone", AccountID: 55})
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b)))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases/gone", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rr.Code)
	}
	if n := len(persisted(t, pool)); n != 0 {
		t.Fatalf("registry still holds %d entries", n)
	}
	if _, ok := r.Resolve("gone"); ok {
		t.Fatal("alias still resolvable")
	}
}

func TestValidateConfiguredAliases(t *testing.T) {
	rng := AutoRange{Min: 1 << 31, Max: NullAccountID - 1}
	ok := []AliasEntry{{OrgID: "a", AccountID: 1}, {OrgID: "b", AccountID: 1, ProjectID: 1}, {OrgID: "c", AccountID: 2}}
	if err := ValidateConfiguredAliases(ok, rng); err != nil {
		t.Fatalf("valid set rejected: %v", err)
	}
	cases := map[string][]AliasEntry{
		"two orgs, one id":      {{OrgID: "a", AccountID: 1}, {OrgID: "b", AccountID: 1}},
		"one org, two ids":      {{OrgID: "a", AccountID: 1}, {OrgID: "a", AccountID: 2}},
		"inside the range":      {{OrgID: "a", AccountID: 1 << 31}},
		"range end":             {{OrgID: "a", AccountID: NullAccountID - 1}},
		"null account":          {{OrgID: "a", AccountID: NullAccountID}},
		"invalid org":           {{OrgID: "a/b", AccountID: 1}},
		"one org, two projects": {{OrgID: "a", AccountID: 1, ProjectID: 0}, {OrgID: "a", AccountID: 1, ProjectID: 1}},
	}
	for name, set := range cases {
		if err := ValidateConfiguredAliases(set, rng); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSanitizeEntries(t *testing.T) {
	in := []AliasEntry{
		{OrgID: "keep", AccountID: 1},
		{OrgID: "keep", AccountID: 1}, // identical duplicate collapses
		{OrgID: "bad/name", AccountID: 2},
		{OrgID: "null", AccountID: NullAccountID},
		{OrgID: "x", AccountID: 3},
		{OrgID: "y", AccountID: 3},
	}
	kept, dropped := SanitizeEntries(in, nil)
	if len(kept) != 1 || kept[0].OrgID != "keep" {
		t.Fatalf("kept = %+v", kept)
	}
	if len(dropped) != 4 {
		t.Fatalf("dropped = %+v", dropped)
	}
	// Same input, different slice identity: the input is untouched.
	if len(in) != 6 {
		t.Fatal("input modified")
	}
}

func TestIsPreconditionFailed(t *testing.T) {
	if !IsPreconditionFailed(fmt.Errorf("wrap: %w", ErrPreconditionFailed)) {
		t.Fatal("sentinel not recognised")
	}
	if IsPreconditionFailed(nil) || IsPreconditionFailed(errors.New("boom")) {
		t.Fatal("false positive")
	}
}

func TestMemConditionalPool_Semantics(t *testing.T) {
	p := NewMemConditionalPool()
	ctx := context.Background()
	if err := p.UploadConditional(ctx, "k", []byte("1"), "\"nope\""); !IsPreconditionFailed(err) {
		t.Fatalf("If-Match on a missing object: %v", err)
	}
	if err := p.UploadConditional(ctx, "k", []byte("1"), ""); err != nil {
		t.Fatal(err)
	}
	if err := p.UploadConditional(ctx, "k", []byte("2"), ""); !IsPreconditionFailed(err) {
		t.Fatalf("If-None-Match: * on an existing object: %v", err)
	}
	_, etag, found, _ := p.DownloadWithETag(ctx, "k")
	if !found {
		t.Fatal("not found")
	}
	if err := p.UploadConditional(ctx, "k", []byte("3"), etag); err != nil {
		t.Fatal(err)
	}
	if err := p.UploadConditional(ctx, "k", []byte("4"), etag); !IsPreconditionFailed(err) {
		t.Fatalf("stale etag accepted: %v", err)
	}
}
