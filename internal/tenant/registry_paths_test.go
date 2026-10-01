package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestNewRegistry_RejectsAnInvalidRange(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	if _, err := NewRegistry(r, RegistryConfig{Range: AutoRange{Min: 10, Max: 5}}); err == nil {
		t.Fatal("min > max accepted")
	}
	if _, err := NewRegistry(r, RegistryConfig{Range: AutoRange{Min: 5, Max: NullAccountID}}); err == nil {
		t.Fatal("a range that reaches the null account accepted")
	}
	g, err := NewRegistry(r, RegistryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if g.Range() != DefaultAutoRange() {
		t.Fatalf("default range = %+v", g.Range())
	}
}

func TestResolver_SetAllocatorNilRemovesIt(t *testing.T) {
	r, _ := newTestRegistry(t, NewMemConditionalPool(), DefaultAutoRange(), nil)
	if r.getAllocator() == nil {
		t.Fatal("NewRegistry must install itself")
	}
	r.SetAllocator(nil)
	if r.getAllocator() != nil {
		t.Fatal("allocator not removed")
	}
	// With no allocator an ingest of an unknown OrgID fails closed.
	h := r.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler reached") }))
	req := httptest.NewRequest("POST", "/insert/jsonline", nil)
	req.Header.Set("X-Scope-OrgID", "orphan")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", rr.Code)
	}
	// Reads still resolve to the null tenant without an allocator.
	var acc string
	h = r.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) { acc = req.Header.Get("AccountID") }))
	req = httptest.NewRequest("GET", "/select/logsql/query", nil)
	req.Header.Set("X-Scope-OrgID", "orphan")
	h.ServeHTTP(httptest.NewRecorder(), req)
	if acc != "4294967295" {
		t.Fatalf("read scope %q", acc)
	}
}

func TestRegister_IdempotentConflictAndRetries(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	ctx := context.Background()

	e := AliasEntry{OrgID: "team-a", AccountID: 300}
	if err := g.Register(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := g.Register(ctx, e); err != nil { // identical entry already registered
		t.Fatalf("idempotent register: %v", err)
	}
	if n := len(persisted(t, pool)); n != 1 {
		t.Fatalf("registry holds %d entries", n)
	}
	// The registry knows a mapping this pod has not adopted yet.
	other, _ := json.Marshal([]AliasEntry{{OrgID: "team-a", AccountID: 300}, {OrgID: "team-b", AccountID: 301}})
	pool.Put(testKey, other)
	if err := g.Register(ctx, AliasEntry{OrgID: "team-c", AccountID: 301}); !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("ID owned by another pod's alias: %v", err)
	}
	if err := g.Register(ctx, AliasEntry{OrgID: "team-b", AccountID: 999}); !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("OrgID owned by another pod's alias: %v", err)
	}
	if _, ok := r.Resolve("team-c"); ok {
		t.Fatal("a refused register must not touch the resolver")
	}

	// 412 twice, then success.
	var fails atomic.Int32
	pool.BeforeWrite = func(string) error {
		if fails.Add(1) <= 2 {
			return ErrPreconditionFailed
		}
		return nil
	}
	if err := g.Register(ctx, AliasEntry{OrgID: "team-d", AccountID: 302}); err != nil {
		t.Fatalf("register after two lost races: %v", err)
	}
	// Always 412: gives up.
	pool.BeforeWrite = func(string) error { return ErrPreconditionFailed }
	g.cfg.MaxAttempts = 3
	if err := g.Register(ctx, AliasEntry{OrgID: "team-e", AccountID: 303}); !errors.Is(err, ErrAllocationConflict) {
		t.Fatalf("err = %v, want ErrAllocationConflict", err)
	}
	// S3 write error.
	pool.BeforeWrite = func(string) error { return errors.New("boom") }
	if err := g.Register(ctx, AliasEntry{OrgID: "team-f", AccountID: 304}); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v, want ErrRegistryUnavailable", err)
	}
	// S3 read error.
	pool.BeforeWrite = nil
	pool.BeforeRead = func(string) error { return errors.New("boom") }
	if err := g.Register(ctx, AliasEntry{OrgID: "team-g", AccountID: 305}); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v, want ErrRegistryUnavailable", err)
	}
}

func TestRegister_CanceledWhileBackingOff(t *testing.T) {
	pool := NewMemConditionalPool()
	pool.BeforeWrite = func(string) error { return ErrPreconditionFailed }
	_, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	g.cfg.Backoff = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Register(ctx, AliasEntry{OrgID: "x", AccountID: 5}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Register err = %v, want context.Canceled", err)
	}
	if err := g.Unregister(ctx, "x"); err != nil { // nothing to remove: no write, no backoff
		t.Fatalf("Unregister of an unknown alias: %v", err)
	}
	if _, err := g.Allocate(ctx, "y"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Allocate err = %v, want context.Canceled", err)
	}
	if err := g.Sync(ctx); err != nil {
		t.Fatalf("idle Sync must not write: %v", err)
	}
}

func TestUnregister_Paths(t *testing.T) {
	pool := NewMemConditionalPool()
	_, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	ctx := context.Background()
	for i, org := range []string{"u1", "u2"} {
		if err := g.Register(ctx, AliasEntry{OrgID: org, AccountID: uint32(10 + i)}); err != nil {
			t.Fatal(err)
		}
	}
	var fails atomic.Int32
	pool.BeforeWrite = func(string) error {
		if fails.Add(1) <= 1 {
			return ErrPreconditionFailed
		}
		return nil
	}
	if err := g.Unregister(ctx, "u1"); err != nil {
		t.Fatalf("unregister after a lost race: %v", err)
	}
	if got := persisted(t, pool); len(got) != 1 || got[0].OrgID != "u2" {
		t.Fatalf("registry = %+v", got)
	}
	pool.BeforeWrite = func(string) error { return errors.New("boom") }
	if err := g.Unregister(ctx, "u2"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
	pool.BeforeWrite = func(string) error { return ErrPreconditionFailed }
	g.cfg.MaxAttempts = 2
	if err := g.Unregister(ctx, "u2"); !errors.Is(err, ErrAllocationConflict) {
		t.Fatalf("err = %v", err)
	}
	pool.BeforeRead = func(string) error { return errors.New("boom") }
	if err := g.Unregister(ctx, "u2"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestSync_RetriesAndFailures(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	_ = r.AddAliasFrom("cfg", TenantID{AccountID: 7}, SourceConfig)
	ctx := context.Background()

	var fails atomic.Int32
	pool.BeforeWrite = func(string) error {
		if fails.Add(1) <= 2 {
			return ErrPreconditionFailed
		}
		return nil
	}
	if err := g.Sync(ctx); err != nil {
		t.Fatalf("sync after two lost races: %v", err)
	}
	if n := len(persisted(t, pool)); n != 1 {
		t.Fatalf("registry holds %d entries", n)
	}

	// A new configured alias needs a write again; the write fails hard.
	_ = r.AddAliasFrom("cfg2", TenantID{AccountID: 8}, SourceConfig)
	pool.BeforeWrite = func(string) error { return errors.New("boom") }
	if err := g.Sync(ctx); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
	pool.BeforeWrite = func(string) error { return ErrPreconditionFailed }
	g.cfg.MaxAttempts = 2
	if err := g.Sync(ctx); !errors.Is(err, ErrAllocationConflict) {
		t.Fatalf("err = %v", err)
	}
	pool.BeforeRead = func(string) error { return errors.New("boom") }
	if err := g.Sync(ctx); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
}

func TestRefreshIfStale_ThrottlesAndSurvivesErrors(t *testing.T) {
	pool := NewMemConditionalPool()
	_, podA := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	rB, podB := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	ctx := context.Background()

	var reads atomic.Int32
	pool.BeforeRead = func(string) error { reads.Add(1); return nil }
	podB.RefreshIfStale(ctx, time.Hour) // first call: never refreshed, reads
	podB.RefreshIfStale(ctx, time.Hour) // throttled
	podB.RefreshIfStale(ctx, time.Hour) // throttled
	if reads.Load() != 1 {
		t.Fatalf("registry read %d times, want 1 (rate limited)", reads.Load())
	}

	if _, err := podA.Allocate(ctx, "later"); err != nil {
		t.Fatal(err)
	}
	podB.RefreshIfStale(ctx, 0)
	if _, ok := rB.Resolve("later"); !ok {
		t.Fatal("refresh did not adopt the new alias")
	}

	// A failing registry leaves the resolver as it was.
	pool.BeforeRead = func(string) error { return errors.New("boom") }
	podB.RefreshIfStale(ctx, 0)
	if _, ok := rB.Resolve("later"); !ok {
		t.Fatal("a failed refresh dropped an alias")
	}
}

func TestRun_SyncsOnATimer(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	_ = r.AddAliasFrom("cfg", TenantID{AccountID: 9}, SourceConfig)

	g.Run(nil, 0) // a zero interval starts nothing
	stop := make(chan struct{})
	g.Run(stop, 5*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for len(persisted(t, pool)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the timer never synced the configured alias into the registry")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
}

func TestAdopt_ConflictIsCountedNotApplied(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	_ = r.AddAliasFrom("local", TenantID{AccountID: 50}, SourceAPI)
	g.adopt([]AliasEntry{
		{OrgID: "intruder", AccountID: 50},
		{OrgID: "local", AccountID: 51},
		{OrgID: "fine", AccountID: 52}, // no source: adopted as synced
	})
	if got := r.DisplayName(50, 0); got != "local" {
		t.Fatalf("display name of 50 = %q", got)
	}
	if tid, ok := r.Resolve("fine"); !ok || tid.AccountID != 52 {
		t.Fatalf("fine = %+v %v", tid, ok)
	}
	var src string
	for _, e := range r.AllAliases() {
		if e.OrgID == "fine" {
			src = e.Source
		}
	}
	if src != SourceSynced {
		t.Fatalf("source = %q, want %q", src, SourceSynced)
	}
}

func TestAllocate_InvalidOrgID(t *testing.T) {
	_, g := newTestRegistry(t, NewMemConditionalPool(), DefaultAutoRange(), nil)
	if _, err := g.Allocate(context.Background(), "has/slash"); err == nil {
		t.Fatal("invalid OrgID accepted")
	}
}

func TestAllocate_WriteErrorIsUnavailable(t *testing.T) {
	pool := NewMemConditionalPool()
	pool.BeforeWrite = func(string) error { return errors.New("boom") }
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	if _, err := g.Allocate(context.Background(), "x"); !errors.Is(err, ErrRegistryUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if _, ok := r.Resolve("x"); ok {
		t.Fatal("registered despite the failed write")
	}
}

func TestAllocate_LocalConflictAfterTheWriteFailsClosed(t *testing.T) {
	// A gossiped alias took the ID between the registry read and the local add:
	// the registry holds the entry, the local view contradicts it, nothing is served.
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, AutoRange{Min: 100, Max: 110}, nil)
	pool.BeforeWrite = func(string) error {
		_ = r.AddAliasFrom("gossiped", TenantID{AccountID: 100}, SourceSynced)
		return nil
	}
	if _, err := g.Allocate(context.Background(), "me"); !errors.Is(err, ErrAliasConflict) {
		t.Fatalf("err = %v, want ErrAliasConflict", err)
	}
	if _, ok := r.Resolve("me"); ok {
		t.Fatal("served an alias that conflicts with the local view")
	}
}

func TestIsPreconditionFailed_S3ErrorShapes(t *testing.T) {
	if !IsPreconditionFailed(&smithy.GenericAPIError{Code: "PreconditionFailed"}) {
		t.Error("PreconditionFailed API error not recognised")
	}
	if !IsPreconditionFailed(&smithy.GenericAPIError{Code: "ConditionalRequestConflict"}) {
		t.Error("ConditionalRequestConflict not recognised")
	}
	if IsPreconditionFailed(&smithy.GenericAPIError{Code: "AccessDenied"}) {
		t.Error("AccessDenied misread as a lost race")
	}
	resp := &smithyhttp.Response{Response: &http.Response{StatusCode: 412}}
	if !IsPreconditionFailed(fmt.Errorf("wrap: %w", &smithyhttp.ResponseError{Response: resp, Err: errors.New("x")})) {
		t.Error("an HTTP 412 response error not recognised")
	}
	resp = &smithyhttp.Response{Response: &http.Response{StatusCode: 500}}
	if IsPreconditionFailed(&smithyhttp.ResponseError{Response: resp, Err: errors.New("x")}) {
		t.Error("an HTTP 500 misread as a lost race")
	}
}

func TestAdminAPI_RegistryFailuresMapTo503(t *testing.T) {
	pool := NewMemConditionalPool()
	r, g := newTestRegistry(t, pool, DefaultAutoRange(), nil)
	mux := http.NewServeMux()
	NewHandler(r, nil, "").WithRegistry(g).Register(mux)

	pool.BeforeRead = func(string) error { return errors.New("s3 down") }
	b, _ := json.Marshal(AliasEntry{OrgID: "t", AccountID: 5})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b)))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("create: %d %s", rr.Code, rr.Body)
	}
	if _, ok := r.Resolve("t"); ok {
		t.Fatal("alias added locally although the registry write failed")
	}
	_ = r.AddAlias("there", TenantID{AccountID: 6})
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("DELETE", "/lakehouse/api/v1/tenants/aliases/there", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("delete: %d %s", rr.Code, rr.Body)
	}
	if _, ok := r.Resolve("there"); !ok {
		t.Fatal("alias removed locally although the registry write failed")
	}
}

func TestAdminAPI_WithoutRegistryKeepsTheBlindPersister(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	var saved atomic.Int32
	mux := http.NewServeMux()
	NewHandler(r, persisterFunc(func([]AliasEntry) error { saved.Add(1); return nil }), "").Register(mux)
	b, _ := json.Marshal(AliasEntry{OrgID: "t", AccountID: 5})
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b)))
	if rr.Code != http.StatusCreated || saved.Load() != 1 {
		t.Fatalf("create: %d saved=%d", rr.Code, saved.Load())
	}
	// A conflict is still a 409 without a registry.
	b, _ = json.Marshal(AliasEntry{OrgID: "u", AccountID: 5})
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("POST", "/lakehouse/api/v1/tenants/aliases", bytes.NewReader(b)))
	if rr.Code != http.StatusConflict {
		t.Fatalf("conflict: %d", rr.Code)
	}
}

type persisterFunc func([]AliasEntry) error

func (f persisterFunc) SaveAliases(e []AliasEntry) error { return f(e) }

func TestMarshalEntries_NilIsAnEmptyArray(t *testing.T) {
	b, err := marshalEntries(nil)
	if err != nil || string(b) != "[]" {
		t.Fatalf("marshalEntries(nil) = %q, %v", b, err)
	}
}
