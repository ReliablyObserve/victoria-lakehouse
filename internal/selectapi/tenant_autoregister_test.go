package selectapi

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// atRow is one ingested log line.
type atRow struct{ msg, traceID string }

// atStore is a tenant-faithful in-memory store: like the real one it answers a
// request from exactly the tenants the request names (0:0 when none) and never
// from any other tenant's rows.
type atStore struct {
	mockStore
	mu   sync.Mutex
	rows map[logstorage.TenantID][]atRow
}

func (s *atStore) add(t logstorage.TenantID, r atRow) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rows == nil {
		s.rows = map[logstorage.TenantID][]atRow{}
	}
	s.rows[t] = append(s.rows[t], r)
}

func (s *atStore) RunQuery(_ context.Context, ids []logstorage.TenantID, _ *logstorage.Query, write logstorage.WriteDataBlockFunc) error {
	if len(ids) == 0 {
		ids = []logstorage.TenantID{{}}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		rows := s.rows[id]
		if len(rows) == 0 {
			continue
		}
		ts, msgs, traces := make([]string, len(rows)), make([]string, len(rows)), make([]string, len(rows))
		for i, r := range rows {
			ts[i] = time.Now().UTC().Format(time.RFC3339Nano)
			msgs[i], traces[i] = r.msg, r.traceID
		}
		var db logstorage.DataBlock
		db.SetColumns([]logstorage.BlockColumn{
			{Name: "_time", Values: ts},
			{Name: "_msg", Values: msgs},
			{Name: "trace_id", Values: traces},
		})
		write(0, &db)
	}
	return nil
}

// atServer is resolver.Middleware in front of the real select handlers and a
// stand-in for the insert endpoint, which stores a posted line under the tenant
// the middleware resolved, exactly as the real insert path does.
func atServer(t *testing.T, resolver *tenant.TenantResolver, store *atStore) *httptest.Server {
	t.Helper()
	internalvlstorage.SetStorage(store, nil)
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	mux := http.NewServeMux()
	NewHandler(store, cfg, WithResolver(resolver)).Register(mux)
	mux.HandleFunc("/insert/jsonline", func(w http.ResponseWriter, r *http.Request) {
		tid, err := logstorage.GetTenantIDFromRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			var row map[string]string
			if json.Unmarshal(sc.Bytes(), &row) == nil {
				store.add(tid, atRow{msg: row["_msg"], traceID: row["trace_id"]})
			}
		}
		w.WriteHeader(http.StatusNoContent)
	})
	srv := httptest.NewServer(resolver.Middleware(mux))
	t.Cleanup(srv.Close)
	return srv
}

func atDo(t *testing.T, method, url, orgID, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if orgID != "" {
		req.Header.Set("X-Scope-OrgID", orgID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// atQuery returns the sorted _msg and trace_id values a select answers with.
func atQuery(t *testing.T, srv *httptest.Server, orgID string) (msgs, traces []string) {
	t.Helper()
	code, body := atDo(t, "GET", srv.URL+"/select/logsql/query?query=*&limit=100", orgID, "")
	if code != http.StatusOK {
		t.Fatalf("query as %q: status %d: %s", orgID, code, body)
	}
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var row map[string]string
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("bad row %q: %v", line, err)
		}
		msgs = append(msgs, row["_msg"])
		traces = append(traces, row["trace_id"])
	}
	sort.Strings(msgs)
	sort.Strings(traces)
	return msgs, traces
}

func TestAutoRegister_CollisionScenario_ThroughTheMiddlewareAndSelectHandlers(t *testing.T) {
	pool := tenant.NewMemConditionalPool()
	resolver := tenant.NewResolver(config2AutoRegister())
	for org, id := range map[string]uint32{"acme-corp": 1001, "staging-team": 1002} {
		if err := resolver.AddAliasFrom(org, tenant.TenantID{AccountID: id}, tenant.SourceConfig); err != nil {
			t.Fatal(err)
		}
	}
	reg, err := tenant.NewRegistry(resolver, tenant.RegistryConfig{Pool: pool, Key: "logs/_meta/tenant-aliases.json"})
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	store := &atStore{}
	srv := atServer(t, resolver, store)

	// Ingest as the configured tenants and as a brand-new OrgID.
	for org, lines := range map[string]string{
		"acme-corp":    `{"_msg":"acme-line-1","trace_id":"acme-trace-1"}` + "\n" + `{"_msg":"acme-line-2","trace_id":"acme-trace-2"}`,
		"staging-team": `{"_msg":"staging-line-1","trace_id":"staging-trace-1"}`,
		"":             `{"_msg":"default-line-1","trace_id":"default-trace-1"}`,
		"new-team-xyz": `{"_msg":"xyz-line-1","trace_id":"xyz-trace-1"}` + "\n" + `{"_msg":"xyz-line-2","trace_id":"xyz-trace-2"}`,
	} {
		if code, body := atDo(t, "POST", srv.URL+"/insert/jsonline", org, lines); code != http.StatusNoContent {
			t.Fatalf("ingest as %s: %d %s", org, code, body)
		}
	}

	// The new OrgID landed in the reserved range, not on a configured alias.
	xyz, ok := resolver.Resolve("new-team-xyz")
	if !ok || !reg.Range().Contains(xyz.AccountID) || xyz.ProjectID != 0 {
		t.Fatalf("new-team-xyz = %+v %v, want an ID inside the reserved range", xyz, ok)
	}
	if acme, _ := resolver.Resolve("acme-corp"); acme.AccountID != 1001 {
		t.Fatalf("acme-corp moved to %+v", acme)
	}

	wantMsgs := map[string][]string{
		"acme-corp":    {"acme-line-1", "acme-line-2"},
		"staging-team": {"staging-line-1"},
		"new-team-xyz": {"xyz-line-1", "xyz-line-2"},
		"":             {"default-line-1"}, // no OrgID: the default tenant 0:0
	}
	seenMsg, seenTrace := map[string]string{}, map[string]string{}
	for org, want := range wantMsgs {
		msgs, traces := atQuery(t, srv, org)
		if strings.Join(msgs, ",") != strings.Join(want, ",") {
			t.Errorf("%s _msg = %v, want %v", org, msgs, want)
		}
		for _, m := range msgs {
			if other, dup := seenMsg[m]; dup {
				t.Errorf("_msg %q visible to both %s and %s", m, other, org)
			}
			seenMsg[m] = org
		}
		for _, tr := range traces {
			if other, dup := seenTrace[tr]; dup {
				t.Errorf("trace_id %q visible to both %s and %s", tr, other, org)
			}
			seenTrace[tr] = org
		}
	}

	// An unknown OrgID reads as a tenant with no data: 200, no rows, and the
	// name is not registered, in memory or in the shared registry.
	before, _, _, _ := pool.DownloadWithETag(context.Background(), "logs/_meta/tenant-aliases.json")
	for _, path := range []string{
		"/select/logsql/query?query=*",
		"/select/logsql/field_names?query=*",
		"/select/logsql/field_values?query=*&field=_msg",
		"/select/logsql/streams?query=*",
		"/select/logsql/hits?query=*&step=1h",
		"/select/logsql/stats_query?query=*%20%7C%20stats%20count()%20c",
	} {
		code, body := atDo(t, "GET", srv.URL+path, "does-not-exist-123", "")
		if code != http.StatusOK {
			t.Errorf("unknown OrgID %s: status %d: %s", path, code, body)
		}
		for _, leaked := range []string{"acme", "staging", "xyz", "default"} {
			if strings.Contains(body, leaked) {
				t.Errorf("unknown OrgID %s leaked %q: %s", path, leaked, body)
			}
		}
	}
	if msgs, _ := atQuery(t, srv, "does-not-exist-123"); len(msgs) != 0 {
		t.Errorf("unknown OrgID returned rows: %v", msgs)
	}
	if _, ok := resolver.Resolve("does-not-exist-123"); ok {
		t.Error("an unknown-OrgID read registered the name")
	}
	after, _, _, _ := pool.DownloadWithETag(context.Background(), "logs/_meta/tenant-aliases.json")
	if string(before) != string(after) {
		t.Errorf("registry changed on reads:\nbefore=%s\nafter=%s", before, after)
	}
	if strings.Contains(string(after), "does-not-exist-123") {
		t.Error("unknown OrgID appears in the alias registry")
	}
}

func config2AutoRegister() tenant.ResolverConfig {
	return tenant.ResolverConfig{AutoRegister: true}
}
