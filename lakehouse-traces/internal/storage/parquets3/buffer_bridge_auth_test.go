package parquets3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// The traces binary's twin of internal/storage/parquets3/buffer_bridge_auth_test.go.
//
// #383: the select pods' buffer bridge must present peer.auth_key to the
// insert pods, or with auth configured a select pod silently answers without
// any unflushed row. #384: a cross-tenant read of the insert buffer is served
// only to a peer that presented the key. These tests run the real chain: rows
// in an insert pod's segments, its real /internal/buffer/query handler on an
// HTTP server, and a select Storage whose bridge is built the way New builds
// it (newBufferBridgeFor) answering LogsQL (the read Jaeger and Tempo are built on).

const bridgeAuthKey = "cluster-gossip-secret"

// ingestTenantRows adds n spans of tenant t with span_id "<tag>-<i>" to e's segments.
func ingestTenantRows(e *viewEnv, t logstorage.TenantID, tag string, n int) {
	ts := time.Now().Add(-time.Hour)
	lr := logstorage.GetLogRows([]string{"resource_attr:service.name"}, nil, nil, nil, "")
	for i := 0; i < n; i++ {
		lr.MustAdd(t, ts.Add(time.Duration(i)*time.Millisecond).UnixNano(), []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "api"},
			{Name: "trace_id", Value: fmt.Sprintf("trace-%s-%d", tag, i)},
			{Name: "span_id", Value: fmt.Sprintf("%s-%d", tag, i)},
			{Name: "name", Value: "op"},
		}, 1)
	}
	e.segs.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	e.segs.DebugFlush()
}

// insertPod serves e's buffer through the production route: the gate and the
// handler, as both mains mount them.
func authInsertPod(t *testing.T, e *viewEnv, key string, disabled bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(buffer.Path, buffer.Gate(buffer.Path, func() bool { return disabled },
		buffer.NewHandler(BridgeSource{Segments: e.segs}, key)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// selectPod is a select Storage reading e's bucket whose bridge carries key
// and reaches the given insert pods.
func authSelectPod(t *testing.T, e *viewEnv, key string, peers ...string) *Storage {
	t.Helper()
	sel := testStorageWithS3(t, e.srv.url())
	sel.cfg.Mode = config.ModeTraces
	sel.cfg.Role = config.RoleSelect
	sel.cfg.Select.BufferQueryEnabled = true
	sel.cfg.Select.BufferQueryTimeout = 10 * time.Second
	sel.cfg.Peer.AuthKey = key
	sel.bufferBridge = newBufferBridgeFor(sel.cfg)
	if sel.bufferBridge == nil {
		t.Fatal("newBufferBridgeFor returned nil for a select pod with buffer_query_enabled")
	}
	sel.bufferBridge.SetEndpoints(peers)
	return sel
}

// msgCounts runs `*` as tenants (globalRead widens it to every tenant) and
// returns each span_id's count.
func msgCounts(t *testing.T, s *Storage, tenants []logstorage.TenantID, globalRead bool) map[string]int {
	t.Helper()
	now := time.Now()
	q, err := logstorage.ParseQueryAtTimestamp("*", now.UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), now.Add(-48*time.Hour).UnixNano(), now.UnixNano())
	ctx := context.Background()
	if globalRead {
		ctx = storage.WithGlobalRead(ctx)
	}
	var mu sync.Mutex
	got := map[string]int{}
	err = s.RunQuery(ctx, tenants, q, func(_ uint, db *logstorage.DataBlock) {
		for _, c := range db.GetColumns(false) {
			if c.Name != "span_id" {
				continue
			}
			mu.Lock()
			for _, v := range c.Values {
				got[v]++
			}
			mu.Unlock()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func countPrefix(m map[string]int, prefix string) int {
	n := 0
	for k, c := range m {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			n += c
		}
	}
	return n
}

var (
	tenantA = logstorage.TenantID{AccountID: 1, ProjectID: 2}
	tenantB = logstorage.TenantID{AccountID: 7, ProjectID: 0}
)

// The issue's reproduction at the logs twin's size: 5000 unflushed spans, auth
// on (the live repro used 4000). Before the fix the select pod answered 0 of
// them with no error.
func TestBridgeAuth_SelectPodSeesEveryUnflushedRowWithPeerKey(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 5000)
	peer := authInsertPod(t, e, bridgeAuthKey, false)
	sel := authSelectPod(t, e, bridgeAuthKey, peer.URL)

	before := metrics.BufferBridgeErrors.Get("auth")
	got := msgCounts(t, sel, []logstorage.TenantID{tenantA}, false)
	if n := countPrefix(got, "A-"); n != 5000 || len(got) != 5000 {
		t.Fatalf("select pod answers %d of 5000 unflushed rows (%d distinct)", n, len(got))
	}
	if d := metrics.BufferBridgeErrors.Get("auth") - before; d != 0 {
		t.Errorf("%d bridge request(s) refused with the right key", d)
	}
}

// A select pod with another key (or none) is refused, and the refusal is
// counted as reason="auth" (alerted on), not as a generic status failure.
func TestBridgeAuth_WrongOrMissingKeyIsRefusedAndCounted(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 50)
	peer := authInsertPod(t, e, bridgeAuthKey, false)
	for _, key := range []string{"", "other-key"} {
		sel := authSelectPod(t, e, key, peer.URL)
		before := metrics.BufferBridgeErrors.Get("auth")
		got := msgCounts(t, sel, []logstorage.TenantID{tenantA}, false)
		if len(got) != 0 {
			t.Errorf("key %q: answered %d rows from a peer that requires another key", key, len(got))
		}
		if metrics.BufferBridgeErrors.Get("auth") == before {
			t.Errorf("key %q: the refusal was not counted as reason=auth", key)
		}
	}
}

// Single-tenant reads through the bridge stay exactly the tenant asked for,
// with the key, for each tenant, and never widen to another tenant's rows.
// tenantB stands for a string OrgID/alias: the select pod resolves the name
// to its AccountID:ProjectID before it reads, so the bridge only ever sees the
// numeric pair (the alias resolution itself is covered in internal/tenant and
// internal/selectapi).
func TestBridgeAuth_SingleTenantReadsStayInTheirTenant(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 30)
	ingestTenantRows(e, tenantB, "B", 20)
	ingestTenantRows(e, logstorage.TenantID{}, "Z", 10)
	peer := authInsertPod(t, e, bridgeAuthKey, false)
	sel := authSelectPod(t, e, bridgeAuthKey, peer.URL)

	for _, tc := range []struct {
		tenant   logstorage.TenantID
		tag      string
		want     int
		otherTag []string
	}{
		{tenantA, "A-", 30, []string{"B-", "Z-"}},
		{tenantB, "B-", 20, []string{"A-", "Z-"}},
		{logstorage.TenantID{}, "Z-", 10, []string{"A-", "B-"}},
		{logstorage.TenantID{AccountID: 99}, "", 0, []string{"A-", "B-", "Z-"}},
	} {
		got := msgCounts(t, sel, []logstorage.TenantID{tc.tenant}, false)
		if tc.tag != "" && countPrefix(got, tc.tag) != tc.want {
			t.Errorf("tenant %v: %d rows, want %d", tc.tenant, countPrefix(got, tc.tag), tc.want)
		}
		for _, o := range tc.otherTag {
			if n := countPrefix(got, o); n != 0 {
				t.Errorf("tenant %v: answer carries %d row(s) of another tenant (%s*)", tc.tenant, n, o)
			}
		}
	}
}

// A global read crosses tenants through the bridge only with the key; a pod
// without one refuses it (403), and the select pod counts reason=auth.
func TestBridgeAuth_GlobalReadNeedsThePeerKey(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 30)
	ingestTenantRows(e, tenantB, "B", 20)

	keyed := authInsertPod(t, e, bridgeAuthKey, false)
	got := msgCounts(t, authSelectPod(t, e, bridgeAuthKey, keyed.URL), nil, true)
	if countPrefix(got, "A-") != 30 || countPrefix(got, "B-") != 20 {
		t.Fatalf("global read with the key: A=%d B=%d, want 30 and 20", countPrefix(got, "A-"), countPrefix(got, "B-"))
	}

	open := authInsertPod(t, e, "", false)
	before := metrics.BufferBridgeErrors.Get("auth")
	got = msgCounts(t, authSelectPod(t, e, "", open.URL), nil, true)
	if len(got) != 0 {
		t.Fatalf("a pod without a key served %d rows of every tenant to an unauthenticated caller", len(got))
	}
	if metrics.BufferBridgeErrors.Get("auth") == before {
		t.Error("the 403 was not counted as reason=auth")
	}
}

// Without any key the single-tenant bridge keeps working (upstream's default
// for its internal endpoints), so a deployment that sets none is unchanged.
func TestBridgeAuth_NoKeyAnywhereStillBridgesSingleTenant(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 40)
	peer := authInsertPod(t, e, "", false)
	got := msgCounts(t, authSelectPod(t, e, "", peer.URL), []logstorage.TenantID{tenantA}, false)
	if countPrefix(got, "A-") != 40 {
		t.Fatalf("%d of 40 rows without any key", countPrefix(got, "A-"))
	}
}

// -internalselect.disable on the insert pod turns the endpoint off: the select
// pod gets upstream's 400 and no row, whatever key it holds.
func TestBridgeAuth_DisabledEndpointServesNothing(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 25)
	peer := authInsertPod(t, e, bridgeAuthKey, true)
	before := metrics.BufferBridgeErrors.Get("status")
	got := msgCounts(t, authSelectPod(t, e, bridgeAuthKey, peer.URL), []logstorage.TenantID{tenantA}, false)
	if len(got) != 0 {
		t.Fatalf("a disabled endpoint served %d rows", len(got))
	}
	if metrics.BufferBridgeErrors.Get("status") == before {
		t.Error("the disabled answer was not counted")
	}
}

func TestNewBufferBridgeFor(t *testing.T) {
	cfg := testConfig()
	cfg.Role = config.RoleSelect
	cfg.Select.BufferQueryEnabled = true
	cfg.Peer.AuthKey = bridgeAuthKey
	b := newBufferBridgeFor(cfg)
	if b == nil || b.authKey != bridgeAuthKey {
		t.Fatalf("bridge = %+v, want one carrying peer.auth_key", b)
	}
	cfg.Select.BufferQueryEnabled = false
	if newBufferBridgeFor(cfg) != nil {
		t.Error("a bridge was built with select.buffer_query_enabled off")
	}
	cfg.Select.BufferQueryEnabled = true
	cfg.Role = config.RoleInsert
	if newBufferBridgeFor(cfg) != nil {
		t.Error("a bridge was built on an insert-only pod")
	}
}

// Every request carries the header, on every peer and every per-tenant
// sub-request of a multi-tenant read.
func TestBridgeAuth_HeaderOnEveryRequest(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	mk := func() *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get("Authorization"))
			mu.Unlock()
			w.Header().Set(buffer.TenantScopeHeader, r.URL.Query().Get("account_id")+":"+r.URL.Query().Get("project_id"))
		}))
		t.Cleanup(s.Close)
		return s
	}
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 5 * time.Second}, config.ModeTraces)
	b.SetAuthKey(bridgeAuthKey)
	b.SetEndpoints([]string{mk().URL, mk().URL})
	b.QueryTraces(context.Background(), 0, 1, resolveTenantScope([]logstorage.TenantID{tenantA, tenantB}))
	if len(seen) != 4 {
		t.Fatalf("%d requests, want 2 peers x 2 tenants", len(seen))
	}
	for _, h := range seen {
		if h != "Bearer "+bridgeAuthKey {
			t.Errorf("request carried Authorization %q", h)
		}
	}
}
