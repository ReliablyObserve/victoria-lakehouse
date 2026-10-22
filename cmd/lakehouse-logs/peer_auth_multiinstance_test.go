package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// Multi-instance proof of peer authentication: two writers (insert pods), two
// readers and a select pod, each a real HTTP server mounting the binary's own
// internal endpoints (mountPeerEndpoints, mountBufferQuery), calling each other
// with the real clients (manifest push, stats sync, tenant sync, peer cache,
// the buffer query a select pod's bridge sends). A pod can be re-mounted with
// another setting, as a rolling restart does; its state survives.

// nextAccount gives every alias its own tenant: the resolver keeps aliases
// one-to-one with tenants.
var nextAccount atomic.Uint32

type swapHandler struct {
	mu sync.RWMutex
	h  http.Handler
}

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	h := s.h
	s.mu.RUnlock()
	h.ServeHTTP(w, r)
}

type clusterPod struct {
	name   string
	writer bool
	srv    *httptest.Server
	swap   *swapHandler
	peer   config.PeerConfig
	st     *peerState
	ph     *peercache.Handler
}

func (p *clusterPod) addr() string { return p.srv.Listener.Addr().String() }

// remount restarts the pod's HTTP surface with a new peer setting.
func (p *clusterPod) remount(peer config.PeerConfig) {
	p.peer = peer
	mux := http.NewServeMux()
	p.ph = peercache.NewHandler(peer.EnforcedKey(), "az-1")
	mountPeerEndpoints(mux, peer, peerEndpoints{
		Cache: p.st.cache, Manifest: &mockManifestStore{manifest: p.st.m},
		PeerCache: p.ph, Stats: p.st.reg, Resolver: p.st.res,
	})
	if p.writer {
		mountBufferQuery(mux, twoTenantBuffer{}, peer)
	}
	p.swap.mu.Lock()
	p.swap.h = mux
	p.swap.mu.Unlock()
}

func newCluster(t *testing.T, peers map[string]config.PeerConfig) []*clusterPod {
	t.Helper()
	var pods []*clusterPod
	for _, name := range []string{"writer-1", "writer-2", "reader-1", "reader-2", "select-1"} {
		sw := &swapHandler{h: http.NotFoundHandler()}
		srv := httptest.NewServer(sw)
		t.Cleanup(srv.Close)
		st := &peerState{
			cache: &mockCacheStore{}, m: manifest.New("bucket", "logs/"),
			reg: stats.NewTenantRegistry(name), res: tenant.NewResolver(tenant.ResolverConfig{}),
		}
		p := &clusterPod{name: name, writer: strings.HasPrefix(name, "writer"), srv: srv, swap: sw, st: st}
		p.remount(peers[name])
		pods = append(pods, p)
	}
	return pods
}

func addrsOf(pods []*clusterPod) []string {
	var a []string
	for _, p := range pods {
		a = append(a, p.addr())
	}
	return a
}

// gossip runs every real outgoing call of pod p towards all the others, with
// the pod's own key (sent whenever it is set, enforced or not) and returns the
// errors the calls reported. Calls whose client swallows failures are checked
// by the effect on the receivers.
func (p *clusterPod) gossip(t *testing.T, all []*clusterPod, tag string) []error {
	t.Helper()
	var errs []error
	key := p.peer.AuthKey
	peers := addrsOf(all)

	// manifest push: a flushed file
	manifest.NewPusher(manifest.PusherConfig{GetPeers: func() []string { return peers }, AuthSecret: key, SelfAddr: p.addr()}).
		Notify([]manifest.FileInfo{{Key: fmt.Sprintf("0/0/logs/dt=2026-10-22/hour=10/%s-%s.parquet", p.name, tag), Size: 1}}, nil)

	// stats sync
	p.st.reg.RecordWrite("t:"+p.name+"-"+tag, 10, 20, 1, "STANDARD")
	urls := make([]string, len(peers))
	for i, a := range peers {
		urls[i] = "http://" + a + "/internal/stats/sync"
	}
	if err := stats.NewSyncPusher(stats.SyncPusherConfig{Registry: p.st.reg, GetPeers: func() []string { return urls }, AuthKey: key, SelfAddr: p.addr(), Compress: true}).PushFull(context.Background()); err != nil {
		errs = append(errs, err)
	}

	// tenant alias sync
	_ = p.st.res.AddAlias("alias-"+p.name+"-"+tag, tenant.TenantID{AccountID: nextAccount.Add(1), ProjectID: 1})
	tenant.NewSyncPusher(tenant.SyncPusherConfig{Resolver: p.st.res, GetPeers: func() []string { return peers }, AuthKey: key, SelfAddr: p.addr()}).Push(context.Background())
	return errs
}

// received reports whether pod r holds everything pod s gossiped.
func received(r, s *clusterPod, tag string) (manifestFile, statsTenant, alias bool) {
	for _, fs := range r.st.m.AllFiles() {
		for _, f := range fs {
			manifestFile = manifestFile || strings.HasSuffix(f.Key, "/"+s.name+"-"+tag+".parquet")
		}
	}
	statsTenant = r.st.reg.Get("t:"+s.name+"-"+tag) != nil
	_, alias = r.st.res.Resolve("alias-" + s.name + "-" + tag)
	return
}

func do(t *testing.T, method, url string, hdr map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

func bearer(k string) map[string]string {
	if k == "" {
		return nil
	}
	return map[string]string{"Authorization": "Bearer " + k}
}

// bufferQuery is what a select pod's bridge sends to a writer.
func bufferQuery(t *testing.T, w *clusterPod, key string, all bool) (int, string) {
	scope := "account_id=7&project_id=0"
	if all {
		scope = "all_tenants=true"
	}
	return do(t, http.MethodGet, fmt.Sprintf("http://%s%s?start=0&end=10&mode=logs&tenant_scope=%s&%s", w.addr(), buffer.Path, buffer.TenantScopeVersion, scope), bearer(key))
}

func assertConverged(t *testing.T, pods []*clusterPod, tag, what string) {
	t.Helper()
	for _, s := range pods {
		for _, r := range pods {
			if s == r {
				continue
			}
			if f, st, al := received(r, s, tag); !f || !st || !al {
				t.Errorf("%s: %s -> %s: manifest %v, stats %v, alias %v; want all true", what, s.name, r.name, f, st, al)
			}
		}
	}
}

// (a) every pod holds the key and enforces it: the whole cluster works, and a
// caller without the key is refused on every mutating and read endpoint of
// every pod without changing it.
func TestPeerAuthMultiInstance_AllKeyedAndEnforcing(t *testing.T) {
	on := config.PeerConfig{AuthKey: peerKey, AuthEnabled: true}
	pods := newCluster(t, map[string]config.PeerConfig{"writer-1": on, "writer-2": on, "reader-1": on, "reader-2": on, "select-1": on})
	for _, p := range pods {
		if errs := p.gossip(t, pods, "a"); len(errs) > 0 {
			t.Errorf("%s: gossip failed: %v", p.name, errs)
		}
	}
	assertConverged(t, pods, "a", "keyed+enforcing")

	// peer cache between readers and writers, with the key
	pods[0].ph.Put("blk", []byte("payload"))
	addr := pods[0].addr()
	pc := peercache.New(pods[2].addr(), peerKey, 2*time.Second, 4)
	if data, ok, err := pc.Fetch(context.Background(), addr, "blk"); err != nil || !ok || string(data) != "payload" {
		t.Errorf("keyed peer cache fetch: %q %v %v", data, ok, err)
	}
	if ok, err := pc.Has(context.Background(), addr, "blk"); err != nil || !ok {
		t.Errorf("keyed peer cache has: %v %v", ok, err)
	}
	if _, ok, err := peercache.New(pods[3].addr(), "", 2*time.Second, 4).Fetch(context.Background(), addr, "blk"); ok || err == nil {
		t.Errorf("a peer cache client without the key was served: ok=%v err=%v", ok, err)
	}

	// a select pod's global read of each writer's buffer, with the key
	for _, w := range pods[:2] {
		if code, body := bufferQuery(t, w, peerKey, true); code != 200 || !strings.Contains(body, "t7") || !strings.Contains(body, "t0") {
			t.Errorf("select -> %s all_tenants with the key: %d %q", w.name, code, body)
		}
		if code, _ := bufferQuery(t, w, "", true); code != 401 {
			t.Errorf("select without a key -> %s: %d, want 401", w.name, code)
		}
	}

	// a rogue caller (no key, wrong key) is refused by every pod and changes nothing
	for _, p := range pods {
		for _, key := range []string{"", "wrong-key"} {
			for _, e := range peerEndpointList() {
				req, _ := http.NewRequest(e.method, "http://"+p.addr()+e.path, bodyReader(e.body, t))
				switch {
				case key == "":
				case e.header != "":
					req.Header.Set(e.header, key)
				default:
					req.Header.Set("Authorization", "Bearer "+key)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusUnauthorized {
					t.Errorf("rogue (%q) %s on %s: %d, want 401", key, e.name, p.name, resp.StatusCode)
				}
			}
			if p.st.cache.cleared {
				t.Errorf("rogue (%q) cleared the cache of %s", key, p.name)
			}
			if _, ok := p.st.res.Resolve("org-x"); ok || p.st.reg.Get("9:9") != nil {
				t.Errorf("rogue (%q) changed the state of %s", key, p.name)
			}
			for _, fs := range p.st.m.AllFiles() {
				for _, f := range fs {
					if strings.HasSuffix(f.Key, "/a.parquet") {
						t.Errorf("rogue (%q) pushed a file into the manifest of %s", key, p.name)
					}
				}
			}
		}
	}
}

func bodyReader(f func(*testing.T) []byte, t *testing.T) io.Reader {
	if f == nil {
		return nil
	}
	return strings.NewReader(string(f(t)))
}

// (b) a rolling upgrade without a flag day. Step 0: old pods (no key, as
// before) and new pods (key configured, enforcement off) interoperate in both
// directions. Step 1: every pod holds the key, still not enforcing. Step 2:
// every pod enforces: the cluster still works, and a pod that missed the key
// is refused.
func TestPeerAuthMultiInstance_RollingUpgradeInTwoSteps(t *testing.T) {
	old := config.PeerConfig{}
	keyed := config.PeerConfig{AuthKey: peerKey}
	pods := newCluster(t, map[string]config.PeerConfig{"writer-1": old, "writer-2": keyed, "reader-1": keyed, "reader-2": old, "select-1": keyed})

	// Step 0: mixed old and new, nobody enforces.
	for _, p := range pods {
		if errs := p.gossip(t, pods, "s0"); len(errs) > 0 {
			t.Errorf("mixed %s: gossip failed: %v", p.name, errs)
		}
	}
	assertConverged(t, pods, "s0", "mixed old/new, not enforcing")
	// peer cache, both directions: a keyed client reads a keyless pod, a keyless client a keyed pod
	for _, srvPod := range []*clusterPod{pods[0], pods[1]} {
		srvPod.ph.Put("blk", []byte("payload"))
		for _, clientKey := range []string{"", peerKey} {
			pc := peercache.New("client:1", clientKey, 2*time.Second, 4)
			if data, ok, err := pc.Fetch(context.Background(), srvPod.addr(), "blk"); err != nil || !ok || string(data) != "payload" {
				t.Errorf("mixed: peer cache fetch from %s with client key %q: %q %v %v", srvPod.name, clientKey, data, ok, err)
			}
		}
	}
	// the select pod's global read: a keyed writer serves it, an old (keyless)
	// writer refuses it exactly as before the setting existed
	if code, _ := bufferQuery(t, pods[1], peerKey, true); code != 200 {
		t.Errorf("select -> keyed writer, not enforcing, all_tenants with the key: %d, want 200", code)
	}
	if code, _ := bufferQuery(t, pods[1], "", true); code != 403 {
		t.Errorf("select -> keyed writer, not enforcing, all_tenants without a key: %d, want 403", code)
	}
	if code, _ := bufferQuery(t, pods[0], peerKey, true); code != 403 {
		t.Errorf("select -> old writer, all_tenants: %d, want 403 (no key to prove it)", code)
	}
	if code, body := bufferQuery(t, pods[1], "", false); code != 200 || !strings.Contains(body, "t7") {
		t.Errorf("single-tenant read of a keyed, not enforcing writer without a key: %d %q, want 200", code, body)
	}

	// Step 1: every pod gets the key (restart one by one), nobody enforces yet.
	for _, p := range pods {
		p.remount(keyed)
		if code, _ := do(t, http.MethodGet, "http://"+p.addr()+"/internal/cache/stats", nil); code != 200 {
			t.Errorf("%s after restart with the key, not enforcing: cache stats without a key = %d, want 200", p.name, code)
		}
	}
	for _, w := range pods[:2] {
		if code, _ := bufferQuery(t, w, peerKey, true); code != 200 {
			t.Errorf("step 1: select -> %s all_tenants with the key: %d, want 200", w.name, code)
		}
	}

	// Step 2: enforce, pod by pod. While the rollout is half done, the pods that
	// enforce still accept the keyed calls of the others.
	on := config.PeerConfig{AuthKey: peerKey, AuthEnabled: true}
	for i, p := range pods {
		p.remount(on)
		tag := fmt.Sprintf("s2-%d", i)
		for _, q := range pods {
			if errs := q.gossip(t, pods, tag); len(errs) > 0 {
				t.Errorf("rollout, %d of 5 pods enforcing: %s gossip failed: %v", i+1, q.name, errs)
			}
		}
		assertConverged(t, pods, tag, fmt.Sprintf("%d of 5 pods enforcing", i+1))
	}

	// All enforce: a pod that missed the key (it sends none) is refused everywhere.
	pods[0].ph.Put("blk", []byte("payload"))
	if _, ok, err := peercache.New("client:1", peerKey, 2*time.Second, 4).Fetch(context.Background(), pods[0].addr(), "blk"); err != nil || !ok {
		t.Errorf("fully enforcing: keyed peer cache fetch: %v %v", ok, err)
	}
	if _, ok, err := peercache.New("client:1", "", 2*time.Second, 4).Fetch(context.Background(), pods[0].addr(), "blk"); ok || err == nil {
		t.Errorf("fully enforcing: keyless peer cache fetch was served: %v %v", ok, err)
	}
	for _, p := range pods {
		if code, _ := do(t, http.MethodPost, "http://"+p.addr()+"/internal/cache/clear", nil); code != 401 {
			t.Errorf("fully enforcing: cache clear without a key on %s: %d, want 401", p.name, code)
		}
	}
	manifest.NewPusher(manifest.PusherConfig{GetPeers: func() []string { return addrsOf(pods) }, SelfAddr: "laggard:1"}).
		Notify([]manifest.FileInfo{{Key: "0/0/logs/dt=2026-10-22/hour=10/laggard.parquet", Size: 1}}, nil)
	for _, p := range pods {
		for _, fs := range p.st.m.AllFiles() {
			for _, f := range fs {
				if strings.HasSuffix(f.Key, "laggard.parquet") {
					t.Errorf("a push without the key reached the manifest of %s", p.name)
				}
			}
		}
	}
}

// main.go wires the setting: the pieces this test file cannot reach by calling
// them are kept in place by reading the source (as the -deleteAuthKey wiring is).
func TestMainWiresPeerAuth(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, want := range []string{
		"mountPeerEndpoints(mux, cfg.Peer, peerDeps)",
		"mountBufferQuery(mux, parquets3.BridgeSource{Segments: segs}, cfg.Peer)",
		"tenant.NewHandler(resolver, nil, cfg.Peer.EnforcedKey())",
		"peerauth.SetMaxBodyBytes(cfg.Peer.MaxRequestBodyBytes)",
		"peerauth.Report(cfg.Peer.AuthEnabled, cfg.Peer.AuthKey != \"\"",
		"PeerAuth: PeerAuthInfo{Enabled: cfg.Peer.AuthEnabled, KeyConfigured: cfg.Peer.AuthKey != \"\"}",
		"AuthSecret: cfg.Peer.AuthKey",
		"AuthKey:  cfg.Peer.AuthKey",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("main.go no longer contains %q: peer authentication would stop being wired", want)
		}
	}
	for _, bad := range []string{"HandleCacheClear(store, cfg.Peer.AuthKey)", "HandleManifestUpdate(store, cfg.Peer.AuthKey)", "stats.NewSyncHandler(registry, cfg.Peer.AuthKey)", "tenant.NewSyncHandler(resolver, cfg.Peer.AuthKey)"} {
		if strings.Contains(s, bad) {
			t.Errorf("main.go mounts %q: it would enforce the key whenever one is set, ignoring peer.auth_enabled", bad)
		}
	}
}
