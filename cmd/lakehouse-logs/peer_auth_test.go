package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/envflag"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// Peer authentication of the internal endpoints (peer.auth_key, tri-state
// peer.auth_enabled): with no key nothing is enforced; a configured key is
// enforced unless peer.auth_enabled is explicitly false, in which case the key
// is only sent (step 1 of a rolling rollout); true enforces and needs the key.
// The key is sent on the pod's own peer calls whenever it is set.

const peerKey = "cluster-gossip-secret"

func boolPtr(v bool) *bool { return &v }

// peerMux mounts the pod's internal endpoints the way the binary does.
func peerMux(peer config.PeerConfig) (*http.ServeMux, *peerState) {
	st := &peerState{
		cache: &mockCacheStore{},
		m:     manifest.New("bucket", "logs/"),
		reg:   stats.NewTenantRegistry("recv"),
		res:   tenant.NewResolver(tenant.ResolverConfig{}),
		ph:    peercache.NewHandler(peer.EnforcedKey(), "az-1"),
	}
	st.ph.Put("block-1", []byte("data"))
	mux := http.NewServeMux()
	mountPeerEndpoints(mux, peer, peerEndpoints{
		Cache: st.cache, Manifest: &mockManifestStore{manifest: st.m},
		PeerCache: st.ph, Stats: st.reg, Resolver: st.res,
	})
	return mux, st
}

type peerState struct {
	cache *mockCacheStore
	m     *manifest.Manifest
	reg   *stats.TenantRegistry
	res   *tenant.TenantResolver
	ph    *peercache.Handler
}

func (s *peerState) files() int {
	n := 0
	for _, fs := range s.m.AllFiles() {
		n += len(fs)
	}
	return n
}

func manifestPush(files ...string) []byte {
	var u manifest.ManifestUpdate
	u.Source = "t"
	for _, k := range files {
		u.Added = append(u.Added, manifest.FileInfo{Key: k, Size: 10})
	}
	b, _ := json.Marshal(u)
	return b
}

func statsPush(t *testing.T, tenantKey string) []byte {
	t.Helper()
	src := stats.NewTenantRegistry("src")
	src.RecordWrite(tenantKey, 10, 20, 1, "STANDARD")
	b, err := json.Marshal(src.BuildDelta(0))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func aliasPush(org string) []byte {
	b, _ := json.Marshal(tenant.AliasDelta{NodeID: "p", Aliases: []tenant.AliasEntry{{OrgID: org, AccountID: 5, ProjectID: 1}}})
	return b
}

// peerEndpoint is one internal endpoint, how a peer authenticates to it, and
// whether it changes the pod (mutating) or only reads.
type peerEndpoint struct {
	name, method, path string
	header             string // "" = Authorization: Bearer; else the header carrying the bare key
	mutating           bool
	body               func(*testing.T) []byte
	okCode             int
}

func peerEndpointList() []peerEndpoint {
	return []peerEndpoint{
		{"manifest update", http.MethodPost, "/internal/manifest/update", "", true, func(*testing.T) []byte { return manifestPush("0/0/logs/dt=2026-10-22/hour=10/a.parquet") }, 200},
		{"cache clear", http.MethodPost, "/internal/cache/clear", "", true, nil, 200},
		{"stats sync", http.MethodPost, "/internal/stats/sync", "", true, func(t *testing.T) []byte { return statsPush(t, "9:9") }, 200},
		{"tenant sync", http.MethodPost, "/internal/tenant/sync", "", true, func(*testing.T) []byte { return aliasPush("org-x") }, 200},
		{"cache stats", http.MethodGet, "/internal/cache/stats", "", false, nil, 200},
		{"cache fetch", http.MethodGet, "/internal/cache/fetch?key=block-1", peerauth.PeerHeader, false, nil, 200},
		{"cache has", http.MethodGet, "/internal/cache/has?key=block-1", peerauth.PeerHeader, false, nil, 200},
	}
}

func (e peerEndpoint) request(t *testing.T, key string) *http.Request {
	var body io.Reader
	if e.body != nil {
		body = bytes.NewReader(e.body(t))
	}
	r := httptest.NewRequest(e.method, e.path, body)
	switch {
	case key == "":
	case e.header != "":
		r.Header.Set(e.header, key)
	default:
		r.Header.Set("Authorization", "Bearer "+key)
	}
	return r
}

// No key, or a key with peer.auth_enabled explicitly false (send-only): every
// internal endpoint takes a request without a credential.
func TestPeerEndpoints_NotEnforcedServesWithoutKey(t *testing.T) {
	for name, peer := range map[string]config.PeerConfig{
		"no key":                 {},
		"no key, explicit false": {AuthEnabled: boolPtr(false)},
		"key, explicit false":    {AuthKey: peerKey, AuthEnabled: boolPtr(false)},
	} {
		for _, e := range peerEndpointList() {
			mux, st := peerMux(peer)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, e.request(t, ""))
			if w.Code != e.okCode {
				t.Errorf("%s, %s without a key: %d %q, want %d", name, e.name, w.Code, w.Body.String(), e.okCode)
			}
			if e.name == "cache clear" && !st.cache.cleared {
				t.Errorf("%s, cache clear did nothing", name)
			}
		}
	}
}

// A configuration written before peer.auth_enabled existed (a key and nothing
// else) enforces the key on every protected endpoint, as it did then; the
// explicit true is the same.
func TestPeerEndpoints_KeyAloneEnforces(t *testing.T) {
	for name, peer := range map[string]config.PeerConfig{
		"key only":      {AuthKey: peerKey},
		"explicit true": {AuthKey: peerKey, AuthEnabled: boolPtr(true)},
	} {
		for _, e := range peerEndpointList() {
			for _, bad := range []string{"", "x" + peerKey[1:], peerKey + "x"} {
				mux, st := peerMux(peer)
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, e.request(t, bad))
				if w.Code != http.StatusUnauthorized {
					t.Errorf("%s, %s with key %q: %d, want 401", name, e.name, bad, w.Code)
				}
				if e.name == "cache clear" && st.cache.cleared {
					t.Errorf("%s: a refused cache clear still cleared", name)
				}
				if e.name == "manifest update" && st.files() != 0 {
					t.Errorf("%s: a refused manifest push merged files", name)
				}
			}
			mux, _ := peerMux(peer)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, e.request(t, peerKey))
			if w.Code != e.okCode {
				t.Errorf("%s, %s with the key: %d, want %d", name, e.name, w.Code, e.okCode)
			}
		}
	}
}

// On: no key, a wrong key (same length, longer, shorter) and the other form of
// credential are refused with 401 before anything is read or changed; the right
// key is served. The state of a refused pod does not change.
func TestPeerEndpoints_EnforcedRefusesWithoutTheKey(t *testing.T) {
	peer := config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(true)}
	for _, e := range peerEndpointList() {
		for _, bad := range []string{"", "x" + peerKey[1:], peerKey + "x", peerKey[:3]} {
			mux, st := peerMux(peer)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, e.request(t, bad))
			if w.Code != http.StatusUnauthorized {
				t.Errorf("%s with key %q: %d %q, want 401", e.name, bad, w.Code, w.Body.String())
			}
			if st.cache.cleared || st.files() != 0 || st.reg.TenantCount() != 0 {
				t.Errorf("%s with key %q changed the pod (cleared %v, files %d, tenants %d)", e.name, bad, st.cache.cleared, st.files(), st.reg.TenantCount())
			}
			if _, ok := st.res.Resolve("org-x"); ok {
				t.Errorf("%s with key %q added an alias", e.name, bad)
			}
		}
		mux, _ := peerMux(peer)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, e.request(t, peerKey))
		if w.Code != e.okCode {
			t.Errorf("%s with the right key: %d %q, want %d", e.name, w.Code, w.Body.String(), e.okCode)
		}
	}
	// the credential of the other form does not open an endpoint
	mux, _ := peerMux(peer)
	r := httptest.NewRequest(http.MethodGet, "/internal/cache/fetch?key=block-1", nil)
	r.Header.Set("Authorization", "Bearer "+peerKey)
	if w := httptest.NewRecorder(); func() int { mux.ServeHTTP(w, r); return w.Code }() != http.StatusUnauthorized {
		t.Error("cache fetch accepted Authorization: Bearer in place of X-Peer-Auth-Key")
	}
}

// The mutating endpoints are the ones a peer without the key must never reach;
// this list is checked against what the binary mounts so a new /internal write
// cannot be added without a decision here.
func TestPeerEndpoints_ListMatchesTheMountedRoutes(t *testing.T) {
	mux, _ := peerMux(config.PeerConfig{})
	for _, e := range peerEndpointList() {
		_, pattern := mux.Handler(httptest.NewRequest(e.method, e.path, nil))
		if pattern == "" {
			t.Errorf("%s %s is not mounted by mountPeerEndpoints", e.method, e.path)
		}
	}
	for _, p := range []string{"/internal/manifest/update", "/internal/stats/sync", "/internal/tenant/sync", "/internal/cache/clear"} {
		found := false
		for _, e := range peerEndpointList() {
			found = found || (strings.HasPrefix(e.path, p) && e.mutating)
		}
		if !found {
			t.Errorf("mutating endpoint %s is not covered", p)
		}
	}
}

// The largest legitimate manifest push passes the default cap; one byte over a
// lowered cap is 413 and changes nothing.
func TestManifestUpdate_BodyCap(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	peerauth.SetMaxBodyBytes(0)

	// A very large push: 2 000 files, each with the per-file maps a flushed or
	// compacted file carries (labels, column stats, label aggregates, column bytes).
	var u manifest.ManifestUpdate
	u.Source = "t"
	for i := 0; i < 2000; i++ {
		fi := manifest.FileInfo{
			Key: fmt.Sprintf("0/0/logs/dt=2026-10-22/hour=10/file-%06d.parquet", i), Size: 1 << 28, RowCount: 1 << 20,
			Labels: map[string][]string{}, LabelAggregates: map[string]map[string]int64{}, ColumnBytes: map[string]int64{},
		}
		for f := 0; f < 20; f++ {
			name := fmt.Sprintf("field_%02d", f)
			fi.Labels[name] = []string{"v1", "v2", "v3"}
			fi.LabelAggregates[name] = map[string]int64{"v1": 1, "v2": 2, "v3": 3}
			fi.ColumnBytes[name] = 1 << 20
		}
		u.Added = append(u.Added, fi)
	}
	body, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("manifest update of 2000 fully annotated files: %d bytes (default cap %d)", len(body), peerauth.DefaultMaxBodyBytes)
	if int64(len(body))*4 > peerauth.DefaultMaxBodyBytes {
		t.Fatalf("the largest legitimate body (%d bytes) leaves no headroom under the default cap %d", len(body), peerauth.DefaultMaxBodyBytes)
	}
	st := &peerState{m: manifest.New("bucket", "logs/")}
	h := HandleManifestUpdate(&mockManifestStore{manifest: st.m}, "")
	w := httptest.NewRecorder()
	h(w, httptest.NewRequest(http.MethodPost, "/internal/manifest/update", bytes.NewReader(body)))
	if w.Code != http.StatusOK || st.files() != 2000 {
		t.Fatalf("largest legitimate push: %d %q, %d files, want 200 and 2000", w.Code, w.Body.String(), st.files())
	}

	small := manifestPush("0/0/logs/dt=2026-10-22/hour=10/a.parquet")
	peerauth.SetMaxBodyBytes(int64(len(small)))
	st = &peerState{m: manifest.New("bucket", "logs/")}
	h = HandleManifestUpdate(&mockManifestStore{manifest: st.m}, "k")
	req := httptest.NewRequest(http.MethodPost, "/internal/manifest/update", bytes.NewReader(small))
	req.Header.Set("Authorization", "Bearer k")
	w = httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("body exactly at the cap: %d, want 200", w.Code)
	}
	peerauth.SetMaxBodyBytes(int64(len(small)) - 1)
	st = &peerState{m: manifest.New("bucket", "logs/")}
	h = HandleManifestUpdate(&mockManifestStore{manifest: st.m}, "k")
	req = httptest.NewRequest(http.MethodPost, "/internal/manifest/update", bytes.NewReader(small))
	req.Header.Set("Authorization", "Bearer k")
	w = httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusRequestEntityTooLarge || st.files() != 0 {
		t.Fatalf("body one byte over the cap: %d %q, %d files, want 413 and none", w.Code, w.Body.String(), st.files())
	}
}

// /lakehouse/info reports the state (never the key), additively.
func TestLakehouseInfo_ReportsPeerAuth(t *testing.T) {
	for _, tc := range []struct {
		peer config.PeerConfig
		want PeerAuthInfo
	}{
		{config.PeerConfig{}, PeerAuthInfo{}},
		{config.PeerConfig{AuthKey: peerKey}, PeerAuthInfo{Enabled: true, KeyConfigured: true}},
		{config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(false)}, PeerAuthInfo{KeyConfigured: true}},
		{config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(true)}, PeerAuthInfo{Enabled: true, KeyConfigured: true}},
	} {
		h := HandleLakehouseInfo(LakehouseInfoConfig{
			Version: "v", Mode: "logs", IsReady: func() bool { return true }, Phase: func() string { return "ready" },
			PeerAuth: PeerAuthInfo{Enabled: tc.peer.EnforcedKey() != "", KeyConfigured: tc.peer.AuthKey != ""},
		})
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodGet, "/lakehouse/info", nil))
		var got struct {
			PeerAuth PeerAuthInfo `json:"peer_auth"`
			Version  string       `json:"version"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.PeerAuth != tc.want || got.Version != "v" {
			t.Errorf("info = %+v, want peer_auth %+v", got, tc.want)
		}
		if strings.Contains(w.Body.String(), peerKey) {
			t.Error("/lakehouse/info carries the key")
		}
	}
}

// -lakehouse.peer.auth-enabled is tri-state: not given leaves the config
// file's value (and the unset state) alone, an explicit =false or =true wins,
// also when the value reaches the flag through the environment (envflag sets
// it with FlagSet.Set, the same call as the command line).
func TestPeerAuthEnabledFlag_GivenOrNot(t *testing.T) {
	f := flag.CommandLine.Lookup("lakehouse.peer.auth-enabled")
	if f == nil {
		t.Fatal("the binary no longer registers -lakehouse.peer.auth-enabled")
	}
	if f.Value != flag.Value(peerAuthEnabled) {
		t.Fatal("-lakehouse.peer.auth-enabled is not the value applyPeerFlags reads")
	}
	// a fresh flag set with a flag of the same type as the binary registers
	newFS := func() (*flag.FlagSet, *peerauth.TriBool) {
		fs := flag.NewFlagSet("peer", flag.ContinueOnError)
		tb := &peerauth.TriBool{}
		fs.Var(tb, "lakehouse.peer.auth-enabled", "")
		return fs, tb
	}
	apply := func(tb *peerauth.TriBool, file *bool) *bool {
		c := config.PeerConfig{AuthEnabled: file}
		tb.Apply(&c.AuthEnabled)
		return c.AuthEnabled
	}
	same := func(a, b *bool) bool { return (a == nil) == (b == nil) && (a == nil || *a == *b) }
	files := []*bool{nil, boolPtr(true), boolPtr(false)}

	fs, tb := newFS()
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if got := apply(tb, file); !same(got, file) {
			t.Errorf("flag not given, file %v: got %v, want the file value", file, got)
		}
	}
	for arg, want := range map[string]bool{"-lakehouse.peer.auth-enabled=false": false, "-lakehouse.peer.auth-enabled=true": true, "-lakehouse.peer.auth-enabled": true} {
		fs, tb := newFS()
		if err := fs.Parse([]string{arg}); err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if got := apply(tb, file); got == nil || *got != want {
				t.Errorf("%s, file %v: got %v, want %v", arg, file, got, want)
			}
		}
	}
}

// The environment path: envflag reads its environment cache once at process
// start, so a child process (this test binary) is started with the variable
// set. The child parses a flag set the way envflag.Parse does and reports.
func TestPeerAuthEnabledFlag_FromEnvironment(t *testing.T) {
	if want := os.Getenv("PEER_AUTH_ENV_WANT"); want != "" {
		if err := flag.Set("envflag.enable", "true"); err != nil {
			t.Fatal(err)
		}
		fs := flag.NewFlagSet("peer", flag.ContinueOnError)
		tb := &peerauth.TriBool{}
		fs.Var(tb, "lakehouse.peer.auth-enabled", "")
		envflag.ParseFlagSet(fs, nil)
		c := config.PeerConfig{}
		tb.Apply(&c.AuthEnabled)
		got := "unset"
		if c.AuthEnabled != nil {
			got = strconv.FormatBool(*c.AuthEnabled)
		}
		if got != want {
			t.Fatalf("lakehouse_peer_auth-enabled in the environment: got %s, want %s", got, want)
		}
		return
	}
	for _, tc := range []struct{ env, want string }{{"false", "false"}, {"true", "true"}, {"", "unset"}} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPeerAuthEnabledFlag_FromEnvironment$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PEER_AUTH_ENV_WANT="+tc.want, "lakehouse_peer_auth-enabled="+tc.env)
		if tc.env == "" {
			cmd.Env = append(os.Environ(), "PEER_AUTH_ENV_WANT="+tc.want)
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("env %q: %v\n%s", tc.env, err, out)
		}
	}
}

// The flags with the config file and the startup validation: a key alone
// enforces, =false is send-only, =true enforces and needs a key.
func TestPeerAuthEnabled_FlagAndStartupValidation(t *testing.T) {
	_ = flag.Set("lakehouse.peer.auth-enabled", "") // unset, whatever an earlier test left
	t.Cleanup(func() {
		_ = flag.Set("lakehouse.peer.auth-enabled", "")
		_ = flag.Set("lakehouse.peer.auth-key", "")
		_ = flag.Set("lakehouse.peer.max-request-body-bytes", "0")
	})
	newCfg := func() *config.Config {
		c := config.Default()
		c.Mode = config.ModeLogs
		c.S3.Bucket = "b"
		return c
	}

	c := newCfg()
	applyPeerFlags(&c.Peer)
	if c.Peer.AuthEnabled != nil || c.Peer.EnforcedKey() != "" {
		t.Fatalf("default: enabled=%v enforced key %q, want unset and nothing enforced", c.Peer.AuthEnabled, c.Peer.EnforcedKey())
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("the default config is invalid: %v", err)
	}

	// a key from the file, nothing else: enforced (the pre-setting behaviour)
	c = newCfg()
	c.Peer.AuthKey = "from-file"
	applyPeerFlags(&c.Peer)
	if err := c.Validate(); err != nil || c.Peer.EnforcedKey() != "from-file" {
		t.Fatalf("key only: err %v, enforced %q, want from-file", err, c.Peer.EnforcedKey())
	}

	// enabled and no key anywhere: one plain message
	c = newCfg()
	c.Peer.AuthEnabled = boolPtr(true)
	applyPeerFlags(&c.Peer)
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "peer.auth_enabled") || !strings.Contains(err.Error(), "peer.auth_key") {
		t.Fatalf("enabled without a key: err = %v, want a message naming both settings", err)
	}
	// ...also when the key is only whitespace
	c = newCfg()
	c.Peer.AuthEnabled = boolPtr(true)
	_ = flag.Set("lakehouse.peer.auth-key", "  \n")
	applyPeerFlags(&c.Peer)
	if err := c.Validate(); err == nil {
		t.Fatal("enabled with a whitespace-only key was accepted")
	}
	// a key set from the flag satisfies it
	c = newCfg()
	c.Peer.AuthEnabled = boolPtr(true)
	_ = flag.Set("lakehouse.peer.auth-key", "k-from-flag")
	applyPeerFlags(&c.Peer)
	if err := c.Validate(); err != nil || c.Peer.EnforcedKey() != "k-from-flag" {
		t.Fatalf("enabled with the key from the flag: err %v, enforced %q", err, c.Peer.EnforcedKey())
	}

	// the body cap flag overrides the default, 0 keeps it
	c = newCfg()
	if c.Peer.MaxRequestBodyBytes != peerauth.DefaultMaxBodyBytes {
		t.Fatalf("default cap = %d, want %d", c.Peer.MaxRequestBodyBytes, peerauth.DefaultMaxBodyBytes)
	}
	_ = flag.Set("lakehouse.peer.max-request-body-bytes", "1048576")
	applyPeerFlags(&c.Peer)
	if c.Peer.MaxRequestBodyBytes != 1048576 {
		t.Fatalf("cap = %d, want 1048576", c.Peer.MaxRequestBodyBytes)
	}
}

// The gauge a scrape sees follows the setting.
func TestPeerAuthGauge(t *testing.T) {
	peerauth.Report(false, false, true)
	if metrics.PeerAuthEnabled.Get() != 0 {
		t.Error("gauge is not 0 with peer authentication off")
	}
	peerauth.Report(true, true, true)
	if metrics.PeerAuthEnabled.Get() != 1 {
		t.Error("gauge is not 1 with peer authentication on")
	}
	peerauth.Report(false, false, false)
}

// A pod sends its key on every outgoing peer call whenever one is configured,
// whether or not it enforces the key itself: the first step of a rollout. With
// no key it sends no credential (an old pod's calls are unchanged).
func TestOutgoingPeerCallsCarryTheKeyWhenConfigured(t *testing.T) {
	for _, key := range []string{"", peerKey} {
		var mu sync.Mutex
		got := map[string]string{}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			got[r.URL.Path] = r.Header.Get("Authorization") + "|" + r.Header.Get(peerauth.PeerHeader)
			mu.Unlock()
			_, _ = io.Copy(io.Discard, r.Body)
		}))
		addr := srv.Listener.Addr().String()

		manifest.NewPusher(manifest.PusherConfig{GetPeers: func() []string { return []string{addr} }, AuthSecret: key, SelfAddr: "self:1"}).
			Notify([]manifest.FileInfo{{Key: "0/0/logs/dt=2026-10-22/hour=10/a.parquet", Size: 1}}, nil)
		reg := stats.NewTenantRegistry("s")
		reg.RecordWrite("1:1", 1, 1, 1, "STANDARD")
		_ = stats.NewSyncPusher(stats.SyncPusherConfig{Registry: reg, GetPeers: func() []string { return []string{"http://" + addr + "/internal/stats/sync"} }, AuthKey: key, SelfAddr: "self:1"}).PushFull(context.Background())
		res := tenant.NewResolver(tenant.ResolverConfig{})
		_ = res.AddAlias("o", tenant.TenantID{AccountID: 1, ProjectID: 1})
		tenant.NewSyncPusher(tenant.SyncPusherConfig{Resolver: res, GetPeers: func() []string { return []string{addr} }, AuthKey: key, SelfAddr: "self:1"}).Push(context.Background())
		pc := peercache.New("self:1", key, time.Second, 2)
		_, _, _ = pc.Fetch(context.Background(), addr, "k")
		_, _ = pc.Has(context.Background(), addr, "k")
		srv.Close()

		want := map[string]string{
			"/internal/manifest/update": "Bearer " + key + "|",
			"/internal/stats/sync":      "Bearer " + key + "|",
			"/internal/tenant/sync":     "Bearer " + key + "|",
			"/internal/cache/fetch":     "|" + key,
			"/internal/cache/has":       "|" + key,
		}
		for path, w := range want {
			if key == "" {
				w = "|"
			}
			if got[path] != w {
				t.Errorf("key %q: %s carried %q, want %q", key, path, got[path], w)
			}
		}
	}
}

// The tenant alias admin writes take the same key as the internal endpoints:
// a key alone or an explicit true enforces it, an explicit false or no key does not.
func TestTenantAliasAPI_FollowsTheEnforcedKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		peer    config.PeerConfig
		enforce bool
	}{
		{"no key", config.PeerConfig{}, false},
		{"key only", config.PeerConfig{AuthKey: peerKey}, true},
		{"explicit true", config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(true)}, true},
		{"explicit false", config.PeerConfig{AuthKey: peerKey, AuthEnabled: boolPtr(false)}, false},
	} {
		mux := http.NewServeMux()
		mountTenantAliasAPI(mux, tc.peer, tenant.NewResolver(tenant.ResolverConfig{}), nil)
		for _, r := range []struct{ method, path string }{
			{http.MethodPost, "/lakehouse/api/v1/tenants/aliases"},
			{http.MethodDelete, "/lakehouse/api/v1/tenants/aliases/org-x"},
		} {
			for _, key := range []string{"", "wrong-" + peerKey} {
				w := httptest.NewRecorder()
				req := httptest.NewRequest(r.method, r.path, strings.NewReader("{}"))
				if key != "" {
					req.Header.Set("Authorization", "Bearer "+key)
				}
				mux.ServeHTTP(w, req)
				if refused := w.Code == http.StatusUnauthorized; refused != tc.enforce {
					t.Errorf("%s: %s %s with key %q: %d, refused=%v want %v", tc.name, r.method, r.path, key, w.Code, refused, tc.enforce)
				}
			}
		}
	}
	// a nil resolver mounts nothing
	mux := http.NewServeMux()
	mountTenantAliasAPI(mux, config.PeerConfig{AuthKey: peerKey}, nil, nil)
	if _, pattern := mux.Handler(httptest.NewRequest(http.MethodPost, "/lakehouse/api/v1/tenants/aliases", nil)); pattern != "" {
		t.Errorf("alias API mounted without a resolver: %q", pattern)
	}
}
