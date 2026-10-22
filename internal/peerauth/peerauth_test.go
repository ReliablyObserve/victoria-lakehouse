package peerauth

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func req(hdr, val string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/internal/x", strings.NewReader("body"))
	if hdr != "" {
		r.Header.Set(hdr, val)
	}
	return r
}

func TestEqual(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"k", "k", true},
		{"", "", true},
		{"k", "K", false},
		{"k", "kk", false},
		{"kk", "k", false},
		{"", "k", false},
		{"cluster-gossip-secret", "cluster-gossip-secreT", false},
	} {
		if got := Equal(tc.a, tc.b); got != tc.want {
			t.Errorf("Equal(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func FuzzEqual(f *testing.F) {
	f.Add("k", "k")
	f.Add("", "x")
	f.Add("a\x00b", "a")
	f.Fuzz(func(t *testing.T, a, b string) {
		if got := Equal(a, b); got != (a == b) {
			t.Fatalf("Equal(%q, %q) = %v", a, b, got)
		}
	})
}

// Only exactly "Bearer <key>" is served; an empty key enforces nothing.
func TestBearer(t *testing.T) {
	for _, tc := range []struct {
		name      string
		authz     string
		key       string
		wantServe bool
		wantProof bool
	}{
		{"no key enforced, no credential", "", "", true, false},
		{"no key enforced, any credential", "Bearer x", "", true, false},
		{"key, no credential", "", "k", false, false},
		{"key, right", "Bearer k", "k", true, true},
		{"key, wrong", "Bearer x", "k", false, false},
		{"key, other scheme", "Basic k", "k", false, false},
		{"key, lowercase scheme", "bearer k", "k", false, false},
		{"key, bare", "k", "k", false, false},
		{"key, empty bearer", "Bearer ", "k", false, false},
		{"key, longer", "Bearer kk", "k", false, false},
		{"key, prefix", "Bearer ", "k", false, false},
		{"key, padded", "Bearer  k", "k", false, false},
	} {
		r := req("Authorization", tc.authz)
		if got := Bearer(r, tc.key); got != tc.wantServe {
			t.Errorf("%s: Bearer = %v, want %v", tc.name, got, tc.wantServe)
		}
		if got := BearerPresented(r, tc.key); got != tc.wantProof {
			t.Errorf("%s: BearerPresented = %v, want %v", tc.name, got, tc.wantProof)
		}
	}
}

func FuzzBearer(f *testing.F) {
	f.Add("Bearer k", "k")
	f.Add("", "k")
	f.Add("Bearer ", "")
	f.Fuzz(func(t *testing.T, authz, key string) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header["Authorization"] = []string{authz}
		got := Bearer(r, key)
		want := key == "" || (strings.HasPrefix(authz, "Bearer ") && authz == "Bearer "+key)
		if got != want {
			t.Fatalf("Bearer(%q, key %q) = %v, want %v", authz, key, got, want)
		}
	})
}

func TestHeader(t *testing.T) {
	for _, tc := range []struct {
		name, val, key string
		want           bool
	}{
		{"no key enforced", "", "", true},
		{"no key enforced, any value", "x", "", true},
		{"key, missing", "", "k", false},
		{"key, right", "k", "k", true},
		{"key, wrong", "x", "k", false},
		{"key, longer", "kk", "k", false},
	} {
		if got := Header(req(PeerHeader, tc.val), PeerHeader, tc.key); got != tc.want {
			t.Errorf("%s: Header = %v, want %v", tc.name, got, tc.want)
		}
	}
	// the Bearer form does not satisfy the header form, and the reverse
	if Header(req("Authorization", "Bearer k"), PeerHeader, "k") {
		t.Error("Authorization: Bearer satisfied the X-Peer-Auth-Key check")
	}
	if Bearer(req(PeerHeader, "k"), "k") {
		t.Error("X-Peer-Auth-Key satisfied the Authorization: Bearer check")
	}
}

func TestLimitBody(t *testing.T) {
	t.Cleanup(func() { SetMaxBodyBytes(0) })
	if MaxBodyBytes() != DefaultMaxBodyBytes {
		t.Fatalf("default cap = %d, want %d", MaxBodyBytes(), DefaultMaxBodyBytes)
	}
	SetMaxBodyBytes(8)
	for _, tc := range []struct {
		body    string
		wantErr bool
	}{{"12345678", false}, {"123456789", true}, {"", false}} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
		w := httptest.NewRecorder()
		LimitBody(w, r)
		buf := make([]byte, 64)
		n, err := r.Body.Read(buf)
		for err == nil {
			var m int
			m, err = r.Body.Read(buf[n:])
			n += m
		}
		tooLarge := TooLarge(err)
		if tooLarge != tc.wantErr {
			t.Errorf("body %q: TooLarge = %v (err %v), want %v", tc.body, tooLarge, err, tc.wantErr)
		}
	}
	SetMaxBodyBytes(-1)
	if MaxBodyBytes() != DefaultMaxBodyBytes {
		t.Errorf("SetMaxBodyBytes(-1) left the cap at %d", MaxBodyBytes())
	}
}

func TestReadError(t *testing.T) {
	w := httptest.NewRecorder()
	ReadError(w, &http.MaxBytesError{Limit: 1})
	if w.Code != http.StatusRequestEntityTooLarge || !strings.Contains(w.Body.String(), "peer.max_request_body_bytes") {
		t.Errorf("oversized: %d %q, want 413 naming the knob", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	ReadError(w, http.ErrBodyReadAfterClose)
	if w.Code != http.StatusBadRequest {
		t.Errorf("other read error: %d, want 400", w.Code)
	}
}

func TestReport_Gauge(t *testing.T) {
	Report(true, true, true)
	if got := metrics.PeerAuthEnabled.Get(); got != 1 {
		t.Errorf("enabled: gauge = %d, want 1", got)
	}
	for _, tc := range []struct{ keySet, peers bool }{{false, false}, {false, true}, {true, false}, {true, true}} {
		Report(false, tc.keySet, tc.peers)
		if got := metrics.PeerAuthEnabled.Get(); got != 0 {
			t.Errorf("disabled (key %v, peers %v): gauge = %d, want 0", tc.keySet, tc.peers, got)
		}
	}
}

// Every server-side key check goes through this package: none of the handlers
// compares a key itself, with != or ==, which is not constant time.
func TestNoHandlerComparesTheKeyItself(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, f := range []string{
		"internal/stats/sync.go",
		"internal/tenant/sync.go",
		"internal/tenant/handler.go",
		"internal/peercache/peercache.go",
		"internal/buffer/handler.go",
		"cmd/lakehouse-logs/handlers.go",
		"lakehouse-traces/handlers.go",
	} {
		b, err := os.ReadFile(filepath.Join(root, f))
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.Contains(line, `!= "Bearer "+`) || strings.Contains(line, `== "Bearer "+`) ||
				strings.Contains(line, "!= h.authKey") || strings.Contains(line, "!= sh.authKey") ||
				strings.Contains(line, "== h.authKey") || strings.Contains(line, "subtle.") {
				t.Errorf("%s:%d compares the key itself (use internal/peerauth): %s", f, i+1, strings.TrimSpace(line))
			}
		}
	}
}

// The key check runs once per internal request, none of them on a query path:
// the cost is two SHA-256 digests of a short key.
func BenchmarkBearer(b *testing.B) {
	r := httptest.NewRequest(http.MethodPost, "/internal/x", nil)
	r.Header.Set("Authorization", "Bearer cluster-gossip-secret")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if !Bearer(r, "cluster-gossip-secret") {
			b.Fatal("refused")
		}
	}
}
