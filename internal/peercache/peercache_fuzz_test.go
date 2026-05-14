package peercache

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func FuzzRingLookup(f *testing.F) {
	f.Add("file-abc.parquet")
	f.Add("")
	f.Add("dt=2026-05-02/hour=10/00000-abc.parquet")
	f.Add("a")
	f.Add("very-long-key-" + string(make([]byte, 1000)))
	f.Add("\x00\x01\x02")
	f.Add("key with spaces")
	f.Add("key/with/slashes")

	f.Fuzz(func(t *testing.T, key string) {
		r := NewRing("self:9428", 150)
		r.Set([]string{"self:9428", "peer1:9428", "peer2:9428"})

		peer, isLocal := r.Lookup(key)
		if peer == "" {
			t.Errorf("Lookup(%q) returned empty peer", key)
		}
		_ = isLocal

		peer2, isLocal2 := r.Lookup(key)
		if peer != peer2 || isLocal != isLocal2 {
			t.Errorf("Lookup(%q) not consistent: (%q,%v) vs (%q,%v)", key, peer, isLocal, peer2, isLocal2)
		}
	})
}

func FuzzHandlerServeHTTP(f *testing.F) {
	f.Add("/internal/cache/fetch", "test-key", "secret")
	f.Add("/internal/cache/has", "test-key", "secret")
	f.Add("/internal/cache/fetch", "", "secret")
	f.Add("/internal/cache/fetch", "key", "wrong-auth")
	f.Add("/unknown/path", "key", "secret")
	f.Add("/internal/cache/fetch", "\x00\x01", "secret")
	f.Add("/internal/cache/has", "key-with/slash", "secret")

	f.Fuzz(func(t *testing.T, path, key, authKey string) {
		h := NewHandler("secret", "")
		h.Put("test-key", []byte("data"))

		req, err := http.NewRequest(http.MethodGet, path+"?key="+key, nil)
		if err != nil {
			return
		}
		req.Header.Set("X-Peer-Auth-Key", authKey)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		code := rec.Code
		if code < 100 || code > 599 {
			t.Errorf("invalid status code %d for path=%q key=%q", code, path, key)
		}
	})
}
