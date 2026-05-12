package manifest

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPusher_Notify_NoPeers_ReturnsImmediately(t *testing.T) {
	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return nil },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	// Should return immediately without panicking
	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
}

func TestPusher_Notify_EmptyPeerList(t *testing.T) {
	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{} },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	// Should return immediately
	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
}

func TestPusher_Notify_SelfAsOnlyPeer_SkipsSelf(t *testing.T) {
	var pushCalled atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pushCalled.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	selfAddr := srv.Listener.Addr().String()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{selfAddr} },
		AuthSecret: "",
		SelfAddr:   selfAddr,
	})

	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(100 * time.Millisecond)

	if got := pushCalled.Load(); got != 0 {
		t.Errorf("expected 0 push calls (self skipped), got %d", got)
	}
}

func TestPusher_Notify_UnreachablePeer_NoFatalError(t *testing.T) {
	// Use an address that will fail to connect
	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{"192.0.2.1:9999"} }, // RFC 5737 TEST-NET
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	// Should not panic; error is logged internally
	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
}

func TestPusher_Push_AuthSecretSetsHeader(t *testing.T) {
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "super-secret-token",
		SelfAddr:   "self:9428",
	})

	p.Notify([]FileInfo{{Key: "test.parquet", Size: 42}}, nil)
	time.Sleep(200 * time.Millisecond)

	expected := "Bearer super-secret-token"
	if gotAuth != expected {
		t.Errorf("expected Authorization %q, got %q", expected, gotAuth)
	}
}

func TestPusher_Push_NoAuthSecret_NoHeader(t *testing.T) {
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(200 * time.Millisecond)

	if gotAuth != "" {
		t.Errorf("expected no Authorization header, got %q", gotAuth)
	}
}

func TestPusher_Push_Non200Response_IncrementsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	// Should not panic; non-200 triggers error metric increment
	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(200 * time.Millisecond)
}

func TestPusher_Push_Non200_MultipleCodes(t *testing.T) {
	codes := []int{http.StatusBadRequest, http.StatusForbidden, http.StatusServiceUnavailable}
	for _, code := range codes {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
			}))
			defer srv.Close()

			p := NewPusher(PusherConfig{
				GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
				AuthSecret: "",
				SelfAddr:   "self:9428",
			})

			// Should not panic for any non-200 code
			p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
			time.Sleep(200 * time.Millisecond)
		})
	}
}

func TestPusher_Notify_MultiplePeersWithSelf(t *testing.T) {
	var received atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	srv1 := httptest.NewServer(handler)
	defer srv1.Close()
	srv2 := httptest.NewServer(handler)
	defer srv2.Close()

	selfAddr := "10.0.0.1:9428"

	p := NewPusher(PusherConfig{
		GetPeers: func() []string {
			return []string{
				selfAddr,
				srv1.Listener.Addr().String(),
				srv2.Listener.Addr().String(),
			}
		},
		AuthSecret: "",
		SelfAddr:   selfAddr,
	})

	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(200 * time.Millisecond)

	if got := received.Load(); got != 2 {
		t.Errorf("expected 2 pushes (self skipped), got %d", got)
	}
}

func TestPusher_Push_ContentTypeJSON(t *testing.T) {
	var gotContentType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(200 * time.Millisecond)

	if gotContentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", gotContentType)
	}
}

func TestPusher_Push_CorrectURL(t *testing.T) {
	var gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "",
		SelfAddr:   "self:9428",
	})

	p.Notify([]FileInfo{{Key: "test.parquet"}}, nil)
	time.Sleep(200 * time.Millisecond)

	if gotPath != "/internal/manifest/update" {
		t.Errorf("expected path /internal/manifest/update, got %q", gotPath)
	}
}
