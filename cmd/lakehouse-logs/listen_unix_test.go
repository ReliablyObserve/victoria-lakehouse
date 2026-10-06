package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

// -httpListenAddr=unix:/path (VictoriaLogs v1.53.0, #1618) reaches the HTTP
// server unchanged: run() hands the flag's value to httpserver.Serve, whose
// library does the listening. The peer and ownership features read the same
// string as host:port, so a unix socket is for a node without peers.
func TestHTTPListenAddr_UnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "lh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "lh.sock")
	addr := "unix:" + sock

	httpserver.Serve([]string{addr}, func(w http.ResponseWriter, _ *http.Request) bool {
		_, _ = io.WriteString(w, "pong")
		return true
	}, httpserver.ServeOptions{})
	t.Cleanup(func() { _ = httpserver.Stop([]string{addr}) })

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	var body []byte
	for deadline := time.Now().Add(5 * time.Second); ; {
		resp, err := client.Get("http://lakehouse/hello")
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no answer over %s: %v", sock, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if string(body) != "pong" {
		t.Errorf("answer over the unix socket = %q, want pong", body)
	}
}
