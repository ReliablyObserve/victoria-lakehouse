package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// #384 at the binary's own route: /internal/buffer/query as newMux mounts it
// on insert pods (mountBufferQuery), and -internalselect.disable as upstream's
// flag, honoured for /internal/select/* and /internal/buffer/query alike.

type twoTenantBuffer struct{}

func (twoTenantBuffer) ReadBuffer(_ context.Context, _ buffer.Selection, _, _ int64, _ string) (buffer.Answer, error) {
	return buffer.Answer{Logs: []schema.LogRow{
		{TimestampUnixNano: 1, AccountID: 0, ProjectID: 0, Body: "t0"},
		{TimestampUnixNano: 2, AccountID: 7, ProjectID: 0, Body: "t7"},
	}}, nil
}

func bufferReq(t *testing.T, mux http.Handler, all bool, authz string) (int, string) {
	t.Helper()
	q := url.Values{"start": {"0"}, "end": {"10"}, "mode": {"logs"}, "tenant_scope": {buffer.TenantScopeVersion}}
	if all {
		q.Set("all_tenants", "true")
	} else {
		q.Set("account_id", "7")
		q.Set("project_id", "0")
	}
	req := httptest.NewRequest(http.MethodGet, buffer.Path+"?"+q.Encode(), nil)
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var rows []string
	dec := json.NewDecoder(strings.NewReader(rec.Body.String()))
	for rec.Code == http.StatusOK && dec.More() {
		var r schema.LogRow
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r.Body)
	}
	if rec.Code != http.StatusOK {
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	return rec.Code, fmt.Sprint(rows)
}

func TestMountBufferQuery_PeerKey(t *testing.T) {
	keyed := http.NewServeMux()
	mountBufferQuery(keyed, twoTenantBuffer{}, "k")
	open := http.NewServeMux()
	mountBufferQuery(open, twoTenantBuffer{}, "")

	for _, tc := range []struct {
		name     string
		mux      http.Handler
		all      bool
		authz    string
		wantCode int
		want     string
	}{
		{"keyed, all_tenants, no credential", keyed, true, "", 401, buffer.MissingKeyMessage},
		{"keyed, all_tenants, wrong key", keyed, true, "Bearer x", 401, buffer.WrongKeyMessage},
		{"keyed, all_tenants, right key", keyed, true, "Bearer k", 200, "[t0 t7]"},
		{"keyed, one tenant, no credential", keyed, false, "", 401, buffer.MissingKeyMessage},
		{"keyed, one tenant, right key", keyed, false, "Bearer k", 200, "[t7]"},
		{"open, all_tenants", open, true, "", 403, buffer.AllTenantsRefusedMessage},
		{"open, all_tenants, any bearer", open, true, "Bearer k", 403, buffer.AllTenantsRefusedMessage},
		{"open, one tenant", open, false, "", 200, "[t7]"},
	} {
		code, body := bufferReq(t, tc.mux, tc.all, tc.authz)
		if code != tc.wantCode || body != tc.want {
			t.Errorf("%s: got %d %q, want %d %q", tc.name, code, body, tc.wantCode, tc.want)
		}
	}
}

// Upstream's -internalselect.disable (registered here by internal_select.go) is honoured,
// with upstream's answer, on both routes other nodes read this node with.
func TestInternalSelectDisable_TurnsOffBothInternalReadRoutes(t *testing.T) {
	f := flag.Lookup(buffer.InternalSelectDisableFlag)
	if f == nil || f.DefValue != "false" {
		t.Fatalf("-%s = %+v, want upstream's flag with default false", buffer.InternalSelectDisableFlag, f)
	}
	mux := http.NewServeMux()
	mountInternalProtocol(mux, false)
	mountBufferQuery(mux, twoTenantBuffer{}, "k")

	if err := flag.Set(buffer.InternalSelectDisableFlag, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(buffer.InternalSelectDisableFlag, "false") })

	// GET first and Fatalf: an ungated POST would reach upstream's
	// internalselect, which this test never Inits, and block.
	for _, tc := range []struct{ method, path, route string }{
		{http.MethodGet, "/internal/select/field_names", "/internal/select/*"},
		{http.MethodGet, buffer.Path + "?all_tenants=true", buffer.Path},
		{http.MethodPost, "/internal/select/query", "/internal/select/*"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer k")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), buffer.DisabledMessage(tc.route)) {
			t.Fatalf("%s %s: got %d %q, want 400 with %q", tc.method, tc.path, rec.Code, rec.Body.String(), buffer.DisabledMessage(tc.route))
		}
	}

	_ = flag.Set(buffer.InternalSelectDisableFlag, "false")
	if code, body := bufferReq(t, mux, true, "Bearer k"); code != 200 || body != "[t0 t7]" {
		t.Errorf("with the flag off again: %d %q, want 200 [t0 t7]", code, body)
	}
}

func TestApplyPeerFlags(t *testing.T) {
	c := &config.PeerConfig{AuthKey: "from-file"}
	applyPeerFlags(c)
	if c.AuthKey != "from-file" {
		t.Fatalf("an unset -lakehouse.peer.auth-key cleared peer.auth_key: %q", c.AuthKey)
	}
	if err := flag.Set("lakehouse.peer.auth-key", "from-flag"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set("lakehouse.peer.auth-key", "") })
	applyPeerFlags(c)
	if c.AuthKey != "from-flag" {
		t.Fatalf("-lakehouse.peer.auth-key did not override peer.auth_key: %q", c.AuthKey)
	}
}

// upstream-copy check for internal_select.go: VictoriaTraces registers the flag
// with exactly this declaration.
func TestInternalSelectDisableFlag_IsVictoriaTracesDeclaration(t *testing.T) {
	src, err := os.ReadFile("deps/VictoriaTraces/app/vtselect/main.go")
	if err != nil {
		t.Fatalf("read the vendored VictoriaTraces source (make deps-vt): %v", err)
	}
	const want = `flag.Bool("internalselect.disable", false, "Whether to disable /internal/select/* HTTP endpoints")`
	if !strings.Contains(string(src), want) {
		t.Fatalf("VictoriaTraces no longer declares %s; update internal_select.go to match", want)
	}
}
