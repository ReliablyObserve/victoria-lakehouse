package buffer

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// A pod that holds the peer key without enforcing it (peer.auth_enabled false:
// the first step of a rollout, where pods already send their key) answers like
// a pod without a key, except that a caller presenting the key is a proven
// peer for all_tenants=true. Only the single-tenant path follows the setting.
func TestBufferQueryAuth_HeldNotEnforcedKey(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "").WithPeerKey(authTestKey)

	for _, tc := range []struct {
		name, authz string
		all         bool
		want        int
	}{
		{"all_tenants, the key", "Bearer " + authTestKey, true, http.StatusOK},
		{"all_tenants, no credential", "", true, http.StatusForbidden},
		{"all_tenants, a wrong key", "Bearer wrong", true, http.StatusForbidden},
		{"all_tenants, the key as a prefix", "Bearer " + authTestKey[:4], true, http.StatusForbidden},
		{"all_tenants, the key with a suffix", "Bearer " + authTestKey + "x", true, http.StatusForbidden},
		{"all_tenants, another scheme", "Basic " + authTestKey, true, http.StatusForbidden},
		{"one tenant, no credential", "", false, http.StatusOK},
		{"one tenant, a wrong key", "Bearer wrong", false, http.StatusOK},
		{"one tenant, the key", "Bearer " + authTestKey, false, http.StatusOK},
	} {
		q := oneTenant(base, "0", "0")
		if tc.all {
			q = allTenants(base)
		}
		rec := serve(h, q, tc.authz)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %q)", tc.name, rec.Code, tc.want, strings.TrimSpace(rec.Body.String()))
		}
		if rec.Code == http.StatusForbidden && strings.TrimSpace(rec.Body.String()) != AllTenantsRefusedMessage {
			t.Errorf("%s: body = %q, want %q", tc.name, rec.Body.String(), AllTenantsRefusedMessage)
		}
	}
}

// With the key enforced, WithPeerKey changes nothing: 401 without it, served with it.
func TestBufferQueryAuth_EnforcedKeyIgnoresPeerKey(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), authTestKey).WithPeerKey(authTestKey)
	if rec := serve(h, allTenants(base), ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("enforced, no credential: %d, want 401", rec.Code)
	}
	if rec := serve(h, allTenants(base), "Bearer "+authTestKey); rec.Code != http.StatusOK {
		t.Errorf("enforced, the key: %d, want 200", rec.Code)
	}
}

// A pod with no key at all is unchanged by the peer key setting existing.
func TestBufferQueryAuth_NoKeyAtAllUnchanged(t *testing.T) {
	base := time.Date(2026, 5, 3, 14, 0, 0, 0, time.UTC)
	h := NewHandler(tenantBufferStore(base), "").WithPeerKey("")
	if rec := serve(h, allTenants(base), "Bearer "+authTestKey); rec.Code != http.StatusForbidden {
		t.Errorf("no key: all_tenants with a bearer = %d, want 403", rec.Code)
	}
	if rec := serve(h, oneTenant(base, "0", "0"), ""); rec.Code != http.StatusOK {
		t.Errorf("no key: single tenant = %d, want 200", rec.Code)
	}
}
