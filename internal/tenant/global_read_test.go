package tenant

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// GlobalReadAuth is the only thing that may widen a query past its own tenant,
// so every "not configured" and "close but wrong" case must come out false.

func TestGlobalReadAuth_Enabled(t *testing.T) {
	cases := []struct {
		name string
		auth GlobalReadAuth
		want bool
	}{
		{"nothing configured", GlobalReadAuth{}, false},
		{"header name without value", NewGlobalReadAuth("X-Global", "", ""), false},
		{"header value without name", NewGlobalReadAuth("", "secret", ""), false},
		{"header pair", NewGlobalReadAuth("X-Global", "secret", ""), true},
		{"bearer only", NewGlobalReadAuth("", "", "token"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.auth.Enabled(); got != tc.want {
				t.Errorf("Enabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGlobalReadAuth_Authorize(t *testing.T) {
	auth := NewGlobalReadAuth("X-Lakehouse-Global-Read", "s3cr3t", "t0ken")

	cases := []struct {
		name    string
		auth    GlobalReadAuth
		headers map[string]string
		want    bool
	}{
		{"unconfigured never authorizes, even with a header", GlobalReadAuth{}, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, false},
		{"unconfigured never authorizes an empty header", GlobalReadAuth{}, map[string]string{"X-Lakehouse-Global-Read": ""}, false},
		{"correct header", auth, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t"}, true},
		{"wrong header value", auth, map[string]string{"X-Lakehouse-Global-Read": "s3cr3T"}, false},
		{"empty header value", auth, map[string]string{"X-Lakehouse-Global-Read": ""}, false},
		{"prefix of the secret", auth, map[string]string{"X-Lakehouse-Global-Read": "s3cr3"}, false},
		{"secret plus suffix", auth, map[string]string{"X-Lakehouse-Global-Read": "s3cr3t1"}, false},
		{"no headers at all", auth, nil, false},
		{"correct bearer", auth, map[string]string{"Authorization": "Bearer t0ken"}, true},
		{"bearer wrong case of scheme", auth, map[string]string{"Authorization": "bearer t0ken"}, false},
		{"bearer without the scheme", auth, map[string]string{"Authorization": "t0ken"}, false},
		{"bearer with a wrong token", auth, map[string]string{"Authorization": "Bearer nope"}, false},
		{"bearer with an empty token", auth, map[string]string{"Authorization": "Bearer "}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/select/logsql/query", nil)
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			if got := tc.auth.Authorize(r); got != tc.want {
				t.Errorf("Authorize() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGlobalReadAuth_NilRequest(t *testing.T) {
	if NewGlobalReadAuth("X-Global", "secret", "token").Authorize(nil) {
		t.Error("a nil request must never authorize a cross-tenant read")
	}
}
