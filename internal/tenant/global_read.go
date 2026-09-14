package tenant

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// GlobalReadAuth validates the operator credential that widens a read from a
// single tenant to every tenant. It is the ONLY thing that may do so: without a
// validated credential a select request is answered from exactly one tenant's
// data, the same rule upstream VL/VT applies.
//
// Both forms the configuration offers are accepted — a named header with a
// fixed value, or a bearer token — so an operator can reuse whichever surface
// their gateway already terminates. Comparison is constant-time.
type GlobalReadAuth struct {
	HeaderName  string
	HeaderValue string
	BearerToken string
}

// NewGlobalReadAuth builds the validator from the tenant configuration.
func NewGlobalReadAuth(headerName, headerValue, bearerToken string) GlobalReadAuth {
	return GlobalReadAuth{HeaderName: headerName, HeaderValue: headerValue, BearerToken: bearerToken}
}

// Enabled reports whether a global-read credential is configured at all. When
// it is not, no request can ever read across tenants.
func (a GlobalReadAuth) Enabled() bool {
	return (a.HeaderName != "" && a.HeaderValue != "") || a.BearerToken != ""
}

// Authorize reports whether r carries the configured global-read credential.
// Always false when none is configured — an unconfigured deployment must not be
// widened by an empty header.
func (a GlobalReadAuth) Authorize(r *http.Request) bool {
	if r == nil {
		return false
	}
	if a.HeaderName != "" && a.HeaderValue != "" {
		if secureEqual(r.Header.Get(a.HeaderName), a.HeaderValue) {
			return true
		}
	}
	if a.BearerToken != "" {
		if token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
			if secureEqual(token, a.BearerToken) {
				return true
			}
		}
	}
	return false
}

func secureEqual(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
