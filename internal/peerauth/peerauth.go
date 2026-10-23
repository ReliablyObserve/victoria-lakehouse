// Package peerauth is the one place that checks the key pods present to each
// other's internal endpoints and bounds the bodies those endpoints read.
//
// Two things here are always on and independent of peer.auth_enabled, because
// neither can refuse a legitimate peer:
//
//   - every key comparison is constant time (Equal);
//   - every request body an internal endpoint reads is capped (LimitBody).
//
// Whether a request without the key is refused is the caller's decision: it
// passes the key it enforces ("" enforces nothing, as before) to Bearer or
// Header.
package peerauth

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
)

const (
	// PeerHeader is the header the peer cache endpoints read the key from.
	PeerHeader = "X-Peer-Auth-Key"

	// DefaultMaxBodyBytes is the default cap on a request body of an internal
	// endpoint (peer.max_request_body_bytes). The largest legitimate bodies
	// are manifest, stats and tenant sync pushes; docs/security.md records
	// the measured sizes.
	DefaultMaxBodyBytes int64 = 64 << 20

	bearerPrefix = "Bearer "
)

// Equal reports whether got equals want in constant time: the time does not
// depend on where the two differ. The length of the key is not secret (a
// different length is answered at once).
func Equal(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// Bearer reports whether r may be served by an endpoint that enforces key and
// reads it from "Authorization: Bearer <key>". An empty key enforces nothing.
func Bearer(r *http.Request, key string) bool {
	if key == "" {
		return true
	}
	return BearerPresented(r, key)
}

// BearerPresented reports whether r carries "Authorization: Bearer <key>" for
// a non-empty key, whether or not the endpoint enforces it.
func BearerPresented(r *http.Request, key string) bool {
	if key == "" {
		return false
	}
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), bearerPrefix)
	if !ok || got == "" {
		return false
	}
	return Equal(got, key)
}

// Header reports whether r may be served by an endpoint that enforces key and
// reads it from the named header (the peer cache uses PeerHeader). An empty
// key enforces nothing.
func Header(r *http.Request, name, key string) bool {
	if key == "" {
		return true
	}
	got := r.Header.Get(name)
	return got != "" && Equal(got, key)
}

// RefuseRedirects is the CheckRedirect of every HTTP client that sends the peer
// key (or another internal credential) to a peer. A peer answers its own
// internal endpoints; a redirect from one would make the client repeat the
// request, with the key header (a custom header is forwarded across hosts), at
// a place the operator never configured. The client returns the redirect
// response itself, which the callers already treat as a failed call.
func RefuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// maxBodyBytes is the configured cap; 0 means DefaultMaxBodyBytes.
var maxBodyBytes atomic.Int64

// SetMaxBodyBytes sets the cap LimitBody applies; n <= 0 restores the default.
func SetMaxBodyBytes(n int64) {
	if n < 0 {
		n = 0
	}
	maxBodyBytes.Store(n)
}

// MaxBodyBytes is the cap LimitBody applies.
func MaxBodyBytes() int64 {
	if n := maxBodyBytes.Load(); n > 0 {
		return n
	}
	return DefaultMaxBodyBytes
}

// LimitBody caps what r.Body yields at MaxBodyBytes. Reading past it fails
// with an error TooLarge recognises, and the server closes the connection.
func LimitBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes())
}

// TooLarge reports whether err is LimitBody's cap being exceeded.
func TooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// TooLargeMessage is the answer to a request that exceeds the cap.
const TooLargeMessage = "request body too large (peer.max_request_body_bytes)"

// ReadError answers a failed read of a request body: 413 when the cap was
// exceeded, 400 otherwise.
func ReadError(w http.ResponseWriter, err error) {
	if TooLarge(err) {
		http.Error(w, TooLargeMessage, http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
}
