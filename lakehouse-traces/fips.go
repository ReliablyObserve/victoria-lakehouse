// lakehouse-traces/fips.go
//
// Thin wrapper around Go's stdlib FIPS 140-3 reporting API so the
// `lakehouse-traces fips-status` subcommand can be tested without touching
// the real (process-wide) FIPS state.
package main

import "crypto/fips140"

// fips140Enabled reports whether the running binary has FIPS 140-3 mode
// active. Driven by:
//   - Build env GOFIPS140=v1.0.0 (selects the FIPS-certified module)
//   - Runtime env GODEBUG=fips140=on (must be set; default is off)
//
// See https://go.dev/doc/security/fips140 for the full activation matrix.
func fips140Enabled() bool {
	return fips140.Enabled()
}
