package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// vtRequireRe matches the traces module's VictoriaTraces requirement, e.g.
//
//	github.com/VictoriaMetrics/VictoriaTraces v0.11.0
var vtRequireRe = regexp.MustCompile(`(?m)^\s*github\.com/VictoriaMetrics/VictoriaTraces\s+(v\S+)`)

// TestVTCompatMatchesGoMod keeps the version this binary advertises equal to
// the VictoriaTraces it is actually built against. vtCompat is served on
// /lakehouse/info and logged at startup, so a stale constant does not fail
// anything — it silently misreports compatibility, which is how it ended up
// three releases behind.
func TestVTCompatMatchesGoMod(t *testing.T) {
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	m := vtRequireRe.FindSubmatch(data)
	if m == nil {
		t.Fatal("go.mod has no VictoriaTraces requirement — the derivation rule for vtCompat needs revisiting")
	}
	want := strings.TrimPrefix(string(m[1]), "v")
	if vtCompat != want {
		t.Fatalf("vtCompat = %q but go.mod requires VictoriaTraces v%s.\n"+
			"Update the constant in main.go; it is reported on /lakehouse/info and in the startup log.", vtCompat, want)
	}
}
