package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// vlRequireRe matches the root module's VictoriaLogs requirement, e.g.
//
//	github.com/VictoriaMetrics/VictoriaLogs v1.52.0
var vlRequireRe = regexp.MustCompile(`(?m)^\s*github\.com/VictoriaMetrics/VictoriaLogs\s+(v\S+)`)

// TestVLCompatMatchesGoMod keeps the version this binary advertises equal to
// the VictoriaLogs it is actually built against. vlCompat is served on
// /lakehouse/info and logged at startup, so a stale constant does not fail
// anything — it silently tells every client the node speaks an older upstream
// than it does, which is exactly how it drifted three releases behind before.
func TestVLCompatMatchesGoMod(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	m := vlRequireRe.FindSubmatch(data)
	if m == nil {
		t.Fatal("go.mod has no VictoriaLogs requirement — the derivation rule for vlCompat needs revisiting")
	}
	want := strings.TrimPrefix(string(m[1]), "v")
	if vlCompat != want {
		t.Fatalf("vlCompat = %q but go.mod requires VictoriaLogs v%s.\n"+
			"Update the constant in main.go; it is reported on /lakehouse/info and in the startup log.", vlCompat, want)
	}
}
