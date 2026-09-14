package inventory

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestExtractProtocol_Fixtures(t *testing.T) {
	logs, err := ExtractProtocol("testdata/mini-vl")
	if err != nil {
		t.Fatal(err)
	}
	if logs != (Protocol{Select: "v5", Delete: "v2"}) {
		t.Fatalf("logs pin: got %+v, want {v5 v2}", logs)
	}

	traces, err := ExtractProtocol("testdata/mini-vl-traces")
	if err != nil {
		t.Fatal(err)
	}
	if traces != (Protocol{Select: "v5", Delete: "v1"}) {
		t.Fatalf("traces pin: got %+v, want {v5 v1}", traces)
	}
}

// TestExtractProtocols_PerModule is the point of the whole extractor: the two
// modules vendor different VictoriaLogs trees and must be read separately. A
// version read from the wrong tree produces requests the peer rejects with
// "unexpected protocol version" before any query runs.
func TestExtractProtocols_PerModule(t *testing.T) {
	got, err := ExtractProtocols("testdata/mini-vl", "testdata/mini-vl-traces")
	if err != nil {
		t.Fatal(err)
	}
	want := Protocols{VL: Protocol{Select: "v5", Delete: "v2"}, VT: Protocol{Select: "v5", Delete: "v1"}}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	if got.IsZero() {
		t.Fatal("IsZero() true for a populated Protocols")
	}
	if !(Protocols{}).IsZero() {
		t.Fatal("IsZero() false for the zero Protocols")
	}
}

func TestExtractProtocol_MissingTree(t *testing.T) {
	if _, err := ExtractProtocol(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("want an error for a missing VictoriaLogs checkout")
	}
	if _, err := ExtractProtocols("testdata/mini-vl", filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("want an error when the traces-pin tree is missing")
	}
}

// TestExtractProtocol_NoConstants fails loudly rather than recording an empty
// protocol when upstream moves the constants out of netselect.go — an empty
// value would silently turn every {{proto.*}} placeholder into "".
func TestExtractProtocol_NoConstants(t *testing.T) {
	dir := t.TempDir()
	writeNetselect(t, dir, "package netselect\n\nconst Unrelated = 1\n")
	_, err := ExtractProtocol(dir)
	if err == nil || !strings.Contains(err.Error(), "declares no *ProtocolVersion constants") {
		t.Fatalf("got %v, want a 'declares no *ProtocolVersion constants' error", err)
	}
}

// TestExtractProtocol_SplitFamily guards the assumption the registry
// placeholders encode: one version per family. If upstream ever versions
// /internal/select/query separately from /internal/select/field_names, a
// single {{proto.internal_select}} can no longer stand for both, and the
// extractor must say so instead of picking one at random.
func TestExtractProtocol_SplitFamily(t *testing.T) {
	dir := t.TempDir()
	writeNetselect(t, dir, `package netselect

const (
	QueryProtocolVersion             = "v5"
	FieldNamesProtocolVersion        = "v6"
	DeleteRunTaskProtocolVersion     = "v2"
	DeleteStopTaskProtocolVersion    = "v2"
	DeleteActiveTasksProtocolVersion = "v2"
)
`)
	_, err := ExtractProtocol(dir)
	if err == nil {
		t.Fatal("want an error when the select-side constants disagree")
	}
	for _, want := range []string{"select-side protocol constants disagree", "v5=QueryProtocolVersion", "v6=FieldNamesProtocolVersion"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	dir2 := t.TempDir()
	writeNetselect(t, dir2, `package netselect

const (
	QueryProtocolVersion             = "v5"
	DeleteRunTaskProtocolVersion     = "v2"
	DeleteStopTaskProtocolVersion    = "v1"
	DeleteActiveTasksProtocolVersion = "v2"
)
`)
	if _, err := ExtractProtocol(dir2); err == nil || !strings.Contains(err.Error(), "delete-side protocol constants disagree") {
		t.Fatalf("got %v, want a 'delete-side protocol constants disagree' error", err)
	}
}

// TestExtractProtocol_SelectOnlyTree covers the other half of `single`: a tree
// with select constants but no delete family at all (VictoriaLogs before the
// delete endpoints existed) must fail rather than record an empty delete
// version.
func TestExtractProtocol_SelectOnlyTree(t *testing.T) {
	dir := t.TempDir()
	writeNetselect(t, dir, "package netselect\n\nconst QueryProtocolVersion = \"v5\"\n")
	if _, err := ExtractProtocol(dir); err == nil || !strings.Contains(err.Error(), "no delete-side") {
		t.Fatalf("got %v, want a 'no delete-side' error", err)
	}
}

// protocolVersionRe is the shape a protocol version must have: "v" followed
// by digits. Anything else means the extractor matched something other than a
// version constant.
var protocolVersionRe = regexp.MustCompile(`^v[0-9]+$`)

// TestExtractProtocols_RealDeps reads both real vendored trees. It asserts the
// derivation rule and the shape, not hard-coded numbers — the numbers live in
// inventory.generated.yaml, where an upstream bump surfaces them as a diff
// that `confgen -check` refuses until the file is regenerated.
func TestExtractProtocols_RealDeps(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := DefaultDirs(root)
	for _, dir := range []string{d.VL, d.VLTraces} {
		if _, err := os.Stat(dir); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("deps missing (%s): run make deps-logs deps-traces", dir)
			}
			t.Skip("vendored deps not present locally")
		}
	}

	got, err := ExtractProtocols(d.VL, d.VLTraces)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		what string
		v    string
	}{
		{"vl.select", got.VL.Select}, {"vl.delete", got.VL.Delete},
		{"vt.select", got.VT.Select}, {"vt.delete", got.VT.Delete},
	} {
		if !protocolVersionRe.MatchString(c.v) {
			t.Errorf("%s protocol version = %q, want vN", c.what, c.v)
		}
	}

	// The recorded pair must be what the generated inventory carries,
	// otherwise the file the drift gate reads and the sources disagree.
	inv, err := Read(filepath.Join(root, "tests", "conformance", "inventory.generated.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if inv.Protocol != got {
		t.Fatalf("inventory.generated.yaml records protocol %+v but the vendored trees say %+v — run make conformance-gen", inv.Protocol, got)
	}

	t.Logf("internal protocol versions: logs pin (%s) select=%s delete=%s; traces pin (%s) select=%s delete=%s",
		d.VLVersion, got.VL.Select, got.VL.Delete, d.VLCommitTraces, got.VT.Select, got.VT.Delete)
}

func writeNetselect(t *testing.T, dir, body string) {
	t.Helper()
	path := filepath.Join(dir, netselectRelPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
