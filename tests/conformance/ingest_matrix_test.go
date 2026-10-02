package conformance

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	im "github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix"
)

// The ingest parity matrix (tests/ingestmatrix, run by tests/e2e/ingest_matrix_test.go)
// must follow the upstream ingest surface. These tests are its drift gate:
//
//   - the ingest routes and listener flags are extracted from the vendored
//     VictoriaLogs and VictoriaTraces trees (not from a hand-written list), and
//     every one must be exercised by a case or probe, or carry a reason in the
//     exclusion table; a protocol upstream adds fails here until the matrix has it;
//   - everything the matrix exercises or excludes must still exist upstream, so a
//     removed route fails here instead of leaving a dead case behind;
//   - every case/form has a registry row with a real test reference, and every
//     ingest registry row has a case.

func extractLive(t *testing.T) *inventory.Inventory {
	t.Helper()
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := inventory.DefaultDirs(root)
	for _, dir := range []string{d.VL, d.VT} {
		if _, err := os.Stat(dir); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("%s missing: run make deps-logs deps-traces deps-vt", dir)
			}
			t.Skipf("%s missing: run make deps-logs deps-traces deps-vt (or set CONFORMANCE_REQUIRE_DEPS=1 to fail)", dir)
		}
	}
	inv, err := inventory.Extract(d)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

// ingestSurface returns the upstream ingest items of one signal as
// "route:<name>" / "flag:<name>" keys: every route registered under
// app/vlinsert or app/vtinsert, and every listener flag those packages register.
func ingestSurface(inv *inventory.Inventory, sig im.Signal) map[string]bool {
	surface, srcPrefix := "vl", "app/vlinsert/"
	if sig == im.Traces {
		surface, srcPrefix = "vt", "app/vtinsert/"
	}
	out := map[string]bool{}
	for _, it := range inv.Items {
		if it.Surface != surface || !strings.HasPrefix(it.Source, srcPrefix) {
			continue
		}
		switch it.Kind {
		case "route":
			out["route:"+it.Name] = true
		case "flag":
			if it.Linked && strings.Contains(strings.ToLower(it.Name), "listenaddr") {
				out["flag:"+it.Name] = true
			}
		}
	}
	return out
}

// matrixCovers reports how an upstream item is accounted for ("" = not).
func matrixCovers(key string, exercised map[string]string, excluded map[string]string) string {
	if c, ok := exercised[key]; ok {
		return "exercised by " + c
	}
	if r, ok := excluded[key]; ok {
		return "excluded: " + r
	}
	// A dispatch prefix ("/insert/loki/") is accounted for by any concrete
	// route under it that the matrix exercises.
	if name, ok := strings.CutPrefix(key, "route:"); ok && strings.HasSuffix(name, "/") {
		for k, c := range exercised {
			if rest, ok := strings.CutPrefix(k, "route:"); ok && strings.HasPrefix(rest, name) {
				return "prefix of " + rest + " (" + c + ")"
			}
		}
	}
	return ""
}

func exclusionMap(sig im.Signal) map[string]string {
	m := map[string]string{}
	for _, e := range im.Exclusions() {
		if e.Signal == sig {
			m[e.Name] = e.Reason
		}
	}
	return m
}

// checkIngestMatrixAgainstUpstream is the pure comparison, separated so its own
// test can feed it a mutated inventory.
func checkIngestMatrixAgainstUpstream(inv *inventory.Inventory, sig im.Signal) []string {
	var problems []string
	upstream := ingestSurface(inv, sig)
	exercised := im.Exercised(sig)
	excluded := exclusionMap(sig)

	var keys []string
	for k := range upstream {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if matrixCovers(k, exercised, excluded) == "" {
			problems = append(problems, fmt.Sprintf("%s: upstream %s has no ingest-matrix case, probe or exclusion (add one in tests/ingestmatrix)", sig, k))
		}
	}
	for k, c := range exercised {
		if !upstream[k] {
			problems = append(problems, fmt.Sprintf("%s: the matrix sends to %s (%s) but upstream no longer registers it", sig, k, c))
		}
	}
	for k, reason := range excluded {
		switch {
		case strings.TrimSpace(reason) == "":
			problems = append(problems, fmt.Sprintf("%s: exclusion %s has no reason", sig, k))
		case !upstream[k]:
			problems = append(problems, fmt.Sprintf("%s: exclusion %s names an item upstream no longer registers", sig, k))
		case exercised[k] != "":
			problems = append(problems, fmt.Sprintf("%s: %s is both excluded and exercised by %s", sig, k, exercised[k]))
		}
	}
	sort.Strings(problems)
	return problems
}

func TestIngestMatrix_FollowsUpstreamIngestSurface(t *testing.T) {
	inv := extractLive(t)
	for _, sig := range []im.Signal{im.Logs, im.Traces} {
		surface := ingestSurface(inv, sig)
		if len(surface) < 5 {
			t.Fatalf("%s: only %d ingest items extracted from upstream (%v): the extractor lost its footing", sig, len(surface), surface)
		}
		if problems := checkIngestMatrixAgainstUpstream(inv, sig); len(problems) > 0 {
			t.Errorf("ingest matrix drifted from upstream:\n%s", strings.Join(problems, "\n"))
		}
		t.Logf("%s: %d upstream ingest items (routes + listener flags) accounted for by the matrix", sig, len(surface))
	}
}

// TestIngestMatrixDrift_DetectsAddedAndRemovedRoutes proves the gate fails in
// both directions on a mutated inventory: an upstream route the matrix does not
// know, and a route the matrix still sends to that upstream dropped.
func TestIngestMatrixDrift_DetectsAddedAndRemovedRoutes(t *testing.T) {
	inv := extractLive(t)

	added := &inventory.Inventory{Items: append([]inventory.Item(nil), inv.Items...)}
	added.Items = append(added.Items, inventory.Item{Kind: "route", Surface: "vl", Name: "/insert/newprotocol/push", Source: "app/vlinsert/newprotocol/newprotocol.go"})
	if p := checkIngestMatrixAgainstUpstream(added, im.Logs); len(p) != 1 || !strings.Contains(p[0], "/insert/newprotocol/push") {
		t.Fatalf("an upstream route the matrix does not know must be the only problem, got %v", p)
	}

	for _, drop := range []struct {
		sig  im.Signal
		name string
	}{
		{im.Logs, "/insert/loki/api/v1/push"},
		{im.Logs, "/insert/jsonline"},
		{im.Traces, "/insert/opentelemetry/v1/traces"},
	} {
		removed := &inventory.Inventory{}
		surface := "vl"
		if drop.sig == im.Traces {
			surface = "vt"
		}
		for _, it := range inv.Items {
			if it.Kind == "route" && it.Surface == surface && it.Name == drop.name {
				continue
			}
			removed.Items = append(removed.Items, it)
		}
		p := checkIngestMatrixAgainstUpstream(removed, drop.sig)
		if !strings.Contains(strings.Join(p, "\n"), "route:"+drop.name) {
			t.Fatalf("dropping upstream route %s must fail the gate, got %v", drop.name, p)
		}
	}

	// The listener flags are part of the surface: losing one fails too.
	noFlag := &inventory.Inventory{}
	for _, it := range inv.Items {
		if it.Kind == "flag" && it.Name == "otlpGRPCListenAddr" {
			continue
		}
		noFlag.Items = append(noFlag.Items, it)
	}
	if p := checkIngestMatrixAgainstUpstream(noFlag, im.Traces); len(p) == 0 {
		t.Fatalf("dropping the otlpGRPCListenAddr flag must fail the gate")
	}
}

func TestIngestMatrix_ListenerFlagsAreDerived(t *testing.T) {
	inv := extractLive(t)
	logs := ingestSurface(inv, im.Logs)
	traces := ingestSurface(inv, im.Traces)
	for _, want := range []string{"flag:syslog.listenAddr.tcp", "flag:syslog.listenAddr.udp"} {
		if !logs[want] {
			t.Errorf("logs ingest surface lacks %s: %v", want, logs)
		}
	}
	if !traces["flag:otlpGRPCListenAddr"] {
		t.Errorf("traces ingest surface lacks flag:otlpGRPCListenAddr: %v", traces)
	}
}

func TestIngestMatrix_CaseTableIsWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range im.AllCases() {
		key := string(c.Signal) + "/" + c.ID
		if seen[key] {
			t.Errorf("duplicate case %s", key)
		}
		seen[key] = true
		if len(c.Forms) == 0 {
			t.Errorf("%s: no tenant form", key)
		}
		if len(c.Forms) == 1 && c.FormNote == "" {
			t.Errorf("%s: runs in a single tenant form without saying why (FormNote)", key)
		}
		if len(c.Routes)+len(c.Flags) == 0 {
			t.Errorf("%s: names no upstream route or flag", key)
		}
		if c.Rejected && c.Rows != 0 {
			t.Errorf("%s: a refused payload stores no rows", key)
		}
		if !c.Rejected && c.Rows == 0 {
			t.Errorf("%s: no rows expected", key)
		}
		if !c.Rejected && c.Counter == "" {
			t.Errorf("%s: no ingest counter to check", key)
		}
		switch c.Transport {
		case im.HTTP:
			if c.Build == nil {
				t.Errorf("%s: HTTP case without Build", key)
			}
		case im.TCP, im.UDP:
			if c.Lines == nil {
				t.Errorf("%s: line transport without Lines", key)
			}
		case im.GRPC:
			if c.Build != nil || c.Lines != nil {
				t.Errorf("%s: gRPC cases are built by GRPCExport", key)
			}
		default:
			t.Errorf("%s: unknown transport %q", key, c.Transport)
		}
	}
}

// The case table implies required registry rows; the rows must exist, run, and
// cite the e2e test that executes them.
func TestIngestMatrix_RegistryRowsMatchCases(t *testing.T) {
	reg, err := registry.LoadDir("registry/rows")
	if err != nil {
		t.Fatal(err)
	}
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	e2eFile := "tests/e2e/ingest_matrix_test.go"
	src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(e2eFile)))
	if err != nil {
		t.Fatal(err)
	}
	wantRows := map[string]bool{}
	for _, c := range im.AllCases() {
		testName := "TestIngestMatrix_Logs"
		if c.Signal == im.Traces {
			testName = "TestIngestMatrix_Traces"
		}
		if !strings.Contains(string(src), "func "+testName+"(") {
			t.Errorf("%s does not define %s", e2eFile, testName)
		}
		for _, f := range c.Forms {
			id := im.RowID(c, f)
			wantRows[id] = true
			row := reg.ByID[id]
			if row == nil {
				t.Errorf("no registry row %s for case %s in the %s form", id, c.ID, f)
				continue
			}
			if row.Pending {
				t.Errorf("%s: pending; an ingest-matrix row runs in CI and must not be pending", id)
			}
			// A cell with declared known gaps is expect: differ and names every
			// issue; a cell without gaps is expect: pass. Both directions, so a
			// fixed gap cannot leave a stale "differ" behind.
			if len(c.Gaps) == 0 && row.Expect != registry.ExpectPass {
				t.Errorf("%s: expect %q, want pass (the case declares no known gap)", id, row.Expect)
			}
			if len(c.Gaps) > 0 {
				if row.Expect != registry.ExpectDiffer {
					t.Errorf("%s: expect %q, want differ (the case declares known gaps %v)", id, row.Expect, c.Gaps)
				}
				for _, gid := range c.Gaps {
					g, ok := im.GapByID(gid)
					if !ok {
						t.Errorf("%s: unknown gap %q", id, gid)
						continue
					}
					if !strings.Contains(row.DifferNote, g.Issue) {
						t.Errorf("%s: differ_note must cite %s", id, g.Issue)
					}
				}
			}
			if string(row.Surface) != im.Surface(c.Signal) {
				t.Errorf("%s: surface %q", id, row.Surface)
			}
			if row.Refs == nil || !contains(row.Refs.Tests, e2eFile+"#"+testName) {
				t.Errorf("%s: refs.tests must cite %s#%s", id, e2eFile, testName)
			}
			if row.Upstream == nil {
				t.Errorf("%s: no upstream link", id)
				continue
			}
			switch {
			case row.Upstream.Route != "":
				if !contains(c.Routes, row.Upstream.Route) {
					t.Errorf("%s: upstream route %s is not one of the case's routes %v", id, row.Upstream.Route, c.Routes)
				}
			case row.Upstream.Flag != "":
				if !contains(c.Flags, row.Upstream.Flag) {
					t.Errorf("%s: upstream flag %s is not one of the case's flags %v", id, row.Upstream.Flag, c.Flags)
				}
			default:
				t.Errorf("%s: upstream link must be a route or a flag", id)
			}
			if !hasTarget(row, registry.TargetHot) || !hasTarget(row, registry.TargetCold) {
				t.Errorf("%s: targets must be [hot, cold]: the row compares both", id)
			}
		}
	}
	for _, g := range im.RouteGaps() {
		id := im.RouteGapRowID(g)
		wantRows[id] = true
		row := reg.ByID[id]
		if row == nil {
			t.Errorf("no registry row %s for route gap %s", id, g.ID)
			continue
		}
		if row.Pending || row.Expect != registry.ExpectDiffer || !strings.Contains(row.DifferNote, g.Issue) {
			t.Errorf("%s: a route gap row is executed (not pending), expect: differ and cites %s", id, g.Issue)
		}
		if row.Upstream == nil || row.Upstream.Route != g.Route {
			t.Errorf("%s: upstream route must be %s", id, g.Route)
		}
		if row.Refs == nil || !contains(row.Refs.Tests, e2eFile+"#TestIngestMatrix_RouteGaps") {
			t.Errorf("%s: refs.tests must cite %s#TestIngestMatrix_RouteGaps", id, e2eFile)
		}
		if !strings.Contains(string(src), "func TestIngestMatrix_RouteGaps(") {
			t.Errorf("%s does not define TestIngestMatrix_RouteGaps", e2eFile)
		}
	}
	for _, r := range reg.Rows {
		if (strings.HasPrefix(r.ID, "vl.ingest.") || strings.HasPrefix(r.ID, "vt.ingest.")) && !wantRows[r.ID] {
			t.Errorf("registry row %s has no case in tests/ingestmatrix", r.ID)
		}
	}
}

func hasTarget(r *registry.Row, t registry.Target) bool {
	for _, x := range r.Targets {
		if x == t {
			return true
		}
	}
	return false
}

// The matrix needs the opt-in listeners on both sides of the e2e stack; the
// stack must also keep them out of the product defaults (Helm: off).
func TestIngestMatrix_E2EComposeEnablesListenersOnBothSides(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "deployment", "docker", "docker-compose-e2e.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"-syslog.listenAddr.tcp=", "-syslog.listenAddr.udp=", "-syslog.tenantID.tcp=", "-syslog.tenantID.udp=", "-otlpGRPCListenAddr=", "-otlpGRPC.tls=false"} {
		if n := strings.Count(string(data), `"`+flag); n != 2 { // hot and Lakehouse
			t.Errorf("docker-compose-e2e.yml has %d occurrences of %s, want 2 (hot and Lakehouse)", n, flag)
		}
	}
	tenant := fmt.Sprintf("%d:%d", im.NumericTenant.Account, im.NumericTenant.Project)
	if !strings.Contains(string(data), "-syslog.tenantID.tcp="+tenant) || !strings.Contains(string(data), "-syslog.tenantID.udp="+tenant) {
		t.Errorf("the syslog listeners must be pinned to the matrix's numeric tenant %s", tenant)
	}
}

// Every gap has a tracking issue, is used by a case (an unused gap is a fixed
// one still listed) and is written up in docs/ingest-parity.md.
func TestIngestMatrix_GapsAreTrackedAndUsed(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := os.ReadFile(filepath.Join(root, "docs", "ingest-parity.md"))
	if err != nil {
		t.Fatal(err)
	}
	e2e, err := os.ReadFile(filepath.Join(root, "tests", "e2e", "ingest_matrix_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range im.RouteGaps() {
		if !strings.Contains(string(doc), g.Issue) {
			t.Errorf("route gap %s: docs/ingest-parity.md does not list %s", g.ID, g.Issue)
		}
	}
	used := map[string]bool{}
	for _, c := range im.AllCases() {
		for _, id := range c.Gaps {
			used[id] = true
		}
	}
	for _, g := range im.Gaps() {
		if !strings.HasPrefix(g.Issue, "https://github.com/ReliablyObserve/victoria-lakehouse/issues/") {
			t.Errorf("gap %s: %q is not an issue link", g.ID, g.Issue)
		}
		if !used[g.ID] {
			t.Errorf("gap %s is used by no case: delete it (the issue is fixed)", g.ID)
		}
		if !strings.Contains(string(doc), g.Issue) {
			t.Errorf("gap %s: docs/ingest-parity.md does not list %s", g.ID, g.Issue)
		}
		if !strings.Contains(string(e2e), `"`+g.ID+`"`) {
			t.Errorf("gap %s has no rewrite in tests/e2e/ingest_matrix_test.go", g.ID)
		}
	}
	for id := range used {
		if _, ok := im.GapByID(id); !ok {
			t.Errorf("a case declares unknown gap %q", id)
		}
	}
}
