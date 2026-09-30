package main

import (
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
)

// The traces binary serves LogsQL through VictoriaTraces' own
// app/vtselect/logsql handlers, so VictoriaTraces' latency offset
// (-search.latencyOffset, disable_latency_offset) comes with them. The only
// Lakehouse-side logic left is the route table in selectapi.LogsQLRoutes: this
// test derives the same table from the vendored source and requires equality,
// including which routes upstream applies the offset to.

const (
	vtLogsQLHandlers = "deps/VictoriaTraces/app/vtselect/logsql/logsql.go"
	vtLogsQLRouter   = "deps/VictoriaTraces/app/vtselect/logsql.go"
	vtSelectMain     = "deps/VictoriaTraces/app/vtselect/main.go"
)

var (
	parseCommonArgsCallRe    = regexp.MustCompile(`\bparseCommonArgs\(r\)`)
	parseCommonArgsExtCallRe = regexp.MustCompile(`\bparseCommonArgsExt\(r,\s*(true|false),\s*(true|false)\)`)
)

// upstreamOffsetByHandler classifies every Process*Request in the vendored
// logsql package: true when the handler applies the latency offset by default
// (it calls parseCommonArgs, or parseCommonArgsExt with a false default), false
// when it does not (live tailing disables it by default, the others never parse
// the query's common arguments).
func upstreamOffsetByHandler(t *testing.T) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(vtLogsQLHandlers)
	if err != nil {
		t.Fatalf("vendored VictoriaTraces missing (run make deps-vt): %v", err)
	}
	out := map[string]bool{}
	for name, body := range funcSources(t, "logsql.go", src) {
		if !regexp.MustCompile(`^Process[A-Za-z]+Request$`).MatchString(name) {
			continue
		}
		offset := parseCommonArgsCallRe.MatchString(body)
		if m := parseCommonArgsExtCallRe.FindStringSubmatch(body); m != nil {
			offset = offset || m[2] == "false"
		}
		out[name] = offset
	}
	return out
}

// upstreamRoutes returns path -> handler name from the switch in
// vtselect.processSelectRequest, plus the tail path, from the vendored source.
func upstreamRoutes(t *testing.T) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, vtLogsQLRouter, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", vtLogsQLRouter, err)
	}
	routes := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok || len(cc.List) != 1 {
			return true
		}
		lit, ok := cc.List[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		path, _ := strconv.Unquote(lit.Value)
		for _, st := range cc.Body {
			es, ok := st.(*ast.ExprStmt)
			if !ok {
				continue
			}
			call, ok := es.X.(*ast.CallExpr)
			if !ok {
				continue
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "logsql" {
				routes[path] = sel.Sel.Name
			}
		}
		return true
	})
	if len(routes) == 0 {
		t.Fatalf("no logsql routes found in %s: the router's shape changed, update this test", vtLogsQLRouter)
	}
	return routes
}

func TestLogsQLRouteTableMatchesVendoredVT(t *testing.T) {
	up := upstreamRoutes(t)
	offset := upstreamOffsetByHandler(t)

	ours := map[string]selectapi.LogsQLRoute{}
	for _, r := range selectapi.LogsQLRoutes {
		if _, dup := ours[r.Path]; dup {
			t.Errorf("route %s listed twice", r.Path)
		}
		ours[r.Path] = r
	}

	var paths []string
	for p := range up {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		r, ok := ours[p]
		if !ok {
			t.Errorf("VictoriaTraces serves %s (%s) but selectapi.LogsQLRoutes does not: add the route", p, up[p])
			continue
		}
		if r.Handler != up[p] {
			t.Errorf("%s: we route to %s, VictoriaTraces routes to %s", p, r.Handler, up[p])
		}
		want, known := offset[up[p]]
		if !known {
			t.Errorf("%s: %s is not a Process*Request of the vendored logsql package", p, up[p])
			continue
		}
		if r.LatencyOffset != want {
			t.Errorf("%s (%s): table says latency offset=%v, the vendored handler applies it by default=%v", p, up[p], r.LatencyOffset, want)
		}
	}
	for p := range ours {
		if _, ok := up[p]; !ok {
			t.Errorf("selectapi.LogsQLRoutes serves %s, which VictoriaTraces no longer routes", p)
		}
	}

	// Live tailing is the one route we answer ourselves (501, the cold tier has
	// no live stream); upstream must still route it, and must still have the
	// offset off by default there.
	mainSrc, err := os.ReadFile(vtSelectMain)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`path == "/select/logsql/tail"`).Match(mainSrc) {
		t.Error("VictoriaTraces no longer routes /select/logsql/tail: re-check handleTailNoop")
	}
	if on, ok := offset["ProcessLiveTailRequest"]; !ok || on {
		t.Errorf("ProcessLiveTailRequest: latency offset by default = %v (known=%v), want false: upstream disables it for live tailing", on, ok)
	}
}

// The default the docs, the registry rows and the changelog quote.
func TestLatencyOffsetFlagDefault(t *testing.T) {
	f := flag.Lookup("search.latencyOffset")
	if f == nil {
		t.Fatal("-search.latencyOffset is not registered")
	}
	if f.DefValue != "30s" {
		t.Errorf("-search.latencyOffset default = %s, want 30s", f.DefValue)
	}
}
