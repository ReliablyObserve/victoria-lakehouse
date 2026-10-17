package main

// The inventory of the main package's background writers. Every call that
// starts a job writing to S3 or to state shared between pods sits in a function
// that checks the role (RunsMaintenance / maintenanceAllowed), or in a function
// listed here with the reason it is not gated. A new writer in main.go that is
// in neither list fails this test, so it cannot ship ungated by accident.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// maintenanceCalls are the selectors that start or perform a bucket-mutating
// background job.
var maintenanceCalls = map[string]bool{
	"NewRewriteScheduler":        true, // delete rewrites: PUT replacement, DELETE source
	"NewOrphanSweep":             true, // compaction orphan sweep: PUT/DELETE
	"NewScheduler":               true, // compaction scheduler: PUT/DELETE
	"SaveToS3":                   true, // stats-aggregate sidecar
	"CleanupLegacyGlobalBundles": true, // pmeta bundle DELETE, marker PUT
	"Upload":                     true, // direct PUT (stats snapshot)
}

// notGated lists the functions that hold such a call without a role check, with
// the reason. Each is request-driven or an adapter whose callers are gated.
var notGated = map[string]string{
	"Upload":       "adapter method (s3PoolAdapter.Upload): called by the tombstone persistence on a delete request, and by the gated jobs",
	"Delete":       "adapter method (s3PoolAdapter.Delete): called by the tombstone persistence on a delete request, and by the gated jobs",
	"DeleteObject": "adapter method (poolDeleter.DeleteObject): handed to the retention manager, which startRetention gates",
}

func TestMaintenanceInventory_EveryS3WriterIsGated(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var hits []string
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok && maintenanceCalls[sel.Sel.Name] {
					hits = append(hits, sel.Sel.Name)
				}
				return true
			})
			if len(hits) == 0 {
				continue
			}
			checked++
			body := string(src[fset.Position(fd.Body.Pos()).Offset:fset.Position(fd.Body.End()).Offset])
			gated := strings.Contains(body, "RunsMaintenance()") || strings.Contains(body, "maintenanceAllowed(")
			if _, listed := notGated[fd.Name.Name]; !gated && !listed {
				t.Errorf("%s: %s calls %v without checking the role (cfg.RunsMaintenance / maintenanceAllowed); gate it, or list it in notGated with the reason", name, fd.Name.Name, hits)
			}
		}
	}
	if checked < 6 {
		t.Fatalf("the inventory found %d functions with maintenance calls, want at least 6 (setupCompaction, startDeleteRewrite, runShutdown, startStatsLoops, saveStatsAggregate, cleanupLegacyPmeta): the scan is broken", checked)
	}
}
