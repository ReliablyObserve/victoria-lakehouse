package main

// The inventory of the background writers. Every call that starts or performs a
// bucket-mutating job sits in a function that really calls the role check
// (cfg.RunsMaintenance or maintenanceAllowed: a call expression, so a comment
// that names them does not count), or in a function listed here with the reason
// it is not gated. A new writer that is in neither list fails these tests, so it
// cannot ship ungated by accident.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// maintenanceCalls are the selectors that start or perform a bucket-mutating
// background job whatever their receiver.
var maintenanceCalls = map[string]bool{
	"NewRewriteScheduler":        true, // delete rewrites: PUT replacement, DELETE source
	"NewOrphanSweep":             true, // compaction orphan sweep: PUT/DELETE
	"SaveToS3":                   true, // stats-aggregate sidecar
	"CleanupLegacyGlobalBundles": true, // pmeta bundle DELETE, marker PUT
	"PersistLabelIndexToS3":      true, // label index PUT
}

// poolMethods are the object-store mutators; they count when called on a pool.
var poolMethods = map[string]bool{
	"Upload": true, "UploadConditional": true, "Copy": true, "Delete": true,
	"PutObject": true, "DeleteObject": true,
}

// isMaintenanceCall reports whether call starts or performs a bucket mutation.
func isMaintenanceCall(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	name := sel.Sel.Name
	recv := strings.ToLower(types.ExprString(sel.X))
	switch {
	case maintenanceCalls[name]:
		return name, true
	case name == "NewScheduler" && recv == "compaction":
		return name, true
	case poolMethods[name] && (strings.Contains(recv, "pool") || strings.Contains(recv, "deleter") || recv == "uploader" || recv == "p"):
		return name, true
	}
	return "", false
}

// callsRoleCheck reports whether the function body really calls the role check.
func callsRoleCheck(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch f := call.Fun.(type) {
		case *ast.Ident:
			if f.Name == "maintenanceAllowed" {
				found = true
			}
		case *ast.SelectorExpr:
			if f.Sel.Name == "RunsMaintenance" {
				found = true
			}
		}
		return true
	})
	return found
}

// writerFuncs returns "file:Func" -> the maintenance calls the function holds,
// for every non-test Go file of dir, with the file's path relative to base.
func writerFuncs(t *testing.T, dir, base string, recursive bool) (map[string][]string, map[string]*ast.BlockStmt) {
	t.Helper()
	out := map[string][]string{}
	bodies := map[string]*ast.BlockStmt{}
	fset := token.NewFileSet()
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if !recursive && path != dir {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatal(perr)
		}
		rel, _ := filepath.Rel(base, path)
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var hits []string
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if name, ok := isMaintenanceCall(call); ok {
						hits = append(hits, name)
					}
				}
				return true
			})
			if len(hits) > 0 {
				key := rel + ":" + fd.Name.Name
				out[key] = hits
				bodies[key] = fd.Body
			}
		}
		return nil
	})
	return out, bodies
}

// notGated lists the functions of the main package that hold a maintenance call
// without a role check, with the reason.
var notGated = map[string]string{
	"Upload":       "adapter method (s3PoolAdapter.Upload): called by the tombstone persistence on a delete request, and by the gated jobs",
	"Delete":       "adapter method (s3PoolAdapter.Delete): called by the tombstone persistence on a delete request, and by the gated jobs",
	"DeleteObject": "adapter method (poolDeleter.DeleteObject): handed to the retention manager, which startRetention gates",
}

func TestMaintenanceInventory_EveryS3WriterIsGated(t *testing.T) {
	funcs, bodies := writerFuncs(t, ".", ".", false)
	if len(funcs) < 6 {
		t.Fatalf("the inventory found %d functions with maintenance calls, want at least 6 (setupCompaction, startDeleteRewrite, runShutdown, startStatsLoops, saveStatsAggregate, cleanupLegacyPmeta): the scan is broken", len(funcs))
	}
	for key, hits := range funcs {
		fn := key[strings.Index(key, ":")+1:]
		if _, listed := notGated[fn]; !callsRoleCheck(bodies[key]) && !listed {
			t.Errorf("%s calls %v without calling the role check (cfg.RunsMaintenance / maintenanceAllowed); gate it, or list it in notGated with the reason", key, hits)
		}
	}
}

// internalWriters is every function of the shared internal packages that holds a
// bucket-mutating call, with who gates it. A new writer, or a removed one, fails
// the test until this list is updated (and the reason checked).
var internalWriters = map[string]string{
	"internal/compaction/compactor.go:compactGroup":                          "compaction: started only by setupCompaction (gated)",
	"internal/compaction/orphan_sweep.go:RunTierB":                           "orphan sweep: started only by setupCompaction (gated)",
	"internal/compaction/scheduler.go:segmentGuard":                          "compaction (gated); the Delete is of the scheduler's own abandoned output",
	"internal/delete/rewriter.go:Commit":                                     "delete rewrite (gated)",
	"internal/delete/rewriter.go:Discard":                                    "delete rewrite (gated)",
	"internal/delete/rewriter.go:Upload":                                     "delete rewrite (gated)",
	"internal/delete/rewriter.go:deleteObject":                               "delete rewrite: started only by startDeleteRewrite (gated)",
	"internal/delete/tombstone.go:SyncToS3":                                  "tombstone write: request-driven, one key per tombstone id, retried on every node",
	"internal/delete/tombstone_persist.go:flushPending":                      "tombstone write retry: request-driven, runs on every node",
	"internal/retention/retention.go:RunOnce":                                "retention: started only by startRetention (gated)",
	"internal/stats/aggregate.go:SaveToS3":                                   "stats-aggregate sidecar: called only through saveStatsAggregate (gated)",
	"internal/tenant/migrate.go:MigrateTenant":                               "explicit authenticated operator action",
	"internal/tenant/persistence.go:SaveAliases":                             "legacy alias persister, not wired in either main",
	"internal/tenant/registry.go:Allocate":                                   "alias registry: conditional PUT with If-Match, request-driven",
	"internal/tenant/registry.go:Register":                                   "alias registry: conditional PUT, request-driven",
	"internal/tenant/registry.go:Sync":                                       "alias registry: conditional PUT, request-driven",
	"internal/tenant/registry.go:Unregister":                                 "alias registry: conditional PUT, request-driven",
	"internal/storage/parquets3/writer.go:putSegmentMarker":                  "insert writer: only the insert role has a writer",
	"internal/storage/parquets3/writer.go:uploadLogGroup":                    "insert writer: only the insert role has a writer",
	"internal/storage/parquets3/writer.go:uploadTraceGroup":                  "insert writer: only the insert role has a writer",
	"internal/storage/parquets3/pmeta_wire.go:PutObject":                     "pmeta bundle persist: only through the insert-side writer",
	"internal/storage/parquets3/pmeta_wire.go:PmetaOnFileExpired":            "retention callback (gated)",
	"internal/storage/parquets3/pmeta_migrate.go:CleanupLegacyGlobalBundles": "called only through cleanupLegacyPmeta (gated)",
}

func TestMaintenanceInventory_InternalPackagesWriters(t *testing.T) {
	got := map[string][]string{}
	for _, dir := range []string{"compaction", "delete", "retention", "stats", "tenant", "storage/parquets3", "manifest", "pmeta", "cache", "peercache", "s3reader"} {
		funcs, bodies := writerFuncs(t, filepath.Join("..", "..", "internal", dir), filepath.Join("..", ".."), true)
		for k, v := range funcs {
			if callsRoleCheck(bodies[k]) {
				continue // checks the role itself
			}
			got[filepath.ToSlash(k)] = v
		}
	}
	var keys []string
	for k := range got {
		if _, ok := internalWriters[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Errorf("new bucket-mutating function %s (%v): gate it on the role or list it in internalWriters with the reason", k, got[k])
	}
	for k := range internalWriters {
		if _, ok := got[k]; !ok {
			t.Errorf("internalWriters lists %s, which no longer holds a bucket-mutating call: update the list", k)
		}
	}
}
