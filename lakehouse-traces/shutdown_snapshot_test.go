// Twin of cmd/lakehouse-logs/shutdown_snapshot_test.go.

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage/parquets3"
)

// shutdownStore builds a real Storage (writer included) over an in-memory
// bucket that accepts PUTs.
func shutdownStore(t *testing.T) *parquets3.Storage {
	t.Helper()
	var mu sync.Mutex
	objects := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut:
			n, _ := io.Copy(io.Discard, r.Body)
			mu.Lock()
			objects[r.URL.Path] = int(n)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	cfg.S3.Bucket = "test-bucket"
	cfg.S3.Region = "us-east-1"
	cfg.S3.Endpoint = srv.URL
	cfg.S3.ForcePathStyle = true
	cfg.S3.AccessKey = "test"
	cfg.S3.SecretKey = "test"
	cfg.Cache.DiskPath = ""
	cfg.Manifest.PersistPath = t.TempDir()
	store, err := parquets3.New(cfg)
	if err != nil {
		t.Fatalf("parquets3.New: %v", err)
	}
	return store
}

// The snapshot persisted AFTER the final flush carries the object that flush
// wrote, with its exact time bounds, so a restart does not have to guess them
// from the S3 listing (the partition hour).
func TestCloseStoreAndPersistManifest_SnapshotCarriesFinalFlush(t *testing.T) {
	store := shutdownStore(t)
	base := time.Date(2026, 10, 1, 7, 10, 0, 0, time.UTC)
	last := base.Add(2 * time.Second)
	span := func(ts time.Time, id string) schema.TraceRow {
		n := ts.UnixNano()
		return schema.TraceRow{TimestampUnixNano: n, StartTimeUnixNano: n, TraceID: "t" + id, SpanID: "s" + id, SpanName: "op", ServiceName: "svc"}
	}
	store.MustAddTraceRows([]schema.TraceRow{
		span(base, "a"), span(base.Add(time.Second), "b"), span(last, "c"),
	})

	path := filepath.Join(t.TempDir(), "manifest-snapshot.json")
	persistManifestSnapshot(store, path, 10*time.Second, "shutdown") // runShutdown's first persist
	before := manifest.New("test-bucket", "traces/")
	if err := before.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	if before.TotalFiles() != 0 {
		t.Fatalf("precondition: the snapshot taken before the final flush holds %d objects, want 0", before.TotalFiles())
	}

	closeStoreAndPersistManifest(store, path, 10*time.Second)

	after := manifest.New("test-bucket", "traces/")
	if err := after.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	files := after.GetFilesForRange(0, 1<<62)
	if len(files) != 1 {
		t.Fatalf("snapshot after the final flush holds %d objects, want 1", len(files))
	}
	fi := files[0]
	if fi.BoundsInferred || fi.RowCount != 3 || fi.MaxTimeNs != last.UnixNano() || fi.MinTimeNs != base.UnixNano() {
		t.Fatalf("object in the post-flush snapshot = %+v, want 3 rows with exact bounds [%d, %d]", fi, base.UnixNano(), last.UnixNano())
	}
}

// persistManifestSnapshot is bounded by its timeout and reports a failure
// without panicking.
func TestPersistManifestSnapshot_BadPathDoesNotPanic(t *testing.T) {
	store := shutdownStore(t)
	persistManifestSnapshot(store, filepath.Join(t.TempDir(), "no", "such", "dir", "m.json"), time.Second, "shutdown")
}

// runShutdown (traces) must not call store.Close() directly (the original ordering, which
// persisted the manifest only BEFORE the final flush): the close goes through
// closeStoreAndPersistManifest, which persists again after it. It also keeps the
// first persist ahead of the long Stop() calls.
func TestRunShutdown_PersistsManifestBeforeAndAfterClose(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	ast.Inspect(f, func(n ast.Node) bool {
		fd, ok := n.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "runShutdown" {
			return true
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				order = append(order, fn.Name)
			case *ast.SelectorExpr:
				if x, ok := fn.X.(*ast.Ident); ok {
					order = append(order, x.Name+"."+fn.Sel.Name)
				}
			}
			return true
		})
		return false
	})
	idx := func(name string) int {
		for i, n := range order {
			if n == name {
				return i
			}
		}
		return -1
	}
	first, stop, closeAndPersist := idx("persistManifestSnapshot"), idx("vtinsert.Stop"), idx("closeStoreAndPersistManifest")
	if first < 0 || stop < 0 || closeAndPersist < 0 {
		t.Fatalf("runShutdown calls %v: want persistManifestSnapshot, vtinsert.Stop and closeStoreAndPersistManifest", order)
	}
	if first >= stop || stop >= closeAndPersist {
		t.Errorf("runShutdown order = %v: want the first manifest persist, then the Stop() calls, then close+persist", order)
	}
	if i := idx("store.Close"); i >= 0 {
		t.Errorf("runShutdown calls store.Close() directly (call #%d): the final flush would not be followed by a manifest persist", i)
	}
}
