package main

import (
	"context"
	"encoding/json"
	"flag"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

func publicDeleteRequest(mux http.Handler, method, path string, form url.Values, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

// With the defaults every /delete/* path the lakehouse does not serve itself
// gets VT's own "disabled" answer, exactly like a VictoriaTraces node started
// without -delete.enable — whatever delete.enabled says.
func TestMountPublicDelete_GatedByDefault(t *testing.T) {
	for _, deleteEnabled := range []bool{true, false} {
		mux := http.NewServeMux()
		mountPublicDelete(mux, deleteEnabled, nil)
		for _, path := range []string{"/delete/run_task", "/delete/stop_task", "/delete/active_tasks", "/delete/anything"} {
			rec := publicDeleteRequest(mux, http.MethodPost, path, url.Values{}, nil)
			if rec.Code != http.StatusBadRequest || rec.Body.String() != deleteDisabledMessage+"\n" {
				t.Fatalf("delete.enabled=%v %s: got %d %q, want VT's disabled answer", deleteEnabled, path, rec.Code, rec.Body.String())
			}
		}
	}
}

func enablePublicDelete(t *testing.T) {
	t.Helper()
	*deleteEnable = true
	t.Cleanup(func() { *deleteEnable = false })
}

func TestMountPublicDelete_NeedsTheDeleteFeature(t *testing.T) {
	enablePublicDelete(t)
	if !internaldelete.PublicFlagEnabled() {
		t.Fatal("internaldelete.PublicFlagEnabled() does not see this binary's -delete.enable")
	}
	mux := http.NewServeMux()
	mountPublicDelete(mux, false, nil)
	rec := publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.PublicDeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
	}
}

type nopStorage struct{}

func (nopStorage) RunQuery(context.Context, []logstorage.TenantID, *logstorage.Query, logstorage.WriteDataBlockFunc) error {
	return nil
}
func (nopStorage) GetFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) GetFieldValues(context.Context, []logstorage.TenantID, *logstorage.Query, string, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) GetStreamFieldNames(context.Context, []logstorage.TenantID, *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) GetStreamFieldValues(context.Context, []logstorage.TenantID, *logstorage.Query, string, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) GetStreams(context.Context, []logstorage.TenantID, *logstorage.Query, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) GetStreamIDs(context.Context, []logstorage.TenantID, *logstorage.Query, uint64) ([]logstorage.ValueWithHits, error) {
	return nil, nil
}
func (nopStorage) HasDataForRange(int64, int64) bool { return false }
func (nopStorage) Close() error                      { return nil }

// End to end: run_task registers a tombstone scoped to the request's tenant,
// active_tasks lists it in upstream's shape and stop_task removes it.
func TestMountPublicDelete_RunTaskIsTenantScoped(t *testing.T) {
	enablePublicDelete(t)
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, true, testGlobalRead)

	rec := publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, map[string]string{"AccountID": "7", "ProjectID": "3"})
	var resp struct {
		TaskID string `json:"task_id"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.TaskID == "" {
		t.Fatalf("run_task: %d %q", rec.Code, rec.Body.String())
	}
	if ts, ok := store.Get(resp.TaskID); !ok || !ts.ScopedExactlyTo(7, 3) {
		t.Fatalf("tombstone = %+v, want scoped to exactly 7:3", ts)
	}
	rec = publicDeleteRequest(mux, http.MethodGet, "/delete/active_tasks", url.Values{}, map[string]string{"AccountID": "7", "ProjectID": "3"})
	var tasks []*logstorage.DeleteTask
	if json.Unmarshal(rec.Body.Bytes(), &tasks) != nil || len(tasks) != 1 || tasks[0].TenantIDs[0] != (logstorage.TenantID{AccountID: 7, ProjectID: 3}) {
		t.Fatalf("active_tasks = %q", rec.Body.String())
	}
	rec = publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {resp.TaskID}}, map[string]string{"AccountID": "7", "ProjectID": "3"})
	if rec.Code != http.StatusOK || store.Count() != 0 {
		t.Fatalf("stop_task: %d %q, tombstones=%d", rec.Code, rec.Body.String(), store.Count())
	}
	for path, want := range map[string]string{
		"/delete/stop_task": "missing task_id arg\n",
		"/delete/nope":      "unsupported path requested: \"/delete/nope\"\n",
	} {
		if rec := publicDeleteRequest(mux, http.MethodPost, path, url.Values{}, nil); rec.Code != http.StatusBadRequest || rec.Body.String() != want {
			t.Errorf("%s: %d %q, want VT's %q", path, rec.Code, rec.Body.String(), want)
		}
	}
}

// The flag keeps VT's name, default and help.
func TestPublicDeleteFlag_IsVTs(t *testing.T) {
	f := flag.Lookup(internaldelete.PublicFlagName)
	if f == nil || f.DefValue != "false" || f.Usage != "Whether to enable /delete/* HTTP endpoints" {
		t.Fatalf("-%s = %+v, want VT's flag", internaldelete.PublicFlagName, f)
	}
}

// public_delete.go is a copy of VT's /delete/* handling until this binary can
// mount vtselect.RequestHandler. Fail the moment the vendored VT source stops
// matching it: the flag, the disabled answer, the counters, and the four
// handler functions (whose intended differences are the storage package and
// the explicitly discarded write results).
func TestUpstreamPublicDelete_MatchesVendoredVTSelect(t *testing.T) {
	mainSrc, err := os.ReadFile("deps/VictoriaTraces/app/vtselect/main.go")
	if err != nil {
		t.Fatalf("vendored VictoriaTraces source missing (run make deps-vt): %v", err)
	}
	joined := regexp.MustCompile(`"\s*\+\s*"`).ReplaceAllString(string(mainSrc), "")
	for what, want := range map[string]string{
		"flag":   `flag.Bool("delete.enable", false, "Whether to enable /delete/* HTTP endpoints")`,
		"answer": `httpserver.Errorf(w, r, "` + deleteDisabledMessage + `")`,
		"gate":   "if !*enableDelete {",
		"branch": `if strings.HasPrefix(path, "/delete/") {`,
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("VT's /delete/ %s changed; update public_delete.go\nwant: %s", what, want)
		}
	}

	logsqlSrc, err := os.ReadFile("deps/VictoriaTraces/app/vtselect/logsql.go")
	if err != nil {
		t.Fatalf("read vendored logsql.go: %v", err)
	}
	for _, counter := range []string{"/delete/run_task", "/delete/stop_task", "/delete/active_tasks"} {
		if !strings.Contains(string(logsqlSrc), "metrics.NewCounter(`vt_http_requests_total{path=\""+counter+"\"}`)") {
			t.Errorf("VT's request counter for %s changed; update public_delete.go", counter)
		}
	}
	ourSrc, err := os.ReadFile("public_delete.go")
	if err != nil {
		t.Fatal(err)
	}
	upstreamFuncs := funcSources(t, "logsql.go", logsqlSrc)
	ourFuncs := funcSources(t, "public_delete.go", ourSrc)
	for _, name := range []string{"deleteHandler", "processDeleteRunTaskRequest", "processDeleteStopTaskRequest", "processDeleteActiveTasksRequest"} {
		up, ok := upstreamFuncs[name]
		if !ok {
			t.Errorf("vendored VT no longer has %s; re-check public_delete.go", name)
			continue
		}
		// The intended differences: VT's storage package is vtstorage, this
		// binary's dispatch into the lakehouse storage is VictoriaLogs'
		// vlstorage; and the copy discards fmt.Fprintf's result explicitly.
		up = strings.ReplaceAll(up, "vtstorage.", "vlstorage.")
		ours := strings.ReplaceAll(ourFuncs[name], "_, _ = fmt.Fprintf(", "fmt.Fprintf(")
		if ours != up {
			t.Errorf("%s drifted from vendored VT; update public_delete.go\nupstream:\n%s\nours:\n%s", name, up, ours)
		}
	}
}

func funcSources(t *testing.T, name string, src []byte) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	out := map[string]string{}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil {
			continue
		}
		out[fd.Name.Name] = string(src[fset.Position(fd.Pos()).Offset:fset.Position(fd.End()).Offset])
	}
	return out
}

// testGlobalRead is the operator credential in these tests.
func testGlobalRead(r *http.Request) bool {
	return r.Header.Get("X-Lakehouse-Global-Read") == "letmein"
}

// The public stop_task and active_tasks act for the caller's tenant only:
// tenant B can neither list nor stop tenant A's task, and stopping it answers
// byte for byte what stopping an unknown task answers. The global-read
// credential lists and stops every task.
func TestMountPublicDelete_TasksAreTenantScoped(t *testing.T) {
	enablePublicDelete(t)
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, true, testGlobalRead)

	asA := map[string]string{"AccountID": "1001"}
	asB := map[string]string{"AccountID": "2002"}
	global := map[string]string{"X-Lakehouse-Global-Read": "letmein"}
	run := func(headers map[string]string) string {
		t.Helper()
		rec := publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, headers)
		var resp struct {
			TaskID string `json:"task_id"`
		}
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &resp) != nil || resp.TaskID == "" {
			t.Fatalf("run_task: %d %q", rec.Code, rec.Body.String())
		}
		return resp.TaskID
	}
	taskA := run(asA)
	time.Sleep(time.Microsecond) // upstream's task id is the issue time in nanoseconds
	taskB := run(asB)

	list := func(headers map[string]string) string {
		return publicDeleteRequest(mux, http.MethodGet, "/delete/active_tasks", url.Values{}, headers).Body.String()
	}
	if got := list(asB); !strings.Contains(got, taskB) || strings.Contains(got, taskA) {
		t.Fatalf("tenant 2002 lists %s, want only its own task %s", got, taskB)
	}
	if got := list(global); !strings.Contains(got, taskA) || !strings.Contains(got, taskB) {
		t.Fatalf("the operator lists %s, want both tasks", got)
	}

	foreign := publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {taskA}}, asB)
	unknown := publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {"1"}}, asB)
	if foreign.Code != unknown.Code || foreign.Body.String() != unknown.Body.String() ||
		foreign.Header().Get("Content-Type") != unknown.Header().Get("Content-Type") {
		t.Fatalf("stopping another tenant's task answered %d %q, an unknown task %d %q: they must be identical",
			foreign.Code, foreign.Body.String(), unknown.Code, unknown.Body.String())
	}
	if _, ok := store.Get(taskA); !ok {
		t.Fatal("tenant 2002 stopped tenant 1001's task")
	}
	publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {taskA}}, global)
	publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {taskB}}, global)
	if store.Count() != 0 {
		t.Fatalf("the operator could not stop every task; %d left", store.Count())
	}
}
