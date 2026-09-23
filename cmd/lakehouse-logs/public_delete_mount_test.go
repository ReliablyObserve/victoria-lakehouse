package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/internaldelete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	internalvlstorage "github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// upstream's answer while -delete.enable is off (vlselect/main.go).
const upstreamPublicDeleteDisabled = "requests to /delete/* are disabled; pass -delete.enable command-line flag for enabling them; " +
	"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs\n"

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
// gets upstream's own "disabled" answer, exactly like a VictoriaLogs node
// started without -delete.enable — whatever delete.enabled says.
func TestMountPublicDelete_GatedByDefault(t *testing.T) {
	for _, deleteEnabled := range []bool{true, false} {
		mux := http.NewServeMux()
		mountPublicDelete(mux, deleteEnabled)
		for _, path := range []string{"/delete/run_task", "/delete/stop_task", "/delete/active_tasks", "/delete/anything"} {
			rec := publicDeleteRequest(mux, http.MethodPost, path, url.Values{}, nil)
			if rec.Code != http.StatusBadRequest || rec.Body.String() != upstreamPublicDeleteDisabled {
				t.Fatalf("delete.enabled=%v %s: got %d %q, want upstream's disabled answer", deleteEnabled, path, rec.Code, rec.Body.String())
			}
		}
	}
}

// The flag is upstream's own registration, with upstream's default and help.
func TestPublicDeleteFlag_IsUpstreams(t *testing.T) {
	f := flag.Lookup(internaldelete.PublicFlagName)
	if f == nil || f.DefValue != "false" ||
		f.Usage != "Whether to enable /delete/* HTTP endpoints; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs" {
		t.Fatalf("-%s = %+v, want upstream's flag with default false", internaldelete.PublicFlagName, f)
	}
}

func TestMountPublicDelete_NeedsTheDeleteFeature(t *testing.T) {
	enablePublicDelete(t)
	mux := http.NewServeMux()
	mountPublicDelete(mux, false)
	rec := publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"*"}}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != internaldelete.PublicDeleteDisabledMessage+"\n" {
		t.Fatalf("got %d %q, want the delete.enabled refusal", rec.Code, rec.Body.String())
	}
}

func enablePublicDelete(t *testing.T) {
	t.Helper()
	if err := flag.Set(internaldelete.PublicFlagName, "true"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(internaldelete.PublicFlagName, "false") })
}

// nopStorage is a cold tier without data: the delete API only registers
// tombstones, it reads nothing.
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

// End to end through upstream's handler: run_task registers a tombstone scoped
// to the request's tenant, active_tasks lists it in upstream's shape, stop_task
// removes it, and the lakehouse's own /delete/logsql/* routes keep working on
// the same mux.
func TestMountPublicDelete_RunTaskIsTenantScoped(t *testing.T) {
	enablePublicDelete(t)
	store := delete.NewTombstoneStore()
	internalvlstorage.SetStorage(nopStorage{}, store)
	mux := http.NewServeMux()
	mountPublicDelete(mux, true)
	delete.NewHandler(store, &manifestQuerierAdapter{m: manifest.New("test-bucket", "")}, delete.NewStorageClassDetector(nil), &config.DeleteConfig{Enabled: true, DefaultMode: "hide"}, "logs").Register(mux)

	rec := publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:error"}}, map[string]string{"AccountID": "1001"})
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("run_task: %d %q (%s)", rec.Code, rec.Body.String(), rec.Header().Get("Content-Type"))
	}
	var resp struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.TaskID == "" {
		t.Fatalf("run_task answer %q is not upstream's {\"task_id\":...}", rec.Body.String())
	}
	ts, ok := store.Get(resp.TaskID)
	if !ok || !ts.ScopedExactlyTo(1001, 0) || ts.Query != "level:error" {
		t.Fatalf("tombstone = %+v, want level:error scoped to exactly 1001:0", ts)
	}

	rec = publicDeleteRequest(mux, http.MethodGet, "/delete/active_tasks", url.Values{}, nil)
	var tasks []*logstorage.DeleteTask
	if err := json.Unmarshal(rec.Body.Bytes(), &tasks); err != nil || len(tasks) != 1 || tasks[0].TaskID != resp.TaskID ||
		len(tasks[0].TenantIDs) != 1 || tasks[0].TenantIDs[0].AccountID != 1001 || tasks[0].Filter != "level:error" {
		t.Fatalf("active_tasks = %d %q, want the task with its tenant and filter", rec.Code, rec.Body.String())
	}

	// The lakehouse's own route on the same mux is still the lakehouse's.
	rec = publicDeleteRequest(mux, http.MethodGet, "/delete/logsql/tombstones", url.Values{}, map[string]string{"AccountID": "1001"})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), resp.TaskID) {
		t.Fatalf("/delete/logsql/tombstones as 1001:0 = %d %q, want the lakehouse listing with the task", rec.Code, rec.Body.String())
	}

	rec = publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{"task_id": {resp.TaskID}}, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("stop_task: %d %q", rec.Code, rec.Body.String())
	}
	if store.Count() != 0 {
		t.Fatalf("tombstones after stop_task = %d, want 0", store.Count())
	}
	rec = publicDeleteRequest(mux, http.MethodPost, "/delete/stop_task", url.Values{}, nil)
	if rec.Code != http.StatusBadRequest || rec.Body.String() != "missing task_id arg\n" {
		t.Fatalf("stop_task without task_id: %d %q, want upstream's answer", rec.Code, rec.Body.String())
	}
	rec = publicDeleteRequest(mux, http.MethodPost, "/delete/run_task", url.Values{"filter": {"level:("}}, nil)
	if rec.Code != http.StatusBadRequest || !strings.HasPrefix(rec.Body.String(), "cannot parse filter [level:(]") {
		t.Fatalf("run_task with a bad filter: %d %q, want upstream's answer", rec.Code, rec.Body.String())
	}
}
