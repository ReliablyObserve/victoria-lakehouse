package delete

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandler_TraceMode_TombstoneByIDAndUndelete: the by-id route is registered
// under the mode's own prefix (/delete/tracessql/tombstone/), so the id must be
// sliced off that prefix. Slicing it off the logs prefix turned
// "/delete/tracessql/tombstone/<id>" into "ne/<id>", so looking a tombstone up —
// and un-deleting it — always 404'd on the traces binary.
func TestHandler_TraceMode_TombstoneByIDAndUndelete(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "trace-ts-1", Query: `service.name:="x"`, StartNs: 0, EndNs: 10, Mode: "hide"})

	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	get := httptest.NewRecorder()
	mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/delete/tracessql/tombstone/trace-ts-1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET tombstone by id on the traces binary: %d %s", get.Code, get.Body.String())
	}

	del := httptest.NewRecorder()
	mux.ServeHTTP(del, httptest.NewRequest(http.MethodDelete, "/delete/tracessql/tombstone/trace-ts-1", nil))
	if del.Code != http.StatusOK {
		t.Fatalf("un-delete on the traces binary: %d %s", del.Code, del.Body.String())
	}
	if store.Count() != 0 {
		t.Fatal("the un-delete did not remove the tombstone")
	}
}

func TestHandler_LogsMode_TombstoneByIDStillWorks(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "logs-ts-1", Query: "*", StartNs: 0, EndNs: 10, Mode: "hide"})
	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	mux := http.NewServeMux()
	h.Register(mux)

	get := httptest.NewRecorder()
	mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/delete/logsql/tombstone/logs-ts-1", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET tombstone by id on the logs binary: %d %s", get.Code, get.Body.String())
	}

	missing := httptest.NewRecorder()
	mux.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/delete/logsql/tombstone/", nil))
	if missing.Code != http.StatusBadRequest {
		t.Fatalf("an empty id must be a 400, got %d", missing.Code)
	}
}

// TestHandler_ListTombstones_ReportsDurability: the listing tells an operator
// whether the tombstones it shows would survive a pod loss — persistence armed,
// and how many records are still owed to S3.
func TestHandler_ListTombstones_ReportsDurability(t *testing.T) {
	store := NewTombstoneStore()
	pool := newFlakyS3Pool()
	pool.failUploads = 1
	store.EnablePersistence(PersistenceConfig{Dir: t.TempDir(), Pool: pool, Prefix: "logs/"})
	store.Add(Tombstone{ID: "ts", Query: "*", StartNs: 0, EndNs: 10, Mode: "hide"})

	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	mux := http.NewServeMux()
	h.Register(mux)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/delete/logsql/tombstones", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("list tombstones: %d", w.Code)
	}
	var body struct {
		Count       int `json:"count"`
		Persistence struct {
			Enabled         bool `json:"enabled"`
			PendingS3Writes int  `json:"pending_s3_writes"`
		} `json:"persistence"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Count != 1 || !body.Persistence.Enabled || body.Persistence.PendingS3Writes != 1 {
		t.Fatalf("listing = %+v, want count 1, persistence enabled, 1 pending S3 write", body)
	}
}
