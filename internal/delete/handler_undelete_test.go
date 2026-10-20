package delete

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

func mustCreate(t *testing.T, s *TombstoneStore, ts Tombstone) Tombstone {
	t.Helper()
	got, err := s.Create(context.Background(), ts)
	if err != nil {
		t.Fatalf("create %s: %v", ts.ID, err)
	}
	return got
}

// TestHandler_TraceMode_TombstoneByIDAndUndelete: the by-id route is registered
// under the mode's own prefix (/delete/tracessql/tombstone/), so the id must be
// sliced off that prefix. Slicing it off the logs prefix turned
// "/delete/tracessql/tombstone/<id>" into "ne/<id>", so looking a tombstone up —
// and un-deleting it — always 404'd on the traces binary.
func TestHandler_TraceMode_TombstoneByIDAndUndelete(t *testing.T) {
	store := newTestStore(t)
	mustCreate(t, store, Tombstone{ID: "trace-ts-1", Query: `service.name:="x"`, StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{}}})

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
	store := newTestStore(t)
	mustCreate(t, store, Tombstone{ID: "logs-ts-1", Query: "*", StartNs: 0, EndNs: 10, Mode: "hide", Tenants: []TenantRef{{}}})
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

// A delete the object store cannot take is an error to the caller; nothing is
// queued and nothing is enforced that was not stored.
func TestHandler_Delete_StoreFailureIsAnErrorAndNothingIsHeld(t *testing.T) {
	b := deletetest.NewBucket()
	store, pod := newSyncedPod(t, b, "pod")
	pod.FailNext("put", 1)
	h := NewHandler(store, &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/delete/logsql/delete", strings.NewReader("query=error&start=0&end=100&mode=hide"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("a failed write answered %d %s, want 502", w.Code, w.Body.String())
	}
	if store.Count() != 0 || len(b.Keys(TombstonePrefix(testPrefix))) != 0 {
		t.Fatal("a delete that was not stored is held in the view or the bucket")
	}
}
