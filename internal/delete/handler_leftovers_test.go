package delete

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The alert on retired keys tells an operator to deal with the leftovers, so
// there has to be a way to see them. These tests pin what the listing reports
// and that it never becomes a way to download the whole retired set.

// leftoverManifest is a ManifestQuerier that also implements LeftoverLister,
// backed by a real manifest so the views describe real bookkeeping.
type leftoverManifest struct {
	m *lhmanifest.Manifest
}

func (l *leftoverManifest) GetFilesForRange(startNs, endNs int64) []FileInfo {
	var out []FileInfo
	for _, fi := range l.m.GetFilesForRange(startNs, endNs) {
		out = append(out, FileInfo{Key: fi.Key, Size: fi.Size, MinTimeNs: fi.MinTimeNs, MaxTimeNs: fi.MaxTimeNs})
	}
	return out
}

func (l *leftoverManifest) RetiredKeys() []lhmanifest.RetiredKey { return l.m.RetiredKeys() }
func (l *leftoverManifest) PendingKeys() []lhmanifest.PendingKey { return l.m.PendingKeys() }

func newLeftoverHandler(t *testing.T) (*Handler, *lhmanifest.Manifest, *TombstoneStore) {
	t.Helper()
	m := newTestManifest(t, map[string]int64{"logs/dt=2026-03-01/hour=07/live.parquet": 10})
	store := NewTombstoneStore()
	h := NewHandler(store, &leftoverManifest{m: m}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	return h, m, store
}

func TestLeftovers_ListsRetiredPendingAndUnfinishedRewrites(t *testing.T) {
	h, m, store := newLeftoverHandler(t)

	const (
		superseded  = "logs/dt=2026-03-01/hour=07/src-0001.parquet"
		replacement = "logs/dt=2026-03-01/hour=07/b2709b0d.parquet"
		claimed     = "logs/dt=2026-03-01/hour=07/0badc0de.parquet"
		byPeer      = "logs/dt=2026-03-01/hour=07/peer.parquet"
		settled     = "logs/dt=2026-03-01/hour=07/src-0002.parquet"
	)
	// A superseded object whose delete this process owes, one whose delete
	// landed (held only against a listing older than the delete), a key retired
	// on someone else's behalf, and an upload that is claimed and held.
	m.Retire(superseded, replacement, true)
	m.Retire(settled, replacement, true)
	m.ConfirmDeleted(settled)
	m.Retire(byPeer, "", false)
	if !m.ClaimPending(claimed) {
		t.Fatal("fixture: the claim was rejected")
	}
	m.Hold(claimed)

	store.Add(sampleTombstone("ts-leftover"))
	store.Update("ts-leftover", func(cur *Tombstone) bool {
		cur.Superseded = map[string]Supersession{
			superseded: {NewKey: replacement, State: SupersessionPublished, At: time.Now()},
		}
		return true
	})

	rec := getRequest(h.handleLeftovers, "/delete/logsql/leftovers")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET leftovers = %d (%s)", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body)

	if body["scope"] != "instance" {
		t.Errorf("the listing must say it is instance-wide, got scope=%v", body["scope"])
	}
	counts, _ := body["counts"].(map[string]any)
	for field, want := range map[string]float64{
		"retired": 3, "retired_delete_owed": 1, "retired_delete_landed": 1,
		"pending": 1, "pending_held": 1, "unfinished_rewrites": 1,
	} {
		if got, _ := counts[field].(float64); got != want {
			t.Errorf("counts[%q] = %v, want %v", field, counts[field], want)
		}
	}

	retired, _ := body["retired_keys"].([]any)
	if len(retired) != 3 {
		t.Fatalf("listed %d retired keys, want 3", len(retired))
	}
	var sawOwed, sawSettled bool
	for _, entry := range retired {
		e, _ := entry.(map[string]any)
		if e["key"] == superseded {
			sawOwed = true
			if e["delete_owed"] != true {
				t.Errorf("%s is owed a delete but the listing says %v", superseded, e["delete_owed"])
			}
			if e["replaced_by"] != replacement {
				t.Errorf("%s: replaced_by = %v, want %s", superseded, e["replaced_by"], replacement)
			}
			if e["deleted"] == true {
				t.Errorf("%s still has an object in the bucket; the listing must not call it deleted", superseded)
			}
		}
		if e["key"] == settled {
			sawSettled = true
			// An operator reading this must be able to tell "still in the
			// bucket, go delete it" from "already gone, nothing to do".
			if e["deleted"] != true {
				t.Errorf("%s: deleted = %v, want true — its delete landed", settled, e["deleted"])
			}
			if e["delete_owed"] != false {
				t.Errorf("%s: delete_owed = %v, want false — nothing is owed once the delete landed", settled, e["delete_owed"])
			}
		}
	}
	if !sawOwed {
		t.Errorf("the superseded key is not in the listing: %v", retired)
	}
	if !sawSettled {
		t.Errorf("the settled guard is not in the listing: %v", retired)
	}

	pending, _ := body["pending_keys"].([]any)
	if len(pending) != 1 {
		t.Fatalf("listed %d pending keys, want 1", len(pending))
	}
	if p, _ := pending[0].(map[string]any); p["key"] != claimed || p["held"] != true {
		t.Errorf("pending entry = %v, want the held claim on %s", pending[0], claimed)
	}

	rewrites, _ := body["unfinished_rewrites"].([]any)
	if len(rewrites) != 1 {
		t.Fatalf("listed %d unfinished rewrites, want 1", len(rewrites))
	}
	r, _ := rewrites[0].(map[string]any)
	if r["tombstone"] != "ts-leftover" || r["source"] != superseded ||
		r["replacement"] != replacement || r["state"] != SupersessionPublished {
		t.Errorf("rewrite record = %v", r)
	}

	ts, _ := body["tombstone_store"].(map[string]any)
	if ts["s3_restore_pending"] != false {
		t.Errorf("tombstone_store.s3_restore_pending = %v, want false", ts["s3_restore_pending"])
	}
}

// TestLeftovers_EmptyInstanceReportsNothingOwed is the negative control: with no
// leftovers the listing is empty and says so, rather than reporting stale state.
func TestLeftovers_EmptyInstanceReportsNothingOwed(t *testing.T) {
	h, _, _ := newLeftoverHandler(t)
	rec := getRequest(h.handleLeftovers, "/delete/logsql/leftovers")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET leftovers = %d (%s)", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body)
	counts, _ := body["counts"].(map[string]any)
	for field, v := range counts {
		if n, _ := v.(float64); n != 0 {
			t.Errorf("counts[%q] = %v on an instance with nothing outstanding", field, n)
		}
	}
	if body["truncated"] != false {
		t.Errorf("truncated = %v on an empty listing", body["truncated"])
	}
}

// TestLeftovers_IsBounded: the retired set is capped at 100k keys, so the
// endpoint must page rather than serialise all of it.
func TestLeftovers_IsBounded(t *testing.T) {
	h, m, _ := newLeftoverHandler(t)
	for i := 0; i < defaultLeftoverLimit+50; i++ {
		m.Retire("logs/dt=2026-03-01/hour=07/gone-"+strconv.Itoa(i)+".parquet", "", true)
	}

	rec := getRequest(h.handleLeftovers, "/delete/logsql/leftovers")
	body := decodeJSON(t, rec.Body)
	retired, _ := body["retired_keys"].([]any)
	if len(retired) != defaultLeftoverLimit {
		t.Fatalf("listed %d retired keys, want the default limit %d", len(retired), defaultLeftoverLimit)
	}
	if body["truncated"] != true {
		t.Error("a truncated listing must say so")
	}
	counts, _ := body["counts"].(map[string]any)
	if got, _ := counts["retired"].(float64); int(got) != defaultLeftoverLimit+50 {
		t.Errorf("counts.retired = %v, want the full %d even when the list is truncated", got, defaultLeftoverLimit+50)
	}

	// An explicit limit is honoured and clamped.
	rec = getRequest(h.handleLeftovers, "/delete/logsql/leftovers?limit=10")
	body = decodeJSON(t, rec.Body)
	if retired, _ = body["retired_keys"].([]any); len(retired) != 10 {
		t.Errorf("limit=10 returned %d entries", len(retired))
	}
	if rec = getRequest(h.handleLeftovers, "/delete/logsql/leftovers?limit=0"); rec.Code != http.StatusBadRequest {
		t.Errorf("limit=0 returned %d, want 400", rec.Code)
	}
	if rec = getRequest(h.handleLeftovers, "/delete/logsql/leftovers?limit=abc"); rec.Code != http.StatusBadRequest {
		t.Errorf("limit=abc returned %d, want 400", rec.Code)
	}
}

// TestLeftovers_RewritesAreOrderedAndBounded: the listing is read by an operator
// comparing it between calls, so the order is stable (tombstone, then source),
// and a store with many records in flight is paged like the other lists.
func TestLeftovers_RewritesAreOrderedAndBounded(t *testing.T) {
	h, _, store := newLeftoverHandler(t)
	const dir = "logs/dt=2026-03-01/hour=07/"

	for _, id := range []string{"ts-b", "ts-a"} {
		ts := sampleTombstone(id)
		store.Add(ts)
		store.Update(id, func(cur *Tombstone) bool {
			cur.Superseded = map[string]Supersession{
				dir + "src-2.parquet": {NewKey: dir + "new-2.parquet", State: SupersessionPrepared, At: time.Now()},
				dir + "src-1.parquet": {NewKey: dir + "new-1.parquet", State: SupersessionPublished, At: time.Now()},
			}
			return true
		})
	}

	recs := store.UnfinishedRewriteRecords()
	if len(recs) != 4 {
		t.Fatalf("listed %d rewrite records, want 4", len(recs))
	}
	want := [][2]string{
		{"ts-a", dir + "src-1.parquet"},
		{"ts-a", dir + "src-2.parquet"},
		{"ts-b", dir + "src-1.parquet"},
		{"ts-b", dir + "src-2.parquet"},
	}
	for i, w := range want {
		if recs[i].Tombstone != w[0] || recs[i].Source != w[1] {
			t.Fatalf("record %d = (%s, %s), want (%s, %s)", i, recs[i].Tombstone, recs[i].Source, w[0], w[1])
		}
	}

	rec := getRequest(h.handleLeftovers, "/delete/logsql/leftovers?limit=2")
	body := decodeJSON(t, rec.Body)
	listed, _ := body["unfinished_rewrites"].([]any)
	if len(listed) != 2 {
		t.Fatalf("limit=2 listed %d rewrites", len(listed))
	}
	if body["truncated"] != true {
		t.Error("a truncated rewrite list must say so")
	}
	counts, _ := body["counts"].(map[string]any)
	if got, _ := counts["unfinished_rewrites"].(float64); got != 4 {
		t.Errorf("counts.unfinished_rewrites = %v, want the full 4", got)
	}
	// The limit is clamped rather than refused.
	rec = getRequest(h.handleLeftovers, "/delete/logsql/leftovers?limit=999999")
	body = decodeJSON(t, rec.Body)
	if got, _ := body["limit"].(float64); int(got) != maxLeftoverLimit {
		t.Errorf("limit = %v, want it clamped to %d", got, maxLeftoverLimit)
	}
}

// TestLeftovers_RejectsNonGET keeps it read-only: nothing here deletes anything.
func TestLeftovers_RejectsNonGET(t *testing.T) {
	h, _, _ := newLeftoverHandler(t)
	if rec := postForm(h.handleLeftovers, "/delete/logsql/leftovers", nil); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST leftovers = %d, want 405", rec.Code)
	}
	if rec := deleteRequest(h.handleLeftovers, "/delete/logsql/leftovers"); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE leftovers = %d, want 405", rec.Code)
	}
}

// TestLeftovers_WithoutAManifestListerStillServes: an embedder that wires only
// the query surface gets the tombstone half rather than a 500.
func TestLeftovers_WithoutAManifestListerStillServes(t *testing.T) {
	store := NewTombstoneStore()
	h := NewHandler(store, &mockManifest{files: testFiles()}, NewStorageClassDetector(nil), defaultCfg(), "logs")
	rec := getRequest(h.handleLeftovers, "/delete/logsql/leftovers")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET leftovers = %d (%s)", rec.Code, rec.Body.String())
	}
	body := decodeJSON(t, rec.Body)
	counts, _ := body["counts"].(map[string]any)
	if got, _ := counts["retired"].(float64); got != 0 {
		t.Errorf("counts.retired = %v without a manifest lister", got)
	}
}

// TestLeftovers_IsRegistered pins the route, since the alert text points at it.
func TestLeftovers_IsRegistered(t *testing.T) {
	for _, tc := range []struct{ mode, path string }{
		{"logs", "/delete/logsql/leftovers"},
		{"traces", "/delete/tracessql/leftovers"},
	} {
		mux := http.NewServeMux()
		NewHandler(NewTombstoneStore(), &mockManifest{}, NewStorageClassDetector(nil), defaultCfg(), tc.mode).Register(mux)
		rec := getRequest(mux.ServeHTTP, tc.path)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: GET %s = %d (%s)", tc.mode, tc.path, rec.Code, rec.Body.String())
		}
	}
}
