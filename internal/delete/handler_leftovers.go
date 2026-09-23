package delete

import (
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The leftovers endpoint.
//
// Three kinds of object outlive the step that created them: a key the manifest
// retired while its delete is still owed, an upload that was claimed but never
// published, and a rewrite whose record is still on its tombstone. Each is
// counted by a metric and each is bounded, but until now nothing could NAME
// them — the alert on `lakehouse_manifest_retired_evicted_total` told an
// operator to delete the leftovers without any way to list them, and an
// unfinished rewrite could only be found by reading `tombstones.json`.
//
// GET {prefix}/leftovers answers exactly that question, read-only: it reports
// what this instance is still holding on to and why. A tenant caller sees the
// entries of its own objects and tombstones ("scope": "tenant"); only the
// validated global-read credential sees the whole instance ("scope":
// "instance"), the same split as the tombstone listing (handler_scope.go).

// LeftoverLister is the slice of the manifest the leftovers endpoint needs. The
// handler takes it from its ManifestQuerier when that value provides it, so an
// embedder that wires only the query surface simply gets empty lists.
type LeftoverLister interface {
	RetiredKeys() []manifest.RetiredKey
	PendingKeys() []manifest.PendingKey
}

// defaultLeftoverLimit / maxLeftoverLimit bound the response: the retired set
// alone is capped at 100k keys, and an operator endpoint must not turn that into
// a multi-megabyte JSON body by default.
const (
	defaultLeftoverLimit = 1000
	maxLeftoverLimit     = 10000
)

type retiredKeyView struct {
	Key        string    `json:"key"`
	RetiredAt  time.Time `json:"retired_at"`
	ReplacedBy string    `json:"replaced_by,omitempty"`
	DeleteOwed bool      `json:"delete_owed"`
	// Deleted marks a key whose object is already gone and which is held only
	// so a bucket listing that began before the delete cannot adopt it back.
	// Nothing is owed for it; the next accepted refresh drops it.
	Deleted bool `json:"deleted,omitempty"`
}

type pendingKeyView struct {
	Key       string    `json:"key"`
	ClaimedAt time.Time `json:"claimed_at"`
	Held      bool      `json:"held"`
}

// RewriteRecord is one unfinished rewrite: the durable record that says what
// must still happen to a pair of objects.
type RewriteRecord struct {
	Tombstone   string    `json:"tombstone"`
	Source      string    `json:"source"`
	Replacement string    `json:"replacement,omitempty"`
	State       string    `json:"state"`
	At          time.Time `json:"at"`
}

// UnfinishedRewriteRecords lists every rewrite record the store holds, sorted by
// tombstone and source so the output is stable between calls.
func (s *TombstoneStore) UnfinishedRewriteRecords() []RewriteRecord {
	var out []RewriteRecord
	s.mu.RLock()
	for id, ts := range s.tombstones {
		for source, sup := range ts.Superseded {
			out = append(out, RewriteRecord{
				Tombstone: id, Source: source, Replacement: sup.NewKey,
				State: sup.State, At: sup.At,
			})
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tombstone != out[j].Tombstone {
			return out[i].Tombstone < out[j].Tombstone
		}
		return out[i].Source < out[j].Source
	})
	return out
}

// handleLeftovers serves the read-only listing of what this instance still owes
// work on.
func (h *Handler) handleLeftovers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	caller, ok := h.callerOrError(w, r)
	if !ok {
		return
	}
	parse := h.keyTenant()
	limit := defaultLeftoverLimit
	if v := r.FormValue("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			http.Error(w, "invalid limit parameter", http.StatusBadRequest)
			return
		}
		limit = min(n, maxLeftoverLimit)
	}

	var (
		retired            []retiredKeyView
		pending            []pendingKeyView
		owed, landed, held int
		nRetired           int
		nPending           int
	)
	if lister, ok := h.manifest.(LeftoverLister); ok {
		var all []manifest.RetiredKey
		for _, rk := range lister.RetiredKeys() {
			if caller.ownsKey(parse, rk.Key) {
				all = append(all, rk)
			}
		}
		nRetired = len(all)
		for _, rk := range all {
			if rk.Reclaim {
				owed++
			}
			if rk.Deleted {
				landed++
			}
			if len(retired) < limit {
				retired = append(retired, retiredKeyView{
					Key: rk.Key, RetiredAt: rk.At, ReplacedBy: rk.By,
					DeleteOwed: rk.Reclaim, Deleted: rk.Deleted,
				})
			}
		}
		var allPending []manifest.PendingKey
		for _, pk := range lister.PendingKeys() {
			if caller.ownsKey(parse, pk.Key) {
				allPending = append(allPending, pk)
			}
		}
		nPending = len(allPending)
		for _, pk := range allPending {
			if pk.Held {
				held++
			}
			if len(pending) < limit {
				pending = append(pending, pendingKeyView{Key: pk.Key, ClaimedAt: pk.At, Held: pk.Held})
			}
		}
	}

	var rewrites []RewriteRecord
	for _, rec := range h.store.UnfinishedRewriteRecords() {
		if ts, found := h.store.Get(rec.Tombstone); caller.global || (found && caller.sees(&ts)) {
			rewrites = append(rewrites, rec)
		}
	}
	nRewrites := len(rewrites)
	if len(rewrites) > limit {
		rewrites = rewrites[:limit]
	}

	writeJSON(w, http.StatusOK, map[string]any{
		// Say which view this is, so a tenant's own view is never mistaken
		// for the instance's.
		"scope":               caller.scopeName(),
		"retired_keys":        retired,
		"pending_keys":        pending,
		"unfinished_rewrites": rewrites,
		"counts": map[string]int{
			"retired":               nRetired,
			"retired_delete_owed":   owed,
			"retired_delete_landed": landed,
			"pending":               nPending,
			"pending_held":          held,
			"unfinished_rewrites":   nRewrites,
		},
		"truncated": nRetired > len(retired) || nPending > len(pending) || nRewrites > len(rewrites),
		"limit":     limit,
		"tombstone_store": map[string]any{
			"persistence_enabled": h.store.PersistenceEnabled(),
			"pending_s3_writes":   h.store.PendingS3Writes(),
			"s3_restore_pending":  h.store.S3RestorePending(),
		},
	})
}
