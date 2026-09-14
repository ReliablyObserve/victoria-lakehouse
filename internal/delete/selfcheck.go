package delete

import (
	"fmt"
	"sort"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Inconsistency is one disagreement between the restored tombstones and the
// restored manifest.
type Inconsistency struct {
	// Kind is the machine-readable classification, also used as the
	// lakehouse_delete_startup_inconsistencies_total{kind=...} label.
	Kind string
	// TombstoneID and Key name the record and file involved.
	TombstoneID string
	Key         string
	// Detail explains what an operator should expect as a consequence.
	Detail string
}

func (i Inconsistency) String() string {
	return fmt.Sprintf("%s: tombstone=%s key=%s — %s", i.Kind, i.TombstoneID, i.Key, i.Detail)
}

// SelfCheck compares the restored tombstone state against the restored manifest
// and reports every disagreement.
//
// Both halves are restored independently at boot — the manifest from its
// snapshot or an S3 listing, the tombstones from disk and S3 — so they can come
// back describing different moments in time. Silence about that was how a
// half-finished rewrite stayed invisible until a query returned the wrong rows.
// Each finding is logged and counted by kind so an operator sees it in metrics
// without reading logs.
//
// Findings are advisory: none of them is fixed here, because every fix is a
// data movement that belongs to the scheduler's normal retry path. The check is
// there to make the state observable, and to make the tests able to assert that
// a recovered process really did converge.
func SelfCheck(store *TombstoneStore, m ManifestUpdater) []Inconsistency {
	var found []Inconsistency

	if store == nil {
		return nil
	}

	if !store.PersistenceEnabled() {
		found = append(found, Inconsistency{
			Kind:   "persistence_disabled",
			Detail: "tombstone store has no durable target; deletes will be lost on an ungraceful restart",
		})
	}

	if m != nil {
		for _, ts := range store.Active() {
			for _, key := range ts.AffectedKeys {
				reaped := ts.Reaped[key]
				clean := ts.Clean[key]
				present := m.HasKey(key)
				switch {
				case reaped && present:
					// The key was recorded as rewritten but the manifest still
					// serves it: the tombstone state is ahead of the manifest
					// snapshot. The query-time filter still hides the rows, and
					// the file will be rewritten again on a later delete.
					found = append(found, Inconsistency{
						Kind:        "reaped_key_still_manifested",
						TombstoneID: ts.ID,
						Key:         key,
						Detail:      "tombstone records this key as rewritten but the manifest still lists it; rows stay hidden by the query filter",
					})
				case !reaped && !clean && !present:
					// The manifest no longer has the key the tombstone wants to
					// rewrite. The scheduler's self-healing path marks it reaped
					// on the next tick rather than retrying a download forever.
					found = append(found, Inconsistency{
						Kind:        "pending_key_missing_from_manifest",
						TombstoneID: ts.ID,
						Key:         key,
						Detail:      "tombstone still lists this key as pending but the manifest does not have it; the scheduler will mark it reaped",
					})
				}
			}
		}
	}

	sort.Slice(found, func(i, j int) bool {
		if found[i].Kind != found[j].Kind {
			return found[i].Kind < found[j].Kind
		}
		if found[i].TombstoneID != found[j].TombstoneID {
			return found[i].TombstoneID < found[j].TombstoneID
		}
		return found[i].Key < found[j].Key
	})

	for _, f := range found {
		metrics.DeleteStartupInconsistencies.Inc(f.Kind)
		logger.Warnf("delete self-check: %s", f)
	}
	if len(found) == 0 {
		logger.Infof("delete self-check: tombstones and manifest agree; tombstones=%d", store.Count())
	}
	return found
}
