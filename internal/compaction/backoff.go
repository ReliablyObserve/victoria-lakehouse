package compaction

import (
	"sync"
	"time"
)

// maxPlanBackoff caps how long a failing plan waits before it is tried again.
const maxPlanBackoff = time.Hour

// planBackoff remembers merges that failed and keeps them out of the next
// scans for a growing interval (the scan interval, doubled per consecutive
// failure, at most an hour). Keyed by partition and tenant group; an entry
// leaves on success or when its plan no longer exists.
type planBackoff struct {
	mu    sync.Mutex
	state map[string]backoffState
}

type backoffState struct {
	failures int
	until    time.Time
}

func backoffKey(p mergePlan) string { return p.partition + "|" + p.group }

// filter drops plans still inside their backoff and forgets entries whose
// plan is gone, so the map stays bounded by the plans that keep failing.
func (b *planBackoff) filter(plans []mergePlan, now time.Time) []mergePlan {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.state) == 0 {
		return plans
	}
	live := make(map[string]bool, len(b.state))
	out := plans[:0]
	for _, p := range plans {
		k := backoffKey(p)
		st, ok := b.state[k]
		if ok {
			live[k] = true
			if now.Before(st.until) {
				continue
			}
		}
		out = append(out, p)
	}
	for k := range b.state {
		if !live[k] {
			delete(b.state, k)
		}
	}
	return out
}

func (b *planBackoff) failed(p mergePlan, now time.Time, interval time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == nil {
		b.state = make(map[string]backoffState)
	}
	k := backoffKey(p)
	st := b.state[k]
	st.failures++
	wait := interval
	if wait <= 0 {
		wait = time.Minute
	}
	for i := 1; i < st.failures && wait < maxPlanBackoff; i++ {
		wait *= 2
	}
	if wait > maxPlanBackoff {
		wait = maxPlanBackoff
	}
	st.until = now.Add(wait)
	b.state[k] = st
}

func (b *planBackoff) succeeded(p mergePlan) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.state, backoffKey(p))
}
