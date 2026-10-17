package selfaddr

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// memoTTL is how long a decision for an unchanged member list is reused. The
// Env changes slowly (interfaces, DNS), and the lists are looked at once per
// partition per scan.
const memoTTL = 5 * time.Second

// Tracker resolves this pod in the member list of one ring, remembers the
// answer for an unchanged list, reports it through two gauges and says why in
// the log once whenever it changes.
//
//	lakehouse_ring_self_resolved{ring}  1 when exactly one member is this pod.
//	lakehouse_ring_self_in_peers{ring}  1 when that member is also in the set
//	                                    the ring hashes over (SetInPeers); a pod
//	                                    that is draining or outside the AZ
//	                                    subset resolves but is not in the set.
type Tracker struct {
	ring   string
	listen string
	env    Env

	mu      sync.Mutex
	members []string
	at      time.Time
	res     Result
	have    bool
	logged  string

	resolved atomic.Int64
	inPeers  atomic.Int64
}

// NewTracker returns a Tracker for the ring named ring ("compaction",
// "peercache") of a pod listening on listen.
func NewTracker(ring, listen string, env Env) *Tracker {
	t := &Tracker{ring: ring, listen: listen, env: env}
	t.inPeers.Store(-1) // not reported yet
	return t
}

// Resolve is Match for members, memoised while members is unchanged and the
// memo is younger than memoTTL.
func (t *Tracker) Resolve(members []string) Result {
	now := t.env.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.have && now.Sub(t.at) < memoTTL && sameList(t.members, members) {
		return t.res
	}
	res := Match(t.listen, members, t.env)
	t.members = append(t.members[:0], members...)
	t.at, t.res, t.have = now, res, true
	t.report(res)
	return res
}

// SetInPeers records whether the resolved member is in the set the ring hashes
// over.
func (t *Tracker) SetInPeers(in bool) {
	v := int64(0)
	if in {
		v = 1
	}
	if t.inPeers.Swap(v) != v { // also true the first time (-1)
		metrics.RingSelfInPeers.Set(t.ring, v)
	}
}

// Resolved is the last decision of Resolve as 0 or 1.
func (t *Tracker) Resolved() int64 { return t.resolved.Load() }

// InPeers is the last SetInPeers as 0 or 1.
func (t *Tracker) InPeers() int64 { return max(t.inPeers.Load(), 0) }

// report publishes the gauge and logs when the decision changed. An empty list
// (nothing discovered yet) says nothing about this pod and publishes nothing.
// Called with t.mu held.
func (t *Tracker) report(res Result) {
	if res.Members == 0 {
		return
	}
	v := int64(0)
	if res.OK() {
		v = 1
	}
	t.resolved.Store(v)
	metrics.RingSelfResolved.Set(t.ring, v)

	state := fmt.Sprintf("%s|%s|%d", res.Reason(), res.Self, res.Members)
	if state == t.logged {
		return
	}
	t.logged = state
	if res.OK() {
		logger.Infof("%s ring: this pod (listen %s) is %s among %d members", t.ring, t.listen, res.Self, res.Members)
		return
	}
	logger.Warnf("%s ring: this pod (listen %s) is %s among %d discovered members %s, so it owns nothing in this ring until it is found; "+
		"set POD_IP / POD_IPS (the chart does) or check that the headless service lists this pod by an address or name it can recognise",
		t.ring, t.listen, describe(res), res.Members, sample(t.members))
}

func describe(res Result) string {
	if res.Matches > 1 {
		return fmt.Sprintf("ambiguous (%d members match it)", res.Matches)
	}
	return "not listed (no member matches its port with one of its addresses or names)"
}

// sample is the first few members, for the log.
func sample(members []string) string {
	const max = 5
	if len(members) <= max {
		return "[" + strings.Join(members, " ") + "]"
	}
	return fmt.Sprintf("[%s ... +%d]", strings.Join(members[:max], " "), len(members)-max)
}

func sameList(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
