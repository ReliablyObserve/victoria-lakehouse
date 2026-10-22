package peerauth

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Report publishes the peer authentication state of this process once at
// startup: the lakehouse_peer_auth_enabled gauge, and one warning when the
// pod's internal endpoints take requests without a credential although other
// pods reach them (peers discovered, or a split insert/select deployment).
//
//   - enabled: peer.auth_enabled, the pod refuses requests without the key;
//   - keySet: peer.auth_key is set, the pod sends it on its own peer calls;
//   - talksToPeers: the process has peers or a role that uses the internal
//     endpoints.
func Report(enabled, keySet, talksToPeers bool) {
	if enabled {
		metrics.PeerAuthEnabled.Set(1)
		return
	}
	metrics.PeerAuthEnabled.Set(0)
	if !talksToPeers {
		return
	}
	if keySet {
		logger.Warnf("peer authentication is not enforced: peer.auth_key is set and sent on this pod's peer calls, but the internal endpoints " +
			"(/internal/manifest/update, /internal/cache/*, /internal/stats/sync, /internal/tenant/sync, /internal/buffer/query) still take requests without it; " +
			"set peer.auth_enabled (-lakehouse.peer.auth-enabled) on every pod once all of them hold the key (docs/security.md)")
		return
	}
	logger.Warnf("peer authentication is off: any client that can reach this pod's port can call its internal endpoints " +
		"(/internal/manifest/update, /internal/cache/*, /internal/stats/sync, /internal/tenant/sync, /internal/buffer/query); " +
		"set peer.auth_key and peer.auth_enabled (-lakehouse.peer.auth-key, -lakehouse.peer.auth-enabled), or keep the pods on a protected network (docs/security.md)")
}
