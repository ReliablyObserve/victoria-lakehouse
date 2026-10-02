package main

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// A select pod configured only with select.insert_headless_service must run
// the discovery loop, or its buffer bridge never learns the insert pods.
func TestDiscoveryEnabled(t *testing.T) {
	for _, c := range []struct {
		peer, insert string
		want         bool
	}{
		{"", "", false},
		{"peers", "", true},
		{"", "lh-logs-insert-headless:9428", true},
		{"peers", "lh-logs-insert-headless:9428", true},
	} {
		cfg := &config.Config{}
		cfg.Discovery.PeerHeadlessService = c.peer
		cfg.Select.InsertHeadlessService = c.insert
		if got := discoveryEnabled(cfg); got != c.want {
			t.Errorf("peer=%q insert=%q: discoveryEnabled = %v, want %v", c.peer, c.insert, got, c.want)
		}
	}
}
