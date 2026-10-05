package startup

import "testing"

func BenchmarkReadyGates(b *testing.B) {
	m := NewManager(1)
	m.SetManifestFiles(1)
	m.SetWALReplayNeeded()
	m.SetWALReplayDone()
	m.SetServingReady()
	m.SetWarmupComplete()
	for _, tc := range []struct {
		name string
		read func() bool
	}{{"Combined", m.IsReady}, {"Serving", m.ServingReady}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if !tc.read() {
					b.Fatal("completed startup must be ready")
				}
			}
		})
	}
}
