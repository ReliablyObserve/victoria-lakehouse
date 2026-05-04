// internal/election/noop.go
package election

import "context"

type NoopElector struct{}

func NewNoopElector() *NoopElector {
	return &NoopElector{}
}

func (n *NoopElector) IsLeader() bool         { return true }
func (n *NoopElector) Start(_ context.Context) {}
func (n *NoopElector) Stop()                  {}
