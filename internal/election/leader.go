// internal/election/leader.go
package election

import "context"

type Leader interface {
	IsLeader() bool
	Start(ctx context.Context)
	Stop()
}
