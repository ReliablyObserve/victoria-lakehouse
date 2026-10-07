//go:build parity

package parity

import (
	"strings"
	"testing"
)

// reportLockCells records that the test running t compared n more cells. A cell
// is one comparison of hot against cold: a case, a query, a row set.
//
// The Parity Tests job (scripts/ci/parity_ratchet.py --lock-cells) holds every
// lock test listed in lock_cells.txt to a minimum number of reported cells, so a
// lock that is skipped, emptied, made to return early, constrained away by a
// build tag or file name, or silenced by a TestMain reports too few cells and
// fails the job, however the test file was edited. The floor only grows: a
// parity fix that adds cells passes freely; lowering it needs the owner.
//
// This file must not call t.Helper: go test then prefixes the line with this
// file's name, and the ratchet accepts a lock-cells line only with that prefix,
// so a test cannot print the line itself. The file is a gate file.
func reportLockCells(t *testing.T, n int) {
	root, _, _ := strings.Cut(t.Name(), "/")
	t.Logf("lock-cells %s %d", root, n)
}
