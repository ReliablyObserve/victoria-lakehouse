//go:build e2e

package e2e

import "testing"

// The e2e stray-object scan accepts exactly the two tombstone key shapes.
func TestOwnTombstoneKeyShapes(t *testing.T) {
	for key, want := range map[string]bool{
		"0/0/logs/_tombstones/t18df901fb79401d8-8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22.json": true,
		"0/0/logs/_tombstones/t18df901fb79401d8-task%2D1.fate":                             false, // earlier protocol
		"0/0/traces/_tombstones/t18df901fb79401d8-x.undo":                                  true,
		"1/1/logs/_tombstones/t18df901fb79401d8-x.done":                                    false, // earlier protocol
		"0/0/logs/_tombstones/8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22.json":                   false, // previous protocol
		"0/0/logs/_tombstones/t18df901fb79401d8-x.json.tmp":                                false,
		"0/0/logs/_tombstones/t18DF901fb79401d8-x.json":                                    false,
		"0/0/logs/_tombstones/sub/t18df901fb79401d8-x.json":                                false,
		"logs/_tombstones/t18df901fb79401d8-x.json":                                        false, // not under a tenant prefix
	} {
		if got := isOwnMetadata(key); got != want {
			t.Errorf("isOwnMetadata(%q) = %v, want %v", key, got, want)
		}
	}
}
