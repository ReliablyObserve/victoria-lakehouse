package parquets3

import (
	"fmt"
	"testing"
)

// The scheme of the plan is pinned: everything below decides how a segment's
// groups are numbered, so a change to any of it makes a drain that is in
// progress across the upgrade settle groups on the keys of other groups (#748).
// Changing this pin means bumping planScheme in the same commit (a process that
// finds a draining segment of another scheme stops at start).
func TestPlanScheme_PinsTheBucketScheme(t *testing.T) {
	got := fmt.Sprintf("scheme=%d buckets=%d div=%d key=%s pipes=%s delete=%s", planScheme, splitBuckets, splitBucketDiv, splitKeyFormat, splitBucketPipes, splitBucketDelete)
	const want = "scheme=1 buckets=1024 div=8796093022208 key=<_time>|<_stream_id>|<_msg> " +
		"pipes=format \"<_time>|<_stream_id>|<_msg>\" as _lh_split_k | hash(_lh_split_k) as _lh_split_h | math floor(_lh_split_h / 8796093022208) as _lh_split_b " +
		"delete=delete _lh_split_k, _lh_split_h, _lh_split_b"
	if got != want {
		t.Fatalf("the plan's bucket scheme changed without a planScheme bump:\n got  %s\n want %s", got, want)
	}
}
