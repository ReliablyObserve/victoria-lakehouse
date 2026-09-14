package storage

import (
	"context"
	"testing"
)

// The global-read marker is the only thing that widens a read past the
// request's tenant, so it must be absent by default and survive derived
// contexts.
func TestGlobalReadMarker(t *testing.T) {
	if IsGlobalRead(context.Background()) {
		t.Fatal("a plain context must not carry the global-read marker")
	}
	ctx := WithGlobalRead(context.Background())
	if !IsGlobalRead(ctx) {
		t.Fatal("WithGlobalRead must mark the context")
	}
	derived, cancel := context.WithCancel(WithTimestampOnlyHint(ctx))
	defer cancel()
	if !IsGlobalRead(derived) {
		t.Error("the marker must survive derived contexts")
	}
	if IsGlobalRead(WithTimestampOnlyHint(context.Background())) {
		t.Error("another hint must not be mistaken for the global-read marker")
	}
}
