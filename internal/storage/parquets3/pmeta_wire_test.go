package parquets3

import (
	"reflect"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestPmetaWire_DisabledIsNil verifies the feature is inert when off.
func TestPmetaWire_DisabledIsNil(t *testing.T) {
	if newCatalogStore(config.PmetaConfig{}, "logs/") != nil {
		t.Fatal("newCatalogStore(false) must return nil so the hot paths stay unchanged")
	}
}

// TestPmetaWire_ObserverFeedsCatalog verifies the flush observer populates the
// catalog and it is queryable through the Store's public read API.
func TestPmetaWire_ObserverFeedsCatalog(t *testing.T) {
	store := newCatalogStore(config.PmetaConfig{Enabled: true}, "logs/")
	if store == nil {
		t.Fatal("newCatalogStore(true) returned nil")
	}
	obs := &catalogObserver{store: store}

	part := "logs/dt=2026-06-09/hour=10"
	obs.OnFileFlush(part, manifest.FileInfo{Key: "f1"}, map[string][]string{
		"service.name": {"api-gateway", "order-service"},
		"level":        {"ERROR"},
	}, nil)
	obs.OnFileFlush(part, manifest.FileInfo{Key: "f2"}, map[string][]string{
		"service.name": {"user-service"},
	}, nil)

	if got, want := store.FieldValues(part, "service.name", "", 0),
		[]string{"api-gateway", "order-service", "user-service"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FieldValues = %v, want %v", got, want)
	}
	if got := store.FieldValues(part, "service.name", "order", 0); !reflect.DeepEqual(got, []string{"order-service"}) {
		t.Fatalf("typeahead 'order' = %v", got)
	}
	if got, want := store.FieldNames(part), []string{"level", "service.name"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FieldNames = %v, want %v", got, want)
	}
	// Unknown partition / field → empty (caller falls through to legacy scan).
	if v := store.FieldValues("nope", "service.name", "", 0); len(v) != 0 {
		t.Fatalf("unknown partition returned %v", v)
	}
}

// TestPmetaWire_NilObserverSafe — a nil observer (pmeta off) must be a no-op.
func TestPmetaWire_NilObserverSafe(t *testing.T) {
	var obs *catalogObserver
	obs.OnFileFlush("p", manifest.FileInfo{Key: "f"}, map[string][]string{"x": {"y"}}, nil) // must not panic
}
