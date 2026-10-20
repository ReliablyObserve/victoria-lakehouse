package compaction

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
)

// Deletes only hide rows at read time. Compaction copies every row forward and
// has no view of the tombstones: it cannot drop, retire or complete one, and it
// cannot decide which rows a delete covers. This test fails if a tombstone store,
// or any type of internal/delete, is wired into the compactor, the scheduler or
// the orphan sweep again. That is the regression class the compaction tombstone
// races of the earlier design guarded (a compaction retiring a tombstone before a
// complete listing, a merge completing a delete while its state was incomplete).
// Physical removal, when it returns, replaces this lock with a race test of
// compaction applying only the tombstones it knows.
func TestCompactorConfig_HasNoTombstoneView(t *testing.T) {
	for _, v := range []any{CompactorConfig{}, Compactor{}, SchedulerConfig{}, Scheduler{}, OrphanSweepConfig{}, OrphanSweep{}} {
		typ := reflect.TypeOf(v)
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if mentionsDeletePackage(f.Type, map[reflect.Type]bool{}) {
				t.Errorf("%s.%s (%s) reaches internal/delete: compaction must not hold a tombstone view", typ.Name(), f.Name, f.Type)
			}
		}
	}
}

func mentionsDeletePackage(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return false
	}
	seen[t] = true
	if strings.HasSuffix(t.PkgPath(), "/internal/delete") {
		return true
	}
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Chan:
		return mentionsDeletePackage(t.Elem(), seen)
	case reflect.Map:
		return mentionsDeletePackage(t.Key(), seen) || mentionsDeletePackage(t.Elem(), seen)
	case reflect.Interface:
		// A locally declared interface over a delete type (a method that takes or
		// returns one) is a tombstone view as much as the store itself is.
		for i := 0; i < t.NumMethod(); i++ {
			if mentionsDeletePackage(t.Method(i).Type, seen) {
				return true
			}
		}
	case reflect.Func:
		for i := 0; i < t.NumIn(); i++ {
			if mentionsDeletePackage(t.In(i), seen) {
				return true
			}
		}
		for i := 0; i < t.NumOut(); i++ {
			if mentionsDeletePackage(t.Out(i), seen) {
				return true
			}
		}
	}
	return false
}

// The walk itself: it must see a delete type behind a pointer, a map, a func
// signature and, as a mutant once slipped past, a method of a locally declared
// interface.
func TestNoTombstoneView_WalkSeesDeleteTypesBehindInterfaces(t *testing.T) {
	type viaInterface interface {
		Active() []delete.Tombstone
	}
	type viaInterfaceParam interface {
		Forget(ts *delete.Tombstone) error
	}
	for name, typ := range map[string]reflect.Type{
		"pointer":          reflect.TypeOf((*delete.TombstoneStore)(nil)),
		"map":              reflect.TypeOf(map[string]delete.Tombstone{}),
		"func":             reflect.TypeOf(func(delete.Tombstone) {}),
		"interface result": reflect.TypeOf((*viaInterface)(nil)).Elem(),
		"interface param":  reflect.TypeOf((*viaInterfaceParam)(nil)).Elem(),
	} {
		if !mentionsDeletePackage(typ, map[reflect.Type]bool{}) {
			t.Errorf("the walk does not see a delete type behind a %s", name)
		}
	}
	if mentionsDeletePackage(reflect.TypeOf((*interface{ Close() error })(nil)).Elem(), map[reflect.Type]bool{}) {
		t.Error("an interface without delete types was reported")
	}
}
