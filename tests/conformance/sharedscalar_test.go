package conformance

import (
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"
	"unsafe"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// TestSharedScalarTypeNamesMatchStdlib guards the one reflect/unsafe assumption
// in flag_dedup.go.src: sharedScalar reads the pointer the standard library's
// flag package keeps for a scalar flag, identified by the unexported type's
// name (`*flag.intValue` ...) and checked by size. If a Go release renames or
// reshapes those types the helper silently stops sharing VL's value (an
// operator's setting no longer reaches VT), so the names written in the .src
// are compared here with what this toolchain really registers. See
// patches/README.md, "Fragile by design: sharedScalar".
func TestSharedScalarTypeNamesMatchStdlib(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(root, "patches", "vt-traces", "flag_dedup.go.src"))
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`sharedScalar\[([A-Za-z.]+)\]\(f, "([^"]+)"\)`)
	found := re.FindAllStringSubmatch(string(src), -1)
	if len(found) != 4 {
		t.Fatalf("expected the 4 sharedScalar uses (int, bool, string, time.Duration), found %d", len(found))
	}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	_ = fs.Int("i", 0, "")
	_ = fs.Bool("b", false, "")
	_ = fs.String("s", "", "")
	_ = fs.Duration("d", 0, "")
	real := map[string]struct {
		name string
		size uintptr
	}{
		"int":           {"i", unsafe.Sizeof(int(0))},
		"bool":          {"b", unsafe.Sizeof(false)},
		"string":        {"s", unsafe.Sizeof("")},
		"time.Duration": {"d", unsafe.Sizeof(time.Duration(0))},
	}
	for _, m := range found {
		typ, want := m[1], m[2]
		r, ok := real[typ]
		if !ok {
			t.Fatalf("unexpected sharedScalar type %q", typ)
		}
		v := fs.Lookup(r.name).Value
		if got := reflect.TypeOf(v).String(); got != want {
			t.Errorf("sharedScalar[%s] expects %q but this Go registers %q: update flag_dedup.go.src (patches/README.md, sharedScalar)", typ, want, got)
		}
		if reflect.TypeOf(v).Elem().Size() != r.size {
			t.Errorf("sharedScalar[%s]: flag value size %d != scalar size %d", typ, reflect.TypeOf(v).Elem().Size(), r.size)
		}
	}
}
