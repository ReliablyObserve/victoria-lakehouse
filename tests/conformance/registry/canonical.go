package registry

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// canonicalBoolKeys are the registry keys that hold a boolean. YAML 1.1 spellings
// such as `yes` or `on` decode into a typed bool field (the loader) but into a
// string in a generic map (the gate), so the two would disagree on whether a row
// is pending. Only `true` and `false` are accepted.
var canonicalBoolKeys = []string{"pending"}

// CheckCanonical rejects registry YAML the loader and the gate could read
// differently: more than one document in a file, and a boolean key that is not
// spelled true or false.
func CheckCanonical(src []byte) error {
	dec := yaml.NewDecoder(bytes.NewReader(src))
	docs := 0
	for {
		var entries []map[string]any
		err := dec.Decode(&entries)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		docs++
		if docs > 1 {
			return errors.New("more than one YAML document in a registry file: put every entry in one list (a second document is read by the loader but must not be hidden from the gate)")
		}
		for _, e := range entries {
			for _, k := range canonicalBoolKeys {
				if v, ok := e[k]; ok {
					if _, isBool := v.(bool); !isBool {
						return fmt.Errorf("%v: %s must be true or false, not %v (%T)", e["id"], k, v, v)
					}
				}
			}
		}
	}
}
