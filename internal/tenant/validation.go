package tenant

import (
	"errors"
	"fmt"
)

const MaxOrgIDLength = 150

var validOrgIDChars [256]bool

func init() {
	for c := 'a'; c <= 'z'; c++ {
		validOrgIDChars[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		validOrgIDChars[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		validOrgIDChars[c] = true
	}
	for _, c := range "!-_.*'()" {
		validOrgIDChars[c] = true
	}
}

func ValidateOrgID(id string) error {
	if len(id) == 0 {
		return errors.New("tenant org ID is empty")
	}
	if len(id) > MaxOrgIDLength {
		return fmt.Errorf("tenant org ID too long: %d > %d", len(id), MaxOrgIDLength)
	}
	if id == "." || id == ".." {
		return fmt.Errorf("tenant org ID %q is unsafe", id)
	}
	for i := 0; i < len(id); i++ {
		if !validOrgIDChars[id[i]] {
			return fmt.Errorf("tenant org ID %q contains invalid character %q at position %d", id, id[i], i)
		}
	}
	return nil
}
