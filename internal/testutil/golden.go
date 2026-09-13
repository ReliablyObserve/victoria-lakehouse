package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func CompareGolden(goldenPath string, actual []byte) error {
	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(goldenPath), 0o750); err != nil {
				return fmt.Errorf("creating golden dir: %w", err)
			}
			return os.WriteFile(goldenPath, actual, 0o600)
		}
		return err
	}
	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("golden mismatch:\n--- expected (golden) ---\n%s\n--- actual ---\n%s",
			string(expected), string(actual))
	}
	return nil
}

func CompareGoldenJSON(goldenPath string, actual []byte) error {
	var actualNorm bytes.Buffer
	if err := json.Compact(&actualNorm, actual); err != nil {
		return fmt.Errorf("compacting actual JSON: %w", err)
	}

	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(goldenPath), 0o750); err != nil {
				return fmt.Errorf("creating golden dir: %w", err)
			}
			return os.WriteFile(goldenPath, actualNorm.Bytes(), 0o600)
		}
		return err
	}

	var expectedNorm bytes.Buffer
	if err := json.Compact(&expectedNorm, expected); err != nil {
		return fmt.Errorf("compacting golden JSON: %w", err)
	}

	if !bytes.Equal(expectedNorm.Bytes(), actualNorm.Bytes()) {
		return fmt.Errorf("golden JSON mismatch:\n--- expected ---\n%s\n--- actual ---\n%s",
			expectedNorm.String(), actualNorm.String())
	}
	return nil
}

// DiffGolden compares actual with the committed golden file byte for byte.
// Unlike CompareGolden it never creates a missing file — a gate must fail
// when its expectation is absent — and it reports the first differing line
// rather than both documents.
func DiffGolden(goldenPath string, actual []byte) error {
	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		return fmt.Errorf("read golden file: %w", err)
	}
	if bytes.Equal(expected, actual) {
		return nil
	}
	exp := strings.Split(string(expected), "\n")
	act := strings.Split(string(actual), "\n")
	line := 0
	for line < len(exp) && line < len(act) && exp[line] == act[line] {
		line++
	}
	return fmt.Errorf("%s is stale: first difference at line %d\n  golden: %s\n  actual: %s",
		goldenPath, line+1, lineAt(exp, line), lineAt(act, line))
}

func lineAt(lines []string, i int) string {
	if i < len(lines) {
		return fmt.Sprintf("%q", lines[i])
	}
	return "(end of file)"
}

func UpdateGolden(goldenPath string, data []byte) {
	_ = os.MkdirAll(filepath.Dir(goldenPath), 0o750)
	_ = os.WriteFile(goldenPath, data, 0o600)
}
