package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func CompareGolden(goldenPath string, actual []byte) error {
	expected, err := os.ReadFile(goldenPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
				return fmt.Errorf("creating golden dir: %w", err)
			}
			return os.WriteFile(goldenPath, actual, 0o644)
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
			if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
				return fmt.Errorf("creating golden dir: %w", err)
			}
			return os.WriteFile(goldenPath, actualNorm.Bytes(), 0o644)
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

func UpdateGolden(goldenPath string, data []byte) {
	_ = os.MkdirAll(filepath.Dir(goldenPath), 0o755)
	_ = os.WriteFile(goldenPath, data, 0o644)
}
