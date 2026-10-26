package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Paths, relative to the repository root, of the files the proof fields of the
// registry point into.
const (
	shapesFile   = "scripts/bench/ab/shapes.json"
	pagesFile    = "tests/playwright/proof/spec.json"
	pinsMakefile = "Makefile"
)

// CheckProofRefs resolves every pointer the proof fields of the registry make
// into the repository: a perf scenario to a shape set (and shape) of
// scripts/bench/ab/shapes.json, a visual page and its ranges to
// tests/playwright/proof/spec.json, a contract to its committed JSON file, a
// shared-code name to its patch, source file or pin, its guard tests to real
// tests, and a feature's `see` to a feature that really is proven. A pointer
// to nothing is worse than none: it claims a proof the repository does not
// have. It returns one problem per line, sorted; reg and set may be nil.
func CheckProofRefs(reg *Registry, set *FeatureSet, repoRoot string) []string {
	var problems []string
	add := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	var (
		shapes    *shapeFile
		pages     *pageFile
		shapesErr error
		pagesErr  error
		loaded    bool
	)
	load := func() {
		if loaded {
			return
		}
		loaded = true
		shapes, shapesErr = readShapes(repoRoot)
		pages, pagesErr = readPages(repoRoot)
	}

	if reg != nil {
		for i := range reg.Rows {
			r := &reg.Rows[i]
			switch {
			case r.Perf != nil && r.Perf.Scenario != nil:
				load()
				checkPerfScenario(r, shapes, shapesErr, add)
			case r.Kind == KindVisual && r.Visual != nil:
				load()
				checkVisual(r, pages, pagesErr, add)
			case r.Kind == KindAPISchema:
				checkContract(r, repoRoot, add)
			case r.Kind == KindSharedCode && r.SharedCode != nil:
				checkSharedCode(r, repoRoot, add)
			}
		}
	}
	if set != nil {
		checkNASee(set, reg, add)
	}
	sort.Strings(problems)
	return problems
}

type shapeFile struct {
	Sets map[string]struct {
		Shapes []struct {
			Name   string `json:"name"`
			Signal string `json:"signal"`
		} `json:"shapes"`
	} `json:"sets"`
}

type pageFile struct {
	Ranges []string `json:"ranges"`
	Pages  []struct {
		ID     string   `json:"id"`
		Ranges []string `json:"ranges"`
	} `json:"pages"`
}

func readShapes(repoRoot string) (*shapeFile, error) {
	var s shapeFile
	if err := readJSON(filepath.Join(repoRoot, shapesFile), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func readPages(repoRoot string) (*pageFile, error) {
	var p pageFile
	if err := readJSON(filepath.Join(repoRoot, pagesFile), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func readJSON(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func checkPerfScenario(r *Row, shapes *shapeFile, shapesErr error, add func(string, ...any)) {
	sc := r.Perf.Scenario
	if shapesErr != nil {
		add("%s: perf.scenario: %v", r.ID, shapesErr)
		return
	}
	set, ok := shapes.Sets[sc.Set]
	if !ok {
		add("%s: perf.scenario.set %q is not a shape set of %s", r.ID, sc.Set, shapesFile)
		return
	}
	if sc.Shape != "" {
		found := false
		for _, sh := range set.Shapes {
			found = found || sh.Name == sc.Shape
		}
		if !found {
			add("%s: perf.scenario.shape %q is not a shape of set %s", r.ID, sc.Shape, sc.Set)
			return
		}
	}
	// A baseline of hot VictoriaLogs answers logs shapes only, hot VictoriaTraces traces shapes only.
	want := map[string]string{"hot-vl": "logs", "hot-vt": "traces"}[r.Perf.Baseline]
	if want == "" {
		return
	}
	for _, sh := range set.Shapes {
		if (sc.Shape == "" || sh.Name == sc.Shape) && sh.Signal != want {
			add("%s: perf.baseline %s cannot judge shape %q of set %s: its signal is %s", r.ID, r.Perf.Baseline, sh.Name, sc.Set, sh.Signal)
		}
	}
}

func checkVisual(r *Row, pages *pageFile, pagesErr error, add func(string, ...any)) {
	v := r.Visual
	if pagesErr != nil {
		add("%s: visual.page: %v", r.ID, pagesErr)
		return
	}
	var pageRanges []string
	found := false
	for _, p := range pages.Pages {
		if p.ID == v.Page {
			found, pageRanges = true, p.Ranges
		}
	}
	if !found {
		add("%s: visual.page %q is not a page of %s", r.ID, v.Page, pagesFile)
		return
	}
	allowed := pageRanges
	if len(allowed) == 0 {
		allowed = pages.Ranges
	}
	for _, rng := range v.Ranges {
		if !containsString(allowed, rng) {
			add("%s: visual.ranges %q is not a capture range of page %s (%s)", r.ID, rng, v.Page, strings.Join(allowed, ", "))
		}
	}
}

func checkContract(r *Row, repoRoot string, add func(string, ...any)) {
	if r.Contract == "" || contractRe.FindStringSubmatch(r.Contract) == nil {
		return // reported by Validate
	}
	var doc any
	if err := readJSON(filepath.Join(repoRoot, filepath.FromSlash(r.Contract)), &doc); err != nil {
		add("%s: contract: %v", r.ID, err)
	}
}

func checkSharedCode(r *Row, repoRoot string, add func(string, ...any)) {
	s := r.SharedCode
	switch s.Type {
	case "patch", "src":
		if strings.Contains(s.Name, "..") {
			add("%s: shared_code.name %q must stay inside the repository", r.ID, s.Name)
		} else if _, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(s.Name))); err != nil {
			add("%s: shared_code.name: %v", r.ID, err)
		}
	case "pin":
		data, err := os.ReadFile(filepath.Join(repoRoot, pinsMakefile))
		if err != nil {
			add("%s: shared_code.name: %v", r.ID, err)
		} else if text := "\n" + string(data); !strings.Contains(text, "\n"+s.Name+" ") && !strings.Contains(text, "\n"+s.Name+"=") {
			add("%s: shared_code.name %s is not set in the Makefile", r.ID, s.Name)
		}
	}
	if s.Gate != "" {
		if strings.Contains(s.Gate, "..") {
			add("%s: shared_code.gate %q must stay inside the repository", r.ID, s.Gate)
		} else if _, err := os.Stat(filepath.Join(repoRoot, filepath.FromSlash(s.Gate))); err != nil {
			add("%s: shared_code.gate: %v", r.ID, err)
		}
	}
	if r.Refs != nil {
		for _, ref := range r.Refs.Tests {
			if err := CheckTestRef(repoRoot, ref); err != nil {
				add("%s: refs.tests: %v", r.ID, err)
			}
		}
	}
}

// checkNASee checks the features a not-applicable statement points at: the
// feature must exist, must not itself be not-applicable in the same
// dimension (no chains), and must have what the statement says covers this one.
func checkNASee(set *FeatureSet, reg *Registry, add func(string, ...any)) {
	for i := range set.Features {
		f := &set.Features[i]
		if f.PerfNA != nil && f.PerfNA.See != "" {
			other := set.ByID[f.PerfNA.See]
			switch {
			case other == nil:
				add("%s: perf_na.see %s is not a feature", f.ID, f.PerfNA.See)
			case other.PerfNA != nil:
				add("%s: perf_na.see %s is itself perf_na: point at the feature that has the scenario", f.ID, other.ID)
			case len(other.Bench) == 0 && !hasPerfRow(other, reg):
				add("%s: perf_na.see %s has no bench scenario or perf row", f.ID, other.ID)
			}
		}
		if f.ProofNA != nil && f.ProofNA.See != "" {
			other := set.ByID[f.ProofNA.See]
			switch {
			case other == nil:
				add("%s: proof_na.see %s is not a feature", f.ID, f.ProofNA.See)
			case other.ProofNA != nil:
				add("%s: proof_na.see %s is itself proof_na: point at the feature that has the proof", f.ID, other.ID)
			case len(other.Rows) == 0:
				add("%s: proof_na.see %s cites no registry row", f.ID, other.ID)
			}
		}
	}
}

func hasPerfRow(f *Feature, reg *Registry) bool {
	if reg == nil {
		return false
	}
	for _, id := range f.Rows {
		if r := reg.ByID[id]; r != nil && r.Perf != nil {
			return true
		}
	}
	return false
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
