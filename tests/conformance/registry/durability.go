package registry

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// CheckDurability is the part of the durability gate that needs the repository:
// the checks Row.Validate cannot make on one row alone. It returns one message
// per problem, sorted.
//
//  1. Every refs.tests entry of a durability cell names a Go test that exists
//     (CheckTestRef, with a "#TestName" suffix: a cell is proved by named tests,
//     and a name that does not exist claims a proof the repository does not have).
//  2. The logs/traces twin: every cell has a twin for the other signal (same
//     fault and mode, id differing only in the _logs/_traces suffix) with the
//     same expect and the same guarantees. A logs cell's tests are not under
//     lakehouse-traces/ and a traces cell's tests are, except an end-to-end test
//     under tests/e2e, which may serve either signal when its file name says
//     which (a file name containing "traces" is the traces binary's).
//  3. A loss-counted cell names a metric that internal/metrics registers.
//  4. docs/durability.md and the cells agree: every cell id appears in the
//     document (exactly, or through a "lh.durability.<prefix>_*" family), and
//     every lh.durability.* id the document names is a cell or a family of cells.
func CheckDurability(reg *Registry, repoRoot string) []string {
	var errs []string
	cells := map[string]*Row{}
	for i := range reg.Rows {
		if reg.Rows[i].Kind == KindDurability {
			cells[reg.Rows[i].ID] = &reg.Rows[i]
		}
	}
	metricsSrc := readMetricsSource(repoRoot)
	for id, r := range cells {
		d := r.Durability
		if r.Refs != nil {
			for _, ref := range r.Refs.Tests {
				if !strings.Contains(ref, "#") {
					errs = append(errs, fmt.Sprintf("%s: refs.tests %q must name a test (path#TestName)", id, ref))
					continue
				}
				if err := CheckTestRef(repoRoot, ref); err != nil {
					errs = append(errs, fmt.Sprintf("%s: refs.tests: %v", id, err))
					continue
				}
				if msg := twinPlacement(d.Signal, ref); msg != "" {
					errs = append(errs, fmt.Sprintf("%s: refs.tests %q: %s", id, ref, msg))
				}
			}
		}
		if d.Metric != "" && !strings.Contains(metricsSrc, `"`+d.Metric+`"`) {
			errs = append(errs, fmt.Sprintf("%s: durability.metric %q is not registered in internal/metrics", id, d.Metric))
		}
		other := "traces"
		if d.Signal == "traces" {
			other = "logs"
		}
		twinID := strings.TrimSuffix(id, "_"+d.Signal) + "_" + other
		twin := cells[twinID]
		switch {
		case twin == nil:
			errs = append(errs, fmt.Sprintf("%s: no %s twin %s (a cell is declared for both signals)", id, other, twinID))
		case twin.Expect != r.Expect:
			errs = append(errs, fmt.Sprintf("%s: expect %s but its twin %s has %s", id, r.Expect, twinID, twin.Expect))
		case strings.Join(twin.Durability.Guarantees, ",") != strings.Join(d.Guarantees, ","):
			errs = append(errs, fmt.Sprintf("%s: guarantees %v differ from its twin %s: %v", id, d.Guarantees, twinID, twin.Durability.Guarantees))
		case twin.Durability.Fault != d.Fault || twin.Durability.Mode != d.Mode:
			errs = append(errs, fmt.Sprintf("%s: fault/mode differ from its twin %s", id, twinID))
		}
	}
	errs = append(errs, checkDurabilityDoc(cells, repoRoot)...)
	sort.Strings(errs)
	return errs
}

// twinPlacement says what is wrong with where a cell's test lives for the cell's
// signal, or "".
func twinPlacement(signal, ref string) string {
	path, _ := splitRef(ref)
	switch {
	case strings.HasPrefix(path, "tests/e2e/"):
		isTraces := strings.Contains(filepath.Base(path), "traces")
		if signal == "traces" && !isTraces {
			return "a traces cell's end-to-end test lives in a file whose name contains \"traces\""
		}
		if signal == "logs" && isTraces {
			return "a logs cell cites an end-to-end test of the traces binary"
		}
	case strings.HasPrefix(path, "tests/conformance/"), strings.HasPrefix(path, "scripts/ci/tests/"):
		// registry-level tests and comparison scripts serve both signals
	case signal == "traces" && !strings.HasPrefix(path, "lakehouse-traces/"):
		return "a traces cell's unit tests live under lakehouse-traces/"
	case signal == "logs" && strings.HasPrefix(path, "lakehouse-traces/"):
		return "a logs cell's unit tests live outside lakehouse-traces/"
	}
	return ""
}

func readMetricsSource(repoRoot string) string {
	var b strings.Builder
	files, _ := filepath.Glob(filepath.Join(repoRoot, "internal", "metrics", "*.go"))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if data, err := os.ReadFile(f); err == nil {
			b.Write(data)
		}
	}
	return b.String()
}

var durabilityIDRe = regexp.MustCompile(`lh\.durability\.[a-z0-9_]+\*?`)

func checkDurabilityDoc(cells map[string]*Row, repoRoot string) []string {
	if len(cells) == 0 {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, "docs", "durability.md"))
	if err != nil {
		return []string{fmt.Sprintf("docs/durability.md: %v", err)}
	}
	mentioned := durabilityIDRe.FindAllString(string(data), -1)
	var errs []string
	covers := func(mention, id string) bool {
		if strings.HasSuffix(mention, "*") {
			return strings.HasPrefix(id, strings.TrimSuffix(mention, "*"))
		}
		return mention == id
	}
	for id := range cells {
		found := false
		for _, m := range mentioned {
			if covers(m, id) {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, fmt.Sprintf("docs/durability.md does not mention the cell %s (name it, or its family as lh.durability.<prefix>_*)", id))
		}
	}
	for _, m := range mentioned {
		matches := 0
		for id := range cells {
			if covers(m, id) {
				matches++
			}
		}
		if matches == 0 {
			errs = append(errs, fmt.Sprintf("docs/durability.md names %s, which is no durability cell", m))
		}
	}
	return errs
}
