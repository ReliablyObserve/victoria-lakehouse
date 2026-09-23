package main

import (
	"flag"
	"fmt"
	"sort"
	"strings"
)

// vlselectFlagsNotHonoured are the flags VictoriaLogs' vlselect package
// registers that this binary does not act on yet. The binary links vlselect to
// serve /internal/delete/* through upstream's own handler, which registers all
// of vlselect's flags; the lakehouse /select/* path is still its own and reads
// none of these. Until it moves onto vlselect.RequestHandler they are rejected
// when set, never silently ignored — a node that kept serving /select/* after
// -select.disable would be worse than one that refuses to start. Before
// vlselect was linked, setting any of them already failed startup ("flag
// provided but not defined"), so rejecting them changes nothing for operators.
var vlselectFlagsNotHonoured = []string{
	"delete.enable",
	"internalselect.disable",
	"search.logSlowQueryDuration",
	"search.maxConcurrentRequests",
	"search.maxQueryDuration",
	"search.maxQueueDuration",
	"select.disable",
	"vmalert.proxyURL",
}

// checkUpstreamFlagsHonoured returns an error naming every flag in
// vlselectFlagsNotHonoured that was set on the command line (or environment).
func checkUpstreamFlagsHonoured(fs *flag.FlagSet) error {
	notHonoured := make(map[string]bool, len(vlselectFlagsNotHonoured))
	for _, name := range vlselectFlagsNotHonoured {
		notHonoured[name] = true
	}
	var set []string
	fs.Visit(func(f *flag.Flag) {
		if notHonoured[f.Name] {
			set = append(set, "-"+f.Name)
		}
	})
	if len(set) == 0 {
		return nil
	}
	sort.Strings(set)
	return fmt.Errorf("%s: VictoriaLogs select flag(s) not supported by lakehouse-logs yet; the lakehouse /select/* path does not read them, so they are refused instead of ignored",
		strings.Join(set, ", "))
}
