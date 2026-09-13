package config

import (
	"encoding/json"
	"errors"
	"flag"
	"reflect"
	"strings"
	"testing"
	"time"
)

// testFlags mirrors the flag-application patterns the two binaries use
// (applyFlags in cmd/lakehouse-logs and lakehouse-traces).
type testFlags struct {
	fs         *flag.FlagSet
	profile    *string
	workers    *int
	interval   *time.Duration
	waste      *float64
	compaction *bool   // enable-only, default false
	pmeta      *bool   // authoritative, default true
	jaeger     *bool   // enable-only, default true
	vmuiTab    *bool   // disable-only, default true
	uiDisable  *bool   // inverted: true writes false
	alias      *string // only an orgid:account:project value writes
	roleGated  *bool   // writes only while role is unset
	metrics    *string // authoritative string, default "id"
	properBase bool    // apply the profile as the base, not under the loaded config
}

func newTestFlags() *testFlags {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	tf := &testFlags{
		fs:         fs,
		profile:    fs.String(ProfileFlagName, "", "Configuration profile"),
		workers:    fs.Int("lakehouse.query.file-workers", 0, "Parallel file workers (default: 64)"),
		interval:   fs.Duration("lakehouse.compaction.interval", 0, "Compaction interval <scan>"),
		waste:      fs.Float64("lakehouse.s3.read-ahead-waste-threshold", 0, "Waste threshold"),
		compaction: fs.Bool("lakehouse.compaction.enabled", false, "Enable compaction"),
		pmeta:      fs.Bool("lakehouse.pmeta.enabled", true, "pmeta"),
		jaeger:     fs.Bool("lakehouse.traces.jaeger-enabled", true, "Jaeger"),
		vmuiTab:    fs.Bool("lakehouse.ui.vmui-tab", true, "VMUI tab"),
		uiDisable:  fs.Bool("lakehouse.ui.disable", false, "Disable the UI"),
		alias:      fs.String("lakehouse.tenant.alias", "", "orgid:account:project"),
		roleGated:  fs.Bool("lakehouse.startup.serve-stale", false, "Serve stale"),
		metrics:    fs.String("lakehouse.tenant.metrics-format", "id", "Tenant label format"),
	}
	fs.String("lakehouse.config", "", "Path to YAML config file")
	return tf
}

func (tf *testFlags) apply(cfg *Config) {
	if p := *tf.profile; p != "" {
		if tf.properBase {
			mode := cfg.Mode
			*cfg = *ProfileConfig(Profile(p))
			cfg.Mode = mode
		} else {
			*cfg = *MergeConfigs(ProfileConfig(Profile(p)), cfg)
			cfg.Profile = Profile(p)
		}
	}
	if *tf.workers > 0 {
		cfg.Query.FileWorkers = *tf.workers
	}
	if *tf.interval > 0 {
		cfg.Compaction.Interval = *tf.interval
	}
	if *tf.waste > 0 {
		cfg.S3.ReadAheadWasteThreshold = *tf.waste
	}
	if *tf.compaction {
		cfg.Compaction.Enabled = true
	}
	cfg.Pmeta.Enabled = *tf.pmeta
	if *tf.jaeger {
		cfg.Traces.JaegerEnabled = true
	}
	if !*tf.vmuiTab {
		cfg.UI.VMUITab = false
	}
	if *tf.uiDisable {
		cfg.UI.Enabled = false
	}
	if parts := strings.Split(*tf.alias, ":"); len(parts) == 3 {
		if cfg.Tenant.Aliases == nil {
			cfg.Tenant.Aliases = map[string]AliasTarget{}
		}
		cfg.Tenant.Aliases[parts[0]] = AliasTarget{AccountID: 1}
	}
	if *tf.roleGated && cfg.Role == "" {
		cfg.Startup.ServeStale = true
	}
	cfg.Tenant.MetricsFormat = *tf.metrics
}

func describeTest(t *testing.T, tf *testFlags, mode Mode) *Surface {
	t.Helper()
	s, err := DescribeSurface("test-binary", mode, tf.fs, tf.apply)
	if err != nil {
		t.Fatalf("DescribeSurface: %v", err)
	}
	return s
}

func keyInfo(t *testing.T, s *Surface, key string) KeyInfo {
	t.Helper()
	for _, k := range s.Keys {
		if k.Key == key {
			return k
		}
	}
	t.Fatalf("key %q not in surface", key)
	return KeyInfo{}
}

func flagInfo(t *testing.T, s *Surface, name string) FlagInfo {
	t.Helper()
	for _, f := range s.Flags {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("flag %q not in surface", name)
	return FlagInfo{}
}

func TestDescribeSurface_KeysCoverEveryYAMLLeaf(t *testing.T) {
	s := describeTest(t, newTestFlags(), ModeLogs)

	fields := configFields()
	if len(s.Keys) != len(fields) || len(s.Keys) < 200 {
		t.Fatalf("surface has %d keys, config has %d leaves", len(s.Keys), len(fields))
	}
	for i, k := range s.Keys {
		if k.Key != fields[i].key {
			t.Fatalf("keys[%d] = %q, want %q (sorted leaf order)", i, k.Key, fields[i].key)
		}
	}
	def := flattenConfig(Default())
	for _, k := range s.Keys {
		if !sameValue(k.Default, def[k.Key]) {
			t.Errorf("%s default = %v, want %v", k.Key, k.Default, def[k.Key])
		}
	}

	wantTypes := map[string]string{
		"compaction.enabled":                           "bool",
		"query.file_workers":                           "int",
		"insert.flush_interval":                        "duration",
		"cache.eviction_watermark":                     "float",
		"logs.bloom_columns":                           "[]string",
		"compaction.compression_level_by_output_level": "[]int",
		"tenant.known_tenants":                         "[]object",
		"tenant.overrides":                             "map[string]object",
		"stats.s3_price_per_gb":                        "map[string]float",
		"mode":                                         "string",
		"logs.insert.profile":                          "string",
	}
	for key, want := range wantTypes {
		if got := keyInfo(t, s, key).Type; got != want {
			t.Errorf("%s type = %q, want %q", key, got, want)
		}
	}

	wantDefaults := map[string]any{
		"query.file_workers":    int64(64),
		"compaction.enabled":    true,
		"insert.flush_interval": "1m",
		"cache.footer_ttl":      "1h",
		"logs.bloom_columns":    []any{"service.name", "trace_id"},
		"tenant.known_tenants":  []any{},
	}
	for key, want := range wantDefaults {
		if got := keyInfo(t, s, key).Default; !sameValue(got, want) {
			t.Errorf("%s default = %#v, want %#v", key, got, want)
		}
	}

	if got := keyInfo(t, s, "mode").Effective; got != "logs" {
		t.Errorf("mode effective = %v, want logs (the binary's own mode)", got)
	}
	if got := keyInfo(t, s, "query.file_workers").Effective; got != nil {
		t.Errorf("query.file_workers effective = %v, want unset (same as default)", got)
	}
}

// The real loader's merge rules, pinned: a change to mergeConfig must show
// up here and in the committed config-surface golden files.
func TestDescribeSurface_FileMergeOfTheRealLoader(t *testing.T) {
	s := describeTest(t, newTestFlags(), ModeTraces)
	want := map[string]string{
		"compaction.enabled":           FileMergeEnableOnly,
		"traces.jaeger_enabled":        FileMergeEnableOnly,
		"query.file_workers":           FileMergeSet,
		"insert.flush_interval":        FileMergeSet,
		"logs.bloom_columns":           FileMergeSet,
		"query.max_files_per_query":    FileMergeIgnored,
		"pmeta.enabled":                FileMergeIgnored,
		"logs.promoted_attributes":     FileMergeIgnored,
		"telemetry.always_sample_slow": FileMergeDisableOnly,
		"mode":                         FileMergeIgnored,
		"profile":                      FileMergeSet,
	}
	for key, w := range want {
		if got := keyInfo(t, s, key).File; got != w {
			t.Errorf("%s file merge = %q, want %q", key, got, w)
		}
	}
}

func TestDescribeSurface_ProfilesAreExplicitOverrideSets(t *testing.T) {
	s := describeTest(t, newTestFlags(), ModeLogs)
	if len(s.Profiles) != len(ValidProfiles()) {
		t.Fatalf("profiles = %d, want %d", len(s.Profiles), len(ValidProfiles()))
	}
	if n := len(s.Profiles["balanced"]); n != 0 {
		t.Errorf("balanced overrides %d keys, want 0 (it is the defaults)", n)
	}
	mcs := s.Profiles["max-cost-savings"]
	if mcs["compaction.enabled"] != false || !sameValue(mcs["query.file_workers"], 4) {
		t.Errorf("max-cost-savings overrides = %v", mcs)
	}
	for name, over := range s.Profiles {
		if _, ok := over["profile"]; ok {
			t.Errorf("profile %s lists its own name as an override", name)
		}
		for key, v := range over {
			if sameValue(v, keyInfo(t, s, key).Default) {
				t.Errorf("profile %s lists %s=%v, which equals the default", name, key, v)
			}
		}
	}
}

func TestDescribeSurface_FlagsMapToKeysWithEffects(t *testing.T) {
	s := describeTest(t, newTestFlags(), ModeTraces)

	cases := []struct {
		name, typ, def, effect string
		keys                   []string
	}{
		{"lakehouse.query.file-workers", "int", "0", FlagEffectSet, []string{"query.file_workers"}},
		{"lakehouse.compaction.interval", "duration", "0s", FlagEffectSet, []string{"compaction.interval"}},
		{"lakehouse.s3.read-ahead-waste-threshold", "float", "0", FlagEffectSet, []string{"s3.read_ahead_waste_threshold"}},
		{"lakehouse.compaction.enabled", "bool", "false", FlagEffectEnableOnly, []string{"compaction.enabled"}},
		{"lakehouse.pmeta.enabled", "bool", "true", FlagEffectAuthoritative, []string{"pmeta.enabled"}},
		{"lakehouse.traces.jaeger-enabled", "bool", "true", FlagEffectEnableOnly, []string{"traces.jaeger_enabled"}},
		{"lakehouse.ui.vmui-tab", "bool", "true", FlagEffectDisableOnly, []string{"ui.vmui_tab"}},
		{"lakehouse.ui.disable", "bool", "false", FlagEffectDisableOnly, []string{"ui.enabled"}},
		{"lakehouse.tenant.alias", "string", "", FlagEffectSet, []string{"tenant.aliases"}},
		{"lakehouse.startup.serve-stale", "bool", "false", FlagEffectNone, []string{"startup.serve_stale"}},
		{"lakehouse.tenant.metrics-format", "string", "id", FlagEffectAuthoritative, []string{"tenant.metrics_format"}},
		{ProfileFlagName, "string", "", FlagEffectSet, []string{"profile"}},
		{"lakehouse.config", "string", "", FlagEffectNone, []string{}},
	}
	for _, c := range cases {
		f := flagInfo(t, s, c.name)
		if f.Type != c.typ || f.Default != c.def || f.Effect != c.effect || !reflect.DeepEqual(f.Keys, c.keys) {
			t.Errorf("%s = {type %s, default %q, effect %s, keys %v}, want {%s, %q, %s, %v}",
				c.name, f.Type, f.Default, f.Effect, f.Keys, c.typ, c.def, c.effect, c.keys)
		}
	}
	for i := 1; i < len(s.Flags); i++ {
		if s.Flags[i-1].Name >= s.Flags[i].Name {
			t.Fatalf("flags not sorted: %q before %q", s.Flags[i-1].Name, s.Flags[i].Name)
		}
	}
	if got := keyInfo(t, s, "query.file_workers").Flags; !reflect.DeepEqual(got, []string{"lakehouse.query.file-workers"}) {
		t.Errorf("query.file_workers flags = %v", got)
	}
}

func TestDescribeSurface_RestoresFlagDefaults(t *testing.T) {
	tf := newTestFlags()
	describeTest(t, tf, ModeLogs)
	tf.fs.VisitAll(func(f *flag.Flag) {
		if got := f.Value.String(); got != f.DefValue {
			t.Errorf("flag -%s left at %q, default %q", f.Name, got, f.DefValue)
		}
	})
}

func TestDescribeSurface_ProfileFlagGaps(t *testing.T) {
	merged := describeTest(t, newTestFlags(), ModeLogs)
	if gaps := merged.ProfileFlagGaps["balanced"]; len(gaps) != 0 {
		t.Errorf("balanced gaps = %v, want none", gaps)
	}
	mcs := strings.Join(merged.ProfileFlagGaps["max-cost-savings"], ",")
	for _, key := range []string{"compaction.enabled", "query.file_workers", "stats.enabled"} {
		if !strings.Contains(mcs, key) {
			t.Errorf("max-cost-savings gaps %q miss %s", mcs, key)
		}
	}

	proper := newTestFlags()
	proper.properBase = true
	for p, gaps := range describeTest(t, proper, ModeLogs).ProfileFlagGaps {
		if len(gaps) != 0 {
			t.Errorf("profile %s applied as the base still has gaps %v", p, gaps)
		}
	}

	noProfile := flag.NewFlagSet("np", flag.ContinueOnError)
	s, err := DescribeSurface("np", ModeLogs, noProfile, func(*Config) {})
	if err != nil || s.ProfileFlagGaps != nil {
		t.Errorf("without a profile flag: gaps %v, err %v", s.ProfileFlagGaps, err)
	}
}

func TestDescribeSurface_UnknownModeKeepsLoaderSemantics(t *testing.T) {
	s := describeTest(t, newTestFlags(), "")
	if got := keyInfo(t, s, "mode"); got.File != FileMergeSet || got.Effective != nil {
		t.Errorf("mode without a binary mode = %+v, want file merge set, no effective override", got)
	}
}

func TestSurfaceJSON_DeterministicAndReadable(t *testing.T) {
	a, err := describeTest(t, newTestFlags(), ModeLogs).JSON()
	if err != nil {
		t.Fatal(err)
	}
	b, err := describeTest(t, newTestFlags(), ModeLogs).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("two renders of the same surface differ")
	}
	if !strings.HasSuffix(string(a), "}\n") || !strings.Contains(string(a), "<scan>") {
		t.Errorf("JSON must end with a newline and keep < > unescaped")
	}
	var back map[string]any
	if err := json.Unmarshal(a, &back); err != nil {
		t.Fatalf("JSON does not round-trip: %v", err)
	}
}

func TestSurfaceJSON_RoundTripsEveryShape(t *testing.T) {
	cases := []*Surface{
		// Nil key and flag lists render as [] so consumers always see lists.
		{Binary: "empty", Keys: []KeyInfo{}, Flags: []FlagInfo{}, Profiles: map[string]map[string]any{}},
		{Binary: "no-gaps", Mode: ModeLogs, Keys: []KeyInfo{{Key: "a", Type: "int", Default: 1.0, File: FileMergeSet}},
			Profiles: map[string]map[string]any{"balanced": {}, "dev": {"a": 2.0, "b": "x"}},
			Flags:    []FlagInfo{{Name: "f", Type: "int", Default: "0", Keys: []string{"a"}, Effect: FlagEffectSet}}},
		{Binary: "gaps", Keys: []KeyInfo{}, Flags: []FlagInfo{}, Profiles: map[string]map[string]any{"dev": {}},
			ProfileFlagGaps: map[string][]string{"balanced": {}, "dev": {"a", "b"}}},
	}
	for _, want := range cases {
		out, err := want.JSON()
		if err != nil {
			t.Fatalf("%s: %v", want.Binary, err)
		}
		var got Surface
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("%s: invalid JSON: %v\n%s", want.Binary, err, out)
		}
		wantJSON, _ := json.Marshal(want)
		gotJSON, _ := json.Marshal(&got)
		if string(wantJSON) != string(gotJSON) {
			t.Errorf("%s round trip:\n got %s\nwant %s", want.Binary, gotJSON, wantJSON)
		}
	}
}

func TestSurfaceJSON_EncodeError(t *testing.T) {
	s := &Surface{Keys: []KeyInfo{{Key: "bad", Default: func() {}}}}
	if _, err := s.JSON(); err == nil {
		t.Fatal("JSON of an unencodable default must fail")
	}
}

// stickyValue refuses to be reset to empty, so restoring its default fails.
type stickyValue struct{ v string }

func (s *stickyValue) String() string { return s.v }
func (s *stickyValue) Set(x string) error {
	if x == "" {
		return errors.New("empty not allowed")
	}
	s.v = x
	return nil
}

// aliasOnlyValue accepts only orgid:account:project values (or empty).
type aliasOnlyValue struct{ v string }

func (a *aliasOnlyValue) String() string { return a.v }
func (a *aliasOnlyValue) Set(x string) error {
	if x != "" && strings.Count(x, ":") != 2 {
		return errors.New("want orgid:account:project")
	}
	a.v = x
	return nil
}

// countingBool is a bool flag whose reset to its default starts failing
// after allowed successful resets.
type countingBool struct {
	v       bool
	resets  int
	allowed int
}

func (c *countingBool) String() string   { return "false" }
func (c *countingBool) IsBoolFlag() bool { return true }
func (c *countingBool) Get() any         { return c.v }
func (c *countingBool) Set(x string) error {
	if x == "false" {
		c.resets++
		if c.resets > c.allowed {
			return errors.New("reset refused")
		}
	}
	c.v = x == "true"
	return nil
}

func TestDescribeSurface_RejectedProbeTriesTheNext(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	v := &aliasOnlyValue{}
	fs.Var(v, "lakehouse.tenant.alias", "aliases")
	apply := func(cfg *Config) {
		if parts := strings.Split(v.v, ":"); len(parts) == 3 {
			cfg.Tenant.Aliases = map[string]AliasTarget{parts[0]: {}}
		}
	}
	s, err := DescribeSurface("t", ModeLogs, fs, apply)
	if err != nil {
		t.Fatal(err)
	}
	if f := flagInfo(t, s, "lakehouse.tenant.alias"); !reflect.DeepEqual(f.Keys, []string{"tenant.aliases"}) {
		t.Errorf("alias keys = %v, want tenant.aliases via the second probe", f.Keys)
	}
}

func TestDescribeSurface_FlagRestoreFailure(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	v := &stickyValue{}
	fs.Var(v, "lakehouse.s3.bucket", "bucket")
	fs.String("lakehouse.s3.region", "", "visited after the failure, not probed")
	apply := func(cfg *Config) { cfg.S3.Bucket = v.v }
	if _, err := DescribeSurface("t", ModeLogs, fs, apply); err == nil || !strings.Contains(err.Error(), "restore flag") {
		t.Fatalf("err = %v, want a restore failure", err)
	}
}

func TestDescribeSurface_EffectProbeRestoreFailure(t *testing.T) {
	for _, allowed := range []int{2, 3} {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		v := &countingBool{allowed: allowed}
		fs.Var(v, "lakehouse.compaction.enabled", "compaction")
		apply := func(cfg *Config) {
			if v.v {
				cfg.Compaction.Enabled = true
			}
		}
		if _, err := DescribeSurface("t", ModeLogs, fs, apply); err == nil {
			t.Errorf("allowed=%d: want the effect probe's failed reset to surface", allowed)
		}
	}
}

// digitsValue accepts only empty or all-digit values: flag probing succeeds,
// selecting a named profile does not.
type digitsValue struct{ v string }

func (d *digitsValue) String() string { return d.v }
func (d *digitsValue) Set(x string) error {
	if strings.Trim(x, "0123456789") != "" {
		return errors.New("digits only")
	}
	d.v = x
	return nil
}

func TestDescribeSurface_ProfileGapFailure(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Var(&digitsValue{}, ProfileFlagName, "profile")
	if _, err := DescribeSurface("t", ModeLogs, fs, func(*Config) {}); err == nil {
		t.Fatal("want the profile gap probe's failure to surface")
	}
}

func TestProfileFlagGaps_RestoreFailure(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Var(&stickyValue{}, ProfileFlagName, "profile")
	if _, err := profileFlagGaps(fs, ModeLogs, func(*Config) {}); err == nil {
		t.Fatal("want the profile flag's failed reset to surface")
	}
	bad := flag.NewFlagSet("t", flag.ContinueOnError)
	bad.Var(&aliasOnlyValue{}, ProfileFlagName, "profile")
	if _, err := profileFlagGaps(bad, ModeLogs, func(*Config) {}); err == nil {
		t.Fatal("want a rejected profile value to surface")
	}
}

func TestFileMerge_Classification(t *testing.T) {
	boolType := reflect.TypeOf(false)
	intType := reflect.TypeOf(0)
	cases := []struct {
		name  string
		merge func(base, file *Config) *Config
		typ   reflect.Type
		key   string
		want  string
	}{
		{"bool replace", func(b, f *Config) *Config { b.GC.Enabled = f.GC.Enabled; return b }, boolType, "gc.enabled", FileMergeReplace},
		{"bool enable-only", func(b, f *Config) *Config {
			if f.GC.Enabled {
				b.GC.Enabled = true
			}
			return b
		}, boolType, "gc.enabled", FileMergeEnableOnly},
		{"bool disable-only", func(b, f *Config) *Config {
			if !f.GC.Enabled {
				b.GC.Enabled = false
			}
			return b
		}, boolType, "gc.enabled", FileMergeDisableOnly},
		{"bool ignored", func(b, _ *Config) *Config { return b }, boolType, "gc.enabled", FileMergeIgnored},
		{"int replace", func(b, f *Config) *Config { b.Query.FileWorkers = f.Query.FileWorkers; return b }, intType, "query.file_workers", FileMergeReplace},
		{"int set", func(b, f *Config) *Config {
			if f.Query.FileWorkers > 0 {
				b.Query.FileWorkers = f.Query.FileWorkers
			}
			return b
		}, intType, "query.file_workers", FileMergeSet},
		{"int reset", func(b, _ *Config) *Config { b.Query.FileWorkers = 0; return b }, intType, "query.file_workers", FileMergeReset},
		{"int ignored", func(b, _ *Config) *Config { return b }, intType, "query.file_workers", FileMergeIgnored},
	}
	for _, c := range cases {
		if got := fileMerge(c.merge, c.key, c.typ); got != c.want {
			t.Errorf("%s: fileMerge = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestFlagProbesAndTypes(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.Int("seven", 7, "")
	fs.Bool("on", true, "")
	fs.Var(&stickyValue{}, "custom", "")
	if got := flagProbes(fs.Lookup("seven"), "int"); !reflect.DeepEqual(got, []string{"11"}) {
		t.Errorf("probes for an int defaulting to 7 = %v, want [11]", got)
	}
	if got := flagProbes(fs.Lookup("on"), "bool"); !reflect.DeepEqual(got, []string{"false"}) {
		t.Errorf("probes for a bool defaulting to true = %v, want [false]", got)
	}
	if got := flagType(fs.Lookup("custom")); got != "string" {
		t.Errorf("flagType of a non-Getter value = %q, want string", got)
	}
	fs.Uint64("u", 0, "")
	if got := flagType(fs.Lookup("u")); got != "int" {
		t.Errorf("flagType(uint64) = %q, want int", got)
	}
	fs.String("s", "", "")
	if got := flagType(fs.Lookup("s")); got != "string" {
		t.Errorf("flagType(string) = %q, want string", got)
	}
}

type ptrHolder struct {
	P    *int `yaml:"p"`
	U    uint `yaml:"u"`
	X    int  // no yaml tag: not part of the surface
	Skip int  `yaml:"-"`
	y    int
}

func TestReflectionHelpers(t *testing.T) {
	n := 3
	if got := plainValue(reflect.ValueOf(ptrHolder{P: &n, U: 4, Skip: 9, y: 1})); !sameValue(got, map[string]any{"p": 3, "u": 4}) {
		t.Errorf("plainValue(struct) = %v", got)
	}
	if got := plainValue(reflect.ValueOf((*int)(nil))); got != nil {
		t.Errorf("plainValue(nil ptr) = %v", got)
	}
	if got := plainValue(reflect.ValueOf(make(chan int))); got == nil {
		t.Error("plainValue(chan) must fall back to a printed value")
	}
	if got := plainValue(reflect.ValueOf([2]int{1, 2})); !sameValue(got, []any{1, 2}) {
		t.Errorf("plainValue(array) = %v", got)
	}

	types := map[reflect.Type]string{
		reflect.TypeOf(&n):                      "int",
		reflect.TypeOf(make(chan int)):          "chan",
		reflect.TypeOf(ptrHolder{}):             "object",
		reflect.TypeOf(map[string][]int{}):      "map[string][]int",
		reflect.TypeOf([2]float32{}):            "[]float",
		reflect.TypeOf(uint8(0)):                "int",
		reflect.TypeOf(time.Duration(0)):        "duration",
		reflect.TypeOf(map[string]string{}):     "map[string]string",
		reflect.TypeOf([]LifecycleRuleConfig{}): "[]object",
	}
	for typ, want := range types {
		if got := typeName(typ); got != want {
			t.Errorf("typeName(%s) = %q, want %q", typ, got, want)
		}
	}

	for _, typ := range []reflect.Type{
		reflect.TypeOf(&n), reflect.TypeOf(uint16(0)), reflect.TypeOf(float32(0)),
		reflect.TypeOf(ptrHolder{}), reflect.TypeOf(map[string]AliasTarget{}),
		reflect.TypeOf([]string{}), reflect.TypeOf(Mode("")), reflect.TypeOf(time.Duration(0)),
	} {
		a, b := plainValue(probeValue(typ, 0)), plainValue(probeValue(typ, 1))
		if sameValue(a, b) {
			t.Errorf("probeValue(%s) variants are equal: %v", typ, a)
		}
	}

	durations := map[time.Duration]string{
		0:                      "0s",
		time.Hour:              "1h",
		90 * time.Minute:       "1h30m",
		5 * time.Minute:        "5m",
		200 * time.Millisecond: "200ms",
		90 * time.Second:       "1m30s",
	}
	for d, want := range durations {
		if got := compactDuration(d); got != want {
			t.Errorf("compactDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestLeafFields(t *testing.T) {
	var keys []string
	for _, f := range leafFields(reflect.TypeOf(ptrHolder{})) {
		keys = append(keys, f.key)
	}
	if !reflect.DeepEqual(keys, []string{"p", "u"}) {
		t.Errorf("leafFields(ptrHolder) = %v, want [p u] (untagged, skipped and unexported fields excluded)", keys)
	}
}

func TestSetKeyAndLookups(t *testing.T) {
	cfg := Default()
	if err := setKey(cfg, "query.max_rows", reflect.ValueOf(5)); err != nil || cfg.Query.MaxRows != 5 {
		t.Errorf("convertible assignment: err %v, max_rows %d", err, cfg.Query.MaxRows)
	}
	if err := setKey(cfg, "query.nope", reflect.ValueOf(1)); err == nil {
		t.Error("unknown key must fail")
	}
	if err := setKey(cfg, "s3.bucket.name", reflect.ValueOf("x")); err == nil {
		t.Error("walking below a scalar must fail")
	}
	if err := setKey(cfg, "s3.bucket", reflect.ValueOf([]int{1})); err == nil {
		t.Error("an unassignable value must fail")
	}
	defer func() {
		if recover() == nil {
			t.Error("mustSetKey on an unknown key must panic")
		}
	}()
	mustSetKey(cfg, "nope", reflect.ValueOf(1))
}

func TestSmallHelpers(t *testing.T) {
	if otherMode(ModeLogs) != ModeTraces || otherMode(ModeTraces) != ModeLogs || otherMode("") != "" {
		t.Error("otherMode")
	}
	if got := diffKeys(map[string]any{"a": 1, "b": 2}, map[string]any{"b": 3, "c": 4}); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("diffKeys = %v", got)
	}
	if sameValue(func() {}, func() {}) {
		t.Error("unencodable values are never the same")
	}
	if _, err := loadConfigBytes([]byte("lakehouse: [not, a, map]"), ModeLogs, ""); err == nil {
		t.Error("loadConfigBytes must reject a non-mapping document")
	}
	if cfg := binaryDefaults(ModeTraces); cfg.Mode != ModeTraces || cfg.Role != RoleAll {
		t.Errorf("binaryDefaults = mode %q role %q", cfg.Mode, cfg.Role)
	}
}
