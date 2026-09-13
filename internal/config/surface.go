package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ProfileFlagName is the flag both binaries use to select a profile.
const ProfileFlagName = "lakehouse.profile"

// How a value written in the --lakehouse.config file is merged over the
// profile the file selects (LoadWithMode → mergeConfig).
const (
	// FileMergeSet: a non-zero, non-empty value replaces the profile value;
	// zero and empty values count as "not set".
	FileMergeSet = "set"
	// FileMergeEnableOnly: true replaces the profile value; false counts as
	// "not set", so a key the profile enables cannot be turned off from the
	// file.
	FileMergeEnableOnly = "enable-only"
	// FileMergeDisableOnly: false — or leaving the key out of the file —
	// replaces the profile value; true keeps the profile value.
	FileMergeDisableOnly = "disable-only"
	// FileMergeReplace: the file value always replaces the profile value;
	// leaving the key out resets it to the zero value.
	FileMergeReplace = "replace"
	// FileMergeReset: loading any config file resets the key to its zero
	// value, whatever the file says.
	FileMergeReset = "reset"
	// FileMergeIgnored: the file value is dropped; the profile value always
	// applies.
	FileMergeIgnored = "ignored"
)

// What a flag does to the config key(s) it writes (applyFlags in both
// binaries).
const (
	// FlagEffectSet: a non-zero value overrides the key; the zero value
	// means "not set" and leaves the loaded value alone.
	FlagEffectSet = "set"
	// FlagEffectEnableOnly: true sets the key; false leaves it unchanged.
	FlagEffectEnableOnly = "enable-only"
	// FlagEffectDisableOnly: false sets the key; true leaves it unchanged.
	FlagEffectDisableOnly = "disable-only"
	// FlagEffectAuthoritative: the flag value — its default included —
	// always replaces the loaded value.
	FlagEffectAuthoritative = "authoritative"
	// FlagEffectNone: the flag writes no config key.
	FlagEffectNone = "none"
)

// Surface is the machine-readable configuration surface of one binary:
// every YAML key with its default and how a config-file value is merged,
// every named profile as the explicit set of keys it overrides, and every
// flag with the key(s) it writes. The binaries print it with
// `print-default-config`; the docs reference and the Helm chart are
// generated and checked from it.
type Surface struct {
	Binary   string                    `json:"binary"`
	Mode     Mode                      `json:"mode"`
	Keys     []KeyInfo                 `json:"keys"`
	Profiles map[string]map[string]any `json:"profiles"`
	Flags    []FlagInfo                `json:"flags"`
	// ProfileFlagGaps lists, per profile, the keys whose profile value is
	// NOT in effect when that profile is selected with --lakehouse.profile
	// instead of `profile:` in the config file.
	ProfileFlagGaps map[string][]string `json:"profile_flag_gaps,omitempty"`
}

// KeyInfo describes one YAML key.
type KeyInfo struct {
	Key     string `json:"key"`
	Type    string `json:"type"`
	Default any    `json:"default"`
	// Effective is set only when the binary, started with no config file
	// and no flags, runs with a value other than Default (a flag whose
	// default always wins, or the binary's own mode).
	Effective any `json:"effective,omitempty"`
	// File is one of the FileMerge* values.
	File string `json:"file"`
	// Flags are the flags that write this key.
	Flags []string `json:"flags,omitempty"`
}

// FlagInfo describes one flag.
type FlagInfo struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Default string   `json:"default"`
	Usage   string   `json:"usage"`
	Keys    []string `json:"keys"`
	// Effect is one of the FlagEffect* values.
	Effect string `json:"effect"`
}

// JSON renders the surface deterministically with one key, flag, profile
// override or gap per line, so a changed default shows up in review as a
// one-line diff that names it. Map keys are sorted and HTML is not escaped.
func (s *Surface) JSON() ([]byte, error) {
	var b bytes.Buffer
	var err error
	compact := func(v any) string {
		if err != nil {
			return ""
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetEscapeHTML(false)
		if e := enc.Encode(v); e != nil {
			err = e
			return ""
		}
		return strings.TrimSuffix(buf.String(), "\n")
	}
	list := func(name string, n int, item func(i int) string, last bool) {
		fmt.Fprintf(&b, "  %s: [", compact(name))
		for i := 0; i < n; i++ {
			sep := ","
			if i == n-1 {
				sep = "\n  "
			}
			fmt.Fprintf(&b, "\n    %s%s", item(i), sep)
		}
		b.WriteString("]")
		if !last {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}
	nested := func(name string, groups []string, entries func(group string) []string, last bool) {
		fmt.Fprintf(&b, "  %s: {", compact(name))
		for gi, g := range groups {
			fmt.Fprintf(&b, "\n    %s: ", compact(g))
			lines := entries(g)
			open, close := "{", "}"
			if strings.HasPrefix(name, "profile_flag") {
				open, close = "[", "]"
			}
			if len(lines) == 0 {
				b.WriteString(open + close)
			} else {
				b.WriteString(open)
				for li, l := range lines {
					sep := ","
					if li == len(lines)-1 {
						sep = ""
					}
					fmt.Fprintf(&b, "\n      %s%s", l, sep)
				}
				b.WriteString("\n    " + close)
			}
			if gi < len(groups)-1 {
				b.WriteString(",")
			}
		}
		b.WriteString("\n  }")
		if !last {
			b.WriteString(",")
		}
		b.WriteString("\n")
	}

	b.WriteString("{\n")
	fmt.Fprintf(&b, "  \"binary\": %s,\n  \"mode\": %s,\n", compact(s.Binary), compact(s.Mode))
	list("keys", len(s.Keys), func(i int) string { return compact(s.Keys[i]) }, false)
	nested("profiles", sortedKeys(s.Profiles), func(p string) []string {
		var lines []string
		for _, k := range sortedKeys(s.Profiles[p]) {
			lines = append(lines, compact(k)+": "+compact(s.Profiles[p][k]))
		}
		return lines
	}, false)
	list("flags", len(s.Flags), func(i int) string { return compact(s.Flags[i]) }, s.ProfileFlagGaps == nil)
	if s.ProfileFlagGaps != nil {
		nested("profile_flag_gaps", sortedKeys(s.ProfileFlagGaps), func(p string) []string {
			var lines []string
			for _, k := range s.ProfileFlagGaps[p] {
				lines = append(lines, compact(k))
			}
			return lines
		}, true)
	}
	b.WriteString("}\n")
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// DescribeSurface builds the configuration surface of one binary.
//
// fs must hold the flags the binary defines, with Values bound to the
// variables apply reads; apply is the binary's flag-application function.
// The flag→key mapping and each flag's effect are discovered by setting
// every flag to a probe value, running apply and diffing the result, so no
// mapping is maintained by hand. Probe values are ordinary non-zero values
// ("7", "7s", "0.7", "sentinel:7:7") that apply accepts without exiting.
// Every flag is restored to its default before DescribeSurface returns;
// call it before flags are parsed.
func DescribeSurface(binary string, mode Mode, fs *flag.FlagSet, apply func(*Config)) (*Surface, error) {
	fields := configFields()
	byKey := make(map[string]reflect.Type, len(fields))
	for _, f := range fields {
		byKey[f.key] = f.typ
	}

	def := flattenConfig(Default())
	effCfg := binaryDefaults(mode)
	apply(effCfg)
	eff := flattenConfig(effCfg)

	s := &Surface{Binary: binary, Mode: mode, Profiles: map[string]map[string]any{}}

	keyFlags := map[string][]string{}
	var flagErr error
	fs.VisitAll(func(f *flag.Flag) {
		if flagErr != nil {
			return
		}
		info, err := describeFlag(fs, f, byKey, apply)
		if err != nil {
			flagErr = err
			return
		}
		for _, k := range info.Keys {
			keyFlags[k] = append(keyFlags[k], f.Name)
		}
		s.Flags = append(s.Flags, info)
	})
	if flagErr != nil {
		return nil, flagErr
	}

	for _, f := range fields {
		ki := KeyInfo{Key: f.key, Type: typeName(f.typ), Default: def[f.key], File: fileMerge(mergeConfig, f.key, f.typ), Flags: keyFlags[f.key]}
		if !sameValue(def[f.key], eff[f.key]) {
			ki.Effective = eff[f.key]
		}
		s.Keys = append(s.Keys, ki)
	}
	// The binary passes its own mode to LoadWithMode, which wins over
	// `mode:` in the file.
	if other := otherMode(mode); other != "" {
		loaded, err := loadConfigBytes([]byte("lakehouse:\n  mode: "+string(other)+"\n"), mode, "")
		if err == nil && loaded.Mode == mode {
			for i := range s.Keys {
				if s.Keys[i].Key == "mode" {
					s.Keys[i].File = FileMergeIgnored
				}
			}
		}
	}

	for _, p := range ValidProfiles() {
		over := map[string]any{}
		for k, v := range flattenConfig(ProfileConfig(p)) {
			// ProfileConfig names itself; that is not an override.
			if k != "profile" && !sameValue(def[k], v) {
				over[k] = v
			}
		}
		s.Profiles[string(p)] = over
	}

	gaps, err := profileFlagGaps(fs, mode, apply)
	if err != nil {
		return nil, err
	}
	s.ProfileFlagGaps = gaps
	return s, nil
}

// binaryDefaults is the config a binary loads when started without
// --lakehouse.config.
func binaryDefaults(mode Mode) *Config {
	cfg, err := LoadWithMode("", mode, "")
	if err != nil {
		panic(err) // no file is read, so loading cannot fail
	}
	return cfg
}

func otherMode(m Mode) Mode {
	switch m {
	case ModeLogs:
		return ModeTraces
	case ModeTraces:
		return ModeLogs
	}
	return ""
}

// profileFlagGaps compares, for every profile, what the profile defines
// (ProfileConfig) with what the binary runs after --lakehouse.profile=X and
// returns the keys that differ.
func profileFlagGaps(fs *flag.FlagSet, mode Mode, apply func(*Config)) (map[string][]string, error) {
	pf := fs.Lookup(ProfileFlagName)
	if pf == nil {
		return nil, nil
	}
	gaps := map[string][]string{}
	for _, p := range ValidProfiles() {
		want := ProfileConfig(p)
		want.Mode = mode
		apply(want)

		got := binaryDefaults(mode)
		if err := withFlag(fs, pf, string(p), func() { apply(got) }); err != nil {
			return nil, err
		}
		gaps[string(p)] = diffKeys(flattenConfig(want), flattenConfig(got))
	}
	return gaps, nil
}

// withFlag sets f to value, runs fn and restores the flag's default.
func withFlag(fs *flag.FlagSet, f *flag.Flag, value string, fn func()) error {
	if err := fs.Set(f.Name, value); err != nil {
		return err
	}
	fn()
	if err := fs.Set(f.Name, f.DefValue); err != nil {
		return fmt.Errorf("restore flag -%s to %q: %w", f.Name, f.DefValue, err)
	}
	return nil
}

func describeFlag(fs *flag.FlagSet, f *flag.Flag, byKey map[string]reflect.Type, apply func(*Config)) (FlagInfo, error) {
	info := FlagInfo{Name: f.Name, Type: flagType(f), Default: f.DefValue, Usage: f.Usage, Keys: []string{}, Effect: FlagEffectNone}
	for _, probe := range flagProbes(f, info.Type) {
		keys, err := flagWrites(fs, f, probe, apply)
		if err != nil {
			return info, err
		}
		if len(keys) > 0 {
			info.Keys = keys
			break
		}
	}
	if len(info.Keys) == 0 {
		return info, nil
	}
	effect, err := flagEffect(fs, f, info.Keys[0], byKey[info.Keys[0]], apply)
	if err != nil {
		return info, err
	}
	info.Effect = effect
	return info, nil
}

func flagType(f *flag.Flag) string {
	g, ok := f.Value.(flag.Getter)
	if !ok {
		return "string"
	}
	switch g.Get().(type) {
	case bool:
		return "bool"
	case int, int64, uint, uint64:
		return "int"
	case float64:
		return "float"
	case time.Duration:
		return "duration"
	}
	return "string"
}

// flagProbes returns the non-default values tried, in order, to find the
// key(s) a flag writes. The second string probe fits the orgid:account:project
// alias syntax, which ignores the first.
func flagProbes(f *flag.Flag, typ string) []string {
	var candidates []string
	switch typ {
	case "bool":
		def, _ := strconv.ParseBool(f.DefValue)
		return []string{strconv.FormatBool(!def)}
	case "int":
		candidates = []string{"7", "11"}
	case "float":
		candidates = []string{"0.7", "0.3"}
	case "duration":
		candidates = []string{"7s", "11s"}
	default:
		candidates = []string{"7", "sentinel:7:7"}
	}
	out := candidates[:0]
	for _, c := range candidates {
		if c != f.DefValue {
			out = append(out, c)
		}
	}
	return out
}

// flagWrites returns the keys that change when f is set to probe. It tries
// an all-zero config and the built-in defaults and keeps the smaller
// non-empty change set: a flag that only enables a key shows up against
// zero values, while --lakehouse.profile rewrites every zero key but only
// `profile` on top of the defaults.
func flagWrites(fs *flag.FlagSet, f *flag.Flag, probe string, apply func(*Config)) ([]string, error) {
	var best []string
	for _, base := range []func() *Config{func() *Config { return &Config{} }, Default} {
		ref := base()
		apply(ref)
		got := base()
		if err := fs.Set(f.Name, probe); err != nil {
			// The flag's parser rejects this probe; the caller tries the next.
			return nil, nil
		}
		apply(got)
		if err := fs.Set(f.Name, f.DefValue); err != nil {
			return nil, fmt.Errorf("restore flag -%s to %q: %w", f.Name, f.DefValue, err)
		}
		if diff := diffKeys(flattenConfig(ref), flattenConfig(got)); len(diff) > 0 && (best == nil || len(diff) < len(best)) {
			best = diff
		}
	}
	return best, nil
}

func flagEffect(fs *flag.FlagSet, f *flag.Flag, key string, typ reflect.Type, apply func(*Config)) (string, error) {
	if typ.Kind() == reflect.Bool && flagType(f) == "bool" {
		// Try both flag values against a false and a true loaded key, so an
		// inverted flag (true writes false) classifies by what it can do to
		// the key.
		enables, disables := false, false
		for _, value := range []string{"true", "false"} {
			fromFalse, err := boolAfterFlag(fs, f, key, false, value, apply)
			if err != nil {
				return "", err
			}
			fromTrue, err := boolAfterFlag(fs, f, key, true, value, apply)
			if err != nil {
				return "", err
			}
			enables = enables || fromFalse
			disables = disables || !fromTrue
		}
		// A plain bool flag cannot tell "set to its default" from "not set",
		// so a flag that writes both true and false writes its default too.
		switch {
		case enables && disables:
			return FlagEffectAuthoritative, nil
		case enables:
			return FlagEffectEnableOnly, nil
		case disables:
			return FlagEffectDisableOnly, nil
		}
		// The flag moved the key only on an all-zero config: its write
		// depends on other values the built-in defaults already set.
		return FlagEffectNone, nil
	}
	// Non-bool: does the flag's default overwrite a loaded value?
	cfg := Default()
	probe := probeValue(typ, 0)
	mustSetKey(cfg, key, probe)
	apply(cfg)
	if !sameValue(flattenConfig(cfg)[key], plainValue(probe)) {
		return FlagEffectAuthoritative, nil
	}
	return FlagEffectSet, nil
}

// boolAfterFlag loads the defaults with key forced to base, applies the
// flags with f set to value and returns the resulting key value.
func boolAfterFlag(fs *flag.FlagSet, f *flag.Flag, key string, base bool, value string, apply func(*Config)) (bool, error) {
	cfg := Default()
	mustSetKey(cfg, key, reflect.ValueOf(base))
	if err := withFlag(fs, f, value, func() { apply(cfg) }); err != nil {
		return false, err
	}
	v, _ := flattenConfig(cfg)[key].(bool)
	return v, nil
}

// fileMerge classifies how merge (mergeConfig in DescribeSurface) treats a
// value of key coming from the config file.
func fileMerge(merge func(base, file *Config) *Config, key string, typ reflect.Type) string {
	merged := func(base, file reflect.Value) any {
		b, o := Default(), &Config{}
		mustSetKey(b, key, base)
		if file.IsValid() {
			mustSetKey(o, key, file)
		}
		return flattenConfig(merge(b, o))[key]
	}
	if typ.Kind() == reflect.Bool {
		t, f := reflect.ValueOf(true), reflect.ValueOf(false)
		enables := merged(f, t) == true
		disables := merged(t, f) == false
		switch {
		case enables && disables:
			return FileMergeReplace
		case enables:
			return FileMergeEnableOnly
		case disables:
			return FileMergeDisableOnly
		}
		return FileMergeIgnored
	}
	base, file := probeValue(typ, 0), probeValue(typ, 1)
	wins := sameValue(merged(base, file), plainValue(file))
	kept := sameValue(merged(base, reflect.Value{}), plainValue(base))
	switch {
	case wins && !kept:
		return FileMergeReplace
	case wins:
		return FileMergeSet
	case !kept:
		return FileMergeReset
	}
	return FileMergeIgnored
}

// ---------------------------------------------------------------------------
// reflection helpers
// ---------------------------------------------------------------------------

var durationType = reflect.TypeOf(time.Duration(0))

type configField struct {
	key string
	typ reflect.Type
}

// configFields lists every leaf YAML key of Config, sorted.
func configFields() []configField {
	return leafFields(reflect.TypeOf(Config{}))
}

// leafFields lists the leaf YAML keys of struct type t, sorted. Nested
// structs are walked; slices, maps and scalars are leaves.
func leafFields(root reflect.Type) []configField {
	var out []configField
	var walk func(t reflect.Type, prefix string)
	walk = func(t reflect.Type, prefix string) {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := yamlName(f)
			if name == "" {
				continue
			}
			key := name
			if prefix != "" {
				key = prefix + "." + name
			}
			if f.Type.Kind() == reflect.Struct {
				walk(f.Type, key)
				continue
			}
			out = append(out, configField{key: key, typ: f.Type})
		}
	}
	walk(root, "")
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func yamlName(f reflect.StructField) string {
	if !f.IsExported() {
		return ""
	}
	tag, ok := f.Tag.Lookup("yaml")
	if !ok {
		return ""
	}
	name := strings.Split(tag, ",")[0]
	if name == "-" {
		return ""
	}
	return name
}

// fieldByKey returns the addressable field of cfg for a dotted YAML key.
func fieldByKey(cfg *Config, key string) (reflect.Value, error) {
	v := reflect.ValueOf(cfg).Elem()
	for _, part := range strings.Split(key, ".") {
		if v.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("config key %q: %q is not a section", key, part)
		}
		found := false
		for i := 0; i < v.NumField(); i++ {
			if yamlName(v.Type().Field(i)) == part {
				v = v.Field(i)
				found = true
				break
			}
		}
		if !found {
			return reflect.Value{}, fmt.Errorf("unknown config key %q", key)
		}
	}
	return v, nil
}

func setKey(cfg *Config, key string, val reflect.Value) error {
	field, err := fieldByKey(cfg, key)
	if err != nil {
		return err
	}
	if !val.Type().AssignableTo(field.Type()) {
		if !val.Type().ConvertibleTo(field.Type()) {
			return fmt.Errorf("config key %q: cannot assign %s to %s", key, val.Type(), field.Type())
		}
		val = val.Convert(field.Type())
	}
	field.Set(val)
	return nil
}

// mustSetKey is setKey for keys taken from configFields, which always exist.
func mustSetKey(cfg *Config, key string, val reflect.Value) {
	if err := setKey(cfg, key, val); err != nil {
		panic(err)
	}
}

// flattenConfig maps every leaf key to a JSON-ready value.
func flattenConfig(cfg *Config) map[string]any {
	out := map[string]any{}
	for _, f := range configFields() {
		field, _ := fieldByKey(cfg, f.key)
		out[f.key] = plainValue(field)
	}
	return out
}

// plainValue converts a config value to plain JSON types. Durations are
// spelled the way operators write them ("5m", not "5m0s"); nil and empty
// collections both become empty collections.
func plainValue(v reflect.Value) any {
	if v.Type() == durationType {
		return compactDuration(time.Duration(v.Int()))
	}
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.Slice, reflect.Array:
		out := make([]any, v.Len())
		for i := range out {
			out[i] = plainValue(v.Index(i))
		}
		return out
	case reflect.Map:
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out[fmt.Sprint(iter.Key().Interface())] = plainValue(iter.Value())
		}
		return out
	case reflect.Struct:
		out := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			if name := yamlName(v.Type().Field(i)); name != "" {
				out[name] = plainValue(v.Field(i))
			}
		}
		return out
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return plainValue(v.Elem())
	}
	return fmt.Sprint(v.Interface())
}

// compactDuration renders 1h0m0s as "1h" and 5m0s as "5m".
func compactDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}

// typeName is the documented type of a key: string, bool, int, float,
// duration, []T, map[string]T, with object for structs.
func typeName(t reflect.Type) string {
	if t == durationType {
		return "duration"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Slice, reflect.Array:
		return "[]" + typeName(t.Elem())
	case reflect.Map:
		return "map[" + typeName(t.Key()) + "]" + typeName(t.Elem())
	case reflect.Struct:
		return "object"
	case reflect.Ptr:
		return typeName(t.Elem())
	}
	return t.Kind().String()
}

// probeValue builds a non-zero value of t; variants 0 and 1 differ.
func probeValue(t reflect.Type, variant int) reflect.Value {
	v := reflect.New(t).Elem()
	switch {
	case t == durationType:
		v.SetInt(int64(time.Duration(1234+variant) * time.Second))
	case t.Kind() == reflect.Bool:
		v.SetBool(variant == 0)
	case t.Kind() == reflect.String:
		v.SetString("probe-" + strconv.Itoa(variant))
	case t.Kind() >= reflect.Int && t.Kind() <= reflect.Int64:
		v.SetInt(int64(1234 + variant))
	case t.Kind() >= reflect.Uint && t.Kind() <= reflect.Uint64:
		v.SetUint(uint64(1234 + variant))
	case t.Kind() == reflect.Float32 || t.Kind() == reflect.Float64:
		v.SetFloat(1.25 + float64(variant))
	case t.Kind() == reflect.Slice:
		s := reflect.MakeSlice(t, 1, 1)
		s.Index(0).Set(probeValue(t.Elem(), variant))
		v.Set(s)
	case t.Kind() == reflect.Map:
		m := reflect.MakeMap(t)
		m.SetMapIndex(probeValue(t.Key(), variant), probeValue(t.Elem(), variant))
		v.Set(m)
	case t.Kind() == reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).IsExported() {
				v.Field(i).Set(probeValue(t.Field(i).Type, variant))
			}
		}
	case t.Kind() == reflect.Ptr:
		p := reflect.New(t.Elem())
		p.Elem().Set(probeValue(t.Elem(), variant))
		v.Set(p)
	}
	return v
}

func sameValue(a, b any) bool {
	ab, errA := json.Marshal(a)
	bb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(ab, bb)
}

// diffKeys returns the sorted keys whose values differ between a and b.
func diffKeys(a, b map[string]any) []string {
	out := []string{}
	for k, av := range a {
		if bv, ok := b[k]; !ok || !sameValue(av, bv) {
			out = append(out, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
