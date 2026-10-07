package schema

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestParseStreamFields(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    []StreamField
		wantErr bool
	}{
		{in: `{}`, want: nil},
		{in: `{app="x"}`, want: []StreamField{{"app", "x"}}},
		{in: `{app="x",ns="n1"}`, want: []StreamField{{"app", "x"}, {"ns", "n1"}}},
		{in: `{resource_attr:service.name="svc",name="GET /a"}`, want: []StreamField{{"resource_attr:service.name", "svc"}, {"name", "GET /a"}}},
		{in: `{msg="a\"b,c=\"d",k="v"}`, want: []StreamField{{"msg", `a"b,c="d`}, {"k", "v"}}},
		{in: `{app=""}`, want: []StreamField{{"app", ""}}},
		{in: `app="x"`, wantErr: true},
		{in: `{app="x"`, wantErr: true},
		{in: `{app=x}`, wantErr: true},
		{in: `{app="x" ns="y"}`, wantErr: true},
		{in: `{app="x`, wantErr: true},
		{in: ``, wantErr: true},
	} {
		got, err := ParseStreamFields(nil, tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: err = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func FuzzParseStreamFields(f *testing.F) {
	for _, s := range []string{`{}`, `{a="b"}`, `{a="b",c="d\"e"}`, `{a=`, `"`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		fields, err := ParseStreamFields(nil, s)
		if err != nil {
			return
		}
		// A parsed name re-marshals and parses to the same tags.
		var b strings.Builder
		b.WriteByte('{')
		for i, fl := range fields {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(fl.Name + "=" + strconv.Quote(fl.Value))
		}
		b.WriteByte('}')
		again, err := ParseStreamFields(nil, b.String())
		if err != nil || !reflect.DeepEqual(fields, again) {
			// A name that holds `="` or a comma re-parses differently; only
			// names free of both are expected to round-trip.
			for _, fl := range fields {
				if strings.Contains(fl.Name, `="`) || strings.Contains(fl.Name, ",") {
					return
				}
			}
			t.Fatalf("%q -> %v -> %q -> %v (%v)", s, fields, b.String(), again, err)
		}
	})
}
