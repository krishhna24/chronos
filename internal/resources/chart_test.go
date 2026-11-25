package resources

import (
	"os"
	"slices"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The Helm chart keeps its own copy of the builtin types (it must resolve
// config.resources and generate RBAC at render time). Fail when the two
// drift apart, or a type could be recorded without RBAC for it.
func TestChartTypesMatchBuiltin(t *testing.T) {
	b, err := os.ReadFile("../../deploy/helm/chronos/templates/_helpers.tpl")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	const open = `{{- define "chronos.builtinTypes" -}}`
	i := strings.Index(src, open)
	if i < 0 {
		t.Fatal("chronos.builtinTypes not found in the chart")
	}
	body := src[i+len(open):]
	body = body[:strings.Index(body, "{{- end }}")]
	var chart []struct {
		Group, Version, Resource, Kind string
		ShortNames                     []string `json:"shortNames"`
	}
	if err := yaml.Unmarshal([]byte(body), &chart); err != nil {
		t.Fatalf("parse chart types: %v", err)
	}
	if len(chart) != len(Builtin) {
		t.Fatalf("chart has %d types, Builtin has %d", len(chart), len(Builtin))
	}
	for i, c := range chart {
		b := Builtin[i]
		if c.Group != b.Group || c.Version != b.Version || c.Resource != b.Resource || c.Kind != b.Kind ||
			!slices.Equal(c.ShortNames, b.ShortNames) && (len(c.ShortNames) != 0 || len(b.ShortNames) != 0) {
			t.Errorf("type %d differs: chart %+v, Builtin %+v", i, c, b)
		}
	}
}
