package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"k8s.io/client-go/util/jsonpath"
)

// templateOutput is a parsed -o jsonpath=... or -o custom-columns=...
// format. Both follow kubectl: jsonpath runs once over the object, or over
// the v1 List for list output; custom-columns prints one row per object.
type templateOutput struct {
	jsonpath *jsonpath.JSONPath
	columns  []column
}

type column struct {
	header string
	path   *jsonpath.JSONPath
}

// parseTemplateOutput returns nil when format is not a template format.
func parseTemplateOutput(format string) (*templateOutput, error) {
	kind, spec, ok := strings.Cut(format, "=")
	if !ok {
		switch format {
		case "jsonpath", "custom-columns":
			return nil, fmt.Errorf("-o %s needs a template, e.g. -o %s=%s", format, format, map[string]string{
				"jsonpath": "'{.metadata.name}'", "custom-columns": "NAME:.metadata.name",
			}[format])
		}
		return nil, nil
	}
	switch kind {
	case "jsonpath":
		jp := jsonpath.New("output").AllowMissingKeys(true)
		if err := jp.Parse(spec); err != nil {
			return nil, fmt.Errorf("error parsing jsonpath %s: %w", spec, err)
		}
		return &templateOutput{jsonpath: jp}, nil
	case "custom-columns":
		var cols []column
		for _, part := range strings.Split(spec, ",") {
			header, expr, ok := strings.Cut(part, ":")
			if !ok || header == "" || expr == "" {
				return nil, fmt.Errorf("unexpected custom-columns spec %q, expected <header>:<json-path-expr>", part)
			}
			expr = strings.TrimSpace(expr)
			if !strings.HasPrefix(expr, "{") {
				expr = "{" + expr + "}"
			}
			jp := jsonpath.New(header).AllowMissingKeys(true)
			if err := jp.Parse(expr); err != nil {
				return nil, fmt.Errorf("error parsing custom-columns %q: %w", part, err)
			}
			cols = append(cols, column{header: header, path: jp})
		}
		if len(cols) == 0 {
			return nil, errors.New("custom-columns needs at least one column")
		}
		return &templateOutput{columns: cols}, nil
	}
	return nil, nil
}

func decodeAny(raw []byte) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, errors.New("the recorded object is not valid JSON")
	}
	return v, nil
}

// printJSONPath runs the template over one document (an object or a List).
// The values come from recorded objects, so control characters are
// escaped like every other terminal output.
func (t *templateOutput) printJSONPath(w io.Writer, doc []byte) error {
	v, err := decodeAny(doc)
	if err != nil {
		return err
	}
	var b strings.Builder
	if err := t.jsonpath.Execute(&b, v); err != nil {
		return fmt.Errorf("error executing jsonpath: %w", err)
	}
	_, err = io.WriteString(w, safeText(b.String()))
	return err
}

// printColumns prints a header and one row per object.
func (t *templateOutput) printColumns(w io.Writer, objs [][]byte) error {
	tw := newTabWriter(w)
	headers := make([]string, len(t.columns))
	for i, c := range t.columns {
		headers[i] = c.header
	}
	_, _ = fmt.Fprintln(tw, strings.Join(safeCells(headers), "\t"))
	for _, raw := range objs {
		v, err := decodeAny(raw)
		if err != nil {
			return err
		}
		row := make([]string, len(t.columns))
		for i, c := range t.columns {
			row[i] = columnValue(c.path, v)
		}
		_, _ = fmt.Fprintln(tw, strings.Join(safeCells(row), "\t"))
	}
	return tw.Flush()
}

// columnValue mirrors kubectl's custom-columns printer: every match is
// printed, comma-separated, and a missing value is <none>.
func columnValue(jp *jsonpath.JSONPath, v any) string {
	results, err := jp.FindResults(v)
	if err != nil {
		return "<none>"
	}
	var vals []string
	for _, set := range results {
		for _, r := range set {
			if !r.IsValid() || (r.Kind() == reflect.Interface && r.IsNil()) {
				continue
			}
			x := r.Interface()
			switch x := x.(type) {
			case string:
				vals = append(vals, x)
			case map[string]any, []any:
				b, _ := json.Marshal(x)
				vals = append(vals, string(b))
			default:
				vals = append(vals, fmt.Sprint(x))
			}
		}
	}
	if len(vals) == 0 {
		return "<none>"
	}
	return strings.Join(vals, ",")
}
