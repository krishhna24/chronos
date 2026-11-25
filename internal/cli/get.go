package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/krishhna24/chronos/internal/api"
)

type getOptions struct {
	app         *app
	output      string
	tmpl        *templateOutput
	selector    string
	allNS       bool
	showDeleted bool
}

func (g *getOptions) addFlags(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVarP(&g.output, "output", "o", "", "Output format: wide, json, yaml, name, jsonpath=<template> or custom-columns=<spec>")
	f.StringVarP(&g.selector, "selector", "l", "", "Label selector to filter on, e.g. -l app=web,tier!=db (matched against the historical labels)")
	f.BoolVarP(&g.allNS, "all-namespaces", "A", false, "List objects across all namespaces")
	f.BoolVar(&g.showDeleted, "show-deleted", false, "Also list objects that had been deleted by the given time (DELETED column)")
}

// run implements "kubectl at <time> get <type> [name]" and
// "kubectl at <time> get <type>/<name>".
func (g *getOptions) run(ctx context.Context, timeArg string, args []string) error {
	tmpl, err := parseTemplateOutput(g.output)
	if err != nil {
		return err
	}
	g.tmpl = tmpl
	switch {
	case tmpl != nil:
	case g.output == "", g.output == "wide", g.output == "json", g.output == "yaml", g.output == "name":
	default:
		return fmt.Errorf("unable to match a printer suitable for the output format %q, allowed formats are: custom-columns,json,jsonpath,name,wide,yaml", g.output)
	}
	var typ, name string
	switch {
	case len(args) == 0:
		return errors.New(`you must specify the type of resource to get, e.g. "kubectl at -20m get pods"`)
	case len(args) == 1 && strings.Contains(args[0], "/"):
		var err error
		if typ, name, err = splitTypeName(args); err != nil {
			return err
		}
	case len(args) <= 2:
		typ = args[0]
		if len(args) == 2 {
			name = args[1]
		}
	default:
		return fmt.Errorf("only one object can be retrieved at a time, got %q", strings.Join(args, " "))
	}
	if name != "" && g.selector != "" {
		return errors.New("name cannot be provided when a selector is specified")
	}
	if name != "" && g.allNS {
		return errors.New("a resource cannot be retrieved by name across all namespaces")
	}
	if name != "" && g.showDeleted {
		return errors.New("--show-deleted applies to lists; a single deleted object is reported as not found")
	}

	at, err := g.app.parser().Parse(timeArg)
	if err != nil {
		return err
	}
	c, err := g.app.client()
	if err != nil {
		return err
	}
	rt, _, err := g.app.lookupType(ctx, c, typ)
	if err != nil {
		return err
	}
	ns := ""
	if rt.Namespaced && !g.allNS {
		ns = g.app.namespace()
	}
	tf := timeFmt{loc: g.app.loc(), ref: at}
	if name != "" {
		return g.getOne(ctx, c, rt, ns, name, at, tf)
	}
	return g.getList(ctx, c, rt, ns, at, tf)
}

func (g *getOptions) getOne(ctx context.Context, c *client, rt api.Resource, ns, name string, at time.Time, tf timeFmt) error {
	st, err := c.state(ctx, api.ObjectKey{Resource: rt.ID, Namespace: ns, Name: name}, wireTime(at))
	if err != nil {
		return err
	}
	when := tf.short(at)
	switch st.Existence {
	case api.ExistencePresent:
	case api.ExistenceAbsent:
		msg := fmt.Sprintf("Error from server (NotFound): %s %q not found at %s", rt.Resource, name, when)
		if st.Version != nil && st.Version.Type == "Deleted" {
			msg += fmt.Sprintf(" (deleted at %s)", tf.short(st.Version.ObservedAt))
		}
		return &verbatimError{msg: msg}
	default: // unknown
		problem := coverageProblem(tf, rt.Resource, at, st.Coverage)
		if historyUnavailable(st.Coverage) {
			return fmt.Errorf("%s; history is unavailable", problem)
		}
		if len(st.Object) == 0 {
			msg := fmt.Sprintf("cannot tell whether %s %q existed at %s: %s", rt.Resource, name, when, problem)
			if st.Version != nil && st.Version.Type == "Deleted" {
				msg += fmt.Sprintf("; it was last seen deleted at %s", tf.short(st.Version.ObservedAt))
			} else {
				msg += "; it was never seen before the gap"
			}
			return errors.New(msg)
		}
		g.app.warnf("%s; showing last known state, which may be stale.", problem)
	}
	if rt.MetadataOnly && g.output == "" {
		g.app.warnf("%s are recorded as metadata only; type and data are not available.", rt.Resource)
	}
	w := g.app.o.Out
	switch {
	case g.tmpl != nil && g.tmpl.jsonpath != nil:
		return g.tmpl.printJSONPath(w, st.Object)
	case g.tmpl != nil:
		return g.tmpl.printColumns(w, [][]byte{st.Object})
	}
	switch g.output {
	case "json":
		return writeJSON(w, st.Object)
	case "yaml":
		return writeYAML(w, st.Object)
	case "name":
		_, err := fmt.Fprintln(w, objectName(rt, name))
		return err
	}
	return printTable(w, rt, []api.ObjectState{*st}, tableOptions{wide: g.output == "wide", at: at, tf: tf})
}

func (g *getOptions) getList(ctx context.Context, c *client, rt api.Resource, ns string, at time.Time, tf timeFmt) error {
	lr, err := c.list(ctx, rt.ID, ns, wireTime(at), g.selector, g.showDeleted)
	if err != nil {
		return err
	}
	if problem := coverageProblem(tf, rt.Resource, at, lr.Coverage); problem != "" {
		if historyUnavailable(lr.Coverage) {
			return fmt.Errorf("%s; history is unavailable", problem)
		}
		g.app.warnf("%s; showing last known state, which may be stale.", problem)
	}
	if rt.MetadataOnly && (g.output == "" || g.output == "wide") {
		g.app.warnf("%s are recorded as metadata only; type and data are not available.", rt.Resource)
	}

	w := g.app.o.Out
	if g.tmpl != nil && g.tmpl.columns != nil {
		var objs [][]byte
		for _, it := range lr.Items {
			if len(it.Object) > 0 {
				objs = append(objs, it.Object)
			}
		}
		return g.tmpl.printColumns(w, objs)
	}
	switch {
	case g.output == "json", g.output == "yaml", g.tmpl != nil:
		items := make([]json.RawMessage, 0, len(lr.Items))
		for _, it := range lr.Items {
			if len(it.Object) > 0 {
				items = append(items, it.Object)
			}
		}
		list, err := json.Marshal(map[string]any{
			"apiVersion": "v1",
			"kind":       "List",
			"metadata":   map[string]any{"resourceVersion": ""},
			"items":      items,
		})
		if err != nil {
			return fmt.Errorf("encode list: %w", err)
		}
		switch {
		case g.tmpl != nil:
			return g.tmpl.printJSONPath(w, list)
		case g.output == "json":
			return writeJSON(w, list)
		}
		return writeYAML(w, list)
	}

	if len(lr.Items) == 0 {
		where := ""
		switch {
		case !rt.Namespaced:
		case ns == "":
			where = " in any namespace"
		default:
			where = fmt.Sprintf(" in %s namespace", ns)
		}
		_, _ = fmt.Fprintf(g.app.o.ErrOut, "No resources found%s at %s.\n", where, tf.short(at))
		return nil
	}
	if g.output == "name" {
		for _, it := range lr.Items {
			if len(it.Object) == 0 {
				continue // deleted: only tables show those
			}
			if _, err := fmt.Fprintln(w, objectName(rt, it.Key.Name)); err != nil {
				return err
			}
		}
		return nil
	}
	return printTable(w, rt, lr.Items, tableOptions{
		wide: g.output == "wide", allNS: ns == "" && rt.Namespaced, showDeleted: g.showDeleted, at: at, tf: tf,
	})
}

// writeJSON prints raw JSON indented like kubectl (four spaces).
func writeJSON(w io.Writer, raw []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "    "); err != nil {
		return errors.New("the recorded object is not valid JSON")
	}
	buf.WriteByte('\n')
	_, err := w.Write(buf.Bytes())
	return err
}

func writeYAML(w io.Writer, raw []byte) error {
	y, err := yaml.JSONToYAML(raw)
	if err != nil {
		return errors.New("the recorded object cannot be rendered as YAML")
	}
	_, err = w.Write(y)
	return err
}
