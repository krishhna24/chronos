package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/krishhna24/chronos/internal/api"
)

func (a *app) historyCommand() *cobra.Command {
	var since, until, output string
	cmd := &cobra.Command{
		Use:   "history (<type>/<name> | <type> <name>)",
		Short: "List the recorded versions of an object, with observation gaps",
		Long: `List every version of an object that Chronos recorded, oldest first.

Rows marked GAP are periods in which Chronos was not observing the resource
type: changes inside them are unknown, and a "resync" version right after a
gap is a change that happened at some unknown time inside it.`,
		Example: `  kubectl at history deployment/web
  kubectl at history pod web-1 -n prod --since -2h --until -1h`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runHistory(cmd.Context(), args, since, until, output)
		},
	}
	f := cmd.Flags()
	f.StringVar(&since, "since", "", "Only versions at or after this time (default: the start of retained history)")
	f.StringVar(&until, "until", "", "Only versions at or before this time (default: now)")
	f.StringVarP(&output, "output", "o", "", "Output format: json or yaml (the raw API response)")
	return cmd
}

func (a *app) runHistory(ctx context.Context, args []string, since, until, output string) error {
	if output != "" && output != "json" && output != "yaml" {
		return fmt.Errorf("unable to match a printer suitable for the output format %q, allowed formats are: json,yaml", output)
	}
	typ, name, err := splitTypeName(args)
	if err != nil {
		return err
	}
	p := a.parser()
	var from, to time.Time
	if since != "" {
		if from, err = p.Parse(since); err != nil {
			return fmt.Errorf("--since: %w", err)
		}
	}
	if until != "" {
		if to, err = p.Parse(until); err != nil {
			return fmt.Errorf("--until: %w", err)
		}
	}
	if !from.IsZero() && !to.IsZero() && !from.Before(to) {
		return fmt.Errorf("--since must be before --until")
	}
	c, err := a.client()
	if err != nil {
		return err
	}
	rt, _, err := a.lookupType(ctx, c, typ)
	if err != nil {
		return err
	}
	ns := ""
	if rt.Namespaced {
		ns = a.namespace()
	}
	var fromS, toS string
	if !from.IsZero() {
		fromS = wireTime(from)
	}
	if !to.IsZero() {
		toS = wireTime(to)
	}
	h, err := c.history(ctx, api.ObjectKey{Resource: rt.ID, Namespace: ns, Name: name}, fromS, toS)
	if err != nil {
		return err
	}
	if output != "" {
		raw, err := json.Marshal(h)
		if err != nil {
			return fmt.Errorf("encode history: %w", err)
		}
		if output == "json" {
			return writeJSON(a.o.Out, raw)
		}
		return writeYAML(a.o.Out, raw)
	}

	now := a.o.Now()
	tf := timeFmt{loc: a.loc(), ref: now}
	if h.Truncated {
		a.warnf("only the first %d versions are shown; narrow the range with --since/--until.", len(h.Versions))
	}
	if len(h.Coverage) == 0 {
		a.warnf("Chronos was not observing %s at any time in this range; history is unavailable.", rt.Resource)
	}
	if len(h.Versions) == 0 {
		if since == "" && until == "" {
			return &verbatimError{msg: fmt.Sprintf("Error from server (NotFound): no recorded history for %s %q", rt.Resource, name)}
		}
		_, _ = fmt.Fprintf(a.o.ErrOut, "No versions of %s %q were recorded in this range.\n", rt.Resource, name)
		return nil
	}

	type row struct {
		t     time.Time
		gap   bool
		cells string
	}
	var rows []row
	for _, v := range h.Versions {
		rows = append(rows, row{t: v.ObservedAt, cells: strings.Join(safeCells([]string{
			tf.full(v.ObservedAt), v.Type, v.Origin, shortUID(v.UID), orNone(v.ResourceVersion),
		}), "\t")})
	}
	gapRow := func(start time.Time, end *time.Time, reason string) row {
		desc := "until now"
		if !to.IsZero() {
			desc = "until after " + tf.full(to)
		}
		if end != nil {
			desc = "until " + tf.full(*end)
		}
		if reason != "" {
			desc += " (" + safeCell(reason) + ")"
		}
		desc += ": not observing, changes in this period are unknown"
		return row{t: start, gap: true, cells: tf.full(start) + "\tGAP\t-\t-\t-\t" + desc}
	}
	if !from.IsZero() && len(h.Coverage) > 0 && from.Before(h.Coverage[0].Start) {
		first := h.Coverage[0].Start
		rows = append(rows, row{t: from, gap: true, cells: tf.full(from) + "\tGAP\t-\t-\t-\t" +
			"until " + tf.full(first) + ": not observed yet, changes before that are unknown"})
	}
	for i, iv := range h.Coverage {
		if iv.End == nil {
			continue
		}
		var next *time.Time
		if i+1 < len(h.Coverage) {
			next = &h.Coverage[i+1].Start
		}
		if next == nil && !to.IsZero() && to.Before(*iv.End) {
			continue
		}
		rows = append(rows, gapRow(*iv.End, next, iv.EndReason))
	}
	// A gap row sorts after versions at the same instant: an End is never
	// earlier than the records before it.
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].t.Equal(rows[j].t) {
			return !rows[i].gap && rows[j].gap
		}
		return rows[i].t.Before(rows[j].t)
	})

	tw := newTabWriter(a.o.Out)
	_, _ = fmt.Fprintln(tw, "TIME\tTYPE\tORIGIN\tUID\tRESOURCEVERSION")
	for _, r := range rows {
		_, _ = fmt.Fprintln(tw, r.cells)
	}
	return tw.Flush()
}

func (a *app) resourcesCommand() *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "resources",
		Short: "List the resource types Chronos records and when it observed them",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if output != "" && output != "json" && output != "yaml" {
				return fmt.Errorf("unable to match a printer suitable for the output format %q, allowed formats are: json,yaml", output)
			}
			c, err := a.client()
			if err != nil {
				return err
			}
			rs, err := c.resources(cmd.Context())
			if err != nil {
				return err
			}
			if output != "" {
				raw, err := json.Marshal(rs)
				if err != nil {
					return fmt.Errorf("encode resources: %w", err)
				}
				if output == "json" {
					return writeJSON(a.o.Out, raw)
				}
				return writeYAML(a.o.Out, raw)
			}
			return a.printResources(rs)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Output format: json or yaml (the raw API response)")
	return cmd
}

func (a *app) printResources(rs *api.ResourcesResponse) error {
	tf := timeFmt{loc: a.loc(), ref: rs.Now}
	tw := newTabWriter(a.o.Out)
	_, _ = fmt.Fprintln(tw, "NAME\tSHORTNAMES\tAPIVERSION\tNAMESPACED\tKIND\tRECORDED\tCOVERAGE")
	for _, r := range rs.Resources {
		apiVersion := r.Version
		if r.Group != "" {
			apiVersion = r.Group + "/" + r.Version
		}
		recorded := "full"
		if r.MetadataOnly {
			recorded = "metadata only"
		}
		var ivs []string
		for _, iv := range r.Coverage {
			s := tf.short(iv.Start) + " to "
			if iv.End == nil {
				s += "now"
			} else {
				s += tf.short(*iv.End)
				if iv.EndReason != "" {
					s += " (" + iv.EndReason + ")"
				}
			}
			ivs = append(ivs, s)
		}
		cov := strings.Join(ivs, ", ")
		if cov == "" {
			cov = "never observed"
		}
		_, _ = fmt.Fprintln(tw, strings.Join(safeCells([]string{r.Resource, strings.Join(r.ShortNames, ","),
			apiVersion, fmt.Sprint(r.Namespaced), r.Kind, recorded, cov}), "\t"))
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if rs.Horizon != nil {
		_, err := fmt.Fprintf(a.o.Out, "\nHistory before %s has been removed by retention.\n", tf.full(*rs.Horizon))
		return err
	}
	return nil
}
