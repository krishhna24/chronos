package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/krishhna24/chronos/internal/api"
)

const (
	ansiReset = "\x1b[0m"
	ansiBold  = "\x1b[1m"
	ansiRed   = "\x1b[31m"
	ansiGreen = "\x1b[32m"
	ansiCyan  = "\x1b[36m"
)

func (a *app) diffCommand() *cobra.Command {
	color := "auto"
	cmd := &cobra.Command{
		Use:   "diff <from> <to> (<type>/<name> | <type> <name>)",
		Short: "Show how an object changed between two times",
		Long: `Show a unified diff of an object between two times.

Exit status is 0 when there are no differences, 1 when there are differences,
and greater than 1 on error, like kubectl diff.`,
		Example: `  kubectl at diff -1h now deployment/web
  kubectl at diff 14:00 14:30 configmap app-config -n prod`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withCode(a.runDiff(cmd.Context(), args, color), 2)
		},
	}
	cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return withCode(fmt.Errorf("%w\nSee '%s --help' for usage", err, c.CommandPath()), 2)
	})
	cmd.Flags().StringVar(&color, "color", "auto", "Colorize the diff: auto (when stdout is a terminal), always or never")
	return cmd
}

func (a *app) runDiff(ctx context.Context, args []string, color string) error {
	if color != "auto" && color != "always" && color != "never" {
		return fmt.Errorf("invalid --color %q: want auto, always or never", color)
	}
	if len(args) < 3 {
		return errors.New(`usage: kubectl at diff <from> <to> <type>/<name>, e.g. "kubectl at diff -1h now deployment/web"`)
	}
	typ, name, err := splitTypeName(args[2:])
	if err != nil {
		return err
	}
	p := a.parser()
	from, err := p.Parse(args[0])
	if err != nil {
		return fmt.Errorf("<from>: %w", err)
	}
	to, err := p.Parse(args[1])
	if err != nil {
		return fmt.Errorf("<to>: %w", err)
	}
	if !from.Before(to) {
		return fmt.Errorf("<from> (%s) must be before <to> (%s)", from.In(a.loc()).Format("2006-01-02 15:04:05"), to.In(a.loc()).Format("2006-01-02 15:04:05"))
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
	d, err := c.diff(ctx, api.ObjectKey{Resource: rt.ID, Namespace: ns, Name: name}, wireTime(from), wireTime(to))
	if err != nil {
		return err
	}
	tf := timeFmt{loc: a.loc(), ref: to}

	fromPresent := d.From.Existence == api.ExistencePresent
	toPresent := d.To.Existence == api.ExistencePresent
	if !fromPresent && !toPresent {
		if d.From.Existence == api.ExistenceAbsent && d.To.Existence == api.ExistenceAbsent {
			return &verbatimError{msg: fmt.Sprintf("Error from server (NotFound): %s %q not found at %s or at %s",
				rt.Resource, name, tf.short(from), tf.short(to))}
		}
		return fmt.Errorf("nothing to compare: %s; %s",
			sideProblem(tf, rt.Resource, name, d.From), sideProblem(tf, rt.Resource, name, d.To))
	}
	for _, side := range []api.ObjectState{d.From, d.To} {
		if side.Existence != api.ExistencePresent {
			a.warnf("%s; that side is shown as empty.", sideProblem(tf, rt.Resource, name, side))
		}
	}
	if d.Recreated {
		a.warnf("%s %q was deleted and recreated between %s and %s (uid %s -> %s); the two sides are different objects.",
			rt.Resource, name, tf.short(from), tf.short(to), shortUID(d.From.Version.UID), shortUID(d.To.Version.UID))
	}
	if !d.Complete {
		msg := fmt.Sprintf("Chronos was not observing %s for all of %s to %s; changes in between may be missing",
			rt.Resource, tf.short(from), tf.short(to))
		if d.Diff == "" {
			msg += ", even though no difference is shown"
		}
		a.warnf("%s.", msg)
	}
	if d.Diff == "" {
		return nil
	}
	colorize := color == "always" || (color == "auto" && a.o.Getenv("NO_COLOR") == "" && a.o.IsTerminal(a.o.Out))
	if err := writeDiff(a.o.Out, d.Diff, colorize); err != nil {
		return err
	}
	return &exitError{code: 1, silent: true}
}

// sideProblem describes a diff side that is not "present".
func sideProblem(tf timeFmt, resource, name string, st api.ObjectState) string {
	switch st.Existence {
	case api.ExistenceAbsent:
		s := fmt.Sprintf("%s %q did not exist at %s", resource, name, tf.short(st.At))
		if st.Version != nil && st.Version.Type == "Deleted" {
			s += fmt.Sprintf(" (deleted at %s)", tf.short(st.Version.ObservedAt))
		}
		return s
	case api.ExistenceUnknown:
		s := coverageProblem(tf, resource, st.At, st.Coverage)
		if historyUnavailable(st.Coverage) {
			return s + ", so its state then is unavailable"
		}
		return s + ", so its state then is unknown"
	}
	return fmt.Sprintf("%s %q is %s at %s", resource, name, st.Existence, tf.short(st.At))
}

func writeDiff(w io.Writer, diff string, color bool) error {
	if !color {
		_, err := io.WriteString(w, diff)
		return err
	}
	var b strings.Builder
	for _, line := range strings.SplitAfter(diff, "\n") {
		if line == "" {
			continue
		}
		body := strings.TrimSuffix(line, "\n")
		nl := line[len(body):]
		switch {
		case strings.HasPrefix(body, "---"), strings.HasPrefix(body, "+++"):
			b.WriteString(ansiBold + body + ansiReset + nl)
		case strings.HasPrefix(body, "@@"):
			b.WriteString(ansiCyan + body + ansiReset + nl)
		case strings.HasPrefix(body, "-"):
			b.WriteString(ansiRed + body + ansiReset + nl)
		case strings.HasPrefix(body, "+"):
			b.WriteString(ansiGreen + body + ansiReset + nl)
		default:
			b.WriteString(line)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}

func shortUID(uid string) string {
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}
