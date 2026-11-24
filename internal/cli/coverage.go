package cli

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/krishhna24/chronos/internal/api"
)

// resolveType maps a user-supplied type name (plural, singular, kind,
// short name, resource.group or canonical ID; case-insensitive) to a type
// the server records. The server's list is authoritative, so a plugin
// older or newer than the server still resolves what the server supports.
func resolveType(rs *api.ResourcesResponse, name string) (api.Resource, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	for _, r := range rs.Resources {
		kind := strings.ToLower(r.Kind)
		cands := []string{r.ID, r.Resource, kind, strings.TrimSuffix(r.Resource, "s")}
		if r.Group != "" {
			cands = append(cands, r.Resource+"."+r.Group, kind+"."+r.Group)
		}
		cands = append(cands, r.ShortNames...)
		for _, c := range cands {
			if n == strings.ToLower(c) {
				return r, nil
			}
		}
	}
	var names []string
	for _, r := range rs.Resources {
		names = append(names, r.Resource)
	}
	return api.Resource{}, fmt.Errorf("the Chronos server doesn't have a resource type %q (recorded types: %s)",
		name, strings.Join(names, ", "))
}

// lookupType fetches the server's resource list and resolves name.
func (a *app) lookupType(ctx context.Context, c *client, name string) (api.Resource, *api.ResourcesResponse, error) {
	rs, err := c.resources(ctx)
	if err != nil {
		return api.Resource{}, nil, err
	}
	rt, err := resolveType(rs, name)
	return rt, rs, err
}

// splitTypeName accepts "<type>/<name>" or "<type> <name>".
func splitTypeName(args []string) (typ, name string, err error) {
	switch len(args) {
	case 1:
		t, n, ok := strings.Cut(args[0], "/")
		if !ok || t == "" || n == "" {
			return "", "", fmt.Errorf("expected <type>/<name> or <type> <name>, got %q", args[0])
		}
		return t, n, nil
	case 2:
		if strings.Contains(args[0], "/") || strings.Contains(args[1], "/") {
			return "", "", fmt.Errorf("expected <type>/<name> or <type> <name>, got %q", strings.Join(args, " "))
		}
		return args[0], args[1], nil
	}
	return "", "", fmt.Errorf("expected <type>/<name> or <type> <name>, got %d arguments", len(args))
}

// timeFmt formats times for humans in the user's zone. Times on the same
// day as ref are shown as a time of day only.
type timeFmt struct {
	loc *time.Location
	ref time.Time
}

func (f timeFmt) short(t time.Time) string {
	lt, lr := t.In(f.loc), f.ref.In(f.loc)
	if lt.Year() == lr.Year() && lt.YearDay() == lr.YearDay() {
		return lt.Format("15:04:05")
	}
	return lt.Format("2006-01-02 15:04:05")
}

// full is an RFC 3339 time in the user's zone, which can be pasted back
// into kubectl at.
func (f timeFmt) full(t time.Time) string { return t.In(f.loc).Format(time.RFC3339) }

// coverageProblem explains a non-observed coverage at instant at for
// resource; it is empty when the coverage is "observed".
func coverageProblem(tf timeFmt, resource string, at time.Time, c api.Coverage) string {
	switch c.State {
	case api.CoverageObserved:
		return ""
	case api.CoverageGap:
		s := fmt.Sprintf("Chronos was not observing %s at %s (gap", resource, tf.short(at))
		if c.Since != nil {
			s += " since " + tf.short(*c.Since)
		}
		if c.Reason != "" {
			s += ": " + c.Reason
		}
		s += ")"
		return s
	case api.CoverageUnobserved:
		s := fmt.Sprintf("Chronos had not started observing %s at %s", resource, tf.short(at))
		if c.Until != nil {
			s += " (first observed at " + tf.short(*c.Until) + ")"
		} else {
			s += " (it has never observed them)"
		}
		return s
	case api.CoverageExpired:
		s := fmt.Sprintf("%s at %s is older than the retention horizon", resource, tf.short(at))
		if c.Until != nil {
			s += " (" + tf.short(*c.Until) + ")"
		}
		return s
	}
	return fmt.Sprintf("coverage of %s at %s is %q", resource, tf.short(at), c.State)
}

// historyUnavailable reports whether no data at all exists for the
// instant (as opposed to a gap, which still has a last known state).
func historyUnavailable(c api.Coverage) bool {
	return c.State == api.CoverageUnobserved || c.State == api.CoverageExpired
}
