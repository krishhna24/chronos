package query

import (
	"context"
	"fmt"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// CoverageAt classifies instant t for resource according to ADR 0004 using
// the given view. It does not validate t against the clock; callers that
// accept user input go through the Engine methods, which do.
func (e *Engine) CoverageAt(ctx context.Context, v store.View, resource string, t time.Time) (api.Coverage, error) {
	events, err := v.Coverage(ctx, resource)
	if err != nil {
		return api.Coverage{}, fmt.Errorf("coverage of %s: %w", resource, err)
	}
	return classify(events, v.Horizon(), t), nil
}

// Intervals returns the observation intervals of resource that the view
// retains, oldest first.
func (e *Engine) Intervals(ctx context.Context, v store.View, resource string) ([]api.Interval, error) {
	events, err := v.Coverage(ctx, resource)
	if err != nil {
		return nil, fmt.Errorf("coverage of %s: %w", resource, err)
	}
	return intervals(events), nil
}

// lastAtOrBefore returns the index of the last event, in Seq order, whose At
// is <= t, or -1. The store keeps At non-decreasing within a resource in
// practice, but the contract only orders events by Seq, so a linear scan is
// used instead of a binary search on At. Coverage histories are short (one
// pair of events per relist or restart).
func lastAtOrBefore(events []model.CoverageEvent, t time.Time) int {
	idx := -1
	for i, ev := range events {
		if !ev.At.After(t) {
			idx = i
		}
	}
	return idx
}

func timePtr(t time.Time) *time.Time { return &t }

// classify implements the ADR 0004 classification of instant t:
//
//  1. t < horizon: expired (Until is the horizon).
//  2. no coverage event with At <= t: unobserved (Until is the first event).
//  3. last such event is Start: observed (Since = Start.At, Until = the
//     following End, nil while still observing).
//  4. last such event is End: gap (Since = End.At, Until = the following
//     Start, Reason = End.Reason).
func classify(events []model.CoverageEvent, horizon, t time.Time) api.Coverage {
	if !horizon.IsZero() && t.Before(horizon) {
		return api.Coverage{State: api.CoverageExpired, Until: timePtr(horizon)}
	}
	i := lastAtOrBefore(events, t)
	if i < 0 {
		c := api.Coverage{State: api.CoverageUnobserved}
		if len(events) > 0 {
			c.Until = timePtr(events[0].At)
		}
		return c
	}
	ev := events[i]
	c := api.Coverage{Since: timePtr(ev.At)}
	if i+1 < len(events) {
		c.Until = timePtr(events[i+1].At)
	}
	if ev.Type == model.CoverageStart {
		c.State = api.CoverageObserved
	} else {
		c.State = api.CoverageGap
		c.Reason = ev.Reason
	}
	return c
}

// observedThroughout reports whether every instant of [from, to] is
// observed: from is observed and no End event falls in (from, to]. Checking
// only the endpoints would miss a gap that lies strictly inside the range.
func observedThroughout(events []model.CoverageEvent, horizon, from, to time.Time) bool {
	if classify(events, horizon, from).State != api.CoverageObserved {
		return false
	}
	for _, ev := range events {
		if ev.Type == model.CoverageEnd && ev.At.After(from) && !ev.At.After(to) {
			return false
		}
	}
	return classify(events, horizon, to).State == api.CoverageObserved
}

// intervals pairs each Start with the End that follows it. A leading End
// (its Start was compacted away) does not describe a retained interval and
// is skipped.
func intervals(events []model.CoverageEvent) []api.Interval {
	out := []api.Interval{}
	for i, ev := range events {
		if ev.Type != model.CoverageStart {
			continue
		}
		iv := api.Interval{Start: ev.At}
		if i+1 < len(events) && events[i+1].Type == model.CoverageEnd {
			iv.End = timePtr(events[i+1].At)
			iv.EndReason = events[i+1].Reason
		}
		out = append(out, iv)
	}
	return out
}

// overlapping returns the intervals that intersect [from, to].
func overlapping(ivs []api.Interval, from, to time.Time) []api.Interval {
	out := []api.Interval{}
	for _, iv := range ivs {
		if iv.Start.After(to) {
			continue
		}
		if iv.End != nil && iv.End.Before(from) {
			continue
		}
		out = append(out, iv)
	}
	return out
}
