package query

import (
	"context"
	"fmt"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// LiveLossReason is the gap reason of an instant classified by the live
// coverage overlay (ADR 0007 §2).
const LiveLossReason = "continuity lost; not yet recorded"

// timeline is everything that determines coverage of one resource within
// one call: the view's events and horizon, and the live overlay sampled
// once, so every part of an answer agrees.
type timeline struct {
	events  []model.CoverageEvent
	horizon time.Time
	lostAt  time.Time
	lost    bool
}

func (e *Engine) timeline(ctx context.Context, v store.View, resource string) (timeline, error) {
	events, err := v.Coverage(ctx, resource)
	if err != nil {
		return timeline{}, fmt.Errorf("coverage of %s: %w", resource, err)
	}
	tl := timeline{events: events, horizon: v.Horizon()}
	if e.live != nil {
		tl.lostAt, tl.lost = e.live(resource)
	}
	return tl, nil
}

// at classifies t: the stored classification, downgraded to the overlay
// gap when t >= lostAt and the store says observed. A stored gap that began
// at or before lostAt is kept as is (the store knew first); one that began
// after it is reported as the overlay gap, which already covers it.
// Expired and unobserved are never changed.
func (tl timeline) at(t time.Time) api.Coverage {
	c := classify(tl.events, tl.horizon, t)
	if !tl.lost || t.Before(tl.lostAt) {
		return c
	}
	switch c.State {
	case api.CoverageObserved:
	case api.CoverageGap:
		if c.Since != nil && !c.Since.After(tl.lostAt) {
			return c
		}
	default:
		return c
	}
	return api.Coverage{State: api.CoverageGap, Since: timePtr(tl.lostAt), Reason: LiveLossReason}
}

// observedThroughout: stored rule plus the overlay. With a loss at lostAt,
// any range reaching lostAt is incomplete; checking to suffices since
// coverage at to is then a gap.
func (tl timeline) observedThroughout(from, to time.Time) bool {
	if !observedThroughout(tl.events, tl.horizon, from, to) {
		return false
	}
	return tl.at(from).State == api.CoverageObserved && tl.at(to).State == api.CoverageObserved
}

// intervals are the stored intervals with everything at or after lostAt
// removed: an interval spanning lostAt ends there with LiveLossReason, and
// one starting at or after it is dropped, matching at().
func (tl timeline) intervals() []api.Interval {
	ivs := intervals(tl.events)
	if !tl.lost {
		return ivs
	}
	out := ivs[:0]
	for _, iv := range ivs {
		if !iv.Start.Before(tl.lostAt) {
			continue
		}
		if iv.End == nil || iv.End.After(tl.lostAt) {
			iv.End = timePtr(tl.lostAt)
			iv.EndReason = LiveLossReason
		}
		out = append(out, iv)
	}
	return out
}

// CoverageAt classifies instant t for resource according to ADR 0004 (and
// the live overlay, if configured) using the given view. It does not
// validate t against the clock; callers that accept user input go through
// the Engine methods, which do.
func (e *Engine) CoverageAt(ctx context.Context, v store.View, resource string, t time.Time) (api.Coverage, error) {
	tl, err := e.timeline(ctx, v, resource)
	if err != nil {
		return api.Coverage{}, err
	}
	return tl.at(t), nil
}

// Intervals returns the observation intervals of resource that the view
// retains, oldest first, cut short by the live overlay if configured.
func (e *Engine) Intervals(ctx context.Context, v store.View, resource string) ([]api.Interval, error) {
	tl, err := e.timeline(ctx, v, resource)
	if err != nil {
		return nil, err
	}
	return tl.intervals(), nil
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
