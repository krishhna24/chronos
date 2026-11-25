package query

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
)

var overlayEvents = []model.CoverageEvent{
	{Seq: 1, At: at(10), Type: model.CoverageStart, Reason: "list"},
	{Seq: 2, At: at(40), Type: model.CoverageEnd, Reason: "watch expired"},
	{Seq: 3, At: at(60), Type: model.CoverageStart, Reason: "relist"},
}

func TestOverlayClassify(t *testing.T) {
	tests := []struct {
		name         string
		events       []model.CoverageEvent
		horizon      time.Time
		lostAt       time.Time
		lost         bool
		t            time.Time
		state        api.CoverageState
		since, until *time.Time
		reason       string
	}{
		{name: "not lost: unchanged", lostAt: at(70), t: at(80),
			state: api.CoverageObserved, since: timePtr(at(60))},
		{name: "t < lostAt: unaffected", lost: true, lostAt: at(70), t: at(70).Add(-1),
			state: api.CoverageObserved, since: timePtr(at(60))},
		{name: "t == lostAt: gap", lost: true, lostAt: at(70), t: at(70),
			state: api.CoverageGap, since: timePtr(at(70)), reason: LiveLossReason},
		{name: "t > lostAt: open gap", lost: true, lostAt: at(70), t: at(90),
			state: api.CoverageGap, since: timePtr(at(70)), reason: LiveLossReason},
		{name: "stored gap before lostAt unaffected", lost: true, lostAt: at(70), t: at(50),
			state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{name: "stored End earlier than lostAt wins inside its gap", lost: true, lostAt: at(50), t: at(55),
			state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{name: "stored End earlier: after the stored gap the overlay applies", lost: true, lostAt: at(50), t: at(65),
			state: api.CoverageGap, since: timePtr(at(50)), reason: LiveLossReason},
		{name: "stored End at lostAt wins", lost: true, lostAt: at(40), t: at(45),
			state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{name: "stored End later than lostAt: overlay covers it", lost: true, lostAt: at(30), t: at(45),
			state: api.CoverageGap, since: timePtr(at(30)), reason: LiveLossReason},
		{name: "stored End later: observed before it is downgraded", lost: true, lostAt: at(30), t: at(35),
			state: api.CoverageGap, since: timePtr(at(30)), reason: LiveLossReason},
		{name: "unobserved never changes", lost: true, lostAt: at(5), t: at(7),
			state: api.CoverageUnobserved, until: timePtr(at(10))},
		{name: "expired never changes", horizon: at(45), lost: true, lostAt: at(0), t: at(44),
			state: api.CoverageExpired, until: timePtr(at(45))},
		{name: "no events at all", events: []model.CoverageEvent{}, lost: true, lostAt: at(0), t: at(50),
			state: api.CoverageUnobserved},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := tc.events
			if ev == nil {
				ev = overlayEvents
			}
			got := timeline{events: ev, horizon: tc.horizon, lostAt: tc.lostAt, lost: tc.lost}.at(tc.t)
			if got.State != tc.state {
				t.Fatalf("state = %q, want %q", got.State, tc.state)
			}
			checkPtr(t, "since", got.Since, tc.since)
			checkPtr(t, "until", got.Until, tc.until)
			if got.Reason != tc.reason {
				t.Errorf("reason = %q, want %q", got.Reason, tc.reason)
			}
		})
	}
}

func TestOverlayIntervals(t *testing.T) {
	tests := []struct {
		name   string
		lostAt time.Time
		want   []api.Interval
	}{
		{"loss inside open interval", at(70), []api.Interval{
			{Start: at(10), End: timePtr(at(40)), EndReason: "watch expired"},
			{Start: at(60), End: timePtr(at(70)), EndReason: LiveLossReason},
		}},
		{"loss at open Start drops it", at(60), []api.Interval{
			{Start: at(10), End: timePtr(at(40)), EndReason: "watch expired"},
		}},
		{"loss inside closed interval cuts it, later ones dropped", at(30), []api.Interval{
			{Start: at(10), End: timePtr(at(30)), EndReason: LiveLossReason},
		}},
		{"loss in stored gap", at(50), []api.Interval{
			{Start: at(10), End: timePtr(at(40)), EndReason: "watch expired"},
		}},
		{"loss before everything", at(5), []api.Interval{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := timeline{events: overlayEvents, lostAt: tc.lostAt, lost: true}.intervals()
			if got == nil || len(got) != len(tc.want) {
				t.Fatalf("intervals = %+v, want %+v", got, tc.want)
			}
			for i := range got {
				g, w := got[i], tc.want[i]
				if !g.Start.Equal(w.Start) || g.EndReason != w.EndReason {
					t.Errorf("interval %d = %+v, want %+v", i, g, w)
				}
				checkPtr(t, "end", g.End, w.End)
			}
		})
	}
}

func TestOverlayObservedThroughout(t *testing.T) {
	tests := []struct {
		name     string
		lostAt   time.Time
		from, to time.Time
		want     bool
	}{
		{"loss strictly inside", at(70), at(65), at(80), false},
		{"loss at to", at(70), at(65), at(70), false},
		{"loss at from", at(70), at(70), at(80), false},
		{"loss after to", at(70), at(65), at(70).Add(-1), true},
		{"loss before from", at(70), at(75), at(80), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tl := timeline{events: overlayEvents, lostAt: tc.lostAt, lost: true}
			if got := tl.observedThroughout(tc.from, tc.to); got != tc.want {
				t.Errorf("observedThroughout = %v, want %v", got, tc.want)
			}
		})
	}
}

func liveFor(resource string, lostAt time.Time) LiveCoverage {
	return func(r string) (time.Time, bool) {
		if r == resource {
			return lostAt, true
		}
		return time.Time{}, false
	}
}

func TestOverlayEngine(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	e := New(f.store, f.clock, WithLiveCoverage(liveFor(pods, at(80))))

	st, err := e.State(ctx, podKey("ns", "a"), at(79))
	if err != nil || st.Existence != api.ExistencePresent {
		t.Fatalf("before lostAt: %+v %v", st, err)
	}
	st, err = e.State(ctx, podKey("ns", "a"), at(85))
	if err != nil {
		t.Fatal(err)
	}
	if st.Existence != api.ExistenceUnknown || st.Coverage.State != api.CoverageGap ||
		st.Coverage.Reason != LiveLossReason || !eqTime(st.Coverage.Since, at(80)) || st.Coverage.Until != nil {
		t.Errorf("after lostAt: %+v", st)
	}
	if st.Version == nil || st.Version.Seq != f.addA2.Seq || st.Object == nil {
		t.Errorf("gap must still return the last known object: %+v", st)
	}

	l, err := e.List(ctx, ListRequest{Resource: pods, At: at(85), IncludeDeleted: true})
	if err != nil {
		t.Fatal(err)
	}
	if l.Coverage.State != api.CoverageGap || len(l.Items) == 0 {
		t.Fatalf("list: %+v", l)
	}
	for _, it := range l.Items {
		if it.Existence != api.ExistenceUnknown {
			t.Errorf("list item %s: existence %s", it.Key.Name, it.Existence)
		}
	}

	for _, tc := range []struct {
		from, to time.Time
		want     bool
	}{{at(65), at(85), false}, {at(65), at(79), true}, {at(81), at(90), false}} {
		d, err := e.Diff(ctx, podKey("ns", "a"), tc.from, tc.to)
		if err != nil {
			t.Fatal(err)
		}
		if d.Complete != tc.want {
			t.Errorf("diff %s..%s complete = %v, want %v", tc.from, tc.to, d.Complete, tc.want)
		}
	}

	h, err := e.History(ctx, podKey("ns", "a"), time.Time{}, time.Time{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	last := h.Coverage[len(h.Coverage)-1]
	if !eqTime(last.End, at(80)) || last.EndReason != LiveLossReason {
		t.Errorf("history coverage = %+v", h.Coverage)
	}

	res, err := e.Resources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res.Resources {
		if r.ID != pods {
			continue
		}
		last := r.Coverage[len(r.Coverage)-1]
		if !eqTime(last.End, at(80)) || last.EndReason != LiveLossReason {
			t.Errorf("resources coverage = %+v", r.Coverage)
		}
	}

	// Exactly at lostAt, a record is no longer vouched for.
	e71 := New(f.store, f.clock, WithLiveCoverage(liveFor(pods, f.addA2.ObservedAt)))
	o, err := e71.ObjectAtSeq(ctx, podKey("ns", "a"), f.addA2.Seq)
	if err != nil || o.Coverage.State != api.CoverageGap || o.Existence != api.ExistenceUnknown {
		t.Errorf("object at lostAt: %+v %v", o, err)
	}

	// Other resources are untouched.
	other := New(f.store, f.clock, WithLiveCoverage(liveFor("v1/services", at(0))))
	if st, _ := other.State(ctx, podKey("ns", "a"), at(85)); st.Existence != api.ExistencePresent {
		t.Errorf("overlay for another resource changed pods: %+v", st)
	}
}

// rank orders coverage by how much it claims: only observed claims
// knowledge, so an overlay may never raise an answer from 0 to 1.
func observedRank(s api.CoverageState) int {
	if s == api.CoverageObserved {
		return 1
	}
	return 0
}

func randomEvents(r *rand.Rand) []model.CoverageEvent {
	var ev []model.CoverageEvent
	tm := r.IntN(20)
	typ := model.CoverageStart
	for i := range r.IntN(7) {
		ev = append(ev, model.CoverageEvent{Seq: uint64(i + 1), At: at(float64(tm)), Type: typ, Reason: "stored"})
		if typ == model.CoverageStart {
			typ = model.CoverageEnd
			tm += r.IntN(20) // End may coincide with its Start
		} else {
			typ = model.CoverageStart
			tm += 1 + r.IntN(20)
		}
	}
	return ev
}

func TestOverlayNeverMoreObservedProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 3000 {
		ev := randomEvents(r)
		var horizon time.Time
		if r.IntN(4) == 0 {
			horizon = at(float64(r.IntN(60)))
		}
		plain := timeline{events: ev, horizon: horizon}
		over := plain
		over.lostAt, over.lost = at(float64(r.IntN(130))), true
		for s := 0; s <= 140; s++ {
			tm := at(float64(s) / 1.0)
			p, o := plain.at(tm), over.at(tm)
			if observedRank(o.State) > observedRank(p.State) {
				t.Fatalf("events %v lostAt %s t %s: overlay %s > plain %s", ev, over.lostAt, tm, o.State, p.State)
			}
			if tm.Before(over.lostAt) && (o.State != p.State || o.Reason != p.Reason) {
				t.Fatalf("t < lostAt changed: %+v vs %+v", o, p)
			}
			if p.State != api.CoverageObserved && o.State != p.State {
				t.Fatalf("non-observed state changed: %s -> %s", p.State, o.State)
			}
			to := tm.Add(time.Duration(r.IntN(30)) * time.Second)
			if to.After(tm) && over.observedThroughout(tm, to) && !plain.observedThroughout(tm, to) {
				t.Fatalf("overlay made [%s, %s] complete", tm, to)
			}
		}
		// Every overlay interval lies inside a plain one.
		pi := plain.intervals()
		for _, oi := range over.intervals() {
			ok := false
			for _, p := range pi {
				if !oi.Start.Before(p.Start) && (p.End == nil || (oi.End != nil && !oi.End.After(*p.End))) {
					ok = true
				}
			}
			if !ok {
				t.Fatalf("overlay interval %+v not within %+v", oi, pi)
			}
		}
	}
}

// The same property through the engine: State and List never report
// present/absent, and Diff never reports complete, where the engine
// without the overlay would not.
func TestOverlayEngineProperty(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for lost := 0; lost <= 100; lost += 5 {
		over := New(f.store, f.clock, WithLiveCoverage(liveFor(pods, at(float64(lost)))))
		for s := 0; s <= 100; s += 3 {
			tm := at(float64(s))
			for _, k := range []model.ObjectKey{podKey("ns", "a"), podKey("ns", "b"), podKey("other", "c")} {
				p, err1 := f.engine.State(ctx, k, tm)
				o, err2 := over.State(ctx, k, tm)
				if err1 != nil || err2 != nil {
					t.Fatal(err1, err2)
				}
				if o.Existence != api.ExistenceUnknown && o.Existence != p.Existence {
					t.Fatalf("lost %d t %d %s: overlay %s, plain %s", lost, s, k, o.Existence, p.Existence)
				}
				if observedRank(o.Coverage.State) > observedRank(p.Coverage.State) {
					t.Fatalf("lost %d t %d: state more observed", lost, s)
				}
			}
			pl, _ := f.engine.List(ctx, ListRequest{Resource: pods, At: tm, IncludeDeleted: true})
			ol, _ := over.List(ctx, ListRequest{Resource: pods, At: tm, IncludeDeleted: true})
			for _, it := range ol.Items {
				if it.Existence == api.ExistenceUnknown {
					continue
				}
				found := false
				for _, pit := range pl.Items {
					if pit.Key == it.Key && pit.Existence == it.Existence {
						found = true
					}
				}
				if !found {
					t.Fatalf("lost %d t %d: list item %s %s not in plain answer", lost, s, it.Key.Name, it.Existence)
				}
			}
			if s+7 <= 100 {
				pd, _ := f.engine.Diff(ctx, podKey("ns", "a"), tm, at(float64(s+7)))
				od, _ := over.Diff(ctx, podKey("ns", "a"), tm, at(float64(s+7)))
				if od.Complete && !pd.Complete {
					t.Fatalf("lost %d diff from %d: overlay complete", lost, s)
				}
			}
		}
	}
}
