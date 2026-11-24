package query

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

func image(t *testing.T, obj json.RawMessage) string {
	t.Helper()
	var p struct {
		Spec struct {
			Containers []struct {
				Image string `json:"image"`
			} `json:"containers"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(obj, &p); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	if len(p.Spec.Containers) == 0 {
		return ""
	}
	return p.Spec.Containers[0].Image
}

func eqTime(a *time.Time, b time.Time) bool { return a != nil && a.Equal(b) }

func TestClassify(t *testing.T) {
	ev := []model.CoverageEvent{
		{Seq: 1, At: at(10), Type: model.CoverageStart, Reason: "list"},
		{Seq: 2, At: at(40), Type: model.CoverageEnd, Reason: "watch expired"},
		{Seq: 3, At: at(60), Type: model.CoverageStart, Reason: "relist"},
	}
	tests := []struct {
		name         string
		events       []model.CoverageEvent
		horizon      time.Time
		t            time.Time
		state        api.CoverageState
		since, until *time.Time
		reason       string
	}{
		{name: "no events", t: at(5), state: api.CoverageUnobserved},
		{name: "before first Start", events: ev, t: at(5), state: api.CoverageUnobserved, until: timePtr(at(10))},
		{name: "1ns before Start", events: ev, t: at(10).Add(-1), state: api.CoverageUnobserved, until: timePtr(at(10))},
		{name: "exactly Start.At", events: ev, t: at(10), state: api.CoverageObserved, since: timePtr(at(10)), until: timePtr(at(40))},
		{name: "inside observed", events: ev, t: at(20), state: api.CoverageObserved, since: timePtr(at(10)), until: timePtr(at(40))},
		{name: "1ns before End", events: ev, t: at(40).Add(-1), state: api.CoverageObserved, since: timePtr(at(10)), until: timePtr(at(40))},
		{name: "exactly End.At", events: ev, t: at(40), state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{name: "inside gap", events: ev, t: at(50), state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{name: "after restart Start", events: ev, t: at(60), state: api.CoverageObserved, since: timePtr(at(60))},
		{name: "open interval", events: ev, t: at(1000), state: api.CoverageObserved, since: timePtr(at(60))},
		{name: "before horizon", events: ev, horizon: at(45), t: at(44), state: api.CoverageExpired, until: timePtr(at(45))},
		{name: "at horizon", events: ev[1:], horizon: at(45), t: at(45), state: api.CoverageGap, since: timePtr(at(40)), until: timePtr(at(60)), reason: "watch expired"},
		{
			name: "End and Start at same instant: later Seq wins",
			events: []model.CoverageEvent{
				{Seq: 1, At: at(10), Type: model.CoverageStart},
				{Seq: 2, At: at(20), Type: model.CoverageEnd},
				{Seq: 3, At: at(20), Type: model.CoverageStart},
			},
			t: at(20), state: api.CoverageObserved, since: timePtr(at(20)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classify(tc.events, tc.horizon, tc.t)
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

func checkPtr(t *testing.T, name string, got, want *time.Time) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil || want == nil:
		t.Errorf("%s = %v, want %v", name, got, want)
	case !got.Equal(*want):
		t.Errorf("%s = %s, want %s", name, got, want)
	}
}

func TestObservedThroughout(t *testing.T) {
	ev := []model.CoverageEvent{
		{Seq: 1, At: at(10), Type: model.CoverageStart},
		{Seq: 2, At: at(40), Type: model.CoverageEnd},
		{Seq: 3, At: at(60), Type: model.CoverageStart},
	}
	tests := []struct {
		name     string
		from, to time.Time
		want     bool
	}{
		{"inside one interval", at(15), at(35), true},
		{"gap strictly inside, endpoints observed", at(20), at(70), false},
		{"to exactly at End", at(20), at(40), false},
		{"from in gap", at(50), at(70), false},
		{"from unobserved", at(5), at(20), false},
		{"from exactly at restart Start", at(60), at(90), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := observedThroughout(ev, time.Time{}, tc.from, tc.to); got != tc.want {
				t.Errorf("observedThroughout = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestState(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	type want struct {
		coverage  api.CoverageState
		existence api.Existence
		seq       uint64 // 0: no version
		vtype     string
		origin    string
		image     string // "": no object
	}
	tests := []struct {
		name string
		key  model.ObjectKey
		at   time.Time
		want want
	}{
		{"before first Start", podKey("ns", "a"), at(5),
			want{api.CoverageUnobserved, api.ExistenceUnknown, 0, "", "", ""}},
		// Baseline records are written just before Start in the same batch,
		// so the instant of the record itself is still unobserved.
		{"at baseline record, before Start", podKey("ns", "a"), f.baseA.ObservedAt,
			want{api.CoverageUnobserved, api.ExistenceUnknown, 0, "", "", ""}},
		{"exactly at Start.At", podKey("ns", "a"), f.start1,
			want{api.CoverageObserved, api.ExistencePresent, f.baseA.Seq, "Added", "baseline", "img:v1"}},
		{"inside observed", podKey("ns", "a"), at(25),
			want{api.CoverageObserved, api.ExistencePresent, f.modA.Seq, "Modified", "watch", "img:v2"}},
		{"exactly at watch record", podKey("ns", "a"), f.modA.ObservedAt,
			want{api.CoverageObserved, api.ExistencePresent, f.modA.Seq, "Modified", "watch", "img:v2"}},
		{"1ns before watch record", podKey("ns", "a"), f.modA.ObservedAt.Add(-1),
			want{api.CoverageObserved, api.ExistencePresent, f.baseA.Seq, "Added", "baseline", "img:v1"}},
		{"deleted while observed", podKey("ns", "b"), at(35),
			want{api.CoverageObserved, api.ExistenceAbsent, f.delB.Seq, "Deleted", "watch", ""}},
		{"never existed while observed", podKey("ns", "zzz"), at(35),
			want{api.CoverageObserved, api.ExistenceAbsent, 0, "", "", ""}},
		{"exactly at End.At: gap with last known", podKey("ns", "a"), f.end1,
			want{api.CoverageGap, api.ExistenceUnknown, f.modA.Seq, "Modified", "watch", "img:v2"}},
		{"inside gap: last known object", podKey("ns", "a"), at(50),
			want{api.CoverageGap, api.ExistenceUnknown, f.modA.Seq, "Modified", "watch", "img:v2"}},
		{"inside gap: last known is a delete", podKey("ns", "b"), at(50),
			want{api.CoverageGap, api.ExistenceUnknown, f.delB.Seq, "Deleted", "watch", ""}},
		{"inside gap: nothing known", podKey("other", "c"), at(50),
			want{api.CoverageGap, api.ExistenceUnknown, 0, "", "", ""}},
		{"at resync record, before restart Start", podKey("other", "c"), f.resyncC.ObservedAt,
			want{api.CoverageGap, api.ExistenceUnknown, f.resyncC.Seq, "Added", "resync", "img:v1"}},
		{"exactly at restart Start", podKey("ns", "a"), f.start2,
			want{api.CoverageObserved, api.ExistencePresent, f.resyncA.Seq, "Modified", "resync", "img:v3"}},
		{"resync-origin Added after restart", podKey("other", "c"), at(65),
			want{api.CoverageObserved, api.ExistencePresent, f.resyncC.Seq, "Added", "resync", "img:v1"}},
		{"deleted before recreation", podKey("ns", "a"), at(70.5),
			want{api.CoverageObserved, api.ExistenceAbsent, f.delA.Seq, "Deleted", "watch", ""}},
		{"recreated", podKey("ns", "a"), at(80),
			want{api.CoverageObserved, api.ExistencePresent, f.addA2.Seq, "Added", "watch", "img:v4"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.ObjectState
			var err error
			f.oneView(t, func() { got, err = f.engine.State(ctx, tc.key, tc.at) })
			if err != nil {
				t.Fatalf("State: %v", err)
			}
			if !got.At.Equal(tc.at) {
				t.Errorf("At = %s, want %s", got.At, tc.at)
			}
			if got.Coverage.State != tc.want.coverage {
				t.Errorf("coverage = %q, want %q", got.Coverage.State, tc.want.coverage)
			}
			if got.Existence != tc.want.existence {
				t.Errorf("existence = %q, want %q", got.Existence, tc.want.existence)
			}
			switch {
			case tc.want.seq == 0 && got.Version != nil:
				t.Errorf("version = %+v, want none", got.Version)
			case tc.want.seq != 0 && got.Version == nil:
				t.Errorf("version = nil, want seq %d", tc.want.seq)
			case tc.want.seq != 0:
				if got.Version.Seq != tc.want.seq || got.Version.Type != tc.want.vtype || got.Version.Origin != tc.want.origin {
					t.Errorf("version = %+v, want seq=%d type=%s origin=%s", got.Version, tc.want.seq, tc.want.vtype, tc.want.origin)
				}
			}
			if tc.want.image == "" {
				if got.Object != nil {
					t.Errorf("object = %s, want none", got.Object)
				}
			} else if img := image(t, got.Object); img != tc.want.image {
				t.Errorf("image = %q, want %q", img, tc.want.image)
			}
		})
	}
}

func TestStateGapCoverageDetails(t *testing.T) {
	f := newFixture(t)
	got, err := f.engine.State(context.Background(), podKey("ns", "a"), at(50))
	if err != nil {
		t.Fatal(err)
	}
	c := got.Coverage
	if !eqTime(c.Since, f.end1) || !eqTime(c.Until, f.start2) || c.Reason != "watch expired" {
		t.Errorf("gap coverage = since %v until %v reason %q", c.Since, c.Until, c.Reason)
	}
}

func TestStateAfterCompaction(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	if err := f.mem.Compact(ctx, at(45)); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		key       model.ObjectKey
		at        time.Time
		coverage  api.CoverageState
		existence api.Existence
		hasObject bool
	}{
		{"before horizon: expired", podKey("ns", "a"), at(25), api.CoverageExpired, api.ExistenceUnknown, false},
		{"before horizon, even if once deleted", podKey("ns", "b"), at(35), api.CoverageExpired, api.ExistenceUnknown, false},
		{"after horizon in gap: retained last known", podKey("ns", "a"), at(50), api.CoverageGap, api.ExistenceUnknown, true},
		{"after horizon in gap: deleted key dropped", podKey("ns", "b"), at(50), api.CoverageGap, api.ExistenceUnknown, false},
		{"after horizon observed", podKey("ns", "a"), at(65), api.CoverageObserved, api.ExistencePresent, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := f.engine.State(ctx, tc.key, tc.at)
			if err != nil {
				t.Fatal(err)
			}
			if got.Coverage.State != tc.coverage || got.Existence != tc.existence || (got.Object != nil) != tc.hasObject {
				t.Errorf("got coverage=%s existence=%s object=%v", got.Coverage.State, got.Existence, got.Object != nil)
			}
			if tc.coverage == api.CoverageExpired && got.Version != nil {
				t.Errorf("expired answer carries a version: %+v", got.Version)
			}
		})
	}
	res, err := f.engine.Resources(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Horizon == nil || !res.Horizon.Equal(at(45)) {
		t.Errorf("horizon = %v, want %s", res.Horizon, at(45))
	}
}

func TestStateValidation(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name string
		key  model.ObjectKey
		at   time.Time
	}{
		{"unknown resource", model.ObjectKey{Resource: "v1/widgets", Namespace: "ns", Name: "a"}, at(50)},
		{"alias instead of canonical ID", model.ObjectKey{Resource: "pods", Namespace: "ns", Name: "a"}, at(50)},
		{"missing resource", model.ObjectKey{Namespace: "ns", Name: "a"}, at(50)},
		{"namespaced without namespace", model.ObjectKey{Resource: pods, Name: "a"}, at(50)},
		{"missing name", model.ObjectKey{Resource: pods, Namespace: "ns"}, at(50)},
		{"slash in name", model.ObjectKey{Resource: pods, Namespace: "ns", Name: "a/b"}, at(50)},
		{"future beyond skew", podKey("ns", "a"), at(100).Add(DefaultMaxClockSkew + time.Nanosecond)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.engine.State(ctx, tc.key, tc.at)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
	if o, c := f.store.opened.Load(), f.store.closed.Load(); o != c {
		t.Errorf("views opened %d, closed %d", o, c)
	}
}

func TestStateClampsSmallSkewAndDefaultsToNow(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	now := f.clock.Now()
	for _, in := range []time.Time{{}, now.Add(DefaultMaxClockSkew), now.Add(time.Second)} {
		got, err := f.engine.State(ctx, podKey("ns", "a"), in)
		if err != nil {
			t.Fatalf("State(%s): %v", in, err)
		}
		if !got.At.Equal(now) {
			t.Errorf("At = %s, want clock now %s", got.At, now)
		}
	}
}

func TestStateCorruptObject(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	bad := recEntry(podKey("ns", "bad"), "x", model.ChangeAdded, model.OriginWatch, at(80), []byte("{not json"))
	if err := f.mem.Write(ctx, []store.Entry{bad}); err != nil {
		t.Fatal(err)
	}
	_, err := f.engine.State(ctx, podKey("ns", "bad"), at(90))
	if !errors.Is(err, store.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if errors.Is(err, ErrInvalidArgument) {
		t.Fatal("corrupt data must not be reported as a client error")
	}
}

func TestIntervalsAndResources(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var res api.ResourcesResponse
	var err error
	f.oneView(t, func() { res, err = f.engine.Resources(ctx) })
	if err != nil {
		t.Fatal(err)
	}
	if res.Horizon != nil {
		t.Errorf("horizon = %v, want nil before compaction", res.Horizon)
	}
	if !res.Now.Equal(at(100)) {
		t.Errorf("now = %s", res.Now)
	}
	var podsRes *api.Resource
	for i := range res.Resources {
		r := &res.Resources[i]
		if r.Coverage == nil {
			t.Errorf("%s: coverage is nil, want empty slice", r.ID)
		}
		if r.ID == pods {
			podsRes = r
		}
	}
	if podsRes == nil {
		t.Fatal("v1/pods missing from resources")
	}
	if !podsRes.Namespaced || podsRes.Kind != "Pod" || len(podsRes.ShortNames) == 0 {
		t.Errorf("pods resource = %+v", podsRes)
	}
	ivs := podsRes.Coverage
	if len(ivs) != 2 {
		t.Fatalf("intervals = %+v, want 2", ivs)
	}
	if !ivs[0].Start.Equal(f.start1) || !eqTime(ivs[0].End, f.end1) || ivs[0].EndReason != "watch expired" {
		t.Errorf("interval 0 = %+v", ivs[0])
	}
	if !ivs[1].Start.Equal(f.start2) || ivs[1].End != nil {
		t.Errorf("interval 1 = %+v", ivs[1])
	}
}

func TestIntervalsSkipLeadingEnd(t *testing.T) {
	ivs := intervals([]model.CoverageEvent{
		{Seq: 2, At: at(40), Type: model.CoverageEnd, Reason: "x"},
		{Seq: 3, At: at(60), Type: model.CoverageStart},
	})
	if len(ivs) != 1 || !ivs[0].Start.Equal(at(60)) {
		t.Errorf("intervals = %+v", ivs)
	}
}

func TestList(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	type item struct {
		ns, name  string
		existence api.Existence
		hasObject bool
	}
	tests := []struct {
		name           string
		namespace      string
		at             time.Time
		selector       string
		includeDeleted bool
		coverage       api.CoverageState
		want           []item
	}{
		{name: "before first Start", at: at(5), coverage: api.CoverageUnobserved},
		{name: "observed", at: at(25), coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}, {"ns", "b", api.ExistencePresent, true}}},
		{name: "deleted hidden", at: at(35), coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}}},
		{name: "includeDeleted while observed", at: at(35), includeDeleted: true, coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}, {"ns", "b", api.ExistenceAbsent, false}}},
		{name: "gap: last known, unknown", at: at(50), coverage: api.CoverageGap,
			want: []item{{"ns", "a", api.ExistenceUnknown, true}}},
		{name: "gap with includeDeleted: never absent", at: at(50), includeDeleted: true, coverage: api.CoverageGap,
			want: []item{{"ns", "a", api.ExistenceUnknown, true}, {"ns", "b", api.ExistenceUnknown, false}}},
		{name: "sorted across namespaces", at: at(65), coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}, {"other", "c", api.ExistencePresent, true}}},
		{name: "namespace filter", namespace: "other", at: at(65), coverage: api.CoverageObserved,
			want: []item{{"other", "c", api.ExistencePresent, true}}},
		{name: "equality selector", at: at(25), selector: "app=web", coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}}},
		{name: "set selector", at: at(25), selector: "app in (web,db)", coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}, {"ns", "b", api.ExistencePresent, true}}},
		{name: "existence selector", at: at(25), selector: "tier", coverage: api.CoverageObserved,
			want: []item{{"ns", "a", api.ExistencePresent, true}}},
		{name: "negated selector", at: at(25), selector: "app!=web", coverage: api.CoverageObserved,
			want: []item{{"ns", "b", api.ExistencePresent, true}}},
		{name: "selector on deleted uses last labels", at: at(35), selector: "app=db", includeDeleted: true, coverage: api.CoverageObserved,
			want: []item{{"ns", "b", api.ExistenceAbsent, false}}},
		{name: "selector matches nothing", at: at(25), selector: "app=none", coverage: api.CoverageObserved},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.ListResponse
			var err error
			f.oneView(t, func() {
				got, err = f.engine.List(ctx, ListRequest{Resource: pods, Namespace: tc.namespace, At: tc.at, LabelSelector: tc.selector, IncludeDeleted: tc.includeDeleted})
			})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if got.Coverage.State != tc.coverage {
				t.Errorf("coverage = %q, want %q", got.Coverage.State, tc.coverage)
			}
			if got.Items == nil {
				t.Fatal("items is nil, want empty slice")
			}
			if len(got.Items) != len(tc.want) {
				t.Fatalf("items = %d, want %d: %+v", len(got.Items), len(tc.want), got.Items)
			}
			for i, w := range tc.want {
				g := got.Items[i]
				if g.Key.Namespace != w.ns || g.Key.Name != w.name || g.Existence != w.existence || (g.Object != nil) != w.hasObject {
					t.Errorf("item %d = %s/%s %s object=%v, want %+v", i, g.Key.Namespace, g.Key.Name, g.Existence, g.Object != nil, w)
				}
				if g.Coverage.State != tc.coverage {
					t.Errorf("item %d coverage = %q", i, g.Coverage.State)
				}
			}
		})
	}
}

func TestListValidation(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"unknown resource": func() error { _, err := f.engine.List(ctx, ListRequest{Resource: "v1/nope", At: at(50)}); return err },
		"bad selector": func() error {
			_, err := f.engine.List(ctx, ListRequest{Resource: pods, At: at(50), LabelSelector: "app in ("})
			return err
		},
		"future": func() error { _, err := f.engine.List(ctx, ListRequest{Resource: pods, At: at(200)}); return err },
		"bad namespace": func() error {
			_, err := f.engine.List(ctx, ListRequest{Resource: pods, Namespace: "a/b", At: at(50)})
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestDiff(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name      string
		key       model.ObjectKey
		from, to  time.Time
		complete  bool
		recreated bool
		contains  []string
		empty     bool
	}{
		{name: "modified inside one interval", key: podKey("ns", "a"), from: at(15), to: at(25), complete: true,
			contains: []string{"-  - image: img:v1", "+  - image: img:v2"}},
		{name: "no change", key: podKey("ns", "a"), from: at(21), to: at(25), complete: true, empty: true},
		{name: "gap strictly inside the range", key: podKey("ns", "a"), from: at(25), to: at(65), complete: false,
			contains: []string{"-  - image: img:v2", "+  - image: img:v3"}},
		{name: "recreated", key: podKey("ns", "a"), from: at(65), to: at(80), complete: true, recreated: true,
			contains: []string{"-  uid: a1", "+  uid: a2"}},
		{name: "from unobserved renders empty", key: podKey("ns", "a"), from: at(5), to: at(15), complete: false,
			contains: []string{"+kind: Pod"}},
		{name: "deleted renders empty", key: podKey("ns", "b"), from: at(25), to: at(35), complete: true,
			contains: []string{"-kind: Pod"}},
		{name: "unknown in gap renders empty", key: podKey("ns", "a"), from: at(50), to: at(65), complete: false,
			contains: []string{"+kind: Pod"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.DiffResponse
			var err error
			f.oneView(t, func() { got, err = f.engine.Diff(ctx, tc.key, tc.from, tc.to) })
			if err != nil {
				t.Fatalf("Diff: %v", err)
			}
			if got.Complete != tc.complete {
				t.Errorf("complete = %v, want %v", got.Complete, tc.complete)
			}
			if got.Recreated != tc.recreated {
				t.Errorf("recreated = %v, want %v", got.Recreated, tc.recreated)
			}
			if tc.empty {
				if got.Diff != "" {
					t.Errorf("diff = %q, want empty", got.Diff)
				}
				return
			}
			wantHeader := "--- " + tc.from.Format(time.RFC3339Nano) + "\n+++ " + tc.to.Format(time.RFC3339Nano) + "\n"
			if !strings.HasPrefix(got.Diff, wantHeader) {
				t.Errorf("diff header:\n%s\nwant prefix:\n%s", got.Diff, wantHeader)
			}
			for _, s := range tc.contains {
				if !strings.Contains(got.Diff, s+"\n") {
					t.Errorf("diff does not contain %q:\n%s", s, got.Diff)
				}
			}
		})
	}
}

func TestDiffSortedYAML(t *testing.T) {
	st := api.ObjectState{
		Existence: api.ExistencePresent,
		Object:    json.RawMessage(`{"z":1,"a":{"y":true,"b":"x"},"m":[{"k":2,"c":3}]}`),
	}
	got, err := renderYAML(st)
	if err != nil {
		t.Fatal(err)
	}
	want := "a:\n  b: x\n  \"y\": true\nm:\n- c: 3\n  k: 2\nz: 1\n"
	if got != want {
		t.Errorf("yaml =\n%s\nwant\n%s", got, want)
	}
}

func TestDiffValidation(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name     string
		key      model.ObjectKey
		from, to time.Time
	}{
		{"from == to", podKey("ns", "a"), at(20), at(20)},
		{"from > to", podKey("ns", "a"), at(30), at(20)},
		{"missing from", podKey("ns", "a"), time.Time{}, at(20)},
		{"missing to", podKey("ns", "a"), at(20), time.Time{}},
		{"to in future", podKey("ns", "a"), at(20), at(200)},
		{"missing namespace", model.ObjectKey{Resource: pods, Name: "a"}, at(10), at(20)},
		{"unknown resource", model.ObjectKey{Resource: "x/y", Namespace: "ns", Name: "a"}, at(10), at(20)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := f.engine.Diff(ctx, tc.key, tc.from, tc.to); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestHistory(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name      string
		key       model.ObjectKey
		from, to  time.Time
		limit     int
		types     []string
		truncated bool
		intervals int
	}{
		{name: "full default range", key: podKey("ns", "a"),
			types: []string{"Added", "Modified", "Modified", "Deleted", "Added"}, intervals: 2},
		{name: "limit truncates", key: podKey("ns", "a"), limit: 2,
			types: []string{"Added", "Modified"}, truncated: true, intervals: 2},
		{name: "limit equal to count is not truncated", key: podKey("ns", "a"), limit: 5,
			types: []string{"Added", "Modified", "Modified", "Deleted", "Added"}, intervals: 2},
		{name: "window inside first interval", key: podKey("ns", "a"), from: at(15), to: at(35),
			types: []string{"Modified"}, intervals: 1},
		{name: "window inside gap", key: podKey("ns", "a"), from: at(45), to: at(55), intervals: 0},
		{name: "no records", key: podKey("ns", "zzz"), intervals: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.HistoryResponse
			var err error
			f.oneView(t, func() { got, err = f.engine.History(ctx, tc.key, tc.from, tc.to, tc.limit) })
			if err != nil {
				t.Fatalf("History: %v", err)
			}
			if got.Versions == nil || got.Coverage == nil {
				t.Fatal("nil slices in response")
			}
			var types []string
			for _, v := range got.Versions {
				types = append(types, v.Type)
			}
			if strings.Join(types, ",") != strings.Join(tc.types, ",") {
				t.Errorf("types = %v, want %v", types, tc.types)
			}
			if got.Truncated != tc.truncated {
				t.Errorf("truncated = %v, want %v", got.Truncated, tc.truncated)
			}
			if len(got.Coverage) != tc.intervals {
				t.Errorf("intervals = %d, want %d", len(got.Coverage), tc.intervals)
			}
		})
	}
	for name, call := range map[string]func() error{
		"limit too large": func() error {
			_, err := f.engine.History(ctx, podKey("ns", "a"), time.Time{}, time.Time{}, MaxLimit+1)
			return err
		},
		"negative limit": func() error {
			_, err := f.engine.History(ctx, podKey("ns", "a"), time.Time{}, time.Time{}, -1)
			return err
		},
		"from >= to": func() error { _, err := f.engine.History(ctx, podKey("ns", "a"), at(30), at(20), 0); return err },
		"no namespace": func() error {
			_, err := f.engine.History(ctx, model.ObjectKey{Resource: pods, Name: "a"}, time.Time{}, time.Time{}, 0)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestChanges(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name      string
		from, to  time.Time
		resource  string
		namespace string
		limit     int
		n         int
		truncated bool
	}{
		{name: "all", from: at(0), to: at(100), n: 8},
		{name: "truncated", from: at(0), to: at(100), limit: 3, n: 3, truncated: true},
		{name: "limit equals count", from: at(0), to: at(100), limit: 8, n: 8},
		{name: "resource filter", from: at(0), to: at(100), resource: pods, n: 8},
		{name: "other resource", from: at(0), to: at(100), resource: "apps/v1/deployments", n: 0},
		{name: "namespace filter", from: at(0), to: at(100), resource: pods, namespace: "other", n: 1},
		{name: "namespace without resource", from: at(0), to: at(100), namespace: "other", n: 1},
		{name: "window", from: at(15), to: at(35), n: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.ChangesResponse
			var err error
			f.oneView(t, func() { got, err = f.engine.Changes(ctx, tc.from, tc.to, tc.resource, tc.namespace, tc.limit) })
			if err != nil {
				t.Fatalf("Changes: %v", err)
			}
			if len(got.Changes) != tc.n || got.Truncated != tc.truncated {
				t.Errorf("got %d changes truncated=%v, want %d truncated=%v", len(got.Changes), got.Truncated, tc.n, tc.truncated)
			}
			for i := 1; i < len(got.Changes); i++ {
				if got.Changes[i].Seq <= got.Changes[i-1].Seq {
					t.Errorf("changes not in Seq order at %d", i)
				}
			}
		})
	}
	for name, call := range map[string]func() error{
		"missing from":     func() error { _, err := f.engine.Changes(ctx, time.Time{}, at(10), "", "", 0); return err },
		"missing to":       func() error { _, err := f.engine.Changes(ctx, at(10), time.Time{}, "", "", 0); return err },
		"from >= to":       func() error { _, err := f.engine.Changes(ctx, at(10), at(10), "", "", 0); return err },
		"future":           func() error { _, err := f.engine.Changes(ctx, at(10), at(500), "", "", 0); return err },
		"unknown resource": func() error { _, err := f.engine.Changes(ctx, at(10), at(20), "v1/nope", "", 0); return err },
		"limit too large":  func() error { _, err := f.engine.Changes(ctx, at(10), at(20), "", "", MaxLimit+1); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestObjectAtSeq(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	tests := []struct {
		name      string
		key       model.ObjectKey
		seq       uint64
		coverage  api.CoverageState
		existence api.Existence
		image     string
	}{
		{"watch record", podKey("ns", "a"), f.modA.Seq, api.CoverageObserved, api.ExistencePresent, "img:v2"},
		{"resync record lies at the end of the gap", podKey("ns", "a"), f.resyncA.Seq, api.CoverageGap, api.ExistenceUnknown, "img:v3"},
		{"baseline record precedes its Start", podKey("ns", "a"), f.baseA.Seq, api.CoverageUnobserved, api.ExistenceUnknown, "img:v1"},
		{"delete record", podKey("ns", "a"), f.delA.Seq, api.CoverageObserved, api.ExistenceAbsent, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got api.ObjectState
			var err error
			f.oneView(t, func() { got, err = f.engine.ObjectAtSeq(ctx, tc.key, tc.seq) })
			if err != nil {
				t.Fatal(err)
			}
			if got.Version == nil || got.Version.Seq != tc.seq {
				t.Fatalf("version = %+v, want seq %d", got.Version, tc.seq)
			}
			if !got.At.Equal(got.Version.ObservedAt) {
				t.Errorf("At = %s, want record time %s", got.At, got.Version.ObservedAt)
			}
			if got.Coverage.State != tc.coverage || got.Existence != tc.existence {
				t.Errorf("coverage=%s existence=%s", got.Coverage.State, got.Existence)
			}
			if tc.image == "" {
				if got.Object != nil {
					t.Errorf("object present for delete")
				}
			} else if img := image(t, got.Object); img != tc.image {
				t.Errorf("image = %q, want %q", img, tc.image)
			}
		})
	}
	for name, seq := range map[string]uint64{"zero": 0, "other key's seq": f.resyncC.Seq, "missing": 999} {
		t.Run(name, func(t *testing.T) {
			if _, err := f.engine.ObjectAtSeq(ctx, podKey("ns", "a"), seq); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestClosedStore(t *testing.T) {
	f := newFixture(t)
	_ = f.mem.Close()
	_, err := f.engine.State(context.Background(), podKey("ns", "a"), at(50))
	if !errors.Is(err, store.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}
