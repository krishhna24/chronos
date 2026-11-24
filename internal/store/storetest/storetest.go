// Package storetest is the conformance suite for store.Store
// implementations. Implementations call Run from their own tests.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Factory returns a new, empty store. Cleanup is the caller's job.
type Factory func(t *testing.T) store.Store

var t0 = time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC)

const pods = "v1/pods"

func key(ns, name string) model.ObjectKey {
	return model.ObjectKey{Resource: pods, Namespace: ns, Name: name}
}

func rec(k model.ObjectKey, uid string, typ model.ChangeType, at time.Time, body string) store.Entry {
	return store.Entry{Record: &model.Record{
		ObservedAt: at, Key: k, UID: uid, ResourceVersion: "1",
		Type: typ, Origin: model.OriginWatch, Object: []byte(body),
	}}
}

func cov(res string, typ model.CoverageType, at time.Time) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: at, Resource: res, Type: typ, Reason: "test"}}
}

func mustWrite(t *testing.T, s store.Store, es ...store.Entry) {
	t.Helper()
	if err := s.Write(context.Background(), es); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func view(t *testing.T, s store.Store) store.View {
	t.Helper()
	v, err := s.View(context.Background())
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v
}

func get(t *testing.T, v store.View, k model.ObjectKey, at time.Time) (model.Record, bool) {
	t.Helper()
	r, ok, err := v.Get(context.Background(), k, at)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return r, ok
}

func list(t *testing.T, v store.View, opts store.ListOptions) []model.Record {
	t.Helper()
	var out []model.Record
	if err := v.List(context.Background(), opts, func(r model.Record) error {
		r.Object = append([]byte(nil), r.Object...)
		out = append(out, r)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	return out
}

// Run runs the conformance suite.
func Run(t *testing.T, newStore Factory) {
	ctx := context.Background()

	t.Run("GetAtTime", func(t *testing.T) {
		s := newStore(t)
		a := key("prod", "a")
		mustWrite(t, s, cov(pods, model.CoverageStart, t0), rec(a, "u1", model.ChangeAdded, t0.Add(time.Second), "v1"))
		mustWrite(t, s, rec(a, "u1", model.ChangeModified, t0.Add(2*time.Second), "v2"))
		mustWrite(t, s, rec(a, "u1", model.ChangeDeleted, t0.Add(3*time.Second), "v2"))
		mustWrite(t, s, rec(a, "u2", model.ChangeAdded, t0.Add(4*time.Second), "v3"))
		v := view(t, s)
		cases := []struct {
			at   time.Duration
			ok   bool
			body string
			typ  model.ChangeType
			uid  string
		}{
			{0, false, "", 0, ""},
			{time.Second, true, "v1", model.ChangeAdded, "u1"},
			{1500 * time.Millisecond, true, "v1", model.ChangeAdded, "u1"},
			{2 * time.Second, true, "v2", model.ChangeModified, "u1"},
			{3 * time.Second, true, "v2", model.ChangeDeleted, "u1"},
			{4 * time.Second, true, "v3", model.ChangeAdded, "u2"},
			{time.Hour, true, "v3", model.ChangeAdded, "u2"},
		}
		for _, c := range cases {
			r, ok := get(t, v, a, t0.Add(c.at))
			if ok != c.ok || (ok && (string(r.Object) != c.body || r.Type != c.typ || r.UID != c.uid)) {
				t.Errorf("at +%v: got ok=%v %s %s %q, want ok=%v %s %s %q", c.at, ok, r.Type, r.UID, r.Object, c.ok, c.typ, c.uid, c.body)
			}
		}
	})

	t.Run("SeqAndTimeMonotonic", func(t *testing.T) {
		s := newStore(t)
		a, b := key("ns", "a"), key("ns", "b")
		// Same and decreasing caller times must be clamped forward.
		e1 := rec(a, "u", model.ChangeAdded, t0, "1")
		e2 := rec(b, "u", model.ChangeAdded, t0, "1")
		e3 := rec(a, "u", model.ChangeModified, t0.Add(-time.Hour), "2")
		mustWrite(t, s, e1, e2)
		mustWrite(t, s, e3)
		rs := []*model.Record{e1.Record, e2.Record, e3.Record}
		for i := 1; i < len(rs); i++ {
			if rs[i].Seq <= rs[i-1].Seq {
				t.Errorf("seq not increasing: %d then %d", rs[i-1].Seq, rs[i].Seq)
			}
			if !rs[i].ObservedAt.After(rs[i-1].ObservedAt) {
				t.Errorf("time not increasing: %s then %s", rs[i-1].ObservedAt, rs[i].ObservedAt)
			}
		}
		v := view(t, s)
		r, ok := get(t, v, a, rs[2].ObservedAt)
		if !ok || string(r.Object) != "2" || r.Seq != rs[2].Seq {
			t.Errorf("Get at clamped time: got %+v ok=%v", r, ok)
		}
	})

	t.Run("ListLatestPerKey", func(t *testing.T) {
		s := newStore(t)
		mustWrite(t, s,
			rec(key("a", "x"), "1", model.ChangeAdded, t0, "ax1"),
			rec(key("b", "y"), "2", model.ChangeAdded, t0.Add(time.Second), "by1"),
			rec(key("a", "x"), "1", model.ChangeModified, t0.Add(2*time.Second), "ax2"),
			rec(key("a", "z"), "3", model.ChangeAdded, t0.Add(3*time.Second), "az1"),
			rec(key("a", "z"), "3", model.ChangeDeleted, t0.Add(4*time.Second), "az1"),
			store.Entry{Record: &model.Record{ObservedAt: t0.Add(5 * time.Second), Key: model.ObjectKey{Resource: "v1/services", Namespace: "a", Name: "x"}, UID: "9", Type: model.ChangeAdded, Origin: model.OriginWatch}},
		)
		v := view(t, s)
		got := list(t, v, store.ListOptions{Resource: pods, Namespace: "a", At: t0.Add(3 * time.Second)})
		want := []string{"ax2", "az1"}
		if len(got) != len(want) {
			t.Fatalf("list: got %d records, want %d", len(got), len(want))
		}
		for i := range want {
			if string(got[i].Object) != want[i] {
				t.Errorf("list[%d] = %q, want %q", i, got[i].Object, want[i])
			}
		}
		got = list(t, v, store.ListOptions{Resource: pods, At: t0.Add(time.Hour)})
		if len(got) != 3 || got[0].Key.Name != "x" || got[1].Key.Name != "z" || got[1].Type != model.ChangeDeleted || got[2].Key.Namespace != "b" {
			t.Errorf("list all namespaces: unexpected %+v", got)
		}
		if got := list(t, v, store.ListOptions{Resource: pods, At: t0.Add(-time.Second)}); len(got) != 0 {
			t.Errorf("list before history: got %d", len(got))
		}
	})

	t.Run("NamespacePrefixIsolation", func(t *testing.T) {
		// "prod" must not match "prod-2", and name "a" must not match "ab".
		s := newStore(t)
		mustWrite(t, s,
			rec(key("prod", "a"), "1", model.ChangeAdded, t0, "1"),
			rec(key("prod", "ab"), "2", model.ChangeAdded, t0, "2"),
			rec(key("prod-2", "a"), "3", model.ChangeAdded, t0, "3"),
		)
		v := view(t, s)
		if got := list(t, v, store.ListOptions{Resource: pods, Namespace: "prod", At: t0.Add(time.Hour)}); len(got) != 2 {
			t.Errorf("namespace prod: got %d, want 2", len(got))
		}
		r, ok := get(t, v, key("prod", "a"), t0.Add(time.Hour))
		if !ok || string(r.Object) != "1" {
			t.Errorf("Get prod/a: %q ok=%v", r.Object, ok)
		}
	})

	t.Run("HistoryAndChanges", func(t *testing.T) {
		s := newStore(t)
		a, b := key("ns", "a"), key("other", "b")
		for i := range 5 {
			mustWrite(t, s, rec(a, "u", model.ChangeModified, t0.Add(time.Duration(i)*time.Second), fmt.Sprint(i)))
			mustWrite(t, s, rec(b, "u", model.ChangeModified, t0.Add(time.Duration(i)*time.Second+time.Millisecond), fmt.Sprint(i)))
		}
		v := view(t, s)
		var hist []string
		if err := v.History(ctx, a, t0.Add(time.Second), t0.Add(3*time.Second), func(r model.Record) error {
			hist = append(hist, string(r.Object))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(hist) != "[1 2 3]" {
			t.Errorf("History = %v", hist)
		}
		var ch []string
		if err := v.Changes(ctx, store.ChangesOptions{From: t0, To: t0.Add(time.Hour), Namespace: "other", Limit: 3}, func(r model.Record) error {
			ch = append(ch, r.Key.Name+string(r.Object))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(ch) != "[b0 b1 b2]" {
			t.Errorf("Changes = %v", ch)
		}
		var all int
		var last uint64
		if err := v.Changes(ctx, store.ChangesOptions{From: t0, To: t0.Add(time.Hour)}, func(r model.Record) error {
			if r.Seq <= last {
				t.Errorf("Changes out of order")
			}
			last = r.Seq
			all++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if all != 10 {
			t.Errorf("Changes all = %d, want 10", all)
		}
		stop := errors.New("stop")
		if err := v.Changes(ctx, store.ChangesOptions{From: t0, To: t0.Add(time.Hour)}, func(model.Record) error { return stop }); !errors.Is(err, stop) {
			t.Errorf("callback error not propagated: %v", err)
		}
	})

	t.Run("CoverageInvariants", func(t *testing.T) {
		s := newStore(t)
		if err := s.Write(ctx, []store.Entry{cov(pods, model.CoverageEnd, t0)}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("End without Start: err=%v, want ErrInvalid", err)
		}
		mustWrite(t, s, cov(pods, model.CoverageStart, t0))
		if err := s.Write(ctx, []store.Entry{cov(pods, model.CoverageStart, t0.Add(time.Second))}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Start after Start: err=%v, want ErrInvalid", err)
		}
		mustWrite(t, s, rec(key("ns", "a"), "u", model.ChangeAdded, t0.Add(10*time.Second), "x"))
		if err := s.Write(ctx, []store.Entry{cov(pods, model.CoverageEnd, t0.Add(5*time.Second))}); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("End before last record: err=%v, want ErrInvalid", err)
		}
		// End in the past but after the last record is allowed and keeps its time.
		end := cov(pods, model.CoverageEnd, t0.Add(11*time.Second))
		mustWrite(t, s, cov("apps/v1/deployments", model.CoverageStart, t0.Add(20*time.Second)), end)
		if !end.Coverage.At.Equal(t0.Add(11 * time.Second)) {
			t.Errorf("End time was changed to %s", end.Coverage.At)
		}
		mustWrite(t, s, cov(pods, model.CoverageStart, t0.Add(30*time.Second)))
		v := view(t, s)
		cs, err := v.Coverage(ctx, pods)
		if err != nil {
			t.Fatal(err)
		}
		if len(cs) != 3 || cs[0].Type != model.CoverageStart || cs[1].Type != model.CoverageEnd || cs[2].Type != model.CoverageStart {
			t.Errorf("coverage = %+v", cs)
		}
		res, err := v.Resources(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(res) != "[apps/v1/deployments v1/pods]" {
			t.Errorf("Resources = %v", res)
		}
	})

	t.Run("StartAfterEndWhenClockMovesBack", func(t *testing.T) {
		s := newStore(t)
		mustWrite(t, s, cov(pods, model.CoverageStart, t0))
		end := cov(pods, model.CoverageEnd, t0.Add(time.Hour))
		mustWrite(t, s, end)
		start := cov(pods, model.CoverageStart, t0.Add(time.Minute))
		mustWrite(t, s, start)
		if !start.Coverage.At.After(end.Coverage.At) {
			t.Errorf("Start at %s is not after End at %s", start.Coverage.At, end.Coverage.At)
		}
		r := rec(key("ns", "a"), "u", model.ChangeAdded, t0.Add(2*time.Minute), "x")
		mustWrite(t, s, r)
		if !r.Record.ObservedAt.After(start.Coverage.At) {
			t.Errorf("record at %s is not after Start at %s", r.Record.ObservedAt, start.Coverage.At)
		}
	})

	t.Run("FailedBatchIsAtomic", func(t *testing.T) {
		s := newStore(t)
		a := key("ns", "a")
		err := s.Write(ctx, []store.Entry{
			rec(a, "u", model.ChangeAdded, t0, "x"),
			rec(model.ObjectKey{Resource: pods, Namespace: "bad/ns", Name: "n"}, "u", model.ChangeAdded, t0, "x"),
		})
		if !errors.Is(err, store.ErrInvalid) {
			t.Fatalf("err=%v, want ErrInvalid", err)
		}
		if _, ok := get(t, view(t, s), a, t0.Add(time.Hour)); ok {
			t.Errorf("partial batch visible")
		}
		// Seq/time bookkeeping must not have advanced either.
		e := rec(a, "u", model.ChangeAdded, t0, "y")
		mustWrite(t, s, e)
		if !e.Record.ObservedAt.Equal(t0) {
			t.Errorf("failed batch advanced the clock: %s", e.Record.ObservedAt)
		}
	})

	t.Run("ViewIsSnapshot", func(t *testing.T) {
		s := newStore(t)
		a := key("ns", "a")
		mustWrite(t, s, rec(a, "u", model.ChangeAdded, t0, "1"))
		v := view(t, s)
		mustWrite(t, s, rec(a, "u", model.ChangeModified, t0.Add(time.Second), "2"))
		if err := s.Compact(ctx, t0.Add(500*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		r, ok := get(t, v, a, t0.Add(time.Hour))
		if !ok || string(r.Object) != "1" {
			t.Errorf("old view sees %q ok=%v, want 1", r.Object, ok)
		}
		if !v.Horizon().IsZero() {
			t.Errorf("old view sees new horizon")
		}
	})

	t.Run("Compact", func(t *testing.T) {
		s := newStore(t)
		a, b, c := key("ns", "a"), key("ns", "b"), key("ns", "c")
		mustWrite(t, s,
			cov(pods, model.CoverageStart, t0),
			rec(a, "u", model.ChangeAdded, t0.Add(1*time.Second), "a1"),
			rec(a, "u", model.ChangeModified, t0.Add(2*time.Second), "a2"),
			rec(b, "u", model.ChangeAdded, t0.Add(3*time.Second), "b1"),
			rec(b, "u", model.ChangeDeleted, t0.Add(4*time.Second), "b1"),
			rec(a, "u", model.ChangeModified, t0.Add(20*time.Second), "a3"),
			rec(c, "u", model.ChangeAdded, t0.Add(21*time.Second), "c1"),
		)
		mustWrite(t, s, cov(pods, model.CoverageEnd, t0.Add(22*time.Second)))
		mustWrite(t, s, cov(pods, model.CoverageStart, t0.Add(25*time.Second)))
		h := t0.Add(10 * time.Second)
		probe := func(v store.View) map[time.Duration][]string {
			out := map[time.Duration][]string{}
			for _, d := range []time.Duration{10, 15, 20, 21, 30} {
				at := t0.Add(d * time.Second)
				for _, r := range list(t, v, store.ListOptions{Resource: pods, At: at}) {
					if r.Type != model.ChangeDeleted {
						out[d] = append(out[d], string(r.Object))
					}
				}
			}
			return out
		}
		before := probe(view(t, s))
		if err := s.Compact(ctx, h); err != nil {
			t.Fatal(err)
		}
		v := view(t, s)
		after := probe(v)
		if fmt.Sprint(before) != fmt.Sprint(after) {
			t.Errorf("state at t >= horizon changed:\nbefore %v\nafter  %v", before, after)
		}
		if !v.Horizon().Equal(h) {
			t.Errorf("Horizon = %s, want %s", v.Horizon(), h)
		}
		var n int
		if err := v.History(ctx, a, time.Time{}, t0.Add(time.Hour), func(model.Record) error { n++; return nil }); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("a has %d records after compaction, want 2 (a2 carried, a3)", n)
		}
		if _, ok := get(t, v, b, t0.Add(time.Hour)); ok {
			t.Errorf("b was deleted before horizon and should be gone")
		}
		cs, _ := v.Coverage(ctx, pods)
		if len(cs) != 3 {
			t.Errorf("coverage after compaction: %d events, want 3 (Start carried, End, Start)", len(cs))
		}
		var changes int
		if err := v.Changes(ctx, store.ChangesOptions{From: time.Time{}, To: t0.Add(time.Hour)}, func(model.Record) error { changes++; return nil }); err != nil {
			t.Fatal(err)
		}
		if changes != 3 {
			t.Errorf("Changes after compaction = %d, want 3", changes)
		}
		// Compacting to an older horizon is a no-op.
		if err := s.Compact(ctx, t0); err != nil {
			t.Fatal(err)
		}
		if v2 := view(t, s); !v2.Horizon().Equal(h) {
			t.Errorf("horizon moved backwards to %s", v2.Horizon())
		}
	})

	t.Run("LatestRecordTimeSurvivesCompaction", func(t *testing.T) {
		s := newStore(t)
		a := key("ns", "a")
		del := rec(a, "u", model.ChangeDeleted, t0.Add(2*time.Second), "x")
		mustWrite(t, s, cov(pods, model.CoverageStart, t0), rec(a, "u", model.ChangeAdded, t0.Add(time.Second), "x"), del)
		if err := s.Compact(ctx, t0.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		v := view(t, s)
		lt, err := v.LatestRecordTime(ctx, pods)
		if err != nil {
			t.Fatal(err)
		}
		if !lt.Equal(del.Record.ObservedAt) {
			t.Errorf("LatestRecordTime = %s, want %s", lt, del.Record.ObservedAt)
		}
		if lt, _ := v.LatestRecordTime(ctx, "v1/services"); !lt.IsZero() {
			t.Errorf("LatestRecordTime of unused resource = %s", lt)
		}
		// The bound it reports must be accepted as a crash End.
		mustWrite(t, s, cov(pods, model.CoverageEnd, lt))
	})

	t.Run("Heartbeat", func(t *testing.T) {
		s := newStore(t)
		if hb, err := s.Heartbeat(ctx); err != nil || !hb.IsZero() {
			t.Fatalf("initial heartbeat %s %v", hb, err)
		}
		if err := s.SetHeartbeat(ctx, t0); err != nil {
			t.Fatal(err)
		}
		if hb, _ := s.Heartbeat(ctx); !hb.Equal(t0) {
			t.Errorf("heartbeat = %s", hb)
		}
	})

	t.Run("Closed", func(t *testing.T) {
		s := newStore(t)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Write(ctx, []store.Entry{rec(key("ns", "a"), "u", model.ChangeAdded, t0, "x")}); !errors.Is(err, store.ErrClosed) {
			t.Errorf("Write after Close: %v", err)
		}
		if _, err := s.View(ctx); !errors.Is(err, store.ErrClosed) {
			t.Errorf("View after Close: %v", err)
		}
	})
}
