package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// TestDifferential applies the same random operation sequence to memstore
// (the oracle) and pebblestore and compares every read at random times.
// Each seed is a subtest, so a failure reproduces with
// -run 'TestDifferential/seed=N'. CHRONOS_DIFF_SEEDS and CHRONOS_DIFF_OPS
// scale the run.
func TestDifferential(t *testing.T) {
	seeds, ops := 24, 300
	if testing.Short() {
		seeds, ops = 6, 150
	}
	if v, err := strconv.Atoi(os.Getenv("CHRONOS_DIFF_SEEDS")); err == nil {
		seeds = v
	}
	if v, err := strconv.Atoi(os.Getenv("CHRONOS_DIFF_OPS")); err == nil {
		ops = v
	}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			runDifferential(t, uint64(seed), ops)
		})
	}
}

var (
	diffResources  = []string{"v1/pods", "apps/v1/deployments", "v1/nodes", "v1/pods2"}
	diffNamespaces = []string{"", "a", "ab", "prod", "prod-2", "kube-system"}
	diffNames      = []string{"a", "ab", "b", "web-0", "web-00", "x"}
)

type diffHarness struct {
	t      *testing.T
	r      *rand.Rand
	dir    string
	mem    *memstore.Store
	peb    *Store
	t0     time.Time
	cursor time.Time
	times  []time.Time // every assigned record/coverage time
	views  []viewPair
	stats  map[string]int
}

type viewPair struct {
	mem, peb store.View
	label    string
}

func runDifferential(t *testing.T, seed uint64, ops int) {
	h := &diffHarness{
		t:     t,
		r:     newRand(seed),
		dir:   t.TempDir(),
		mem:   memstore.New(),
		t0:    time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		stats: map[string]int{},
	}
	h.cursor = h.t0
	h.peb = openTest(t, h.dir)
	t.Cleanup(func() {
		h.closeViews()
		_ = h.peb.Close()
	})
	for i := range ops {
		op := h.r.IntN(100)
		switch {
		case op < 55:
			h.write(i)
		case op < 63:
			h.coverage(i)
		case op < 68:
			h.compact(i)
		case op < 74:
			if err := h.peb.Snapshot(context.Background()); err != nil {
				t.Fatalf("op %d: Snapshot: %v", i, err)
			}
			h.stats["snapshot"]++
		case op < 77:
			h.reopen(i)
		case op < 80:
			h.heartbeat(i)
		case op < 84:
			h.openViewPair(i)
		default:
			h.compareAll(i)
		}
	}
	h.compareAll(ops)
	t.Logf("ops: %v; manifest lists %d, scan lists %d, fallbacks %d", h.stats,
		h.peb.stats.manifest.Load(), h.peb.stats.scan.Load(), h.peb.stats.fallbacks.Load())
	if h.peb.stats.fallbacks.Load() != 0 {
		t.Errorf("manifest fallbacks without corruption: %d", h.peb.stats.fallbacks.Load())
	}
}

func (h *diffHarness) pick(xs []string) string { return xs[h.r.IntN(len(xs))] }

func (h *diffHarness) advance() time.Time {
	switch h.r.IntN(10) {
	case 0: // clock stalls
	case 1: // clock goes backwards
		h.cursor = h.cursor.Add(-time.Duration(h.r.IntN(3000)) * time.Millisecond)
	default:
		h.cursor = h.cursor.Add(time.Duration(h.r.IntN(3000)) * time.Millisecond)
	}
	return h.cursor
}

func (h *diffHarness) randKey() model.ObjectKey {
	res := h.pick(diffResources)
	ns := h.pick(diffNamespaces)
	if res == "v1/nodes" {
		ns = ""
	}
	return model.ObjectKey{Resource: res, Namespace: ns, Name: h.pick(diffNames)}
}

func (h *diffHarness) randEntry(i int) store.Entry {
	if h.r.IntN(6) == 0 {
		res := h.pick(diffResources)
		typ := model.CoverageStart
		if h.r.IntN(2) == 0 {
			typ = model.CoverageEnd
		}
		return store.Entry{Coverage: &model.CoverageEvent{At: h.advance(), Resource: res, Type: typ, Reason: fmt.Sprintf("op%d", i)}}
	}
	k := h.randKey()
	typ := model.ChangeType(1 + h.r.IntN(3))
	var obj []byte
	if h.r.IntN(10) != 0 {
		obj = fmt.Appendf(nil, `{"op":%d,"k":%q,"pad":%q}`, i, k.String(), bytes.Repeat([]byte{'p'}, h.r.IntN(200)))
	}
	return store.Entry{Record: &model.Record{
		ObservedAt: h.advance(), Key: k, UID: fmt.Sprintf("uid-%d", h.r.IntN(4)),
		ResourceVersion: strconv.Itoa(i), Type: typ, Origin: model.Origin(1 + h.r.IntN(3)), Object: obj,
	}}
}

// cloneEntries deep-copies entries so each store gets its own write-back.
func cloneEntries(es []store.Entry) []store.Entry {
	out := make([]store.Entry, len(es))
	for i, e := range es {
		if e.Record != nil {
			r := *e.Record
			r.Object = slices.Clone(r.Object)
			out[i].Record = &r
		}
		if e.Coverage != nil {
			c := *e.Coverage
			out[i].Coverage = &c
		}
	}
	return out
}

func (h *diffHarness) applyBoth(i int, es []store.Entry) {
	t := h.t
	ctx := context.Background()
	me, pe := cloneEntries(es), cloneEntries(es)
	merr := h.mem.Write(ctx, me)
	perr := h.peb.Write(ctx, pe)
	if (merr == nil) != (perr == nil) || (merr != nil && errors.Is(merr, store.ErrInvalid) != errors.Is(perr, store.ErrInvalid)) {
		t.Fatalf("op %d: Write errors differ: mem=%v peb=%v", i, merr, perr)
	}
	if merr != nil {
		h.stats["writeRejected"]++
		return
	}
	h.stats["write"]++
	for j := range me {
		if me[j].Record != nil {
			m, p := me[j].Record, pe[j].Record
			if m.Seq != p.Seq || !m.ObservedAt.Equal(p.ObservedAt) {
				t.Fatalf("op %d entry %d: write-back mem=(%d,%s) peb=(%d,%s)", i, j, m.Seq, m.ObservedAt, p.Seq, p.ObservedAt)
			}
			h.times = append(h.times, m.ObservedAt)
		} else {
			m, p := me[j].Coverage, pe[j].Coverage
			if m.Seq != p.Seq || !m.At.Equal(p.At) {
				t.Fatalf("op %d entry %d: coverage write-back mem=(%d,%s) peb=(%d,%s)", i, j, m.Seq, m.At, p.Seq, p.At)
			}
			h.times = append(h.times, m.At)
		}
	}
}

func (h *diffHarness) write(i int) {
	n := 1 + h.r.IntN(5)
	es := make([]store.Entry, 0, n+1)
	for range n {
		es = append(es, h.randEntry(i))
	}
	if h.r.IntN(15) == 0 { // poison the batch: must be rejected atomically
		es = append(es, store.Entry{Record: &model.Record{Key: model.ObjectKey{Resource: "v1/pods", Namespace: "bad/ns", Name: "n"}, Type: model.ChangeAdded, Origin: model.OriginWatch}})
	}
	h.applyBoth(i, es)
}

// coverage writes a single coverage event, choosing End times around the
// latest record so both the accepted and the rejected branches happen.
func (h *diffHarness) coverage(i int) {
	res := h.pick(diffResources)
	c := &model.CoverageEvent{Resource: res, Reason: fmt.Sprintf("cov%d", i)}
	if h.r.IntN(2) == 0 {
		c.Type, c.At = model.CoverageStart, h.advance()
	} else {
		c.Type = model.CoverageEnd
		c.At = h.cursor.Add(time.Duration(h.r.IntN(4000)-1000) * time.Millisecond)
	}
	h.applyBoth(i, []store.Entry{{Coverage: c}})
}

func (h *diffHarness) compact(i int) {
	// Horizons mostly inside the written range, sometimes beyond it.
	span := max(h.cursor.Sub(h.t0), 0) + 5*time.Second
	hz := h.t0.Add(time.Duration(h.r.Int64N(int64(span))))
	ctx := context.Background()
	if err := h.mem.Compact(ctx, hz); err != nil {
		h.t.Fatalf("op %d: mem Compact: %v", i, err)
	}
	if err := h.peb.Compact(ctx, hz); err != nil {
		h.t.Fatalf("op %d: peb Compact: %v", i, err)
	}
	h.stats["compact"]++
}

func (h *diffHarness) heartbeat(i int) {
	ctx := context.Background()
	hb := h.cursor.Add(time.Duration(h.r.IntN(2000)-1000) * time.Millisecond)
	if err := errors.Join(h.mem.SetHeartbeat(ctx, hb), h.peb.SetHeartbeat(ctx, hb)); err != nil {
		h.t.Fatalf("op %d: SetHeartbeat: %v", i, err)
	}
	m, _ := h.mem.Heartbeat(ctx)
	p, err := h.peb.Heartbeat(ctx)
	if err != nil || !m.Equal(p) {
		h.t.Fatalf("op %d: heartbeat mem=%s peb=%s err=%v", i, m, p, err)
	}
}

func (h *diffHarness) closeViews() {
	for _, vp := range h.views {
		_ = vp.mem.Close()
		_ = vp.peb.Close()
	}
	h.views = nil
}

func (h *diffHarness) reopen(i int) {
	h.closeViews()
	if err := h.peb.Close(); err != nil {
		h.t.Fatalf("op %d: Close: %v", i, err)
	}
	h.peb = openTest(h.t, h.dir)
	h.stats["reopen"]++
	ctx := context.Background()
	m, _ := h.mem.Heartbeat(ctx)
	p, err := h.peb.Heartbeat(ctx)
	if err != nil || !m.Equal(p) {
		h.t.Fatalf("op %d: heartbeat after reopen mem=%s peb=%s err=%v", i, m, p, err)
	}
}

// openViewPair keeps a pair of views open across later writes and
// compactions; compareAll checks they still agree (snapshot isolation).
func (h *diffHarness) openViewPair(i int) {
	ctx := context.Background()
	mv, err := h.mem.View(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	pv, err := h.peb.View(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	h.views = append(h.views, viewPair{mv, pv, fmt.Sprintf("view@op%d", i)})
	if len(h.views) > 4 {
		_ = h.views[0].mem.Close()
		_ = h.views[0].peb.Close()
		h.views = h.views[1:]
	}
}

func (h *diffHarness) randTime() time.Time {
	if len(h.times) > 0 && h.r.IntN(2) == 0 {
		t := h.times[h.r.IntN(len(h.times))]
		return t.Add(time.Duration(h.r.IntN(3)-1) * time.Nanosecond)
	}
	span := max(h.cursor.Sub(h.t0), 0) + 4*time.Second
	return h.t0.Add(-2*time.Second + time.Duration(h.r.Int64N(int64(span))))
}

func (h *diffHarness) compareAll(i int) {
	ctx := context.Background()
	mv, err := h.mem.View(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	pv, err := h.peb.View(ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	pairs := append([]viewPair{{mv, pv, "fresh"}}, h.views...)
	for _, vp := range pairs {
		h.compareViews(fmt.Sprintf("op %d %s", i, vp.label), vp.mem, vp.peb)
	}
	_ = mv.Close()
	_ = pv.Close()
	h.stats["compare"]++
}

func (h *diffHarness) compareViews(where string, mv, pv store.View) {
	t := h.t
	ctx := context.Background()
	if !mv.Horizon().Equal(pv.Horizon()) {
		t.Fatalf("%s: Horizon mem=%s peb=%s", where, mv.Horizon(), pv.Horizon())
	}
	mres, err1 := mv.Resources(ctx)
	pres, err2 := pv.Resources(ctx)
	if err := errors.Join(err1, err2); err != nil || fmt.Sprint(mres) != fmt.Sprint(pres) {
		t.Fatalf("%s: Resources mem=%v peb=%v err=%v", where, mres, pres, err)
	}
	for _, res := range diffResources {
		ml, err1 := mv.LatestRecordTime(ctx, res)
		pl, err2 := pv.LatestRecordTime(ctx, res)
		if err := errors.Join(err1, err2); err != nil || !ml.Equal(pl) {
			t.Fatalf("%s: LatestRecordTime(%s) mem=%s peb=%s err=%v", where, res, ml, pl, err)
		}
		mc, err1 := mv.Coverage(ctx, res)
		pc, err2 := pv.Coverage(ctx, res)
		if err := errors.Join(err1, err2); err != nil {
			t.Fatalf("%s: Coverage: %v", where, err)
		}
		if d := diffCoverage(mc, pc); d != "" {
			t.Fatalf("%s: Coverage(%s): %s", where, res, d)
		}
	}
	for range 6 {
		k := h.randKey()
		at := h.randTime()
		mr, mok, err1 := mv.Get(ctx, k, at)
		pr, pok, err2 := pv.Get(ctx, k, at)
		if err := errors.Join(err1, err2); err != nil {
			t.Fatalf("%s: Get: %v", where, err)
		}
		if mok != pok || (mok && diffRecord(mr, pr) != "") {
			t.Fatalf("%s: Get(%s, %s): mem ok=%v peb ok=%v %s", where, k, at, mok, pok, diffRecord(mr, pr))
		}
	}
	for j := range 4 {
		opts := store.ListOptions{Resource: h.pick(diffResources), At: h.randTime()}
		if j == 0 {
			opts.At = h.cursor.Add(time.Second) // "now": the common query
		}
		if h.r.IntN(2) == 0 {
			opts.Namespace = h.pick(diffNamespaces)
		}
		h.compareStreams(where, fmt.Sprintf("List(%+v)", opts),
			func(fn func(model.Record) error) error { return mv.List(ctx, opts, fn) },
			func(fn func(model.Record) error) error { return pv.List(ctx, opts, fn) })
		// The manifest path must agree with the scan path on its own too.
		pview := pv.(*view)
		h.compareStreams(where, fmt.Sprintf("List scan vs default (%+v)", opts),
			func(fn func(model.Record) error) error { return pview.listScan(ctx, opts, fn) },
			func(fn func(model.Record) error) error { return pv.List(ctx, opts, fn) })
	}
	for range 3 {
		k := h.randKey()
		from, to := h.randTime(), h.randTime()
		if h.r.IntN(4) == 0 {
			from = time.Time{}
		}
		h.compareStreams(where, fmt.Sprintf("History(%s, %s, %s)", k, from, to),
			func(fn func(model.Record) error) error { return mv.History(ctx, k, from, to, fn) },
			func(fn func(model.Record) error) error { return pv.History(ctx, k, from, to, fn) })
	}
	for range 3 {
		opts := store.ChangesOptions{From: h.randTime(), To: h.randTime()}
		if h.r.IntN(3) == 0 {
			opts.From = time.Time{}
		}
		if h.r.IntN(2) == 0 {
			opts.Resource = h.pick(diffResources)
		}
		if h.r.IntN(2) == 0 {
			opts.Namespace = h.pick(diffNamespaces)
		}
		if h.r.IntN(2) == 0 {
			opts.Limit = 1 + h.r.IntN(5)
		}
		h.compareStreams(where, fmt.Sprintf("Changes(%+v)", opts),
			func(fn func(model.Record) error) error { return mv.Changes(ctx, opts, fn) },
			func(fn func(model.Record) error) error { return pv.Changes(ctx, opts, fn) })
	}
}

func collect(run func(func(model.Record) error) error) ([]model.Record, error) {
	var out []model.Record
	err := run(func(r model.Record) error {
		r.Object = slices.Clone(r.Object)
		out = append(out, r)
		return nil
	})
	return out, err
}

func (h *diffHarness) compareStreams(where, what string, mrun, prun func(func(model.Record) error) error) {
	m, err1 := collect(mrun)
	p, err2 := collect(prun)
	if err := errors.Join(err1, err2); err != nil {
		h.t.Fatalf("%s: %s: %v", where, what, err)
	}
	if len(m) != len(p) {
		h.t.Fatalf("%s: %s: mem %d records, peb %d\nmem: %s\npeb: %s", where, what, len(m), len(p), summarize(m), summarize(p))
	}
	for i := range m {
		if d := diffRecord(m[i], p[i]); d != "" {
			h.t.Fatalf("%s: %s: record %d: %s", where, what, i, d)
		}
	}
}

func summarize(rs []model.Record) string {
	var b bytes.Buffer
	for _, r := range rs {
		fmt.Fprintf(&b, "[%d %s %s %s] ", r.Seq, r.Key, r.Type, r.ObservedAt.Format(time.RFC3339Nano))
	}
	return b.String()
}

func diffRecord(a, b model.Record) string {
	if a.Seq != b.Seq || !a.ObservedAt.Equal(b.ObservedAt) || a.Key != b.Key || a.UID != b.UID ||
		a.ResourceVersion != b.ResourceVersion || a.Type != b.Type || a.Origin != b.Origin || !bytes.Equal(a.Object, b.Object) {
		return fmt.Sprintf("mem={%d %s %s %s %s %s %s %q} peb={%d %s %s %s %s %s %s %q}",
			a.Seq, a.ObservedAt, a.Key, a.UID, a.ResourceVersion, a.Type, a.Origin, a.Object,
			b.Seq, b.ObservedAt, b.Key, b.UID, b.ResourceVersion, b.Type, b.Origin, b.Object)
	}
	return ""
}

func diffCoverage(a, b []model.CoverageEvent) string {
	if len(a) != len(b) {
		return fmt.Sprintf("mem %d events %+v, peb %d events %+v", len(a), a, len(b), b)
	}
	for i := range a {
		if a[i].Seq != b[i].Seq || !a[i].At.Equal(b[i].At) || a[i].Resource != b[i].Resource || a[i].Type != b[i].Type || a[i].Reason != b[i].Reason {
			return fmt.Sprintf("event %d: mem %+v peb %+v", i, a[i], b[i])
		}
	}
	return ""
}
