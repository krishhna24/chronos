package pebblestore

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Benchmarks for ADR 0001 "Validation". Run with, for example:
//
//	go test -run '^$' -bench . -benchtime 2000x ./internal/store/pebblestore
//	go test -run '^$' -bench 'DiskPerChange|Compact' -benchtime 1x ./internal/store/pebblestore
//
// The churn dataset is built once per process (see churnPlan). Set
// CHRONOS_BENCH_DATASET_DIR to keep it between runs, and
// CHRONOS_BENCH_PODS to scale it down.

var benchStart = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// benchBlock is the SSTable block size for the churn dataset
// (CHRONOS_BENCH_BLOCK, bytes; 0 = DefaultBlockSize).
func benchBlock() int {
	v, _ := strconv.Atoi(os.Getenv("CHRONOS_BENCH_BLOCK"))
	return v
}

func benchPlan() churnPlan {
	p := churnPlan{namespaces: 50, deployments: 1000, pods: 9000, days: 7, meanLifetime: 48 * time.Hour}
	if v, err := strconv.Atoi(os.Getenv("CHRONOS_BENCH_PODS")); err == nil && v > 0 {
		p.deployments = max(1, p.deployments*v/p.pods)
		p.pods = v
	}
	return p
}

type dataset struct {
	dir        string
	plan       churnPlan
	end        time.Time
	records    int
	rawBytes   int64
	podKeys    int // distinct pod keys written
	live       []model.ObjectKey
	manifests  int
	buildTime  time.Duration
	diskBytes  int64
	namespace0 string
}

var (
	dsOnce sync.Once
	ds     *dataset
	dsErr  error
)

func benchDataset(b *testing.B) *dataset {
	b.Helper()
	dsOnce.Do(func() { ds, dsErr = buildDataset() })
	if dsErr != nil {
		b.Fatal(dsErr)
	}
	return ds
}

func nsName(i int) string { return fmt.Sprintf("team-%02d", i) }

func buildDataset() (*dataset, error) {
	plan := benchPlan()
	dir := os.Getenv("CHRONOS_BENCH_DATASET_DIR")
	if dir == "" {
		var err error
		if dir, err = os.MkdirTemp("", "chronos-bench-"); err != nil {
			return nil, err
		}
	} else {
		dir = filepath.Join(dir, fmt.Sprintf("pods%d-block%d", plan.pods, benchBlock()))
	}
	d := &dataset{dir: dir, plan: plan, namespace0: nsName(7)}
	g := newSynth(7)
	evs := plan.events(g, benchStart)
	d.end = benchStart.Add(time.Duration(plan.days) * 24 * time.Hour)

	type podSlot struct {
		p   *podObj
		key model.ObjectKey
	}
	deps := make([]*deployObj, plan.deployments)
	depKeys := make([]model.ObjectKey, plan.deployments)
	slots := make([]podSlot, plan.pods)
	rv := 1
	var entries []store.Entry
	add := func(k model.ObjectKey, uid string, typ model.ChangeType, origin model.Origin, at time.Time, obj []byte) {
		entries = append(entries, store.Entry{Record: &model.Record{ObservedAt: at, Key: k, UID: uid, ResourceVersion: strconv.Itoa(rv), Type: typ, Origin: origin, Object: obj}})
		d.records++
		d.rawBytes += int64(len(obj))
	}
	// Build the store's contents in memory first so a reused dataset
	// directory can be validated against the same plan.
	var batches [][]store.Entry
	var batchTimes []time.Time
	flush := func(at time.Time) {
		if len(entries) > 0 {
			batches = append(batches, entries)
			batchTimes = append(batchTimes, at)
			entries = nil
		}
	}
	for _, ev := range evs {
		rv++
		if ev.slot < 0 {
			di := -1 - ev.slot
			if ev.kind == evPodCreate {
				name := fmt.Sprintf("svc-%04d", di)
				deps[di] = g.newDeployment(nsName(di%plan.namespaces), name, 9, ev.at.Add(-30*24*time.Hour))
				depKeys[di] = model.ObjectKey{Resource: "apps/v1/deployments", Namespace: nsName(di % plan.namespaces), Name: name}
				deps[di].bumpRV(rv)
				add(depKeys[di], "dep", model.ChangeAdded, model.OriginBaseline, ev.at, mustJSON(deps[di].m))
			} else {
				g.deployStatusUpdate(deps[di], ev.at)
				deps[di].bumpRV(rv)
				add(depKeys[di], "dep", model.ChangeModified, model.OriginWatch, ev.at, mustJSON(deps[di].m))
			}
		} else {
			sl := &slots[ev.slot]
			di := ev.slot % plan.deployments
			origin := model.OriginWatch
			switch ev.kind {
			case evPodCreate:
				ns := nsName(di % plan.namespaces)
				name := fmt.Sprintf("svc-%04d-%s-%s", di, g.hex(10), g.hex(5))
				sl.p = g.newPod(ns, name, fmt.Sprintf("svc-%04d", di), ev.at)
				sl.key = model.ObjectKey{Resource: "v1/pods", Namespace: ns, Name: name}
				d.podKeys++
				typ := model.ChangeAdded
				if ev.at.Equal(benchStart) {
					origin = model.OriginBaseline
					g.podScheduled(sl.p, ev.at)
					g.podRunning(sl.p, ev.at)
				}
				sl.p.bumpRV(rv)
				add(sl.key, "pod", typ, origin, ev.at, mustJSON(sl.p.m))
				continue
			case evPodSchedule:
				if ev.at.Before(benchStart.Add(time.Minute)) && ev.inst == 0 {
					continue // baseline pods are already running
				}
				g.podScheduled(sl.p, ev.at)
			case evPodRun:
				if ev.at.Before(benchStart.Add(time.Minute)) && ev.inst == 0 {
					continue
				}
				g.podRunning(sl.p, ev.at)
			case evPodUpdate:
				g.podStatusUpdate(sl.p, ev.at)
			case evPodTerminate:
				g.podTerminating(sl.p, ev.at)
			case evPodDelete:
				sl.p.bumpRV(rv)
				add(sl.key, "pod", model.ChangeDeleted, origin, ev.at, mustJSON(sl.p.m))
				continue
			}
			sl.p.bumpRV(rv)
			add(sl.key, "pod", model.ChangeModified, origin, ev.at, mustJSON(sl.p.m))
		}
		if len(entries) >= 256 {
			flush(ev.at)
		}
	}
	flush(d.end)
	for di := range deps {
		d.live = append(d.live, depKeys[di])
	}
	for i := range slots {
		d.live = append(d.live, slots[i].key)
	}

	marker := filepath.Join(dir, "DATASET_READY")
	if _, err := os.Stat(marker); err == nil { //nolint:gosec // benchmark-owned directory

		d.diskBytes, _ = dirSize(dir)
		s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, BlockSize: benchBlock()})
		if err != nil {
			return nil, err
		}
		d.manifests = countManifests(s)
		return d, s.Close()
	}
	start := time.Now()
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, BlockSize: benchBlock()})
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if err := s.Write(ctx, []store.Entry{
		{Coverage: &model.CoverageEvent{At: benchStart.Add(-time.Second), Resource: "v1/pods", Type: model.CoverageStart, Reason: "list"}},
		{Coverage: &model.CoverageEvent{At: benchStart.Add(-time.Second), Resource: "apps/v1/deployments", Type: model.CoverageStart, Reason: "list"}},
	}); err != nil {
		return nil, err
	}
	nextSnap := benchStart.Add(time.Hour)
	for i, batch := range batches {
		if err := s.Write(ctx, batch); err != nil {
			return nil, err
		}
		// Hourly manifests, as the background job would take them.
		if !batchTimes[i].Before(nextSnap) {
			if err := s.Snapshot(ctx); err != nil {
				return nil, err
			}
			for !batchTimes[i].Before(nextSnap) {
				nextSnap = nextSnap.Add(time.Hour)
			}
		}
	}
	if err := s.Flush(); err != nil {
		return nil, err
	}
	d.manifests = countManifests(s)
	if err := s.Close(); err != nil {
		return nil, err
	}
	d.buildTime = time.Since(start)
	d.diskBytes, _ = dirSize(dir)
	if err := os.WriteFile(marker, []byte(plan.String()), 0o600); err != nil { //nolint:gosec // benchmark-owned directory
		return nil, err
	}
	return d, nil
}

func countManifests(s *Store) int {
	it, err := s.db.NewIter(nil)
	if err != nil {
		return -1
	}
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.SeekGE([]byte{tabManifest}); ok && it.Key()[0] == tabManifest; ok = it.Next() {
		n++
	}
	return n
}

func dirSize(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(_ string, de fs.DirEntry, err error) error { //nolint:gosec // benchmark-owned directory
		if err != nil {
			return err
		}
		if !de.IsDir() {
			info, err := de.Info()
			if err != nil {
				return err
			}
			n += info.Size()
		}
		return nil
	})
	return n, err
}

func sstSize(dir string) (int64, error) {
	var n int64
	err := filepath.WalkDir(dir, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasSuffix(p, ".sst") || strings.HasSuffix(p, ".blob") {
			info, err := de.Info()
			if err != nil {
				return err
			}
			n += info.Size()
		}
		return nil
	})
	return n, err
}

var dsLogOnce sync.Once

func openDataset(b *testing.B) (*dataset, *Store) {
	b.Helper()
	d := benchDataset(b)
	dsLogOnce.Do(func() {
		b.Logf("dataset: %s; block size %d; %d records (%d pod keys, %.0f MB raw JSON, avg %d B), %d manifests, %.0f MB on disk, built in %s",
			d.plan, benchBlock(), d.records, d.podKeys, float64(d.rawBytes)/1e6, d.rawBytes/int64(max(d.records, 1)), d.manifests,
			float64(d.diskBytes)/1e6, d.buildTime.Round(time.Second))
	})
	s, err := Open(d.dir, Options{Logger: testLogger(), SnapshotInterval: -1, BlockSize: benchBlock()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return d, s
}

func BenchmarkWrite(b *testing.B) {
	for _, batch := range []int{1, 100} {
		b.Run(fmt.Sprintf("batch=%d", batch), func(b *testing.B) {
			s, err := Open(b.TempDir(), Options{Logger: testLogger(), SnapshotInterval: -1})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			g := newSynth(3)
			const objects = 500
			pods := make([]*podObj, objects)
			for i := range pods {
				pods[i] = g.newPod("bench", fmt.Sprintf("pod-%04d", i), "app", benchStart)
				g.podScheduled(pods[i], benchStart)
				g.podRunning(pods[i], benchStart)
			}
			// Pre-render a pool of status updates so JSON encoding is not
			// part of the measurement.
			bodies := make([][]byte, 4*objects)
			var raw int64
			for i := range bodies {
				p := pods[i%objects]
				g.podStatusUpdate(p, benchStart.Add(time.Duration(i)*time.Second))
				bodies[i] = mustJSON(p.m)
				raw += int64(len(bodies[i]))
			}
			ctx := context.Background()
			at := benchStart
			n := 0
			es := make([]store.Entry, batch)
			b.SetBytes(raw / int64(len(bodies)) * int64(batch))
			b.ResetTimer()
			for b.Loop() {
				for j := range es {
					at = at.Add(time.Millisecond)
					es[j] = store.Entry{Record: &model.Record{
						ObservedAt: at, Key: model.ObjectKey{Resource: "v1/pods", Namespace: "bench", Name: fmt.Sprintf("pod-%04d", n%objects)},
						UID: "u", ResourceVersion: strconv.Itoa(n), Type: model.ChangeModified, Origin: model.OriginWatch, Object: bodies[n%len(bodies)],
					}}
					n++
				}
				if err := s.Write(ctx, es); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(n)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

func BenchmarkGet(b *testing.B) {
	d, s := openDataset(b)
	ctx := context.Background()
	v, err := s.View(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	r := newRand(9)
	span := int64(d.end.Sub(benchStart))
	for _, c := range []struct {
		name string
		at   func() time.Time
	}{
		{"at=latest", func() time.Time { return d.end }},
		{"at=random", func() time.Time { return benchStart.Add(time.Duration(r.Int64N(span))) }},
	} {
		b.Run(c.name, func(b *testing.B) {
			found := 0
			for b.Loop() {
				k := d.live[r.IntN(len(d.live))]
				_, ok, err := v.Get(ctx, k, c.at())
				if err != nil {
					b.Fatal(err)
				}
				if ok {
					found++
				}
			}
			b.ReportMetric(float64(found)/float64(b.N), "found/op")
		})
	}
}

func BenchmarkList(b *testing.B) {
	d, s := openDataset(b)
	ctx := context.Background()
	v, err := s.View(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	pv := v.(*view)
	// 30 minutes after the newest manifest at or before the query time.
	latest := d.end.Add(-30 * time.Minute)
	historical := benchStart.Add(3*24*time.Hour + 30*time.Minute)
	for _, at := range []struct {
		name string
		t    time.Time
	}{{"latest", latest}, {"day3", historical}} {
		for _, scope := range []struct{ name, ns string }{{"namespace", d.namespace0}, {"cluster", ""}} {
			opts := store.ListOptions{Resource: "v1/pods", Namespace: scope.ns, At: at.t}
			for _, path := range []string{"manifest", "scan"} {
				b.Run(fmt.Sprintf("at=%s/scope=%s/path=%s", at.name, scope.name, path), func(b *testing.B) {
					var total, live int
					before := s.stats.fallbacks.Load()
					for b.Loop() {
						total, live = 0, 0
						fn := func(r model.Record) error {
							total++
							if r.Type != model.ChangeDeleted {
								live++
							}
							return nil
						}
						var err error
						if path == "manifest" {
							err = v.List(ctx, opts, fn)
						} else {
							err = pv.listScan(ctx, opts, fn)
						}
						if err != nil {
							b.Fatal(err)
						}
					}
					if s.stats.fallbacks.Load() != before {
						b.Fatal("manifest path fell back")
					}
					b.ReportMetric(float64(total), "records/op")
					b.ReportMetric(float64(live), "live/op")
				})
			}
			// Lower bound for a List that skips deleted keys (a live-only
			// manifest, ADR 0001's wording): one Get per pod live at the
			// dataset end. Only meaningful for at=latest.
			if at.name != "latest" {
				continue
			}
			var keys []model.ObjectKey
			for _, k := range d.live {
				if k.Resource == "v1/pods" && (scope.ns == "" || k.Namespace == scope.ns) {
					keys = append(keys, k)
				}
			}
			b.Run(fmt.Sprintf("at=%s/scope=%s/path=live-gets", at.name, scope.name), func(b *testing.B) {
				for b.Loop() {
					for _, k := range keys {
						if _, _, err := v.Get(ctx, k, d.end); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.ReportMetric(float64(len(keys)), "records/op")
			})
		}
	}
}

func copyDir(dst, src string) error {
	return filepath.WalkDir(src, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if de.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		in, err := os.Open(p) //nolint:gosec // benchmark copies its own dataset
		if err != nil {
			return err
		}
		defer func() { _ = in.Close() }()
		out, err := os.Create(target) //nolint:gosec // benchmark copies its own dataset
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

// BenchmarkCompact drops the first day of the 7-day dataset (the daily
// retention step at 7-day retention).
func BenchmarkCompact(b *testing.B) {
	d := benchDataset(b)
	ctx := context.Background()
	for b.Loop() {
		b.StopTimer()
		dir := filepath.Join(b.TempDir(), "db")
		if err := copyDir(dir, d.dir); err != nil {
			b.Fatal(err)
		}
		s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
		if err != nil {
			b.Fatal(err)
		}
		var removed int
		v, _ := s.View(ctx)
		_ = v.Changes(ctx, store.ChangesOptions{To: benchStart.Add(24 * time.Hour)}, func(model.Record) error { removed++; return nil })
		_ = v.Close()
		b.StartTimer()
		if err := s.Compact(ctx, benchStart.Add(24*time.Hour)); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		v, _ = s.View(ctx)
		kept := 0
		_ = v.Changes(ctx, store.ChangesOptions{To: benchStart.Add(24 * time.Hour)}, func(model.Record) error { kept++; return nil })
		_ = v.Close()
		b.ReportMetric(float64(removed-kept), "records-removed")
		_ = s.Close()
		b.StartTimer()
	}
}

// BenchmarkSnapshot measures building one manifest over the full dataset.
func BenchmarkSnapshot(b *testing.B) {
	d := benchDataset(b)
	ctx := context.Background()
	b.StopTimer()
	dir := filepath.Join(b.TempDir(), "db")
	if err := copyDir(dir, d.dir); err != nil {
		b.Fatal(err)
	}
	s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	b.StartTimer()
	for b.Loop() {
		s.mu.Lock()
		s.lastManifest = time.Time{} // force a rebuild at the same S
		s.mu.Unlock()
		if err := s.Snapshot(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiskPerChange writes typical status updates of Deployments and
// Pods and reports on-disk bytes per change against raw JSON and JSON
// merge patch (initial object + one merge patch per update). Run with
// -benchtime 1x.
func BenchmarkDiskPerChange(b *testing.B) {
	const (
		nPods, nDeploys, updates = 2000, 500, 20
	)
	type obj struct {
		key  model.ObjectKey
		m    func() map[string]any
		step func(at time.Time)
		prev map[string]any
	}
	type cfg struct {
		comp  Compression
		block int
	}
	var cfgs []cfg
	for _, comp := range []Compression{CompressionZstd, CompressionSnappy, CompressionNone} {
		for _, block := range []int{4 << 10, 32 << 10, 64 << 10} {
			cfgs = append(cfgs, cfg{comp, block})
		}
	}
	for _, c := range cfgs {
		comp := c.comp
		b.Run(fmt.Sprintf("compression=%s/block=%dK", comp, c.block>>10), func(b *testing.B) {
			for b.Loop() {
				g := newSynth(11)
				var objs []*obj
				for i := range nPods {
					p := g.newPod(nsName(i%50), fmt.Sprintf("web-%05d-%s", i, g.hex(5)), fmt.Sprintf("web-%03d", i%nDeploys), benchStart)
					g.podScheduled(p, benchStart)
					g.podRunning(p, benchStart)
					rv := 0
					objs = append(objs, &obj{
						key: model.ObjectKey{Resource: "v1/pods", Namespace: nsName(i % 50), Name: p.m["metadata"].(map[string]any)["name"].(string)},
						m:   func() map[string]any { return p.m },
						step: func(at time.Time) {
							rv++
							g.podStatusUpdate(p, at)
							p.bumpRV(rv)
						},
					})
				}
				for i := range nDeploys {
					dep := g.newDeployment(nsName(i%50), fmt.Sprintf("web-%03d", i), 4, benchStart)
					rv := 0
					objs = append(objs, &obj{
						key: model.ObjectKey{Resource: "apps/v1/deployments", Namespace: nsName(i % 50), Name: fmt.Sprintf("web-%03d", i)},
						m:   func() map[string]any { return dep.m },
						step: func(at time.Time) {
							rv++
							g.deployStatusUpdate(dep, at)
							dep.bumpRV(rv)
						},
					})
				}
				b.StopTimer()
				dir := b.TempDir()
				s, err := Open(dir, Options{Logger: testLogger(), SnapshotInterval: -1, Compression: comp, BlockSize: c.block})
				if err != nil {
					b.Fatal(err)
				}
				ctx := context.Background()
				at := benchStart
				var raw, patch, changes, minRaw, maxRaw int64
				minRaw = 1 << 40
				var batch []store.Entry
				write := func(o *obj, typ model.ChangeType) {
					cur := o.m()
					body := mustJSON(cur)
					raw += int64(len(body))
					minRaw, maxRaw = min(minRaw, int64(len(body))), max(maxRaw, int64(len(body)))
					// Compare JSON-decoded forms on both sides so numbers
					// have the same Go type.
					decoded := deepCopy(cur)
					if o.prev == nil {
						patch += int64(len(body))
					} else {
						patch += int64(len(mustJSON(mergePatch(o.prev, decoded))))
					}
					o.prev = decoded
					changes++
					at = at.Add(50 * time.Millisecond)
					batch = append(batch, store.Entry{Record: &model.Record{ObservedAt: at, Key: o.key, UID: "u", ResourceVersion: strconv.FormatInt(changes, 10), Type: typ, Origin: model.OriginWatch, Object: body}})
					if len(batch) == 200 {
						if err := s.Write(ctx, batch); err != nil {
							b.Fatal(err)
						}
						batch = nil
					}
				}
				for _, o := range objs {
					write(o, model.ChangeAdded)
				}
				// Interleave updates across objects, as a cluster does.
				r := newRand(5)
				for u := range updates {
					for _, i := range r.Perm(len(objs)) {
						objs[i].step(at.Add(time.Duration(u) * time.Minute))
						write(objs[i], model.ChangeModified)
					}
				}
				if len(batch) > 0 {
					if err := s.Write(ctx, batch); err != nil {
						b.Fatal(err)
					}
				}
				if err := s.Flush(); err != nil {
					b.Fatal(err)
				}
				if err := s.CompactLSM(ctx); err != nil {
					b.Fatal(err)
				}
				// Close first so background compactions cannot delete
				// files during the walk.
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
				sst, err := sstSize(dir)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				b.ReportMetric(float64(changes), "changes")
				b.ReportMetric(float64(raw)/float64(changes), "raw-B/change")
				b.ReportMetric(float64(patch)/float64(changes), "mergepatch-B/change")
				b.ReportMetric(float64(sst)/float64(changes), "disk-B/change")
				b.ReportMetric(float64(sst)/float64(patch), "disk/mergepatch")
				b.Logf("%s: %d changes, raw JSON %d-%d B (avg %d), sst %d B", comp, changes, minRaw, maxRaw, raw/changes, sst)
			}
		})
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if ds != nil && os.Getenv("CHRONOS_BENCH_DATASET_DIR") == "" {
		_ = os.RemoveAll(ds.dir)
	}
	os.Exit(code)
}
