//go:build e2e

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	resConfigMaps  = "v1/configmaps"
	resDeployments = "apps/v1/deployments"
)

// write is one storm update, as logged to the CSV.
type write struct {
	seq       int64
	resource  string
	namespace string
	name      string
	rv        string
	t0, t1    time.Time // client clock before send / after response
	err       string
}

var csvHeader = []string{"seq", "resource", "namespace", "name", "rv", "t0", "t1", "err"}

func (w write) row() []string {
	return []string{strconv.FormatInt(w.seq, 10), w.resource, w.namespace, w.name, w.rv,
		strconv.FormatInt(w.t0.UnixNano(), 10), strconv.FormatInt(w.t1.UnixNano(), 10), w.err}
}

func parseWrite(r []string) (write, error) {
	if len(r) != len(csvHeader) {
		return write{}, fmt.Errorf("bad row %q", r)
	}
	seq, err := strconv.ParseInt(r[0], 10, 64)
	if err != nil {
		return write{}, err
	}
	t0, err := strconv.ParseInt(r[5], 10, 64)
	if err != nil {
		return write{}, err
	}
	t1, err := strconv.ParseInt(r[6], 10, 64)
	if err != nil {
		return write{}, err
	}
	return write{seq: seq, resource: r[1], namespace: r[2], name: r[3], rv: r[4],
		t0: time.Unix(0, t0), t1: time.Unix(0, t1), err: r[7]}, nil
}

// csvLog serializes writes to a CSV file.
type csvLog struct {
	mu sync.Mutex
	f  *os.File
	w  *csv.Writer
}

func newCSVLog(path string) (*csvLog, error) {
	f, err := createFile(path)
	if err != nil {
		return nil, err
	}
	l := &csvLog{f: f, w: csv.NewWriter(f)}
	if err := l.w.Write(csvHeader); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *csvLog) add(w write) {
	l.mu.Lock()
	_ = l.w.Write(w.row())
	l.mu.Unlock()
}

func (l *csvLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Flush()
	if err := l.w.Error(); err != nil {
		return err
	}
	return l.f.Close()
}

// patchAnnotations stamps the object with the client time and a sequence
// number. Every patch changes the object, so Chronos must record each.
func patchAnnotations(ctx context.Context, cs *kubernetes.Clientset, resource, ns, name string, seq int64) write {
	w := write{seq: seq, resource: resource, namespace: ns, name: name}
	w.t0 = time.Now()
	body, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{
		annTS: w.t0.Format(time.RFC3339Nano), annSeq: strconv.FormatInt(seq, 10),
	}}})
	var meta metav1.Object
	var err error
	switch resource {
	case resConfigMaps:
		meta, err = cs.CoreV1().ConfigMaps(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	case resDeployments:
		meta, err = cs.AppsV1().Deployments(ns).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	default:
		err = fmt.Errorf("unsupported resource %s", resource)
	}
	w.t1 = time.Now()
	if err != nil {
		w.err = err.Error()
	} else {
		w.rv = meta.GetResourceVersion()
	}
	return w
}

func runStorm(args []string) error {
	fs := flag.NewFlagSet("storm", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	prefix := fs.String("prefix", "perf", "namespace prefix")
	nns := fs.Int("namespaces", 10, "namespaces populate created")
	ncm := fs.Int("configmaps", 7000, "ConfigMaps populate created")
	ndep := fs.Int("deployments", 500, "Deployments populate created")
	depFrac := fs.Float64("deploy-fraction", 0.2, "fraction of updates that patch a Deployment's metadata (no rollout)")
	rate := fs.Float64("rate", 100, "updates per second")
	dur := fs.Duration("duration", 3*time.Minute, "storm duration")
	workers := fs.Int("workers", 32, "concurrent writers")
	out := fs.String("out", "storm.csv", "CSV log of every update")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	log, err := newCSVLog(*out)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *dur)
	defer cancel()

	tokens := make(chan int64, *workers)
	var seq, skipped, failed atomic.Int64
	var lat []time.Duration
	var latMu sync.Mutex
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for s := range tokens {
				var res, ns, name string
				if *ndep > 0 && rand.Float64() < *depFrac { // #nosec G404 -- load generation
					i := rand.IntN(*ndep) // #nosec G404
					res, ns, name = resDeployments, perfNamespace(*prefix, i%*nns), fmt.Sprintf("dep-%04d", i)
				} else {
					i := rand.IntN(*ncm) // #nosec G404
					res, ns, name = resConfigMaps, perfNamespace(*prefix, i%*nns), fmt.Sprintf("cm-%05d", i)
				}
				// Not ctx: an update in flight when the storm ends still completes
				// and is logged, so the CSV matches what the apiserver applied.
				wctx, wcancel := context.WithTimeout(context.Background(), 30*time.Second)
				w := patchAnnotations(wctx, cs, res, ns, name, s)
				wcancel()
				if w.err != "" {
					failed.Add(1)
				}
				log.add(w)
				latMu.Lock()
				lat = append(lat, w.t1.Sub(w.t0))
				latMu.Unlock()
			}
		})
	}
	start := time.Now()
	interval := time.Duration(float64(time.Second) / *rate)
	t := time.NewTicker(interval)
	report := time.NewTicker(10 * time.Second)
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-report.C:
			n := seq.Load()
			fmt.Printf("  %s: %d sent (%.1f/s) skipped=%d failed=%d\n", time.Since(start).Round(time.Second), n,
				float64(n)/time.Since(start).Seconds(), skipped.Load(), failed.Load())
		case <-t.C:
			select {
			case tokens <- seq.Add(1):
			default:
				skipped.Add(1) // every worker busy: the apiserver is not keeping up
			}
		}
	}
	t.Stop()
	report.Stop()
	close(tokens)
	wg.Wait()
	el := time.Since(start)
	if err := log.close(); err != nil {
		return err
	}
	n := seq.Load()
	fmt.Printf("storm: sent=%d ok=%d failed=%d skipped=%d in %s (achieved %.1f/s, target %.0f/s)\n",
		n, n-failed.Load(), failed.Load(), skipped.Load(), el.Round(time.Millisecond), float64(n-failed.Load())/el.Seconds(), *rate)
	fmt.Printf("storm: apiserver patch latency %s\n", summarize(lat))
	return nil
}
