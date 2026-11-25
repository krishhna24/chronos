//go:build e2e

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// waitRecorded polls Chronos until every rv in want is in the history of
// the key, or timeout. It returns the history it last saw.
func waitRecorded(ctx context.Context, c *chronosClient, resource, ns, name string, want map[string]bool, timeout time.Duration) (map[string]string, error) {
	deadline := time.Now().Add(timeout)
	for {
		h, err := c.history(ctx, resource, ns, name)
		if err != nil {
			return nil, err
		}
		got := map[string]string{} // rv -> origin
		missing := 0
		for _, v := range h.Versions {
			got[v.ResourceVersion] = v.Origin
		}
		for rv := range want {
			if _, ok := got[rv]; !ok {
				missing++
			}
		}
		if missing == 0 || time.Now().After(deadline) {
			return got, nil
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// runBurst sends n back-to-back patches to one ConfigMap (each waits for
// the previous response) and checks that every resulting resourceVersion
// is a distinct record.
func runBurst(args []string) error {
	fs := flag.NewFlagSet("burst", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	server := fs.String("server", "http://127.0.0.1:18080", "Chronos base URL")
	ns := fs.String("namespace", "perf-burst", "namespace (created, labelled for cleanup)")
	name := fs.String("name", "burst", "ConfigMap name")
	n := fs.Int("n", 100, "updates")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err := ensureNamespace(ctx, kf, *ns); err != nil {
		return err
	}
	_, err = cs.CoreV1().ConfigMaps(*ns).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: *name}, Data: map[string]string{"k": "v"},
	}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	want := map[string]bool{}
	start := time.Now()
	for i := range *n {
		w := patchAnnotations(ctx, cs, resConfigMaps, *ns, *name, int64(i))
		if w.err != "" {
			return fmt.Errorf("patch %d: %s", i, w.err)
		}
		want[w.rv] = true
	}
	el := time.Since(start)
	c := newChronosClient(*server)
	got, err := waitRecorded(ctx, c, resConfigMaps, *ns, *name, want, 15*time.Second)
	if err != nil {
		return err
	}
	recorded, byOrigin := 0, map[string]int{}
	for rv := range want {
		if o, ok := got[rv]; ok {
			recorded++
			byOrigin[o]++
		}
	}
	fmt.Printf("burst: %d updates in %s (%.0f/s); %d distinct resourceVersions; recorded %d/%d %v\n",
		*n, el.Round(time.Millisecond), float64(*n)/el.Seconds(), len(want), recorded, len(want), byOrigin)
	if recorded != len(want) {
		return fmt.Errorf("%d of %d updates not recorded", len(want)-recorded, len(want))
	}
	return nil
}

// runRecreate deletes and recreates one ConfigMap n times as fast as the
// apiserver allows, then checks the history alternates Added/Deleted with
// a new UID per instance, and that diff across the run reports recreated.
func runRecreate(args []string) error {
	fs := flag.NewFlagSet("recreate", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	server := fs.String("server", "http://127.0.0.1:18080", "Chronos base URL")
	ns := fs.String("namespace", "perf-burst", "namespace")
	name := fs.String("name", "phoenix", "ConfigMap name")
	n := fs.Int("n", 20, "delete+create cycles")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err := ensureNamespace(ctx, kf, *ns); err != nil {
		return err
	}
	cms := cs.CoreV1().ConfigMaps(*ns)
	_ = cms.Delete(ctx, *name, metav1.DeleteOptions{})
	time.Sleep(2 * time.Second)
	uids := map[string]bool{}
	t0 := time.Now()
	for i := range *n {
		cm, err := cms.Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: *name}, Data: map[string]string{"gen": fmt.Sprint(i)},
		}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("create %d: %w", i, err)
		}
		uids[string(cm.UID)] = true
		if i < *n-1 {
			if err := cms.Delete(ctx, *name, metav1.DeleteOptions{}); err != nil {
				return fmt.Errorf("delete %d: %w", i, err)
			}
		}
	}
	t1 := time.Now()
	c := newChronosClient(*server)
	var h struct {
		Versions []struct {
			Type, Origin, UID string
			ObservedAt        time.Time
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := c.get(ctx, "/api/v1/history", map[string][]string{
			"resource": {resConfigMaps}, "namespace": {*ns}, "name": {*name}, "from": {t0.Add(-time.Second).Format(time.RFC3339Nano)},
		}, &h); err != nil {
			return err
		}
		if len(h.Versions) >= 2**n-1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if len(h.Versions) == 0 {
		return fmt.Errorf("no history recorded for %s/%s", *ns, *name)
	}
	added, deleted, bad := 0, 0, 0
	seen := map[string]bool{}
	for i, v := range h.Versions {
		switch v.Type {
		case "Added":
			added++
			seen[v.UID] = true
		case "Deleted":
			deleted++
		}
		if i > 0 && v.Type == h.Versions[i-1].Type {
			bad++ // two Added (or two Deleted) in a row
		}
	}
	missingUID := 0
	for u := range uids {
		if !seen[u] {
			missingUID++
		}
	}
	var d struct {
		Recreated bool `json:"recreated"`
		Complete  bool `json:"complete"`
	}
	_, derr := c.get(ctx, "/api/v1/diff", map[string][]string{
		"resource": {resConfigMaps}, "namespace": {*ns}, "name": {*name},
		"from": {h.Versions[0].ObservedAt.Add(time.Nanosecond).Format(time.RFC3339Nano)}, "to": {time.Now().Format(time.RFC3339Nano)},
	}, &d)
	fmt.Printf("recreate: %d cycles in %s; history: %d versions (Added=%d Deleted=%d) adjacent-same-type=%d; UIDs created=%d missing-in-history=%d\n",
		*n, t1.Sub(t0).Round(time.Millisecond), len(h.Versions), added, deleted, bad, len(uids), missingUID)
	fmt.Printf("recreate: diff first..now recreated=%v complete=%v err=%v\n", d.Recreated, d.Complete, derr)
	if bad > 0 || missingUID > 0 || !d.Recreated {
		return fmt.Errorf("history does not reflect every instance")
	}
	return nil
}

func ensureNamespace(ctx context.Context, kf kubeFlags, ns string) error {
	cs, err := kf.client()
	if err != nil {
		return err
	}
	_, err = cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: ns, Labels: map[string]string{labelPerf: "true"},
	}}, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}
