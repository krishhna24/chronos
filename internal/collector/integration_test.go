//go:build integration

// Integration test against a real API server. Run with
//
//	CHRONOS_KUBECONFIG=/path/to/kubeconfig go test -race -tags integration ./internal/collector/ -run Integration -v
//
// The kubeconfig's current context is used unless CHRONOS_KUBE_CONTEXT is
// set. The test creates and deletes its own namespace.
package collector

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/watchlist"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/resources"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// gateLW wraps a real ListerWatcher. pause() closes the current watch
// stream and holds every List/Watch call until resume(); the first Watch
// after resume fails with 410 Expired, so the reflector has to relist
// against the real server after changes made during the pause.
type gateLW struct {
	inner       cache.ListerWatcherWithContext
	unsupported bool

	mu      sync.Mutex
	paused  chan struct{} // non-nil while paused; closed on resume
	expire  bool
	cur     watch.Interface
	listTM  atomic.Value // TypeMeta of the first list item seen (string)
	watches atomic.Int32
}

func (g *gateLW) IsWatchListSemanticsUnSupported() bool { return g.unsupported }

func (g *gateLW) hold(ctx context.Context) error {
	g.mu.Lock()
	p := g.paused
	g.mu.Unlock()
	if p == nil {
		return nil
	}
	select {
	case <-p:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gateLW) pause() {
	g.mu.Lock()
	g.paused = make(chan struct{})
	w := g.cur
	g.mu.Unlock()
	if w != nil {
		w.Stop()
	}
}

func (g *gateLW) resume() {
	g.mu.Lock()
	g.expire = true
	close(g.paused)
	g.paused = nil
	g.mu.Unlock()
}

func (g *gateLW) List(o metav1.ListOptions) (runtime.Object, error) {
	return g.ListWithContext(context.Background(), o)
}

func (g *gateLW) Watch(o metav1.ListOptions) (watch.Interface, error) {
	return g.WatchWithContext(context.Background(), o)
}

func (g *gateLW) ListWithContext(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	obj, err := g.inner.ListWithContext(ctx, o)
	if l, ok := obj.(*metav1.PartialObjectMetadataList); ok && len(l.Items) > 0 {
		g.listTM.CompareAndSwap(nil, l.Items[0].APIVersion+"/"+l.Items[0].Kind)
	}
	return obj, err
}

func (g *gateLW) WatchWithContext(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
	if err := g.hold(ctx); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.expire {
		g.expire = false
		g.mu.Unlock()
		return nil, apierrors.NewResourceExpired("integration test: forced gap")
	}
	g.mu.Unlock()
	g.watches.Add(1)
	w, err := g.inner.WatchWithContext(ctx, o)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	g.cur = w
	g.mu.Unlock()
	return w, nil
}

func TestIntegration(t *testing.T) {
	kubeconfig := os.Getenv("CHRONOS_KUBECONFIG")
	if kubeconfig == "" {
		t.Skip("CHRONOS_KUBECONFIG not set")
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: os.Getenv("CHRONOS_KUBE_CONTEXT")})
	rc, err := cc.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	md, err := metadata.NewForConfig(rc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	deployRT := mustType("apps/v1/deployments")
	secretRT := mustType("v1/secrets")
	nsGVR := schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	depGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	secGVR := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}

	ns := fmt.Sprintf("chronos-it-%d", time.Now().UnixNano())
	if _, err := dyn.Resource(nsGVR).Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dyn.Resource(nsGVR).Delete(context.Background(), ns, metav1.DeleteOptions{}) })

	const secretValue = "aW50ZWdyYXRpb24tc2VjcmV0LXZhbHVl" //nolint:gosec // fake test value
	deployment := func(name string, replicas int64, label string) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": name, "namespace": ns, "labels": map[string]any{"rev": label}},
			"spec": map[string]any{
				"replicas": replicas,
				"selector": map[string]any{"matchLabels": map[string]any{"app": name}},
				"template": map[string]any{
					"metadata": map[string]any{"labels": map[string]any{"app": name}},
					"spec": map[string]any{"containers": []any{map[string]any{
						"name": "pause", "image": "registry.k8s.io/pause:3.10",
					}}},
				},
			},
		}}
	}
	deps := dyn.Resource(depGVR).Namespace(ns)
	secs := dyn.Resource(secGVR).Namespace(ns)
	if _, err := deps.Create(ctx, deployment("web", 0, "1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := secs.Create(ctx, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Secret",
		"metadata": map[string]any{"name": "creds", "namespace": ns, "annotations": map[string]any{
			"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"` + secretValue + `"}}`,
		}},
		"data": map[string]any{"password": secretValue},
	}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// What the metadata API really returns as item type information.
	pl, err := md.Resource(secGVR).Namespace(ns).List(ctx, metav1.ListOptions{})
	if err != nil || len(pl.Items) != 1 {
		t.Fatalf("metadata list: %v (%d items)", err, len(pl.Items))
	}
	t.Logf("metadata LIST item TypeMeta: %q", pl.Items[0].APIVersion+"/"+pl.Items[0].Kind)

	// Real list/watch, built exactly as the collector builds it, wrapped
	// in a gate for the deployments.
	base := &Collector{cfg: Config{Dynamic: dyn, Metadata: md}}
	gates := map[string]*gateLW{}
	lwFor := func(rt resources.Type) (cache.ListerWatcher, error) {
		lw, err := base.listerWatcher(rt)
		if err != nil {
			return nil, err
		}
		g := &gateLW{inner: cache.ToListerWatcherWithContext(lw), unsupported: watchlist.DoesClientNotSupportWatchListSemantics(lw)}
		gates[rt.ID()] = g
		return g, nil
	}

	st := memstore.New()
	c, err := New(Config{
		Store: st, Resources: []resources.Type{deployRT, secretRT},
		Logger: testLogger(), ListerWatcherFor: lwFor, HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	depID, secID := deployRT.ID(), secretRT.ID()
	key := func(id, name string) model.ObjectKey { return model.ObjectKey{Resource: id, Namespace: ns, Name: name} }
	find := func(k model.ObjectKey, pred func(model.Record) bool) (model.Record, bool) {
		for _, r := range recordsOf(t, st, k.Resource) {
			if r.Key == k && pred(r) {
				return r, true
			}
		}
		return model.Record{}, false
	}
	waitRecord := func(what string, k model.ObjectKey, pred func(model.Record) bool) model.Record {
		t.Helper()
		var rec model.Record
		deadline := time.Now().Add(60 * time.Second)
		for {
			var ok bool
			if rec, ok = find(k, pred); ok {
				return rec
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	is := func(typ model.ChangeType, o model.Origin, contains string) func(model.Record) bool {
		return func(r model.Record) bool {
			return r.Type == typ && r.Origin == o && bytes.Contains(r.Object, []byte(contains))
		}
	}

	// Baseline.
	waitRecord("deployment baseline", key(depID, "web"), is(model.ChangeAdded, model.OriginBaseline, `"rev":"1"`))
	waitRecord("secret baseline", key(secID, "creds"), is(model.ChangeAdded, model.OriginBaseline, `"kind":"Secret"`))
	eventually(t, "both synced", func() bool {
		for _, s := range c.Status() {
			if !s.Synced {
				return false
			}
		}
		return true
	})
	if tm, _ := gates[secID].listTM.Load().(string); tm != "" {
		t.Logf("metadata LIST item TypeMeta: %q", tm)
	} else {
		t.Logf("secrets were not LISTed (streaming list path)")
	}

	// Watch: update the Deployment and the Secret.
	web, err := deps.Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	web.SetLabels(map[string]string{"rev": "2"})
	if _, err := deps.Update(ctx, web, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitRecord("deployment watch update", key(depID, "web"), is(model.ChangeModified, model.OriginWatch, `"rev":"2"`))

	sec, err := secs.Get(ctx, "creds", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rvBefore := sec.GetResourceVersion()
	_ = unstructured.SetNestedField(sec.Object, "bmV3LXZhbHVl", "data", "password")
	if _, err := secs.Update(ctx, sec, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	// ADR 0005: a data-only change is a Modified record with a new RV and
	// no visible content difference.
	waitRecord("secret data-only update", key(secID, "creds"), func(r model.Record) bool {
		return r.Type == model.ChangeModified && r.Origin == model.OriginWatch && r.ResourceVersion != rvBefore
	})
	if err := secs.Delete(ctx, "creds", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitRecord("secret delete", key(secID, "creds"), is(model.ChangeDeleted, model.OriginWatch, `"name":"creds"`))
	for _, r := range recordsOf(t, st, secID) {
		for _, bad := range []string{secretValue, "bmV3LXZhbHVl", `"data"`, "last-applied", "managedFields"} {
			if bytes.Contains(r.Object, []byte(bad)) {
				t.Fatalf("secret record %s contains %q", r.Key, bad)
			}
		}
	}

	// Provoke a relist against the real server: close the watch, change
	// things while it is down, then make the resume fail with 410.
	g := gates[depID]
	g.pause()
	eventually(t, "deployment watch down", func() bool { return !c.res[0].src.healthy() })
	web, err = deps.Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	web.SetLabels(map[string]string{"rev": "3"})
	if _, err := deps.Update(ctx, web, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := deps.Create(ctx, deployment("api", 0, "1"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	g.resume()

	resynced := waitRecord("deployment resync", key(depID, "web"), is(model.ChangeModified, model.OriginResync, `"rev":"3"`))
	waitRecord("added in gap", key(depID, "api"), is(model.ChangeAdded, model.OriginResync, `"name":"api"`))
	cov := coverageOf(t, st, depID)
	if len(cov) < 3 || cov[len(cov)-1].Type != model.CoverageStart || cov[len(cov)-2].Type != model.CoverageEnd {
		t.Fatalf("deployment coverage after relist: %+v", cov)
	}
	if end := cov[len(cov)-2]; !end.At.Before(resynced.ObservedAt) {
		t.Fatalf("End %v must precede the resync records %v", end.At, resynced.ObservedAt)
	}
	if n := len(coverageOf(t, st, secID)); n != 1 {
		t.Fatalf("secret coverage must be unaffected by the deployment gap: %d events", n)
	}

	// After the relist, watch events flow again.
	web, err = deps.Get(ctx, "web", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	web.SetLabels(map[string]string{"rev": "4"})
	if _, err := deps.Update(ctx, web, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitRecord("deployment watch after relist", key(depID, "web"), is(model.ChangeModified, model.OriginWatch, `"rev":"4"`))
	if err := deps.Delete(ctx, "api", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitRecord("deployment delete", key(depID, "api"), is(model.ChangeDeleted, model.OriginWatch, `"name":"api"`))

	eventually(t, "heartbeat", func() bool {
		hb, _ := st.Heartbeat(ctx)
		return !hb.IsZero()
	})

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	done <- nil
	for _, id := range []string{depID, secID} {
		cov := coverageOf(t, st, id)
		if last := cov[len(cov)-1]; last.Type != model.CoverageEnd || last.Reason != reasonShutdown {
			t.Fatalf("%s: last coverage %+v, want End(shutdown)", id, last)
		}
	}
	var summary []string
	for _, id := range []string{depID, secID} {
		n := 0
		for _, r := range recordsOf(t, st, id) {
			if r.Key.Namespace == ns {
				n++
			}
		}
		summary = append(summary, fmt.Sprintf("%s: %d records in test namespace, coverage %d events", id, n, len(coverageOf(t, st, id))))
	}
	t.Log(strings.Join(summary, "; "))
}
