package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/server"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
)

// The fixture timeline, all on 2026-11-24 in UTC (the tests run with
// Location UTC, so local times below are UTC too):
//
//	14:00:00  baseline: pods ns/web-1 (uid w1, app=web), ns/db-0 (app=db),
//	          other/web-2 (app=web, Pending); one object of every other
//	          type in ns; coverage Start for every type
//	14:02:00  watch Modified ns/web-1 (image v2, one restart at 14:01:30)
//	14:03:00  watch Deleted ns/db-0
//	14:04:00  pods coverage End "watch expired"
//	14:06:00  resync Modified ns/web-1 (image v3), pods Start "relist"
//	14:07:00  watch Deleted ns/web-1 (uid w1)
//	14:07:30  watch Added ns/web-1 (uid w2, image v4)
//	14:10:00  now (server and client clock)
var (
	day = time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)
	now = clock(14, 10, 0)
)

func clock(h, m, s int) time.Time {
	return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second)
}

const (
	uidW1 = "0f1e2d3c-aaaa-4bbb-8ccc-000000000001"
	uidW2 = "9a8b7c6d-aaaa-4bbb-8ccc-000000000002"
)

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func meta(ns, name, uid string, created time.Time, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid), Labels: labels,
		CreationTimestamp: metav1.NewTime(created), ResourceVersion: "1"}
}

func pod(ns, name, uid string, labels map[string]string, created time.Time, image string, mutate func(*corev1.Pod)) []byte {
	p := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: meta(ns, name, uid, created, labels),
		Spec: corev1.PodSpec{NodeName: "node-a",
			Containers: []corev1.Container{{Name: "app", Image: image}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.7",
			ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Ready: true, Image: image,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}
	if mutate != nil {
		mutate(p)
	}
	return mustJSON(p)
}

type fixtureStore struct {
	t   *testing.T
	mem *memstore.Store
}

func (f fixtureStore) write(es ...store.Entry) {
	f.t.Helper()
	if err := f.mem.Write(context.Background(), es); err != nil {
		f.t.Fatalf("write fixture: %v", err)
	}
}

func rec(resource, ns, name, uid string, typ model.ChangeType, origin model.Origin, at time.Time, obj []byte) store.Entry {
	return store.Entry{Record: &model.Record{
		ObservedAt: at, Key: model.ObjectKey{Resource: resource, Namespace: ns, Name: name},
		UID: uid, ResourceVersion: "1", Type: typ, Origin: origin, Object: obj,
	}}
}

func cov(resource string, typ model.CoverageType, at time.Time, reason string) store.Entry {
	return store.Entry{Coverage: &model.CoverageEvent{At: at, Resource: resource, Type: typ, Reason: reason}}
}

func newFixtureStore(t *testing.T) *memstore.Store {
	t.Helper()
	mem := memstore.New()
	t.Cleanup(func() { _ = mem.Close() })
	f := fixtureStore{t: t, mem: mem}
	t0 := clock(14, 0, 0)
	web := map[string]string{"app": "web"}
	const (
		pods = "v1/pods"
		base = model.OriginBaseline
	)

	f.write(
		rec(pods, "ns", "web-1", uidW1, model.ChangeAdded, base, t0, pod("ns", "web-1", uidW1, web, clock(13, 0, 0), "img:v1", nil)),
		rec(pods, "ns", "db-0", "d1", model.ChangeAdded, base, t0, pod("ns", "db-0", "d1", map[string]string{"app": "db"}, clock(13, 30, 0), "db:v1", nil)),
		rec(pods, "other", "web-2", "x1", model.ChangeAdded, base, t0, pod("other", "web-2", "x1", web, clock(13, 50, 0), "img:v1", func(p *corev1.Pod) {
			p.Status.Phase = corev1.PodPending
			p.Status.ContainerStatuses[0].Ready = false
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
		})),
		cov(pods, model.CoverageStart, t0, "initial list"),
	)

	created := clock(12, 0, 0)
	dep := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: meta("ns", "web", "dep1", created, web),
		Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3),
			Selector: &metav1.LabelSelector{MatchLabels: web},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "img:v1"}}}}},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 2, UpdatedReplicas: 3, AvailableReplicas: 2, Replicas: 3},
	}
	rs := &appsv1.ReplicaSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"},
		ObjectMeta: meta("ns", "web-5d4f", "rs1", created, web),
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr.To[int32](3),
			Selector: &metav1.LabelSelector{MatchLabels: web},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "img:v1"}}}}},
		Status: appsv1.ReplicaSetStatus{Replicas: 3, ReadyReplicas: 2},
	}
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta("ns", "web", "svc1", created, web),
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ClusterIP: "10.96.0.10", ClusterIPs: []string{"10.96.0.10"},
			Selector: web, Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}}},
	}
	lb := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta("ns", "lb", "svc2", created, nil),
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer, ClusterIP: "10.96.0.11",
			Ports: []corev1.ServicePort{{Port: 443, NodePort: 30443, Protocol: corev1.ProtocolTCP}}},
		Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{Ingress: []corev1.LoadBalancerIngress{{IP: "203.0.113.5"}}}},
	}
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: meta("ns", "cfg", "cm1", clock(13, 0, 0), nil),
		Data:       map[string]string{"a": "1", "b": "2"},
	}
	ev := &corev1.Event{
		TypeMeta:       metav1.TypeMeta{APIVersion: "v1", Kind: "Event"},
		ObjectMeta:     meta("ns", "web-1.17a", "ev1", clock(13, 50, 0), nil),
		InvolvedObject: corev1.ObjectReference{Kind: "Pod", Name: "web-1", Namespace: "ns"},
		Type:           "Normal", Reason: "Pulled", Message: "Successfully pulled image",
		Count: 3, FirstTimestamp: metav1.NewTime(clock(13, 50, 0)), LastTimestamp: metav1.NewTime(clock(13, 58, 0)),
	}
	secret := &metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: meta("ns", "tok", "sec1", clock(11, 0, 0), nil),
	}
	for _, o := range []struct {
		res, name, uid string
		obj            any
	}{
		{"apps/v1/deployments", "web", "dep1", dep},
		{"apps/v1/replicasets", "web-5d4f", "rs1", rs},
		{"v1/services", "lb", "svc2", lb},
		{"v1/services", "web", "svc1", svc},
		{"v1/configmaps", "cfg", "cm1", cm},
		{"v1/events", "web-1.17a", "ev1", ev},
		{"v1/secrets", "tok", "sec1", secret},
	} {
		f.write(rec(o.res, "ns", o.name, o.uid, model.ChangeAdded, base, t0, mustJSON(o.obj)))
	}
	for _, r := range []string{"apps/v1/deployments", "apps/v1/replicasets", "v1/services", "v1/configmaps", "v1/events", "v1/secrets"} {
		f.write(cov(r, model.CoverageStart, t0, "initial list"))
	}

	f.write(rec(pods, "ns", "web-1", uidW1, model.ChangeModified, model.OriginWatch, clock(14, 2, 0),
		pod("ns", "web-1", uidW1, web, clock(13, 0, 0), "img:v2", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].RestartCount = 1
			p.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, FinishedAt: metav1.NewTime(clock(14, 1, 30))}}
		})))
	f.write(rec(pods, "ns", "db-0", "d1", model.ChangeDeleted, model.OriginWatch, clock(14, 3, 0),
		pod("ns", "db-0", "d1", map[string]string{"app": "db"}, clock(13, 30, 0), "db:v1", nil)))
	f.write(cov(pods, model.CoverageEnd, clock(14, 4, 0), "watch expired"))
	f.write(
		rec(pods, "ns", "web-1", uidW1, model.ChangeModified, model.OriginResync, clock(14, 6, 0),
			pod("ns", "web-1", uidW1, web, clock(13, 0, 0), "img:v3", nil)),
		cov(pods, model.CoverageStart, clock(14, 6, 0), "relist"),
	)
	f.write(rec(pods, "ns", "web-1", uidW1, model.ChangeDeleted, model.OriginWatch, clock(14, 7, 0),
		pod("ns", "web-1", uidW1, web, clock(13, 0, 0), "img:v3", nil)))
	f.write(rec(pods, "ns", "web-1", uidW2, model.ChangeAdded, model.OriginWatch, clock(14, 7, 30),
		pod("ns", "web-1", uidW2, web, clock(14, 7, 30), "img:v4", nil)))
	return mem
}

// newChronos serves the real Chronos API over the fixture.
func newChronos(t *testing.T) *httptest.Server {
	t.Helper()
	h := server.New(server.Options{Engine: query.New(newFixtureStore(t), testingclock.NewFakePassiveClock(now))})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// writeKubeconfig writes a kubeconfig whose current context has namespace
// ns and whose cluster is server.
func writeKubeconfig(t *testing.T, server, ns string) string {
	t.Helper()
	cfg := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: ` + server + `
users:
- name: alice
  user:
    token: secret-token
contexts:
- name: test
  context:
    cluster: test
    user: alice
    namespace: ` + ns + `
current-context: test
`
	p := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(p, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

type result struct {
	stdout, stderr string
	code           int
}

type runOpts struct {
	env      map[string]string
	terminal bool
}

// runCLI runs the plugin in-process with the fixture clock and UTC.
func runCLI(t *testing.T, ro runOpts, args ...string) result {
	t.Helper()
	var out, errOut bytes.Buffer
	o := Options{
		Out: &out, ErrOut: &errOut,
		Now:        func() time.Time { return now },
		Location:   time.UTC,
		Getenv:     func(k string) string { return ro.env[k] },
		IsTerminal: func(io.Writer) bool { return ro.terminal },
		Version:    "v1.2.3-test",
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
	code := Run(context.Background(), args, o)
	return result{stdout: out.String(), stderr: errOut.String(), code: code}
}
