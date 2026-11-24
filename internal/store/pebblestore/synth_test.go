package pebblestore

// Synthetic Kubernetes data for benchmarks: realistic Pod and Deployment
// JSON (2-8 KB, as normalized by the collector: no managedFields, no
// last-applied annotation), typical status updates, and a churn
// simulation of a cluster with a fixed number of live objects.

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type synth struct {
	r *rand.Rand
}

func newSynth(seed uint64) *synth { return &synth{r: newRand(seed)} }

func (g *synth) hex(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = digits[g.r.IntN(16)]
	}
	return string(b)
}

func (g *synth) uid() string {
	return g.hex(8) + "-" + g.hex(4) + "-" + g.hex(4) + "-" + g.hex(4) + "-" + g.hex(12)
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func (g *synth) container(name, image string, env int) map[string]any {
	var envs []any
	for i := range env {
		envs = append(envs, map[string]any{"name": fmt.Sprintf("%s_SETTING_%d", strings.ToUpper(name), i), "value": fmt.Sprintf("value-%s-%d", g.hex(6), i)})
	}
	return map[string]any{
		"name":            name,
		"image":           image,
		"imagePullPolicy": "IfNotPresent",
		"args":            []any{"--port=8080", "--log-level=info", "--metrics-addr=:9090", "--config=/etc/" + name + "/config.yaml"},
		"ports":           []any{map[string]any{"name": "http", "containerPort": 8080, "protocol": "TCP"}, map[string]any{"name": "metrics", "containerPort": 9090, "protocol": "TCP"}},
		"env":             envs,
		"resources": map[string]any{
			"limits":   map[string]any{"cpu": "500m", "memory": "512Mi"},
			"requests": map[string]any{"cpu": "100m", "memory": "128Mi"},
		},
		"volumeMounts": []any{
			map[string]any{"name": "config", "mountPath": "/etc/" + name},
			map[string]any{"name": "kube-api-access-" + g.hex(5), "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true},
		},
		"livenessProbe":            map[string]any{"httpGet": map[string]any{"path": "/healthz", "port": 8080, "scheme": "HTTP"}, "initialDelaySeconds": 10, "periodSeconds": 10, "timeoutSeconds": 1, "successThreshold": 1, "failureThreshold": 3},
		"readinessProbe":           map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": 8080, "scheme": "HTTP"}, "periodSeconds": 5, "timeoutSeconds": 1, "successThreshold": 1, "failureThreshold": 3},
		"terminationMessagePath":   "/dev/termination-log",
		"terminationMessagePolicy": "File",
		"securityContext":          map[string]any{"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true, "runAsNonRoot": true, "capabilities": map[string]any{"drop": []any{"ALL"}}},
	}
}

// podObj is a mutable pod so status updates can be applied and diffed.
type podObj struct {
	m       map[string]any
	rv      int
	restart int
	cids    map[string]string // container IDs; new ones on restart
}

func (g *synth) newPod(ns, name, app string, created time.Time) *podObj {
	labels := map[string]any{
		"app.kubernetes.io/name": app, "app.kubernetes.io/instance": app + "-prod",
		"app.kubernetes.io/component": "server", "app.kubernetes.io/part-of": "platform",
		"pod-template-hash": g.hex(10), "team": ns,
	}
	m := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": name, "namespace": ns, "uid": g.uid(), "resourceVersion": "1",
			"creationTimestamp": ts(created), "generateName": app + "-",
			"labels": labels,
			"annotations": map[string]any{
				"prometheus.io/scrape": "true", "prometheus.io/port": "9090",
				"kubectl.kubernetes.io/restartedAt": ts(created.Add(-72 * time.Hour)),
			},
			"ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": app + "-" + g.hex(10), "uid": g.uid(), "controller": true, "blockOwnerDeletion": true}},
		},
		"spec": map[string]any{
			"containers":                    []any{g.container(app, "registry.example.com/"+app+":v1."+g.hex(2), 6), g.container("sidecar", "registry.example.com/envoy:v1.31.2", 3)},
			"volumes":                       []any{map[string]any{"name": "config", "configMap": map[string]any{"name": app + "-config", "defaultMode": 420}}, map[string]any{"name": "kube-api-access", "projected": map[string]any{"defaultMode": 420, "sources": []any{map[string]any{"serviceAccountToken": map[string]any{"expirationSeconds": 3607, "path": "token"}}}}}},
			"restartPolicy":                 "Always",
			"terminationGracePeriodSeconds": 30,
			"dnsPolicy":                     "ClusterFirst",
			"serviceAccountName":            app,
			"schedulerName":                 "default-scheduler",
			"tolerations":                   []any{map[string]any{"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300}, map[string]any{"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300}},
			"priority":                      0,
			"enableServiceLinks":            true,
			"preemptionPolicy":              "PreemptLowerPriority",
		},
		"status": map[string]any{"phase": "Pending", "qosClass": "Burstable"},
	}
	return &podObj{m: m, rv: 1, cids: map[string]string{}}
}

// imageDigest is stable per image, as in a real cluster.
func imageDigest(image string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(image))
	return strings.Repeat(fmt.Sprintf("%016x", h.Sum64()), 4)
}

func (p *podObj) bumpRV(base int) {
	p.rv = base
	p.m["metadata"].(map[string]any)["resourceVersion"] = fmt.Sprint(base)
}

// schedule, run, and update are the typical status transitions.
func (g *synth) podScheduled(p *podObj, at time.Time) {
	p.m["spec"].(map[string]any)["nodeName"] = fmt.Sprintf("node-%03d", g.r.IntN(200))
	st := p.m["status"].(map[string]any)
	st["conditions"] = []any{
		map[string]any{"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(at)},
	}
}

func (g *synth) podRunning(p *podObj, at time.Time) {
	st := p.m["status"].(map[string]any)
	st["phase"] = "Running"
	st["hostIP"] = fmt.Sprintf("10.0.%d.%d", g.r.IntN(256), g.r.IntN(256))
	st["podIP"] = fmt.Sprintf("10.244.%d.%d", g.r.IntN(256), g.r.IntN(256))
	st["podIPs"] = []any{map[string]any{"ip": st["podIP"]}}
	st["startTime"] = ts(at)
	st["conditions"] = []any{
		map[string]any{"type": "PodReadyToStartContainers", "status": "True", "lastTransitionTime": ts(at)},
		map[string]any{"type": "Initialized", "status": "True", "lastTransitionTime": ts(at)},
		map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(at)},
		map[string]any{"type": "ContainersReady", "status": "True", "lastTransitionTime": ts(at)},
		map[string]any{"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(at)},
	}
	g.setContainerStatuses(p, at, true, false)
}

func (g *synth) setContainerStatuses(p *podObj, at time.Time, ready, restarted bool) {
	st := p.m["status"].(map[string]any)
	var cs []any
	for _, c := range p.m["spec"].(map[string]any)["containers"].([]any) {
		c := c.(map[string]any)
		name, image := c["name"].(string), c["image"].(string)
		if _, ok := p.cids[name]; !ok || restarted {
			p.cids[name] = "containerd://" + g.hex(64)
		}
		cs = append(cs, map[string]any{
			"name": name, "image": image, "imageID": image + "@sha256:" + imageDigest(image),
			"containerID": p.cids[name], "ready": ready, "started": ready, "restartCount": p.restart,
			"state": map[string]any{"running": map[string]any{"startedAt": ts(at)}},
		})
	}
	st["containerStatuses"] = cs
}

// podStatusUpdate is a typical steady-state update: a container restart
// or a readiness flap.
func (g *synth) podStatusUpdate(p *podObj, at time.Time) {
	st := p.m["status"].(map[string]any)
	if g.r.IntN(2) == 0 {
		p.restart++
		g.setContainerStatuses(p, at, true, true)
		return
	}
	conds := st["conditions"].([]any)
	for _, c := range conds {
		c := c.(map[string]any)
		if c["type"] == "Ready" || c["type"] == "ContainersReady" {
			c["lastTransitionTime"] = ts(at)
		}
	}
}

func (g *synth) podTerminating(p *podObj, at time.Time) {
	md := p.m["metadata"].(map[string]any)
	md["deletionTimestamp"] = ts(at.Add(30 * time.Second))
	md["deletionGracePeriodSeconds"] = 30
}

type deployObj struct {
	m  map[string]any
	rv int
}

func (g *synth) newDeployment(ns, name string, replicas int, created time.Time) *deployObj {
	m := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name": name, "namespace": ns, "uid": g.uid(), "resourceVersion": "1", "generation": 3,
			"creationTimestamp": ts(created),
			"labels":            map[string]any{"app.kubernetes.io/name": name, "app.kubernetes.io/part-of": "platform", "team": ns},
			"annotations":       map[string]any{"deployment.kubernetes.io/revision": "3"},
		},
		"spec": map[string]any{
			"replicas":                replicas,
			"revisionHistoryLimit":    10,
			"progressDeadlineSeconds": 600,
			"selector":                map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": name}},
			"strategy":                map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": "25%", "maxUnavailable": "25%"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app.kubernetes.io/name": name, "team": ns}, "annotations": map[string]any{"prometheus.io/scrape": "true"}},
				"spec": map[string]any{
					"containers":                    []any{g.container(name, "registry.example.com/"+name+":v1."+g.hex(2), 6), g.container("sidecar", "registry.example.com/envoy:v1.31.2", 3)},
					"restartPolicy":                 "Always",
					"terminationGracePeriodSeconds": 30,
					"serviceAccountName":            name,
				},
			},
		},
		"status": map[string]any{
			"observedGeneration": 3, "replicas": replicas, "updatedReplicas": replicas, "readyReplicas": replicas, "availableReplicas": replicas,
			"conditions": []any{
				map[string]any{"type": "Available", "status": "True", "reason": "MinimumReplicasAvailable", "message": "Deployment has minimum availability.", "lastUpdateTime": ts(created), "lastTransitionTime": ts(created)},
				map[string]any{"type": "Progressing", "status": "True", "reason": "NewReplicaSetAvailable", "message": fmt.Sprintf("ReplicaSet %q has successfully progressed.", name+"-"+g.hex(10)), "lastUpdateTime": ts(created), "lastTransitionTime": ts(created)},
			},
		},
	}
	return &deployObj{m: m, rv: 1}
}

func (d *deployObj) bumpRV(base int) {
	d.rv = base
	d.m["metadata"].(map[string]any)["resourceVersion"] = fmt.Sprint(base)
}

// deployStatusUpdate: replicas dip while a pod is replaced, conditions'
// lastUpdateTime moves.
func (g *synth) deployStatusUpdate(d *deployObj, at time.Time) {
	st := d.m["status"].(map[string]any)
	replicas := d.m["spec"].(map[string]any)["replicas"].(int)
	ready := replicas
	if g.r.IntN(2) == 0 && replicas > 0 {
		ready = replicas - 1
	}
	st["readyReplicas"], st["availableReplicas"] = ready, ready
	for _, c := range st["conditions"].([]any) {
		c.(map[string]any)["lastUpdateTime"] = ts(at)
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// deepCopy clones a JSON value via a round trip (good enough for tests).
func deepCopy(m map[string]any) map[string]any {
	var out map[string]any
	if err := json.Unmarshal(mustJSON(m), &out); err != nil {
		panic(err)
	}
	return out
}

// mergePatch returns the RFC 7386 JSON merge patch that turns a into b.
func mergePatch(a, b map[string]any) map[string]any {
	p := map[string]any{}
	for k, bv := range b {
		av, ok := a[k]
		if !ok {
			p[k] = bv
			continue
		}
		am, aok := av.(map[string]any)
		bm, bok := bv.(map[string]any)
		if aok && bok {
			if sub := mergePatch(am, bm); len(sub) > 0 {
				p[k] = sub
			}
			continue
		}
		if !reflect.DeepEqual(av, bv) {
			p[k] = bv
		}
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			p[k] = nil
		}
	}
	return p
}

// churnEvent is one change in the churn simulation.
type churnEvent struct {
	at   time.Time
	slot int // pod slot, or -1-deploymentIndex for a deployment update
	kind int // podCreate, podSchedule, podRun, podUpdate, podTerminate, podDelete, deployUpdate
	inst int // pod instance number within the slot
}

const (
	evPodCreate = iota
	evPodSchedule
	evPodRun
	evPodUpdate
	evPodTerminate
	evPodDelete
	evDeployUpdate
)

// churnPlan describes the 7-day cluster simulation used by the List/Get
// benchmarks.
type churnPlan struct {
	namespaces   int
	deployments  int
	pods         int // live pod slots
	days         int
	meanLifetime time.Duration
}

func (c churnPlan) String() string {
	return fmt.Sprintf("%d namespaces, %d deployments, %d live pods, %d days, mean pod lifetime %s",
		c.namespaces, c.deployments, c.pods, c.days, c.meanLifetime)
}

// events generates the churn schedule sorted by time.
func (c churnPlan) events(g *synth, start time.Time) []churnEvent {
	end := start.Add(time.Duration(c.days) * 24 * time.Hour)
	var evs []churnEvent
	for d := range c.deployments {
		evs = append(evs, churnEvent{at: start, slot: -1 - d, kind: evPodCreate})
	}
	for slot := range c.pods {
		// Initial pods have been running for a while (baseline at start).
		born := start
		for inst := 0; born.Before(end); inst++ {
			life := time.Duration(g.r.ExpFloat64() * float64(c.meanLifetime))
			life = max(life, 2*time.Minute)
			death := born.Add(life)
			evs = append(evs,
				churnEvent{at: born, slot: slot, kind: evPodCreate, inst: inst},
				churnEvent{at: born.Add(time.Second), slot: slot, kind: evPodSchedule, inst: inst},
				churnEvent{at: born.Add(8 * time.Second), slot: slot, kind: evPodRun, inst: inst})
			for range g.r.IntN(3) {
				if u := born.Add(time.Duration(g.r.Int64N(int64(life)))); u.After(born.Add(10*time.Second)) && u.Before(death) {
					evs = append(evs, churnEvent{at: u, slot: slot, kind: evPodUpdate, inst: inst})
				}
			}
			if death.Before(end) {
				evs = append(evs,
					churnEvent{at: death, slot: slot, kind: evPodTerminate, inst: inst},
					churnEvent{at: death.Add(30 * time.Second), slot: slot, kind: evPodDelete, inst: inst})
				if g.r.IntN(3) == 0 {
					evs = append(evs, churnEvent{at: death.Add(31 * time.Second), slot: -1 - slot%c.deployments, kind: evDeployUpdate})
				}
			}
			born = death.Add(31 * time.Second)
		}
	}
	slices.SortStableFunc(evs, func(a, b churnEvent) int { return a.at.Compare(b.at) })
	return evs
}

func TestMergePatchHelper(t *testing.T) {
	a := map[string]any{"a": 1.0, "o": map[string]any{"x": "1", "y": "2"}, "gone": true}
	b := map[string]any{"a": 1.0, "o": map[string]any{"x": "1", "y": "3"}, "new": []any{1.0}}
	got := string(mustJSON(mergePatch(a, b)))
	if want := `{"gone":null,"new":[1],"o":{"y":"3"}}`; got != want {
		t.Errorf("mergePatch = %s, want %s", got, want)
	}
}
