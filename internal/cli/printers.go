package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/duration"

	"github.com/krishhna24/chronos/internal/api"
)

// newTabWriter matches kubectl's table layout.
func newTabWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 6, 4, 3, ' ', 0)
}

// tableDef describes the kubectl-style columns of one resource type.
type tableDef struct {
	// named: the first column is NAME, taken from the object key (so it is
	// known even for deleted rows that carry no object).
	named       bool
	headers     []string
	wideHeaders []string
	// cells renders the columns after NAME (all columns when !named) of
	// an object observed at the query instant at.
	cells func(obj []byte, at time.Time) (cells, wide []string, err error)
}

var tableDefs = map[string]tableDef{
	"v1/pods": {named: true,
		headers:     []string{"READY", "STATUS", "RESTARTS", "AGE"},
		wideHeaders: []string{"IP", "NODE"},
		cells:       podCells},
	"apps/v1/deployments": {named: true,
		headers:     []string{"READY", "UP-TO-DATE", "AVAILABLE", "AGE"},
		wideHeaders: []string{"CONTAINERS", "IMAGES", "SELECTOR"},
		cells:       deploymentCells},
	"apps/v1/replicasets": {named: true,
		headers:     []string{"DESIRED", "CURRENT", "READY", "AGE"},
		wideHeaders: []string{"CONTAINERS", "IMAGES", "SELECTOR"},
		cells:       replicaSetCells},
	"v1/services": {named: true,
		headers:     []string{"TYPE", "CLUSTER-IP", "EXTERNAL-IP", "PORT(S)", "AGE"},
		wideHeaders: []string{"SELECTOR"},
		cells:       serviceCells},
	"v1/configmaps": {named: true,
		headers: []string{"DATA", "AGE"},
		cells:   configMapCells},
	"v1/events": {named: false,
		headers:     []string{"LAST SEEN", "TYPE", "REASON", "OBJECT", "MESSAGE"},
		wideHeaders: []string{"FIRST SEEN", "COUNT", "NAME"},
		cells:       eventCells},
}

// metadataTable serves Secrets (recorded through the metadata API, so
// TYPE and DATA are unknown) and any type the plugin does not know.
var metadataTable = tableDef{named: true, headers: []string{"AGE"}, cells: metadataCells}

func tableFor(rt api.Resource) tableDef {
	if d, ok := tableDefs[rt.ID]; ok && !rt.MetadataOnly {
		return d
	}
	return metadataTable
}

// tableOptions are the per-invocation table settings.
type tableOptions struct {
	wide        bool
	allNS       bool
	showDeleted bool
	at          time.Time
	tf          timeFmt
}

// printTable writes items as a kubectl-style table.
func printTable(w io.Writer, rt api.Resource, items []api.ObjectState, o tableOptions) error {
	def := tableFor(rt)
	var header []string
	if o.allNS && rt.Namespaced {
		header = append(header, "NAMESPACE")
	}
	if def.named {
		header = append(header, "NAME")
	}
	header = append(header, def.headers...)
	if o.wide {
		header = append(header, def.wideHeaders...)
		header = append(header, "CHRONOS")
	}
	if o.showDeleted {
		header = append(header, "DELETED")
	}

	tw := newTabWriter(w)
	_, _ = fmt.Fprintln(tw, strings.Join(header, "\t"))
	for _, it := range items {
		var row []string
		if o.allNS && rt.Namespaced {
			row = append(row, it.Key.Namespace)
		}
		if def.named {
			row = append(row, it.Key.Name)
		}
		var cells, wide []string
		if len(it.Object) > 0 {
			var err error
			cells, wide, err = def.cells(it.Object, o.at)
			if err != nil {
				return fmt.Errorf("cannot decode the recorded %s %q: %w", rt.Resource, it.Key.Name, err)
			}
		} else {
			// A deleted object: Chronos returns its delete version only.
			cells = dashes(len(def.headers))
			wide = dashes(len(def.wideHeaders))
		}
		row = append(row, cells...)
		if o.wide {
			row = append(row, wide...)
			row = append(row, chronosCell(it.Version, o.tf))
		}
		if o.showDeleted {
			row = append(row, deletedCell(it, o.tf))
		}
		_, _ = fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

func dashes(n int) []string {
	s := make([]string, n)
	for i := range s {
		s[i] = "-"
	}
	return s
}

// chronosCell is the -o wide CHRONOS column: the change the shown state
// comes from, e.g. "Modified 14:02:00 (watch)".
func chronosCell(v *api.Version, tf timeFmt) string {
	if v == nil {
		return "<none>"
	}
	return fmt.Sprintf("%s %s (%s)", v.Type, tf.short(v.ObservedAt), v.Origin)
}

func deletedCell(it api.ObjectState, tf timeFmt) string {
	if it.Version == nil || it.Version.Type != "Deleted" || len(it.Object) > 0 {
		return "-"
	}
	return tf.short(it.Version.ObservedAt)
}

// age is kubectl's AGE, measured to the query instant rather than to now.
func age(ts metav1.Time, at time.Time) string {
	if ts.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(at.Sub(ts.Time))
}

func orNone(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func decode(obj []byte, v any) error {
	if err := json.Unmarshal(obj, v); err != nil {
		// json errors can quote input; keep only the type of failure.
		return fmt.Errorf("not a valid %T", v)
	}
	return nil
}

func podCells(obj []byte, at time.Time) ([]string, []string, error) {
	var pod corev1.Pod
	if err := decode(obj, &pod); err != nil {
		return nil, nil, err
	}
	restarts, ready := 0, 0
	var lastRestart time.Time
	noteRestart := func(cs corev1.ContainerStatus) {
		restarts += int(cs.RestartCount)
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(lastRestart) {
			lastRestart = t.FinishedAt.Time
		}
	}
	reason := string(pod.Status.Phase)
	if pod.Status.Reason != "" {
		reason = pod.Status.Reason
	}

	// Mirrors kubectl's printPod: the first init container that has not
	// succeeded determines the status.
	initializing := false
	for i, c := range pod.Status.InitContainerStatuses {
		noteRestart(c)
		switch {
		case c.State.Terminated != nil && c.State.Terminated.ExitCode == 0:
			continue
		case c.State.Terminated != nil:
			switch t := c.State.Terminated; {
			case t.Reason != "":
				reason = "Init:" + t.Reason
			case t.Signal != 0:
				reason = fmt.Sprintf("Init:Signal:%d", t.Signal)
			default:
				reason = fmt.Sprintf("Init:ExitCode:%d", t.ExitCode)
			}
		case c.State.Waiting != nil && c.State.Waiting.Reason != "" && c.State.Waiting.Reason != "PodInitializing":
			reason = "Init:" + c.State.Waiting.Reason
		default:
			reason = fmt.Sprintf("Init:%d/%d", i, len(pod.Spec.InitContainers))
		}
		initializing = true
		break
	}
	if !initializing {
		restarts, lastRestart = 0, time.Time{}
		hasRunning := false
		for i := len(pod.Status.ContainerStatuses) - 1; i >= 0; i-- {
			c := pod.Status.ContainerStatuses[i]
			noteRestart(c)
			switch {
			case c.State.Waiting != nil && c.State.Waiting.Reason != "":
				reason = c.State.Waiting.Reason
			case c.State.Terminated != nil && c.State.Terminated.Reason != "":
				reason = c.State.Terminated.Reason
			case c.State.Terminated != nil && c.State.Terminated.Signal != 0:
				reason = fmt.Sprintf("Signal:%d", c.State.Terminated.Signal)
			case c.State.Terminated != nil:
				reason = fmt.Sprintf("ExitCode:%d", c.State.Terminated.ExitCode)
			case c.Ready && c.State.Running != nil:
				hasRunning = true
				ready++
			}
		}
		if reason == "Completed" && hasRunning {
			reason = "Running"
		}
	}
	if pod.DeletionTimestamp != nil {
		if pod.Status.Reason == "NodeLost" {
			reason = "Unknown"
		} else {
			reason = "Terminating"
		}
	}

	restartStr := strconv.Itoa(restarts)
	if restarts > 0 && !lastRestart.IsZero() && !lastRestart.After(at) {
		restartStr += fmt.Sprintf(" (%s ago)", duration.HumanDuration(at.Sub(lastRestart)))
	}
	cells := []string{
		fmt.Sprintf("%d/%d", ready, len(pod.Spec.Containers)),
		orNone(reason),
		restartStr,
		age(pod.CreationTimestamp, at),
	}
	wide := []string{orNone(pod.Status.PodIP), orNone(pod.Spec.NodeName)}
	return cells, wide, nil
}

func replicas(p *int32) int32 {
	if p == nil {
		return 1 // the API default
	}
	return *p
}

func podTemplateWide(spec corev1.PodSpec, sel *metav1.LabelSelector) []string {
	var names, images []string
	for _, c := range spec.Containers {
		names = append(names, c.Name)
		images = append(images, c.Image)
	}
	selector := "<none>"
	if sel != nil {
		if s, err := metav1.LabelSelectorAsSelector(sel); err == nil && !s.Empty() {
			selector = s.String()
		}
	}
	return []string{orNone(strings.Join(names, ",")), orNone(strings.Join(images, ",")), selector}
}

func deploymentCells(obj []byte, at time.Time) ([]string, []string, error) {
	var d appsv1.Deployment
	if err := decode(obj, &d); err != nil {
		return nil, nil, err
	}
	cells := []string{
		fmt.Sprintf("%d/%d", d.Status.ReadyReplicas, replicas(d.Spec.Replicas)),
		strconv.Itoa(int(d.Status.UpdatedReplicas)),
		strconv.Itoa(int(d.Status.AvailableReplicas)),
		age(d.CreationTimestamp, at),
	}
	return cells, podTemplateWide(d.Spec.Template.Spec, d.Spec.Selector), nil
}

func replicaSetCells(obj []byte, at time.Time) ([]string, []string, error) {
	var rs appsv1.ReplicaSet
	if err := decode(obj, &rs); err != nil {
		return nil, nil, err
	}
	cells := []string{
		strconv.Itoa(int(replicas(rs.Spec.Replicas))),
		strconv.Itoa(int(rs.Status.Replicas)),
		strconv.Itoa(int(rs.Status.ReadyReplicas)),
		age(rs.CreationTimestamp, at),
	}
	return cells, podTemplateWide(rs.Spec.Template.Spec, rs.Spec.Selector), nil
}

func serviceCells(obj []byte, at time.Time) ([]string, []string, error) {
	var svc corev1.Service
	if err := decode(obj, &svc); err != nil {
		return nil, nil, err
	}
	typ := svc.Spec.Type
	if typ == "" {
		typ = corev1.ServiceTypeClusterIP
	}
	clusterIP := svc.Spec.ClusterIP
	if len(svc.Spec.ClusterIPs) > 0 {
		clusterIP = strings.Join(svc.Spec.ClusterIPs, ",")
	}
	if clusterIP == "" {
		clusterIP = "<none>"
	}
	var ports []string
	for _, p := range svc.Spec.Ports {
		proto := p.Protocol
		if proto == "" {
			proto = corev1.ProtocolTCP
		}
		if p.NodePort != 0 {
			ports = append(ports, fmt.Sprintf("%d:%d/%s", p.Port, p.NodePort, proto))
		} else {
			ports = append(ports, fmt.Sprintf("%d/%s", p.Port, proto))
		}
	}
	sel := "<none>"
	if len(svc.Spec.Selector) > 0 {
		keys := make([]string, 0, len(svc.Spec.Selector))
		for k := range svc.Spec.Selector {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var kv []string
		for _, k := range keys {
			kv = append(kv, k+"="+svc.Spec.Selector[k])
		}
		sel = strings.Join(kv, ",")
	}
	cells := []string{string(typ), clusterIP, externalIP(&svc), orNone(strings.Join(ports, ",")), age(svc.CreationTimestamp, at)}
	return cells, []string{sel}, nil
}

// externalIP follows kubectl's getServiceExternalIP.
func externalIP(svc *corev1.Service) string {
	switch svc.Spec.Type {
	case corev1.ServiceTypeLoadBalancer:
		var ips []string
		for _, in := range svc.Status.LoadBalancer.Ingress {
			if in.IP != "" {
				ips = append(ips, in.IP)
			} else if in.Hostname != "" {
				ips = append(ips, in.Hostname)
			}
		}
		ips = append(ips, svc.Spec.ExternalIPs...)
		if len(ips) == 0 {
			return "<pending>"
		}
		return strings.Join(ips, ",")
	case corev1.ServiceTypeExternalName:
		return svc.Spec.ExternalName
	}
	if len(svc.Spec.ExternalIPs) > 0 {
		return strings.Join(svc.Spec.ExternalIPs, ",")
	}
	return "<none>"
}

func configMapCells(obj []byte, at time.Time) ([]string, []string, error) {
	var cm corev1.ConfigMap
	if err := decode(obj, &cm); err != nil {
		return nil, nil, err
	}
	return []string{strconv.Itoa(len(cm.Data) + len(cm.BinaryData)), age(cm.CreationTimestamp, at)}, nil, nil
}

func eventCells(obj []byte, at time.Time) ([]string, []string, error) {
	var ev corev1.Event
	if err := decode(obj, &ev); err != nil {
		return nil, nil, err
	}
	first := ev.FirstTimestamp
	if first.IsZero() {
		first = metav1.Time{Time: ev.EventTime.Time}
	}
	last := ev.LastTimestamp
	switch {
	case ev.Series != nil:
		last = metav1.Time{Time: ev.Series.LastObservedTime.Time}
	case last.IsZero():
		last = metav1.Time{Time: ev.EventTime.Time}
	}
	if first.IsZero() {
		first = ev.CreationTimestamp
	}
	if last.IsZero() {
		last = first
	}
	count := ev.Count
	if ev.Series != nil {
		count = ev.Series.Count
	}
	lastSeen := age(last, at)
	if count > 1 && !first.IsZero() {
		lastSeen = fmt.Sprintf("%s (x%d over %s)", lastSeen, count, age(first, at))
	}
	object := strings.ToLower(ev.InvolvedObject.Kind) + "/" + ev.InvolvedObject.Name
	cells := []string{lastSeen, ev.Type, ev.Reason, object, strings.TrimSpace(ev.Message)}
	wide := []string{age(first, at), strconv.Itoa(int(count)), ev.Name}
	return cells, wide, nil
}

func metadataCells(obj []byte, at time.Time) ([]string, []string, error) {
	var m metav1.PartialObjectMetadata
	if err := decode(obj, &m); err != nil {
		return nil, nil, err
	}
	return []string{age(m.CreationTimestamp, at)}, nil, nil
}

// objectName is the "-o name" form: "pod/web-1", "deployment.apps/web".
func objectName(rt api.Resource, name string) string {
	kind := strings.ToLower(rt.Kind)
	if rt.Group != "" {
		kind += "." + rt.Group
	}
	return kind + "/" + name
}
