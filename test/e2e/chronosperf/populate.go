//go:build e2e

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// neverSchedule keeps perf Pods Pending: no node has this label, so they
// consume no CPU or memory on the kind node.
var neverSchedule = map[string]string{"chronos-perf/never-schedule": "true"}

func runPopulate(args []string) error {
	fs := flag.NewFlagSet("populate", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	prefix := fs.String("prefix", "perf", "namespace prefix")
	nns := fs.Int("namespaces", 10, "number of namespaces")
	ncm := fs.Int("configmaps", 7000, "ConfigMaps in total")
	npod := fs.Int("pods", 2000, "bare Pending Pods in total")
	ndep := fs.Int("deployments", 500, "Deployments (1 replica each: also 1 ReplicaSet + 1 Pending Pod)")
	cmBytes := fs.Int("cm-bytes", 3072, "approximate ConfigMap data size")
	workers := fs.Int("workers", 32, "concurrent creators")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	for i := range *nns {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name: perfNamespace(*prefix, i), Labels: map[string]string{labelPerf: "true"},
		}}
		if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	// The default ServiceAccount must exist before Pods can be created.
	for i := range *nns {
		ns := perfNamespace(*prefix, i)
		for {
			if _, err := cs.CoreV1().ServiceAccounts(ns).Get(ctx, "default", metav1.GetOptions{}); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	type job struct {
		kind string
		i    int
	}
	jobs := make(chan job, 1024)
	var created, existed, failed atomic.Int64
	var firstErr sync.Once
	var errMsg string
	start := time.Now()
	var wg sync.WaitGroup
	for range *workers {
		wg.Go(func() {
			for j := range jobs {
				ns := perfNamespace(*prefix, j.i%*nns)
				err := createOne(ctx, cs, j.kind, ns, j.i, *cmBytes)
				switch {
				case err == nil:
					created.Add(1)
				case apierrors.IsAlreadyExists(err):
					existed.Add(1)
				default:
					failed.Add(1)
					firstErr.Do(func() { errMsg = err.Error() })
				}
				if n := created.Load() + existed.Load(); n%1000 == 0 && n > 0 {
					fmt.Printf("  %d objects (%.0f/s)\n", n, float64(n)/time.Since(start).Seconds())
				}
			}
		})
	}
	for i := range *ncm {
		jobs <- job{"cm", i}
	}
	for i := range *npod {
		jobs <- job{"pod", i}
	}
	for i := range *ndep {
		jobs <- job{"deploy", i}
	}
	close(jobs)
	wg.Wait()
	el := time.Since(start)
	fmt.Printf("populate: created=%d existed=%d failed=%d in %s (%.0f creates/s)\n",
		created.Load(), existed.Load(), failed.Load(), el.Round(time.Millisecond), float64(created.Load())/el.Seconds())
	if failed.Load() > 0 {
		return fmt.Errorf("%d creates failed, first: %s", failed.Load(), errMsg)
	}
	return nil
}

func createOne(ctx context.Context, cs *kubernetes.Clientset, kind, ns string, i, cmBytes int) error {
	switch kind {
	case "cm":
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("cm-%05d", i), Labels: map[string]string{"app": "perf", "shard": fmt.Sprint(i % 16)}},
			Data:       map[string]string{"config.ini": configText(cmBytes)},
		}
		_, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{})
		return err
	case "pod":
		p := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%05d", i), Labels: map[string]string{"app": "perf-pod"}},
			Spec:       podSpec(),
		}
		_, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		return err
	case "deploy":
		one := int32(1)
		sel := map[string]string{"app": "perf-deploy", "idx": fmt.Sprint(i)}
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("dep-%04d", i), Labels: map[string]string{"app": "perf-deploy"}},
			Spec: appsv1.DeploymentSpec{
				Replicas: &one,
				Selector: &metav1.LabelSelector{MatchLabels: sel},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: sel},
					Spec:       podSpec(),
				},
			},
		}
		_, err := cs.AppsV1().Deployments(ns).Create(ctx, d, metav1.CreateOptions{})
		return err
	}
	return fmt.Errorf("unknown kind %q", kind)
}

func podSpec() corev1.PodSpec {
	zero := int64(0)
	return corev1.PodSpec{
		NodeSelector:                  neverSchedule,
		TerminationGracePeriodSeconds: &zero,
		Containers: []corev1.Container{{
			Name:  "pause",
			Image: "registry.k8s.io/pause:3.10",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1m"),
				corev1.ResourceMemory: resource.MustParse("4Mi"),
			}},
		}},
	}
}

// configText returns about n bytes of ini-style text with random values,
// so the store's compression sees realistic, not trivially repetitive,
// data.
func configText(n int) string {
	var b strings.Builder
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, "setting_%03d = %016x\n", i, rand.Uint64()) // #nosec G404 -- test data
	}
	return b.String()
}

func runCleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	wait := fs.Bool("wait", true, "wait until the namespaces are gone")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	ctx := context.Background()
	sel := metav1.ListOptions{LabelSelector: labelPerf + "=true"}
	nss, err := cs.CoreV1().Namespaces().List(ctx, sel)
	if err != nil {
		return err
	}
	for _, ns := range nss.Items {
		fmt.Println("deleting namespace", ns.Name)
		if err := cs.CoreV1().Namespaces().Delete(ctx, ns.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for *wait {
		l, err := cs.CoreV1().Namespaces().List(ctx, sel)
		if err != nil {
			return err
		}
		if len(l.Items) == 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	return nil
}
