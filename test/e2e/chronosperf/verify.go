//go:build e2e

package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krishhna24/chronos/internal/api"
)

// runVerify compares Chronos's answer "now" with a live LIST of the same
// ConfigMaps: every live object must be present with the same UID and
// resourceVersion, and nothing else may be present. It retries for a
// while so watch delivery lag does not count as a mismatch.
func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	var kf kubeFlags
	kf.register(fs)
	server := fs.String("server", "http://127.0.0.1:18080", "Chronos base URL")
	ns := fs.String("namespace", "", "namespace (empty: all namespaces)")
	timeout := fs.Duration("timeout", 20*time.Second, "keep retrying until this long")
	allowGap := fs.Bool("allow-gap", false, "accept differences while coverage is not observed (Chronos is then saying it does not know); fail only if it claims observed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cs, err := kf.client()
	if err != nil {
		return err
	}
	c := newChronosClient(*server)
	ctx := context.Background()
	deadline := time.Now().Add(*timeout)
	for {
		live, err := cs.CoreV1().ConfigMaps(*ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		want := map[string][2]string{}
		for _, o := range live.Items {
			want[o.Namespace+"/"+o.Name] = [2]string{string(o.UID), o.ResourceVersion}
		}
		got := map[string][2]string{}
		var cov api.Coverage
		q := url.Values{"resource": {resConfigMaps}, "limit": {"5000"}, "at": {time.Now().Format(time.RFC3339Nano)}}
		if *ns != "" {
			q.Set("namespace", *ns)
		}
		for {
			var lr api.ListResponse
			if _, err := c.get(ctx, "/api/v1/list", q, &lr); err != nil {
				return err
			}
			cov = lr.Coverage
			for _, it := range lr.Items {
				if it.Version != nil {
					got[it.Key.Namespace+"/"+it.Key.Name] = [2]string{it.Version.UID, it.Version.ResourceVersion}
				}
			}
			if lr.Continue == "" {
				break
			}
			q.Set("continue", lr.Continue)
		}
		missing, extra, stale := 0, 0, 0
		var examples []string
		for k, w := range want {
			g, ok := got[k]
			switch {
			case !ok:
				missing++
				examples = append(examples, "missing "+k)
			case g != w:
				stale++
				examples = append(examples, fmt.Sprintf("stale %s chronos=%v live=%v", k, g, w))
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				extra++
				examples = append(examples, "extra "+k)
			}
		}
		if missing+extra+stale == 0 || time.Now().After(deadline) {
			fmt.Printf("verify: live=%d chronos=%d coverage=%s missing=%d extra=%d stale=%d\n",
				len(want), len(got), cov.State, missing, extra, stale)
			for i := range min(5, len(examples)) {
				fmt.Println("verify:", examples[i])
			}
			if missing+extra+stale > 0 {
				if *allowGap && cov.State != api.CoverageObserved {
					fmt.Printf("verify: differences are reported under coverage %q, not as observed: OK\n", cov.State)
					return nil
				}
				return fmt.Errorf("chronos state differs from the cluster")
			}
			return nil
		}
		time.Sleep(time.Second)
	}
}
