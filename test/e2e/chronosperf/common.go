//go:build e2e

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/krishhna24/chronos/internal/api"
)

// Annotation keys written by storm/burst. The timestamp is the client's
// clock just before the request was sent.
const (
	annTS  = "perf.chronos.dev/ts"
	annSeq = "perf.chronos.dev/seq"
	// labelPerf marks every namespace this tool creates.
	labelPerf = "chronos-perf"
)

type kubeFlags struct {
	kubeconfig string
	context    string
	qps        float64
	burst      int
}

func (k *kubeFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&k.kubeconfig, "kubeconfig", "", "kubeconfig path (required)")
	fs.StringVar(&k.context, "context", "", "kubeconfig context (required)")
	fs.Float64Var(&k.qps, "qps", 200, "client QPS limit")
	fs.IntVar(&k.burst, "burst", 400, "client burst")
}

func (k *kubeFlags) client() (*kubernetes.Clientset, error) {
	if k.kubeconfig == "" || k.context == "" {
		return nil, fmt.Errorf("-kubeconfig and -context are required")
	}
	rc, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: k.kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: k.context},
	).ClientConfig()
	if err != nil {
		return nil, err
	}
	rc.QPS, rc.Burst = float32(k.qps), k.burst
	rc.UserAgent = "chronosperf"
	return kubernetes.NewForConfig(rc)
}

// chronosClient calls the Chronos HTTP API directly (local process or a
// port-forward).
type chronosClient struct {
	base string
	hc   *http.Client
}

func newChronosClient(base string) *chronosClient {
	return &chronosClient{
		base: strings.TrimSuffix(base, "/"),
		hc: &http.Client{
			Timeout:   60 * time.Second,
			Transport: &http.Transport{MaxIdleConnsPerHost: 64},
		},
	}
}

// get decodes the JSON response of path?q into out and returns the body
// size. Non-2xx responses are errors carrying the status code.
func (c *chronosClient) get(ctx context.Context, path string, q url.Values, out any) (int, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode/100 != 2 {
		return len(body), &httpError{status: resp.StatusCode, body: string(body)}
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return len(body), fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return len(body), nil
}

type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.status, e.body) }

func (c *chronosClient) resources(ctx context.Context) (api.ResourcesResponse, error) {
	var r api.ResourcesResponse
	_, err := c.get(ctx, "/api/v1/resources", nil, &r)
	return r, err
}

// history returns every version of key (up to the server's 10000 cap).
func (c *chronosClient) history(ctx context.Context, resource, ns, name string) (api.HistoryResponse, error) {
	var h api.HistoryResponse
	_, err := c.get(ctx, "/api/v1/history", url.Values{
		"resource": {resource}, "namespace": {ns}, "name": {name}, "limit": {"10000"},
	}, &h)
	return h, err
}

// coverageOf returns the coverage intervals of resource id.
func coverageOf(rr api.ResourcesResponse, id string) []api.Interval {
	for _, r := range rr.Resources {
		if r.ID == id {
			return r.Coverage
		}
	}
	return nil
}

// observedThroughout reports whether [from, to] lies inside one coverage
// interval.
func observedThroughout(iv []api.Interval, from, to time.Time) bool {
	for _, i := range iv {
		if i.Start.After(from) {
			continue
		}
		if i.End == nil || i.End.After(to) {
			return true
		}
	}
	return false
}

// stats summarizes durations.
type stats struct {
	n                            int
	min, p50, p90, p95, p99, max time.Duration
	mean                         time.Duration
}

func summarize(d []time.Duration) stats {
	if len(d) == 0 {
		return stats{}
	}
	s := slices.Clone(d)
	slices.Sort(s)
	var sum time.Duration
	for _, v := range s {
		sum += v
	}
	pct := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	return stats{
		n: len(s), min: s[0], max: s[len(s)-1], mean: sum / time.Duration(len(s)),
		p50: pct(0.50), p90: pct(0.90), p95: pct(0.95), p99: pct(0.99),
	}
}

func (s stats) String() string {
	r := func(d time.Duration) string { return d.Round(10 * time.Microsecond).String() }
	return fmt.Sprintf("n=%d min=%s p50=%s p90=%s p95=%s p99=%s max=%s mean=%s",
		s.n, r(s.min), r(s.p50), r(s.p90), r(s.p95), r(s.p99), r(s.max), r(s.mean))
}

func perfNamespace(prefix string, i int) string { return fmt.Sprintf("%s-%d", prefix, i) }

// createFile creates path for writing (0600, truncating).
func createFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- operator-supplied output path
}
