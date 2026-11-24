package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krishhna24/chronos/internal/api"
)

// maxResponseBytes bounds how much of a response the client reads. A list
// of every pod of a large cluster is a few hundred MiB at most.
const maxResponseBytes = 512 << 20

// client talks to the Chronos /api/v1 either directly (--server) or
// through the kube-apiserver service proxy (ADR 0006).
type client struct {
	http *http.Client
	// base is the URL that "api/v1/<endpoint>" is appended to.
	base *url.URL
	// via describes the route for error messages.
	via string
	// proxy is set when requests go through the service proxy.
	proxy *proxyTarget
}

type proxyTarget struct {
	namespace, service string
}

// ServiceProxyPath is the kube-apiserver path that proxies to the "http"
// port of the Chronos Service.
func ServiceProxyPath(namespace, service string) string {
	return "/api/v1/namespaces/" + namespace + "/services/" + service + ":http/proxy"
}

// newDirectClient talks to a Chronos server URL, e.g. one reached through
// kubectl port-forward.
func newDirectClient(hc *http.Client, server string) (*client, error) {
	u, err := url.Parse(server)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid --server %q: want an http(s) URL such as http://localhost:8080", server)
	}
	u.RawQuery, u.Fragment = "", ""
	return &client{http: hc, base: u, via: u.Redacted()}, nil
}

// newProxyClient talks to Chronos through apiServer (the kubeconfig's
// cluster URL); hc must carry the user's credentials.
func newProxyClient(hc *http.Client, apiServer *url.URL, namespace, service string) *client {
	u := *apiServer
	u.Path = strings.TrimSuffix(u.Path, "/") + ServiceProxyPath(namespace, service)
	u.RawPath = ""
	return &client{
		http:  hc,
		base:  &u,
		via:   fmt.Sprintf("service %s/%s through the Kubernetes API server", namespace, service),
		proxy: &proxyTarget{namespace: namespace, service: service},
	}
}

// endpointURL returns the full URL of an API endpoint ("list", "state").
func (c *client) endpointURL(endpoint string, q url.Values) string {
	u := *c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + "/api/v1/" + endpoint
	u.RawPath = ""
	u.RawQuery = q.Encode()
	return u.String()
}

// get performs GET /api/v1/<endpoint>?q and decodes the JSON body into out.
func (c *client) get(ctx context.Context, endpoint string, q url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpointURL(endpoint, q), nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("cannot reach Chronos (%s): %w", c.via, unwrapURLError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read response from Chronos (%s): %w", c.via, err)
	}
	if len(body) > maxResponseBytes {
		return fmt.Errorf("response from Chronos exceeds %d MiB; narrow the query with -n or -l", maxResponseBytes>>20)
	}
	if resp.StatusCode != http.StatusOK {
		return c.responseError(resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		// The body may be an object; do not echo it.
		return fmt.Errorf("unexpected response from Chronos (%s): invalid JSON for %s", c.via, endpoint)
	}
	return nil
}

func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// ServerError is an api.Error returned by the Chronos server.
type ServerError struct {
	Status  int
	Code    string
	Message string
}

func (e *ServerError) Error() string {
	reason := map[string]string{
		"bad_request": "BadRequest",
		"not_found":   "NotFound",
		"internal":    "InternalError",
		"unavailable": "ServiceUnavailable",
	}[e.Code]
	if reason == "" {
		reason = http.StatusText(e.Status)
	}
	return fmt.Sprintf("Error from server (%s): %s", reason, e.Message)
}

// responseError maps a non-200 response to an error. It never includes the
// response body verbatim unless it is an api.Error or a Kubernetes Status,
// both of which are short messages written for clients.
func (c *client) responseError(status int, body []byte) error {
	var ae api.Error
	if json.Unmarshal(body, &ae) == nil && ae.Code != "" {
		se := &ServerError{Status: status, Code: ae.Code, Message: ae.Message}
		if ae.Code == "not_found" {
			se.Message += " (the Chronos server may be older than this plugin)"
		}
		return se
	}
	var st metav1.Status
	if json.Unmarshal(body, &st) == nil && st.Kind == "Status" {
		return c.kubeStatusError(status, &st)
	}
	if c.proxy == nil && status == http.StatusNotFound {
		return fmt.Errorf("no Chronos API at %s (HTTP 404); check --server", c.via)
	}
	return fmt.Errorf("unexpected HTTP %d from Chronos (%s)", status, c.via)
}

// kubeStatusError explains errors produced by the kube-apiserver itself
// (rather than by Chronos behind the proxy).
func (c *client) kubeStatusError(status int, st *metav1.Status) error {
	if c.proxy == nil {
		return fmt.Errorf("unexpected Kubernetes API response from %s: %s", c.via, st.Message)
	}
	ns, svc := c.proxy.namespace, c.proxy.service
	switch {
	case status == http.StatusUnauthorized || st.Reason == metav1.StatusReasonUnauthorized:
		return fmt.Errorf("the Kubernetes API server rejected your credentials (Unauthorized); check your kubeconfig and context")
	case status == http.StatusForbidden || st.Reason == metav1.StatusReasonForbidden:
		return fmt.Errorf("access to Chronos denied: you need the \"get\" verb on \"services/proxy\" "+
			"for service %q in namespace %q.\n"+
			"  Check with: kubectl auth can-i get services/%s --subresource=proxy -n %s\n"+
			"  Kubernetes said: %s", svc, ns, svc, ns, st.Message)
	case status == http.StatusNotFound && (st.Details == nil || st.Details.Kind == "services" || st.Details.Kind == ""):
		return fmt.Errorf("the Chronos service %q was not found in namespace %q: is Chronos installed? "+
			"Set --chronos-namespace/--chronos-service (or CHRONOS_NAMESPACE), "+
			"or --server to connect directly", svc, ns)
	case status == http.StatusServiceUnavailable:
		return fmt.Errorf("the Chronos service %s/%s is not reachable (is the Chronos pod running and ready?): %s", ns, svc, st.Message)
	}
	return fmt.Errorf("the Kubernetes API server returned HTTP %d for the Chronos service proxy: %s", status, st.Message)
}

// API calls.

func (c *client) resources(ctx context.Context) (*api.ResourcesResponse, error) {
	var r api.ResourcesResponse
	return &r, c.get(ctx, "resources", nil, &r)
}

func (c *client) state(ctx context.Context, key api.ObjectKey, at string) (*api.ObjectState, error) {
	q := keyParams(key)
	q.Set("at", at)
	var r api.ObjectState
	return &r, c.get(ctx, "state", q, &r)
}

func (c *client) list(ctx context.Context, resource, namespace, at, selector string, includeDeleted bool) (*api.ListResponse, error) {
	q := url.Values{"resource": {resource}, "at": {at}}
	if namespace != "" {
		q.Set("namespace", namespace)
	}
	if selector != "" {
		q.Set("labelSelector", selector)
	}
	if includeDeleted {
		q.Set("includeDeleted", "true")
	}
	var r api.ListResponse
	return &r, c.get(ctx, "list", q, &r)
}

func (c *client) diff(ctx context.Context, key api.ObjectKey, from, to string) (*api.DiffResponse, error) {
	q := keyParams(key)
	q.Set("from", from)
	q.Set("to", to)
	var r api.DiffResponse
	return &r, c.get(ctx, "diff", q, &r)
}

func (c *client) history(ctx context.Context, key api.ObjectKey, from, to string) (*api.HistoryResponse, error) {
	q := keyParams(key)
	if from != "" {
		q.Set("from", from)
	}
	if to != "" {
		q.Set("to", to)
	}
	var r api.HistoryResponse
	return &r, c.get(ctx, "history", q, &r)
}

func keyParams(k api.ObjectKey) url.Values {
	q := url.Values{"resource": {k.Resource}, "name": {k.Name}}
	if k.Namespace != "" {
		q.Set("namespace", k.Namespace)
	}
	return q
}
