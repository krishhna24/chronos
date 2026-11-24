package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/krishhna24/chronos/internal/api"
)

var update = flag.Bool("update", false, "rewrite golden files in testdata/golden")

// goldenCases run against the fixture timeline in fixture_test.go. Each
// golden file records exit code, stdout and stderr, so a change in any of
// them (including where a warning is printed) fails the test.
var goldenCases = []string{
	// get: tables, namespaces, selectors, output formats
	"14:01 get pods",
	"-9m get pods -A -l app=web",
	"-9m get pods -o wide",
	"14:02:30 get pods web-1 -o json",
	"14:01 get pods -o yaml -A -l app=web",
	"14:01 get pods -o name -A",
	"now get deploy",
	"now get rs -o wide",
	"now get svc",
	"now get cm",
	"now get ev -o wide",
	"now get secrets",
	"now get secret tok -o name",
	// get: deletion, gaps, coverage boundaries
	"14:03:30 get pods --show-deleted",
	"14:03:30 get pods db-0",
	"14:01 get pods nope",
	"14:05 get pods",
	"14:05 get pods --show-deleted -A",
	"14:05 get pods db-0",
	"14:05 get pod/web-1",
	"13:59 get pods",
	"13:59 get pods web-1",
	// get: input errors
	"14:11 get pods",
	"14:01 get foo",
	"14:01 get pods -l app=(",
	// diff
	"diff 14:01 14:02:30 pods/web-1",
	"diff 14:01 14:01:30 pod web-1",
	"diff 14:01 14:08 pods/web-1",
	"diff 14:01 14:05 pods/web-1",
	"diff 14:03:30 14:04:30 pods/db-0",
	"diff 14:01 14:02 foo/x",
	"diff 14:01 14:02 pods/nope",
	"diff 14:01 14:02:30 pods/web-1 --color=always",
	// history, resources, version
	"history pods/web-1",
	"history pod web-1 --since 13:50 --until 14:05",
	"history pods/nope",
	"resources",
	"version",
}

var unsafeName = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func goldenName(i int, c string) string {
	return fmt.Sprintf("%02d-%s.golden", i, strings.Trim(unsafeName.ReplaceAllString(c, "_"), "_"))
}

func render(r result) string {
	return fmt.Sprintf("exit %d\n--- stdout\n%s--- stderr\n%s", r.code, r.stdout, r.stderr)
}

func TestGolden(t *testing.T) {
	srv := newChronos(t)
	kc := writeKubeconfig(t, "https://127.0.0.1:1", "ns")
	dir := filepath.Join("testdata", "golden")
	if *update {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for i, c := range goldenCases {
		t.Run(c, func(t *testing.T) {
			got := render(runCLI(t, runOpts{}, append([]string{"--server", srv.URL, "--kubeconfig", kc}, strings.Fields(c)...)...))
			path := filepath.Join(dir, goldenName(i, c))
			if *update {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run go test ./internal/cli -update)", err)
			}
			if got != string(want) {
				t.Errorf("output mismatch for %q\n--- want\n%s\n--- got\n%s", c, want, got)
			}
		})
	}
}

// Invariants that must hold regardless of golden contents.
func TestOutputInvariants(t *testing.T) {
	srv := newChronos(t)
	kc := writeKubeconfig(t, "https://127.0.0.1:1", "ns")
	run := func(args string) result {
		return runCLI(t, runOpts{}, append([]string{"--server", srv.URL, "--kubeconfig", kc}, strings.Fields(args)...)...)
	}

	// Warnings never go to stdout, so -o json stays parseable during a gap.
	r := run("14:05 get pods -o json")
	if r.code != 0 || !strings.Contains(r.stderr, "not observing") {
		t.Fatalf("gap get: code %d stderr %q", r.code, r.stderr)
	}
	var list map[string]any
	if err := json.Unmarshal([]byte(r.stdout), &list); err != nil {
		t.Errorf("stdout is not JSON during a gap: %v", err)
	}

	// kubectl diff exit codes: 0 equal, 1 different, >1 error.
	for args, want := range map[string]int{
		"diff 14:01 14:01:30 pods/web-1": 0,
		"diff 14:01 14:02:30 pods/web-1": 1,
		"diff 14:01 14:02 pods/nope":     2,
	} {
		if got := run(args).code; got != want {
			t.Errorf("%s: exit %d, want %d", args, got, want)
		}
	}

	// No color unless asked for or on a terminal.
	if strings.Contains(run("diff 14:01 14:02:30 pods/web-1").stdout, "\x1b[") {
		t.Error("color escape codes written to a non-terminal")
	}
	r = runCLI(t, runOpts{terminal: true}, "--server", srv.URL, "--kubeconfig", kc, "diff", "14:01", "14:02:30", "pods/web-1")
	if !strings.Contains(r.stdout, "\x1b[") {
		t.Error("no color on a terminal with --color=auto")
	}
}

// fakeAPIServer stands in for kube-apiserver: it checks the bearer token
// from the kubeconfig and proxies /api/v1/namespaces/<ns>/services/<svc>:http/proxy/*
// to the Chronos handler, returning Kubernetes Status errors otherwise.
func fakeAPIServer(t *testing.T, chronos http.Handler, ns, svc string) *httptest.Server {
	t.Helper()
	prefix := ServiceProxyPath(ns, svc)
	status := func(w http.ResponseWriter, code int, reason metav1.StatusReason, kind string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure, Code: int32(code), Reason: reason, Message: string(reason),
			Details: &metav1.StatusDetails{Kind: kind},
		})
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret-token" {
			status(w, http.StatusUnauthorized, metav1.StatusReasonUnauthorized, "")
			return
		}
		if !strings.HasPrefix(r.URL.Path, prefix+"/") {
			if strings.Contains(r.URL.Path, "/services/") {
				status(w, http.StatusNotFound, metav1.StatusReasonNotFound, "services")
			} else {
				status(w, http.StatusForbidden, metav1.StatusReasonForbidden, "")
			}
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		r2.URL.RawPath = ""
		chronos.ServeHTTP(w, r2)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func writeTLSKubeconfig(t *testing.T, srv *httptest.Server, token string) string {
	t.Helper()
	p := writeKubeconfig(t, srv.URL, "ns")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	s := strings.Replace(string(b), "    server: "+srv.URL, "    server: "+srv.URL+"\n    insecure-skip-tls-verify: true", 1)
	s = strings.Replace(s, "token: secret-token", "token: "+token, 1)
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestServiceProxyPath(t *testing.T) {
	if got, want := ServiceProxyPath("chronos", "chronos"), "/api/v1/namespaces/chronos/services/chronos:http/proxy"; got != want {
		t.Errorf("ServiceProxyPath = %q, want %q", got, want)
	}
}

func TestThroughServiceProxy(t *testing.T) {
	chronos := newChronos(t).Config.Handler
	api := fakeAPIServer(t, chronos, "chronos", "chronos")

	r := runCLI(t, runOpts{}, "--kubeconfig", writeTLSKubeconfig(t, api, "secret-token"), "14:01", "get", "pods")
	if r.code != 0 || !strings.Contains(r.stdout, "web-1") {
		t.Fatalf("via proxy: code %d\nstdout %s\nstderr %s", r.code, r.stdout, r.stderr)
	}

	r = runCLI(t, runOpts{}, "--kubeconfig", writeTLSKubeconfig(t, api, "secret-token"), "--chronos-namespace", "elsewhere", "14:01", "get", "pods")
	if r.code == 0 || !strings.Contains(r.stderr, `"chronos" was not found in namespace "elsewhere"`) {
		t.Errorf("missing service: code %d stderr %q", r.code, r.stderr)
	}

	r = runCLI(t, runOpts{}, "--kubeconfig", writeTLSKubeconfig(t, api, "wrong"), "14:01", "get", "pods")
	if r.code == 0 || !strings.Contains(r.stderr, "Unauthorized") {
		t.Errorf("bad credentials: code %d stderr %q", r.code, r.stderr)
	}

	r = runCLI(t, runOpts{env: map[string]string{"CHRONOS_NAMESPACE": "Bad_NS"}}, "--kubeconfig", writeTLSKubeconfig(t, api, "secret-token"), "14:01", "get", "pods")
	if r.code == 0 || !strings.Contains(r.stderr, "invalid --chronos-namespace") {
		t.Errorf("invalid namespace: code %d stderr %q", r.code, r.stderr)
	}
}

// TestListFollowsContinue serves /list in pages of one item and checks the
// CLI asks for every page at the same instant and prints all items.
func TestListFollowsContinue(t *testing.T) {
	chronos := newChronos(t).Config.Handler
	var pages []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/list" {
			chronos.ServeHTTP(w, r)
			return
		}
		rec := httptest.NewRecorder()
		r2 := r.Clone(r.Context())
		q := r2.URL.Query()
		q.Del("continue")
		r2.URL.RawQuery = q.Encode()
		chronos.ServeHTTP(rec, r2)
		var full api.ListResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &full); err != nil {
			t.Fatal(err)
		}
		i := 0
		if c := r.URL.Query().Get("continue"); c != "" {
			if _, err := fmt.Sscanf(c, "page-%d", &i); err != nil {
				t.Errorf("bad continue token %q", c)
			}
		}
		pages = append(pages, r.URL.Query().Get("at"))
		page := full
		page.Items = full.Items[i : i+1]
		if i+1 < len(full.Items) {
			page.Continue = fmt.Sprintf("page-%d", i+1)
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	t.Cleanup(srv.Close)
	r := runCLI(t, runOpts{}, "--server", srv.URL, "--kubeconfig", writeKubeconfig(t, "https://127.0.0.1:1", "ns"), "14:01", "get", "pods", "-A", "-o", "name")
	if r.code != 0 || r.stdout != "pod/db-0\npod/web-1\npod/web-2\n" {
		t.Fatalf("paged get: code %d stdout %q stderr %q", r.code, r.stdout, r.stderr)
	}
	if len(pages) != 3 || pages[0] != pages[1] || pages[1] != pages[2] {
		t.Errorf("pages requested at %v, want 3 pages at one instant", pages)
	}
}
