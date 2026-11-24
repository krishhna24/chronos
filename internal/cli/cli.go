// Package cli implements the kubectl-at plugin: "kubectl at <time> get",
// "kubectl at diff", "kubectl at history", "kubectl at resources" and
// "kubectl at version". It consumes the Chronos HTTP API (docs/api.md),
// reached through the kube-apiserver service proxy by default (ADR 0006)
// or directly with --server.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
)

// Options are the process-level inputs of Run. Zero fields get defaults
// from the real environment, so tests inject only what they control.
type Options struct {
	Out, ErrOut io.Writer
	// Now is the client clock used for relative times and AGE columns.
	Now func() time.Time
	// Location interprets times without an offset and formats output
	// times (time.Local by default; --utc overrides it).
	Location *time.Location
	// Getenv reads CHRONOS_* defaults.
	Getenv func(string) string
	// IsTerminal reports whether w is a terminal (for --color=auto).
	IsTerminal func(w io.Writer) bool
	// Version is the client version printed by "kubectl at version".
	Version string
	// HTTPClient is used for --server; nil means a default client.
	HTTPClient *http.Client
}

func (o *Options) setDefaults() {
	if o.Out == nil {
		o.Out = os.Stdout
	}
	if o.ErrOut == nil {
		o.ErrOut = os.Stderr
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Location == nil {
		o.Location = time.Local
	}
	if o.Getenv == nil {
		o.Getenv = os.Getenv
	}
	if o.IsTerminal == nil {
		o.IsTerminal = func(w io.Writer) bool {
			f, ok := w.(*os.File)
			return ok && term.IsTerminal(int(f.Fd()))
		}
	}
	if o.Version == "" {
		o.Version = "dev"
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 2 * time.Minute}
	}
}

// exitError carries a specific exit code. silent errors print nothing
// (diff's "differences found" is exit 1 without a message).
type exitError struct {
	code   int
	err    error
	silent bool
}

func (e *exitError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("exit status %d", e.code)
	}
	return e.err.Error()
}
func (e *exitError) Unwrap() error { return e.err }

func withCode(err error, code int) error {
	if err == nil {
		return nil
	}
	var ee *exitError
	if errors.As(err, &ee) {
		return err
	}
	return &exitError{code: code, err: err}
}

// verbatimError is printed as is, without the "error: " prefix (it already
// follows kubectl's "Error from server (Reason): ..." format).
type verbatimError struct{ msg string }

func (e *verbatimError) Error() string { return e.msg }

// Run executes the plugin with args (os.Args[1:]) and returns the process
// exit code.
func Run(ctx context.Context, args []string, o Options) int {
	o.setDefaults()
	a := newApp(&o)
	root := a.rootCommand()
	root.SetArgs(protectRelativeTimes(args))
	root.SetOut(o.Out)
	root.SetErr(o.ErrOut)
	err := root.ExecuteContext(ctx)
	if err == nil {
		return 0
	}
	code := 1
	var ee *exitError
	if errors.As(err, &ee) {
		code = ee.code
		if ee.silent {
			return code
		}
		if ee.err != nil {
			err = ee.err
		}
	}
	printError(o.ErrOut, err)
	return code
}

func printError(w io.Writer, err error) {
	var se *ServerError
	var ve *verbatimError
	switch {
	case errors.As(err, &se):
		_, _ = fmt.Fprintln(w, safeText(se.Error()))
	case errors.As(err, &ve):
		_, _ = fmt.Fprintln(w, safeText(ve.Error()))
	case errors.Is(err, context.Canceled):
		_, _ = fmt.Fprintln(w, "error: interrupted")
	default:
		_, _ = fmt.Fprintf(w, "error: %s\n", safeText(err.Error()))
	}
}

// relativeArgRe matches a relative time such as -20m or -1h30m. No flag of
// kubectl at starts with a digit, so such an argument is always a time.
var relativeArgRe = regexp.MustCompile(`^-[0-9]`)

// protectRelativeTimes rewrites "-20m" to the equivalent "now-20m" so the
// flag parser does not read it as a cluster of shorthand flags.
func protectRelativeTimes(args []string) []string {
	out := make([]string, len(args))
	for i, a := range args {
		if relativeArgRe.MatchString(a) {
			a = "now" + a
		}
		out[i] = a
	}
	return out
}

// app holds the global flags and lazily built dependencies of one run.
type app struct {
	o        *Options
	kube     *genericclioptions.ConfigFlags
	server   string
	chronoNS string
	chronoSv string
	utc      bool

	cl *client
}

func newApp(o *Options) *app {
	kube := genericclioptions.NewConfigFlags(true)
	// --server/-s means the Chronos URL here; the Kubernetes API server is
	// taken from the kubeconfig (or --kubeconfig/--context/--cluster).
	kube.APIServer = nil
	a := &app{o: o, kube: kube}
	return a
}

func envOr(o *Options, name, def string) string {
	if v := o.Getenv(name); v != "" {
		return v
	}
	return def
}

func (a *app) rootCommand() *cobra.Command {
	g := &getOptions{app: a}
	root := &cobra.Command{
		Use:         "kubectl-at <time> get <type> [name] [flags]",
		Annotations: map[string]string{cobra.CommandDisplayNameAnnotation: "kubectl at"},
		Short:       "Show Kubernetes resources as they were at a point in the past",
		Long: `kubectl at shows Kubernetes resources as Chronos recorded them at a point
in the past, and the differences and history between points in time.

Times may be relative (-20m, -1h30m, -2d, now), RFC 3339 (2026-11-24T14:00:00Z,
2026-11-24T15:00:00+01:00), a local date and time (2026-11-24T14:00:00,
"2026-11-24 14:00"), or a time of day today (14:00, 14:05:30). Times without an
offset are in the local time zone, or UTC with --utc.

Chronos is reached through the Kubernetes API server's service proxy using your
kubeconfig; that needs "get" on services/proxy in the Chronos namespace. Use
--server (or CHRONOS_SERVER) to connect directly, e.g. through
"kubectl port-forward -n chronos svc/chronos 8080:http".`,
		Example: `  # Pods in the current namespace 20 minutes ago
  kubectl at -20m get pods

  # One deployment at 14:00 today, as YAML
  kubectl at 14:00 get deployment/web -o yaml

  # Everything labelled app=web in all namespaces, including deleted pods
  kubectl at "2026-11-24 14:00" get pods -A -l app=web --show-deleted

  # What changed in a deployment over the last hour
  kubectl at diff -1h now deployment/web

  # Every recorded version of a pod
  kubectl at history pod/web-1`,
		Args:          cobra.ArbitraryArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			if args[0] == "get" {
				return errors.New(`missing time: usage is "kubectl at <time> get <type> [name]", e.g. "kubectl at -20m get pods"`)
			}
			if len(args) < 2 || args[1] != "get" {
				return fmt.Errorf("unknown command %q; usage is \"kubectl at <time> get <type> [name]\" "+
					"or one of: diff, history, resources, version (see kubectl at --help)", strings.Join(args, " "))
			}
			return g.run(cmd.Context(), args[0], args[2:])
		},
	}
	root.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		return fmt.Errorf("%w\nSee '%s --help' for usage", err, c.CommandPath())
	})
	root.CompletionOptions.DisableDefaultCmd = true

	pf := root.PersistentFlags()
	a.kube.AddFlags(pf)
	pf.StringVarP(&a.server, "server", "s", envOr(a.o, "CHRONOS_SERVER", ""),
		"URL of the Chronos API to use directly instead of the Kubernetes service proxy, e.g. http://localhost:8080 (env CHRONOS_SERVER)")
	pf.StringVar(&a.chronoNS, "chronos-namespace", envOr(a.o, "CHRONOS_NAMESPACE", "chronos"),
		"Namespace of the Chronos service (env CHRONOS_NAMESPACE)")
	pf.StringVar(&a.chronoSv, "chronos-service", envOr(a.o, "CHRONOS_SERVICE", "chronos"),
		"Name of the Chronos service; its port must be named \"http\" (env CHRONOS_SERVICE)")
	pf.BoolVar(&a.utc, "utc", false, "Interpret and print times without an offset in UTC instead of the local time zone")

	g.addFlags(root)
	root.AddCommand(a.diffCommand(), a.historyCommand(), a.resourcesCommand(), a.versionCommand())
	return root
}

// loc is the zone for parsing and printing times.
func (a *app) loc() *time.Location {
	if a.utc {
		return time.UTC
	}
	return a.o.Location
}

func (a *app) parser() TimeParser {
	return TimeParser{Now: a.o.Now, Location: a.loc()}
}

// namespace is the namespace for namespaced queries: -n, or the
// kubeconfig context's namespace, or "default".
func (a *app) namespace() string {
	ns, _, err := a.kube.ToRawKubeConfigLoader().Namespace()
	if err != nil || ns == "" {
		if a.kube.Namespace != nil && *a.kube.Namespace != "" {
			return *a.kube.Namespace
		}
		return "default"
	}
	return ns
}

// client returns the API client, building it on first use.
func (a *app) client() (*client, error) {
	if a.cl != nil {
		return a.cl, nil
	}
	if a.server != "" {
		c, err := newDirectClient(a.o.HTTPClient, a.server)
		if err != nil {
			return nil, err
		}
		a.cl = c
		return c, nil
	}
	for flag, v := range map[string]string{"--chronos-namespace": a.chronoNS, "--chronos-service": a.chronoSv} {
		if errs := validation.IsDNS1123Label(v); len(errs) > 0 {
			return nil, fmt.Errorf("invalid %s %q: %s", flag, v, strings.Join(errs, "; "))
		}
	}
	cfg, err := a.kube.ToRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig (or use --server to reach Chronos directly): %w", err)
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes client: %w", err)
	}
	host, _, err := rest.DefaultServerUrlFor(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig server: %w", err)
	}
	a.cl = newProxyClient(hc, host, a.chronoNS, a.chronoSv)
	return a.cl, nil
}

// warnf prints a warning to stderr; data goes to stdout only.
func (a *app) warnf(format string, args ...any) {
	_, _ = fmt.Fprintln(a.o.ErrOut, safeText("Warning: "+fmt.Sprintf(format, args...)))
}

func (a *app) versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the kubectl-at client version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(a.o.Out, "kubectl-at version %s\n", a.o.Version)
			return err
		},
	}
}
