// Package server is the Chronos HTTP API (/api/v1, see docs/api.md) and,
// optionally, the embedded dashboard. It is a thin transport over
// query.Engine: it parses parameters, maps errors to api.Error bodies, and
// applies timeouts, security headers, panic recovery and access logging.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/query"
	"github.com/krishhna24/chronos/internal/store"
)

// DefaultRequestTimeout bounds the time an API request may spend in the
// engine.
const DefaultRequestTimeout = 30 * time.Second

// Options configures the handler returned by New.
type Options struct {
	// Engine answers queries. Required.
	Engine *query.Engine
	// Readiness reports whether the store is open and the API may serve.
	// nil means always ready. It is called on /readyz and before every API
	// request, so it must be cheap.
	Readiness func() error
	// Web, if non-nil, is the dashboard's static build (index.html at its
	// root), served at / with SPA fallback.
	Web fs.FS
	// Logger receives access and error logs. nil discards them.
	Logger *slog.Logger
	// RequestTimeout bounds each API request (default DefaultRequestTimeout).
	RequestTimeout time.Duration
}

type server struct {
	engine    *query.Engine
	readiness func() error
	log       *slog.Logger
	timeout   time.Duration
}

// New returns the HTTP handler for the API, health endpoints and optional
// dashboard. It panics if o.Engine is nil, which is a wiring bug.
func New(o Options) http.Handler {
	if o.Engine == nil {
		panic("server.New: Options.Engine is required")
	}
	s := &server{engine: o.Engine, readiness: o.Readiness, log: o.Logger, timeout: o.RequestTimeout}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.timeout <= 0 {
		s.timeout = DefaultRequestTimeout
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", getOnly(s.healthz))
	mux.HandleFunc("/readyz", getOnly(s.readyz))

	// Routes are registered without a method so that a known route with the
	// wrong method gets 405 (via getOnly) while an unknown route under
	// /api/ gets the JSON 404 below.
	for path, h := range map[string]apiFunc{
		"/api/v1/resources": s.resources,
		"/api/v1/state":     s.state,
		"/api/v1/list":      s.list,
		"/api/v1/diff":      s.diff,
		"/api/v1/history":   s.history,
		"/api/v1/changes":   s.changes,
		"/api/v1/object":    s.object,
	} {
		mux.HandleFunc(path, getOnly(s.api(h)))
	}
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "unknown API route "+r.URL.Path)
	})
	if o.Web != nil {
		mux.Handle("/", getOnly(spa(o.Web).ServeHTTP))
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			writeError(w, http.StatusNotFound, "not_found", "not found")
		})
	}

	return s.accessLog(securityHeaders(s.recoverPanics(mux)))
}

// NewHTTPServer returns an http.Server with conservative timeouts for h.
// The write timeout leaves room for the per-request timeout plus encoding.
func NewHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      DefaultRequestTimeout + 15*time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
}

// apiFunc produces the JSON body of a successful response.
type apiFunc func(r *http.Request) (any, error)

func getOnly(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "bad_request", "method "+r.Method+" not allowed")
			return
		}
		h(w, r)
	}
}

func (s *server) api(h apiFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.readiness != nil {
			if err := s.readiness(); err != nil {
				s.log.LogAttrs(r.Context(), slog.LevelWarn, "api request while not ready", slog.String("error", err.Error()))
				writeError(w, http.StatusServiceUnavailable, "unavailable", "store is not ready")
				return
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
		defer cancel()
		body, err := h(r.WithContext(ctx))
		if err != nil {
			s.writeEngineError(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, body)
	}
}

// badRequestError is a parameter error detected by the server itself.
type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

func (s *server) writeEngineError(w http.ResponseWriter, r *http.Request, err error) {
	var bre *badRequestError
	switch {
	case errors.As(err, &bre), errors.Is(err, query.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		s.log.LogAttrs(r.Context(), slog.LevelWarn, "api request timed out", slog.String("path", r.URL.Path))
		writeError(w, http.StatusServiceUnavailable, "unavailable", "request timed out")
	case errors.Is(err, context.Canceled):
		// The client went away; nobody will read this.
		writeError(w, http.StatusServiceUnavailable, "unavailable", "request canceled")
	case errors.Is(err, store.ErrClosed):
		writeError(w, http.StatusServiceUnavailable, "unavailable", "store is not available")
	default:
		// The error text can name internal paths or keys; log it, but return
		// a generic message.
		s.log.LogAttrs(r.Context(), slog.LevelError, "api request failed",
			slog.String("path", r.URL.Path), slog.String("error", err.Error()))
		writeError(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		// Only possible for a programming error (unencodable type) or a
		// corrupt RawMessage; the engine validates objects, so this is a bug.
		status = http.StatusInternalServerError
		buf.Reset()
		_ = json.NewEncoder(&buf).Encode(api.Error{Code: "internal", Message: "internal error"})
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(buf.Bytes())
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, api.Error{Code: code, Message: msg})
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeText(w, http.StatusOK, "ok\n")
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.readiness != nil {
		if err := s.readiness(); err != nil {
			s.log.LogAttrs(r.Context(), slog.LevelInfo, "not ready", slog.String("error", err.Error()))
			writeError(w, http.StatusServiceUnavailable, "unavailable", "not ready")
			return
		}
	}
	writeText(w, http.StatusOK, "ok\n")
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
