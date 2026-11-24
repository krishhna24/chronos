package server

import (
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"
)

const (
	// cspAPI applies to JSON and health responses: nothing may load.
	cspAPI = "default-src 'none'; frame-ancestors 'none'"
	// cspSPA allows the dashboard to load only its own same-origin assets
	// and call its own API. No inline script, no eval, no framing.
	cspSPA = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
		"font-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; " +
		"form-action 'none'; frame-ancestors 'none'"
)

// securityHeaders sets headers on every response. No CORS headers are ever
// sent, so browsers keep the API same-origin only.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cross-Origin-Resource-Policy", "same-origin")
		if isAPIPath(r.URL.Path) {
			h.Set("Content-Security-Policy", cspAPI)
		} else {
			h.Set("Content-Security-Policy", cspSPA)
		}
		next.ServeHTTP(w, r)
	})
}

func isAPIPath(p string) bool {
	return p == "/api" || strings.HasPrefix(p, "/api/") || p == "/healthz" || p == "/readyz"
}

// recoverPanics turns a handler panic into a 500 with a generic body. The
// panic value and stack go to the log only.
func (s *server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v) // deliberate abort: let net/http handle it
			}
			s.log.LogAttrs(r.Context(), slog.LevelError, "panic in handler",
				slog.String("path", r.URL.Path),
				slog.Any("panic", v),
				slog.String("stack", string(debug.Stack())))
			if rec, ok := w.(*statusRecorder); ok && rec.wroteHeader {
				return // too late to change the response
			}
			writeError(w, http.StatusInternalServerError, "internal", "internal error")
		}()
		next.ServeHTTP(w, r)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// accessLog logs one line per request. It deliberately omits the query
// string and bodies: responses carry object contents, and names in the
// query are left out to keep logs free of anything a user asked about.
func (s *server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.LogAttrs(r.Context(), slog.LevelInfo, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("route", r.Pattern),
			slog.Int("status", rec.status),
			slog.Int64("bytes", rec.bytes),
			slog.Duration("duration", time.Since(start)),
			slog.String("remote", r.RemoteAddr))
	})
}
