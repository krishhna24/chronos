package server

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/query"
)

// params reads query parameters, remembering the first error so handlers
// can parse everything and check once.
type params struct {
	q   url.Values
	err error
}

func newParams(r *http.Request) *params { return &params{q: r.URL.Query()} }

func (p *params) fail(format string, args ...any) {
	if p.err == nil {
		p.err = &badRequestError{msg: fmt.Sprintf(format, args...)}
	}
}

// str returns the parameter, rejecting repeats: a repeated parameter is
// almost always a client bug, and silently picking one hides it.
func (p *params) str(name string) string {
	vs := p.q[name]
	if len(vs) > 1 {
		p.fail("parameter %q given %d times", name, len(vs))
		return ""
	}
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}

func (p *params) required(name string) string {
	v := p.str(name)
	if v == "" && p.err == nil {
		p.fail("parameter %q is required", name)
	}
	return v
}

// time parses an RFC 3339 instant (fractional seconds optional). The zero
// time means "absent"; the engine supplies the default.
func (p *params) time(name string, required bool) time.Time {
	var v string
	if required {
		v = p.required(name)
	} else {
		v = p.str(name)
	}
	if v == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		p.fail("parameter %q: %q is not an RFC 3339 time (relative times are resolved by the client)", name, v)
		return time.Time{}
	}
	return t
}

func (p *params) bool(name string) bool {
	v := p.str(name)
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail("parameter %q: %q is not a boolean", name, v)
	}
	return b
}

// limit parses a page size in [1, maxLimit]; absent yields 0, which the
// engine turns into its default.
func (p *params) limit(maxLimit int) int {
	v := p.str("limit")
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > maxLimit {
		p.fail("parameter \"limit\": %q must be an integer between 1 and %d", v, maxLimit)
		return 0
	}
	return n
}

func (p *params) seq() uint64 {
	v := p.required("seq")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		p.fail("parameter \"seq\": %q must be a positive integer", v)
	}
	return n
}

// key reads resource/namespace/name; namespace requirements depend on the
// resource type and are checked by the engine.
func (p *params) key() model.ObjectKey {
	return model.ObjectKey{
		Resource:  p.required("resource"),
		Namespace: p.str("namespace"),
		Name:      p.required("name"),
	}
}

func (s *server) resources(r *http.Request) (any, error) {
	return s.engine.Resources(r.Context())
}

func (s *server) state(r *http.Request) (any, error) {
	p := newParams(r)
	key, at := p.key(), p.time("at", false)
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.State(r.Context(), key, at)
}

func (s *server) list(r *http.Request) (any, error) {
	p := newParams(r)
	req := query.ListRequest{
		Resource:       p.required("resource"),
		Namespace:      p.str("namespace"),
		At:             p.time("at", false),
		LabelSelector:  p.str("labelSelector"),
		IncludeDeleted: p.bool("includeDeleted"),
		Limit:          p.limit(query.ListMaxLimit),
		Continue:       p.str("continue"),
	}
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.List(r.Context(), req)
}

func (s *server) diff(r *http.Request) (any, error) {
	p := newParams(r)
	key := p.key()
	from, to := p.time("from", true), p.time("to", true)
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.Diff(r.Context(), key, from, to)
}

func (s *server) history(r *http.Request) (any, error) {
	p := newParams(r)
	key := p.key()
	from, to := p.time("from", false), p.time("to", false)
	limit := p.limit(query.MaxLimit)
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.History(r.Context(), key, from, to, limit)
}

func (s *server) changes(r *http.Request) (any, error) {
	p := newParams(r)
	from, to := p.time("from", true), p.time("to", true)
	resource, ns := p.str("resource"), p.str("namespace")
	limit := p.limit(query.MaxLimit)
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.Changes(r.Context(), from, to, resource, ns, limit)
}

func (s *server) object(r *http.Request) (any, error) {
	p := newParams(r)
	key := p.key()
	seq := p.seq()
	if p.err != nil {
		return nil, p.err
	}
	return s.engine.ObjectAtSeq(r.Context(), key, seq)
}
