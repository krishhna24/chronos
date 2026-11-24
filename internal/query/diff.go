package query

import (
	"context"
	"fmt"
	"time"

	// Unified diff: go-udiff is an exported copy of golang.org/x/tools'
	// internal/diff (the one gopls uses). It has zero dependencies, is
	// maintained against upstream, and its LCS search is bounded, so a
	// pathological pair of large objects degrades to a coarser diff rather
	// than quadratic time or memory. Writing Myers ourselves would mean
	// owning that bound and the hunk formatting for no gain.
	udiff "github.com/aymanbagabas/go-udiff"
	"sigs.k8s.io/yaml"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// Diff compares an object at two instants. Both sides are rendered as YAML
// with sorted keys; a side whose existence is not "present" renders as
// empty. Complete is true only if the resource was observed over every
// instant of [from, to].
func (e *Engine) Diff(ctx context.Context, key model.ObjectKey, from, to time.Time) (api.DiffResponse, error) {
	if err := checkKey(key); err != nil {
		return api.DiffResponse{}, err
	}
	if from.IsZero() || to.IsZero() {
		return api.DiffResponse{}, invalidf("from and to are required")
	}
	from, to, err := e.window(from, to)
	if err != nil {
		return api.DiffResponse{}, err
	}

	var out api.DiffResponse
	err = e.withView(ctx, func(v store.View) error {
		events, err := v.Coverage(ctx, key.Resource)
		if err != nil {
			return fmt.Errorf("coverage of %s: %w", key.Resource, err)
		}
		if out.From, err = e.stateIn(ctx, v, events, key, from); err != nil {
			return err
		}
		if out.To, err = e.stateIn(ctx, v, events, key, to); err != nil {
			return err
		}
		out.Complete = observedThroughout(events, v.Horizon(), from, to)
		return nil
	})
	if err != nil {
		return api.DiffResponse{}, err
	}

	out.Recreated = out.From.Existence == api.ExistencePresent &&
		out.To.Existence == api.ExistencePresent &&
		out.From.Version.UID != out.To.Version.UID

	a, err := renderYAML(out.From)
	if err != nil {
		return api.DiffResponse{}, err
	}
	b, err := renderYAML(out.To)
	if err != nil {
		return api.DiffResponse{}, err
	}
	out.Diff, err = unifiedDiff(from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), a, b)
	if err != nil {
		return api.DiffResponse{}, err
	}
	return out, nil
}

// window validates a [from, to] pair of user instants.
func (e *Engine) window(from, to time.Time) (time.Time, time.Time, error) {
	var err error
	if from, err = e.instant("from", from); err != nil {
		return from, to, err
	}
	if to, err = e.instant("to", to); err != nil {
		return from, to, err
	}
	if !from.Before(to) {
		return from, to, invalidf("from (%s) must be before to (%s)",
			from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano))
	}
	return from, to, nil
}

// renderYAML converts the object JSON to YAML. sigs.k8s.io/yaml decodes the
// JSON into Go maps and the YAML encoder emits map keys sorted, so the
// rendering is canonical regardless of the stored key order.
func renderYAML(st api.ObjectState) (string, error) {
	if st.Existence != api.ExistencePresent || len(st.Object) == 0 {
		return "", nil
	}
	y, err := yaml.JSONToYAML(st.Object)
	if err != nil {
		return "", fmt.Errorf("render %s as YAML: %w", st.Key.Name, err)
	}
	return string(y), nil
}

func unifiedDiff(fromLabel, toLabel, a, b string) (string, error) {
	if a == b {
		return "", nil
	}
	d, err := udiff.ToUnified(fromLabel, toLabel, a, udiff.Lines(a, b), udiff.DefaultContextLines)
	if err != nil {
		return "", fmt.Errorf("unified diff: %w", err)
	}
	return d, nil
}
