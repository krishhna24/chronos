package query

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// List returns the objects of resource (in namespace; empty: all) at an
// instant (zero: now).
//
//   - observed: live objects are "present"; with includeDeleted, keys whose
//     latest record is a delete are added as "absent".
//   - gap: live objects (last known state) are "unknown"; with
//     includeDeleted, deleted keys are added as "unknown" with their delete
//     version and no object. ADR 0004 forbids "absent" outside observed
//     coverage, which takes precedence over docs/api.md here.
//   - unobserved / expired: no items.
//
// labelSelector uses Kubernetes syntax and is matched against
// metadata.labels of each historical object (for a delete record, the last
// known labels).
func (e *Engine) List(ctx context.Context, resource, namespace string, at time.Time, labelSelector string, includeDeleted bool) (api.ListResponse, error) {
	rt, err := lookupResource(resource)
	if err != nil {
		return api.ListResponse{}, err
	}
	if err := checkNamespace(rt, namespace, false); err != nil {
		return api.ListResponse{}, err
	}
	sel, err := labels.Parse(labelSelector)
	if err != nil {
		return api.ListResponse{}, fmt.Errorf("%w: labelSelector: %w", ErrInvalidArgument, err)
	}
	at, err = e.instant("at", at)
	if err != nil {
		return api.ListResponse{}, err
	}

	out := api.ListResponse{At: at, Items: []api.ObjectState{}}
	err = e.withView(ctx, func(v store.View) error {
		cov, err := e.CoverageAt(ctx, v, resource, at)
		if err != nil {
			return err
		}
		out.Coverage = cov
		if cov.State != api.CoverageObserved && cov.State != api.CoverageGap {
			return nil
		}
		opts := store.ListOptions{Resource: resource, Namespace: namespace, At: at}
		return v.List(ctx, opts, func(r model.Record) error {
			if r.Type == model.ChangeDeleted && !includeDeleted {
				return nil
			}
			if !sel.Empty() {
				ok, err := matches(sel, r)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			st, err := objectState(r.Key, at, cov, r, true)
			if err != nil {
				return err
			}
			out.Items = append(out.Items, st)
			return nil
		})
	})
	if err != nil {
		return api.ListResponse{}, fmt.Errorf("list %s: %w", resource, err)
	}
	// The store contract says "key order"; the API promises namespace, name.
	// Sorting here keeps the API promise independent of the key encoding.
	slices.SortStableFunc(out.Items, func(a, b api.ObjectState) int {
		if c := strings.Compare(a.Key.Namespace, b.Key.Namespace); c != 0 {
			return c
		}
		return strings.Compare(a.Key.Name, b.Key.Name)
	})
	return out, nil
}

type objectMeta struct {
	Metadata struct {
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
}

func matches(sel labels.Selector, r model.Record) (bool, error) {
	var m objectMeta
	if len(r.Object) > 0 {
		if err := json.Unmarshal(r.Object, &m); err != nil {
			return false, fmt.Errorf("%w: record %d of %s: %w", store.ErrCorrupt, r.Seq, r.Key, err)
		}
	}
	return sel.Matches(labels.Set(m.Metadata.Labels)), nil
}
