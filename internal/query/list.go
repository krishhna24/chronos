package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/labels"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// ListRequest selects objects for List.
type ListRequest struct {
	Resource  string    // canonical resource ID, required
	Namespace string    // empty: all namespaces
	At        time.Time // zero: now; must be explicit with Continue
	// LabelSelector uses Kubernetes syntax and is matched against
	// metadata.labels of each historical object (for a delete record, the
	// last known labels).
	LabelSelector  string
	IncludeDeleted bool
	// Limit is the page size, 1..ListMaxLimit (0: ListDefaultLimit). It
	// counts returned items, i.e. after selector and delete filtering.
	Limit int
	// Continue is the token from the previous page's ListResponse.
	Continue string
}

const (
	// ListDefaultLimit is the List page size when none is given.
	ListDefaultLimit = 500
	// ListMaxLimit is the largest List page size accepted.
	ListMaxLimit = 5000
)

// List returns one page of the objects of a resource type at an instant,
// sorted by namespace and name.
//
//   - observed: live objects are "present"; with IncludeDeleted, keys whose
//     latest record is a delete are added as "absent".
//   - gap: live objects (last known state) are "unknown"; with
//     IncludeDeleted, deleted keys are added as "unknown" with their delete
//     version and no object (ADR 0004: never "absent" outside observed).
//   - unobserved / expired: no items.
//
// When more items match, the response carries a Continue token. Each page
// is read from its own view but at the same instant, and records at or
// before an instant never change except through compaction, so the pages
// concatenate to the unpaged answer (unless compaction passes the instant
// between pages, in which case the later page reports "expired").
//
// Cost: the store resumes after the cursor (ListOptions.After), so a page
// visits O(limit) keys plus those filtered out by the selector or delete
// filtering. Objects are only decoded for selector matching and for
// returned items. The cursor is also re-checked here, so a store that
// ignored After would still return correct (if slower) pages.
func (e *Engine) List(ctx context.Context, req ListRequest) (api.ListResponse, error) {
	rt, err := lookupResource(req.Resource)
	if err != nil {
		return api.ListResponse{}, err
	}
	if err := checkNamespace(rt, req.Namespace, false); err != nil {
		return api.ListResponse{}, err
	}
	sel, err := labels.Parse(req.LabelSelector)
	if err != nil {
		return api.ListResponse{}, fmt.Errorf("%w: labelSelector: %w", ErrInvalidArgument, err)
	}
	limit := req.Limit
	switch {
	case limit == 0:
		limit = ListDefaultLimit
	case limit < 0 || limit > ListMaxLimit:
		return api.ListResponse{}, invalidf("limit must be between 1 and %d, got %d", ListMaxLimit, limit)
	}
	if req.Continue != "" && req.At.IsZero() {
		return api.ListResponse{}, invalidf("at is required with continue (use the at of the first page)")
	}
	at, err := e.instant("at", req.At)
	if err != nil {
		return api.ListResponse{}, err
	}
	var after *model.ObjectKey
	if req.Continue != "" {
		tok, err := decodeListToken(req.Continue)
		if err != nil {
			return api.ListResponse{}, err
		}
		if err := tok.check(req, at); err != nil {
			return api.ListResponse{}, err
		}
		after = &model.ObjectKey{Resource: req.Resource, Namespace: tok.AfterNamespace, Name: tok.AfterName}
	}

	out := api.ListResponse{At: at, Items: []api.ObjectState{}}
	err = e.withView(ctx, func(v store.View) error {
		cov, err := e.CoverageAt(ctx, v, req.Resource, at)
		if err != nil {
			return err
		}
		out.Coverage = cov
		if cov.State != api.CoverageObserved && cov.State != api.CoverageGap {
			return nil
		}
		var (
			prev    *model.ObjectKey
			last    model.ObjectKey
			hasMore bool
		)
		opts := store.ListOptions{Resource: req.Resource, Namespace: req.Namespace, At: at, After: after}
		err = v.List(ctx, opts, func(r model.Record) error {
			// Paging relies on the store's key order being (namespace,
			// name); fail loudly rather than return wrong pages.
			if prev != nil && compareKeys(r.Key, *prev) <= 0 {
				return fmt.Errorf("store returned %s after %s: not in (namespace, name) order", r.Key, *prev)
			}
			k := r.Key
			prev = &k
			if after != nil && compareKeys(r.Key, *after) <= 0 {
				return nil
			}
			if r.Type == model.ChangeDeleted && !req.IncludeDeleted {
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
			if len(out.Items) == limit {
				hasMore = true
				return errStop
			}
			st, err := objectState(r.Key, at, cov, r, true)
			if err != nil {
				return err
			}
			out.Items = append(out.Items, st)
			last = r.Key
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return err
		}
		if hasMore {
			out.Continue = encodeListToken(listToken{
				V: listTokenVersion, Resource: req.Resource, Namespace: req.Namespace, At: at,
				Selector: req.LabelSelector, IncludeDeleted: req.IncludeDeleted,
				AfterNamespace: last.Namespace, AfterName: last.Name,
			})
		}
		return nil
	})
	if err != nil {
		return api.ListResponse{}, fmt.Errorf("list %s: %w", req.Resource, err)
	}
	return out, nil
}

func compareKeys(a, b model.ObjectKey) int {
	if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
		return c
	}
	return strings.Compare(a.Name, b.Name)
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
