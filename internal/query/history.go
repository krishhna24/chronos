package query

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

// History returns the versions of key with ObservedAt in [from, to], oldest
// first, at most limit of them (0: DefaultLimit). from defaults to the
// retention horizon and to to now. Coverage lists the observation intervals
// of the resource type that intersect [from, to], so callers can see which
// stretches of the history may be missing changes.
func (e *Engine) History(ctx context.Context, key model.ObjectKey, from, to time.Time, limit int) (api.HistoryResponse, error) {
	if err := checkKey(key); err != nil {
		return api.HistoryResponse{}, err
	}
	limit, err := checkLimit(limit)
	if err != nil {
		return api.HistoryResponse{}, err
	}
	out := api.HistoryResponse{Key: apiKey(key), Versions: []api.Version{}}
	err = e.withView(ctx, func(v store.View) error {
		var err error
		if from.IsZero() {
			// Default: everything retained. The horizon is zero when nothing
			// has been compacted, which is a valid lower bound.
			if to, err = e.instant("to", to); err != nil {
				return err
			}
			from = v.Horizon()
			if !from.Before(to) {
				return invalidf("to (%s) must be after the retention horizon (%s)",
					to.Format(time.RFC3339Nano), from.Format(time.RFC3339Nano))
			}
		} else if from, to, err = e.window(from, to); err != nil {
			return err
		}
		err = v.History(ctx, key, from, to, func(r model.Record) error {
			if len(out.Versions) == limit {
				out.Truncated = true
				return errStop
			}
			out.Versions = append(out.Versions, *version(r))
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return fmt.Errorf("history of %s: %w", key, err)
		}
		ivs, err := e.Intervals(ctx, v, key.Resource)
		if err != nil {
			return err
		}
		out.Coverage = overlapping(ivs, from, to)
		return nil
	})
	if err != nil {
		return api.HistoryResponse{}, err
	}
	return out, nil
}

// Changes returns records with ObservedAt in [from, to] in Seq order,
// optionally restricted to one resource type and namespace, at most limit
// of them (0: DefaultLimit). Truncated is set when more records match.
func (e *Engine) Changes(ctx context.Context, from, to time.Time, resource, namespace string, limit int) (api.ChangesResponse, error) {
	if from.IsZero() || to.IsZero() {
		return api.ChangesResponse{}, invalidf("from and to are required")
	}
	if resource != "" {
		rt, err := lookupResource(resource)
		if err != nil {
			return api.ChangesResponse{}, err
		}
		if err := checkNamespace(rt, namespace, false); err != nil {
			return api.ChangesResponse{}, err
		}
	}
	limit, err := checkLimit(limit)
	if err != nil {
		return api.ChangesResponse{}, err
	}
	from, to, err = e.window(from, to)
	if err != nil {
		return api.ChangesResponse{}, err
	}
	out := api.ChangesResponse{Changes: []api.Change{}}
	err = e.withView(ctx, func(v store.View) error {
		// Ask for one more than the limit: its presence is what tells us the
		// result was truncated.
		opts := store.ChangesOptions{From: from, To: to, Resource: resource, Namespace: namespace, Limit: limit + 1}
		err := v.Changes(ctx, opts, func(r model.Record) error {
			if len(out.Changes) == limit {
				out.Truncated = true
				return errStop
			}
			out.Changes = append(out.Changes, api.Change{Key: apiKey(r.Key), Version: *version(r)})
			return nil
		})
		if err != nil && !errors.Is(err, errStop) {
			return fmt.Errorf("changes: %w", err)
		}
		return nil
	})
	if err != nil {
		return api.ChangesResponse{}, err
	}
	return out, nil
}
