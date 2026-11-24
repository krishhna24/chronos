package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/krishhna24/chronos/internal/api"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
)

func TestListPagingOverHTTP(t *testing.T) {
	s := newStore(t)
	var es []store.Entry
	for i := range 7 {
		name := fmt.Sprintf("p%d", i)
		es = append(es, rec(name, name, model.ChangeAdded, model.OriginWatch, at(75), podObj(name, name, "img")))
	}
	if err := s.Write(context.Background(), es); err != nil {
		t.Fatal(err)
	}
	srv := newTestServer(t, s, nil)

	full := decode[api.ListResponse](t, srv.get(t, "/api/v1/list", q("resource", pods, "at", ts(80), "includeDeleted", "true")))
	if full.Continue != "" || len(full.Items) != 9 {
		t.Fatalf("unpaged: %d items, continue %q", len(full.Items), full.Continue)
	}
	var names []string
	next := ""
	for page := 0; ; page++ {
		params := q("resource", pods, "at", ts(80), "includeDeleted", "true", "limit", "2")
		if next != "" {
			params.Set("continue", next)
		}
		w := srv.get(t, "/api/v1/list", params)
		wantStatus(t, w, http.StatusOK)
		r := decode[api.ListResponse](t, w)
		for _, it := range r.Items {
			names = append(names, it.Key.Name)
		}
		if next = r.Continue; next == "" {
			break
		}
		if page > 10 {
			t.Fatal("paging does not terminate")
		}
	}
	var want []string
	for _, it := range full.Items {
		want = append(want, it.Key.Name)
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("paged %v, unpaged %v", names, want)
	}

	first := decode[api.ListResponse](t, srv.get(t, "/api/v1/list", q("resource", pods, "at", ts(80), "limit", "2")))
	for name, params := range map[string]url.Values{
		"limit 0":                  q("resource", pods, "limit", "0"),
		"limit above 5000":         q("resource", pods, "limit", "5001"),
		"limit not a number":       q("resource", pods, "limit", "many"),
		"continue without at":      q("resource", pods, "limit", "2", "continue", first.Continue),
		"continue with other at":   q("resource", pods, "at", ts(81), "limit", "2", "continue", first.Continue),
		"continue with other sel":  q("resource", pods, "at", ts(80), "labelSelector", "app=web", "continue", first.Continue),
		"continue garbage":         q("resource", pods, "at", ts(80), "continue", "%%%"),
		"continue repeated":        q("resource", pods, "at", ts(80), "continue", first.Continue, "continue", first.Continue),
		"continue other namespace": q("resource", pods, "at", ts(80), "namespace", "other", "continue", first.Continue),
	} {
		t.Run(name, func(t *testing.T) {
			wantError(t, srv.get(t, "/api/v1/list", params), http.StatusBadRequest, "bad_request")
		})
	}
}
