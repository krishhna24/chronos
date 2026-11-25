package main

import (
	"context"
	"log/slog"

	"github.com/krishhna24/chronos/internal/collector"
	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/store"
	"github.com/krishhna24/chronos/internal/store/memstore"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
	c, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.retention != 7*24*time.Hour || c.storage != "pebble" || c.listen != ":8080" {
		t.Errorf("defaults: %+v", c)
	}
	for _, bad := range [][]string{
		{"--retention", "10m"},
		{"--retention-interval", "0s"},
		{"--retention-interval", "1s"},
		{"--kube-api-qps", "-1"},
		{"--kube-api-burst", "0"},
		{"--nope"},
	} {
		if _, err := parseFlags(bad); err == nil {
			t.Errorf("parseFlags(%v) succeeded", bad)
		}
	}
}

func TestSelectResources(t *testing.T) {
	all, err := selectResources("")
	if err != nil || len(all) != 7 {
		t.Fatalf("default: %d %v", len(all), err)
	}
	got, err := selectResources("po, deploy,pods")
	if err != nil || len(got) != 2 || got[0].ID() != "v1/pods" || got[1].ID() != "apps/v1/deployments" {
		t.Errorf("subset: %v %v", got, err)
	}
	if _, err := selectResources("nodes"); err == nil {
		t.Error("unsupported type accepted")
	}
}

type fakeStatus []collector.ResourceStatus

func (f fakeStatus) Status() []collector.ResourceStatus { return f }

func TestLiveCoverage(t *testing.T) {
	lost := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	f := liveCoverage(fakeStatus{
		{Resource: "v1/pods", LostAt: &lost},
		{Resource: "v1/configmaps"},
	}, &degradedState{})
	if at, ok := f("v1/pods"); !ok || !at.Equal(lost) {
		t.Errorf("pods: %v %v, want %v true", at, ok, lost)
	}
	if _, ok := f("v1/configmaps"); ok {
		t.Error("configmaps reported lost without LostAt")
	}
	if _, ok := f("v1/secrets"); ok {
		t.Error("unknown resource reported lost")
	}
}

func TestDegradedOverlay(t *testing.T) {
	ctx := context.Background()
	st := memstore.New()
	t0 := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	rec := &model.Record{ObservedAt: t0.Add(time.Minute), Key: model.ObjectKey{Resource: "v1/pods", Namespace: "ns", Name: "a"},
		UID: "u", Type: model.ChangeAdded, Origin: model.OriginWatch}
	if err := st.Write(ctx, []store.Entry{{Record: rec}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetHeartbeat(ctx, t0.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	types, err := selectResources("pods,cm")
	if err != nil {
		t.Fatal(err)
	}
	d := &degradedState{}
	earlier := t0.Add(30 * time.Second)
	f := liveCoverage(fakeStatus{{Resource: "v1/configmaps", LostAt: &earlier}}, d)
	if _, ok := f("v1/pods"); ok {
		t.Fatal("lost before entering read-only mode")
	}
	d.enter(ctx, st, types, slog.New(slog.DiscardHandler))
	// pods: max(heartbeat 9:02, latest record 9:01) = 9:02.
	if at, ok := f("v1/pods"); !ok || !at.Equal(t0.Add(2*time.Minute)) {
		t.Errorf("pods lost at %v %v, want heartbeat %v", at, ok, t0.Add(2*time.Minute))
	}
	// configmaps: the collector's earlier LostAt wins over the heartbeat.
	if at, ok := f("v1/configmaps"); !ok || !at.Equal(earlier) {
		t.Errorf("configmaps lost at %v %v, want %v", at, ok, earlier)
	}
	// A resource read-only mode never heard of is lost from the start.
	if at, ok := f("v1/secrets"); !ok || !at.IsZero() {
		t.Errorf("unknown resource: %v %v, want zero true", at, ok)
	}
}
