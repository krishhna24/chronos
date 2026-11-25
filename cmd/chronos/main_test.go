package main

import (
	"github.com/krishhna24/chronos/internal/collector"
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
	})
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
