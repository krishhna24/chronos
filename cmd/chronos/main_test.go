package main

import (
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
