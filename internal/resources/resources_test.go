package resources

import "testing"

func TestResolve(t *testing.T) {
	for in, want := range map[string]string{
		"po": "v1/pods", "Pods": "v1/pods", "pod": "v1/pods",
		"deploy": "apps/v1/deployments", "Deployment": "apps/v1/deployments",
		"deployments.apps": "apps/v1/deployments", "apps/v1/deployments": "apps/v1/deployments",
		"rs": "apps/v1/replicasets", "svc": "v1/services", "cm": "v1/configmaps",
		"ev": "v1/events", "secret": "v1/secrets",
	} {
		got, err := Resolve(in)
		if err != nil || got.ID() != want {
			t.Errorf("Resolve(%q) = %q, %v; want %q", in, got.ID(), err, want)
		}
	}
	if _, err := Resolve("nodes"); err == nil {
		t.Error("Resolve(nodes) should fail")
	}
	if tp, _ := ByID("v1/secrets"); !tp.MetadataOnly {
		t.Error("secrets must be metadata-only")
	}
}
