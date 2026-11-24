// Package resources is the registry of resource types Chronos records.
package resources

import (
	"fmt"
	"strings"

	"github.com/krishhna24/chronos/internal/model"
)

// Type is a supported resource type plus CLI aliases.
type Type struct {
	model.ResourceType
	ShortNames []string
}

// Builtin are the MVP resource types, in display order.
var Builtin = []Type{
	{model.ResourceType{Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true}, []string{"po"}},
	{model.ResourceType{Group: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Namespaced: true}, []string{"deploy"}},
	{model.ResourceType{Group: "apps", Version: "v1", Resource: "replicasets", Kind: "ReplicaSet", Namespaced: true}, []string{"rs"}},
	{model.ResourceType{Version: "v1", Resource: "services", Kind: "Service", Namespaced: true}, []string{"svc"}},
	{model.ResourceType{Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespaced: true}, []string{"cm"}},
	{model.ResourceType{Version: "v1", Resource: "events", Kind: "Event", Namespaced: true}, []string{"ev"}},
	{model.ResourceType{Version: "v1", Resource: "secrets", Kind: "Secret", Namespaced: true, MetadataOnly: true}, nil},
}

// ByID returns the builtin type with the given canonical ID.
func ByID(id string) (Type, bool) {
	for _, t := range Builtin {
		if t.ID() == id {
			return t, true
		}
	}
	return Type{}, false
}

// Resolve maps a user-supplied name (plural, singular, kind, short name,
// "resource.group", or canonical ID; case-insensitive) to a builtin type.
func Resolve(name string) (Type, error) {
	n := strings.ToLower(strings.TrimSpace(name))
	for _, t := range Builtin {
		cands := []string{t.ID(), t.Resource, strings.ToLower(t.Kind), strings.TrimSuffix(t.Resource, "s")}
		if t.Group != "" {
			cands = append(cands, t.Resource+"."+t.Group, strings.ToLower(t.Kind)+"."+t.Group)
		}
		cands = append(cands, t.ShortNames...)
		for _, c := range cands {
			if n == c {
				return t, nil
			}
		}
	}
	return Type{}, fmt.Errorf("unknown or unsupported resource type %q (supported: %s)", name, supported())
}

func supported() string {
	var s []string
	for _, t := range Builtin {
		s = append(s, t.Resource)
	}
	return strings.Join(s, ", ")
}
