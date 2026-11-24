// Package model defines the core data types shared by every Chronos
// subsystem: the collector produces them, the store persists them, and the
// query engine reads them. Changes to this package are contract changes and
// must be coordinated across owners (see docs/architecture.md).
package model

import (
	"fmt"
	"strings"
	"time"
)

// ResourceType identifies a Kubernetes API resource Chronos records.
type ResourceType struct {
	Group      string `json:"group"`
	Version    string `json:"version"`
	Resource   string `json:"resource"` // plural, lower-case: "deployments"
	Kind       string `json:"kind"`     // "Deployment"
	Namespaced bool   `json:"namespaced"`
	// MetadataOnly resources are collected through the metadata API, so their
	// contents (e.g. Secret data) never enter the Chronos process.
	MetadataOnly bool `json:"metadataOnly"`
}

// ID returns the canonical resource identifier used in storage keys and the
// HTTP API: "v1/pods" for the core group, "apps/v1/deployments" otherwise.
func (r ResourceType) ID() string {
	if r.Group == "" {
		return r.Version + "/" + r.Resource
	}
	return r.Group + "/" + r.Version + "/" + r.Resource
}

// ObjectKey is the logical identity of an object: what a user names on the
// command line. Several object instances (UIDs) can share one key over time
// when an object is deleted and recreated.
type ObjectKey struct {
	Resource  string `json:"resource"` // ResourceType.ID()
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (k ObjectKey) String() string {
	if k.Namespace == "" {
		return k.Resource + "/" + k.Name
	}
	return k.Resource + "/" + k.Namespace + "/" + k.Name
}

// Validate rejects keys that cannot be encoded unambiguously. Kubernetes
// names never contain '/' or NUL, so a key containing them is malformed.
func (k ObjectKey) Validate() error {
	if k.Resource == "" || k.Name == "" {
		return fmt.Errorf("object key %q: resource and name are required", k)
	}
	for _, s := range []string{k.Namespace, k.Name} {
		if strings.ContainsAny(s, "/\x00") {
			return fmt.Errorf("object key %q: namespace and name must not contain '/' or NUL", k)
		}
	}
	if strings.ContainsRune(k.Resource, '\x00') {
		return fmt.Errorf("object key %q: resource must not contain NUL", k)
	}
	return nil
}

// ChangeType is what happened to an object.
type ChangeType uint8

const (
	ChangeAdded ChangeType = iota + 1
	ChangeModified
	ChangeDeleted
)

func (c ChangeType) String() string {
	switch c {
	case ChangeAdded:
		return "Added"
	case ChangeModified:
		return "Modified"
	case ChangeDeleted:
		return "Deleted"
	}
	return fmt.Sprintf("ChangeType(%d)", c)
}

// Origin says how Chronos learned about a change, and therefore how much its
// timestamp can be trusted.
type Origin uint8

const (
	// OriginWatch: delivered by a watch event while coverage was continuous.
	// ObservedAt is when Chronos received it (subject to delivery lag).
	OriginWatch Origin = iota + 1
	// OriginBaseline: the object was present in the first LIST of a coverage
	// interval that had no prior history for it. It existed at ObservedAt;
	// nothing is known about it before that.
	OriginBaseline
	// OriginResync: inferred by diffing a LIST after a gap against the last
	// known state. The change happened at some unknown point inside the gap
	// that ends at ObservedAt. Intermediate states inside the gap are lost.
	OriginResync
)

func (o Origin) String() string {
	switch o {
	case OriginWatch:
		return "watch"
	case OriginBaseline:
		return "baseline"
	case OriginResync:
		return "resync"
	}
	return fmt.Sprintf("Origin(%d)", o)
}

// Record is one entry of the change journal.
type Record struct {
	// Seq is assigned by the store: globally unique, strictly increasing in
	// commit order. It is the only total order Chronos guarantees.
	Seq uint64 `json:"seq"`
	// ObservedAt is Chronos's clock when the change was observed. The store
	// guarantees ObservedAt is strictly increasing with Seq.
	ObservedAt time.Time `json:"observedAt"`
	Key        ObjectKey `json:"key"`
	UID        string    `json:"uid"`
	// ResourceVersion is opaque and only meaningful for equality within one
	// resource type. Never compare it numerically or across types.
	ResourceVersion string     `json:"resourceVersion"`
	Type            ChangeType `json:"type"`
	Origin          Origin     `json:"origin"`
	// Object is the normalized JSON of the object after the change. For
	// ChangeDeleted it is the last state known to Chronos.
	Object []byte `json:"-"`
}

// CoverageType marks a boundary of continuous observation for a resource.
type CoverageType uint8

const (
	// CoverageStart: from At onward Chronos holds a complete, continuous view
	// of the resource type (written after a LIST has been fully applied).
	CoverageStart CoverageType = iota + 1
	// CoverageEnd: from At onward continuity is no longer guaranteed (watch
	// lost and a relist was required, shutdown, or crash detected at start).
	CoverageEnd
)

func (c CoverageType) String() string {
	switch c {
	case CoverageStart:
		return "Start"
	case CoverageEnd:
		return "End"
	}
	return fmt.Sprintf("CoverageType(%d)", c)
}

// CoverageEvent records a coverage boundary for one resource type. Within a
// resource type, events alternate Start, End, Start, ... in Seq order; an
// End without a Start, or two Starts in a row, is a store invariant
// violation the store must reject.
type CoverageEvent struct {
	Seq      uint64       `json:"seq"`
	At       time.Time    `json:"at"`
	Resource string       `json:"resource"`
	Type     CoverageType `json:"type"`
	Reason   string       `json:"reason"`
}
