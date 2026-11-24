// Package api defines the JSON wire types of the Chronos HTTP API
// (/api/v1). They are the contract between the server, the kubectl-at
// plugin, and the dashboard (web/src/api/types.ts mirrors this file).
// See docs/api.md for endpoint semantics.
package api

import (
	"encoding/json"
	"time"
)

// CoverageState describes how much Chronos knows about a resource type at a
// point in time.
type CoverageState string

const (
	// CoverageObserved: Chronos was continuously watching. Answers are
	// authoritative up to watch delivery lag.
	CoverageObserved CoverageState = "observed"
	// CoverageGap: continuity was lost (watch expired, Chronos down). Any
	// object may have changed; the last known state is returned for
	// reference only.
	CoverageGap CoverageState = "gap"
	// CoverageUnobserved: before Chronos first observed this resource type.
	CoverageUnobserved CoverageState = "unobserved"
	// CoverageExpired: older than the retention horizon.
	CoverageExpired CoverageState = "expired"
)

// Existence answers "did this object exist at that time?".
type Existence string

const (
	ExistencePresent Existence = "present"
	ExistenceAbsent  Existence = "absent"
	// ExistenceUnknown: coverage is not "observed", so Chronos cannot say.
	ExistenceUnknown Existence = "unknown"
)

// Coverage is the coverage of one resource type at one instant.
type Coverage struct {
	State CoverageState `json:"state"`
	// Since/Until bound the coverage interval containing the instant
	// (Until is nil while the interval is still open).
	Since  *time.Time `json:"since,omitempty"`
	Until  *time.Time `json:"until,omitempty"`
	Reason string     `json:"reason,omitempty"` // why the preceding interval ended, for gaps
}

// Interval is one interval of continuous observation.
type Interval struct {
	Start     time.Time  `json:"start"`
	End       *time.Time `json:"end,omitempty"` // nil: still observing
	EndReason string     `json:"endReason,omitempty"`
}

// Resource describes a recorded resource type.
type Resource struct {
	ID           string     `json:"id"` // "apps/v1/deployments"
	Group        string     `json:"group"`
	Version      string     `json:"version"`
	Resource     string     `json:"resource"`
	Kind         string     `json:"kind"`
	Namespaced   bool       `json:"namespaced"`
	MetadataOnly bool       `json:"metadataOnly"`
	ShortNames   []string   `json:"shortNames,omitempty"`
	Coverage     []Interval `json:"coverage"`
}

// ResourcesResponse: GET /api/v1/resources
type ResourcesResponse struct {
	Now       time.Time  `json:"now"`
	Horizon   *time.Time `json:"horizon,omitempty"`
	Resources []Resource `json:"resources"`
}

// ObjectKey identifies an object by name.
type ObjectKey struct {
	Resource  string `json:"resource"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

// Version summarizes one journal record.
type Version struct {
	Seq             uint64    `json:"seq"`
	ObservedAt      time.Time `json:"observedAt"`
	Type            string    `json:"type"`   // Added | Modified | Deleted
	Origin          string    `json:"origin"` // watch | baseline | resync
	UID             string    `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
}

// ObjectState is the state of one object at one instant.
type ObjectState struct {
	Key       ObjectKey `json:"key"`
	At        time.Time `json:"at"`
	Existence Existence `json:"existence"`
	Coverage  Coverage  `json:"coverage"`
	// Version is the record the answer is based on: the latest record at
	// or before At. Present for "present" and "absent" (when the object was
	// deleted) and for "unknown" during a gap (last known state).
	Version *Version `json:"version,omitempty"`
	// Object is the normalized object. Set when Existence is present, or
	// when unknown and a last known state exists (check Coverage.State).
	Object json.RawMessage `json:"object,omitempty"`
}

// ListResponse: GET /api/v1/list
type ListResponse struct {
	At       time.Time     `json:"at"`
	Coverage Coverage      `json:"coverage"`
	Items    []ObjectState `json:"items"`
}

// DiffResponse: GET /api/v1/diff
type DiffResponse struct {
	From ObjectState `json:"from"`
	To   ObjectState `json:"to"`
	// Recreated: both sides exist but have different UIDs (the object was
	// deleted and recreated in between).
	Recreated bool `json:"recreated"`
	// Diff is a unified diff of the YAML renderings (empty if equal).
	Diff string `json:"diff"`
	// Complete is false when any part of [from, to] was not observed, so
	// changes in between may be missing even if Diff is empty.
	Complete bool `json:"complete"`
}

// HistoryResponse: GET /api/v1/history
type HistoryResponse struct {
	Key       ObjectKey  `json:"key"`
	Versions  []Version  `json:"versions"`
	Coverage  []Interval `json:"coverage"`
	Truncated bool       `json:"truncated"`
}

// Change is a Version with its object key.
type Change struct {
	Key ObjectKey `json:"key"`
	Version
}

// ChangesResponse: GET /api/v1/changes
type ChangesResponse struct {
	Changes   []Change `json:"changes"`
	Truncated bool     `json:"truncated"`
}

// Error is the body of every non-2xx response.
type Error struct {
	Code    string `json:"code"` // bad_request | not_found | internal | unavailable
	Message string `json:"message"`
}
