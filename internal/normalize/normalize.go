// Package normalize turns Kubernetes objects into the canonical JSON that
// Chronos stores, and computes the content hash used for change detection.
//
// Rules (see docs/adr/0003 and docs/adr/0005):
//   - Output is deterministic: object keys are sorted at every level and
//     HTML characters are not escaped, so the same object always produces
//     the same bytes.
//   - metadata.managedFields is dropped unless Options.KeepManagedFields is
//     set. It is large and changes on every write.
//   - Annotations listed in Options.RedactAnnotations are dropped from every
//     type.
//   - MetadataOnly types (Secrets) produce only apiVersion, kind and
//     metadata, never data or stringData, even when handed a full object.
//     Their annotations are dropped except those listed in
//     Options.MetadataOnlyAnnotationAllowlist (default: none). Annotations
//     on Secrets routinely carry credentials or the whole object
//     (kubectl.kubernetes.io/last-applied-configuration includes data;
//     others such as openshift.io/token-secret.value hold tokens), so an
//     allow-list is the only safe default. managedFields are dropped too
//     (their field set names the data keys), regardless of
//     Options.KeepManagedFields.
//   - The hash covers the stored object minus metadata.resourceVersion and
//     metadata.managedFields, so writes that change neither the content nor
//     anything a user would read are detected as no-ops. For MetadataOnly
//     types resourceVersion IS hashed: their content is invisible, so a
//     resourceVersion change is the only evidence of a data change (ADR
//     0005 requires recording it as Modified).
package normalize

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krishhna24/chronos/internal/model"
)

// LastAppliedAnnotation is written by `kubectl apply` and holds the full
// applied object. On a Secret that includes its data.
const LastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// Options configures normalization.
type Options struct {
	// KeepManagedFields keeps metadata.managedFields in the stored object.
	// It is never hashed and never kept for MetadataOnly types.
	KeepManagedFields bool
	// RedactAnnotations are annotation keys removed from every object.
	RedactAnnotations []string
	// MetadataOnlyAnnotationAllowlist are the only annotation keys kept on
	// MetadataOnly types (still subject to RedactAnnotations, and never
	// the last-applied annotation). Empty: no annotations are kept.
	MetadataOnlyAnnotationAllowlist []string
}

// Hash is the SHA-256 of an object's hashed view.
type Hash [sha256.Size]byte

func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// Result is a normalized object.
type Result struct {
	// Object is the canonical JSON to store.
	Object []byte
	// Hash identifies the object's content for change detection.
	Hash            Hash
	Namespace       string
	Name            string
	UID             string
	ResourceVersion string
}

// Key returns the object's storage key for resource type rt.
func (r Result) Key(rt model.ResourceType) model.ObjectKey {
	return model.ObjectKey{Resource: rt.ID(), Namespace: r.Namespace, Name: r.Name}
}

// ErrNoName is returned for objects without metadata.name.
var ErrNoName = errors.New("normalize: object has no metadata.name")

// Normalize converts obj, which must be an *unstructured.Unstructured, a
// *metav1.PartialObjectMetadata, or any typed object convertible to
// unstructured, into its canonical stored form. obj is not modified.
func Normalize(rt model.ResourceType, obj runtime.Object, opts Options) (Result, error) {
	content, err := toMap(obj)
	if err != nil {
		return Result{}, err
	}
	return normalizeMap(rt, content, opts)
}

// NormalizeJSON re-normalizes a previously stored object (the bytes of a
// model.Record) and returns the same Result Normalize would have produced
// for it. The collector uses it to rebuild change-detection state from the
// store after a restart.
func NormalizeJSON(rt model.ResourceType, data []byte, opts Options) (Result, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber() // keep integers exact; json.Number re-encodes verbatim
	var content map[string]any
	if err := dec.Decode(&content); err != nil {
		return Result{}, fmt.Errorf("normalize: decode stored object: %w", err)
	}
	return normalizeMap(rt, content, opts)
}

func toMap(obj runtime.Object) (map[string]any, error) {
	switch o := obj.(type) {
	case nil:
		return nil, errors.New("normalize: nil object")
	case *unstructured.Unstructured:
		if o == nil || o.Object == nil {
			return nil, errors.New("normalize: empty unstructured object")
		}
		return o.Object, nil
	case *metav1.PartialObjectMetadata:
		if o == nil {
			return nil, errors.New("normalize: nil PartialObjectMetadata")
		}
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("normalize: convert %T: %w", obj, err)
	}
	return m, nil
}

// normalizeMap never mutates content or anything reachable from it: it
// copies the top level and metadata maps before removing keys.
func normalizeMap(rt model.ResourceType, content map[string]any, opts Options) (Result, error) {
	meta, _ := content["metadata"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
	}
	meta = maps.Clone(meta)

	if anns, ok := meta["annotations"].(map[string]any); ok {
		if rt.MetadataOnly {
			kept := map[string]any{}
			for _, k := range opts.MetadataOnlyAnnotationAllowlist {
				if v, ok := anns[k]; ok && k != LastAppliedAnnotation {
					kept[k] = v
				}
			}
			anns = kept
		} else {
			anns = maps.Clone(anns)
		}
		for _, k := range opts.RedactAnnotations {
			delete(anns, k)
		}
		if len(anns) == 0 {
			delete(meta, "annotations")
		} else {
			meta["annotations"] = anns
		}
	}
	if !opts.KeepManagedFields || rt.MetadataOnly {
		delete(meta, "managedFields")
	}

	var out map[string]any
	if rt.MetadataOnly {
		// Allow-list, not deny-list: nothing but these three fields can
		// ever leave this function for a metadata-only type.
		out = map[string]any{}
	} else {
		out = maps.Clone(content)
	}
	// The metadata API reports apiVersion meta.k8s.io/v1 and kind
	// PartialObjectMetadata, and list items may lack type information;
	// the resource type is authoritative.
	out["apiVersion"] = schema.GroupVersion{Group: rt.Group, Version: rt.Version}.String()
	out["kind"] = rt.Kind
	out["metadata"] = meta

	res := Result{
		Namespace:       str(meta["namespace"]),
		Name:            str(meta["name"]),
		UID:             str(meta["uid"]),
		ResourceVersion: str(meta["resourceVersion"]),
	}
	if res.Name == "" {
		return Result{}, ErrNoName
	}
	if !rt.Namespaced {
		res.Namespace = ""
	}

	var err error
	if res.Object, err = canonicalJSON(out); err != nil {
		return Result{}, err
	}

	// Hashed view: the stored object minus noise.
	hmeta := maps.Clone(meta)
	delete(hmeta, "managedFields")
	if !rt.MetadataOnly {
		delete(hmeta, "resourceVersion")
	}
	hashed := maps.Clone(out)
	hashed["metadata"] = hmeta
	hb, err := canonicalJSON(hashed)
	if err != nil {
		return Result{}, err
	}
	res.Hash = sha256.Sum256(hb)
	return res, nil
}

// canonicalJSON marshals v with sorted map keys (encoding/json always sorts
// map[string]any keys) and without HTML escaping.
func canonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("normalize: encode: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
