package normalize

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"github.com/krishhna24/chronos/internal/model"
	"github.com/krishhna24/chronos/internal/resources"
)

var (
	deployments = mustType("apps/v1/deployments")
	secrets     = mustType("v1/secrets")
)

func mustType(id string) model.ResourceType {
	t, ok := resources.ByID(id)
	if !ok {
		panic(id)
	}
	return t.ResourceType
}

func deployment() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":            "web",
			"namespace":       "default",
			"uid":             "uid-1",
			"resourceVersion": "100",
			"generation":      int64(3),
			"labels":          map[string]any{"b": "2", "a": "1"},
			"annotations":     map[string]any{"note": "<keep & raw>", "secret/token": "x"},
			"managedFields": []any{map[string]any{
				"manager": "kubectl", "operation": "Update", "time": "2026-11-24T10:00:00Z",
			}},
		},
		"spec": map[string]any{
			"replicas": int64(3),
			"big":      int64(9007199254740993), // > 2^53: must survive round trips
			"ratio":    0.25,
		},
	}}
}

func TestDeterministic(t *testing.T) {
	a, err := Normalize(deployments, deployment(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	for range 50 {
		b, err := Normalize(deployments, deployment(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a.Object, b.Object) || a.Hash != b.Hash {
			t.Fatalf("non-deterministic output:\n%s\n%s", a.Object, b.Object)
		}
	}
	// Keys sorted at every level, no HTML escaping.
	want := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"note":"<keep & raw>","secret/token":"x"},"generation":3,"labels":{"a":"1","b":"2"},"name":"web","namespace":"default","resourceVersion":"100","uid":"uid-1"},"spec":{"big":9007199254740993,"ratio":0.25,"replicas":3}}`
	if string(a.Object) != want {
		t.Fatalf("got  %s\nwant %s", a.Object, want)
	}
	if a.Name != "web" || a.Namespace != "default" || a.UID != "uid-1" || a.ResourceVersion != "100" {
		t.Fatalf("identity fields: %+v", a)
	}
}

func TestDoesNotMutateInput(t *testing.T) {
	in := deployment()
	before, _ := json.Marshal(in.Object)
	if _, err := Normalize(deployments, in, Options{RedactAnnotations: []string{"note"}}); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(in.Object)
	if !bytes.Equal(before, after) {
		t.Fatalf("input mutated:\n%s\n%s", before, after)
	}
}

func TestManagedFields(t *testing.T) {
	dropped, err := Normalize(deployments, deployment(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(dropped.Object, []byte("managedFields")) {
		t.Fatalf("managedFields not dropped: %s", dropped.Object)
	}
	kept, err := Normalize(deployments, deployment(), Options{KeepManagedFields: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(kept.Object, []byte(`"managedFields":[{"manager":"kubectl"`)) {
		t.Fatalf("managedFields not kept: %s", kept.Object)
	}
	// managedFields are never part of the hash.
	if kept.Hash != dropped.Hash {
		t.Fatal("managedFields affected the hash")
	}
	other := deployment()
	other.Object["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "other"}}
	o, _ := Normalize(deployments, other, Options{KeepManagedFields: true})
	if o.Hash != kept.Hash {
		t.Fatal("managedFields-only change changed the hash")
	}
}

func TestHashIgnoresResourceVersionOnly(t *testing.T) {
	base, _ := Normalize(deployments, deployment(), Options{})

	rv := deployment()
	rv.SetResourceVersion("101")
	r, _ := Normalize(deployments, rv, Options{})
	if r.Hash != base.Hash {
		t.Fatal("resourceVersion-only change changed the hash")
	}
	if r.ResourceVersion != "101" || !bytes.Contains(r.Object, []byte(`"resourceVersion":"101"`)) {
		t.Fatal("resourceVersion must be kept in the record and stored object")
	}

	spec := deployment()
	spec.Object["spec"].(map[string]any)["replicas"] = int64(4)
	s, _ := Normalize(deployments, spec, Options{})
	if s.Hash == base.Hash {
		t.Fatal("spec change not detected")
	}
	gen := deployment()
	gen.SetGeneration(4)
	g, _ := Normalize(deployments, gen, Options{})
	if g.Hash == base.Hash {
		t.Fatal("generation change not detected")
	}
}

func TestRedactAnnotations(t *testing.T) {
	r, err := Normalize(deployments, deployment(), Options{RedactAnnotations: []string{"secret/token"}})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(r.Object, []byte("secret/token")) {
		t.Fatalf("redacted annotation present: %s", r.Object)
	}
	// Removing every annotation removes the empty map too.
	r, _ = Normalize(deployments, deployment(), Options{RedactAnnotations: []string{"secret/token", "note"}})
	if bytes.Contains(r.Object, []byte("annotations")) {
		t.Fatalf("empty annotations kept: %s", r.Object)
	}
}

// secretValue is a fake test value (base64 "super-secret-value").
const secretValue = "c3VwZXItc2VjcmV0LXZhbHVl" //nolint:gosec // test fixture, not a credential

func fullSecret() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"type":       "Opaque",
		"metadata": map[string]any{
			"name":            "db",
			"namespace":       "prod",
			"uid":             "uid-s",
			"resourceVersion": "7",
			"labels":          map[string]any{"app": "db"},
			"annotations": map[string]any{
				LastAppliedAnnotation: `{"apiVersion":"v1","data":{"password":"` + secretValue + `"},"kind":"Secret"}`,
				"team":                "storage",
				"vault/path":          "kv/prod/db",
			},
			"managedFields": []any{map[string]any{
				"manager": "kubectl", "fieldsV1": map[string]any{"f:data": map[string]any{"f:password": map[string]any{}}},
			}},
		},
		"data":       map[string]any{"password": secretValue},
		"stringData": map[string]any{"token": "plain-text-token"},
	}}
}

func assertNoSecretContent(t *testing.T, b []byte) {
	t.Helper()
	for _, bad := range []string{secretValue, "plain-text-token", `"data"`, "stringData", "last-applied", "f:password", "managedFields", `"type"`} {
		if bytes.Contains(b, []byte(bad)) {
			t.Fatalf("secret content %q leaked: %s", bad, b)
		}
	}
}

func TestSecretFullObjectIsStripped(t *testing.T) {
	// Defence in depth: even when handed a full Secret (the collector never
	// requests one) only apiVersion, kind and metadata survive.
	for _, keep := range []bool{false, true} {
		r, err := Normalize(secrets, fullSecret(), Options{KeepManagedFields: keep, RedactAnnotations: []string{"vault/path"}})
		if err != nil {
			t.Fatal(err)
		}
		assertNoSecretContent(t, r.Object)
		if bytes.Contains(r.Object, []byte("vault/path")) {
			t.Fatalf("redacted annotation leaked: %s", r.Object)
		}
		var m map[string]any
		if err := json.Unmarshal(r.Object, &m); err != nil {
			t.Fatal(err)
		}
		if len(m) != 3 || m["apiVersion"] != "v1" || m["kind"] != "Secret" || m["metadata"] == nil {
			t.Fatalf("unexpected top-level fields: %s", r.Object)
		}
		// No allow-list: no annotations at all.
		if strings.Contains(string(r.Object), "annotations") {
			t.Fatalf("annotations kept without an allow-list: %s", r.Object)
		}
	}
}

func TestSecretTypedObjectIsStripped(t *testing.T) {
	s := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: "prod", UID: "u", Annotations: map[string]string{LastAppliedAnnotation: secretValue}},
		Data:       map[string][]byte{"password": []byte("super-secret-value")},
		StringData: map[string]string{"token": "plain-text-token"},
	}
	r, err := Normalize(secrets, s, Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretContent(t, r.Object)
	if bytes.Contains(r.Object, []byte("super-secret-value")) {
		t.Fatalf("data leaked: %s", r.Object)
	}
}

func TestPartialObjectMetadata(t *testing.T) {
	p := &metav1.PartialObjectMetadata{
		// What the metadata API actually returns as type information.
		TypeMeta: metav1.TypeMeta{APIVersion: "meta.k8s.io/v1", Kind: "PartialObjectMetadata"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "db", Namespace: "prod", UID: types.UID("u"), ResourceVersion: "7",
			Annotations: map[string]string{LastAppliedAnnotation: secretValue},
		},
	}
	r, err := Normalize(secrets, p, Options{})
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretContent(t, r.Object)
	if !bytes.HasPrefix(r.Object, []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{`)) {
		t.Fatalf("type information not taken from the resource type: %s", r.Object)
	}
	// For metadata-only types a resourceVersion change is the only evidence
	// of a data change, so it must change the hash.
	p2 := p.DeepCopy()
	p2.ResourceVersion = "8"
	r2, _ := Normalize(secrets, p2, Options{})
	if r2.Hash == r.Hash {
		t.Fatal("metadata-only resourceVersion change not detected")
	}
}

func TestNormalizeJSONRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		rt  model.ResourceType
		obj *unstructured.Unstructured
	}{{deployments, deployment()}, {secrets, fullSecret()}} {
		for _, opts := range []Options{{}, {KeepManagedFields: true}} {
			a, err := Normalize(tc.rt, tc.obj, opts)
			if err != nil {
				t.Fatal(err)
			}
			b, err := NormalizeJSON(tc.rt, a.Object, opts)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(a.Object, b.Object) || a.Hash != b.Hash || a.UID != b.UID {
				t.Fatalf("round trip differs:\n%s\n%s", a.Object, b.Object)
			}
		}
	}
}

func TestErrors(t *testing.T) {
	if _, err := Normalize(deployments, &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{}}}, Options{}); err == nil {
		t.Fatal("want error for object without name")
	}
	if _, err := Normalize(deployments, nil, Options{}); err == nil {
		t.Fatal("want error for nil")
	}
	if _, err := NormalizeJSON(deployments, []byte("{"), Options{}); err == nil {
		t.Fatal("want error for bad JSON")
	}
}

func TestClusterScopedHasNoNamespace(t *testing.T) {
	rt := model.ResourceType{Version: "v1", Resource: "nodes", Kind: "Node"}
	u := &unstructured.Unstructured{Object: map[string]any{"metadata": map[string]any{"name": "n1", "namespace": "bogus"}}}
	r, err := Normalize(rt, u, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Namespace != "" || r.Key(rt) != (model.ObjectKey{Resource: "v1/nodes", Name: "n1"}) {
		t.Fatalf("key: %+v", r.Key(rt))
	}
}

func TestSecretAnnotationAllowlist(t *testing.T) {
	s := fullSecret()
	anns := s.GetAnnotations()
	anns["openshift.io/token-secret.value"] = "plain-text-token"
	s.SetAnnotations(anns)
	opts := Options{
		MetadataOnlyAnnotationAllowlist: []string{"team", "vault/path", LastAppliedAnnotation},
		RedactAnnotations:               []string{"vault/path"},
	}
	r, err := Normalize(secrets, s, opts)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretContent(t, r.Object)
	for _, bad := range []string{"openshift.io", "vault/path"} {
		if bytes.Contains(r.Object, []byte(bad)) {
			t.Fatalf("annotation %q survived: %s", bad, r.Object)
		}
	}
	if !bytes.Contains(r.Object, []byte(`"annotations":{"team":"storage"}`)) {
		t.Fatalf("allow-listed annotation lost: %s", r.Object)
	}
	// Labels are not affected.
	if !bytes.Contains(r.Object, []byte(`"labels":{"app":"db"}`)) {
		t.Fatalf("labels lost: %s", r.Object)
	}
	// Non-metadata-only types keep their annotations.
	d, _ := Normalize(deployments, deployment(), opts)
	if !bytes.Contains(d.Object, []byte(`"note":"<keep & raw>"`)) {
		t.Fatalf("allow-list must not apply to full types: %s", d.Object)
	}
}
