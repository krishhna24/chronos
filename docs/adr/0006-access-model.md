# ADR 0006: API access and authorization

Status: accepted for MVP (2026-11-24); per-user authorization is future work.

- The HTTP server listens on a ClusterIP Service only. The chart creates no
  Ingress by default.
- `kubectl at` reaches it through the kube-apiserver **service proxy**
  (`/api/v1/namespaces/<ns>/services/<svc>:http/proxy/...`) using the user's
  kubeconfig. Access therefore requires Kubernetes authentication and the
  `get` verb on `services/proxy` in the Chronos namespace.
- The dashboard is reached with `kubectl port-forward` or `kubectl proxy`.
- **Limitation:** anyone who can reach the API can read all recorded history
  of every namespace, including deleted objects. A future mode will verify
  the caller with TokenReview and filter by SubjectAccessReview.
- The API is read-only; there is no endpoint that writes or deletes history.
