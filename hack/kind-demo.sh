#!/usr/bin/env bash
# Build Chronos, install it with the Helm chart into the kind cluster
# "chronos-dev", run a short demo scenario and print commands to try.
#
# Idempotent: re-running rebuilds the image, upgrades the release (the
# image tag is derived from the image ID, so a changed build rolls the pod)
# and replays the scenario.
#
# Every kubectl/helm/kind call passes the kubeconfig and context
# explicitly; the user's current context and other clusters are never
# touched.
#
# Environment:
#   CLUSTER          kind cluster name              (chronos-dev)
#   KUBECONFIG_FILE  kubeconfig for that cluster    (~/.kube/chronos-dev.kubeconfig)
#   NAMESPACE        release namespace              (chronos)
#   RELEASE          Helm release name              (chronos)
#   DEMO_NS          namespace for the scenario     (demo)
#   STORAGE          pebble | memory | auto         (auto: memory if this
#                    build has no pebble backend, else pebble)
#   HELM             helm binary; empty runs alpine/helm in docker
#   HELM_IMAGE       image for that                 (alpine/helm:3.19.0)
#   SKIP_BUILD=1     reuse the newest local chronos:dev-* image
#   SKIP_SCENARIO=1  install only
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER=${CLUSTER:-chronos-dev}
KUBECONFIG_FILE=${KUBECONFIG_FILE:-$HOME/.kube/chronos-dev.kubeconfig}
CONTEXT=kind-${CLUSTER}
NAMESPACE=${NAMESPACE:-chronos}
RELEASE=${RELEASE:-chronos}
DEMO_NS=${DEMO_NS:-demo}
STORAGE=${STORAGE:-auto}
HELM_IMAGE=${HELM_IMAGE:-alpine/helm:3.19.0}
DEMO_IMAGE_V1=${DEMO_IMAGE_V1:-nginx:1.27-alpine}
DEMO_IMAGE_V2=${DEMO_IMAGE_V2:-nginx:1.28-alpine}
CHART=$ROOT/deploy/helm/chronos

log() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { printf 'kind-demo: %s\n' "$*" >&2; exit 1; }

for bin in docker kind kubectl; do
  command -v "$bin" >/dev/null || die "$bin not found on PATH"
done

k() { kubectl --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" "$@"; }

helm_() {
  if [[ -n "${HELM:-}" ]]; then
    "$HELM" --kubeconfig "$KUBECONFIG_FILE" --kube-context "$CONTEXT" "$@"
  else
    # kind's API server listens on 127.0.0.1 of the host: use host network.
    docker run --rm -i --network host \
      --user "$(id -u):$(id -g)" -e HOME=/tmp \
      -v "$KUBECONFIG_FILE:/kubeconfig:ro" \
      -v "$CHART:/chart:ro" \
      "$HELM_IMAGE" --kubeconfig /kubeconfig --kube-context "$CONTEXT" "$@"
  fi
}
chart_path() { if [[ -n "${HELM:-}" ]]; then echo "$CHART"; else echo /chart; fi; }

# ---- cluster ----------------------------------------------------------------
log "kind cluster $CLUSTER"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "exists"
  if [[ ! -f "$KUBECONFIG_FILE" ]]; then
    # Write this cluster's kubeconfig to its own file, never the default one.
    mkdir -p "$(dirname "$KUBECONFIG_FILE")"
    kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE"
  fi
else
  kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s
fi
k get nodes

# ---- storage backend --------------------------------------------------------
if [[ "$STORAGE" == auto ]]; then
  if grep -q 'pebble storage not available' "$ROOT/cmd/chronos/store.go" 2>/dev/null; then
    STORAGE=memory
  else
    STORAGE=pebble
  fi
fi
echo "storage backend: $STORAGE"
if [[ "$STORAGE" == memory ]]; then
  echo "NOTE: memory storage loses all history when the pod restarts (pebble backend not in this build)."
fi

# ---- image ------------------------------------------------------------------
if [[ "${SKIP_BUILD:-}" == 1 ]]; then
  IMAGE=$(docker images --format '{{.Repository}}:{{.Tag}}' chronos | grep ':dev-' | head -1)
  [[ -n "$IMAGE" ]] || die "SKIP_BUILD=1 but no chronos:dev-* image found"
else
  log "docker build"
  VERSION=$(git -C "$ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)
  docker build --build-arg VERSION="$VERSION" -t chronos:dev "$ROOT"
  ID=$(docker image inspect -f '{{.Id}}' chronos:dev)
  ID=${ID#sha256:}
  IMAGE=chronos:dev-${ID:0:12}
  docker tag chronos:dev "$IMAGE"
fi
echo "image: $IMAGE"

log "kind load docker-image $IMAGE"
kind load docker-image --name "$CLUSTER" "$IMAGE"

# ---- install ----------------------------------------------------------------
HELM_ARGS=(
  --namespace "$NAMESPACE"
  --set image.repository="${IMAGE%%:*}"
  --set image.tag="${IMAGE#*:}"
  --set image.pullPolicy=Never
  --set config.storage="$STORAGE"
  --set config.logFormat=text
)

log "helm lint"
helm_ lint --strict "$(chart_path)"

log "validate rendered manifests (kubectl apply --dry-run=server)"
k create namespace "$NAMESPACE" --dry-run=client -o yaml | k apply -f -
helm_ template "$RELEASE" "$(chart_path)" "${HELM_ARGS[@]}" | k apply --dry-run=server -f -

log "helm upgrade --install $RELEASE"
helm_ upgrade --install "$RELEASE" "$(chart_path)" "${HELM_ARGS[@]}" \
  --create-namespace --wait --timeout 5m

SVC=$(k -n "$NAMESPACE" get svc -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/name=chronos" \
  -o jsonpath='{range .items[?(@.spec.clusterIP!="None")]}{.metadata.name}{end}')
[[ -n "$SVC" ]] || die "Chronos Service not found"
STS=$SVC
k -n "$NAMESPACE" rollout status "statefulset/$STS" --timeout 3m
PROXY=/api/v1/namespaces/$NAMESPACE/services/$SVC:http/proxy

log "service proxy check: $PROXY/readyz"
for i in $(seq 1 30); do
  if k get --raw "$PROXY/readyz" 2>/dev/null; then break; fi
  [[ $i == 30 ]] && die "Chronos is not reachable through the service proxy"
  sleep 2
done

# ---- scenario ---------------------------------------------------------------
if [[ "${SKIP_SCENARIO:-}" != 1 ]]; then
  log "demo scenario in namespace $DEMO_NS"
  k create namespace "$DEMO_NS" --dry-run=client -o yaml | k apply -f -

  # Start from v1 every run so the rollout below is always a real change.
  k -n "$DEMO_NS" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  labels: {app: web}
spec:
  replicas: 2
  selector:
    matchLabels: {app: web}
  template:
    metadata:
      labels: {app: web}
    spec:
      containers:
        - name: web
          image: $DEMO_IMAGE_V1
          ports: [{containerPort: 80}]
YAML
  k -n "$DEMO_NS" rollout status deployment/web --timeout 3m
  T_BEFORE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  sleep 5

  echo "rolling out $DEMO_IMAGE_V2"
  k -n "$DEMO_NS" set image deployment/web "web=$DEMO_IMAGE_V2"
  k -n "$DEMO_NS" rollout status deployment/web --timeout 3m

  echo "deleting and recreating configmap app-config"
  k -n "$DEMO_NS" delete configmap app-config --ignore-not-found
  k -n "$DEMO_NS" create configmap app-config --from-literal=color=blue
  sleep 2
  T_BLUE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  sleep 1
  k -n "$DEMO_NS" delete configmap app-config
  sleep 2
  k -n "$DEMO_NS" create configmap app-config --from-literal=color=green
  sleep 3
  T_AFTER=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  log "what Chronos recorded (through the service proxy)"
  echo "-- deployment demo/web, image at $T_BEFORE:"
  k get --raw "$PROXY/api/v1/state?resource=apps/v1/deployments&namespace=$DEMO_NS&name=web&at=$T_BEFORE" \
    | grep -o '"image":"[^"]*"' | head -1 || true
  echo "-- deployment demo/web, image now:"
  k get --raw "$PROXY/api/v1/state?resource=apps/v1/deployments&namespace=$DEMO_NS&name=web" \
    | grep -o '"image":"[^"]*"' | head -1 || true
  echo "-- configmap demo/app-config diff $T_BLUE .. $T_AFTER (expect recreated:true, blue -> green):"
  DIFF=$(k get --raw "$PROXY/api/v1/diff?resource=v1/configmaps&namespace=$DEMO_NS&name=app-config&from=$T_BLUE&to=$T_AFTER")
  if command -v jq >/dev/null; then
    jq -r '"recreated: \(.recreated)", .diff' <<<"$DIFF"
  elif command -v python3 >/dev/null; then
    python3 -c 'import json,sys; d=json.load(sys.stdin); print("recreated:", d["recreated"]); print(d["diff"])' <<<"$DIFF"
  else
    echo "$DIFF"
  fi
fi

# ---- next steps -------------------------------------------------------------
KC="--kubeconfig $KUBECONFIG_FILE --context $CONTEXT"
cat <<EOT

Chronos is running in $CONTEXT ($NAMESPACE/$SVC, storage=$STORAGE).

Try (with kubectl-at on PATH: make build-plugin && export PATH=\$PWD/bin:\$PATH).
kubectl does not accept flags before a plugin name, so they go last:

  kubectl at -5m get pods -n $DEMO_NS $KC
  kubectl at -5m get deployment/web -n $DEMO_NS -o yaml $KC
  kubectl at diff -10m now deployment/web -n $DEMO_NS $KC
  kubectl at history configmap/app-config -n $DEMO_NS $KC

Raw API through the service proxy:

  kubectl $KC get --raw $PROXY/api/v1/resources

Dashboard:

  kubectl $KC -n $NAMESPACE port-forward svc/$SVC 8080:http
  # open http://localhost:8080/

The cluster and the release are left running. Remove the release with:

  ${HELM:-helm} --kubeconfig $KUBECONFIG_FILE --kube-context $CONTEXT -n $NAMESPACE uninstall $RELEASE
EOT
