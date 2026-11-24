#!/usr/bin/env bash
# Cross-compile kubectl-at for every release platform and package it as
# dist/kubectl-at_<version>_<os>_<arch>.{tar.gz,zip} plus
# dist/kubectl-at_<version>_checksums.txt (sha256).
#
#   hack/build-plugin-release.sh v0.1.0 [outdir]
#
# Exits 0 with a warning when cmd/kubectl-at does not exist yet, so a
# branch without the plugin can still run the release pipeline.
set -euo pipefail

VERSION=${1:?usage: $0 <version> [outdir]}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT=${2:-$ROOT/dist}
PLATFORMS=${PLATFORMS:-"linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64"}

if [[ ! -d "$ROOT/cmd/kubectl-at" ]]; then
  echo "::warning::cmd/kubectl-at not found; no plugin binaries built"
  exit 0
fi

mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

for p in $PLATFORMS; do
  os=${p%/*} arch=${p#*/}
  name=kubectl-at_${VERSION}_${os}_${arch}
  dir=$WORK/$name
  mkdir -p "$dir"
  ext=""; [[ $os == windows ]] && ext=.exe
  echo "building $name"
  (cd "$ROOT" && CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath \
    -ldflags "-s -w -X main.version=$VERSION" -o "$dir/kubectl-at$ext" ./cmd/kubectl-at)
  cp "$ROOT/README.md" "$dir/" 2>/dev/null || true
  if [[ $os == windows ]]; then
    (cd "$WORK" && zip -qr "$OUT/$name.zip" "$name")
  else
    # Reproducible-ish archives: fixed owner and sorted entries.
    tar -C "$WORK" --owner=0 --group=0 --numeric-owner --sort=name -czf "$OUT/$name.tar.gz" "$name"
  fi
done

(cd "$OUT" && sha256sum kubectl-at_"${VERSION}"_*.tar.gz kubectl-at_"${VERSION}"_*.zip > "kubectl-at_${VERSION}_checksums.txt")
cat "$OUT/kubectl-at_${VERSION}_checksums.txt"
