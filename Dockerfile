# syntax=docker/dockerfile:1

# Chronos server image: dashboard (web/dist) embedded in a static Go binary,
# running on distroless as UID 65532.
#
#   docker build --build-arg VERSION=$(git describe --tags --always --dirty) -t chronos:dev .

# Base images are pinned by multi-platform index digest (resolved with
# `docker buildx imagetools inspect <image:tag>`); the tag is kept for
# readers and for Dependabot/Renovate. Update tag and digest together.
ARG NODE_IMAGE=node:26-alpine@sha256:6bd9ab43c0ec0190ca7b2d193906cb8b7edf6e7f4afa43ddeccd6452710bffb5
ARG GO_IMAGE=golang:1.26.11-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab
ARG RUNTIME_IMAGE=gcr.io/distroless/static:nonroot@sha256:8ce6dd87e9823c5b48dcfb2c8604714ccb351ce59607327bcc4957448da279b5

# ---- dashboard -------------------------------------------------------------
# Always built for the build platform: the output is static JS/CSS.
FROM --platform=$BUILDPLATFORM ${NODE_IMAGE} AS web
# Node >= 25 no longer ships corepack and web/package.json has no
# packageManager field, so pnpm is pinned here: the npm registry tarball is
# fetched, checked against a pinned sha512 (the registry's dist.integrity,
# from `npm view pnpm@<version> dist.integrity`, base64-decoded), and
# unpacked. Install scripts never run and nothing else is fetched; the
# JavaScript entry point is used instead of the optional native binary.
ARG PNPM_VERSION=12.21.0
ARG PNPM_SHA512=66fd8278ee28935ce951792d34367ff3af5c432a2756445823029533835b8da529bbe0be5130afd56ed0713fb8789e47bad82d93bd5ae2fe54b15562d6276ef0
RUN set -eu; cd /tmp; \
    npm pack --silent "pnpm@${PNPM_VERSION}"; \
    echo "${PNPM_SHA512}  pnpm-${PNPM_VERSION}.tgz" | sha512sum -c -; \
    mkdir -p /opt/pnpm; \
    tar -xzf "pnpm-${PNPM_VERSION}.tgz" -C /opt/pnpm --strip-components=1; \
    rm "pnpm-${PNPM_VERSION}.tgz"; \
    ln -s /opt/pnpm/pnpm /usr/local/bin/pnpm; \
    test "$(pnpm --version)" = "${PNPM_VERSION}"
WORKDIR /src/web
COPY web/package.json web/pnpm-lock.yaml ./
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile
COPY web/ ./
RUN pnpm build && test -f dist/index.html

# ---- server binary ---------------------------------------------------------
# Cross-compiles on the build platform for TARGETOS/TARGETARCH (buildx).
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY web/ web/
COPY --from=web /src/web/dist/ web/dist/
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/chronos ./cmd/chronos

# The data directory must exist and be owned by the runtime user so that
# an emptyDir or a fresh PVC (fsGroup) and a plain `docker run` all work.
RUN mkdir -p /out/var/lib/chronos

# ---- runtime ---------------------------------------------------------------
FROM ${RUNTIME_IMAGE}
COPY --from=build /out/chronos /usr/local/bin/chronos
COPY --from=build --chown=65532:65532 /out/var/lib/chronos /var/lib/chronos
USER 65532:65532
EXPOSE 8080
VOLUME /var/lib/chronos
ENTRYPOINT ["/usr/local/bin/chronos"]
