# syntax=docker/dockerfile:1

# Chronos server image: dashboard (web/dist) embedded in a static Go binary,
# running on distroless as UID 65532.
#
#   docker build --build-arg VERSION=$(git describe --tags --always --dirty) -t chronos:dev .

ARG NODE_IMAGE=node:26-alpine
ARG GO_IMAGE=golang:1.26.11-alpine
ARG RUNTIME_IMAGE=gcr.io/distroless/static:nonroot

# ---- dashboard -------------------------------------------------------------
# Always built for the build platform: the output is static JS/CSS.
FROM --platform=$BUILDPLATFORM ${NODE_IMAGE} AS web
# Node >= 25 no longer ships corepack and web/package.json has no
# packageManager field, so pnpm is pinned here explicitly.
ARG PNPM_VERSION=12.21.0
RUN npm install -g pnpm@${PNPM_VERSION} && pnpm --version
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
