GO        ?= go
BIN       := bin
PKGS      := ./...

.PHONY: all build test test-race lint fmt vet web clean

all: lint test build

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test $(PKGS)

test-race:
	$(GO) test -race $(PKGS)

vet:
	$(GO) vet $(PKGS)

fmt:
	gofmt -w $$(git ls-files '*.go')

lint:
	@test -z "$$(gofmt -l $$(git ls-files '*.go'))" || (gofmt -l $$(git ls-files '*.go'); echo "gofmt needed"; exit 1)
	$(GO) vet $(PKGS)
	golangci-lint run $(PKGS)

web:
	cd web && pnpm install --frozen-lockfile && pnpm build

clean:
	rm -rf $(BIN)

# ---- packaging (DevOps) ----------------------------------------------------
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE      ?= chronos:dev
HELM_IMAGE ?= alpine/helm:3.19.0
CHART      := deploy/helm/chronos
# Use a host helm when installed, otherwise the pinned container.
HELM       ?= $(shell command -v helm 2>/dev/null || echo 'docker run --rm -v $(CURDIR):/w -w /w $(HELM_IMAGE)')

.PHONY: image kind-demo helm-lint build-plugin

image:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

kind-demo:
	./hack/kind-demo.sh

helm-lint:
	$(HELM) lint --strict $(CHART)
	$(HELM) template chronos $(CHART) --namespace chronos >/dev/null

build-plugin:
	@if [ -d cmd/kubectl-at ]; then \
		CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/kubectl-at ./cmd/kubectl-at; \
		echo "built $(BIN)/kubectl-at"; \
	else \
		echo "cmd/kubectl-at not present on this branch; nothing to build"; \
	fi
