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
