GO := $(shell command -v go 2>/dev/null || echo .tools/go/bin/go)
.PHONY: build test check dev
build:
	cd web && npm ci && npm run build
	$(GO) build -trimpath -o bin/relay ./cmd/relay
test:
	$(GO) test -race -count=1 -timeout=180s ./...
	cd web && npm test
check:
	$(GO) vet ./...
	cd web && npm run build && npm audit
dev:
	./scripts/dev.sh
