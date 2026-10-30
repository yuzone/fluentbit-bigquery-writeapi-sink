GO ?= go
INTEGRATION_TESTS := ^Test(Pipeline|ExactlyOnce|ErrorHandlingDefault|ErrorHandlingExactlyOnce|DefaultValue)$$

.PHONY: build test vet e2e e2e-build e2e-down

build: ## Build the plugin for the host platform
	CGO_ENABLED=1 $(GO) build -buildmode=c-shared -o out_writeapi.so

vet:
	$(GO) vet ./...

test: ## Unit tests (integration tests need GCP credentials and are skipped)
	$(GO) test -race -skip "$(INTEGRATION_TESTS)" ./...

e2e-build: ## Build the linux plugin with Dockerfile.build into hack/e2e/build/
	docker build -f Dockerfile.build --target artifact --output type=local,dest=hack/e2e/build .

e2e: e2e-build ## Run the Docker-based end-to-end scenarios (needs Docker only)
	$(GO) test -tags e2e -count=1 -v -timeout 15m ./hack/e2e

e2e-down: ## Tear down a stack left running with E2E_KEEP=1
	cd hack/e2e && docker compose down --remove-orphans --volumes
