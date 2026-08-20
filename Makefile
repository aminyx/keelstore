# keelstore build and verification.
#
# There is no CI in this repository. `make check` is the gate: it is what
# the git pre-commit hook runs, and it is what the README documents.

GO       ?= go
PKG      := ./...
BIN      := bin
FUZZTIME ?= 30s

# The module targets Go 1.25 and relies on the toolchain line in go.mod to
# fetch it when the local go is older. buildvcs is off so that the tree
# builds before it is a git repository.
export GOFLAGS := -buildvcs=false

ifeq ($(OS),Windows_NT)
EXE := .exe
else
EXE :=
endif

# The race detector needs a C toolchain, which a Windows development box
# usually does not have. Docker does, so `make race` runs the suite inside
# the pinned image instead of skipping the check.
RACE_IMAGE ?= golang:1.25
RACE_MOUNT ?= $(CURDIR)

.DEFAULT_GOAL := help

.PHONY: help
help: ## list the targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.PHONY: check
check: fmtcheck vet lint test build bench-smoke ## the full gate: format, vet, lint, test, build, benchmark smoke
	@echo "==> check: all clear"

.PHONY: fmt
fmt: ## rewrite every file with gofmt -s
	gofmt -s -w .

.PHONY: fmtcheck
fmtcheck: ## fail if anything is unformatted
	@echo "==> gofmt"
	@out="$$(gofmt -s -l .)"; \
	if [ -n "$$out" ]; then \
		echo "these files need gofmt -s -w:"; echo "$$out"; exit 1; \
	fi
	@echo "    clean"

.PHONY: vet
vet: ## run go vet
	@echo "==> go vet"
	@$(GO) vet $(PKG)
	@echo "    clean"

.PHONY: lint
lint: ## run staticcheck when it is installed
	@echo "==> lint"
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck $(PKG) && echo "    staticcheck clean"; \
	else \
		echo "    staticcheck not installed, skipping (go vet already ran)"; \
	fi

.PHONY: test
test: ## run the test suite
	@echo "==> go test"
	@$(GO) test $(PKG)

.PHONY: testv
testv: ## run the test suite verbosely
	$(GO) test -v $(PKG)

.PHONY: cover
cover: ## run the tests and report coverage
	$(GO) test -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: race
race: ## run the tests under the race detector, in Docker (no C toolchain needed locally)
	@echo "==> go test -race (in $(RACE_IMAGE))"
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(RACE_MOUNT):/app" -w /app $(RACE_IMAGE) \
		go test -race $(PKG)

.PHONY: build
build: ## build cmd/keelctl into ./bin
	@echo "==> go build"
	@$(GO) build -o $(BIN)/keelctl$(EXE) ./cmd/keelctl
	@echo "    $(BIN)/keelctl$(EXE)"

.PHONY: bench-smoke
bench-smoke: ## run every benchmark once, just to prove they compile and run
	@echo "==> benchmark smoke test"
	@$(GO) test -run='^$$' -bench=. -benchtime=1x $(PKG) > /dev/null
	@echo "    all benchmarks ran"

.PHONY: bench
bench: ## run the benchmarks for real
	$(GO) test -run='^$$' -bench=. -benchtime=3s -benchmem .

.PHONY: fuzz
fuzz: ## fuzz the write path (override with FUZZTIME=5m)
	$(GO) test -run='^$$' -fuzz=FuzzPutGet -fuzztime=$(FUZZTIME) .

.PHONY: hooks
hooks: ## point git at the hooks in .githooks
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed; it runs 'make check'"

.PHONY: doc
doc: ## print the package documentation
	$(GO) doc -all .

.PHONY: clean
clean: ## remove build output and cached test results
	$(GO) clean -testcache
	rm -rf $(BIN) coverage.out keeldata
