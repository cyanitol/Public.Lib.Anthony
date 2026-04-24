.PHONY: all build test test-fast test-ci test-short test-cover test-cover-report test-cover-func test-race lint check clean help commit check-spdx check-complexity check-complexity-all check-complexity-prod check-fmt bench vet quality-gates complexity complexity-all complexity-prod

# Test configuration
TEST_PARALLEL ?= 4
TEST_PKG_PARALLEL ?= 8
COMPLEXITY_MAX ?= 8
PROD_COMPLEXITY_MAX ?= 8

# Default target
all: test build

# Build the project
build:
	go build ./...


# Run performance comparison benchmark (Anthony vs sqlite3 if available)
bench:
	@export CGO_ENABLED=0; \
	BENCH_DURATION=$${BENCH_DURATION:-300}; \
	if command -v sqlite3 >/dev/null 2>&1; then \
		BENCH_DURATION=$$BENCH_DURATION /bin/bash scripts/bench.sh; \
	elif [ -x /nix/var/nix/profiles/default/bin/nix-shell ]; then \
		/nix/var/nix/profiles/default/bin/nix-shell --run "BENCH_DURATION=$$BENCH_DURATION /bin/bash scripts/bench.sh"; \
	else \
		echo "sqlite3 not found; running Anthony benchmark only"; \
		BENCH_DURATION=$$BENCH_DURATION SKIP_SQLITE=1 /bin/bash scripts/bench.sh; \
	fi

# Run all tests
test:
	CGO_ENABLED=0 go test ./...

# Fast parallel testing for development
test-fast:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) ./...

# CI testing with JSON output and trimpath
test-ci:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) -trimpath -json ./...

# Short mode skipping slow tests
test-short:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) -short ./...

# Run tests with coverage
test-cover:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) -cover ./...

# Run tests with coverage report
test-cover-report:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with coverage and show functions
test-cover-func:
	CGO_ENABLED=0 go test -p $(TEST_PKG_PARALLEL) -parallel $(TEST_PARALLEL) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Run tests with race detection (CGO required for -race, but set explicitly)
test-race:
	CGO_ENABLED=1 go test -race ./...

# Run cyclomatic complexity check across the whole tree (including tests)
complexity: complexity-all

complexity-all:
	@which gocyclo > /dev/null || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	gocyclo -over $(COMPLEXITY_MAX) .

# Run cyclomatic complexity check for production code only
complexity-prod:
	@which gocyclo > /dev/null || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	gocyclo -over $(PROD_COMPLEXITY_MAX) . | grep -v '_test.go' || true

# Run go vet
vet:
	go vet ./...

# Format code
fmt:
	go fmt ./...

# Tidy dependencies
tidy:
	go mod tidy

# Clean generated files
clean:
	rm -f *.out *.cov *.html coverage.txt
	rm -f internal/driver/coverage.out
	rm -f test_explain driver.test
	rm -rf build/
	go clean -testcache

# Full clean including test cache and go cache
clean-all: clean
	rm -rf .gocache/
	go clean -cache -testcache

# Static analysis: fmt, spdx, complexity, vet
lint: check-fmt check-spdx check-complexity vet
	@echo ""
	@echo "All lint checks passed."

# Fast checks (lint + build) without running tests
check: lint build
	@echo ""
	@echo "All checks passed."

# Pre-commit validation - run before committing
commit: check test
	@echo ""
	@echo "✓ All pre-commit checks passed!"
	@echo "Ready to commit."

# Publication-grade quality gates
quality-gates: check-fmt check-spdx check-complexity vet build test
	@echo ""
	@echo "All quality gates passed."

# Check that go fmt produces no changes
check-fmt:
	@echo "Checking go fmt..."
	@test -z "$$(gofmt -l .)" || (echo "Files need formatting:"; gofmt -l .; exit 1)
	@echo "✓ Code is properly formatted"

# Check that all .go files have SPDX license headers
check-spdx:
	@echo "Checking SPDX headers..."
	@missing=$$(find . -name '*.go' -not -path './vendor/*' -exec grep -L 'SPDX-License-Identifier' {} \;); \
	if [ -n "$$missing" ]; then \
		echo "Files missing SPDX-License-Identifier:"; \
		echo "$$missing"; \
		exit 1; \
	fi
	@echo "✓ All files have SPDX headers"

# Check cyclomatic complexity (whole tree, including tests)
check-complexity:
	@echo "Checking cyclomatic complexity (max $(COMPLEXITY_MAX), whole tree)..."
	@which gocyclo > /dev/null || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	@violations=$$(gocyclo -over $(COMPLEXITY_MAX) . 2>/dev/null || true); \
	if [ -n "$$violations" ]; then \
		echo "Functions with complexity > $(COMPLEXITY_MAX):"; \
		echo "$$violations"; \
		exit 1; \
	fi
	@echo "✓ All functions have complexity ≤ $(COMPLEXITY_MAX)"

# Explicit alias for whole-tree complexity gate
check-complexity-all: check-complexity

# Check cyclomatic complexity for production code only
check-complexity-prod:
	@echo "Checking production cyclomatic complexity (max $(PROD_COMPLEXITY_MAX))..."
	@which gocyclo > /dev/null || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	@violations=$$(gocyclo -over $(PROD_COMPLEXITY_MAX) . 2>/dev/null | grep -v '_test.go' || true); \
	if [ -n "$$violations" ]; then \
		echo "Production functions with complexity > $(PROD_COMPLEXITY_MAX):"; \
		echo "$$violations"; \
		exit 1; \
	fi
	@echo "✓ All production functions have complexity ≤ $(PROD_COMPLEXITY_MAX)"

# Show help
help:
	@echo "Available targets:"
	@echo "  all              - Run tests and build"
	@echo "  build            - Build the project"
	@echo "  test             - Run all tests"
	@echo "  test-fast        - Fast parallel testing for development"
	@echo "  test-ci          - CI testing with JSON output and trimpath"
	@echo "  test-short       - Short mode skipping slow tests"
	@echo "  test-cover       - Run tests with coverage summary"
	@echo "  test-cover-report - Generate HTML coverage report"
	@echo "  test-cover-func  - Show per-function coverage"
	@echo "  test-race        - Run tests with race detection"
	@echo "  complexity       - Check cyclomatic complexity across the whole tree"
	@echo "  complexity-all   - Print whole-tree complexity violations over COMPLEXITY_MAX"
	@echo "  complexity-prod  - Print production-only complexity violations over PROD_COMPLEXITY_MAX"
	@echo "  vet              - Run go vet"
	@echo "  fmt              - Format code"
	@echo "  tidy             - Tidy go.mod"
	@echo "  clean            - Remove generated files"
	@echo "  clean-all        - Remove all caches and generated files"
	@echo "  lint             - Static analysis (fmt, spdx, complexity, vet)"
	@echo "  check            - Lint + build (no tests)"
	@echo "  commit           - Full pre-commit validation (check + test)"
	@echo "  quality-gates    - Full release-quality gate run (fmt, spdx, complexity, vet, build, test)"
	@echo "  check-fmt        - Check code is formatted"
	@echo "  check-spdx       - Check SPDX headers"
	@echo "  check-complexity - Enforce whole-tree cyclomatic complexity ≤ COMPLEXITY_MAX"
	@echo "  check-complexity-all - Alias for whole-tree complexity gate"
	@echo "  check-complexity-prod - Enforce production cyclomatic complexity ≤ PROD_COMPLEXITY_MAX"
	@echo "  help             - Show this help"
