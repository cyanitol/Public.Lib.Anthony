.PHONY: all build test test-fast test-ci test-short test-cover test-race lint clean help

# Test configuration
TEST_PARALLEL ?= 4
TEST_PKG_PARALLEL ?= 8

# Default target
all: test build

# Build the project
build:
	go build ./...

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
	go test -cover ./...

# Run tests with coverage report
test-cover-report:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run tests with coverage and show functions
test-cover-func:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Run tests with race detection
test-race:
	go test -race ./...

# Run cyclomatic complexity check
complexity:
	@which gocyclo > /dev/null || go install github.com/fzipp/gocyclo/cmd/gocyclo@latest
	gocyclo -over 9 .

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
	rm -f test_explain
	go clean -testcache

# Full clean including test cache
clean-all: clean
	go clean -cache -testcache

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
	@echo "  complexity       - Check cyclomatic complexity"
	@echo "  vet              - Run go vet"
	@echo "  fmt              - Format code"
	@echo "  tidy             - Tidy go.mod"
	@echo "  clean            - Remove generated files"
	@echo "  clean-all        - Remove all caches and generated files"
	@echo "  help             - Show this help"
