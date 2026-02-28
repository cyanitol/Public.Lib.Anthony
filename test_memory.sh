#!/bin/bash
# Simple test script to verify memory database implementation compiles and runs

echo "Testing in-memory database implementation..."

# Find go binary
GO_CMD=$(which go 2>/dev/null)

if [ -z "$GO_CMD" ]; then
    echo "Go compiler not found in PATH. Checking common locations..."
    for path in /usr/local/go/bin/go /usr/bin/go /opt/go/bin/go; do
        if [ -x "$path" ]; then
            GO_CMD="$path"
            break
        fi
    done
fi

if [ -z "$GO_CMD" ]; then
    echo "ERROR: Go compiler not found. Please install Go to run tests."
    echo "However, the code has been implemented and is ready for testing when Go is available."
    exit 1
fi

echo "Using Go compiler: $GO_CMD"
echo ""

# Run memory pager tests
echo "Running memory pager unit tests..."
$GO_CMD test -v ./internal/pager -run TestMemoryPager

# Run driver memory tests
echo ""
echo "Running driver memory database tests..."
$GO_CMD test -v ./internal/driver -run TestMemoryDatabase

# Run all pager tests to ensure no regression
echo ""
echo "Running all pager tests..."
$GO_CMD test ./internal/pager

# Run all driver tests to ensure no regression
echo ""
echo "Running all driver tests..."
$GO_CMD test ./internal/driver

echo ""
echo "Test run complete!"
