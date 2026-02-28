#!/usr/bin/env bash

# Add t.Parallel() to test functions - CORRECT VERSION
# Use sed instead of awk to avoid getline issues

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"
COUNT=0

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    # Pass 1: Add t.Parallel() to test function bodies
    sed -i '/^func Test.*\(t \*testing\.T\) {$/a\
\tt.Parallel()' "$file"

    # Pass 2: Add t.Parallel() to t.Run subtests (if not already there)
    # This is trickier - we need to find t.Run(...func(t *testing.T) {
    # and add t.Parallel() as the first line if not present

    # Use perl for multi-line matching
    perl -i -p0e 's/(t\.Run\([^,]+,\s*func\(t \*testing\.T\)\s*\{\n)(\s+)(?!t\.Parallel)/$1$2t.Parallel()\n$2/msg' "$file"

    # Pass 3: Add range variable capture for table-driven tests
    # Match: for _, tt := range tests {
    # Followed by: t.Run(
    # Insert: tt := tt before t.Run

    perl -i -p0e 's/(for\s*_,\s*(\w+)\s*:=\s*range\s*\w+[^\{]*\{\n)(\s+)(t\.Run\()(?!\2\s*:=\s*\2)/$1$3$2 := $2  \/\/ Capture range variable\n$3$4/msg' "$file"

    COUNT=$((COUNT + 1))
    echo "Processed: $(basename "$file")"
done

echo ""
echo "=== Summary ==="
echo "Files processed: $COUNT"
