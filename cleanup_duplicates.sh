#!/usr/bin/env bash

# Remove duplicate t.Parallel() and misplaced ones

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    # Remove lines that are just "t.Parallel()" and immediately preceded by "if err != nil {"
    # These are incorrectly placed
    sed -i '/if err != nil {$/{N;s/if err != nil {\n\tt\.Parallel()/if err != nil {/}' "$file"
    sed -i '/if err != nil {$/{N;s/if err != nil {\n\t\tt\.Parallel()/if err != nil {/}' "$file"
done

echo "Cleanup complete"
