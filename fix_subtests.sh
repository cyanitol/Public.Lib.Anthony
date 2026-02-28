#!/usr/bin/env bash

# Fix t.Parallel() placement and add to subtests

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    # Remove misplaced t.Parallel() from struct definitions and other wrong places
    sed -i '/tests := \[\]struct {$/,/^[\t ]*}[\t ]*{$/{/^\t*t\.Parallel()$/d}' "$file"
    sed -i '/r := &Result{$/,/^[\t ]*}$/{/^\t*t\.Parallel()$/d}' "$file"

    # Process t.Run subtests - add t.Parallel() if not present
    # This is a simple approach: look for "t.Run" followed by "func(t *testing.T) {"
    # and ensure next non-empty line is t.Parallel()

    tmp=$(mktemp)
    awk '
    BEGIN {
        in_subtest = 0
        pending_parallel = 0
    }

    # Detect t.Run with function literal
    /t\.Run\(.*func\(t \*testing\.T\) \{/ {
        print $0
        in_subtest = 1
        pending_parallel = 1
        next
    }

    # If we need to add t.Parallel() in subtest
    pending_parallel == 1 {
        # Skip empty lines or comments
        if ($0 ~ /^[\t ]*$/ || $0 ~ /^[\t ]*\/\//) {
            print $0
            next
        }

        # Check if already has t.Parallel()
        if ($0 !~ /t\.Parallel\(\)/) {
            # Add it before this line
            print "\t\t\tt.Parallel()"
        }
        pending_parallel = 0
        in_subtest = 0
        print $0
        next
    }

    # Default: print the line
    {
        print $0
    }
    ' "$file" > "$tmp"

    mv "$tmp" "$file"
done

echo "Fixed subtest parallel calls"
