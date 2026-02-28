#!/usr/bin/env bash

# Add range variable capture for table-driven tests

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    tmp=$(mktemp)

    awk '
    BEGIN {
        in_for_range = 0
        range_var = ""
        for_indent = ""
    }

    # Detect for _, tt := range tests (or similar patterns)
    /for _, .*:= range .* {$/ {
        print $0
        # Extract the range variable name (e.g., "tt", "tc", "test")
        match($0, /for _, ([a-zA-Z_][a-zA-Z0-9_]*) :=/, arr)
        if (arr[1]) {
            range_var = arr[1]
            in_for_range = 1
            for_indent = substr($0, 1, match($0, /for/)-1)
        }
        next
    }

    # If we found a for range and next line is t.Run
    in_for_range == 1 && /t\.Run\(/ {
        # Check if range variable capture already exists
        getline next_line
        if (next_line !~ range_var " := " range_var) {
            # Add range variable capture
            print for_indent "\t" range_var " := " range_var "  // Capture range variable"
        } else {
            # Already has capture, print it
            print next_line
            in_for_range = 0
            range_var = ""
            next
        }
        print $0
        in_for_range = 0
        range_var = ""
        next
    }

    # If we see a closing brace at the original indentation, we exited the for loop
    in_for_range == 1 && $0 ~ "^" for_indent "}" {
        in_for_range = 0
        range_var = ""
    }

    # Default: print the line
    {
        print $0
    }
    ' "$file" > "$tmp"

    mv "$tmp" "$file"
done

echo "Added range variable captures"
