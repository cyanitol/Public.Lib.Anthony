#!/usr/bin/env bash

# Add t.Parallel() to test functions - VERSION 2
# Simplified without temp files for counting

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"
TOTAL=0
UPDATED=0

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    tmp=$(mktemp)
    modified=0

    awk '
    BEGIN {
        in_test = 0
        found_func_brace = 0
        func_indent = ""
        range_var = ""
        in_for_range = 0
    }

    # Match test function declaration with opening brace on same line
    /^func Test.*\(t \*testing\.T\) \{$/ {
        print $0
        # Check next line to see if t.Parallel() already exists
        getline
        if ($0 !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print "\tt.Parallel()"
        }
        print $0
        next
    }

    # Detect for _, <var> := range
    /^[\t ]+for _, ([a-zA-Z_][a-zA-Z0-9_]*) := range / {
        print $0
        # Extract range variable
        match($0, /for _, ([a-zA-Z_][a-zA-Z0-9_]*) :=/, arr)
        if (arr[1]) {
            range_var = arr[1]
            in_for_range = 1
        }
        next
    }

    # If we are in a for-range loop and see t.Run
    in_for_range == 1 && /t\.Run\(/ {
        # Get the indent
        indent_match = match($0, /^[\t ]+/)
        current_indent = substr($0, 1, indent_match)

        # Check if range variable capture already exists on next line
        getline next_line
        if (next_line !~ range_var " := " range_var) {
            # Add capture
            print current_indent range_var " := " range_var "  // Capture range variable"
        } else {
            # Already has capture, print it
            print next_line
            print $0
            in_for_range = 0
            next
        }
        print $0
        in_for_range = 0
        next
    }

    # Detect t.Run with inline function
    /t\.Run\(.*func\(t \*testing\.T\) \{$/ {
        print $0
        # Get the indent for the subtest body
        indent_match = match($0, /^[\t ]+/)
        current_indent = substr($0, 1, indent_match) "\t"

        # Check next line
        getline
        if ($0 !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print current_indent "t.Parallel()"
        }
        print $0
        next
    }

    # Default: print the line
    {
        print $0
    }
    ' "$file" > "$tmp"

    # Check if file was modified
    if ! cmp -s "$file" "$tmp"; then
        mv "$tmp" "$file"
        modified=1
        basename=$(basename "$file")
        echo "Modified: $basename"
    else
        rm "$tmp"
    fi

    if [ "$modified" -eq 1 ]; then
        UPDATED=$((UPDATED + 1))
    fi
done

echo ""
echo "=== Summary ==="
echo "Files modified: $UPDATED"
