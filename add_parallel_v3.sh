#!/usr/bin/env bash

# Add t.Parallel() to test functions - VERSION 3
# Fixed getline issue

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    tmp=$(mktemp)

    awk '
    # Match test function declaration with opening brace on same line
    /^func Test.*\(t \*testing\.T\) \{$/ {
        print $0
        # Peek at next line
        next_line_pos = NR + 1
        getline next_line
        if (next_line !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print "\tt.Parallel()"
        }
        print next_line
        next
    }

    # Detect for _, <var> := range
    /^[\t ]+for _, / && /:= range / {
        print $0
        # Extract range variable name
        match($0, /for _, ([a-zA-Z_][a-zA-Z0-9_]*) :=/, arr)
        range_var = arr[1]

        # Peek at next line to see if it is t.Run
        getline next_line
        if (next_line ~ /t\.Run\(/ && range_var) {
            # Check line after t.Run for range variable capture
            getline after_run
            if (after_run !~ range_var " := " range_var) {
                # Need to add capture before t.Run
                indent_match = match(next_line, /^[\t ]+/)
                current_indent = substr(next_line, 1, indent_match)
                print current_indent range_var " := " range_var "  // Capture range variable"
            } else {
                # Already has capture
                print after_run
            }
            print next_line
        } else {
            # Not followed by t.Run
            print next_line
        }
        next
    }

    # Detect t.Run with inline function
    /t\.Run\(.*func\(t \*testing\.T\) \{$/ {
        print $0
        # Get the indent for the subtest body
        indent_match = match($0, /^[\t ]+/)
        current_indent = substr($0, 1, indent_match) "\t"

        # Peek at next line
        getline next_line
        if (next_line !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print current_indent "t.Parallel()"
        }
        print next_line
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
        basename=$(basename "$file")
        echo "Modified: $basename"
    else
        rm "$tmp"
    fi
done

echo ""
echo "=== Complete ==="
