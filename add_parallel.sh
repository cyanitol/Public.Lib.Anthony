#!/usr/bin/env bash

# Add t.Parallel() to test functions

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"
TOTAL=0
UPDATED=0

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    # Create a temp file
    tmp=$(mktemp)

    # Process the file
    awk '
    BEGIN {
        in_test = 0
        found_brace = 0
        test_line = ""
        modified = 0
    }

    # Match test function declaration
    /^func Test.*\(t \*testing\.T\)/ {
        in_test = 1
        found_brace = 0
        test_line = $0
        print $0
        next
    }

    # If in test function, look for opening brace
    in_test == 1 && /{/ {
        found_brace = 1
        print $0
        # Check if next line already has t.Parallel()
        getline
        if ($0 !~ /t\.Parallel\(\)/) {
            # Add t.Parallel() with proper indentation
            print "\tt.Parallel()"
            modified++
        }
        print $0
        in_test = 0
        next
    }

    # Print all other lines
    {
        print $0
    }

    END {
        print modified > "/tmp/modified_count"
    }
    ' "$file" > "$tmp"

    # Get modification count
    if [ -f /tmp/modified_count ]; then
        mod_count=$(cat /tmp/modified_count)
        rm /tmp/modified_count
        if [ "$mod_count" -gt 0 ]; then
            mv "$tmp" "$file"
            UPDATED=$((UPDATED + mod_count))
            echo "Updated $(basename "$file"): $mod_count tests"
        else
            rm "$tmp"
        fi
    else
        rm "$tmp"
    fi

    # Count total test functions
    test_count=$(grep -c "^func Test.*testing\.T)" "$file")
    TOTAL=$((TOTAL + test_count))
done

echo ""
echo "=== Summary ==="
echo "Total test functions: $TOTAL"
echo "Updated with t.Parallel(): $UPDATED"
