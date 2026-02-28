#!/usr/bin/env bash

# Add t.Parallel() to test functions - FINAL VERSION
# This script properly handles:
# 1. Adding t.Parallel() as first line after function declaration
# 2. Adding t.Parallel() inside t.Run() subtests
# 3. Adding range variable capture for table-driven tests

DIR="/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver"
TOTAL=0
UPDATED=0

for file in "$DIR"/*_test.go; do
    [ -f "$file" ] || continue

    tmp=$(mktemp)

    awk '
    BEGIN {
        in_test = 0
        found_func_brace = 0
        func_indent = ""
        test_count = 0
        updated_count = 0
        range_var = ""
        in_for_range = 0
    }

    # Match test function declaration
    /^func Test.*\(t \*testing\.T\) \{$/ {
        print $0
        in_test = 1
        found_func_brace = 1
        test_count++
        # Get indentation (should be one tab)
        func_indent = "\t"
        # Check next line to see if t.Parallel() already exists
        getline
        if ($0 !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print func_indent "t.Parallel()"
            updated_count++
        }
        in_test = 0
        print $0
        next
    }

    # Match test function declaration split across lines
    /^func Test.*\(t \*testing\.T\)/ && !/\{/ {
        print $0
        in_test = 1
        found_func_brace = 0
        test_count++
        func_indent = "\t"
        next
    }

    # Found opening brace of test function
    in_test == 1 && found_func_brace == 0 && /^[\t ]*\{/ {
        print $0
        found_func_brace = 1
        # Check next line
        getline
        if ($0 !~ /^[\t ]+t\.Parallel\(\)/) {
            # Add t.Parallel()
            print func_indent "t.Parallel()"
            updated_count++
        }
        in_test = 0
        print $0
        next
    }

    # Detect for _, <var> := range <tests>
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

    END {
        # Write counts to temp files for parent script
        print test_count > "/tmp/test_count_" FILENAME
        print updated_count > "/tmp/updated_count_" FILENAME
    }
    ' "$file" > "$tmp"

    # Get counts
    base=$(basename "$file")
    if [ -f "/tmp/test_count_$file" ]; then
        tests=$(cat "/tmp/test_count_$file")
        updated=$(cat "/tmp/updated_count_$file")
        rm "/tmp/test_count_$file" "/tmp/updated_count_$file"

        if [ "$updated" -gt 0 ]; then
            mv "$tmp" "$file"
            TOTAL=$((TOTAL + tests))
            UPDATED=$((UPDATED + updated))
            echo "Updated $base: $updated/$tests tests"
        else
            rm "$tmp"
        fi
    else
        rm "$tmp"
    fi
done

echo ""
echo "=== Summary ==="
echo "Total test functions processed: $TOTAL"
echo "Updated with t.Parallel(): $UPDATED"
