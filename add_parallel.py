#!/usr/bin/env python3
import os
import re
import sys
from pathlib import Path

def has_parallel(lines, start_idx):
    """Check if t.Parallel() is already present as first line"""
    # Skip past the function signature
    for i in range(start_idx, min(start_idx + 5, len(lines))):
        stripped = lines[i].strip()
        if stripped == 't.Parallel()':
            return True
        # Stop at first non-comment, non-empty line that's not the function signature
        if stripped and not stripped.startswith('//') and not stripped.startswith('func') and '{' not in stripped:
            break
    return False

def get_indent(line):
    """Get the indentation of a line"""
    return line[:len(line) - len(line.lstrip())]

def process_file(filepath):
    """Process a single test file"""
    with open(filepath, 'r') as f:
        lines = f.readlines()

    new_lines = []
    i = 0
    updated = 0
    total = 0

    while i < len(lines):
        line = lines[i]
        new_lines.append(line)

        # Look for test function declarations
        if line.strip().startswith('func Test') and '(t *testing.T)' in line:
            total += 1

            # Find the opening brace
            brace_idx = i
            if '{' not in line:
                brace_idx = i + 1
                while brace_idx < len(lines) and '{' not in lines[brace_idx]:
                    new_lines.append(lines[brace_idx])
                    brace_idx += 1
                if brace_idx < len(lines):
                    new_lines.append(lines[brace_idx])

            # Check if t.Parallel() is already there
            if not has_parallel(lines, brace_idx):
                # Find the first line after the opening brace
                next_idx = brace_idx + 1
                if next_idx < len(lines):
                    # Get indent from the next non-empty line
                    indent = '\t'
                    for j in range(next_idx, min(next_idx + 5, len(lines))):
                        if lines[j].strip():
                            indent = get_indent(lines[j])
                            break

                    # Insert t.Parallel()
                    new_lines.append(indent + 't.Parallel()\n')
                    updated += 1

            i = brace_idx

        i += 1

    # Write back if modified
    if updated > 0:
        with open(filepath, 'w') as f:
            f.writelines(new_lines)

    return updated, total

def add_parallel_to_subtests(filepath):
    """Add t.Parallel() to subtests in t.Run calls and capture range variables"""
    with open(filepath, 'r') as f:
        content = f.read()

    modified = False

    # Pattern 1: for _, tt := range tests with t.Run
    pattern1 = re.compile(
        r'(for _, (\w+) := range \w+.*?\{)\n'
        r'(\s+)(t\.Run\(\2\.\w+, func\(t \*testing\.T\) \{)\n'
        r'(?!\s+\2 := \2)',  # Not already captured
        re.MULTILINE | re.DOTALL
    )

    def replace1(match):
        nonlocal modified
        modified = True
        loop_start = match.group(1)
        var_name = match.group(2)
        indent = match.group(3)
        run_start = match.group(4)
        return f'{loop_start}\n{indent}{var_name} := {var_name}  // Capture range variable\n{indent}{run_start}\n{indent}\tt.Parallel()\n'

    content = pattern1.sub(replace1, content)

    # Pattern 2: for _, tc := range (alternative variable name)
    pattern2 = re.compile(
        r'(for _, (tc|test|tt) := range \w+.*?\{)\n'
        r'(\s+)(t\.Run\([^,]+, func\(t \*testing\.T\) \{)\n'
        r'(?!\s+t\.Parallel\(\))',  # Not already has t.Parallel()
        re.MULTILINE | re.DOTALL
    )

    def replace2(match):
        nonlocal modified
        modified = True
        loop_start = match.group(1)
        var_name = match.group(2)
        indent = match.group(3)
        run_start = match.group(4)
        # Check if capture already exists
        next_line_idx = match.end()
        if var_name + ' := ' + var_name in content[match.start():next_line_idx + 100]:
            return match.group(0)  # Already captured
        return f'{loop_start}\n{indent}{var_name} := {var_name}  // Capture range variable\n{indent}{run_start}\n{indent}\tt.Parallel()\n'

    # Only apply if we're in a for loop context
    lines = content.split('\n')
    new_lines = []
    in_for_loop = False

    for line in lines:
        if re.match(r'\s*for .* := range ', line):
            in_for_loop = True
        elif in_for_loop and re.match(r'\s*}', line) and 'for' not in line:
            in_for_loop = False

        new_lines.append(line)

        # Add t.Parallel() to t.Run if in for loop and not already present
        if in_for_loop and 't.Run(' in line and 'func(t *testing.T) {' in line:
            indent = get_indent(line)
            # Check next line
            next_idx = len(new_lines)
            if next_idx < len(lines):
                next_line = lines[len(new_lines)] if len(new_lines) < len(lines) else ''
                if 't.Parallel()' not in next_line:
                    # We'll handle this in the re-parse
                    pass

    if modified:
        with open(filepath, 'w') as f:
            f.write(content)
        return True

    return False

def main():
    driver_dir = Path('/home/justin/Programming/Workspace/Public.Lib.Anthony/internal/driver')
    test_files = list(driver_dir.glob('*_test.go'))

    total_tests = 0
    total_updated = 0
    files_modified = 0

    for filepath in sorted(test_files):
        updated, total = process_file(filepath)
        total_tests += total
        total_updated += updated
        if updated > 0:
            files_modified += 1
            print(f'Updated {filepath.name}: {updated}/{total} tests')

        # Handle subtests
        if add_parallel_to_subtests(filepath):
            print(f'  Added parallel to subtests in {filepath.name}')

    print(f'\n=== Summary ===')
    print(f'Files modified: {files_modified}')
    print(f'Total test functions: {total_tests}')
    print(f'Updated with t.Parallel(): {total_updated}')

if __name__ == '__main__':
    main()
