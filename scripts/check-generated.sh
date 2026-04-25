#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

go generate ./...

generated_files=(
	internal/driver/test_compare_helpers_test.go
)

if ! git diff --quiet -- "${generated_files[@]}"; then
	echo "Generated files are out of date:"
	git diff --stat -- "${generated_files[@]}"
	git diff -- "${generated_files[@]}"
	exit 1
fi

status_output=$(git status --porcelain --untracked-files=all -- "${generated_files[@]}")
if [ -n "$status_output" ]; then
	echo "Generated files left the worktree dirty:"
	printf '%s\n' "$status_output"
	exit 1
fi
