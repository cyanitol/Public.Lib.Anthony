#!/usr/bin/env bash
# SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

needs_formatting=$(gofmt -l .)
if [ -n "$needs_formatting" ]; then
	echo "Files need formatting:"
	printf '%s\n' "$needs_formatting"
	exit 1
fi

echo "All Go files are properly formatted."
