#!/usr/bin/env bash
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

export CGO_ENABLED=0

/bin/bash scripts/check-generated.sh

command -v gocyclo >/dev/null 2>&1 || {
	echo "gocyclo is required; run in nix-shell or install github.com/fzipp/gocyclo/cmd/gocyclo@latest"
	exit 1
}

coverage_profile=$(mktemp "${TMPDIR:-/tmp}/public-lib-anthony.coverprofile.XXXXXX")
trap 'rm -f "$coverage_profile"' EXIT

gocyclo -over 8 .
go vet ./...
go build ./...
go test -coverprofile="$coverage_profile" ./...
go tool cover -func="$coverage_profile" | tail -n 1
