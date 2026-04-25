#!/usr/bin/env bash
# SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)
set -euo pipefail

repo_root=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

missing=0
while IFS= read -r -d '' file; do
	if ! head -20 "$file" | grep -q "SPDX-License-Identifier:"; then
		echo "MISSING SPDX header: $file"
		missing=$((missing + 1))
	fi
done < <(git ls-files -z -- '*.go' '*.sh')

if [ "$missing" -gt 0 ]; then
	echo
	echo "ERROR: $missing file(s) are missing SPDX-License-Identifier headers."
	echo "Add one of these as the SPDX header near the top of each file:"
	echo "  // SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)"
	echo "  # SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-or-later OR CC0-1.0 OR BSD-3-Clause)"
	exit 1
fi

echo "All tracked Go and shell files have SPDX headers."
