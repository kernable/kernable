#!/usr/bin/env bash
# The line between open kernable and kernable Cloud: this repository never
# refers to kernable Cloud's code or internals. It may name the product and talk to its public
# API (`kernable cloud …`), nothing else: no crate of the cloud, no path into its repository.
# Run by `make test` and so in CI. A line that must stay goes into ALLOWED (file:line, exactly),
# with why.
set -uo pipefail
cd "$(dirname "$0")/.."

# The cloud's crates and its repository.
FORBIDDEN='kernable-(host|control|ingress|state|account)([^a-z0-9_-]|$)|kernable-cloud'

ALLOWED=(
  # The service name the CLI stores the cloud's token under in the OS keychain: a name, not code.
  'crates/kernable-kernel/src/cloud.rs:const KEYCHAIN_SERVICE: &str = "kernable-cloud";'
)

found=$(git grep -E "$FORBIDDEN" -- . ':!*.lock' ':!scripts/check-open.sh' \
  | grep -vxF -f <(printf '%s\n' "${ALLOWED[@]}"))
if [ -n "$found" ]; then
  echo "kernable refers to kernable Cloud's code or internals:" >&2
  printf '%s\n' "$found" >&2
  exit 1
fi
echo "open: no reference to kernable Cloud's code"
