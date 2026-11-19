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

# Nothing internal: no IDs of kernable's own decisions and tasks (P58, A126, …), no public IPv4
# address (servers kernable runs or tests on); documentation and test ranges, private and
# loopback addresses are fine. SVG paths and binary fuzz seeds look like addresses: left out.
internal=$(git grep -nIP '\b(P\d{2}|A\d{2,3})\b' -- . ':!*.lock' ':!*package-lock.json' ':!bench/results-*' \
  ':!scripts/check-open.sh')
addresses=$(git grep -nIP '(?<![\d.])\d{1,3}(\.\d{1,3}){3}(?![\d.])' -- . ':!*.lock' ':!*package-lock.json' \
  ':!docs/.vitepress/theme/flow/icons.js' ':!scripts/check-open.sh' \
  | grep -vE '(^|[^0-9.])(127|10|0)\.[0-9]|192\.168\.|172\.(1[6-9]|2[0-9]|3[01])\.|203\.0\.113\.|198\.51\.100\.|192\.0\.2\.|255\.255\.')
if [ -n "$internal$addresses" ]; then
  echo "kernable names something internal (a decision or task ID, a server's address):" >&2
  printf '%s\n' "$internal" "$addresses" | sed '/^$/d' >&2
  exit 1
fi
echo "open: no reference to kernable Cloud's code"
