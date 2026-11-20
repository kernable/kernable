#!/usr/bin/env bash
# Checks a published release as a user gets it, outside this repository: the binary from
# install.sh on this machine and on the Linux server, the SDKs from npm, PyPI and crates.io
# (installed into fresh projects, imported, their kit copied out), and the GitHub release.
# scripts/release.sh runs it last; it can run on its own any time.
#
#   scripts/verify-release.sh <version>
#   LINUX=root@<host>    the Linux machine (default: ~/.config/kernable/check-linux.server)
#
# A registry may show a new version some minutes late (npm stages it): each is asked again for
# up to 15 minutes.
set -euo pipefail
export PIP_DISABLE_PIP_VERSION_CHECK=1
VERSION=${1:?usage: scripts/verify-release.sh <version>}
SITE=https://kernable.com
CONFIG=$HOME/.config/kernable/check-linux.server
LINUX=${LINUX:-$([ -f "$CONFIG" ] && tr -d '[:space:]' < "$CONFIG")}
DIR=$(mktemp -d /tmp/kv-XXXX)
trap 'rm -rf "$DIR"' EXIT
cd "$DIR"
fail() { echo "FAILED: $*" >&2; exit 1; }
ok() { echo "  ok  $*"; }

# Asks `check` (a command) every 30 s for up to 15 minutes.
eventually() {
  local what=$1
  shift
  for _ in $(seq 30); do
    "$@" >/dev/null 2>&1 && return 0
    sleep 30
  done
  fail "$what: still missing after 15 minutes"
}

echo "== kernable $VERSION as a user gets it"

latest=$(curl -fsSL "$SITE/download/latest")
[ "$latest" = "$VERSION" ] || fail "$SITE/download/latest says $latest"
ok "download/latest is $VERSION"

curl -fsSL "$SITE/install.sh" | KERNABLE_INSTALL="$DIR/bin" sh >/dev/null
"$DIR/bin/kernable" --version | grep -q "$VERSION" || fail "install.sh installed $("$DIR/bin/kernable" --version)"
ok "install.sh on $(uname -sm)"

if [ -n "$LINUX" ]; then
  got=$(ssh -o BatchMode=yes "$LINUX" "t=\$(mktemp -d) && curl -fsSL $SITE/install.sh | KERNABLE_INSTALL=\$t sh >/dev/null && \$t/kernable --version; rm -rf \$t")
  echo "$got" | grep -q "$VERSION" || fail "install.sh on Linux installed: $got"
  ok "install.sh on Linux ($LINUX)"
else
  echo "  --  install.sh on Linux skipped: no LINUX and no $CONFIG"
fi

eventually "@kernable/sdk@$VERSION on npm" npm view "@kernable/sdk@$VERSION" version
mkdir npm && (cd npm && npm init -y >/dev/null && npm pkg set type=module \
  && npm install --silent --no-audit --no-fund "@kernable/sdk@$VERSION" \
  && node -e 'import("@kernable/sdk").then(m => { if (!m.Kernable || !m.domain) process.exit(1) })' \
  && node_modules/.bin/kernable-sdk add kit >/dev/null && [ -f kit/README.md ]) \
  || fail "@kernable/sdk@$VERSION: install, import or kit"
ok "npm: @kernable/sdk $VERSION installs, imports, carries its kit"

pypi() { curl -fsS -o /dev/null "https://pypi.org/pypi/kernable-sdk/$VERSION/json"; }
eventually "kernable-sdk $VERSION on PyPI" pypi
python3 -m venv py
# The index's simple pages may lag its JSON: install until it finds the version.
eventually "pip install kernable-sdk==$VERSION" py/bin/pip install -q --no-cache-dir "kernable-sdk==$VERSION"
(mkdir pykit && cd pykit && ../py/bin/python -c 'from kernable import Kernable, domain' \
  && ../py/bin/kernable-sdk add kit >/dev/null && [ -f kit/README.md ]) \
  || fail "kernable-sdk $VERSION: import or kit"
ok "PyPI: kernable-sdk $VERSION installs, imports, carries its kit"

for crate in kernable-core kernable-sdk-macros kernable-sdk; do
  crate_ok() { curl -fsS -o /dev/null -A 'kernable-release-check' "https://crates.io/api/v1/crates/$crate/$VERSION"; }
  eventually "$crate $VERSION on crates.io" crate_ok
done
cargo new -q --vcs none rs
(cd rs && cargo add -q "kernable-sdk@=$VERSION" && CARGO_TARGET_DIR="$DIR/rs-target" cargo check -q) \
  || fail "kernable-sdk $VERSION from crates.io does not build"
ok "crates.io: kernable-sdk $VERSION builds in a new project"

assets=$(gh release view "v$VERSION" -R kernable/kernable --json assets -q '.assets | length')
[ "$assets" -ge 5 ] || fail "the GitHub release v$VERSION has $assets files, not the 4 archives and SHA256SUMS"
ok "GitHub release v$VERSION with $assets files"

echo "== kernable $VERSION verified"
