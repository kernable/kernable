#!/usr/bin/env bash
# Releases kernable <version>, from the version number to a verified release, in steps that each
# hold before the next one starts. Running it is the decision to release. See RELEASING.md.
#
#   scripts/release.sh <version>                 all steps not done yet for <version>
#   scripts/release.sh <version> --from <step>   again from <step> on (the ones before stay done)
#   scripts/release.sh <version> --tokens        publish the SDKs from here with the tokens in
#                                                ~/.config/kernable/{crates,npm,pypi}.token
#                                                instead of GitHub's release workflow
#
# Steps (a step that held is noted in dist/<version>/.done and skipped on the next run, so a
# release stopped anywhere goes on where it stopped):
#   preflight  main, clean, up to date with origin; the tag free; the servers reachable
#   version    the version in every manifest and lock file, committed (not pushed)
#   tests      make check and make check-packages here, make check and make flaky on Linux
#   binaries   scripts/build-binaries.sh: four archives and SHA256SUMS in dist/<version>/
#   notes      dist/<version>/NOTES.md, the release text: when missing, drafted from the commits
#              since the last tag, and the script stops for it to be edited; the next run takes
#              it as it is (written beforehand, it is taken at once)
#   push       main and the tag v<version> to origin
#   publish    GitHub's release workflow publishes crates.io, npm and PyPI (Trusted Publishing,
#              no tokens): it waits for approval in the `release` environment; this waits for it
#   downloads  the archives to kernable.com/download/<version>/, then download/latest
#   github     the GitHub release: NOTES.md, the archives, SHA256SUMS
#   docs       the documentation on kernable.com (make deploy-docs in ../kernable.com)
#   verify     scripts/verify-release.sh: everything as a user gets it
#
# Servers, never in this repository: ~/.config/kernable/site.server (kernable.com, root over
# SSH) and ~/.config/kernable/check-linux.server (the Linux test machine).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
STEPS=(preflight version tests binaries notes push publish downloads github docs verify)
VERSION=${1:?usage: scripts/release.sh <version> [--from <step>] [--tokens]}
shift
FROM=
TOKENS=0
while [ $# -gt 0 ]; do
  case $1 in
    --from) FROM=${2:?--from needs a step: ${STEPS[*]}}; shift 2 ;;
    --tokens) TOKENS=1; shift ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "a version is x.y.z, not $VERSION" >&2; exit 2; }
TAG=v$VERSION
DIST=$ROOT/dist/$VERSION
DONE=$DIST/.done
LOGS=$DIST/logs
REPO=kernable/kernable
CONF=$HOME/.config/kernable
SITE_SERVER=$(tr -d '[:space:]' < "$CONF/site.server" 2>/dev/null || true)
LINUX=$(tr -d '[:space:]' < "$CONF/check-linux.server" 2>/dev/null || true)
WEBROOT=/var/www/kernable.com
SITE_REPO=${SITE_REPO:-$ROOT/../kernable.com}
mkdir -p "$LOGS"
touch "$DONE"
cd "$ROOT"

say() { echo "== $(date +%H:%M:%S) $*"; }
fail() { echo "release $VERSION stopped: $*" >&2; exit 1; }
# Runs a command with its output in dist/<version>/logs/<name>.log; on failure, its end.
logged() {
  local name=$1
  shift
  "$@" >"$LOGS/$name.log" 2>&1 || { tail -40 "$LOGS/$name.log"; fail "$name failed (all of it: $LOGS/$name.log)"; }
}
current() { sed -n '/^\[workspace.package\]/,/^\[/s/^version = "\(.*\)"/\1/p' Cargo.toml; }

if [ -n "$FROM" ]; then
  printf '%s\n' "${STEPS[@]}" | grep -qx "$FROM" || fail "no step $FROM: ${STEPS[*]}"
  keep=()
  for s in "${STEPS[@]}"; do [ "$s" = "$FROM" ] && break; keep+=("$s"); done
  { [ ${#keep[@]} -gt 0 ] && grep -xF -f <(printf '%s\n' "${keep[@]}") "$DONE" || true; } > "$DONE.new"
  mv "$DONE.new" "$DONE"
fi

step_preflight() {
  [ "$(git branch --show-current)" = main ] || fail "not on main"
  [ -z "$(git status --porcelain --untracked-files=no)" ] || fail "uncommitted changes: commit or stash them first"
  git fetch -q origin main --tags
  git merge-base --is-ancestor origin/main HEAD || fail "origin/main has commits this checkout lacks: pull first"
  ! git rev-parse -q --verify "refs/tags/$TAG" >/dev/null || fail "the tag $TAG exists already"
  local was
  was=$(git describe --tags --abbrev=0 2>/dev/null || echo none)
  echo "  last release $was, this one $VERSION"
  gh auth status >/dev/null 2>&1 || fail "gh is not logged in"
  command -v cargo-zigbuild >/dev/null || fail "cargo zigbuild is missing (cargo install cargo-zigbuild)"
  [ -n "$SITE_SERVER" ] || fail "no $CONF/site.server (root@<kernable.com's address>)"
  [ -n "$LINUX" ] || fail "no $CONF/check-linux.server"
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$SITE_SERVER" true || fail "cannot reach $SITE_SERVER"
  ssh -o BatchMode=yes -o ConnectTimeout=10 "$LINUX" true || fail "cannot reach $LINUX"
  [ -d "$SITE_REPO" ] || fail "no kernable.com repository at $SITE_REPO (SITE_REPO=...)"
  if [ $TOKENS = 1 ]; then
    for t in crates npm pypi; do [ -s "$CONF/$t.token" ] || fail "--tokens: no $CONF/$t.token"; done
  fi
}

step_version() {
  local was
  was=$(current)
  if [ "$was" = "$VERSION" ]; then
    echo "  the manifests say $VERSION already"
    return
  fi
  python3 - "$was" "$VERSION" <<'PY'
import json, re, sys
from pathlib import Path
was, new = sys.argv[1], sys.argv[2]

def sub(path, pattern, repl, count=1):
    p = Path(path)
    text, n = re.subn(pattern, repl, p.read_text(), count=count, flags=re.M)
    if n != count:
        sys.exit(f"{path}: expected {count} version line(s), found {n}")
    p.write_text(text)

sub("Cargo.toml", r'^(\[workspace\.package\]\n(?:.*\n)*?version = )"[^"]*"', rf'\g<1>"{new}"')
sub("kits/rust/Cargo.toml", r'^version = "[^"]*"', f'version = "{new}"')
sub("sdks/rust/Cargo.toml", r'version = "=[^"]*"', f'version = "={new}"', count=2)
sub("sdks/python/pyproject.toml", r'^version = "[^"]*"', f'version = "{new}"')

def lock(path, entry, root=False):
    p = Path(path)
    data = json.loads(p.read_text())
    if root:
        data["version"] = new
    data["packages"][entry]["version"] = new
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")

pkg = Path("sdks/typescript/package.json")
data = json.loads(pkg.read_text())
data["version"] = new
pkg.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
lock("sdks/typescript/package-lock.json", "", root=True)
lock("kits/typescript/package-lock.json", "../../sdks/typescript")
lock("docs/package-lock.json", "../sdks/typescript")
PY
  cargo update -q --workspace
  git diff --stat
  local changed
  changed=$(git diff --name-only | sort | tr '\n' ' ')
  local want="Cargo.lock Cargo.toml docs/package-lock.json kits/rust/Cargo.toml kits/typescript/package-lock.json sdks/python/pyproject.toml sdks/rust/Cargo.toml sdks/typescript/package-lock.json sdks/typescript/package.json "
  [ "$changed" = "$want" ] || fail "the version change touched other files than expected: $changed"
  git -c user.email=artur.susdorf@gmail.com commit -q -am "$VERSION"
  echo "  committed $(git log --oneline -1)"
}

step_tests() {
  [ "$(current)" = "$VERSION" ] || fail "the manifests say $(current)"
  say "make check on Linux and make check here, side by side (logs in $LOGS)"
  "$ROOT/scripts/check-linux.sh" check >"$LOGS/check-linux.log" 2>&1 &
  local linux=$!
  local mac=0
  make check >"$LOGS/check.log" 2>&1 || mac=$?
  local lin=0
  wait $linux || lin=$?
  [ $mac = 0 ] || { tail -40 "$LOGS/check.log"; fail "make check failed here ($LOGS/check.log)"; }
  [ $lin = 0 ] || { tail -40 "$LOGS/check-linux.log"; fail "make check failed on Linux ($LOGS/check-linux.log)"; }
  say "make check-packages"
  logged check-packages make check-packages
  say "make flaky on Linux (3 rounds, every CPU busy)"
  logged flaky-linux env FLAKY=3 "$ROOT/scripts/check-linux.sh" flaky
}

step_binaries() {
  rm -f "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS"
  logged binaries "$ROOT/scripts/build-binaries.sh" "$VERSION"
  [ "$(ls "$DIST"/kernable-*.tar.gz | wc -l | tr -d ' ')" = 4 ] || fail "not 4 archives in $DIST"
  cat "$DIST/SHA256SUMS"
}

step_notes() {
  if [ -s "$DIST/NOTES.md" ]; then
    grep -q '<what this release brings' "$DIST/NOTES.md" && fail "$DIST/NOTES.md still has the placeholder line"
    echo "  taking $DIST/NOTES.md as it is"
    return
  fi
  local was
  was=$(git describe --tags --abbrev=0)
  {
    echo "**kernable $VERSION.** <what this release brings, in two or three sentences; what an operator must do: nothing before 1.0>"
    echo
    echo "Changes since $was:"
    echo
    git log --no-merges --format='- %s' "$was..HEAD" | grep -vx "- $VERSION"
    echo
    echo 'Install: `curl -fsSL https://kernable.com/install.sh | sh` · SDKs: `npm install @kernable/sdk`, `pip install kernable-sdk`, `cargo add kernable-sdk`'
    echo
    echo "SHA-256:"
    echo '```'
    cat "$DIST/SHA256SUMS"
    echo '```'
    echo
    echo "**Full Changelog**: https://github.com/$REPO/compare/$was...$TAG"
  } > "$DIST/NOTES.md"
  echo "  drafted $DIST/NOTES.md from the commits since $was."
  echo "  Read and edit it, then run scripts/release.sh $VERSION again: it goes on from here."
  exit 3
}

step_push() {
  grep -q '<what this release brings' "$DIST/NOTES.md" && fail "NOTES.md still has the placeholder line"
  [ "$(current)" = "$VERSION" ] || fail "the manifests say $(current)"
  git push -q origin main
  git tag -a "$TAG" -m "kernable $VERSION"
  git push -q origin "$TAG"
  echo "  pushed main and $TAG ($(git rev-parse --short HEAD))"
}

publish_with_tokens() {
  say "crates.io, from here"
  (cd crates/kernable-core && ./pack_proto.sh) && (cd sdks/rust && ./pack_kit.sh)
  local missing=
  for crate in kernable-core kernable-sdk-macros kernable-sdk; do
    curl -fsS -o /dev/null -A 'kernable-release' "https://crates.io/api/v1/crates/$crate/$VERSION" 2>/dev/null \
      && echo "  $crate is published" || missing="$missing -p $crate"
  done
  if [ -n "$missing" ]; then
    # shellcheck disable=SC2086
    CARGO_REGISTRY_TOKEN=$(cat "$CONF/crates.token") logged crates cargo publish --allow-dirty $missing
  fi
  git checkout -q -- crates/kernable-core sdks/rust 2>/dev/null || true
  git clean -fdq crates/kernable-core sdks/rust

  say "npm, from here"
  if ! npm view "@kernable/sdk@$VERSION" version >/dev/null 2>&1; then
    local rc=$DIST/.npmrc
    (umask 077 && printf '//registry.npmjs.org/:_authToken=%s\n' "$(cat "$CONF/npm.token")" > "$rc")
    (cd sdks/typescript && npm ci --no-audit --no-fund --silent \
      && npx -y npm@11 publish --access public --provenance=false --userconfig "$rc") >"$LOGS/npm.log" 2>&1 \
      || { rm -f "$rc"; tail -20 "$LOGS/npm.log"; fail "npm publish"; }
    rm -f "$rc"
  fi

  say "PyPI, from here"
  if ! curl -fsS -o /dev/null "https://pypi.org/pypi/kernable-sdk/$VERSION/json" 2>/dev/null; then
    local venv=$ROOT/target/release-venv
    [ -x "$venv/bin/twine" ] || { python3 -m venv "$venv" && "$venv/bin/pip" install -q build twine; }
    make -s py
    rm -rf "$DIST/python"
    (cd sdks/python && .venv/bin/python pack_kit.py >/dev/null && "$venv/bin/python" -m build -q --outdir "$DIST/python" . \
      && rm -rf kernable/_kit build ./*.egg-info) >"$LOGS/pypi-build.log" 2>&1 || fail "the Python package did not build"
    TWINE_USERNAME=__token__ TWINE_PASSWORD=$(cat "$CONF/pypi.token") \
      logged pypi "$venv/bin/twine" upload --non-interactive "$DIST/python"/*
  fi
}

step_publish() {
  if [ $TOKENS = 1 ]; then
    publish_with_tokens
    return
  fi
  say "waiting for GitHub's release workflow for $TAG"
  local run=
  for _ in $(seq 30); do
    run=$(gh run list -R "$REPO" --workflow release.yml --branch "$TAG" -L 1 --json databaseId -q '.[0].databaseId' 2>/dev/null || true)
    [ -n "$run" ] && break
    sleep 10
  done
  [ -n "$run" ] || fail "no run of release.yml for $TAG after 5 minutes"
  echo
  echo "  >>> Approve it: https://github.com/$REPO/actions/runs/$run"
  echo "      (Review deployments, release, Approve and deploy)"
  echo
  gh run watch "$run" -R "$REPO" --exit-status --interval 30 >"$LOGS/publish.log" 2>&1 \
    || { gh run view "$run" -R "$REPO" --log-failed | tail -40; fail "the release workflow failed: https://github.com/$REPO/actions/runs/$run (fix, then --from publish; or --tokens)"; }
  echo "  the release workflow published the SDKs"
}

step_downloads() {
  [ -f "$DIST/SHA256SUMS" ] || fail "no binaries in $DIST"
  (cd "$DIST" && shasum -a 256 -c SHA256SUMS >/dev/null) || fail "the archives in $DIST do not match SHA256SUMS"
  ssh -o BatchMode=yes "$SITE_SERVER" "mkdir -p $WEBROOT/download/$VERSION"
  rsync -a --chmod=F644 "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS" "$SITE_SERVER:$WEBROOT/download/$VERSION/"
  # Checked as served, then made the latest.
  local d
  d=$(mktemp -d)
  curl -fsSL "https://kernable.com/download/$VERSION/SHA256SUMS" -o "$d/SHA256SUMS"
  cmp -s "$d/SHA256SUMS" "$DIST/SHA256SUMS" || fail "kernable.com serves another SHA256SUMS"
  rm -rf "$d"
  ssh -o BatchMode=yes "$SITE_SERVER" "printf '%s\n' $VERSION > $WEBROOT/download/latest.new && mv $WEBROOT/download/latest.new $WEBROOT/download/latest"
  # install.sh as in this release.
  rsync -a --chmod=F644 scripts/install.sh "$SITE_SERVER:$WEBROOT/install.sh"
  echo "  https://kernable.com/download/$VERSION/, latest $(curl -fsSL https://kernable.com/download/latest)"
}

step_github() {
  if gh release view "$TAG" -R "$REPO" >/dev/null 2>&1; then
    gh release upload "$TAG" -R "$REPO" --clobber "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS"
    gh release edit "$TAG" -R "$REPO" --notes-file "$DIST/NOTES.md"
  else
    gh release create "$TAG" -R "$REPO" --verify-tag --latest --title "kernable $VERSION" \
      --notes-file "$DIST/NOTES.md" "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS"
  fi
  echo "  https://github.com/$REPO/releases/tag/$TAG"
}

step_docs() {
  logged docs make -C "$SITE_REPO" deploy-docs SERVER="$SITE_SERVER" KERNABLE="$ROOT"
  echo "  https://kernable.com/docs/"
}

step_verify() {
  LINUX=$LINUX "$ROOT/scripts/verify-release.sh" "$VERSION" 2>&1 | tee "$LOGS/verify.log"
  [ "${PIPESTATUS[0]}" = 0 ] || fail "the release does not hold as a user gets it ($LOGS/verify.log)"
}

say "kernable $VERSION"
for s in "${STEPS[@]}"; do
  if grep -qx "$s" "$DONE"; then
    echo "   $s: done before"
    continue
  fi
  say "$s"
  "step_$s"
  echo "$s" >> "$DONE"
done
say "kernable $VERSION is released"
