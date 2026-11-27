#!/usr/bin/env bash
# Releases kernable <version>: on this machine the version, every check and the tag; then GitHub's release
# workflow builds, publishes and deploys it after one approval; last, the release is checked as
# a user gets it. Running it is the decision to release. See RELEASING.md.
#
#   scripts/release.sh <version> [--summary "<text>"]   the release; the summary starts its notes
#   scripts/release.sh <version> --from <step>          again from <step> on
#   scripts/release.sh <version> --local                everything from this machine, without
#                                                       GitHub's workflow (the fallback): tokens
#                                                       in ~/.config/kernable/{crates,npm,pypi}.token
#
# Steps (one that held is noted in dist/<version>/.done and skipped next time, so a release
# stopped anywhere goes on where it stopped; logs in dist/<version>/logs/):
#   preflight  main, clean, up to date with origin; the tag free
#   version    the version in every manifest and lock file, committed
#   tests      make check (its four parts side by side; after a failure, run again, only the
#              parts that failed run), make check-packages, make flaky (3 rounds), on this
#              machine only
#   push       main and the annotated tag v<version> (its message: the summary)
#   workflow   GitHub's release workflow (.github/workflows/release.yml): binaries and notes,
#              then, after approval in the `release` environment, crates.io, npm, PyPI, the Go
#              module (its tag sdks/go/v<version>), the GitHub release, the downloads and the
#              docs on kernable.com, and its own check on Linux and macOS. This waits for it and prints where to approve.
#   verify     scripts/verify-release.sh on this machine (GitHub's workflow checked Linux)
# With --local, instead of workflow: binaries, notes, publish, downloads, github, docs.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
VERSION=${1:?usage: scripts/release.sh <version> [--summary <text>] [--from <step>] [--local]}
shift
FROM=
LOCAL=0
SUMMARY=
while [ $# -gt 0 ]; do
  case $1 in
    --from) FROM=${2:?--from needs a step}; shift 2 ;;
    --local) LOCAL=1; shift ;;
    --summary) SUMMARY=${2:?--summary needs a text}; shift 2 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
if [ $LOCAL = 1 ]; then
  STEPS=(preflight version tests push binaries notes publish downloads github docs verify)
else
  STEPS=(preflight version tests push workflow verify)
fi
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "a version is x.y.z, not $VERSION" >&2; exit 2; }
TAG=v$VERSION
# The Go module (sdks/go) is released by a tag of its own on the same commit.
GO_TAG=sdks/go/v$VERSION
DIST=$ROOT/dist/$VERSION
DONE=$DIST/.done
LOGS=$DIST/logs
REPO=kernable/kernable
CONF=$HOME/.config/kernable
KEY=$CONF/release-deploy
RSYNC=$(command -v /opt/homebrew/bin/rsync /usr/local/bin/rsync 2>/dev/null | head -1 || true)
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
site_rsync() {
  "$RSYNC" -e "ssh -i $KEY -o IdentitiesOnly=yes -o BatchMode=yes -o UserKnownHostsFile=$KEY.known_hosts -o StrictHostKeyChecking=yes" "$@"
}

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
  ! git rev-parse -q --verify "refs/tags/$GO_TAG" >/dev/null || fail "the tag $GO_TAG exists already"
  echo "  last release $(git describe --tags --abbrev=0 --match 'v*' 2>/dev/null || echo none), this one $VERSION"
  gh auth status >/dev/null 2>&1 || fail "gh is not logged in"
  if [ $LOCAL = 1 ]; then
    command -v cargo-zigbuild >/dev/null || fail "cargo zigbuild is missing (cargo install cargo-zigbuild)"
    [ -n "$RSYNC" ] || fail "GNU rsync is missing (brew install rsync): kernable.com refuses openrsync"
    [ -f "$KEY" ] && [ -f "$KEY.known_hosts" ] || fail "no $KEY (the key that may rsync into download/ and docs/)"
    [ -d "$SITE_REPO" ] || fail "no kernable.com repository at $SITE_REPO (SITE_REPO=...)"
    for t in crates npm pypi; do [ -s "$CONF/$t.token" ] || fail "no $CONF/$t.token"; done
  fi
}

step_version() {
  local was
  was=$(current)
  if [ "$was" = "$VERSION" ]; then
    echo "  the manifests say $VERSION already"
    return
  fi
  python3 - "$VERSION" <<'PY'
import json, re, sys
from pathlib import Path
new = sys.argv[1]

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
  local changed want
  changed=$(git diff --name-only | sort | tr '\n' ' ')
  want="Cargo.lock Cargo.toml docs/package-lock.json kits/rust/Cargo.toml kits/typescript/package-lock.json sdks/python/pyproject.toml sdks/rust/Cargo.toml sdks/typescript/package-lock.json sdks/typescript/package.json "
  [ "$changed" = "$want" ] || fail "the version change touched other files than expected: $changed"
  git -c user.email=artur.susdorf@gmail.com commit -q -am "$VERSION"
  echo "  committed $(git log --oneline -1)"
  # The new number makes every build of the workspace's own crates new; the old ones would stay
  # in target/ for ever. They go now, in every target directory; the dependencies' builds stay.
  local packages t
  packages=$(cargo metadata -q --no-deps --format-version 1 |
    python3 -c 'import json, sys; print(" ".join("-p " + p["name"] for p in json.load(sys.stdin)["packages"]))')
  for t in target target/nextest target/crash; do
    [ -d "$t" ] && CARGO_TARGET_DIR="$ROOT/$t" cargo clean -q $packages
  done
  echo "  the workspace's builds of $was removed from target/ (dependencies kept)"
}

step_tests() {
  [ "$(current)" = "$VERSION" ] || fail "the manifests say $(current)"
  # Each part that passed is noted (tests:<part>): run again after a fix, only the others run.
  local part ok todo=()
  for part in check-rust check-crash check-sdks check-docs; do
    grep -qx "tests:$part" "$DONE" || todo+=("$part")
  done
  if [ ${#todo[@]} -gt 0 ]; then
    say "make check: ${todo[*]} side by side (logs in $LOGS/check)"
    rm -f "$LOGS"/check/*.status
    ./scripts/check.sh --logs "$LOGS/check" "${todo[@]}" && ok=1 || ok=0
    for part in "${todo[@]}"; do
      if [ "$(cut -d' ' -f1 "$LOGS/check/$part.status" 2>/dev/null)" = 0 ]; then
        echo "tests:$part" >>"$DONE"
      fi
    done
    [ $ok = 1 ] || fail "make check failed: $(tr '\n' ' ' <"$LOGS/check/failed")(logs in $LOGS/check); run the release again for only those"
  fi
  if ! grep -qx tests:check-packages "$DONE"; then
    say "make check-packages"
    logged check-packages make check-packages
    echo tests:check-packages >>"$DONE"
  fi
  say "make flaky (3 rounds, every CPU busy)"
  logged flaky make flaky FLAKY=3
}

step_push() {
  [ "$(current)" = "$VERSION" ] || fail "the manifests say $(current)"
  git push -q origin main
  if [ -n "$SUMMARY" ]; then
    git tag -a "$TAG" -m "kernable $VERSION" -m "$SUMMARY"
  else
    git tag -a "$TAG" -m "kernable $VERSION"
  fi
  git push -q origin "$TAG"
  echo "  pushed main and $TAG ($(git rev-parse --short HEAD))"
}

# The run of the release workflow for the tag (it starts a few seconds after the push). A
# cancelled one is passed over: the run of a tag pushed again (same commit) comes after it.
workflow_run() {
  local run=
  for _ in $(seq 30); do
    run=$(gh run list -R "$REPO" --workflow release.yml --branch "$TAG" -L 5 --json databaseId,conclusion \
      -q '[.[] | select(.conclusion != "cancelled")][0].databaseId // empty' 2>/dev/null || true)
    [ -n "$run" ] && break
    sleep 10
  done
  echo "$run"
}

step_workflow() {
  say "GitHub's release workflow for $TAG"
  local run
  run=$(workflow_run)
  [ -n "$run" ] || fail "no run of release.yml for $TAG after 5 minutes"
  echo "  building the binaries and the notes first (about 15 minutes); then it waits for you:"
  echo
  echo "  >>> Approve: https://github.com/$REPO/actions/runs/$run"
  echo "      (Review deployments, tick release, Approve and deploy)"
  echo
  gh run watch "$run" -R "$REPO" --exit-status --interval 30 >"$LOGS/workflow.log" 2>&1 || {
    gh run view "$run" -R "$REPO" --log-failed 2>/dev/null | tail -40
    fail "the release workflow failed: https://github.com/$REPO/actions/runs/$run
  Fix it and re-run the failed jobs there (each part done is skipped), then: --from workflow.
  Or without GitHub's workflow: scripts/release.sh $VERSION --local --from binaries"
  }
  echo "  published, deployed and checked on GitHub: https://github.com/$REPO/releases/tag/$TAG"
}

# --local: what the workflow does, from here.

step_binaries() {
  # The tag's workflow is not wanted now: it would wait for an approval that never comes.
  local run
  run=$(gh run list -R "$REPO" --workflow release.yml --branch "$TAG" -L 1 --json databaseId,status \
    -q '.[0] | select(.status != "completed") | .databaseId' 2>/dev/null || true)
  [ -z "$run" ] || { gh run cancel "$run" -R "$REPO" >/dev/null && echo "  cancelled the workflow run $run"; }
  rm -f "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS"
  logged binaries "$ROOT/scripts/build-binaries.sh" "$VERSION"
  [ "$(ls "$DIST"/kernable-*.tar.gz | wc -l | tr -d ' ')" = 4 ] || fail "not 4 archives in $DIST"
  cat "$DIST/SHA256SUMS"
}

step_notes() {
  python3 scripts/release-notes.py "$VERSION" --sums "$DIST/SHA256SUMS" \
    --summary "$(git tag -l --format='%(contents:body)' "$TAG")" > "$DIST/NOTES.md"
  cat "$DIST/NOTES.md"
}

step_publish() {
  say "crates.io"
  local missing=
  for crate in kernable-core kernable-sdk-macros kernable-sdk; do
    if curl -fsS -o /dev/null -A 'kernable-release' "https://crates.io/api/v1/crates/$crate/$VERSION" 2>/dev/null; then
      echo "  $crate is published"
    else
      missing="$missing -p $crate"
    fi
  done
  if [ -n "$missing" ]; then
    (cd crates/kernable-core && ./pack_proto.sh) && (cd sdks/rust && ./pack_kit.sh)
    # shellcheck disable=SC2086
    CARGO_REGISTRY_TOKEN=$(cat "$CONF/crates.token") logged crates cargo publish --allow-dirty $missing
    git checkout -q -- crates/kernable-core sdks/rust
    git clean -fdq crates/kernable-core sdks/rust
  fi

  say "npm"
  if ! npm view "@kernable/sdk@$VERSION" version >/dev/null 2>&1; then
    local rc=$DIST/.npmrc
    (umask 077 && printf '//registry.npmjs.org/:_authToken=%s\n' "$(cat "$CONF/npm.token")" > "$rc")
    (cd sdks/typescript && npm ci --no-audit --no-fund --silent \
      && NPM_CONFIG_USERCONFIG=$rc npx -y npm@11 publish --access public --provenance=false) >"$LOGS/npm.log" 2>&1 \
      || { rm -f "$rc"; tail -20 "$LOGS/npm.log"; fail "npm publish"; }
    rm -f "$rc"
  fi

  say "Go module"
  if ! git ls-remote --exit-code --tags origin "refs/tags/$GO_TAG" >/dev/null; then
    git rev-parse -q --verify "refs/tags/$GO_TAG" >/dev/null || git tag "$GO_TAG" "$TAG^{commit}"
    git push -q origin "refs/tags/$GO_TAG"
  fi
  local fetched=0
  for _ in $(seq 20); do
    if (cd "$DIST" && GOPROXY=https://proxy.golang.org GOFLAGS=-mod=mod go list -m "github.com/kernable/kernable/sdks/go@$TAG" >/dev/null 2>&1); then
      fetched=1
      break
    fi
    sleep 15
  done
  [ $fetched = 1 ] || fail "proxy.golang.org does not have github.com/kernable/kernable/sdks/go@$TAG"
  echo "  github.com/kernable/kernable/sdks/go@$TAG"

  say "PyPI"
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

step_downloads() {
  (cd "$DIST" && shasum -a 256 -c SHA256SUMS >/dev/null) || fail "the archives in $DIST do not match SHA256SUMS"
  local up
  up=$(mktemp -d)
  mkdir "$up/$VERSION"
  cp "$DIST"/kernable-*.tar.gz "$DIST/SHA256SUMS" "$up/$VERSION/"
  site_rsync -rt --chmod=D755,F644 "$up/$VERSION" release@kernable.com:download/
  curl -fsSL "https://kernable.com/download/$VERSION/SHA256SUMS" | cmp -s - "$DIST/SHA256SUMS" \
    || fail "kernable.com serves another SHA256SUMS"
  cp scripts/install.sh "$up/install.sh"
  printf '%s\n' "$VERSION" > "$up/latest"
  site_rsync -t --chmod=F644 "$up/install.sh" "$up/latest" release@kernable.com:download/
  rm -rf "$up"
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
  echo "  https://github.com/$REPO/releases/tag/$TAG (without build provenance: built here)"
}

step_docs() {
  logged docs make -C "$SITE_REPO" deploy-docs KERNABLE="$ROOT"
  echo "  https://kernable.com/docs/"
}

step_verify() {
  "$ROOT/scripts/verify-release.sh" "$VERSION" 2>&1 | tee "$LOGS/verify.log"
  [ "${PIPESTATUS[0]}" = 0 ] || fail "the release does not hold as a user gets it ($LOGS/verify.log)"
}

say "kernable $VERSION$([ $LOCAL = 1 ] && echo ', all from here')"
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
