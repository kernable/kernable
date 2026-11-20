#!/usr/bin/env bash
# make check on one machine: the four parts side by side (as CI runs them as four jobs), each
# with its log, and at the end which part failed. What every part needs first (prune, py, the
# workspace's build) happens once before; the parts share target/ then, every build in them
# finds it up to date.
#
#   scripts/check.sh [--logs <dir>] [part...]   parts: check-rust check-crash check-sdks check-docs
#
# Without parts, all four. The parts that failed are written to <logs>/failed, so after a fix
# `scripts/check.sh $(cat <logs>/failed)` runs only those (and `make check-rust RERUN=1` only
# the Rust tests that failed).
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
LOGS=target/check
if [ "${1:-}" = --logs ]; then
  LOGS=${2:?--logs needs a directory}
  shift 2
fi
PARTS=("$@")
[ ${#PARTS[@]} -gt 0 ] || PARTS=(check-rust check-crash check-sdks check-docs)
mkdir -p "$LOGS"
say() { echo "== $(date +%H:%M:%S) $*"; }

say "prepare: make prune py, cargo build --workspace"
if ! { make -s prune py && cargo build -q --workspace; } >"$LOGS/prepare.log" 2>&1; then
  tail -30 "$LOGS/prepare.log"
  echo "check: prepare failed ($LOGS/prepare.log)" >&2
  printf '%s\n' "${PARTS[@]}" >"$LOGS/failed"
  exit 1
fi

say "${PARTS[*]} side by side (logs in $LOGS)"
pids=()
for part in "${PARTS[@]}"; do
  (
    start=$(date +%s)
    # -o: prune and py are done; each part would otherwise run them again, at the same time.
    make -o prune -o py "$part" >"$LOGS/$part.log" 2>&1
    status=$?
    echo "$status $(( $(date +%s) - start ))" >"$LOGS/$part.status"
  ) &
  pids+=($!)
done
wait "${pids[@]}"

failed=()
for part in "${PARTS[@]}"; do
  read -r status seconds <"$LOGS/$part.status"
  if [ "$status" = 0 ]; then
    printf '  ok      %-12s %4d s\n' "$part" "$seconds"
  else
    printf '  FAILED  %-12s %4d s  %s\n' "$part" "$seconds" "$LOGS/$part.log"
    failed+=("$part")
  fi
done
printf '%s\n' "${failed[@]+"${failed[@]}"}" | sed '/^$/d' >"$LOGS/failed"
if [ ${#failed[@]} -gt 0 ]; then
  for part in "${failed[@]}"; do
    echo "---- $part: the end of $LOGS/$part.log"
    tail -25 "$LOGS/$part.log"
  done
  echo "check failed: ${failed[*]}; again only those: scripts/check.sh ${failed[*]}" >&2
  exit 1
fi
say "check passed"
