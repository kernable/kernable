#!/usr/bin/env bash
# What a synchronous standby costs: the same load on a kernel alone, and on a primary
# with --replication sync whose standby (--follow) runs on the same machine, over Unix sockets.
# Release builds, a Rust worker, 1 KB payloads, wait=committed. STORE and DURABILITY as in
# run.sh (defaults sqlite:k.db and full); STANDBY_DURABILITY sets the standby's (default: the
# same). On one machine the two kernels flush the same disk; on two machines they do not.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
DIR=$(mktemp -d /tmp/kr-XXXX)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$DIR"; }
trap cleanup EXIT

for name in root kernel standby-kernel client worker standby; do "$BIN/kernable" keys new "$DIR/$name" >/dev/null; done
"$BIN/kernable" grant "$DIR/root.key" "$DIR/client.pub" 'submit=bench.*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/worker.pub" 'run=bench.*' 'listen=*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/standby.pub" 'replicate=*' >/dev/null
SOCK="unix:$DIR/k.sock"
STANDBY_SOCK="unix:$DIR/s.sock"
STORE=${STORE:-sqlite:k.db}
DURABILITY=${DURABILITY:-full}
STANDBY_DURABILITY=${STANDBY_DURABILITY:-$DURABILITY}
standby_store() { case "$STORE" in file:*) echo "file:s.log" ;; *) echo "sqlite:s.db" ;; esac; }

wait_socket() { for _ in $(seq 200); do [ -S "${1#unix:}" ] && return; sleep 0.05; done; }

start() { # <alone|standby>
  rm -rf "$DIR"/k.* "$DIR"/s.* "$DIR"/*-snapshots.db* "$DIR"/data
  local replication=off
  [ "$1" = standby ] && replication=sync
  (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$STORE" --durability "$DURABILITY" \
    --listen "$SOCK" --kernel-key kernel.key --replication "$replication" --metrics-listen 127.0.0.1:47290 2>/dev/null) &
  KERNEL=$!
  wait_socket "$SOCK"
  if [ "$1" = standby ]; then
    (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$(standby_store)" --durability "$STANDBY_DURABILITY" \
      --listen "$STANDBY_SOCK" --kernel-key standby-kernel.key --snapshots sqlite:s-snapshots.db --data-store file:s-data \
      --follow "$SOCK" --follow-key standby.key --primary-kernel-key kernel.pub 2>/dev/null) &
    PIDS+=($!)
    for _ in $(seq 200); do
      curl -sf -H 'host: 127.0.0.1' http://127.0.0.1:47290/ready >/dev/null && break
      sleep 0.05
    done
  fi
  "$BIN/kernable-bench" worker "$SOCK" "$DIR/worker.key" 64 &
  PIDS+=($!)
  sleep 1.5
}

stop() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  PIDS=()
  kill -TERM "$KERNEL" 2>/dev/null || true
  wait "$KERNEL" 2>/dev/null || true
}

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(date -u +%F), store=$STORE, durability=$DURABILITY, standby durability=$STANDBY_DURABILITY"
for mode in alone standby; do
  for c in 1 64; do
    start "$mode"
    printf '%-28s ' "$mode, concurrency=$c"
    "$BIN/kernable-bench" load "$SOCK" "$DIR/client.key" concurrency=$c seconds=10 wait=committed
    stop
  done
done
