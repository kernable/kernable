#!/usr/bin/env bash
# Runs the PoC benchmark matrix (POC_PLAN.md §9) on this machine and prints one line per run.
# Release builds, SQLite WAL with synchronous=NORMAL, 1 KB payloads, warm connections.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
DIR=$(mktemp -d /tmp/kb-XXXX)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$DIR"; }
trap cleanup EXIT

"$BIN/kernable" keys new "$DIR/kernel" >/dev/null
CLIENT=$("$BIN/kernable" keys new "$DIR/client")
WORKER=$("$BIN/kernable" keys new "$DIR/worker")
SOCK="unix:$DIR/k.sock"
TCP="127.0.0.1:47233"

start_kernel() { # <listen>
  rm -f "$DIR"/k.db*
  cat > "$DIR/kernable.toml" <<TOML
db = "k.db"
listen = "$1"
kernel_key = "kernel.key"
[clients]
"$CLIENT" = ["*"]
[workers]
"$WORKER" = ["Write"]
TOML
  "$BIN/kernable-kernel" "$DIR/kernable.toml" 2>/dev/null &
  KERNEL=$!
  sleep 0.5
}

stop_all() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  PIDS=()
  kill -TERM "$KERNEL" 2>/dev/null || true
  wait "$KERNEL" 2>/dev/null || true
}

start_workers() { # <rust|ts> <count> <addr>
  for _ in $(seq "$2"); do
    if [ "$1" = rust ]; then
      "$BIN/kernable-bench" worker "$3" "$DIR/worker.key" 64 &
    else
      WORK_MS="${WORK_MS:-0}" node "$ROOT/bench/worker.ts" "$3" "$DIR/worker.key" &
    fi
    PIDS+=($!)
  done
  sleep 1.5
}

scenario() { # <label> <kind> <workers> <addr> <load args...>
  local label=$1 kind=$2 n=$3 addr=$4
  shift 4
  start_kernel "$addr"
  start_workers "$kind" "$n" "$addr"
  printf '%-28s ' "$label"
  "$BIN/kernable-bench" "$@"
  stop_all
}

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(date -u +%F)"
"$BIN/kernable-bench" crypto
printf '%-28s ' "baseline in-process"
"$BIN/kernable-bench" store "$DIR/baseline.db" 5000

# Latency without load: one client, back to back (no timer artifacts).
for kind in rust ts; do
  scenario "$kind worker, UDS, sequential" "$kind" 1 "$SOCK" load "$SOCK" "$DIR/client.key" concurrency=1 seconds=5
done
scenario "ts worker, TCP, sequential" ts 1 "$TCP" load "$TCP" "$DIR/client.key" concurrency=1 seconds=5

# Latency under moderate load: open loop at a fixed rate. Sends are scheduled with a ~1 ms
# timer, so these latencies include up to ~1 ms of scheduling slack.
for kind in rust ts; do
  scenario "$kind worker, UDS, 2000/s" "$kind" 1 "$SOCK" load "$SOCK" "$DIR/client.key" rate=2000
done

# Maximum throughput with trivial handlers: shows the kernel's ceiling.
for kind in rust ts; do
  scenario "$kind worker, UDS, max" "$kind" 1 "$SOCK" load "$SOCK" "$DIR/client.key" concurrency=64
done
scenario "ts worker, TCP, max" ts 1 "$TCP" load "$TCP" "$DIR/client.key" concurrency=64

# Worker scaling with 1 ms of CPU work per command.
for n in 1 2 4; do
  WORK_MS=1 scenario "ts x$n, 1ms work, max" ts "$n" "$SOCK" load "$SOCK" "$DIR/client.key" concurrency=64
done

for events in 10 100 1000; do
  scenario "ts, history $events" ts 1 "$SOCK" history "$SOCK" "$DIR/client.key" "$events"
done
scenario "ts, hot aggregate" ts 1 "$SOCK" load "$SOCK" "$DIR/client.key" concurrency=8 aggregates=1 seconds=5
