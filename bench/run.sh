#!/usr/bin/env bash
# Runs the benchmark matrix on this machine and prints one line per run.
# Release builds, 1 KB payloads, warm connections. STORE=file:k.log or STORE=memory: picks the backend
# (default: sqlite:k.db, WAL with synchronous=NORMAL).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
DIR=$(mktemp -d /tmp/kb-XXXX)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$DIR"; }
trap cleanup EXIT

for name in root kernel client worker; do "$BIN/kernable" keys new "$DIR/$name" >/dev/null; done
"$BIN/kernable" grant "$DIR/root.key" "$DIR/client.pub" 'submit=bench.*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/worker.pub" 'run=bench.*' 'listen=*' >/dev/null
SOCK="unix:$DIR/k.sock"
TCP="127.0.0.1:47233"

STORE=${STORE:-sqlite:k.db}

start_kernel() { # <listen>
  rm -f "$DIR"/k.db* "$DIR"/k.log* "$DIR/k.sock" # a stale socket would look like a ready kernel
  # Relative store paths resolve against the working directory: run the kernel in $DIR.
  (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$STORE" --listen "$1" --kernel-key kernel.key 2>/dev/null) &
  KERNEL=$!
  # Wait until it accepts connections; the Rust worker does not retry.
  for _ in $(seq 200); do
    case "$1" in
      unix:*) [ -S "${1#unix:}" ] && break ;;
      *) (echo >"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null && break ;;
    esac
    sleep 0.05
  done
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

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(date -u +%F), store=$STORE"
"$BIN/kernable-bench" crypto
printf '%-28s ' "baseline in-process"
case "$STORE" in
  file:*) BASELINE="file:$DIR/baseline.log" ;;
  memory:*) BASELINE="memory:" ;;
  *) BASELINE="sqlite:$DIR/baseline.db" ;;
esac
"$BIN/kernable-bench" store "$BASELINE" 5000

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

# Cost of settling: two listeners that do no work (orders 0 and 10), each submit waits for
# both (wait=settled) or only for the commit (wait=committed).
for wait in committed settled; do
  for c in 1 64; do
    start_kernel "$SOCK"
    start_workers rust 1 "$SOCK"
    for order in 0 10; do
      "$BIN/kernable-bench" listener "$SOCK" "$DIR/worker.key" "l$order" "$order" &
      PIDS+=($!)
    done
    sleep 0.5
    printf '%-28s ' "rust, 2 listeners, $wait"
    "$BIN/kernable-bench" load "$SOCK" "$DIR/client.key" concurrency=$c seconds=5 wait=$wait
    stop_all
  done
done
