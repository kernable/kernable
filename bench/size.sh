#!/usr/bin/env bash
# Measures commands, queries and the kernel itself against the size of the store: fills one
# store step by step to each of SIZES events (10 per aggregate, each aggregate holding a unique
# key and a fact) and measures at every size. One line per measurement.
#
#   STORE=sqlite|file   backend (default sqlite)
#   SIZES="0 1000000"   store sizes in events (default 0 1M 10M 50M)
#   DURABILITY=normal   of the kernel while measuring (default normal: shows what the store
#                       costs, without the disk's sync time on top; `full` as `kernable start`)
#   DIR=<dir>           where the store goes (default a new directory in /tmp); kept with KEEP=1
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
STORE=${STORE:-sqlite}
SIZES=${SIZES:-0 1000000 10000000 50000000}
DURABILITY=${DURABILITY:-normal}
DIR=${DIR:-$(mktemp -d /tmp/kb-size-XXXX)}
mkdir -p "$DIR"
PIDS=()
KERNEL=
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  [ -n "$KERNEL" ] && kill -TERM "$KERNEL" 2>/dev/null || true
  [ "${KEEP:-0}" = 1 ] || rm -rf "$DIR"
}
trap cleanup EXIT

case "$STORE" in
  file) URL="file:$DIR/k.log" ;;
  *) URL="sqlite:$DIR/k.db" ;;
esac
# TCP, not a Unix socket: DIR may be longer than a socket path can be.
ADDR=127.0.0.1:${PORT:-47234}
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }

for name in root kernel client worker; do
  [ -f "$DIR/$name.key" ] || "$BIN/kernable" keys new "$DIR/$name" >/dev/null
done
"$BIN/kernable" grant "$DIR/root.key" "$DIR/client.pub" 'submit=bench.*' 'query=bench.*' 'subscribe=bench.*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/worker.pub" 'run=bench.*' 'listen=*' 'answer=bench.*' >/dev/null

rss_mb() { echo $(( $(ps -o rss= -p "$KERNEL") / 1024 )); }

start_kernel() {
  local t0; t0=$(now)
  (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$URL" --durability "$DURABILITY" --listen "$ADDR" --kernel-key kernel.key 2>"$DIR/kernel.err") &
  KERNEL=$!
  # Startup reads the store: allow it up to 30 minutes.
  for _ in $(seq 36000); do
    (echo >"/dev/tcp/${ADDR%:*}/${ADDR##*:}") 2>/dev/null && break
    kill -0 "$KERNEL" 2>/dev/null || { echo "kernel exited:"; cat "$DIR/kernel.err"; exit 1; }
    sleep 0.05
  done
  printf '%-30s %.2fs, rss=%sMB\n' "  kernel startup" "$(echo "$(now) - $t0" | bc)" "$(rss_mb)"
  for _ in 1 2; do
    "$BIN/kernable-bench" worker "$ADDR" "$DIR/worker.key" 64 keys &
    PIDS+=($!)
  done
  "$BIN/kernable-bench" reader "$ADDR" "$DIR/worker.key" reader &
  PIDS+=($!)
  sleep 1.5
}

stop_kernel() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  PIDS=()
  kill -TERM "$KERNEL" 2>/dev/null || true
  wait "$KERNEL" 2>/dev/null || true
  KERNEL=
}

run() { # <label> <bench args...>
  printf '%-30s ' "  $1"
  shift
  "$BIN/kernable-bench" "$@"
}

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/2^30 "GB"}'), $(date -u +%F), store=$STORE, durability=$DURABILITY"
filled=0
for size in $SIZES; do
  if [ "$size" -gt "$filled" ]; then
    "$BIN/kernable-bench" fill "$URL" "$DIR/client.key" "$DIR/worker.key" "$filled" "$size" 2>"$DIR/fill.err" >/dev/null
    filled=$size
  fi
  echo "-- $size events ($(du -sh "$DIR" | cut -f1) on disk)"
  start_kernel
  C="$ADDR $DIR/client.key"
  # Latency at a moderate fixed rate (open loop), then the ceiling (closed loop, 64 clients).
  run "command, new aggregate" load $C rate=500 seconds=20 target=new
  run "command, new, max" load $C concurrency=64 seconds=15 target=new
  if [ "$size" -gt 0 ]; then
    run "command, existing aggregate" load $C rate=500 seconds=20 target=existing filled=$size
    run "command, existing, max" load $C concurrency=64 seconds=15 target=existing filled=$size
    run "command, repeated ID" load $C rate=500 seconds=10 target=repeat filled=$size
  fi
  run "query" load $C rate=1000 seconds=10 target=query payload=64
  run "query, max" load $C concurrency=64 seconds=10 target=query payload=64
  if [ "$size" -gt 0 ]; then
    run "subscribe, near the head" subscribe $C $(( size > 10000 ? size - 10000 : 0 )) 10000
    run "subscribe, middle" subscribe $C $(( size / 2 )) 100000
  fi
  printf '%-30s rss=%sMB\n' "  kernel after" "$(rss_mb)"
  stop_kernel
done
