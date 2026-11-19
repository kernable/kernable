#!/usr/bin/env bash
# How long moving a tenant takes and how long it is stopped: the move copies the
# tenant while it runs, then stops it for the rest. Two partitions; a load generator (64
# clients, wait=committed, 1 KB payloads) fills tenant t1 on p1 for each of FILL (seconds,
# default "5 15 45"), then `kernable partitions move` copies it to p2, timed.
# DURABILITY as in run.sh; both kernels share this machine.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
DIR=$(mktemp -d /tmp/km-XXXX)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$DIR"; }
trap cleanup EXIT

for name in root client worker node ops; do "$BIN/kernable" keys new "$DIR/$name" >/dev/null; done
"$BIN/kernable" grant "$DIR/root.key" "$DIR/client.pub" 'submit=bench.*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/worker.pub" 'run=bench.*' 'listen=*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/node.pub" 'replicate=*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/ops.pub" 'manage=*' >/dev/null
STORE=${STORE:-sqlite}
DURABILITY=${DURABILITY:-normal}
FILL=${FILL:-5 15 45}

store_of() { echo "sqlite:p$1.db"; }
sock() { echo "unix:$DIR/p$1.sock"; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }

run() { # <fill seconds>
  rm -rf "$DIR"/p*.db* "$DIR"/p*.log* "$DIR"/p*.sock "$DIR"/p*-snapshots.db* "$DIR"/p*-data
  for i in 1 2; do
    "$BIN/kernable" keys new "$DIR/kernel$i" >/dev/null 2>&1 || true
    local control=()
    [ "$i" -gt 1 ] && control=(--control "$(sock 1)")
    (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$(store_of "$i")" --durability "$DURABILITY" \
      --listen "$(sock "$i")" --kernel-key "kernel$i.key" --snapshots "sqlite:p$i-snapshots.db" --data-store "file:p$i-data" \
      --partition "p$i" --node-key node.key ${control[@]+"${control[@]}"} 2>/dev/null) &
    PIDS+=($!)
    for _ in $(seq 200); do [ -S "$DIR/p$i.sock" ] && break; sleep 0.05; done
  done
  echo "{\"epoch\": 1, \"partitions\": {\"p1\": [\"$(sock 1)\"], \"p2\": [\"$(sock 2)\"]}, \"default\": \"p1\", \"control\": \"p1\"}" >"$DIR/map.json"
  (cd "$DIR" && "$BIN/kernable" partitions apply "$(sock 1)" ops.key map.json >/dev/null)
  for _ in $(seq 100); do
    (cd "$DIR" && "$BIN/kernable" partitions show "$(sock 2)" ops.key >/dev/null 2>&1) && break
    sleep 0.1
  done
  "$BIN/kernable-bench" worker "$(sock 1)" "$DIR/worker.key" 64 &
  PIDS+=($!)
  sleep 1
  KERNABLE_BENCH_TENANT=t1 "$BIN/kernable-bench" load "$(sock 1)" "$DIR/client.key" concurrency=64 \
    seconds="$1" wait=committed >"$DIR/load.out" 2>&1
  local start end out
  start=$(now)
  out=$(cd "$DIR" && "$BIN/kernable" partitions move "$(sock 1)" ops.key t1 p2)
  end=$(now)
  local events
  local events stopped
  events=$(echo "$out" | sed -n 's/.*source head \([0-9]*\).*/\1/p' | tail -1)
  stopped=$(echo "$out" | sed -n 's/.*stopped for \([0-9]*\) ms.*/\1/p' | tail -1)
  printf 'events=%s move=%.2fs (%.0f events/s) stopped=%s ms  p1 log %s\n' "$events" "$(echo "$end - $start" | bc)" \
    "$(echo "$events / ($end - $start)" | bc -l)" "$stopped" "$(du -sh "$DIR"/p1.* 2>/dev/null | awk '{print $1}' | head -1)"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done
  PIDS=()
}

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) CPUs, $(date -u +%F), store=$STORE, durability=$DURABILITY"
for s in $FILL; do run "$s"; done
