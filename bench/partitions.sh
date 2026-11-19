#!/usr/bin/env bash
# How throughput grows with partitions: 1 to PARTITIONS (default 4) kernels sharing the
# tenants by a map, each with its own Rust worker and its own load generator (64 clients,
# wait=committed, 1 KB payloads) writing one tenant that lives on it. Prints the sum per run.
# DURABILITY as in run.sh. All kernels share this machine's CPUs and
# disk: on separate machines each partition has its own.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cargo build --release -q --manifest-path "$ROOT/Cargo.toml"
BIN=$ROOT/target/release
DIR=$(mktemp -d /tmp/kp-XXXX)
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$DIR"; }
trap cleanup EXIT

for name in root client worker node ops; do "$BIN/kernable" keys new "$DIR/$name" >/dev/null; done
"$BIN/kernable" grant "$DIR/root.key" "$DIR/client.pub" 'submit=bench.*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/worker.pub" 'run=bench.*' 'listen=*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/node.pub" 'replicate=*' >/dev/null
"$BIN/kernable" grant "$DIR/root.key" "$DIR/ops.pub" 'manage=*' >/dev/null
STORE=${STORE:-sqlite}
DURABILITY=${DURABILITY:-full}
MAX=${PARTITIONS:-4}
SECONDS_EACH=${SECONDS_EACH:-10}

store_of() { echo "sqlite:p$1.db"; }
sock() { echo "unix:$DIR/p$1.sock"; }

run() { # <partitions>
  local n=$1 i
  rm -rf "$DIR"/p*.db* "$DIR"/p*.log* "$DIR"/p*.sock "$DIR"/p*-snapshots.db* "$DIR"/p*-data
  for i in $(seq "$n"); do
    "$BIN/kernable" keys new "$DIR/kernel$i" >/dev/null 2>&1 || true
    local control=()
    [ "$i" -gt 1 ] && control=(--control "$(sock 1)" --node-key node.key)
    (cd "$DIR" && exec "$BIN/kernable" start --root root.pub --store "$(store_of "$i")" --durability "$DURABILITY" \
      --listen "$(sock "$i")" --kernel-key "kernel$i.key" --snapshots "sqlite:p$i-snapshots.db" --data-store "file:p$i-data" \
      --partition "p$i" ${control[@]+"${control[@]}"} 2>/dev/null) &
    PIDS+=($!)
    for _ in $(seq 200); do [ -S "$DIR/p$i.sock" ] && break; sleep 0.05; done
  done
  # The map: tenant t<i> on partition p<i>.
  local partitions="" tenants=""
  for i in $(seq "$n"); do
    partitions+="${partitions:+,}\"p$i\": [\"$(sock "$i")\"]"
    tenants+="${tenants:+,}\"t$i\": \"p$i\""
  done
  echo "{\"epoch\": 1, \"partitions\": {$partitions}, \"default\": \"p1\", \"control\": \"p1\", \"tenants\": {$tenants}}" >"$DIR/map.json"
  (cd "$DIR" && "$BIN/kernable" partitions apply "$(sock 1)" ops.key map.json >/dev/null)
  for i in $(seq 2 "$n"); do
    for _ in $(seq 100); do
      (cd "$DIR" && "$BIN/kernable" partitions show "$(sock "$i")" ops.key >/dev/null 2>&1) && break
      sleep 0.1
    done
  done
  for i in $(seq "$n"); do
    "$BIN/kernable-bench" worker "$(sock "$i")" "$DIR/worker.key" 64 &
    PIDS+=($!)
  done
  sleep 1.5
  local loads=()
  for i in $(seq "$n"); do
    KERNABLE_BENCH_TENANT="t$i" "$BIN/kernable-bench" load "$(sock "$i")" "$DIR/client.key" concurrency=64 \
      seconds="$SECONDS_EACH" wait=committed >"$DIR/load$i.out" 2>&1 &
    loads+=($!)
  done
  for p in "${loads[@]}"; do wait "$p"; done
  local total=0 each="" t
  for i in $(seq "$n"); do
    t=$(sed -n 's/.*throughput=\([0-9]*\)\/s.*/\1/p' "$DIR/load$i.out")
    total=$((total + ${t:-0}))
    each+=" p$i=${t:-?}/s"
  done
  printf 'partitions=%d total=%d/s  (%s )  %s\n' "$n" "$total" "$each" "$(sed -n 's/.*\(p99=[^ ]*\).*/\1/p' "$DIR/load1.out")"
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done
  PIDS=()
}

echo "== $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(nproc 2>/dev/null || sysctl -n hw.ncpu) CPUs, $(date -u +%F), store=$STORE, durability=$DURABILITY"
for n in $(seq "$MAX"); do run "$n"; done
