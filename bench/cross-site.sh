#!/usr/bin/env bash
# Partitions across sites that reach each other only through a relay: what works today.
#
# Site A runs partition p1 (the control partition), site B partition p2. Each writer pushes its
# copy to one relay; each site runs a reader of the other site's copy. Then:
#   T1  each site's reader follows the other site's copy through the relay
#   T2  p2 takes the map and the trust records from its own site's reader of p1's copy, not
#       from p1 itself (whose site it cannot reach)
#   T3  a command for a p2 tenant sent to site A's reader of p2's copy reaches p2 through the
#       relay and commits
#   T4  the SDK at site A, with a map that names site A's reader of p2's copy for p2: do p2
#       tenants' commands commit, and do the workers still get them?
# Everything runs on this machine; "a site cannot reach" means it is never given the address.
# Prints PASS / FAIL per test, and keeps the logs in a directory it names.
set -uo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
K=${KERNABLE_BIN:-$ROOT/target/debug/kernable}
DIR=$(mktemp -d /tmp/kx-XXXX)
RELAY=127.0.0.1:${RELAY_PORT:-17400}
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null; echo "logs: $DIR"; }
trap cleanup EXIT
cd "$DIR"

say() { printf '%s\n' "$*"; }
result() { printf '%-5s %s\n' "$1" "$2"; }
start() { # <log> <command...>
  local log=$1; shift
  "$@" >"$log" 2>&1 &
  PIDS+=($!)
}
wait_sock() { for _ in $(seq 300); do [ -S "$1" ] && return 0; sleep 0.1; done; return 1; }
logged() { # <log> <pattern> <seconds>
  for _ in $(seq $(( $3 * 10 ))); do sed 's/\x1b\[[0-9;]*m//g' "$1" | grep -q "$2" && return 0; sleep 0.1; done; return 1
}
submit_cli() { # <addr> <tenant> <id>
  timeout 20 "$K" submit "$1" client.key "$2" bench Bench "$3" Write '{}' 2>&1 | tail -1
}
submit_sdk() { # <url> <tenant> <id>
  node --conditions=source --no-warnings "$ROOT/bench/cross-site-submit.ts" "$1" "$DIR/client.key" "$2" "$3" 20000
}

# --- keys -------------------------------------------------------------------------------------
for n in root client worker ops readerA readerB kernel1 kernel2; do "$K" keys new "$n" >/dev/null; done
"$K" grant root.key client.pub 'submit=bench.*' >/dev/null
"$K" grant root.key worker.pub 'run=bench.*' 'listen=*' >/dev/null
# A writer's key takes the map (replicate), and reads: its pusher follows it, and it asks the
# other partition through the relay as a reader.
for n in kernel1 kernel2; do "$K" grant root.key "$n.pub" 'replicate=*' 'read=*' >/dev/null; done
"$K" grant root.key ops.pub 'manage=*' >/dev/null
for n in readerA readerB; do "$K" grant root.key "$n.pub" 'read=*' >/dev/null; done
"$K" relay key copyA.key >/dev/null; "$K" relay key copyB.key >/dev/null
# The relay knows every key by itself (no root counts there). One key per kernel: a writer's
# pushes its copy, and reads the other site's copy, to ask that partition through the relay. Each
# partition has the other's copy key.
cat >access.toml <<EOF
[copies."site.a".keys]
"$(cat kernel1.pub)" = { role = "push", state = "active" }
"$(cat readerB.pub)" = { role = "read", state = "active" }
"$(cat kernel2.pub)" = { role = "read", state = "active" }
[copies."site.b".keys]
"$(cat kernel2.pub)" = { role = "push", state = "active" }
"$(cat readerA.pub)" = { role = "read", state = "active" }
"$(cat kernel1.pub)" = { role = "read", state = "active" }
EOF
mkdir -p copy-keys && cp copyA.key copy-keys/site.a.key && cp copyB.key copy-keys/site.b.key

# --- the relay, the writers, their pushers ------------------------------------------------------
start relay.log "$K" relay --dir copies --access access.toml --listen "$RELAY"
sleep 0.5
writer() { # <n> [extra flags]
  local n=$1; shift
  start "p$n.log" "$K" start --root root.pub --partition "p$n" --readers on --listen "unix:$DIR/p$n.sock" \
    --instance-key "kernel$n.key" --store "sqlite:p$n.db" --vault-store "sqlite:p$n-vault.db" \
    --data-store "file:p$n-data" --snapshots "sqlite:p$n-snap.db" "$@"
}
writer 1
wait_sock p1.sock || { say "p1 did not start"; exit 1; }
start pushA.log "$K" push --follow "unix:$DIR/p1.sock" --instance-key kernel1.key --to "$RELAY" --copy site.a --copy-key copyA.key
reader() { # <site> <copy> <copy key> <writer's kernel.pub>
  start "r$1.log" "$K" start --root root.pub --reader "$RELAY" --reader-copy "$2" --reader-copy-key "$3" \
    --instance-key "reader$1.key" --primary-kernel-key "$4" \
    --store "sqlite:r$1.db" --vault-store "sqlite:r$1-vault.db" --data-store "file:r$1-data" \
    --snapshots "sqlite:r$1-snap.db" --listen "unix:$DIR/r$1.sock"
}
# Site B's reader of p1's copy first: T2 lets p2 take the map from it.
reader B site.a copyA.key kernel1.pub
wait_sock rB.sock || { say "site B's reader did not start"; exit 1; }

# --- T2: p2 with its control partition reached through site B's reader --------------------------
writer 2 --control "unix:$DIR/rB.sock"
wait_sock p2.sock || { say "p2 did not start"; exit 1; }
start pushB.log "$K" push --follow "unix:$DIR/p2.sock" --instance-key kernel2.key --to "$RELAY" --copy site.b --copy-key copyB.key
reader A site.b copyB.key kernel2.pub
wait_sock rA.sock || { say "site A's reader did not start"; exit 1; }

# The map: tenant t1 on p1 (the default), t2 on p2. Applied at p1, at site A.
cat >map.json <<EOF
{"epoch": 1, "partitions": {"p1": ["unix:$DIR/p1.sock"], "p2": ["unix:$DIR/p2.sock"]},
 "default": "p1", "control": "p1", "tenants": {"t2": "p2"}}
EOF
"$K" partitions apply "unix:$DIR/p1.sock" ops.key map.json >apply.log 2>&1
got=""
for _ in $(seq 100); do
  if "$K" partitions show "unix:$DIR/p2.sock" ops.key >show2.log 2>&1 && grep -q '"epoch": *1\|epoch 1\|epoch=1' show2.log; then got=yes; break; fi
  sleep 0.2
done
if [ -n "$got" ]; then result PASS "T2 p2 took the map through its site's reader of p1's copy"
else result FAIL "T2 p2 has no map from its site's reader of p1's copy: $(tail -1 show2.log)"; fi

# Workers beside each writer (each reads the map and runs on every partition it names).
start workerA.log node --conditions=source --no-warnings "$ROOT/bench/worker.ts" "unix:$DIR/p1.sock" "$DIR/worker.key"
start workerB.log node --conditions=source --no-warnings "$ROOT/bench/worker.ts" "unix:$DIR/p2.sock" "$DIR/worker.key"
sleep 3

# --- T1: each site follows the other's copy through the relay -----------------------------------
w1=$(submit_cli "unix:$DIR/p1.sock" t1 one)
w2=$(submit_cli "unix:$DIR/p2.sock" t2 two)
say "      (writes at the writers: p1 $w1; p2 $w2)"
if logged rA.log "caught up" 30 && logged rB.log "caught up" 30; then
  result PASS "T1 each site's reader follows the other site's copy through the relay"
else result FAIL "T1 a reader did not catch up (see rA.log, rB.log)"; fi

# --- T3: a p2 command sent to site A's reader of p2's copy --------------------------------------
t3=$(submit_cli "unix:$DIR/rA.sock" t2 via-reader)
case "$t3" in *ommitted*) result PASS "T3 a command at site A's reader of p2's copy committed at p2 through the relay ($t3)";;
  *) result FAIL "T3 a command at site A's reader of p2's copy: $t3";; esac
t3b=$(submit_cli "unix:$DIR/rA.sock" t1 wrong-partition)
say "      (a p1 tenant's command at that reader: $t3b)"

# --- T4: a map naming site A's reader for p2 ------------------------------------------------------
cat >map2.json <<EOF
{"epoch": 2, "partitions": {"p1": ["unix:$DIR/p1.sock"], "p2": ["unix:$DIR/rA.sock", "unix:$DIR/p2.sock"]},
 "default": "p1", "control": "p1", "tenants": {"t2": "p2"}}
EOF
"$K" partitions apply "unix:$DIR/p1.sock" ops.key map2.json >apply2.log 2>&1 || say "      (map 2 refused: $(tail -1 apply2.log))"
sleep 3
t4=$(submit_sdk "unix:$DIR/p1.sock" t2 sdk-at-a)
case "$t4" in committed) result PASS "T4 the SDK at site A committed a p2 tenant's command through its reader";;
  *) result FAIL "T4 the SDK at site A with its reader in the map for p2: $t4";; esac
grep -h -i "reader_of\|no commands\|waiting" workerA.log workerB.log 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | head -3 | sed 's/^/      worker: /'

# --- T4b: workers that start after that map ----------------------------------------------------
# A worker reads the map and runs on the first address it names for each partition: for p2 now
# site A's reader, which hands workers no commands. (T4 passed with workers that had connected
# to p2 before the map changed.)
for p in "${PIDS[@]}"; do
  case "$(ps -o command= -p "$p" 2>/dev/null)" in *worker.ts*) kill "$p" 2>/dev/null;; esac
done
sleep 1
start workerA2.log node --conditions=source --no-warnings "$ROOT/bench/worker.ts" "unix:$DIR/p1.sock" "$DIR/worker.key"
start workerB2.log node --conditions=source --no-warnings "$ROOT/bench/worker.ts" "unix:$DIR/p2.sock" "$DIR/worker.key"
sleep 3
t4b=$(submit_sdk "unix:$DIR/p1.sock" t2 after-restart)
case "$t4b" in committed) result PASS "T4b after a restart, workers still get p2's commands with site A's reader in the map";;
  *) result FAIL "T4b after a restart, with site A's reader first in the map for p2: $t4b";; esac
# The map is one for every site: site B's own processes now go to site A's reader for p2 first.
say "      (the map names the same addresses for every site: site B's processes reach p2 through site A's reader too)"
