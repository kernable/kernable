#!/usr/bin/env bash
# Partitions across two real sites that reach each other only through a relay: global unique
# keys and tenant moves (bench/cross-site.sh tests the rest on one machine).
#
# Site A is this machine (behind NAT: nothing reaches it), site B a Linux server ($SERVER). The
# relay runs on the server; this machine reaches it through an SSH tunnel for that one port. Every
# other address is valid only on its own site: the map names p1 at 127.0.0.1:17301 (on this
# machine) and p2 at 127.0.0.1:17302 (on the server), so neither partition can reach the other
# directly; each is also named as relay:<relay>/<its copy>, the way through the relay.
#   site A: p1 (control partition), a reader of site B's copy, a worker
#   site B: p2 (its --control: its reader of p1's copy), a reader of site A's copy, a worker
# Then:
#   T5a  a global unique key registered for a tenant of p2
#   T5b  the same key for a tenant of p1: must be refused (unique_taken)
#   T5c  another key for a tenant of p1: commits
#   T6a  moving a tenant with data from p2 to p1
#   T6b  moving a tenant with data from p1 to p2
# Needs: ssh root@$SERVER, an official Node 22 there (NODE_B; Ubuntu's runs no TypeScript), cargo zigbuild (the Linux kernable), Node here.
set -uo pipefail

SERVER=${SERVER:?set SERVER to site B, a Linux server you reach over SSH as root}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
K=$ROOT/target/debug/kernable
KL=$ROOT/target/x86_64-unknown-linux-gnu/release/kernable
DIR=$(mktemp -d /tmp/kxr-XXXX)
R=/root/kx # site B's directory on the server
PIDS=()
ssh_b() { ssh -o BatchMode=yes "root@$SERVER" "$@"; }
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  # [/]: the pattern does not match this command itself.
  ssh_b "cd $R 2>/dev/null && for u in relay rB p2 pushB workerB; do journalctl -u kx-\$u --no-pager -o cat >\$u.log 2>/dev/null; done; systemctl stop 'kx-*' 2>/dev/null; systemctl reset-failed 'kx-*' 2>/dev/null; true"
  wait 2>/dev/null
  echo "logs: $DIR (site A), $SERVER:$R (site B)"
}
trap cleanup EXIT
cd "$DIR"

say() { printf '%s\n' "$*"; }
result() { printf '%-5s %s\n' "$1" "$2"; }
start() { local log=$1; shift; "$@" >"$log" 2>&1 & PIDS+=($!); }
port_up() { for _ in $(seq 300); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done; return 1; }
start_b() { # <name> <command...>: a transient unit kx-<name> on the server, in $R (its log: the journal)
  local name=$1; shift
  ssh_b "systemd-run --quiet --unit kx-$name --working-directory $R $*"
}
port_up_b() { ssh_b "for i in \$(seq 300); do (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null && exit 0; sleep 0.1; done; exit 1"; }

# --- builds, keys, and site B's share of them -----------------------------------------------------
(cd "$ROOT" && cargo build -q -p kernable-kernel && cargo zigbuild -q --release --locked --target x86_64-unknown-linux-gnu.2.31 -p kernable-kernel) || exit 1
for n in root client worker ops readerA readerB kernel1 kernel2; do "$K" keys new "$n" >/dev/null; done
"$K" grant root.key client.pub 'submit=bench.*,acct.*' >/dev/null
"$K" grant root.key worker.pub 'run=bench.*,acct.*' 'listen=*' >/dev/null
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
# Two calls: a command line that names $R itself would match the pattern and end its own shell.
ssh_b "systemctl stop 'kx-*' 2>/dev/null; systemctl reset-failed 'kx-*' 2>/dev/null; true"
ssh_b "rm -rf $R && mkdir -p $R/repo/sdks/typescript $R/repo/bench" || { say "no directory on the server"; exit 1; }
scp -q "$KL" "root@$SERVER:$R/kernable"
scp -q root.pub kernel1.pub kernel2.key kernel2.pub kernel2.grant client.key client.grant worker.key worker.grant \
  ops.key ops.grant readerB.key readerB.grant copyB.key access.toml "root@$SERVER:$R/"
scp -qr copy-keys "root@$SERVER:$R/"
rsync -az --exclude test "$ROOT/sdks/typescript/" "root@$SERVER:$R/repo/sdks/typescript/"
rsync -az "$ROOT/proto" "root@$SERVER:$R/repo/"
scp -q "$ROOT/bench/cross-site-worker.ts" "root@$SERVER:$R/repo/bench/"

# --- the relay (site B's server), and this machine's tunnel to it ---------------------------------
start_b relay "$R/kernable relay --dir $R/copies --access $R/access.toml --listen 127.0.0.1:17400"
port_up_b 17400 || { say "the relay did not start"; exit 1; }
start tunnel.log ssh -o BatchMode=yes -N -L 17400:127.0.0.1:17400 "root@$SERVER"
port_up 17400 || { say "no tunnel to the relay"; exit 1; }
RELAY=127.0.0.1:17400

writer_flags() { # <n>
  echo "--root root.pub --partition p$1 --readers on --listen 127.0.0.1:1730$1 --instance-key kernel$1.key \
    --store sqlite:p$1.db --vault-store sqlite:p$1-vault.db --data-store file:p$1-data \
    --snapshots sqlite:p$1-snap.db"
}
reader_flags() { # <site> <copy> <copy key> <writer's kernel.pub> <port>
  echo "--root root.pub --reader $RELAY --reader-copy $2 --reader-copy-key $3 --instance-key reader$1.key \
    --primary-kernel-key $4 --store sqlite:r$1.db \
    --vault-store sqlite:r$1-vault.db --data-store file:r$1-data --snapshots sqlite:r$1-snap.db --listen 127.0.0.1:$5"
}

# Site A: p1, its pusher.
start p1.log "$K" start $(writer_flags 1)
port_up 17301 || { say "p1 did not start"; exit 1; }
start pushA.log "$K" push --follow 127.0.0.1:17301 --instance-key kernel1.key --to "$RELAY" --copy site.a --copy-key copyA.key
# Site B: its reader of p1's copy, then p2 with that reader as its control, p2's pusher.
start_b rB "$R/kernable start $(reader_flags B site.a copyA.key kernel1.pub 17312)"
port_up_b 17312 || { say "site B's reader did not start"; exit 1; }
start_b p2 "$R/kernable start $(writer_flags 2) --control 127.0.0.1:17312"
port_up_b 17302 || { say "p2 did not start"; exit 1; }
start_b pushB "$R/kernable push --follow 127.0.0.1:17302 --instance-key kernel2.key --to $RELAY --copy site.b --copy-key copyB.key"
# Site A: its reader of p2's copy.
start rA.log "$K" start $(reader_flags A site.b copyB.key kernel2.pub 17311)
port_up 17311 || { say "site A's reader did not start"; exit 1; }

# The map: t1 (and the default) on p1, t2 and t3 on p2. Applied at p1, at site A.
cat >map.json <<'EOF'
{"epoch": 1, "partitions": {"p1": ["127.0.0.1:17301", "relay:127.0.0.1:17400/site.a"],
                            "p2": ["127.0.0.1:17302", "relay:127.0.0.1:17400/site.b"]},
 "default": "p1", "control": "p1", "tenants": {"t2": "p2", "t3": "p2"}}
EOF
"$K" partitions apply 127.0.0.1:17301 ops.key map.json >apply.log 2>&1 || { say "map refused: $(tail -1 apply.log)"; exit 1; }
for _ in $(seq 100); do ssh_b "cd $R && ./kernable partitions show 127.0.0.1:17302 ops.key 2>/dev/null | grep -q 'epoch 1'" && break; sleep 0.3; done
say "      (p2 holds the map: $(ssh_b "cd $R && ./kernable partitions show 127.0.0.1:17302 ops.key 2>&1 | head -1"))"

# Workers beside each writer.
start workerA.log node --conditions=source --no-warnings "$ROOT/bench/cross-site-worker.ts" 127.0.0.1:17301 "$DIR/worker.key"
start_b workerB "${NODE_B:-/opt/node-v22.22.1-linux-x64/bin/node} --conditions=source --no-warnings $R/repo/bench/cross-site-worker.ts 127.0.0.1:17302 $R/worker.key"
sleep 4

submit_a() { timeout 30 "$K" submit "$@" 2>&1 | tail -1; }                  # <addr> <client.key> ...
submit_b() { ssh_b "cd $R && timeout 30 ./kernable submit $* 2>&1 | tail -1"; }

# --- T5: a global unique key ----------------------------------------------------------------------
t5a=$(submit_b 127.0.0.1:17302 client.key t2 acct Account a-b1 Register "'{\"email\":\"ada@example.com\"}'")
case "$t5a" in *ommitted*) result PASS "T5a a global key for a tenant of p2 (site B) committed: $t5a";;
  *) result FAIL "T5a a global key for a tenant of p2 (site B): $t5a";; esac
t5b=$(submit_a 127.0.0.1:17301 client.key t1 acct Account a-a1 Register '{"email":"ada@example.com"}')
case "$t5b" in *unique_taken*) result PASS "T5b the same key for a tenant of p1 (site A) was refused: unique_taken";;
  *) result FAIL "T5b the same key for a tenant of p1 (site A): $t5b";; esac
t5c=$(submit_a 127.0.0.1:17301 client.key t1 acct Account a-a2 Register '{"email":"grace@example.com"}')
case "$t5c" in *ommitted*) result PASS "T5c another key for a tenant of p1 committed";;
  *) result FAIL "T5c another key for a tenant of p1: $t5c";; esac

# --- T6: moves ------------------------------------------------------------------------------------
for i in 1 2 3; do submit_b 127.0.0.1:17302 client.key t3 bench Bench "m$i" Write '{}' >/dev/null; done
for i in 1 2 3; do submit_a 127.0.0.1:17301 client.key t1 bench Bench "n$i" Write '{}' >/dev/null; done
t6a=$(timeout 90 "$K" partitions move 127.0.0.1:17301 ops.key t3 p1 2>&1 | tail -1)
if [[ "$t6a" == *"moved from p2 to p1"* ]]; then
  result PASS "T6a moved a tenant with data from p2 (site B) to p1 (site A): $t6a"
else result FAIL "T6a moving a tenant from p2 (site B) to p1 (site A): ${t6a:-no answer within 90 s}"; fi
t6b=$(timeout 90 "$K" partitions move 127.0.0.1:17301 ops.key t1 p2 2>&1 | tail -1)
if [[ "$t6b" == *"moved from p1 to p2"* ]]; then
  result PASS "T6b moved a tenant with data from p1 (site A) to p2 (site B): $t6b"
else result FAIL "T6b moving a tenant from p1 (site A) to p2 (site B): ${t6b:-no answer within 90 s}"; fi
