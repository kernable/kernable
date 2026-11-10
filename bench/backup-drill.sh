#!/usr/bin/env bash
# A restore drill: what a continuous backup to object storage loses and how long the way
# back takes, when the machine is lost under load.
#
# A kernel on a store filled with SIZE commands takes RATE commands a second; a backup follower
# writes them to TO (a base, then a segment every SEG seconds). At a random moment the kernel,
# the follower and the clients are killed together, as if the machine were gone. The backup is
# restored into new stores and checked; a kernel starts on them. Measured:
#   RPO  the commits the kernel had stored but the backup lacks, and the time they span
#   RTO  the restore (download, apply, verify) plus the kernel's start, until it answers
# and a point-in-time restore (--at) holds nothing after its time.
#
#   TO=s3://<bucket>/<prefix> [SIZE=200000] [RATE=200] [SEG=5] [LOAD=60] [WORK=<dir on a disk>] bash bench/backup-drill.sh
#   (the stores take about 2.5 times the filled store's size in WORK: not a small tmpfs)
#   (on Linux, kernable and kernable-bench in BIN; S3: KERNABLE_S3_ENDPOINT, KERNABLE_S3_REGION,
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY; python3 for reading the stores)
set -euo pipefail
BIN=${BIN:-$(cd "$(dirname "$0")/.." && pwd)/target/release}
TO=${TO:?s3://<bucket>/<prefix> or file:<dir>}
SIZE=${SIZE:-200000}
RATE=${RATE:-200}
SEG=${SEG:-5}
LOAD=${LOAD:-60}
mkdir -p "${WORK:=${TMPDIR:-/tmp}}"
DIR=$(mktemp -d "$WORK/drill.XXXXXX")
RUN=$TO/drill-$(date +%s)
pids=()
trap 'kill -9 ${pids[@]+"${pids[@]}"} 2>/dev/null || true; rm -rf "$DIR"' EXIT
now() { date +%s.%N; }
since() { echo "$(now) - $1" | bc; }
# Events in a store, and the newest one's time (ms).
head_of() {
  python3 -c "import sqlite3,sys; c=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True); print(*c.execute('select count(*), coalesce(max(created_at),0) from events').fetchone())" "$1"
}
ready() { # <socket>
  for _ in $(seq 1 600); do [ -S "$1" ] && return 0; sleep 0.05; done
  return 1
}

cd "$DIR"
for name in root kernel client worker backup-node; do "$BIN/kernable" keys new "$name" >/dev/null; done
"$BIN/kernable" grant root.key client.pub 'submit=bench.*' >/dev/null
"$BIN/kernable" grant root.key worker.pub 'run=bench.*' 'listen=*' >/dev/null
"$BIN/kernable" grant root.key backup-node.pub 'backup=*' >/dev/null
"$BIN/kernable" backup key backup.key >/dev/null

echo "== a store of $SIZE commands; a backup to $RUN, a segment every ${SEG} s"
t=$(now)
"$BIN/kernable-bench" fill "sqlite:$DIR/k.db" client.key worker.key 0 "$SIZE" >/dev/null 2>fill.err
read -r events _ < <(head_of k.db)
printf '  filled: %s events, %s MB, in %.1f s\n' "$events" "$(du -m k.db | cut -f1)" "$(since "$t")"
"$BIN/kernable" start --root root.pub --store "sqlite:$DIR/k.db" --listen "unix:$DIR/k.sock" \
  --kernel-key kernel.key --data-store "file:$DIR/data" --backup on 2>kernel.err &
pids+=($!)
ready "$DIR/k.sock"
"$BIN/kernable-bench" worker "unix:$DIR/k.sock" worker.key 64 >/dev/null 2>worker.err &
pids+=($!)
t=$(now)
"$BIN/kernable" backup --follow "unix:$DIR/k.sock" --key backup-node.key --to "$RUN" \
  --backup-key backup.key --segment-seconds "$SEG" 2>backup.err &
pids+=($!)
until grep -q "segment" backup.err 2>/dev/null || grep -qi "base.*written\|wrote the base\|base written" backup.err 2>/dev/null; do
  sleep 0.2
  if [ "$(printf '%.0f' "$(since "$t")")" -gt 900 ]; then echo "  no base after 15 min"; tail -5 backup.err; exit 1; fi
done
printf '  the base is in the bucket after %.1f s\n' "$(since "$t")"

echo "== $RATE commands a second for at most $LOAD s; the machine is lost at a random moment"
"$BIN/kernable-bench" load "unix:$DIR/k.sock" client.key rate="$RATE" seconds="$LOAD" wait=committed \
  aggregates=10000 payload=512 >load.out 2>load.err &
pids+=($!)
lost_at=$((LOAD / 3 + RANDOM % (LOAD / 2)))
sleep "$lost_at"
kill -9 ${pids[@]+"${pids[@]}"} 2>/dev/null || true
lost=$(python3 -c 'import time; print(int(time.time() * 1000))') # not date +%s%3N: GNU only
pids=()
sleep 0.5
read -r stored stored_ms < <(head_of k.db)
echo "  lost after ${lost_at} s: the kernel had stored $stored events, the newest at $stored_ms"

echo "== restore into new stores, then a kernel on them"
t=$(now)
"$BIN/kernable" restore "$RUN" --backup-key backup.key --store "sqlite:$DIR/r.db" \
  --data-store "file:$DIR/rdata" >restore.out 2>restore.err || { echo "  restore failed"; tail -5 restore.err; exit 1; }
restored_s=$(since "$t")
read -r restored restored_ms < <(head_of r.db)
"$BIN/kernable" start --root root.pub --store "sqlite:$DIR/r.db" --listen "unix:$DIR/r.sock" \
  --kernel-key kernel.key --data-store "file:$DIR/rdata" 2>restored-kernel.err &
pids+=($!)
ready "$DIR/r.sock"
# It answers: a command goes through on the restored kernel.
"$BIN/kernable-bench" worker "unix:$DIR/r.sock" worker.key 4 >/dev/null 2>&1 &
pids+=($!)
"$BIN/kernable-bench" load "unix:$DIR/r.sock" client.key rate=10 seconds=1 wait=committed >after.out 2>&1
rto_s=$(since "$t")
kill ${pids[@]+"${pids[@]}"} 2>/dev/null || true
pids=()
missing=$((stored - restored))
printf '  restored %s events in %.1f s (download, apply, verify); the kernel answered after %.1f s in all\n' \
  "$restored" "$restored_s" "$rto_s"
echo "== RPO: $missing events missing, the newest restored $((stored_ms - restored_ms)) ms older than the newest stored (segments every ${SEG} s)"
printf '== RTO: %.1f s for %s events (%s MB)\n' "$rto_s" "$restored" "$(du -m r.db | cut -f1)"
tail -1 restore.out | grep -qx OK && echo "  the restored store verifies" || { echo "  restore output:"; tail -3 restore.out; }
if [[ -s restore.err ]]; then
  echo "  the restore's steps:"
  sed 's/\x1b\[[0-9;]*m//g' restore.err | cut -c12-23,40-180 | sed 's/^/    /' | tail -12
fi

echo "== a point in time: the store as it was 10 s before the loss"
at_ms=$((lost - 10000))
at=$(python3 -c "import datetime; print(datetime.datetime.fromtimestamp($at_ms // 1000, datetime.UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))")
"$BIN/kernable" restore "$RUN" --backup-key backup.key --at "$at" --store "sqlite:$DIR/p.db" \
  --data-store "file:$DIR/pdata" >pit.out 2>pit.err || { echo "  restore --at failed"; tail -5 pit.err; exit 1; }
read -r pit pit_ms < <(head_of p.db)
if [ "$pit_ms" -le "$(((at_ms / 1000 + 1) * 1000))" ] && [ "$pit" -gt 0 ]; then
  echo "  ok: $pit events, the newest at $pit_ms, not after $at"
else
  echo "  FAILED: the newest at $pit_ms is after $at"
  exit 1
fi
echo "RESULT size=$SIZE rate=$RATE seg=$SEG stored=$stored restored=$restored missing=$missing rpo_ms=$((stored_ms - restored_ms)) restore_s=$restored_s rto_s=$rto_s"
