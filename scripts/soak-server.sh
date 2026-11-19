#!/usr/bin/env bash
# The soak on a Linux server: two runs side by side, each with the kernel and an app per SDK
# under load and random faults (crates/kernable-soak):
#
# - A: SQLite, its data on a small ext4 file system that the soak fills (disk-full);
# - B: SQLite too, its data on LazyFS, whose unsynced writes the soak drops (power-loss).
#
# From this machine (the repository is copied, not cloned):
#   scripts/soak-server.sh push root@HOST         copy the repository and set the server up
#   scripts/soak-server.sh start root@HOST 72     start both runs for 72 hours
#   scripts/soak-server.sh status root@HOST       last lines of both runs
#   scripts/soak-server.sh fetch root@HOST        copy logs and reports to target/soak-server/
# On the server, the same script runs as `setup`, `run-a HOURS` and `run-b HOURS`.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
REMOTE=/opt/kernable
SOAK=/srv/soak
LAZYFS_FIFO=/tmp/lazyfs.fifo

case "${1:-}" in
push)
  host=$2
  rsync -az --delete --exclude target --exclude node_modules --exclude .venv --exclude '__pycache__' \
    --exclude '*.db' --exclude '*.db-*' --exclude '*.key' --exclude '*.pub' --exclude '*.grant' \
    --exclude fuzz/corpus --exclude fuzz/artifacts --exclude docs/.vitepress/cache \
    "$ROOT/" "$host:$REMOTE/"
  ssh "$host" "$REMOTE/scripts/soak-server.sh setup"
  ;;

start)
  host=$2 hours=${3:-72}
  ssh "$host" "systemd-run --unit soak-a --collect $REMOTE/scripts/soak-server.sh run-a $hours &&
    systemd-run --unit soak-b --collect $REMOTE/scripts/soak-server.sh run-b $hours"
  ;;

status)
  ssh "$2" "for r in a b; do echo \"== \$r: \$(systemctl is-active soak-\$r)\"; tail -n 5 $SOAK/\$r/run/soak.log 2>/dev/null; done"
  ;;

fetch)
  mkdir -p "$ROOT/target/soak-server"
  rsync -az --exclude data --exclude '*.db*' --exclude 'k.log' "$2:$SOAK/" "$ROOT/target/soak-server/"
  ;;

setup)
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -q
  apt-get install -yq build-essential pkg-config cmake git curl rsync sqlite3 python3-venv python3-pip \
    fuse3 libfuse3-dev g++
  if ! command -v node >/dev/null || [ "$(node -p 'process.versions.node.split(".")[0]')" -lt 22 ]; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
    apt-get install -yq nodejs
  fi
  if ! command -v cargo >/dev/null; then
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
  fi
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"
  cd "$REMOTE"
  make py
  (cd sdks/typescript && npm install --no-audit --no-fund --silent)
  cargo build --release -p kernable-kernel -p kernable-soak

  # A: an ext4 file system in a file, for disk-full: 30 GiB, or half of what is free if less.
  mkdir -p $SOAK/a $SOAK/a/fs
  if ! mountpoint -q $SOAK/a/fs; then
    free_g=$(df --output=avail -BG $SOAK | tail -1 | tr -dc 0-9)
    size_g=$(( free_g / 2 < 30 ? free_g / 2 : 30 ))
    [ -f $SOAK/a.img ] || { fallocate -l ${size_g}G $SOAK/a.img && mkfs.ext4 -q $SOAK/a.img; }
    mount -o loop $SOAK/a.img $SOAK/a/fs
  fi

  # B: LazyFS over a plain directory, for power loss (lazyfs::clear-cache drops unsynced writes).
  if [ ! -x /opt/lazyfs/lazyfs/build/lazyfs ]; then
    [ -d /opt/lazyfs/.git ] || git clone -q https://github.com/dsrhaslab/lazyfs.git /opt/lazyfs
    # CMake 4 refuses LazyFS's dependencies' old minimum versions without this.
    export CMAKE_POLICY_VERSION_MINIMUM=3.5
    (cd /opt/lazyfs/libs/libpcache && ./build.sh >/dev/null)
    (cd /opt/lazyfs/lazyfs && ./build.sh >/dev/null)
  fi
  mkdir -p $SOAK/b/root $SOAK/b/fs
  cat > $SOAK/b/lazyfs.toml <<EOF
[faults]
fifo_path="$LAZYFS_FIFO"
[cache]
apply_eviction=false
[cache.simple]
custom_size="4gb"
blocks_per_page=1
[filesystem]
log_all_operations=false
logfile=""
EOF
  if ! mountpoint -q $SOAK/b/fs; then
    (cd /opt/lazyfs/lazyfs && ./scripts/mount-lazyfs.sh -c $SOAK/b/lazyfs.toml -m $SOAK/b/fs -r $SOAK/b/root -f >/dev/null 2>&1 &)
    for _ in $(seq 50); do mountpoint -q $SOAK/b/fs && break; sleep 0.2; done
    mountpoint -q $SOAK/b/fs || { echo "LazyFS did not mount" >&2; exit 1; }
  fi
  echo "setup done: $(nproc) cores, $(free -g | awk '/Mem/ {print $2}') GiB"
  ;;

run-a)
  mkdir -p $SOAK/a/fs/data
  exec "$REMOTE/target/release/kernable-soak" run --minutes $(( ${2:-72} * 60 )) --backend sqlite \
    --rate 50 --check-every 60 --dir $SOAK/a/run --data $SOAK/a/fs/data --fill $SOAK/a/fs \
    --kernel-port 17233 --proxy-port 17234
  ;;

run-b)
  mkdir -p $SOAK/b/fs/data
  exec "$REMOTE/target/release/kernable-soak" run --minutes $(( ${2:-72} * 60 )) --backend sqlite \
    --rate 50 --check-every 60 --dir $SOAK/b/run --data $SOAK/b/fs/data --power-loss-cmd "echo lazyfs::clear-cache > $LAZYFS_FIFO" \
    --kernel-port 17243 --proxy-port 17244
  ;;

*)
  sed -n '2,15p' "$0"
  exit 1
  ;;
esac
