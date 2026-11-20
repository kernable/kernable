#!/usr/bin/env bash
# The tests that depend on timing, <rounds> times each, with every CPU kept busy meanwhile as on a
# loaded CI machine (`make flaky`; on Linux: scripts/check-linux.sh flaky). Stops at the first
# failure. Expects `make py` and the workspace built.
#
#   scripts/flaky.sh [<rounds>]     (default 5)
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ROUNDS=${1:-5}
cd "$ROOT"

cpus=$(nproc 2>/dev/null || sysctl -n hw.ncpu)
busy=()
for _ in $(seq "$cpus"); do
  (while :; do :; done) &
  busy+=($!)
done
stop() { kill "${busy[@]}" 2>/dev/null; }
trap stop EXIT

run() {
  echo "  $1"
  shift
  "$@" >"$ROOT/target/flaky.log" 2>&1 || { tail -40 "$ROOT/target/flaky.log"; return 1; }
}

python_kit() {
  (cd kits/python && PYTHONPATH=../../sdks/python ../../sdks/python/.venv/bin/python -m unittest discover -s tests -t . -p 'e2e_*.py')
}
typescript_kit() { (cd kits/typescript && npm run -s check); }

for n in $(seq "$ROUNDS"); do
  echo "== round $n of $ROUNDS, $cpus CPUs busy"
  run "deployments" cargo test -q -p kernable-kernel --test deployments || { echo "== failed in round $n"; exit 1; }
  run "workers" cargo test -q -p kernable-kernel --test workers || { echo "== failed in round $n"; exit 1; }
  run "limits" cargo test -q -p kernable-kernel --test limits || { echo "== failed in round $n"; exit 1; }
  run "queries" cargo test -q -p kernable-kernel --test queries || { echo "== failed in round $n"; exit 1; }
  run "locks" cargo test -q -p kernable-kernel --test locks || { echo "== failed in round $n"; exit 1; }
  run "relays" cargo test -q -p kernable-kernel --test relays || { echo "== failed in round $n"; exit 1; }
  run "Rust kit end to end" cargo test -q -p kernable-kit --test e2e -- --ignored || { echo "== failed in round $n"; exit 1; }
  run "Python kit end to end" python_kit || { echo "== failed in round $n"; exit 1; }
  run "TypeScript kit" typescript_kit || { echo "== failed in round $n"; exit 1; }
done
echo "== $ROUNDS rounds passed"
