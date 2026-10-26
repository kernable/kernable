#!/usr/bin/env bash
# Builds the SDK packages (npm tarball, Python wheel), installs them into fresh projects outside
# the repository and runs a small app against a real kernel: a TypeScript worker writes events,
# a Python read model reads them. Needs network for the packages' dependencies.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/kp-XXXX)
KERNEL_PID=
cleanup() { [ -n "$KERNEL_PID" ] && kill "$KERNEL_PID" 2>/dev/null; rm -rf "$DIR"; }
trap cleanup EXIT

cargo build -q --manifest-path "$ROOT/Cargo.toml"
make -s -C "$ROOT" py
(cd "$ROOT/sdks/typescript" && npm pack --silent --pack-destination "$DIR" >/dev/null)
"$ROOT/sdks/python/.venv/bin/pip" wheel -q "$ROOT/sdks/python" -w "$DIR" --no-deps
rm -rf "$ROOT/sdks/python/build" "$ROOT/sdks/python/"*.egg-info

# A kernel with one key for everything.
cd "$DIR"
"$ROOT/target/debug/kernable" keys new kernel >/dev/null
APP=$("$ROOT/target/debug/kernable" keys new app)
cat > kernable.toml <<TOML
store = "sqlite:k.db"
listen = "unix:$DIR/k.sock"
kernel_key = "kernel.key"
[clients]
"$APP" = ["*"]
[workers]
"$APP" = ["demo.*"]
[listeners]
"$APP" = ["*"]
TOML
"$ROOT/target/debug/kernable-kernel" kernable.toml 2>kernel.log &
KERNEL_PID=$!
for _ in $(seq 50); do [ -S k.sock ] && break; sleep 0.1; done

# Python: a read model of the demo domain, installed from the wheel.
python3 -m venv py && py/bin/pip install -q --disable-pip-version-check kernable-*.whl
cat > model.py <<'PY'
import sys, threading
from kernable import Kernable
totals = {}
k = Kernable(sys.argv[1], "app.key")
model = k.read_model("totals")
model.on("demo.Incremented", lambda e: totals.__setitem__(e.aggregate_id, totals.get(e.aggregate_id, 0) + e.data["by"]))
threading.Thread(target=model.run, daemon=True).start()
model.ready.wait(5)
print("ready", flush=True)
sys.stdin.readline()  # the TypeScript app is done: settled submits mean the model is up to date
print(totals, flush=True)
PY
mkfifo go
py/bin/python model.py "unix:$DIR/k.sock" <go >model.out &
MODEL_PID=$!
exec 3>go
until grep -q ready model.out; do sleep 0.1; done

# TypeScript: a worker and two settling submits, installed from the tarball.
npm init -y >/dev/null && npm pkg set type=module && npm install --silent kernable-sdk-*.tgz
cat > app.js <<'JS'
import { Kernable, domain } from "@kernable/sdk";
const counter = domain("demo").aggregate("Counter", 0)
  .apply("Incremented", (n, e) => n + e.by)
  .command("Increment", (n, cmd) => [{ type: "Incremented", data: { by: cmd.by } }]);
const kernable = new Kernable({ url: process.argv[2], key: "app.key" });
const worker = kernable.worker(counter);
const running = worker.run();
await worker.ready;
for (const by of [2, 3]) {
  const outcome = await kernable.submit({ domain: "demo", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by } });
  if (!outcome.committed) throw new Error(outcome.message);
}
worker.stop();
await running;
kernable.close();
JS
node app.js "unix:$DIR/k.sock"

echo >&3
exec 3>&-
wait $MODEL_PID
RESULT=$(tail -1 model.out)
[ "$RESULT" = "{'c1': 5}" ] || { echo "unexpected read model: $RESULT"; exit 1; }
echo "packages OK: npm $(ls kernable-sdk-*.tgz), wheel $(ls kernable-*.whl); TypeScript worker -> Python read model: $RESULT"
