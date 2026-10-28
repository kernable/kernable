#!/usr/bin/env bash
# Builds the SDK packages (npm tarball, Python wheel), installs them into fresh projects outside
# the repository and runs a small app against a real kernel: a TypeScript worker writes events,
# a Python read model reads them. Needs network for the packages' dependencies.
set -euo pipefail
export PIP_DISABLE_PIP_VERSION_CHECK=1

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

# A kernel trusting one root key, and one app key for everything, granted by the root.
cd "$DIR"
K="$ROOT/target/debug/kernable"
for name in root kernel app; do "$K" keys new "$name" >/dev/null; done
"$K" grant root.key app.pub 'submit=*' 'run=demo.*' 'listen=*' 'query=*' 'answer=demo.*' >/dev/null
"$K" start --root root.pub --store sqlite:k.db --listen "unix:$DIR/k.sock" --kernel-key kernel.key 2>kernel.log &
KERNEL_PID=$!
for _ in $(seq 50); do [ -S k.sock ] && break; sleep 0.1; done

# Python: a read model of the demo domain answering demo.GetTotal, installed from the wheel.
python3 -m venv py && py/bin/pip install -q --disable-pip-version-check kernable-*.whl
cat > model.py <<'PY'
import sys
from kernable import Kernable
totals = {}
model = Kernable(sys.argv[1], "app.key").read_model("totals")
model.on("demo.Incremented", lambda e: totals.__setitem__(e.aggregate_id, totals.get(e.aggregate_id, 0) + e.data["by"]))
model.query("demo.GetTotal", lambda data: totals.get(data["id"], 0))
model.run()
PY
py/bin/python model.py "unix:$DIR/k.sock" 2>model.log &
MODEL_PID=$!

# TypeScript: a worker, two settling submits and a query, installed from the tarball.
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
// Wait for the Python read model to answer.
for (let i = 0; ; i++) {
  try { await kernable.query({ domain: "demo", type: "GetTotal", data: { id: "c1" } }); break; }
  catch (err) { if (i > 100) throw err; await new Promise((r) => setTimeout(r, 100)); }
}
for (const by of [2, 3]) {
  const outcome = await kernable.submit({ domain: "demo", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by } });
  if (!outcome.committed) throw new Error(outcome.message);
}
console.log(await kernable.query({ domain: "demo", type: "GetTotal", data: { id: "c1" } }));
worker.stop();
await running;
kernable.close();
JS
RESULT=$(node app.js "unix:$DIR/k.sock")
kill $MODEL_PID 2>/dev/null || true
wait $MODEL_PID 2>/dev/null || true
[ "$RESULT" = "5" ] || { echo "unexpected answer: $RESULT"; exit 1; }
echo "packages OK: npm $(ls kernable-sdk-*.tgz), wheel $(ls kernable-*.whl); TypeScript worker -> Python read model -> TypeScript query: $RESULT"
