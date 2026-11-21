// A client given several kernels (a pair, see Run a standby) uses the one that answers.

import assert from "node:assert/strict";
import { createConnection, createServer, type Socket } from "node:net";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

test("a kernel that cannot be reached hands over to the next address", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const gone = `unix:${join(kernel.dir, "gone.sock")}`;
  const kernable = new Kernable({ url: `${gone},${kernel.url}`, key: kernel.appKey, timeoutMs: 10_000 });
  const worker = kernable.worker(counter);
  const running = worker.run({ reconnectMs: 50 });
  try {
    await worker.ready;
    const outcome = await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 2 }, wait: "committed" });
    assert.ok(outcome.committed);
    assert.equal(outcome.events.length, 1);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});

/**
 * A TCP proxy to the kernel's socket at `to` that can be frozen, as when the network cuts a
 * kernel off without a word (a partition): its address, the switch, and its end.
 */
async function freezableProxy(to: string): Promise<{ url: string; freeze: () => void; close: () => void }> {
  let frozen = false;
  const sockets: Socket[] = [];
  const pump = (from: Socket, into: Socket) =>
    from.on("data", (chunk) => {
      // Cut off: what comes in is dropped silently, nothing is closed.
      if (!frozen) into.write(chunk);
    });
  const server = createServer((tcp) => {
    const unix = createConnection(to);
    sockets.push(tcp, unix);
    pump(tcp, unix);
    pump(unix, tcp);
    tcp.on("error", () => {});
    unix.on("error", () => {});
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  return {
    url: `127.0.0.1:${port}`,
    freeze: () => {
      frozen = true;
    },
    close: () => {
      for (const s of sockets) s.destroy();
      server.close();
    },
  };
}

test("a pair client turns to the other kernel within seconds when one is cut off", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const proxy = await freezableProxy(kernel.url.replace(/^unix:/, ""));
  // The worker connects directly: what is measured is the client's turn (through the proxy, the
  // one kernel behind both addresses would lease the next command to the frozen worker session
  // until its lease ran out).
  const direct = new Kernable({ url: kernel.url, key: kernel.appKey, timeoutMs: 10_000 });
  const kernable = new Kernable({ url: `${proxy.url},${kernel.url}`, key: kernel.appKey, timeoutMs: 30_000 });
  const worker = direct.worker(counter);
  const running = worker.run({ reconnectMs: 50 });
  try {
    await worker.ready;
    const first = await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 1 }, wait: "committed" });
    assert.ok(first.committed);
    // The proxy freezes: the connection the client holds looks open and answers nothing.
    proxy.freeze();
    const started = Date.now();
    const second = await kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c2", type: "Increment", data: { by: 1 }, wait: "committed" });
    assert.ok(second.committed);
    assert.ok(Date.now() - started < 10_000, `the client turned to the other address after ${Date.now() - started} ms`);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    direct.close();
    proxy.close();
    await kernel.dispose();
  }
});

// HTTP/2 keeps headers in a table both sides share, so a header damaged on the way (the grant)
// stays wrong there for every later request: a refused grant is not tried again on that
// connection, the next call opens a new one.
test("a refused grant is not tried again on the same connection", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const socket = kernel.url.replace(/^unix:/, "");
  let accepted = 0;
  const server = createServer((tcp) => {
    accepted++;
    const unix = createConnection(socket);
    tcp.pipe(unix).pipe(tcp);
    tcp.on("error", () => {});
    unix.on("error", () => {});
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  // Hex, but no Grant: the kernel refuses it as unreadable.
  const grant = join(kernel.dir, "damaged.grant");
  writeFileSync(grant, "ff00ff00");
  const kernable = new Kernable({ url: `127.0.0.1:${port}`, key: kernel.appKey, grant, timeoutMs: 10_000 });
  try {
    for (let i = 0; i < 3; i++) {
      await assert.rejects(
        kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: `c${i}`, type: "Increment", data: { by: 1 } }),
        /grant/,
      );
      assert.equal(accepted, i + 1, `call ${i}`);
    }
  } finally {
    kernable.close();
    server.close();
    await kernel.dispose();
  }
});
