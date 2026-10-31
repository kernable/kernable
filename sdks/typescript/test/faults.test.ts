// Faults around listeners: a first connect that fails, and a batch that fails while the next one
// is already on its way. Nothing may be skipped, and nothing from before a reactor existed may be
// replayed.

import assert from "node:assert/strict";
import { join } from "node:path";
import { test } from "node:test";

import { Kernable, Rejected, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const counters = domain("counters");
const counter = counters.aggregate("Counter", 0)
  .apply("Incremented", (n, data: { by: number }) => n + data.by)
  .command("Increment", (_n, data: { by: number }) => [{ type: "Incremented", data: { by: data.by } }]);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function until(what: string, done: () => boolean): Promise<void> {
  for (let i = 0; i < 250 && !done(); i++) await sleep(20);
  assert.ok(done(), what);
}

test("a new reactor whose first connect fails still starts at the newest event", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  const worker = kernable.worker(counter);
  const running = [worker.run({ reconnectMs: 50 })];
  const seen: number[] = [];
  const reactor = kernable.listener(counters.reactor("mailer", { reconnectMs: 50 }).on("Incremented", (data: { by: number }) => {
    seen.push(data.by);
  }));
  try {
    await worker.ready;
    for (const by of [1, 2, 3]) await increment("c1", by);

    // The kernel is away when the reactor connects for the first time.
    await kernel.kill();
    running.push(reactor.run());
    await sleep(200);
    await kernel.start();
    await reactor.ready;
    await until("the worker is back", () => true);
    await increment("c1", 4);
    await until("the new event arrives", () => seen.length > 0);
    await sleep(100);
    assert.deepEqual(seen, [4], "only what happened after it existed");
  } finally {
    worker.stop();
    reactor.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a batch that fails stops the batches behind it, so none of its events is skipped", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  let open!: () => void;
  const gate = new Promise<void>((resolve) => (open = resolve));
  let failed = false;
  const Totals = counters.readModel("totals", { store: `sqlite:${join(kernel.dir, "totals.db")}`, reconnectMs: 50 })
    .on("Incremented", async (data: { by: number }, evt, ctx) => {
      if (data.by === 1 && !failed) {
        await gate; // the next batch arrives meanwhile
        failed = true;
        throw new Error("boom");
      }
      ctx.store!.set("totals", evt.aggregateId, (ctx.store!.get<number>("totals", evt.aggregateId) ?? 0) + data.by);
    });
  const worker = kernable.worker(counter);
  const model = kernable.listener(Totals);
  const running = [worker.run(), model.run()];
  try {
    await Promise.all([worker.ready, model.ready]);
    await increment("c1", 1);
    await increment("c1", 2);
    await sleep(200);
    open();
    await until("both events handled", () => model.store.get<number>("totals", "c1") === 3);
  } finally {
    worker.stop();
    model.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("a command handler that changes its state changes neither the initial state nor others", async () => {
  const boxes = domain("boxes");
  const box = boxes.aggregate("Box", { items: [] as string[] })
    .apply("Opened", (s) => void s.items.push("opened"))
    .command("Open", (s) => {
      if (s.items.length > 0) throw new Rejected("already open", "invalid_state");
      s.items.push("opened"); // changes its copy only
      return [{ type: "Opened" }];
    })
    .command("Fail", () => {
      throw Object.create(null); // cannot even be shown as text
    })
    .command("Leak", () => {
      throw new TypeError("password hunter2 rejected by db://admin@10.0.0.5");
    });
  const kernel = new TestKernel(["boxes.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const submit = (id: string, type: string) =>
    kernable.submit({ domain: "boxes", aggregateType: "Box", aggregateId: id, type, wait: "committed" });
  const worker = kernable.worker(box);
  const running = worker.run();
  try {
    await worker.ready;
    for (const id of ["b1", "b2", "b3"]) assert.ok((await submit(id, "Open")).committed, id);
    const failed = await submit("b4", "Fail");
    assert.ok(!failed.committed && failed.code === "handler_error", JSON.stringify(failed));
    // The rejection is kept for good: it names the error's type, not its text.
    const leaked = await submit("b6", "Leak");
    assert.deepEqual(leaked, { committed: false, code: "handler_error", message: "TypeError" });
    assert.ok((await submit("b5", "Open")).committed, "the worker is still there");
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});

test("a new reactor with a store starts at the newest event, then resumes from its store", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, by: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: id, type: "Increment", data: { by }, wait: "committed" });
  const worker = kernable.worker(counter);
  const running = [worker.run()];
  const seen: number[] = [];
  const mailer = counters.reactor("stored-mailer", { store: `sqlite:${join(kernel.dir, "mailer.db")}` })
    .on("Incremented", (data: { by: number }) => void seen.push(data.by));
  const reactor = kernable.listener(mailer);
  try {
    await worker.ready;
    for (const by of [1, 2]) await increment("c1", by);
    running.push(reactor.run());
    await reactor.ready;
    await increment("c1", 3);
    await until("the new event arrives", () => seen.length > 0);
    await sleep(100);
    assert.deepEqual(seen, [3]);
  } finally {
    worker.stop();
    reactor.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});

test("plain text goes only to a kernel on this machine, unless allowed", () => {
  const kernel = new TestKernel(["counters.*"]);
  try {
    const client = (url: string, insecure?: boolean) => new Kernable({ url, key: kernel.appKey, insecure }).close();
    for (const url of ["unix:/tmp/k.sock", "http://127.0.0.1:7233", "localhost:7233", "[::1]:7233", "127.0.0.2:7233"]) client(url);
    for (const url of ["http://10.0.0.5:7233", "kernel.internal:7233", "http://127.0.0.1.example.com:7233"]) {
      assert.throws(() => client(url), /not on this machine/, url);
    }
    client("http://10.0.0.5:7233", true);
  } finally {
    void kernel.dispose();
  }
});

test("a request waits for the kernel to come back, until its deadline", async () => {
  const kernel = new TestKernel(["counters.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const increment = (id: string, timeoutMs?: number) =>
    kernable.submit({ domain: "counters", aggregateType: "Counter", aggregateId: "c1", type: "Increment", data: { by: 1 }, id, wait: "committed", timeoutMs });
  const worker = kernable.worker(counter);
  const running = worker.run({ reconnectMs: 50 });
  try {
    await worker.ready;
    await kernel.kill();
    // Down for good, as far as this request knows: it gives up at its deadline.
    const started = Date.now();
    await assert.rejects(increment("i0", 300), (err: { code: number }) => err.code === 4); // DEADLINE_EXCEEDED
    assert.ok(Date.now() - started < 2_000);
    // Back while the request waits: it goes through.
    const waiting = increment("i1");
    await sleep(300);
    await kernel.start();
    assert.ok((await waiting).committed);
  } finally {
    worker.stop();
    await running;
    kernable.close();
    await kernel.dispose();
  }
});

test("reconnects back off, and a refusal is logged as an error", async () => {
  const kernel = new TestKernel(["counters.*"]);
  const before = process.env.KERNABLE_LOG;
  process.env.KERNABLE_LOG = "warn";
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  if (before === undefined) delete process.env.KERNABLE_LOG;
  else process.env.KERNABLE_LOG = before;
  const lines: string[] = [];
  const print = console.error;
  console.error = (line: string) => void lines.push(line);
  // No kernel at all: every attempt fails, and each waits longer.
  const worker = kernable.worker(counter);
  const running = worker.run({ reconnectMs: 50 });
  try {
    await until("four attempts", () => lines.length >= 4);
    const delays = lines.slice(0, 4).map((l) => Number(/reconnecting in ([\d.]+) s/.exec(l)![1]));
    for (let i = 1; i < delays.length; i++) assert.ok(delays[i] > delays[i - 1] * 1.3, `${delays}`);
  } finally {
    worker.stop();
    await running;
  }
  // A worker for an aggregate the key may not run: refused, an error (a client that logs errors
  // only still prints it).
  lines.length = 0;
  await kernel.start();
  process.env.KERNABLE_LOG = "error";
  const strict = new Kernable({ url: kernel.url, key: kernel.appKey });
  if (before === undefined) delete process.env.KERNABLE_LOG;
  else process.env.KERNABLE_LOG = before;
  const other = domain("other").aggregate("Thing", 0).command("Do", () => []);
  const refused = strict.worker(other);
  const refusing = refused.run({ reconnectMs: 50 });
  try {
    await until("a refusal", () => lines.some((l) => /refused \(/.test(l)));
  } finally {
    refused.stop();
    await refusing;
    console.error = print;
    strict.close();
    kernable.close();
    await kernel.dispose();
  }
});

test("a handler that hangs is reported: a command past its lease, a query past 5 s", async () => {
  const kernel = new TestKernel(["counters.*"], ["--lease-ttl-ms", "500", "--query-timeout-ms", "8000"]);
  await kernel.start();
  const before = process.env.KERNABLE_LOG;
  process.env.KERNABLE_LOG = "error";
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  if (before === undefined) delete process.env.KERNABLE_LOG;
  else process.env.KERNABLE_LOG = before;
  let release!: () => void;
  const stuck = new Promise<void>((resolve) => (release = resolve));
  const slow = domain("counters").aggregate("Slow", 0).command("Do", async () => {
    await stuck;
    return [];
  });
  const model = domain("counters").readModel("slow-answers").query("Ask", async () => {
    await stuck;
    return 1;
  });
  const lines: string[] = [];
  const print = console.error;
  console.error = (line: string) => void lines.push(line);
  const worker = kernable.worker(slow);
  const listener = kernable.listener(model);
  const running = [worker.run(), listener.run()];
  try {
    await Promise.all([worker.ready, listener.ready]);
    void kernable.enqueue({ domain: "counters", aggregateType: "Slow", aggregateId: "s1", type: "Do" });
    await until("the command is reported", () => lines.some((l) => /Slow\/s1 still running after its lease ended/.test(l)));
    const asked = kernable.query({ domain: "counters", type: "Ask" }).catch(() => undefined);
    await sleep(5_500);
    assert.ok(lines.some((l) => /still handling the query counters\.Ask after 5 s/.test(l)), lines.join("\n"));
    release();
    await asked;
  } finally {
    console.error = print;
    worker.stop();
    listener.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
