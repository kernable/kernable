// A kernel that misbehaves, played by a fake gRPC server: a listener must not handle an event
// twice when batches repeat or go back, nor acknowledge what it did not handle.

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

const PROTO = fileURLToPath(new URL("../../../proto/kernable/v1/kernable.proto", import.meta.url));
const v1 = (grpc.loadPackageDefinition(protoLoader.loadSync(PROTO, { keepCase: true, longs: Number, defaults: true, oneofs: true })) as any)
  .kernable.v1;

const event = (position: number) => ({
  global_position: position,
  tenant_id: "t1",
  domain: "counters",
  aggregate_type: "Counter",
  aggregate_id: "c1",
  event_type: "Incremented",
  version: position,
  schema: 1,
  payload: Buffer.from(JSON.stringify({ by: position })),
});

test("a listener handles each event once when the kernel repeats batches or goes back", async () => {
  const keys = new TestKernel(["counters.*"]); // only for its key files
  const dir = mkdtempSync("/tmp/kn-fake-");
  const url = `unix:${join(dir, "fake.sock")}`;
  const hellos: (number | undefined)[] = [];
  const acks: number[] = [];
  const server = new grpc.Server();
  server.addService(v1.ListenerService.service, {
    Listen: (call: grpc.ServerDuplexStream<any, any>) => {
      let step = 0;
      call.on("data", (m: any) => {
        if (m.hello) {
          hellos.push(m.hello.from_position ?? undefined);
          if (hellos.length > 1) return call.end(); // the reconnect: seen, that is all
          call.write({ ready: { after_position: 0, head_position: 0 } });
          call.write({ batch: { events: [event(1), event(2)], up_to: 2 } });
        } else if (m.ack) {
          acks.push(m.ack.position);
          step++;
          // The same events again with a new one, then a batch that goes back.
          if (step === 1) call.write({ batch: { events: [event(1), event(2), event(3)], up_to: 3 } });
          if (step === 2) call.write({ batch: { events: [event(2)], up_to: 2 } });
        }
      });
      call.on("error", () => {});
    },
  });
  await new Promise<void>((resolve, reject) =>
    server.bindAsync(url, grpc.ServerCredentials.createInsecure(), (err) => (err ? reject(err) : resolve())),
  );
  const kernable = new Kernable({ url, key: keys.appKey });
  const handled: number[] = [];
  const model = kernable.listener(domain("counters").readModel("fake", { reconnectMs: 50 }).on("Incremented", (data: { by: number }) => void handled.push(data.by)));
  const running = model.run();
  try {
    for (let i = 0; i < 250 && hellos.length < 2; i++) await new Promise((r) => setTimeout(r, 20));
    assert.deepEqual(handled, [1, 2, 3], "each event once");
    assert.deepEqual(acks, [2, 3], "no ack for the batch that went back");
    assert.deepEqual(hellos, [0, 3], "the reconnect resumes after what was handled");
  } finally {
    model.stop();
    await running;
    kernable.close();
    server.forceShutdown();
    rmSync(dir, { recursive: true, force: true });
    await keys.dispose();
  }
});
