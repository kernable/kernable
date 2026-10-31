// Query answers: what a handler returns is what the caller gets, also no answer at all.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Kernable, domain } from "../src/index.ts";
import { TestKernel } from "./kernel.ts";

test("a query handler's null answer arrives as null, not as {}", async () => {
  const kernel = new TestKernel(["shop.*"]);
  await kernel.start();
  const kernable = new Kernable({ url: kernel.url, key: kernel.appKey });
  const carts = kernable.listener(domain("shop").readModel("carts")
    .query("Find", (data: { id: string }) => (data.id === "c1" ? { items: 0 } : null))
    .query("Nothing", () => {}));
  const running = [carts.run()];
  const ask = (type: string, id?: string) => kernable.query({ domain: "shop", type, data: { id } });
  try {
    await carts.ready;
    assert.deepEqual(await ask("Find", "c1"), { items: 0 });
    assert.equal(await ask("Find", "c2"), null);
    assert.equal(await ask("Nothing"), null);
  } finally {
    carts.stop();
    await Promise.allSettled(running);
    kernable.close();
    await kernel.dispose();
  }
});
