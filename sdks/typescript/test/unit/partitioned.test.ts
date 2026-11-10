// The partition map: read once and kept; a refresh reads it anew, also while a read is under way.

import assert from "node:assert/strict";
import { test } from "node:test";

import { Partitioned, type Clients, type WirePartitionMap } from "../../src/rpc.ts";

function mapOf(epoch: number): WirePartitionMap {
  return { epoch, partitions: [], tenants: [], default: "", control: "" };
}

test("a refresh reads the map anew even while an earlier read is under way", async () => {
  const pending: ((m: WirePartitionMap) => void)[] = [];
  const entry = { map: () => new Promise<WirePartitionMap>((resolve) => pending.push(resolve)) } as unknown as Clients;
  const parts = new Partitioned("x", () => entry);

  const first = parts.current();
  const second = parts.current();
  assert.equal(pending.length, 1, "callers share one read");
  const refreshed = parts.current(true);
  assert.equal(pending.length, 2, "the refresh started a read of its own");

  // The newer read answers first; the older one, finishing later, does not replace it.
  pending[1](mapOf(2));
  assert.equal((await refreshed)?.epoch, 2);
  pending[0](mapOf(1));
  assert.equal((await first)?.epoch, 1);
  assert.equal((await second)?.epoch, 1);
  assert.equal((await parts.current())?.epoch, 2);
  assert.equal(pending.length, 2, "kept: no further read");
});
